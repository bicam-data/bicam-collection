"""
Optimized Storage Manager for high-volume data processing with hierarchical checkpoints.

This module provides efficient batched storage with:
- In-memory buffering with periodic flushes
- PostgreSQL COPY for bulk inserts
- SQLite for high-frequency checkpointing
- Asynchronous write queues
- Comprehensive checkpoint management for staging operations
"""

import asyncio
import contextlib
import hashlib
import io
import json
import logging
import sqlite3
import time
import uuid
from collections import defaultdict
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any, Literal

import asyncpg
from asyncpg.pool import Pool

from ..libs.hierarchical_checkpoint_system import (
    CleaningPhase,
    FetchingPhase,
    HierarchicalCheckpointManager,
    ProcessingStage,
    StagingCheckpoint,
    StagingPhase,
)

logger = logging.getLogger(__name__)


class OptimizedStorageManager:
    """
    High-performance storage manager optimized for 19M+ records with checkpoint support.

    Features:
    - In-memory buffering with configurable batch sizes
    - PostgreSQL COPY for bulk inserts (100x faster than INSERT)
    - Asynchronous write queue to prevent blocking
    - SQLite for lightweight checkpointing
    - Automatic table creation and schema management
    - Hierarchical checkpoint management for staging operations
    """

    def __init__(
        self,
        pg_pool: Pool,
        checkpoint_db_path: str = "hierarchical_checkpoints.db",
        batch_size: int = 10000,
        flush_interval: int = 10,
        max_memory_mb: int = 500,
    ):
        self.pg_pool = pg_pool
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.max_memory_mb = max_memory_mb

        # Initialize checkpoint manager
        self.checkpoint_manager = HierarchicalCheckpointManager(checkpoint_db_path)

        # SQLite for checkpointing (much faster for frequent writes)
        self.checkpoint_db = checkpoint_db_path
        self._init_checkpoint_db()

        # In-memory buffers for each table
        self.buffers = defaultdict(list)
        self.buffer_sizes = defaultdict(int)

        # Write queue for async processing
        self.write_queue = asyncio.Queue(maxsize=100)
        self.flush_lock = asyncio.Lock()

        # Statistics
        self.stats = {
            "total_written": 0,
            "total_batches": 0,
            "total_time": 0,
            "records_per_second": 0,
        }

        # Start background workers
        self.workers = []
        self.running = True

        logger.info(
            f"OptimizedStorageManager initialized: batch_size={batch_size}, "
            f"flush_interval={flush_interval}s, max_memory={max_memory_mb}MB"
        )

    def _init_checkpoint_db(self):
        """Initialize SQLite database for lightweight checkpointing."""
        conn = sqlite3.connect(self.checkpoint_db)
        conn.execute("PRAGMA journal_mode=WAL")  # Better concurrency
        conn.execute("PRAGMA synchronous=NORMAL")  # Faster writes
        conn.execute("PRAGMA cache_size=10000")  # More cache

        # Simple checkpoint table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS checkpoints (
                data_type TEXT,
                phase TEXT,
                last_offset INTEGER,
                last_item_id TEXT,
                records_processed INTEGER,
                timestamp REAL,
                PRIMARY KEY (data_type, phase)
            )
        """)
        conn.commit()
        conn.close()

    # =============================================================================
    # CHECKPOINT MANAGEMENT METHODS
    # =============================================================================

    def get_staging_checkpoint(self, data_type: str) -> StagingCheckpoint:
        """Get specialized staging checkpoint manager for a data type."""
        return StagingCheckpoint(self.checkpoint_manager, data_type)

    def clear_staging_checkpoints(
        self, data_type: str, phases: list[StagingPhase] | None = None
    ):
        """Clear staging checkpoints for a data type."""
        if phases is None:
            phases = [
                StagingPhase.JSONB_TO_STAGING,
                StagingPhase.EXTRACT_LISTS,
                StagingPhase.VALIDATE_STAGING,
            ]

        for phase in phases:
            self.checkpoint_manager.reset_checkpoint(
                ProcessingStage.STAGING, phase.value, data_type
            )

        logger.info(
            f"Cleared staging checkpoints for {data_type}: {[p.value for p in phases]}"
        )

    def list_staging_checkpoints(self, data_type: str) -> dict[str, Any]:
        """List all staging checkpoints for a data type."""
        return self.checkpoint_manager.get_progress_summary(data_type)

    def get_staging_checkpoint_stats(self, data_type: str) -> dict[str, Any]:
        """Get detailed staging checkpoint statistics."""
        staging_checkpoint = self.get_staging_checkpoint(data_type)

        stats = {}
        for phase in StagingPhase:
            checkpoint = staging_checkpoint.cm.get_or_create_checkpoint(
                ProcessingStage.STAGING, phase.value, data_type
            )
            stats[phase.value] = {
                "processed_items": checkpoint.processed_items,
                "failed_items": checkpoint.failed_items,
                "total_items": checkpoint.total_items,
                "current_offset": checkpoint.current_offset,
                "current_table": checkpoint.current_table,
                "current_field": checkpoint.current_field,
                "last_updated": checkpoint.updated_at.isoformat()
                if checkpoint.updated_at
                else None,
            }

        return stats

    async def start(self):
        """Start background workers for async processing."""
        # Start write worker
        self.workers.append(asyncio.create_task(self._write_worker()))

        # Start periodic flush worker
        self.workers.append(asyncio.create_task(self._flush_worker()))

        # Start checkpoint saving worker
        self.workers.append(asyncio.create_task(self._checkpoint_worker()))

        logger.info("Storage manager workers started")

    async def stop(self):
        """Stop workers and flush remaining data."""
        logger.info("Stopping storage manager...")
        self.running = False

        # Flush all remaining data
        await self.flush_all()

        # Cancel workers
        for worker in self.workers:
            worker.cancel()

        await asyncio.gather(*self.workers, return_exceptions=True)

        # Final checkpoint flush
        self.checkpoint_manager.flush_all_caches()

        logger.info("Storage manager stopped")

    async def add_records(
        self,
        schema: str,
        table: str,
        records: list[dict],
        ensure_table: bool = True,
        data_type: str | None = None,
        checkpoint_table: bool = False,
    ):
        """
        Add records to buffer for batched writing with checkpoint support.

        This method is non-blocking and returns immediately.
        """
        if not records:
            return

        table_key = f"{schema}.{table.lower()}"

        # Add to buffer
        self.buffers[table_key].extend(records)
        self.buffer_sizes[table_key] += len(records)

        logger.debug(
            f"Added {len(records)} records to buffer for {table_key}. Buffer size: {self.buffer_sizes[table_key]}"
        )

        # Update checkpoint if requested
        if checkpoint_table and data_type:
            staging_checkpoint = self.get_staging_checkpoint(data_type)

            # Mark table as being processed
            staging_checkpoint.cm.mark_item_processed(
                ProcessingStage.STAGING,
                StagingPhase.JSONB_TO_STAGING.value,
                data_type,
                table,
            )

        # Check if we should flush this table
        if self.buffer_sizes[table_key] >= self.batch_size:
            # Queue for flushing
            logger.info(
                f"Buffer for {table_key} reached batch size ({self.buffer_sizes[table_key]}), queuing for flush"
            )
            await self.write_queue.put((table_key, ensure_table, data_type))

    async def _write_worker(self):
        """Background worker that processes write queue."""
        while self.running:
            try:
                # Wait for work with timeout
                table_key, ensure_table, data_type = await asyncio.wait_for(
                    self.write_queue.get(), timeout=1.0
                )

                # Flush this specific table
                await self._flush_table(table_key, ensure_table, data_type)

            except TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Write worker error: {e}")

    async def _flush_worker(self):
        """Periodic flush worker to prevent data sitting too long."""
        while self.running:
            await asyncio.sleep(self.flush_interval)

            try:
                # Find tables that need flushing
                tables_to_flush = []
                for table_key, records in self.buffers.items():
                    if records:  # Has data
                        tables_to_flush.append(table_key)

                # Flush each table
                for table_key in tables_to_flush:
                    await self._flush_table(table_key, True, None)

            except Exception as e:
                logger.error(f"Flush worker error: {e}")

    async def _checkpoint_worker(self):
        """Background worker that periodically saves checkpoints."""
        while self.running:
            try:
                await asyncio.sleep(30)  # Save checkpoints every 30 seconds

                # Flush checkpoint cache
                self.checkpoint_manager.flush_all_caches()

                logger.debug("Saved staging checkpoints")

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Checkpoint worker error: {e}")

    async def _flush_table(
        self, table_key: str, ensure_table: bool = True, data_type: str | None = None
    ):
        """Flush a specific table's buffer to PostgreSQL using COPY."""
        async with self.flush_lock:
            records = self.buffers[table_key]
            if not records:
                return

            # Clear buffer immediately to prevent double-flush
            self.buffers[table_key] = []
            self.buffer_sizes[table_key] = 0

        schema, table = table_key.split(".")

        try:
            start_time = time.time()

            # Use COPY for bulk insert
            await self._bulk_copy(schema, table, records, ensure_table)

            # Update statistics
            elapsed = time.time() - start_time
            self.stats["total_written"] += len(records)
            self.stats["total_batches"] += 1
            self.stats["total_time"] += elapsed

            records_per_second = len(records) / elapsed if elapsed > 0 else 0
            logger.info(
                f"Flushed {len(records)} records to {table_key} "
                f"in {elapsed:.2f}s ({records_per_second:.0f} rec/s)"
            )

            # Update checkpoint if data type is provided
            if data_type:
                staging_checkpoint = self.get_staging_checkpoint(data_type)

                # Update checkpoint state
                checkpoint = staging_checkpoint.cm.get_or_create_checkpoint(
                    ProcessingStage.STAGING,
                    StagingPhase.JSONB_TO_STAGING.value,
                    data_type,
                )
                checkpoint.processed_items += len(records)
                checkpoint.current_table = table
                staging_checkpoint.cm.save_checkpoint(checkpoint)

        except Exception as e:
            logger.error(f"Failed to flush {table_key}: {e}")

            # Log error in checkpoint system if data type is provided
            if data_type:
                staging_checkpoint = self.get_staging_checkpoint(data_type)
                staging_checkpoint.cm.log_error(
                    ProcessingStage.STAGING,
                    StagingPhase.JSONB_TO_STAGING.value,
                    data_type,
                    table,
                    str(e),
                    error_type="bulk_insert",
                )

            # Re-queue the records
            self.buffers[table_key].extend(records)
            self.buffer_sizes[table_key] += len(records)
            raise

    async def _bulk_copy(
        self, schema: str, table: str, records: list[dict], ensure_table: bool = True
    ):
        """Use PostgreSQL COPY for ultra-fast bulk inserts with fixed schema."""
        if not records:
            return

        async with self.pg_pool.acquire() as conn:
            # Ensure table exists if requested
            if ensure_table:
                await self._ensure_table_exists(conn, schema, table, records[0])

            # Use fixed column order for raw tables
            columns = [
                "id_uuid",
                "url",
                "batch_id",
                "scraped_at",
                "payload",
                "source_doc_id",
                "etl_batch_id",
            ]

            # Create CSV data in memory - handle formatting manually for better control
            output = io.BytesIO()
            string_buffer = io.StringIO()

            # Write data using fixed schema with manual formatting
            for record in records:
                # Create the row for fixed schema
                row_values = []

                # Handle each column in the correct order
                for col_name in columns:
                    if col_name == "payload":
                        # Handle payload as JSON with proper escaping
                        # Use the payload directly from the record (already processed)
                        payload_data = record.get("payload")
                        if payload_data:
                            try:
                                json_str = json.dumps(
                                    payload_data,
                                    ensure_ascii=False,
                                    separators=(",", ":"),
                                )
                                # Escape the JSON string for PostgreSQL COPY
                                escaped_json = (
                                    json_str.replace("\\", "\\\\")
                                    .replace("\t", "\\t")
                                    .replace("\n", "\\n")
                                    .replace("\r", "\\r")
                                )
                                row_values.append(escaped_json)
                            except (TypeError, ValueError) as e:
                                logger.error(f"Failed to serialize payload data: {e}")
                                row_values.append("\\N")
                        else:
                            row_values.append("\\N")
                    else:
                        # Handle regular metadata fields
                        value = record.get(col_name)
                        if value is None:
                            row_values.append("\\N")  # PostgreSQL NULL
                        else:
                            # Escape any tabs, newlines, or backslashes in the value
                            escaped_value = (
                                str(value)
                                .replace("\\", "\\\\")
                                .replace("\t", "\\t")
                                .replace("\n", "\\n")
                                .replace("\r", "\\r")
                            )
                            row_values.append(escaped_value)

                # Write the row
                string_buffer.write("\t".join(row_values) + "\n")

            # Convert string buffer to bytes
            string_buffer.seek(0)
            output.write(string_buffer.getvalue().encode("utf-8"))
            output.seek(0)

            # Use COPY command
            await conn.copy_to_table(
                table_name=table,
                source=output,
                schema_name=schema,
                columns=columns,
                delimiter="\t",
                null="\\N",
            )

    async def _ensure_table_exists(
        self, conn: asyncpg.Connection, schema: str, table: str, sample_record: dict
    ):
        """Ensure table exists with proper fixed schema."""
        # Replace dashes with underscores in table name
        safe_table_name = table.replace("-", "_")

        # Check if table exists
        exists = await conn.fetchval(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = $1 AND table_name = $2
            )
            """,
            schema,
            safe_table_name,
        )

        if not exists:
            # Use fixed schema for raw tables - don't unnest JSON fields
            await conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {schema}.{safe_table_name} (
                    id_uuid       TEXT NOT NULL PRIMARY KEY,
                    url           TEXT,
                    batch_id      TEXT,
                    scraped_at    TIMESTAMPTZ,
                    payload       JSONB,
                    source_doc_id TEXT,
                    etl_batch_id  TEXT
                )
            """)

            logger.info(
                f"Created table {schema}.{safe_table_name} with fixed schema (8 columns)"
            )
        else:
            # Table exists, ensure it has all required columns
            await self._ensure_required_columns_exist(conn, schema, safe_table_name)

    async def _ensure_required_columns_exist(
        self, conn: asyncpg.Connection, schema: str, table: str
    ):
        """Ensure existing table has all required columns for fixed schema."""
        # Get existing columns
        existing_columns = await conn.fetch(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = $1 AND table_name = $2
            """,
            schema,
            table,
        )
        existing_column_names = {row["column_name"] for row in existing_columns}

        # Fixed schema columns that should be present
        required_columns = {
            "id_uuid": "TEXT NOT NULL",
            "url": "TEXT",
            "batch_id": "TEXT",
            "scraped_at": "TIMESTAMPTZ",
            "payload": "JSONB",
            "source_doc_id": "TEXT",
            "etl_batch_id": "TEXT",
        }

        # Add missing columns
        for col_name, col_type in required_columns.items():
            if col_name not in existing_column_names:
                logger.info(f"Adding missing column {col_name} to {schema}.{table}")
                await conn.execute(f"""
                    ALTER TABLE {schema}.{table}
                    ADD COLUMN IF NOT EXISTS {col_name} {col_type}
                """)

    async def flush_all(self):
        """Flush all buffers to database."""
        logger.info("Flushing all buffers...")

        tables_to_flush = list(self.buffers.keys())

        # Log buffer status before flush
        buffer_status = {
            table: len(records) for table, records in self.buffers.items() if records
        }
        if buffer_status:
            logger.info(f"Buffer status before flush: {buffer_status}")
        else:
            logger.info("No buffered data to flush")

        for table_key in tables_to_flush:
            if self.buffers[table_key]:
                logger.info(
                    f"Flushing {len(self.buffers[table_key])} records from {table_key}"
                )
                await self._flush_table(table_key, True, None)

        logger.info(
            f"Flush complete. Total written: {self.stats['total_written']:,} records "
            f"in {self.stats['total_batches']} batches"
        )

    def save_checkpoint(
        self,
        data_type: str,
        phase: str,
        offset: int,
        item_id: str,
        records_processed: int,
    ):
        """Save checkpoint to SQLite (fast, lightweight)."""
        conn = sqlite3.connect(self.checkpoint_db)
        try:
            conn.execute(
                """
                INSERT OR REPLACE INTO checkpoints
                (data_type, phase, last_offset, last_item_id, records_processed, timestamp)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (data_type, phase, offset, item_id, records_processed, time.time()),
            )
            conn.commit()
        finally:
            conn.close()

    def get_checkpoint(self, data_type: str, phase: str) -> dict | None:
        """Get checkpoint from SQLite."""
        conn = sqlite3.connect(self.checkpoint_db)
        try:
            cursor = conn.execute(
                """
                SELECT last_offset, last_item_id, records_processed, timestamp
                FROM checkpoints
                WHERE data_type = ? AND phase = ?
                """,
                (data_type, phase),
            )
            row = cursor.fetchone()
            if row:
                return {
                    "last_offset": row[0],
                    "last_item_id": row[1],
                    "records_processed": row[2],
                    "timestamp": row[3],
                }
            return None
        finally:
            conn.close()

    def get_stats(self) -> dict:
        """Get performance statistics."""
        avg_records_per_second = (
            self.stats["total_written"] / self.stats["total_time"]
            if self.stats["total_time"] > 0
            else 0
        )

        return {
            "total_written": self.stats["total_written"],
            "total_batches": self.stats["total_batches"],
            "avg_batch_size": (
                self.stats["total_written"] / self.stats["total_batches"]
                if self.stats["total_batches"] > 0
                else 0
            ),
            "avg_records_per_second": avg_records_per_second,
            "buffer_status": {
                table: len(records) for table, records in self.buffers.items()
            },
        }

    @property
    def total_buffered(self) -> int:
        """Get total number of records currently buffered."""
        return sum(len(records) for records in self.buffers.values())


class OptimizedFetcherStorage:
    """
    Storage adapter for Congressional/GovInfo fetchers with optimized batching and checkpoints.

    This replaces the individual insert operations in the fetchers with
    efficient batched operations and checkpoint management.
    """

    def __init__(
        self,
        storage_manager: OptimizedStorageManager,
        id_field: str = "parent_id",
        fetcher=None,
    ):
        self.storage = storage_manager
        self.id_field = id_field
        self.fetcher = fetcher  # Store fetcher reference for ID extraction
        self.phase_buffers = defaultdict(list)
        self.checkpoint_interval = 10000  # Checkpoint every 10k records
        self.last_checkpoint = 0

    async def store_phase_1_data(
        self, schema: str, table: str, data: dict[str, Any], batch_id: str | None = None
    ):
        """Store Phase 1 data with fixed schema and checkpoint support."""
        # Create record with fixed schema
        record = {
            "id_uuid": data.get("id_uuid", str(uuid.uuid4())),
            "url": data.get("url"),
            "batch_id": batch_id or str(uuid.uuid4()),
            "scraped_at": datetime.now(UTC),
            "etl_batch_id": batch_id or str(uuid.uuid4()),
        }

        # Use fetcher's ID extraction method for source_doc_id
        if self.fetcher and hasattr(self.fetcher, "extract_preliminary_item_id"):
            record["source_doc_id"] = await self.fetcher.extract_preliminary_item_id(
                data
            )
        elif "url" in data and data["url"]:
            record["source_doc_id"] = data["url"].split("/")[-1]
        else:
            record["source_doc_id"] = str(uuid.uuid4())

        # Store original data in payload (everything except metadata fields)
        metadata_fields = {
            "id_uuid",
            "url",
            "batch_id",
            "scraped_at",
            "source_doc_id",
            "etl_batch_id",
        }

        # Store payload data (everything except metadata fields)
        payload_data = {k: v for k, v in data.items() if k not in metadata_fields}
        record["payload"] = payload_data

        # Buffer it
        self.phase_buffers["phase1"].append(record)

        # Get data type for checkpoint
        data_type = getattr(self.fetcher, "data_type_name", "unknown")

        # Add to storage manager (non-blocking) with checkpoint support
        await self.storage.add_records(
            schema, table, [record], data_type=data_type, checkpoint_table=True
        )

    async def store_phase_2_data(
        self, schema: str, table: str, data: dict[str, Any], batch_id: str | None = None
    ):
        """Store Phase 2 data with fixed schema and checkpoint support."""
        # Create record with fixed schema
        if isinstance(data, list):
            logger.debug(f"PHASE 2 STORAGE: Data is a list: {data}")
            data = data[0]

        record = {
            "id_uuid": data.get("id_uuid", str(uuid.uuid4())),
            "url": data.get("url"),
            "batch_id": batch_id or str(uuid.uuid4()),
            "scraped_at": datetime.now(UTC),
            "etl_batch_id": batch_id or str(uuid.uuid4()),
        }

        # Use fetcher's ID extraction method for source_doc_id
        if self.fetcher and hasattr(self.fetcher, "extract_item_id"):
            try:
                # Check if the method is async by trying to call it
                result = self.fetcher.extract_item_id(data)
                if hasattr(result, "__await__"):
                    # It's a coroutine, we need to await it
                    record["source_doc_id"] = await result
                else:
                    # It's a regular return value
                    record["source_doc_id"] = result
            except Exception as e:
                logger.warning(f"Error extracting item ID: {e}")
                record["source_doc_id"] = str(uuid.uuid4())
        elif "url" in data and data["url"]:
            record["source_doc_id"] = data["url"].split("/")[-1]
        else:
            record["source_doc_id"] = str(uuid.uuid4())

        # Store original data in payload (everything except metadata fields)
        metadata_fields = {
            "id_uuid",
            "url",
            "batch_id",
            "scraped_at",
            "source_doc_id",
            "etl_batch_id",
        }

        # Store payload data (everything except metadata fields)
        payload_data = {k: v for k, v in data.items() if k not in metadata_fields}
        record["payload"] = payload_data

        # Buffer it
        self.phase_buffers["phase2"].append(record)

        # Get data type for checkpoint
        data_type = getattr(self.fetcher, "data_type_name", "unknown")

        # Add to storage manager (non-blocking) with checkpoint support
        await self.storage.add_records(
            schema, table, [record], data_type=data_type, checkpoint_table=True
        )

    async def store_phase_3_data(
        self,
        schema: str,
        table_prefix: str,
        data: list[dict[str, Any]],
        parent_id: str,
        batch_id: str | None = None,
    ):
        """Store Phase 3 data with fixed schema and checkpoint support."""
        logger.debug(
            f"PHASE 3 STORAGE: Processing {len(data)} items for parent {parent_id}"
        )
        logger.debug(f"PHASE 3 STORAGE: Schema={schema}, table_prefix={table_prefix}")

        # Log the raw data structure
        logger.debug(f"PHASE 3 STORAGE: Raw data structure: {data}")

        # Get data type for checkpoint
        data_type = getattr(self.fetcher, "data_type_name", "unknown")

        # Group by type
        by_type = defaultdict(list)

        for i, item in enumerate(data):
            logger.debug(f"PHASE 3 STORAGE: Processing item {i}: {item}")

            item_type = item.get("type", "unknown")
            item_data = item.get("data", item)

            logger.debug(
                f"PHASE 3 STORAGE: Item {i} - type='{item_type}', data_type={type(item_data)}"
            )

            # FIXED: item_data is a LIST of related records, not a single dict
            if isinstance(item_data, list):
                logger.debug(
                    f"PHASE 3 STORAGE: Item {i} - Processing list of {len(item_data)} related records"
                )

                # Process each record in the list
                for j, record_data in enumerate(item_data):
                    if isinstance(record_data, dict):
                        logger.debug(
                            f"PHASE 3 STORAGE: Item {i}.{j} - Processing record: {record_data}"
                        )

                        # Create record with fixed schema
                        record = {
                            "id_uuid": str(uuid.uuid4()),
                            "url": record_data.get("url"),
                            "batch_id": batch_id or str(uuid.uuid4()),
                            "scraped_at": datetime.now(UTC),
                            "etl_batch_id": batch_id or str(uuid.uuid4()),
                        }

                        # Phase 3 data should use parent_id as source_doc_id to maintain relationship
                        record["source_doc_id"] = parent_id

                        # Store original data in payload (everything except metadata fields)
                        metadata_fields = {
                            "id_uuid",
                            "url",
                            "batch_id",
                            "scraped_at",
                            "source_doc_id",
                            "etl_batch_id",
                        }
                        payload_data = {
                            k: v
                            for k, v in record_data.items()
                            if k not in metadata_fields
                        }
                        # Add the parent_id to payload for easier querying
                        payload_data[self.id_field] = parent_id

                        record["payload"] = payload_data

                        by_type[item_type].append(record)
                        logger.debug(
                            f"PHASE 3 STORAGE: Item {i}.{j} - Added to by_type['{item_type}']"
                        )
                    else:
                        logger.warning(
                            f"PHASE 3 STORAGE: Item {i}.{j} - record_data is not a dict: {type(record_data)}"
                        )

                logger.debug(
                    f"PHASE 3 STORAGE: Item {i} - Processed {len(item_data)} records for type '{item_type}'"
                )
            else:
                logger.warning(
                    f"PHASE 3 STORAGE: Item {i} - item_data is not a list: {type(item_data)}"
                )
                logger.warning(
                    f"PHASE 3 STORAGE: Item {i} - Raw item_data: {item_data}"
                )

        logger.debug(f"PHASE 3 STORAGE: Grouped by type: {list(by_type.keys())}")

        # Store each type with checkpoint support
        for item_type, items in by_type.items():
            logger.info(
                f"PHASE 3 STORAGE: Storing {len(items)} items of type '{item_type}'"
            )
            # Clean up method name to get just the table suffix
            # e.g., "get_nominations_individualnominees" -> "individualnominees"
            table_suffix = item_type
            if item_type.startswith("get_"):
                table_suffix = item_type[4:]  # Remove "get_" prefix

                # Remove data type prefix if present
                # e.g., "nominations_individualnominees" -> "individualnominees"
                if table_suffix.startswith(f"{table_prefix}_"):
                    table_suffix = table_suffix[len(f"{table_prefix}_") :]

            # For GovInfo granules, use the item_type directly as the table name
            # e.g., "hearingpackages_granules" -> "hearingpackages_granules_raw"
            if item_type.endswith("_granules"):
                table = f"{item_type}_list_raw"
            else:
                table = f"{table_prefix}_{table_suffix}_raw"
            logger.debug(
                f"PHASE 3 STORAGE: Storing {len(items)} items of type '{item_type}' -> table '{table}'"
            )

            # Add to storage manager with checkpoint support
            await self.storage.add_records(
                schema, table, items, data_type=data_type, checkpoint_table=True
            )

            # Mark this related data type as processed in checkpoints
            if data_type != "unknown":
                staging_checkpoint = self.storage.get_staging_checkpoint(data_type)
                staging_checkpoint.mark_list_extracted(table_prefix, table_suffix)

            logger.debug(
                f"PHASE 3 STORAGE: Successfully stored {len(items)} items in {table}"
            )

        if not by_type:
            logger.warning(
                f"PHASE 3 STORAGE: No Phase 3 data items processed for parent {parent_id}"
            )
            logger.warning(
                "PHASE 3 STORAGE: This indicates an issue with data structure parsing"
            )

    async def store_phase_4_data(
        self, data: dict[str, Any], parent_id: str, batch_id: str | None = None
    ):
        """Store Phase 4 full related data with fixed schema and checkpoint support."""
        # Create record with fixed schema
        record = {
            "id_uuid": data.get("id_uuid", str(uuid.uuid4())),
            "url": data.get("granuleLink"),
            "batch_id": batch_id or str(uuid.uuid4()),
            "scraped_at": datetime.now(UTC),
            "etl_batch_id": batch_id or str(uuid.uuid4()),
        }

        # Use fetcher's ID extraction method for source_doc_id
        if self.fetcher and hasattr(self.fetcher, "extract_item_id"):
            try:
                # Check if the method is async by trying to call it
                result = self.fetcher.extract_item_id(data, granule=True)
                if hasattr(result, "__await__"):
                    # It's a coroutine, we need to await it
                    record["source_doc_id"] = await result
                else:
                    # It's a regular return value
                    record["source_doc_id"] = result
            except Exception as e:
                logger.warning(f"Error extracting item ID: {e}")
                record["source_doc_id"] = str(uuid.uuid4())
        elif "granuleLink" in data and data["granuleLink"]:
            record["source_doc_id"] = data["granuleLink"].split("/")[-1]
        else:
            record["source_doc_id"] = str(uuid.uuid4())

        # Store original data in payload (everything except metadata fields)
        metadata_fields = {
            "id_uuid",
            "url",
            "batch_id",
            "scraped_at",
            "source_doc_id",
            "etl_batch_id",
        }

        # Store payload data (everything except metadata fields)
        payload_data = {k: v for k, v in data.items() if k not in metadata_fields}

        # we do not need to add a parent ID because packageId is built into the payload already

        record["payload"] = payload_data

        # Buffer it
        self.phase_buffers["phase4"].append(record)

        # Use consistent naming scheme for checkpoint and table name
        data_type_name = getattr(self.fetcher, "data_type_name", "unknown")
        granule_table_name = f"{data_type_name}_granules"

        # Add to storage manager (non-blocking) with checkpoint support
        await self.storage.add_records(
            "bicam_raw_govinfo",
            f"{granule_table_name}_raw",
            [record],
            data_type=granule_table_name,  # Use consistent naming for checkpointing
            checkpoint_table=True,
        )

    def should_checkpoint(self, total_processed: int) -> bool:
        """Check if we should save a checkpoint."""
        if total_processed - self.last_checkpoint >= self.checkpoint_interval:
            self.last_checkpoint = total_processed
            return True
        return False

    def save_checkpoint(
        self,
        data_type: str,
        phase: str,
        offset: int,
        item_id: str,
        total_processed: int,
    ):
        """Save checkpoint using hierarchical system."""
        # Map old phase names to new hierarchical phases
        if phase == "list_items":
            stage = ProcessingStage.FETCHING
            phase_name = FetchingPhase.LIST_ITEMS.value
        elif phase == "full_data":
            stage = ProcessingStage.FETCHING
            phase_name = FetchingPhase.FULL_DATA.value
        elif phase == "related_data":
            stage = ProcessingStage.FETCHING
            phase_name = FetchingPhase.RELATED_DATA.value
        elif phase == "full_related_data":
            stage = ProcessingStage.FETCHING
            phase_name = FetchingPhase.FULL_RELATED_DATA.value
        else:
            # Default to fetching stage for unknown phases
            stage = ProcessingStage.FETCHING
            phase_name = phase

        checkpoint = self.storage.checkpoint_manager.get_or_create_checkpoint(
            stage, phase_name, data_type
        )
        checkpoint.current_offset = offset
        checkpoint.current_item_id = item_id
        checkpoint.processed_items = total_processed
        self.storage.checkpoint_manager.save_checkpoint(checkpoint)


class OptimizedNormalizerStorage:
    """
    Storage adapter for StreamlinedNormalizer with dynamic schema support.

    Handles all database operations including:
    - Dynamic table creation (all TEXT columns, no constraints)
    - Bulk inserts using PostgreSQL COPY
    - Schema evolution (adding missing columns)
    - Efficient batching and flushing
    """

    def __init__(
        self,
        storage_manager: OptimizedStorageManager,
        target_schema: str = "bicam_staging",
        data_type: str = None,
    ):
        """
        Initialize the normalizer storage adapter.

        Args:
            storage_manager: The underlying optimized storage manager
            target_schema: Target schema for normalized data
            data_type: Data type being processed (for checkpointing)
        """
        self.storage = storage_manager
        self.target_schema = target_schema
        self.data_type = data_type

        # Table schema cache to avoid repeated operations
        self._table_schema_cache = {}
        self._cache_lock = asyncio.Lock()

        # Batch accumulator for bulk operations
        self.batch_accumulator = defaultdict(list)
        self.batch_sizes = defaultdict(int)
        self.max_batch_size = 5000

        logger.info(f"OptimizedNormalizerStorage initialized for {target_schema}")

    async def store_normalized_records(
        self,
        table_name: str,
        records: list[dict[str, Any]],
        record_type: Literal["main", "related", "nested"] = "main",
        ensure_table: bool = True,
        checkpoint: bool = True,
    ) -> dict[str, int]:
        """
        Store normalized records with dynamic schema support.

        Args:
            table_name: Target table name
            records: List of records to store
            record_type: Type of records (affects deduplication)
            ensure_table: Whether to ensure table exists
            checkpoint: Whether to update checkpoints

        Returns:
            Dictionary with storage statistics
        """
        if not records:
            return {"stored": 0, "errors": 0}

        # Convert field names to lowercase for PostgreSQL compatibility
        normalized_records = []
        for record in records:
            normalized_record = {k.lower(): v for k, v in record.items()}
            normalized_records.append(normalized_record)

        # Add to batch accumulator
        key = f"{self.target_schema}.{table_name.lower()}"
        self.batch_accumulator[key].extend(normalized_records)
        self.batch_sizes[key] += len(normalized_records)

        logger.debug(
            f"Added {len(records)} {record_type} records to buffer for {key}. "
            f"Buffer size: {self.batch_sizes[key]}"
        )

        # Check if we should flush
        if self.batch_sizes[key] >= self.max_batch_size:
            return await self._flush_table(table_name, ensure_table, checkpoint)

        # Return provisional stats (records added but not yet flushed)
        return {"stored": len(records), "errors": 0}

    async def _flush_table(
        self, table_name: str, ensure_table: bool = True, checkpoint: bool = True
    ) -> dict[str, int]:
        """Flush a specific table's buffer to PostgreSQL."""
        key = f"{self.target_schema}.{table_name.lower()}"
        records = self.batch_accumulator.get(key, [])

        if not records:
            return {"stored": 0, "errors": 0}

        # Clear buffer immediately to prevent double-flush
        self.batch_accumulator[key] = []
        self.batch_sizes[key] = 0

        stats = {"stored": 0, "errors": 0}

        try:
            # Get database pool
            db_pool = self.storage.pg_pool

            async with db_pool.acquire() as conn:
                # Ensure table exists with dynamic schema
                if ensure_table:
                    # Create combined sample record with ALL columns
                    combined_sample = {}
                    for record in records:
                        combined_sample.update(record)

                    await self._ensure_table_with_schema(
                        conn, table_name, combined_sample
                    )

                # Perform bulk insert using COPY
                stored_count = await self._bulk_copy_insert(conn, table_name, records)

                stats["stored"] = stored_count

                # Update checkpoint if requested
                if checkpoint and self.data_type:
                    staging_checkpoint = self.storage.get_staging_checkpoint(
                        self.data_type
                    )
                    staging_checkpoint.mark_table_processed(table_name)

                logger.info(
                    f"Flushed {stored_count} records to {self.target_schema}.{table_name}"
                )

        except Exception as e:
            logger.error(f"Error flushing {table_name}: {e}")
            stats["errors"] = len(records)

            # Re-add records to buffer for retry
            self.batch_accumulator[key].extend(records)
            self.batch_sizes[key] += len(records)

            # Log error in checkpoint system
            if checkpoint and self.data_type:
                staging_checkpoint = self.storage.get_staging_checkpoint(self.data_type)
                staging_checkpoint.cm.log_error(
                    ProcessingStage.STAGING,
                    StagingPhase.JSONB_TO_STAGING.value,
                    self.data_type,
                    table_name,
                    str(e),
                    error_type="bulk_insert",
                )

        return stats

    async def _ensure_table_with_schema(
        self, conn: asyncpg.Connection, table_name: str, sample_record: dict[str, Any]
    ) -> bool:
        """Ensure table exists with proper schema (all TEXT columns, no constraints)."""
        # Ensure schema exists
        await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self.target_schema}")

        cache_key = f"{self.target_schema}.{table_name.lower()}"

        # Check cache first
        table_exists = False
        async with self._cache_lock:
            if cache_key in self._table_schema_cache:
                table_exists = True

        # Use advisory lock for this table to prevent concurrent modifications
        lock_id = int(hashlib.md5(cache_key.encode()).hexdigest()[:8], 16)

        try:
            # Try to acquire lock
            lock_acquired = await conn.fetchval(
                "SELECT pg_try_advisory_lock($1)", lock_id
            )

            if not lock_acquired:
                # Wait for lock
                await conn.fetchval("SELECT pg_advisory_lock($1)", lock_id)

            # Check table existence if not cached
            if not table_exists:
                table_exists = await self._table_exists(conn, table_name.lower())

                if table_exists:
                    # Cache existence
                    async with self._cache_lock:
                        self._table_schema_cache[cache_key] = True

            if not table_exists:
                # Create table with all TEXT columns, no constraints
                await self._create_table(conn, table_name.lower(), sample_record)
                logger.info(f"Created table {self.target_schema}.{table_name}")

                # Cache existence
                async with self._cache_lock:
                    self._table_schema_cache[cache_key] = True

                return True
            else:
                # Table exists, check for missing columns
                await self._add_missing_columns(conn, table_name.lower(), sample_record)
                return True

        finally:
            # Release lock
            try:
                await conn.fetchval("SELECT pg_advisory_unlock($1)", lock_id)
            except Exception as e:
                logger.warning(f"Failed to release lock for {table_name}: {e}")

    async def _table_exists(self, conn: asyncpg.Connection, table_name: str) -> bool:
        """Check if table exists."""
        result = await conn.fetchval(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = $1 AND table_name = $2
            )
            """,
            self.target_schema,
            table_name.lower(),
        )
        return result

    async def _create_table(
        self, conn: asyncpg.Connection, table_name: str, sample_record: dict[str, Any]
    ) -> bool:
        """Create table with all TEXT columns, no constraints."""
        try:
            # Replace dashes with underscores in table name
            safe_table_name = table_name.replace("-", "_")

            # Build column definitions - ALL TEXT, no constraints
            columns = []
            for key in sample_record:
                # Replace dashes with underscores in column names
                safe_column_name = key.replace("-", "_")
                columns.append(f"{safe_column_name} TEXT")

            # Create table
            create_sql = f"""
                CREATE TABLE IF NOT EXISTS {self.target_schema}.{safe_table_name} (
                    {", ".join(columns)}
                )
            """

            await conn.execute(create_sql)
            return True

        except asyncpg.DuplicateTableError:
            # Table already exists (concurrent creation)
            logger.debug(f"Table {self.target_schema}.{table_name} already exists")
            return True
        except Exception as e:
            # Handle PostgreSQL type constraint violations
            if "duplicate key value violates unique constraint" in str(e):
                logger.debug(f"Table type for {table_name} already exists (concurrent)")
                # Check if table actually exists now
                if await self._table_exists(conn, table_name.lower()):
                    return True

            logger.error(f"Error creating table {table_name}: {e}")
            raise

    async def _add_missing_columns(
        self, conn: asyncpg.Connection, table_name: str, sample_record: dict[str, Any]
    ) -> None:
        """Add missing columns to existing table (all as TEXT)."""
        # Replace dashes with underscores in table name
        safe_table_name = table_name.replace("-", "_")

        # Get existing columns
        rows = await conn.fetch(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = $1 AND table_name = $2
            """,
            self.target_schema,
            safe_table_name,
        )
        existing_columns = {row["column_name"].lower() for row in rows}

        # Find missing columns
        missing_columns = []
        for key in sample_record:
            # Replace dashes with underscores in column names for comparison
            safe_key = key.replace("-", "_")
            if safe_key.lower() not in existing_columns:
                missing_columns.append(safe_key)

        if missing_columns:
            logger.info(
                f"Adding {len(missing_columns)} columns to {safe_table_name}: {missing_columns}"
            )

            for col in missing_columns:
                try:
                    alter_sql = f"""
                        ALTER TABLE {self.target_schema}.{safe_table_name}
                        ADD COLUMN {col} TEXT
                    """
                    await conn.execute(alter_sql)
                    logger.debug(f"Added column {col} to {safe_table_name}")
                except Exception as e:
                    if "already exists" in str(e).lower():
                        logger.debug(f"Column {col} already exists (concurrent)")
                    else:
                        logger.error(f"Failed to add column {col}: {e}")

    async def _bulk_copy_insert(
        self, conn: asyncpg.Connection, table_name: str, records: list[dict[str, Any]]
    ) -> int:
        """Use PostgreSQL COPY for high-performance bulk inserts."""
        if not records:
            return 0

        # Get all unique columns from all records and convert dashes to underscores
        all_columns = set().union(*(record.keys() for record in records))
        # Replace dashes with underscores in column names
        safe_columns = [col.replace("-", "_") for col in all_columns]
        columns = sorted(safe_columns)

        # Create mapping from safe column names back to original names
        column_mapping = {}
        for original_col in all_columns:
            safe_col = original_col.replace("-", "_")
            column_mapping[safe_col] = original_col

        # Create tab-separated values
        output = io.StringIO()

        for record in records:
            row_values = []
            for col in columns:
                # Get the original column name for record lookup
                original_col = column_mapping.get(col, col)
                value = record.get(original_col)
                if value is None:
                    row_values.append("\\N")  # PostgreSQL NULL
                elif isinstance(value, bool):
                    row_values.append(str(value).lower())
                elif isinstance(value, dict | list):
                    # JSON encode complex types
                    json_str = json.dumps(value)
                    # Escape special characters
                    json_str = (
                        json_str.replace("\\", "\\\\")
                        .replace("\t", "\\t")
                        .replace("\n", "\\n")
                        .replace("\r", "\\r")
                    )
                    row_values.append(json_str)
                else:
                    # Convert to string and escape
                    str_value = str(value)
                    str_value = (
                        str_value.replace("\\", "\\\\")
                        .replace("\t", "\\t")
                        .replace("\n", "\\n")
                        .replace("\r", "\\r")
                    )
                    row_values.append(str_value)

            output.write("\t".join(row_values) + "\n")

        # Reset to beginning
        output.seek(0)

        # Use COPY to insert - asyncpg copy_records_to_table method
        # Convert StringIO data to list of tuples for copy_records_to_table
        records_data = []
        for line in output:
            if line.strip():  # Skip empty lines
                values = line.rstrip("\n").split("\t")
                # Convert '\N' back to None for NULL values
                processed_values = [None if v == "\\N" else v for v in values]
                records_data.append(tuple(processed_values))

        if records_data:
            # Replace dashes with underscores in table name
            safe_table_name = table_name.replace("-", "_")
            await conn.copy_records_to_table(
                safe_table_name,
                records=records_data,
                columns=columns,
                schema_name=self.target_schema,
                timeout=300.0,  # 5 minute timeout
            )

        return len(records)

    async def flush_all(self) -> dict[str, int]:
        """Flush all buffered records to database."""
        logger.info("Flushing all normalizer buffers...")

        total_stats = {"stored": 0, "errors": 0}
        tables_to_flush = list(self.batch_accumulator.keys())

        for table_key in tables_to_flush:
            if self.batch_accumulator[table_key]:
                # Extract table name from key
                table_name = table_key.split(".")[-1]

                stats = await self._flush_table(
                    table_name, ensure_table=True, checkpoint=True
                )

                total_stats["stored"] += stats["stored"]
                total_stats["errors"] += stats["errors"]

        logger.info(
            f"Normalizer flush complete: {total_stats['stored']} stored, "
            f"{total_stats['errors']} errors"
        )

        return total_stats

    def get_buffer_status(self) -> dict[str, int]:
        """Get current buffer status."""
        return {
            table: len(records)
            for table, records in self.batch_accumulator.items()
            if records
        }

    async def store_multiple_tables(
        self, table_records: dict[str, list[dict[str, Any]]], checkpoint: bool = True
    ) -> dict[str, Any]:
        """
        Store records for multiple tables in a single operation.

        Args:
            table_records: Dictionary mapping table names to lists of records
            checkpoint: Whether to update checkpoints

        Returns:
            Storage statistics
        """
        total_stats = {"tables": 0, "stored": 0, "errors": 0}

        for table_name, records in table_records.items():
            if not records:
                continue

            stats = await self.store_normalized_records(
                table_name=table_name,
                records=records,
                ensure_table=True,
                checkpoint=checkpoint,
            )

            total_stats["tables"] += 1
            total_stats["stored"] += stats["stored"]
            total_stats["errors"] += stats["errors"]

        return total_stats


class OptimizedCleanerStorage:
    """
    Storage adapter for StreamlinedCleaner with production table support.

    Handles:
    - Streaming data from staging tables
    - Bulk inserts to production tables
    - Schema management for production tables
    - Checkpoint integration
    """

    def __init__(
        self,
        storage_manager: "OptimizedStorageManager",
        staging_schema: str = "bicam_staging_congressional",
        production_schema: str = "bicam_congressional",
        data_type: str = None,
    ):
        """
        Initialize the cleaner storage adapter.

        Args:
            storage_manager: The underlying optimized storage manager
            staging_schema: Source schema for staging data
            production_schema: Target schema for cleaned data
            data_type: Data type being processed (for checkpointing)
        """
        self.storage = storage_manager
        self.staging_schema = staging_schema
        self.production_schema = production_schema
        self.data_type = data_type

        # Production table schema cache
        self._prod_schema_cache = {}
        self._cache_lock = asyncio.Lock()

        logger.info(
            f"OptimizedCleanerStorage initialized: "
            f"{staging_schema} -> {production_schema}"
        )

    async def stream_staging_data(
        self,
        table_name: str,
        batch_size: int = 1000,
        order_by: str = None,
        where_clause: str = None,
        checkpoint_offset: int = 0,
        multi_table_data_types: dict[str, list[str]] = None,
        custom_logic=None,
    ) -> AsyncIterator[list[dict[str, Any]]]:
        """
        Stream data from staging table in batches.

        Args:
            table_name: Staging table to read from
            batch_size: Number of records per batch
            order_by: Column to order by (auto-detected if None)
            where_clause: Optional WHERE clause
            checkpoint_offset: Start offset for checkpoint-based streaming
            multi_table_data_types: Multi-table configuration for complex joins
            custom_logic: Custom logic instance that may have custom streaming methods

        Yields:
            Batches of records as list of dictionaries
        """
        # First, check if custom logic has a specific streaming method for this table
        if custom_logic and hasattr(
            custom_logic, f"_stream_{table_name}_joined_chunks"
        ):
            logger.info(f"Using custom streaming method for {table_name}")
            custom_stream_method = getattr(
                custom_logic, f"_stream_{table_name}_joined_chunks"
            )

            # Call the custom streaming method with batch_size
            async for chunk in custom_stream_method(batch_size):
                yield chunk
            return

        # Check if this is a multi-table data type that needs special handling
        if multi_table_data_types and table_name in multi_table_data_types:
            logger.info(f"Using multi-table streaming for {table_name}")
            async for chunk in self._stream_multi_table_data(
                table_name,
                batch_size,
                checkpoint_offset,
                multi_table_data_types[table_name],
            ):
                yield chunk
            return

        # Fall back to standard single-table streaming
        logger.info(f"Using standard streaming for {table_name}")
        async for chunk in self._stream_single_table_data(
            table_name, batch_size, order_by, where_clause, checkpoint_offset
        ):
            yield chunk

    async def _stream_multi_table_data(
        self,
        table_name: str,
        batch_size: int,
        checkpoint_offset: int,
        multi_table_data_types: dict[str, list[str]],
    ) -> AsyncIterator[list[dict[str, Any]]]:
        """
        Stream data from multiple related tables with basic joins.

        This is for simple multi-table relationships. For complex aggregations,
        use custom streaming methods in custom logic.

        If there's only one table in the list, it streams from that table directly.
        """
        async with self.storage.pg_pool.acquire() as conn:
            # Get the related tables for this data type
            related_tables = multi_table_data_types.get(table_name, [])
            if not related_tables:
                logger.warning(f"No related tables found for {table_name}")
                return

            # If there's only one table, stream from it directly (like _stream_multi_table_chunks)
            if len(related_tables) == 1:
                main_table = related_tables[0]
                logger.info(
                    f"Single table multi-table streaming for {table_name} from {main_table}"
                )

                # Check if table exists
                table_exists = await conn.fetchval(
                    """
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_schema = $1 AND table_name = $2
                    )
                    """,
                    self.staging_schema,
                    main_table,
                )

                if not table_exists:
                    logger.warning(
                        f"Table {self.staging_schema}.{main_table} does not exist"
                    )
                    return

                # Stream from main table directly
                offset = checkpoint_offset
                while True:
                    query = f"""
                        SELECT * FROM {self.staging_schema}.{main_table}
                        LIMIT {batch_size} OFFSET {offset}
                    """

                    try:
                        rows = await conn.fetch(query)
                        if not rows:
                            break

                        batch = [dict(row) for row in rows]
                        yield batch

                        offset += len(batch)
                        if len(batch) < batch_size:
                            break

                    except Exception as e:
                        logger.error(
                            f"Error streaming {main_table} at offset {offset}: {e}"
                        )
                        raise

            # If there are multiple tables, try a simple join
            elif len(related_tables) >= 2:
                # Build a simple join query
                # For now, assume the first table is the main table and others are related
                main_table = table_name
                related_table = related_tables[0] if related_tables else None

                if not related_table:
                    logger.warning(f"No related table specified for {table_name}")
                    return

                # Check if both tables exist
                tables_exist_query = """
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_schema = $1
                AND table_name = ANY($2)
                """
                tables_count = await conn.fetchval(
                    tables_exist_query, self.staging_schema, [main_table, related_table]
                )

                if tables_count < 2:
                    logger.warning(
                        f"One or both tables {main_table}, {related_table} do not exist"
                    )
                    return

                # Simple LEFT JOIN - assumes they share a common key (usually the main table's primary key)
                # This is a basic implementation - custom logic should handle complex cases
                query = f"""
                SELECT mt.*, rt.*
                FROM {self.staging_schema}.{main_table} mt
                LEFT JOIN {self.staging_schema}.{related_table} rt
                    ON mt.id = rt.{main_table}_id
                ORDER BY mt.id
                LIMIT {batch_size} OFFSET {checkpoint_offset}
                """

                offset = checkpoint_offset
                while True:
                    try:
                        rows = await conn.fetch(query, batch_size, offset)
                        if not rows:
                            break

                        batch = [dict(row) for row in rows]
                        yield batch

                        offset += len(batch)
                        if len(batch) < batch_size:
                            break

                    except Exception as e:
                        logger.error(
                            f"Error streaming multi-table data for {table_name}: {e}"
                        )
                        raise

    async def _stream_single_table_data(
        self,
        table_name: str,
        batch_size: int,
        order_by: str = None,
        where_clause: str = None,
        checkpoint_offset: int = 0,
    ) -> AsyncIterator[list[dict[str, Any]]]:
        """
        Stream data from a single staging table.
        """
        async with self.storage.pg_pool.acquire() as conn:
            # Check table existence
            exists = await conn.fetchval(
                """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = $1 AND table_name = $2
                )
                """,
                self.staging_schema,
                table_name,
            )

            if not exists:
                logger.warning(
                    f"Table {self.staging_schema}.{table_name} does not exist"
                )
                return

            # Auto-detect order column if not specified
            if not order_by:
                columns = await conn.fetch(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = $1 AND table_name = $2
                    ORDER BY ordinal_position
                    """,
                    self.staging_schema,
                    table_name,
                )

                column_names = [col["column_name"] for col in columns]

                # Prefer these columns for ordering
                for preferred in ["processed_at", "id", "created_at", "scraped_at"]:
                    if preferred in column_names:
                        order_by = preferred
                        break

                if not order_by and column_names:
                    order_by = column_names[0]

            # Build query
            where_part = f"WHERE {where_clause}" if where_clause else ""
            query = f"""
                SELECT * FROM {self.staging_schema}.{table_name}
                {where_part}
                ORDER BY {order_by}
                LIMIT $1 OFFSET $2
            """

            offset = checkpoint_offset
            records_yielded = 0

            while True:
                try:
                    rows = await conn.fetch(query, batch_size, offset)
                    if not rows:
                        break

                    batch = [dict(row) for row in rows]
                    yield batch

                    records_yielded += len(batch)
                    offset += len(batch)

                    # Update checkpoint periodically using hierarchical system
                    if self.data_type and records_yielded % 10000 == 0:
                        checkpoint = (
                            self.storage.checkpoint_manager.get_or_create_checkpoint(
                                ProcessingStage.CLEANING,
                                CleaningPhase.APPLY_RULES.value,
                                self.data_type,
                            )
                        )
                        checkpoint.current_offset = offset
                        checkpoint.current_item_id = batch[-1].get(
                            "source_doc_id",
                            batch[-1].get(self.id_field, batch[-1].get("id", "")),
                        )
                        checkpoint.processed_items = records_yielded
                        checkpoint.current_table = table_name
                        self.storage.checkpoint_manager.save_checkpoint(checkpoint)

                    if len(batch) < batch_size:
                        break

                except Exception as e:
                    logger.error(
                        f"Error streaming {table_name} at offset {offset}: {e}"
                    )
                    raise

    async def bulk_insert_production(
        self,
        table_name: str,
        records: list[dict[str, Any]],
        upsert: bool = False,
        primary_keys: list[str] = None,
    ) -> int:
        """
        Bulk insert cleaned records to production table.

        Args:
            table_name: Production table name
            records: Cleaned records to insert
            upsert: Whether to use UPSERT (requires primary_keys)
            primary_keys: Primary key columns for UPSERT

        Returns:
            Number of records inserted
        """
        if not records:
            return 0

        async with self.storage.pg_pool.acquire() as conn:
            # Ensure production schema and table exist
            await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self.production_schema}")
            await self._ensure_production_table(conn, table_name)

            # Use COPY for bulk insert
            if upsert and primary_keys:
                # For UPSERT, we need to use a temp table approach
                return await self._bulk_upsert_via_temp(
                    conn, table_name, records, primary_keys
                )
            else:
                # Direct COPY for simple inserts
                return await self._bulk_copy_production(conn, table_name, records)

    async def _ensure_production_table(self, conn: asyncpg.Connection, table_name: str):
        """Ensure production table exists with proper schema."""
        cache_key = f"{self.production_schema}.{table_name}"

        # Check cache
        async with self._cache_lock:
            if cache_key in self._prod_schema_cache:
                return

        # Check existence
        exists = await conn.fetchval(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = $1 AND table_name = $2
            )
            """,
            self.production_schema,
            table_name,
        )

        if not exists:
            raise Exception(
                f"Table {self.production_schema}.{table_name} does not exist"
            )

        # Cache existence
        async with self._cache_lock:
            self._prod_schema_cache[cache_key] = True

    async def _bulk_copy_production(
        self, conn: asyncpg.Connection, table_name: str, records: list[dict[str, Any]]
    ) -> int:
        """Use COPY for fast bulk inserts to production."""
        if not records:
            return 0

        # Get all unique columns (optimized)
        all_columns = set().union(*(record.keys() for record in records))
        columns = sorted(all_columns)

        # Fast direct conversion to tuples using list comprehension
        records_data = [tuple(record.get(col) for col in columns) for record in records]

        await conn.copy_records_to_table(
            table_name,
            records=records_data,
            columns=columns,
            schema_name=self.production_schema,
            timeout=300.0,
        )

        return len(records)

    async def _bulk_upsert_via_temp(
        self,
        conn: asyncpg.Connection,
        table_name: str,
        records: list[dict[str, Any]],
        primary_keys: list[str],
    ) -> int:
        """Perform bulk UPSERT using temporary table."""
        if not records:
            return 0

        temp_table = f"temp_{table_name}_{id(records)}"

        try:
            # Create temp table
            await conn.execute(f"""
                CREATE TEMP TABLE {temp_table}
                AS SELECT * FROM {self.production_schema}.{table_name}
                WHERE FALSE
            """)

            # Bulk insert to temp table
            inserted = await self._bulk_copy_production(conn, temp_table, records)

            # Get columns for UPDATE clause
            all_columns = set()
            for record in records:
                all_columns.update(record.keys())

            update_columns = [col for col in all_columns if col not in primary_keys]
            update_clause = ", ".join(
                f"{col} = EXCLUDED.{col}" for col in update_columns
            )

            # UPSERT from temp to production
            pk_clause = ", ".join(primary_keys)
            merge_sql = f"""
                INSERT INTO {self.production_schema}.{table_name}
                SELECT * FROM {temp_table}
                ON CONFLICT ({pk_clause}) DO UPDATE SET {update_clause}
            """

            result = await conn.execute(merge_sql)

            # Extract affected rows from result
            if result:
                parts = result.split()
                if len(parts) >= 2:
                    return int(parts[1])

            return inserted

        finally:
            # Clean up temp table
            with contextlib.suppress(Exception):
                await conn.execute(f"DROP TABLE IF EXISTS {temp_table}")

    async def get_staging_record_count(
        self, table_name: str, where_clause: str = None
    ) -> int:
        """Get count of records in staging table."""
        async with self.storage.pg_pool.acquire() as conn:
            where_part = f"WHERE {where_clause}" if where_clause else ""
            query = f"""
                SELECT COUNT(*) FROM {self.staging_schema}.{table_name}
                {where_part}
            """

            return await conn.fetchval(query)

    async def get_production_record_count(self, table_name: str) -> int:
        """Get count of records in production table."""
        async with self.storage.pg_pool.acquire() as conn:
            # Check if table exists first
            exists = await conn.fetchval(
                """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = $1 AND table_name = $2
                )
                """,
                self.production_schema,
                table_name,
            )

            if not exists:
                return 0

            query = f"SELECT COUNT(*) FROM {self.production_schema}.{table_name}"
            return await conn.fetchval(query)

    def get_cleaning_checkpoint(self, table_name: str) -> dict[str, Any] | None:
        """Get cleaning checkpoint for a specific table using hierarchical system."""
        if self.data_type:
            # Use hierarchical checkpoint system instead of old simple one
            checkpoint = self.storage.checkpoint_manager.get_or_create_checkpoint(
                ProcessingStage.CLEANING,
                CleaningPhase.APPLY_RULES.value,
                self.data_type,
            )

            # Return in the format expected by the cleaner
            return {
                "last_offset": checkpoint.current_offset,
                "last_item_id": checkpoint.current_item_id,
                "records_processed": checkpoint.processed_items,
                "timestamp": checkpoint.updated_at.timestamp()
                if checkpoint.updated_at
                else None,
            }
        return None
