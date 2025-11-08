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
from datetime import datetime, timezone
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


class StorageManager:
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
        batch_size: int = 25000,  # Increased from 10000 for better performance with large datasets
        flush_interval: int = 10,
        max_memory_mb: int = 1000,  # Increased from 500 for better performance with large datasets
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
            f"StorageManager initialized: batch_size={batch_size}, "
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
