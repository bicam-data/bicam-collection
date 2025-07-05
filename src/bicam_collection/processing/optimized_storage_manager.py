"""
Optimized Storage Manager for high-volume data processing.

This module provides efficient batched storage with:
- In-memory buffering with periodic flushes
- PostgreSQL COPY for bulk inserts
- SQLite for high-frequency checkpointing
- Asynchronous write queues
"""

import asyncio
import csv
import io
import json
import logging
import sqlite3
import time
import uuid
from collections import defaultdict
from datetime import UTC, datetime
from typing import Any

import asyncpg
from asyncpg.pool import Pool

logger = logging.getLogger(__name__)


class OptimizedStorageManager:
    """
    High-performance storage manager optimized for 19M+ records.

    Features:
    - In-memory buffering with configurable batch sizes
    - PostgreSQL COPY for bulk inserts (100x faster than INSERT)
    - Asynchronous write queue to prevent blocking
    - SQLite for lightweight checkpointing
    - Automatic table creation and schema management
    """

    def __init__(
        self,
        pg_pool: Pool,
        checkpoint_db_path: str = "checkpoints.db",
        batch_size: int = 10000,
        flush_interval: int = 10,
        max_memory_mb: int = 500
    ):
        self.pg_pool = pg_pool
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.max_memory_mb = max_memory_mb

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
            'total_written': 0,
            'total_batches': 0,
            'total_time': 0,
            'records_per_second': 0
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

    async def start(self):
        """Start background workers for async processing."""
        # Start write worker
        self.workers.append(
            asyncio.create_task(self._write_worker())
        )

        # Start periodic flush worker
        self.workers.append(
            asyncio.create_task(self._flush_worker())
        )

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
        logger.info("Storage manager stopped")

    async def add_records(
        self,
        schema: str,
        table: str,
        records: list[dict],
        ensure_table: bool = True
    ):
        """
        Add records to buffer for batched writing.

        This method is non-blocking and returns immediately.
        """
        if not records:
            return

        table_key = f"{schema}.{table}"

        # Add to buffer
        self.buffers[table_key].extend(records)
        self.buffer_sizes[table_key] += len(records)

        # Check if we should flush this table
        if self.buffer_sizes[table_key] >= self.batch_size:
            # Queue for flushing
            await self.write_queue.put((table_key, ensure_table))

    async def _write_worker(self):
        """Background worker that processes write queue."""
        while self.running:
            try:
                # Wait for work with timeout
                table_key, ensure_table = await asyncio.wait_for(
                    self.write_queue.get(),
                    timeout=1.0
                )

                # Flush this specific table
                await self._flush_table(table_key, ensure_table)

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
                    await self._flush_table(table_key, True)

            except Exception as e:
                logger.error(f"Flush worker error: {e}")

    async def _flush_table(self, table_key: str, ensure_table: bool = True):
        """Flush a specific table's buffer to PostgreSQL using COPY."""
        async with self.flush_lock:
            records = self.buffers[table_key]
            if not records:
                return

            # Clear buffer immediately to prevent double-flush
            self.buffers[table_key] = []
            self.buffer_sizes[table_key] = 0

        schema, table = table_key.split('.')

        try:
            start_time = time.time()

            # Use COPY for bulk insert
            await self._bulk_copy(schema, table, records, ensure_table)

            # Update statistics
            elapsed = time.time() - start_time
            self.stats['total_written'] += len(records)
            self.stats['total_batches'] += 1
            self.stats['total_time'] += elapsed

            records_per_second = len(records) / elapsed if elapsed > 0 else 0
            logger.info(
                f"Flushed {len(records)} records to {table_key} "
                f"in {elapsed:.2f}s ({records_per_second:.0f} rec/s)"
            )

        except Exception as e:
            logger.error(f"Failed to flush {table_key}: {e}")
            # Re-queue the records
            self.buffers[table_key].extend(records)
            self.buffer_sizes[table_key] += len(records)
            raise

    async def _bulk_copy(
        self,
        schema: str,
        table: str,
        records: list[dict],
        ensure_table: bool = True
    ):
        """Use PostgreSQL COPY for ultra-fast bulk inserts."""
        if not records:
            return

        async with self.pg_pool.acquire() as conn:
            # Ensure table exists if requested
            if ensure_table:
                await self._ensure_table_exists(conn, schema, table, records[0])

            # Get column order from first record
            columns = list(records[0].keys())

            # Create CSV data in memory
            output = io.StringIO()
            writer = csv.DictWriter(
                output,
                fieldnames=columns,
                delimiter='\t',
                quoting=csv.QUOTE_MINIMAL
            )

            # Write data
            for record in records:
                # Handle JSON fields
                processed_record = {}
                for key, value in record.items():
                    if isinstance(value, dict | list):
                        processed_record[key] = json.dumps(value)
                    elif value is None:
                        processed_record[key] = '\\N'
                    else:
                        processed_record[key] = str(value)

                writer.writerow(processed_record)

            # Reset to beginning
            output.seek(0)

            # Use COPY command
            await conn.copy_to_table(
                table_name=table,
                source=output,
                schema_name=schema,
                columns=columns,
                delimiter='\t',
                null='\\N'
            )

    async def _ensure_table_exists(
        self,
        conn: asyncpg.Connection,
        schema: str,
        table: str,
        sample_record: dict
    ):
        """Ensure table exists with proper columns."""
        # Check if table exists
        exists = await conn.fetchval(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = $1 AND table_name = $2
            )
            """,
            schema,
            table
        )

        if not exists:
            # Create table based on sample record
            columns = []
            for key, value in sample_record.items():
                if key == 'id_uuid':
                    columns.append(f"{key} TEXT PRIMARY KEY")
                elif key == 'payload' or isinstance(value, dict | list):
                    columns.append(f"{key} JSONB")
                elif key in ['scraped_at', 'created_at', 'updated_at']:
                    columns.append(f"{key} TIMESTAMPTZ")
                elif isinstance(value, bool):
                    columns.append(f"{key} BOOLEAN")
                elif isinstance(value, int):
                    columns.append(f"{key} INTEGER")
                else:
                    columns.append(f"{key} TEXT")

            columns_sql = ",\n    ".join(columns)

            await conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table} (
                    {columns_sql}
                )
            """)

            # Create indexes for common queries
            await conn.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{table}_batch_id
                ON {schema}.{table}(batch_id)
            """)

            await conn.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{table}_source_doc_id
                ON {schema}.{table}(source_doc_id)
            """)

            logger.info(f"Created table {schema}.{table}")

    async def flush_all(self):
        """Flush all buffers to database."""
        logger.info("Flushing all buffers...")

        tables_to_flush = list(self.buffers.keys())

        for table_key in tables_to_flush:
            if self.buffers[table_key]:
                await self._flush_table(table_key, True)

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
        records_processed: int
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
                (data_type, phase, offset, item_id, records_processed, time.time())
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
                (data_type, phase)
            )
            row = cursor.fetchone()
            if row:
                return {
                    'last_offset': row[0],
                    'last_item_id': row[1],
                    'records_processed': row[2],
                    'timestamp': row[3]
                }
            return None
        finally:
            conn.close()

    def get_stats(self) -> dict:
        """Get performance statistics."""
        avg_records_per_second = (
            self.stats['total_written'] / self.stats['total_time']
            if self.stats['total_time'] > 0 else 0
        )

        return {
            'total_written': self.stats['total_written'],
            'total_batches': self.stats['total_batches'],
            'avg_batch_size': (
                self.stats['total_written'] / self.stats['total_batches']
                if self.stats['total_batches'] > 0 else 0
            ),
            'avg_records_per_second': avg_records_per_second,
            'buffer_status': {
                table: len(records)
                for table, records in self.buffers.items()
            }
        }

    @property
    def total_buffered(self) -> int:
        """Get total number of records currently buffered."""
        return sum(len(records) for records in self.buffers.values())


class OptimizedFetcherStorage:
    """
    Storage adapter for Congressional/GovInfo fetchers with optimized batching.

    This replaces the individual insert operations in the fetchers with
    efficient batched operations.
    """

    def __init__(self, storage_manager: OptimizedStorageManager):
        self.storage = storage_manager
        self.phase_buffers = defaultdict(list)
        self.checkpoint_interval = 10000  # Checkpoint every 10k records
        self.last_checkpoint = 0

    async def store_phase_1_data(
        self,
        schema: str,
        table: str,
        data: dict[str, Any],
        batch_id: str | None = None
    ):
        """Store Phase 1 data with batching."""
        # Add metadata
        data['id_uuid'] = data.get('id_uuid', str(uuid.uuid4()))
        data['batch_id'] = batch_id or str(uuid.uuid4())
        data['scraped_at'] = datetime.now(UTC)

        # Buffer it
        self.phase_buffers['phase1'].append(data)

        # Add to storage manager (non-blocking)
        await self.storage.add_records(schema, table, [data])

    async def store_phase_2_data(
        self,
        schema: str,
        table: str,
        data: dict[str, Any],
        batch_id: str | None = None
    ):
        """Store Phase 2 data with batching."""
        # Add metadata
        data['id_uuid'] = data.get('id_uuid', str(uuid.uuid4()))
        data['batch_id'] = batch_id or str(uuid.uuid4())
        data['scraped_at'] = datetime.now(UTC)

        # Buffer it
        self.phase_buffers['phase2'].append(data)

        # Add to storage manager (non-blocking)
        await self.storage.add_records(schema, table, [data])

    async def store_phase_3_data(
        self,
        schema: str,
        table_prefix: str,
        data: list[dict[str, Any]],
        parent_id: str,
        batch_id: str | None = None
    ):
        """Store Phase 3 data with batching."""
        # Group by type
        by_type = defaultdict(list)

        for item in data:
            item_type = item.get('type', 'unknown')
            item_data = item.get('data', item)

            # Add metadata
            if isinstance(item_data, dict):
                item_data['parent_id'] = parent_id
                item_data['id_uuid'] = str(uuid.uuid4())
                item_data['batch_id'] = batch_id or str(uuid.uuid4())
                item_data['scraped_at'] = datetime.now(UTC)

            by_type[item_type].append(item_data)

        # Store each type
        for item_type, items in by_type.items():
            table = f"{table_prefix}_{item_type}_raw"
            await self.storage.add_records(schema, table, items)

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
        total_processed: int
    ):
        """Save checkpoint using SQLite."""
        self.storage.save_checkpoint(
            data_type,
            phase,
            offset,
            item_id,
            total_processed
        )
