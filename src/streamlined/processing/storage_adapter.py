"""
Unified Storage Adapter

Replaces OptimizedFetcherStorage, OptimizedNormalizerStorage, and OptimizedCleanerStorage
with a single unified interface that dispatches to stage-specific implementations.

Uses ProcessingStage enum (FETCHING, STAGING, CLEANING) for consistency with checkpoint system.
"""

import asyncio
import hashlib
import json
import logging
import uuid
from collections import defaultdict
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any, Literal

import asyncpg

from ..libs.hierarchical_checkpoint_system import (
    CleaningPhase,
    ProcessingStage,
)
from .storage_manager import StorageManager

logger = logging.getLogger(__name__)


class StorageAdapter:
    """
    Storage adapter that handles all processing phases.

    Provides a single interface that dispatches to stage-specific implementations.
    """

    def __init__(
        self,
        storage_manager: StorageManager,
        phase: ProcessingStage,
        **phase_config,
    ):
        """
        Initialize unified storage adapter.

        Args:
            storage_manager: The underlying optimized storage manager
            phase: Processing stage (FETCHING, STAGING, or CLEANING)
            **phase_config: Phase-specific configuration:
                - For FETCHING: fetcher, id_field
                - For STAGING: target_schema, data_type
                - For CLEANING: staging_schema, production_schema, data_type
        """
        self.storage = storage_manager
        self.phase = phase

        # Phase-specific initialization
        if phase == ProcessingStage.FETCHING:
            self.fetcher = phase_config.get("fetcher")
            self.id_field = phase_config.get("id_field", "parent_id")
            self.phase_buffers = defaultdict(list)
            self.checkpoint_interval = 10000
            self.last_checkpoint = 0
        elif phase == ProcessingStage.STAGING:
            self.target_schema = phase_config.get("target_schema", "bicam_staging")
            self.data_type = phase_config.get("data_type")
            self._table_schema_cache = {}
            self._cache_lock = asyncio.Lock()
            self.batch_accumulator = defaultdict(list)
            self.batch_sizes = defaultdict(int)
            self.max_batch_size = 25000
        elif phase == ProcessingStage.CLEANING:
            self.staging_schema = phase_config.get(
                "staging_schema", "bicam_staging_congressional"
            )
            self.production_schema = phase_config.get(
                "production_schema", "bicam_congressional"
            )
            self.data_type = phase_config.get("data_type")
            self._prod_schema_cache = {}
            self._cache_lock = asyncio.Lock()

        logger.info(f"StorageAdapter initialized for phase: {phase.value}")

    # =============================================================================
    # FETCHING PHASE METHODS
    # =============================================================================

    async def store_phase_1_data(
        self, schema: str, table: str, data: dict[str, Any], batch_id: str | None = None
    ):
        """Store Phase 1 data (fetching phase only)."""
        if self.phase != ProcessingStage.FETCHING:
            raise ValueError("store_phase_1_data only available for FETCHING phase")

        record = {
            "id_uuid": data.get("id_uuid", str(uuid.uuid4())),
            "url": data.get("url"),
            "batch_id": batch_id or str(uuid.uuid4()),
            "scraped_at": datetime.now(UTC),
            "etl_batch_id": batch_id or str(uuid.uuid4()),
        }

        if self.fetcher and hasattr(self.fetcher, "extract_preliminary_item_id"):
            record["source_doc_id"] = await self.fetcher.extract_preliminary_item_id(
                data
            )
        elif "url" in data and data["url"]:
            record["source_doc_id"] = data["url"].split("/")[-1]
        else:
            record["source_doc_id"] = str(uuid.uuid4())

        metadata_fields = {
            "id_uuid",
            "url",
            "batch_id",
            "scraped_at",
            "source_doc_id",
            "etl_batch_id",
        }
        payload_data = {k: v for k, v in data.items() if k not in metadata_fields}
        record["payload"] = payload_data

        self.phase_buffers["phase1"].append(record)
        data_type = (
            getattr(self.fetcher, "data_type_name", "unknown")
            if self.fetcher
            else "unknown"
        )

        await self.storage.add_records(
            schema, table, [record], data_type=data_type, checkpoint_table=True
        )

    async def store_phase_2_data(
        self, schema: str, table: str, data: dict[str, Any], batch_id: str | None = None
    ):
        """Store Phase 2 data (fetching phase only)."""
        if self.phase != ProcessingStage.FETCHING:
            raise ValueError("store_phase_2_data only available for FETCHING phase")

        if isinstance(data, list):
            data = data[0]

        record = {
            "id_uuid": data.get("id_uuid", str(uuid.uuid4())),
            "url": data.get("url"),
            "batch_id": batch_id or str(uuid.uuid4()),
            "scraped_at": datetime.now(UTC),
            "etl_batch_id": batch_id or str(uuid.uuid4()),
        }

        if self.fetcher and hasattr(self.fetcher, "extract_item_id"):
            try:
                result = self.fetcher.extract_item_id(data)
                if hasattr(result, "__await__"):
                    record["source_doc_id"] = await result
                else:
                    record["source_doc_id"] = result
            except Exception as e:
                logger.warning(f"Error extracting item ID: {e}")
                record["source_doc_id"] = str(uuid.uuid4())
        elif "url" in data and data["url"]:
            record["source_doc_id"] = data["url"].split("/")[-1]
        else:
            record["source_doc_id"] = str(uuid.uuid4())

        metadata_fields = {
            "id_uuid",
            "url",
            "batch_id",
            "scraped_at",
            "source_doc_id",
            "etl_batch_id",
        }
        payload_data = {k: v for k, v in data.items() if k not in metadata_fields}
        record["payload"] = payload_data

        self.phase_buffers["phase2"].append(record)
        data_type = (
            getattr(self.fetcher, "data_type_name", "unknown")
            if self.fetcher
            else "unknown"
        )

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
        """Store Phase 3 data (fetching phase only)."""
        if self.phase != ProcessingStage.FETCHING:
            raise ValueError("store_phase_3_data only available for FETCHING phase")

        data_type = (
            getattr(self.fetcher, "data_type_name", "unknown")
            if self.fetcher
            else "unknown"
        )
        by_type = defaultdict(list)

        for item in data:
            item_type = item.get("type", "unknown")
            item_data = item.get("data", item)

            if isinstance(item_data, list):
                for record_data in item_data:
                    if isinstance(record_data, dict):
                        record = {
                            "id_uuid": str(uuid.uuid4()),
                            "url": record_data.get("url"),
                            "batch_id": batch_id or str(uuid.uuid4()),
                            "scraped_at": datetime.now(UTC),
                            "etl_batch_id": batch_id or str(uuid.uuid4()),
                            "source_doc_id": parent_id,
                        }

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
                        payload_data[self.id_field] = parent_id
                        record["payload"] = payload_data
                        by_type[item_type].append(record)

        for item_type, items in by_type.items():
            table_suffix = item_type
            if item_type.startswith("get_"):
                table_suffix = item_type[4:]
            if table_suffix.startswith(f"{table_prefix}_"):
                table_suffix = table_suffix[len(f"{table_prefix}_") :]

            if item_type.endswith("_granules"):
                table = f"{item_type}_list_raw"
            else:
                table = f"{table_prefix}_{table_suffix}_raw"

            await self.storage.add_records(
                schema, table, items, data_type=data_type, checkpoint_table=True
            )

            if data_type != "unknown":
                staging_checkpoint = self.storage.get_staging_checkpoint(data_type)
                staging_checkpoint.mark_list_extracted(table_prefix, table_suffix)

    async def store_phase_4_data(
        self, data: dict[str, Any], parent_id: str, batch_id: str | None = None
    ):
        """Store Phase 4 data (fetching phase only)."""
        if self.phase != ProcessingStage.FETCHING:
            raise ValueError("store_phase_4_data only available for FETCHING phase")

        record = {
            "id_uuid": data.get("id_uuid", str(uuid.uuid4())),
            "url": data.get("granuleLink"),
            "batch_id": batch_id or str(uuid.uuid4()),
            "scraped_at": datetime.now(UTC),
            "etl_batch_id": batch_id or str(uuid.uuid4()),
        }

        if self.fetcher and hasattr(self.fetcher, "extract_item_id"):
            try:
                result = self.fetcher.extract_item_id(data, granule=True)
                if hasattr(result, "__await__"):
                    record["source_doc_id"] = await result
                else:
                    record["source_doc_id"] = result
            except Exception as e:
                logger.warning(f"Error extracting item ID: {e}")
                record["source_doc_id"] = str(uuid.uuid4())
        elif "granuleLink" in data and data["granuleLink"]:
            record["source_doc_id"] = data["granuleLink"].split("/")[-1]
        else:
            record["source_doc_id"] = str(uuid.uuid4())

        metadata_fields = {
            "id_uuid",
            "url",
            "batch_id",
            "scraped_at",
            "source_doc_id",
            "etl_batch_id",
        }
        payload_data = {k: v for k, v in data.items() if k not in metadata_fields}
        record["payload"] = payload_data

        self.phase_buffers["phase4"].append(record)
        data_type_name = (
            getattr(self.fetcher, "data_type_name", "unknown")
            if self.fetcher
            else "unknown"
        )
        granule_table_name = f"{data_type_name}_granules"

        await self.storage.add_records(
            "bicam_raw_govinfo",
            f"{granule_table_name}_raw",
            [record],
            data_type=granule_table_name,
            checkpoint_table=True,
        )

    # =============================================================================
    # STAGING PHASE METHODS
    # =============================================================================

    async def store_normalized_records(
        self,
        table_name: str,
        records: list[dict[str, Any]],
        record_type: Literal["main", "related", "nested"] = "main",
        ensure_table: bool = True,
        checkpoint: bool = True,
    ) -> dict[str, int]:
        """Store normalized records (staging phase only)."""
        if self.phase != ProcessingStage.STAGING:
            raise ValueError(
                "store_normalized_records only available for STAGING phase"
            )

        if not records:
            return {"stored": 0, "errors": 0}

        normalized_records = []
        for record in records:
            normalized_record = {k.lower(): v for k, v in record.items()}
            normalized_records.append(normalized_record)

        key = f"{self.target_schema}.{table_name.lower()}"
        self.batch_accumulator[key].extend(normalized_records)
        self.batch_sizes[key] += len(normalized_records)

        if self.batch_sizes[key] >= self.max_batch_size:
            return await self._flush_normalizer_table(
                table_name, ensure_table, checkpoint
            )

        return {"stored": len(records), "errors": 0}

    async def _flush_normalizer_table(
        self, table_name: str, ensure_table: bool = True, checkpoint: bool = True
    ) -> dict[str, int]:
        """Flush staging buffer to database."""
        key = f"{self.target_schema}.{table_name.lower()}"
        records = self.batch_accumulator.get(key, [])

        if not records:
            return {"stored": 0, "errors": 0}

        self.batch_accumulator[key] = []
        self.batch_sizes[key] = 0

        stats = {"stored": 0, "errors": 0}

        try:
            async with self.storage.pg_pool.acquire() as conn:
                if ensure_table:
                    combined_sample = {}
                    for record in records:
                        combined_sample.update(record)
                    await self._ensure_normalizer_table(
                        conn, table_name, combined_sample
                    )

                stored_count = await self._bulk_copy_normalizer(
                    conn, table_name, records
                )
                stats["stored"] = stored_count

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
            self.batch_accumulator[key].extend(records)
            self.batch_sizes[key] += len(records)

        return stats

    async def _ensure_normalizer_table(
        self, conn: asyncpg.Connection, table_name: str, sample_record: dict[str, Any]
    ):
        """Ensure staging table exists with dynamic schema."""
        await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self.target_schema}")

        cache_key = f"{self.target_schema}.{table_name.lower()}"
        async with self._cache_lock:
            if cache_key in self._table_schema_cache:
                return

        lock_id = int(hashlib.md5(cache_key.encode()).hexdigest()[:8], 16)

        try:
            lock_acquired = await conn.fetchval(
                "SELECT pg_try_advisory_lock($1)", lock_id
            )
            if not lock_acquired:
                await conn.fetchval("SELECT pg_advisory_lock($1)", lock_id)

            table_exists = await conn.fetchval(
                """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = $1 AND table_name = $2
                )
                """,
                self.target_schema,
                table_name.lower(),
            )

            if not table_exists:
                safe_table_name = table_name.replace("-", "_")
                columns = []
                for key in sample_record:
                    safe_column_name = self._safe_column_name(key)
                    columns.append(f"{safe_column_name} TEXT")

                create_sql = f"""
                    CREATE TABLE IF NOT EXISTS {self.target_schema}.{safe_table_name} (
                        {", ".join(columns)}
                    )
                """
                await conn.execute(create_sql)
                logger.info(f"Created table {self.target_schema}.{table_name}")

                async with self._cache_lock:
                    self._table_schema_cache[cache_key] = True
            else:
                await self._add_missing_normalizer_columns(
                    conn, table_name, sample_record
                )

        finally:
            await conn.fetchval("SELECT pg_advisory_unlock($1)", lock_id)

    def _safe_column_name(self, column_name: str) -> str:
        """Convert column name to safe PostgreSQL identifier."""
        safe_name = column_name.replace("-", "_")
        if safe_name.lower() == "references":
            safe_name = "_references"
        return safe_name

    async def _add_missing_normalizer_columns(
        self, conn: asyncpg.Connection, table_name: str, sample_record: dict[str, Any]
    ):
        """Add missing columns to staging table."""
        safe_table_name = table_name.replace("-", "_")
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

        missing_columns = []
        for key in sample_record:
            safe_key = self._safe_column_name(key)
            if safe_key.lower() not in existing_columns:
                missing_columns.append(safe_key)

        if missing_columns:
            for col in missing_columns:
                try:
                    await conn.execute(
                        f"""
                        ALTER TABLE {self.target_schema}.{safe_table_name}
                        ADD COLUMN {col} TEXT
                        """
                    )
                except Exception as e:
                    if "already exists" not in str(e).lower():
                        logger.error(f"Failed to add column {col}: {e}")

    async def _bulk_copy_normalizer(
        self, conn: asyncpg.Connection, table_name: str, records: list[dict[str, Any]]
    ) -> int:
        """Use PostgreSQL COPY for staging bulk inserts."""
        if not records:
            return 0

        all_columns = set().union(*(record.keys() for record in records))
        safe_columns = [self._safe_column_name(col) for col in all_columns]
        columns = sorted(safe_columns)

        column_mapping = {}
        for original_col in all_columns:
            safe_col = self._safe_column_name(original_col)
            column_mapping[safe_col] = original_col

        records_data = []
        for record in records:
            row_values = []
            for col in columns:
                original_col = column_mapping.get(col, col)
                value = record.get(original_col)
                if value is None:
                    row_values.append(None)
                elif isinstance(value, bool):
                    row_values.append(str(value).lower())
                elif isinstance(value, dict | list):
                    json_str = json.dumps(value)
                    json_str = (
                        json_str.replace("\\", "\\\\")
                        .replace("\t", "\\t")
                        .replace("\n", "\\n")
                        .replace("\r", "\\r")
                    )
                    row_values.append(json_str)
                else:
                    str_value = str(value)
                    str_value = (
                        str_value.replace("\\", "\\\\")
                        .replace("\t", "\\t")
                        .replace("\n", "\\n")
                        .replace("\r", "\\r")
                    )
                    row_values.append(str_value)
            records_data.append(tuple(row_values))

        if records_data:
            safe_table_name = table_name.replace("-", "_")
            await conn.copy_records_to_table(
                safe_table_name,
                records=records_data,
                columns=columns,
                schema_name=self.target_schema,
                timeout=300.0,
            )

        return len(records)

    async def flush_all(self) -> dict[str, int]:
        """Flush all buffered records (staging phase only)."""
        if self.phase != ProcessingStage.STAGING:
            raise ValueError("flush_all only available for STAGING phase")

        total_stats = {"stored": 0, "errors": 0}
        tables_to_flush = list(self.batch_accumulator.keys())

        for table_key in tables_to_flush:
            if self.batch_accumulator[table_key]:
                table_name = table_key.split(".")[-1]
                stats = await self._flush_normalizer_table(
                    table_name, ensure_table=True, checkpoint=True
                )
                total_stats["stored"] += stats["stored"]
                total_stats["errors"] += stats["errors"]

        return total_stats

    # =============================================================================
    # CLEANING PHASE METHODS
    # =============================================================================

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
        """Stream data from staging table (cleaning phase only)."""
        if self.phase != ProcessingStage.CLEANING:
            raise ValueError("stream_staging_data only available for CLEANING phase")

        table_specific_cleaner_exists = bool(
            custom_logic and hasattr(custom_logic, f"_clean_{table_name}_singular")
        )
        table_specific_stream_exists = bool(
            custom_logic
            and hasattr(custom_logic, f"_stream_{table_name}_joined_chunks")
        )
        mtd_normalized = multi_table_data_types
        if isinstance(mtd_normalized, list):
            mtd_normalized = {table_name: mtd_normalized}
        is_multi_table_root = bool(mtd_normalized and table_name in mtd_normalized)

        if not (
            table_specific_cleaner_exists
            or table_specific_stream_exists
            or is_multi_table_root
        ):
            logger.info(f"Skipping table {table_name}: no cleaner or stream defined")
            return

        if custom_logic and hasattr(
            custom_logic, f"_stream_{table_name}_joined_chunks"
        ):
            custom_stream_method = getattr(
                custom_logic, f"_stream_{table_name}_joined_chunks"
            )
            async for chunk in custom_stream_method(batch_size):
                yield chunk
            return

        if mtd_normalized and table_name in mtd_normalized:
            async for chunk in self._stream_multi_table_data(
                table_name, batch_size, checkpoint_offset, mtd_normalized[table_name]
            ):
                yield chunk
            return

        async for chunk in self._stream_single_table_data(
            table_name, batch_size, order_by, where_clause, checkpoint_offset
        ):
            yield chunk

    async def _stream_multi_table_data(
        self,
        table_name: str,
        batch_size: int,
        checkpoint_offset: int,
        related_tables_config: list[str] | dict[str, list[str]],
    ) -> AsyncIterator[list[dict[str, Any]]]:
        """Stream multi-table data."""
        async with self.storage.pg_pool.acquire() as conn:
            if isinstance(related_tables_config, dict):
                related_tables = related_tables_config.get(table_name, [])
            else:
                related_tables = related_tables_config or []

            if not related_tables:
                return

            if len(related_tables) == 1:
                main_table = related_tables[0]
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
                    return

                offset = checkpoint_offset
                while True:
                    query = f"""
                        SELECT * FROM {self.staging_schema}.{main_table}
                        LIMIT {batch_size} OFFSET {offset}
                    """
                    rows = await conn.fetch(query)
                    if not rows:
                        break
                    batch = [dict(row) for row in rows]
                    yield batch
                    offset += len(batch)
                    if len(batch) < batch_size:
                        break

    async def _stream_single_table_data(
        self,
        table_name: str,
        batch_size: int,
        order_by: str = None,
        where_clause: str = None,
        checkpoint_offset: int = 0,
    ) -> AsyncIterator[list[dict[str, Any]]]:
        """Stream single table data."""
        async with self.storage.pg_pool.acquire() as conn:
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
                return

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
                for preferred in ["processed_at", "id", "created_at", "scraped_at"]:
                    if preferred in column_names:
                        order_by = preferred
                        break
                if not order_by and column_names:
                    order_by = column_names[0]

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
                rows = await conn.fetch(query, batch_size, offset)
                if not rows:
                    break

                batch = [dict(row) for row in rows]
                yield batch

                records_yielded += len(batch)
                offset += len(batch)

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
                        "source_doc_id", batch[-1].get("id", "")
                    )
                    checkpoint.processed_items = records_yielded
                    checkpoint.current_table = table_name
                    self.storage.checkpoint_manager.save_checkpoint(checkpoint)

                if len(batch) < batch_size:
                    break

    async def bulk_insert_production(
        self,
        table_name: str,
        records: list[dict[str, Any]],
        upsert: bool = False,
        primary_keys: list[str] = None,
    ) -> int:
        """Bulk insert to production table (cleaning phase only)."""
        if self.phase != ProcessingStage.CLEANING:
            raise ValueError("bulk_insert_production only available for CLEANING phase")

        if not records:
            return 0

        async with self.storage.pg_pool.acquire() as conn:
            await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self.production_schema}")

            all_columns = set().union(*(record.keys() for record in records))
            columns = sorted(all_columns)
            records_data = [
                tuple(record.get(col) for col in columns) for record in records
            ]

            temp_table = f"temp_{table_name}_{id(records)}"

            try:
                await conn.execute(f"""
                    CREATE TEMP TABLE {temp_table}
                    AS SELECT * FROM {self.production_schema}.{table_name}
                    WHERE FALSE
                """)

                await conn.copy_records_to_table(
                    temp_table, records=records_data, columns=columns, timeout=300.0
                )

                columns_str = ", ".join(columns)
                insert_sql = f"""
                    INSERT INTO {self.production_schema}.{table_name} ({columns_str})
                    SELECT {columns_str} FROM {temp_table}
                    ON CONFLICT DO NOTHING
                """

                result = await conn.execute(insert_sql)
                if result:
                    parts = result.split()
                    if len(parts) >= 2:
                        return int(parts[1])
                return len(records)

            finally:
                await conn.execute(f"DROP TABLE IF EXISTS {temp_table}")

    def get_buffer_status(self) -> dict[str, int]:
        """Get current buffer status (staging phase only)."""
        if self.phase != ProcessingStage.STAGING:
            raise ValueError("get_buffer_status only available for STAGING phase")
        return {
            table: len(records)
            for table, records in self.batch_accumulator.items()
            if records
        }

    async def store_multiple_tables(
        self, table_records: dict[str, list[dict[str, Any]]], checkpoint: bool = True
    ) -> dict[str, Any]:
        """Store records for multiple tables (staging phase only)."""
        if self.phase != ProcessingStage.STAGING:
            raise ValueError("store_multiple_tables only available for STAGING phase")
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

    def get_cleaning_checkpoint(self, table_name: str) -> dict[str, Any] | None:
        """Get cleaning checkpoint (cleaning phase only)."""
        if self.phase != ProcessingStage.CLEANING:
            raise ValueError(
                "get_cleaning_checkpoint only available for CLEANING phase"
            )
        if self.data_type:
            checkpoint = self.storage.checkpoint_manager.get_or_create_checkpoint(
                ProcessingStage.CLEANING,
                CleaningPhase.APPLY_RULES.value,
                self.data_type,
            )
            return {
                "last_offset": checkpoint.current_offset,
                "last_item_id": checkpoint.current_item_id,
                "records_processed": checkpoint.processed_items,
                "timestamp": checkpoint.updated_at.timestamp()
                if checkpoint.updated_at
                else None,
            }
        return None
