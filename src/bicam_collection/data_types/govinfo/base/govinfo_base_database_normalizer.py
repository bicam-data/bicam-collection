"""
GovInfo-specific base database normalizer.

This module provides GovInfo-specific implementations including:
- GovInfo schema defaults (bicam_staging_govinfo, bicam_raw_govinfo)
- GovInfo progress tracking configuration
- Package/granule relationship handling
- GovInfo-specific normalization patterns
"""

import asyncio
import json
import logging
import uuid
from datetime import UTC, datetime
from typing import Any

import asyncpg

from ....libs.checkpoint import HierarchicalProgressTracker, ProcessingStage
from ....libs.run_tracking import RunMetadata, RunType
from ...abstract import AbstractDatabaseNormalizer

logger = logging.getLogger(__name__)


class GovInfoBaseDatabaseNormalizer(AbstractDatabaseNormalizer):
    """
    GovInfo-specific base normalizer.

    Provides GovInfo schema defaults and handles package/granule relationships.
    """

    def __init__(self, **kwargs):
        # Provide GovInfo-specific schema defaults
        kwargs.setdefault("target_schema", "bicam_staging_govinfo")
        kwargs.setdefault("source_schema", "bicam_raw_govinfo")
        super().__init__(**kwargs)
        self._main_config = None

    def setup_progress_tracker(
        self, checkpoint_manager=None
    ) -> HierarchicalProgressTracker | None:
        """GovInfo-specific progress tracker setup."""
        if checkpoint_manager:
            self.checkpoint_manager = checkpoint_manager

        if not self.checkpoint_manager or not self.data_type_name:
            logger.warning("Cannot setup progress tracker - missing dependencies")
            return None

        self.progress_tracker = HierarchicalProgressTracker.create_for_stage(
            self.checkpoint_manager,
            "govinfo",  # GovInfo-specific system name
            self.data_type_name,
            ProcessingStage.NORMALIZATION,
        )
        return self.progress_tracker

    async def setup_run_tracking(
        self, run_metadata: RunMetadata | None = None
    ) -> str | None:
        """Setup run tracking for this normalization session."""
        if not self.run_manager:
            return None

        if not run_metadata:
            run_metadata = RunMetadata(
                run_id="",
                run_type=RunType.DATA_NORMALIZATION,
                system_name=f"{self.data_type_name}_database_normalizer",
                description=f"GovInfo {self.data_type_name} data normalization to staging",
                data_types=[self.data_type_name] if self.data_type_name else [],
            )

        self.current_run_id = self.run_manager.create_run(run_metadata)
        await self.run_manager.start_run(self.current_run_id)
        return self.current_run_id

    def _get_main_id_field_name(self) -> str:
        """Get the main ID field name for GovInfo data types."""
        # GovInfo typically uses package-based IDs
        return f"{self.data_type_name}_package_id"

    def _get_related_tables_with_raw_data(self) -> list[str]:
        """Get list of related table suffixes that have raw data tables for GovInfo."""
        # GovInfo standard related tables
        return ["package", "granules", "granule_data", "collection"]

    # =============================================================================
    # MAIN PROCESSING METHODS
    # =============================================================================

    async def process_items(
        self,
        item_ids: list[str],
        batch_id: str | None = None,
        rerun: bool = False,
        batch_size: int = 50,
        max_concurrent: int = 10,
    ) -> dict[str, Any]:
        """
        Main processing method that normalizes GovInfo raw data to staging tables.

        Args:
            item_ids: List of package IDs to process
            batch_id: Batch identifier for tracking
            rerun: Whether to reprocess existing records
            batch_size: Size of processing batches
            max_concurrent: Maximum concurrent operations

        Returns:
            Processing statistics
        """
        logger.info(
            f"Starting GovInfo normalization for {len(item_ids)} {self.data_type_name} packages"
        )

        # Setup progress tracking
        self.setup_progress_tracker()

        # Setup run tracking
        await self.setup_run_tracking()

        stats = {
            "total_packages": len(item_ids),
            "package_records_processed": 0,
            "granule_records_processed": 0,
            "collection_records_processed": 0,
            "errors": 0,
            "start_time": datetime.now(UTC),
        }

        try:
            # Process package records
            package_stats = await self._process_package_records(
                item_ids, batch_size, max_concurrent, rerun
            )
            stats.update(package_stats)

            # Process granule records
            granule_stats = await self._process_granule_records(
                item_ids, batch_size, max_concurrent, rerun
            )
            stats["granule_records_processed"] = granule_stats.get(
                "granule_records_processed", 0
            )
            stats["errors"] += granule_stats.get("errors", 0)

            # Process collection records
            collection_stats = await self._process_collection_records(
                item_ids, batch_size, max_concurrent, rerun
            )
            stats["collection_records_processed"] = collection_stats.get(
                "collection_records_processed", 0
            )
            stats["errors"] += collection_stats.get("errors", 0)

        except Exception as e:
            logger.error(f"GovInfo normalization failed: {e}")
            if self.run_manager and self.current_run_id:
                await self.run_manager.fail_run(self.current_run_id, str(e))
            raise

        finally:
            if self.run_manager and self.current_run_id:
                await self.run_manager.complete_run(self.current_run_id)

        stats["end_time"] = datetime.now(UTC)
        stats["duration"] = (stats["end_time"] - stats["start_time"]).total_seconds()

        logger.info(f"GovInfo normalization completed: {stats}")
        return stats

    async def _process_package_records(
        self, package_ids: list[str], batch_size: int, max_concurrent: int, rerun: bool
    ) -> dict[str, Any]:
        """Process main package records."""
        raw_data = await self._get_raw_package_data_by_ids(package_ids)
        return await self._process_data_batch(
            raw_data, "package", batch_size, max_concurrent, rerun
        )

    async def _process_granule_records(
        self, package_ids: list[str], batch_size: int, max_concurrent: int, rerun: bool
    ) -> dict[str, Any]:
        """Process granule records for all packages."""
        total_stats = {"granule_records_processed": 0, "errors": 0}

        # Process granules list
        try:
            logger.info("Processing granules list records")
            raw_data = await self._get_raw_granules_data_by_package_ids(package_ids)

            if raw_data:
                stats = await self._process_data_batch(
                    raw_data, "granules", batch_size, max_concurrent, rerun
                )
                total_stats["granule_records_processed"] += stats.get(
                    "records_processed", 0
                )
                total_stats["errors"] += stats.get("errors", 0)

        except Exception as e:
            logger.error(f"Error processing granules: {e}")
            total_stats["errors"] += 1

        # Process granule data
        try:
            logger.info("Processing granule data records")
            raw_data = await self._get_raw_granule_data_by_package_ids(package_ids)

            if raw_data:
                stats = await self._process_data_batch(
                    raw_data, "granule_data", batch_size, max_concurrent, rerun
                )
                total_stats["granule_records_processed"] += stats.get(
                    "records_processed", 0
                )
                total_stats["errors"] += stats.get("errors", 0)

        except Exception as e:
            logger.error(f"Error processing granule data: {e}")
            total_stats["errors"] += 1

        return total_stats

    async def _process_collection_records(
        self, package_ids: list[str], batch_size: int, max_concurrent: int, rerun: bool
    ) -> dict[str, Any]:
        """Process collection records for all packages."""
        total_stats = {"collection_records_processed": 0, "errors": 0}

        try:
            logger.info("Processing collection records")
            raw_data = await self._get_raw_collection_data_by_package_ids(package_ids)

            if raw_data:
                stats = await self._process_data_batch(
                    raw_data, "collection", batch_size, max_concurrent, rerun
                )
                total_stats["collection_records_processed"] += stats.get(
                    "records_processed", 0
                )
                total_stats["errors"] += stats.get("errors", 0)

        except Exception as e:
            logger.error(f"Error processing collection: {e}")
            total_stats["errors"] += 1

        return total_stats

    async def _process_data_batch(
        self,
        raw_data: list[dict[str, Any]],
        record_type: str,
        batch_size: int,
        max_concurrent: int,
        rerun: bool,
    ) -> dict[str, Any]:
        """Process a batch of raw data records."""
        if not raw_data:
            return {"records_processed": 0, "errors": 0}

        stats = {"records_processed": 0, "errors": 0}

        # Process in batches
        for i in range(0, len(raw_data), batch_size):
            batch = raw_data[i : i + batch_size]
            batch_stats = await self._process_concurrent_batch(
                batch, record_type, max_concurrent, rerun
            )
            stats["records_processed"] += batch_stats.get("records_processed", 0)
            stats["errors"] += batch_stats.get("errors", 0)

        return stats

    async def _process_concurrent_batch(
        self,
        batch: list[dict[str, Any]],
        record_type: str,
        max_concurrent: int,
        rerun: bool,
    ) -> dict[str, Any]:
        """Process batch with concurrency control."""
        semaphore = asyncio.Semaphore(max_concurrent)

        async def process_record_concurrent(record):
            async with semaphore:
                return await self._process_single_record(record, record_type, rerun)

        tasks = [process_record_concurrent(record) for record in batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        stats = {"records_processed": 0, "errors": 0}
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"GovInfo record processing failed: {result}")
                stats["errors"] += 1
            elif isinstance(result, dict) and result.get("success"):
                stats["records_processed"] += 1
            else:
                stats["errors"] += 1

        return stats

    async def _process_single_record(
        self, raw_record: dict[str, Any], record_type: str, rerun: bool
    ) -> dict[str, Any]:
        """Process a single raw record."""
        try:
            record_id = self.extract_item_id(raw_record)
            payload = json.loads(raw_record.get("payload", "{}"))

            if record_type == "package":
                await self._process_package_record(payload, record_id)
            elif record_type in ["granules", "granule_data", "collection"]:
                await self._process_related_record(payload, record_id, record_type)

            return {"success": True, "record_id": record_id}

        except Exception as e:
            logger.error(f"Error processing GovInfo record: {e}")
            return {"success": False, "error": str(e)}

    async def _process_package_record(
        self, payload: dict[str, Any], record_id: str
    ) -> dict[str, Any]:
        """Process a main package record."""
        # Flatten the payload and extract lists
        flat_data, extracted_lists = self._process_package_record_data(payload)

        # Add record ID
        flat_data[self._get_main_id_field_name()] = record_id

        # Prepare all table records
        all_table_records = {self.get_main_table_name(): [flat_data]}
        all_table_records.update(extracted_lists)

        # Store all records
        return await self._store_all_table_records(all_table_records)

    async def _process_related_record(
        self, payload: dict[str, Any], record_id: str, table_suffix: str
    ) -> dict[str, Any]:
        """Process a related record (granules, granule_data, collection)."""
        # Extract list items with proper parent relationships
        items = payload if isinstance(payload, list) else [payload]

        processed_items = []
        for item_idx, item_data in enumerate(items):
            # Process item with extraction
            flat_item = self._flatten_dict(item_data)

            # Generate unique ID for this item
            item_id = f"{record_id}_{table_suffix}_{item_idx}"
            flat_item["id"] = item_id

            # Add parent reference
            flat_item[self.get_foreign_key_column()] = record_id

            # Extract nested lists
            extracted_lists = self._extract_lists(
                item_data,
                item_id,
                is_nested=True,
                parent_table=table_suffix,
                root_id=record_id,
            )

            processed_items.append(flat_item)

            # Store any nested lists from this item
            if extracted_lists:
                await self._store_all_table_records(extracted_lists)

        # Store the main items for this table
        table_name = f"{self.data_type_name}_{table_suffix}"
        all_table_records = {table_name: processed_items}

        return await self._store_all_table_records(all_table_records)

    # =============================================================================
    # DATA PROCESSING UTILITIES
    # =============================================================================

    def _flatten_dict(self, data: dict[str, Any], prefix: str = "") -> dict[str, Any]:
        """
        Flatten nested dictionary structure.

        Args:
            data: Dictionary to flatten
            prefix: Prefix for keys

        Returns:
            Flattened dictionary
        """
        flattened = {}

        for key, value in data.items():
            new_key = f"{prefix}_{key}" if prefix else key

            if isinstance(value, dict):
                # Recursively flatten nested dictionaries
                flattened.update(self._flatten_dict(value, new_key))
            elif isinstance(value, list):
                # Store lists as JSON for now, they'll be extracted separately
                flattened[new_key] = json.dumps(value) if value else None
            else:
                # Store primitive values directly
                flattened[new_key] = value

        return flattened

    def _extract_lists(
        self,
        data: dict[str, Any],
        parent_id: str,
        is_nested: bool = False,
        parent_table: str | None = None,
        root_id: str | None = None,
    ) -> dict[str, list[dict[str, Any]]]:
        """
        Extract list fields to separate table records.

        Args:
            data: Source data containing lists
            parent_id: ID of parent record
            is_nested: Whether this is a nested extraction
            parent_table: Name of parent table for nested records
            root_id: Root record ID for deeply nested structures

        Returns:
            Dictionary mapping table names to record lists
        """
        extracted = {}

        def _get_parent_foreign_key_column(parent_table_name: str) -> str:
            """Get appropriate foreign key column for parent table."""
            if parent_table_name == self.get_main_table_name():
                return self.get_foreign_key_column()
            else:
                # For nested tables, use the parent table's ID column
                return f"{parent_table_name}_id"

        for key, value in data.items():
            if not isinstance(value, list) or not value:
                continue

            # Determine table name
            if is_nested and parent_table:
                table_name = f"{self.data_type_name}_{parent_table}_{key}"
            else:
                table_name = f"{self.data_type_name}_{key}"

            # Process list items
            records = []
            for item_idx, item in enumerate(value):
                if isinstance(item, dict):
                    # Flatten the item
                    flat_item = self._flatten_dict(item)

                    # Generate unique ID
                    item_id = f"{parent_id}_{key}_{item_idx}"
                    flat_item["id"] = item_id

                    # Add parent reference
                    if is_nested and parent_table:
                        flat_item[_get_parent_foreign_key_column(parent_table)] = (
                            parent_id
                        )
                        if root_id:
                            flat_item[self.get_foreign_key_column()] = root_id
                    else:
                        flat_item[self.get_foreign_key_column()] = parent_id

                    records.append(flat_item)

                    # Recursively extract nested lists
                    nested_extracted = self._extract_lists(
                        item,
                        item_id,
                        is_nested=True,
                        parent_table=key,
                        root_id=root_id or parent_id,
                    )
                    for nested_table, nested_records in nested_extracted.items():
                        if nested_table not in extracted:
                            extracted[nested_table] = []
                        extracted[nested_table].extend(nested_records)

                else:
                    # Handle primitive list items
                    item_id = f"{parent_id}_{key}_{item_idx}"
                    record = {
                        "id": item_id,
                        "value": str(item),
                        self.get_foreign_key_column(): root_id or parent_id,
                    }
                    if is_nested and parent_table:
                        record[_get_parent_foreign_key_column(parent_table)] = parent_id

                    records.append(record)

            if records:
                extracted[table_name] = records

        return extracted

    def _process_package_record_data(
        self, raw_row: dict[str, Any]
    ) -> tuple[dict[str, Any], dict[str, list[dict[str, Any]]]]:
        """
        Process a single package record, extracting main data and lists.

        Args:
            raw_row: Raw package record data

        Returns:
            Tuple of (flattened_main_data, extracted_lists)
        """
        # Create a copy to avoid modifying original
        data = raw_row.copy()

        # Generate a temporary ID for list extraction
        temp_id = data.get(self._get_main_id_field_name(), str(uuid.uuid4()))

        # Extract lists first (before flattening removes them)
        extracted_lists = self._extract_lists(data, temp_id)

        # Then flatten the remaining data
        flat_data = self._flatten_dict(data)

        return flat_data, extracted_lists

    # =============================================================================
    # STORAGE METHODS
    # =============================================================================

    async def _store_all_table_records(
        self, all_table_records: dict[str, list[dict[str, Any]]]
    ) -> dict[str, Any]:
        """Store records for all tables."""
        stats = {"tables_updated": 0, "records_stored": 0}

        for table_name, records in all_table_records.items():
            if not records:
                continue

            try:
                # Ensure table exists with proper schema
                sample_record = records[0]
                await self._ensure_table_with_schema(table_name, sample_record)

                # Store records
                stored_count = await self._batch_store_records(
                    table_name, records, conflict_strategy="UPDATE"
                )

                stats["tables_updated"] += 1
                stats["records_stored"] += stored_count

                logger.debug(f"Stored {stored_count} records in {table_name}")

            except Exception as e:
                logger.error(f"Error storing records for {table_name}: {e}")
                raise

        return stats

    async def _ensure_table_with_schema(
        self,
        table_name: str,
        sample_record: dict[str, Any],
        primary_key: str = "id",
        auto_generate_pk: bool = True,
    ) -> bool:
        """Ensure table exists with proper schema."""
        await self._ensure_schema_exists()

        async with self.db_pool.acquire() as conn:
            table_exists = await self._table_exists(conn, table_name)

            if not table_exists:
                return await self._ensure_table_exists(conn, table_name, sample_record)
            else:
                # Add any missing columns
                await self._add_missing_columns(conn, table_name, sample_record)
                return True

    async def _table_exists(self, conn, table_name: str) -> bool:
        """Check if table exists."""
        result = await conn.fetchval(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = $1 AND table_name = $2
            )
            """,
            self.target_schema,
            table_name,
        )
        return result

    async def _ensure_table_exists(
        self, conn: asyncpg.Connection, table_name: str, sample_record: dict[str, Any]
    ) -> bool:
        """Create table if it doesn't exist."""
        try:
            # Build column definitions from sample record
            columns = []
            for key, value in sample_record.items():
                if key == "id":
                    columns.append("id TEXT PRIMARY KEY")
                elif isinstance(value, bool):
                    columns.append(f"{key} BOOLEAN")
                elif isinstance(value, int):
                    columns.append(f"{key} BIGINT")
                elif isinstance(value, float):
                    columns.append(f"{key} DOUBLE PRECISION")
                elif isinstance(value, datetime):
                    columns.append(f"{key} TIMESTAMPTZ")
                elif isinstance(value, dict | list):
                    columns.append(f"{key} JSONB")
                else:
                    columns.append(f"{key} TEXT")

            create_sql = f"""
                CREATE TABLE {self.target_schema}.{table_name} (
                    {", ".join(columns)}
                )
            """

            await conn.execute(create_sql)
            logger.info(f"Created table {self.target_schema}.{table_name}")
            return True

        except Exception as e:
            logger.error(f"Error creating table {table_name}: {e}")
            raise

    async def _add_missing_columns(
        self, conn: asyncpg.Connection, table_name: str, sample_record: dict[str, Any]
    ) -> None:
        """Add missing columns to existing table."""
        existing_columns = await self._get_existing_columns(conn, table_name)

        for key, value in sample_record.items():
            if key not in existing_columns:
                # Determine column type
                if isinstance(value, bool):
                    col_type = "BOOLEAN"
                elif isinstance(value, int):
                    col_type = "BIGINT"
                elif isinstance(value, float):
                    col_type = "DOUBLE PRECISION"
                elif isinstance(value, datetime):
                    col_type = "TIMESTAMPTZ"
                elif isinstance(value, dict | list):
                    col_type = "JSONB"
                else:
                    col_type = "TEXT"

                try:
                    await conn.execute(
                        f"ALTER TABLE {self.target_schema}.{table_name} ADD COLUMN {key} {col_type}"
                    )
                    logger.debug(f"Added column {key} to {table_name}")
                except Exception as e:
                    logger.warning(f"Could not add column {key} to {table_name}: {e}")

    async def _batch_store_records(
        self,
        table_name: str,
        records: list[dict[str, Any]],
        conflict_strategy: str = "DO_NOTHING",  # "DO_NOTHING", "UPDATE"
        id_field: str = "id",
    ) -> int:
        """Store records in batches with conflict handling."""
        if not records:
            return 0

        async with self.db_pool.acquire() as conn:
            return await self._store_table_records(conn, table_name, records)

    async def _get_existing_columns(self, conn, table_name: str) -> set[str]:
        """Get existing column names for a table."""
        rows = await conn.fetch(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = $1 AND table_name = $2
            """,
            self.target_schema,
            table_name,
        )
        return {row["column_name"] for row in rows}

    async def _store_table_records(
        self, conn: asyncpg.Connection, table_name: str, records: list[dict[str, Any]]
    ) -> int:
        """Store records to table with proper conflict handling."""
        if not records:
            return 0

        stored_count = 0

        for record in records:
            try:
                # Prepare record for storage
                columns = list(record.keys())
                values = list(record.values())

                # Build INSERT query with UPSERT
                placeholders = ", ".join(f"${i + 1}" for i in range(len(values)))
                columns_str = ", ".join(columns)

                # Build conflict resolution
                update_clauses = ", ".join(
                    f"{col} = EXCLUDED.{col}" for col in columns if col != "id"
                )

                insert_sql = f"""
                    INSERT INTO {self.target_schema}.{table_name} ({columns_str})
                    VALUES ({placeholders})
                    ON CONFLICT (id) DO UPDATE SET {update_clauses}
                """

                await conn.execute(insert_sql, *values)
                stored_count += 1

            except Exception as e:
                logger.error(f"Error storing record in {table_name}: {e}")
                # Continue with other records rather than failing entire batch

        return stored_count

    async def _ensure_schema_exists(self) -> bool:
        """Ensure the target schema exists."""
        async with self.db_pool.acquire() as conn:
            await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self.target_schema}")
            return True

    # =============================================================================
    # DATA RETRIEVAL METHODS
    # =============================================================================

    async def _get_raw_package_data_by_ids(
        self, package_ids: list[str]
    ) -> list[dict[str, Any]]:
        """Get raw package data records by package IDs."""
        if not package_ids:
            return []

        placeholders = ", ".join(f"${i + 1}" for i in range(len(package_ids)))
        query = f"""
            SELECT * FROM {self.source_schema}.{self.data_type_name}_package_raw
            WHERE extracted_package_id IN ({placeholders})
        """

        async with self.db_pool.acquire() as conn:
            rows = await conn.fetch(query, *package_ids)
            return [dict(row) for row in rows]

    async def _get_raw_granules_data_by_package_ids(
        self, package_ids: list[str]
    ) -> list[dict[str, Any]]:
        """Get raw granules data records by parent package IDs."""
        if not package_ids:
            return []

        table_name = f"{self.data_type_name}_granules_raw"
        placeholders = ", ".join(f"${i + 1}" for i in range(len(package_ids)))
        query = f"""
            SELECT * FROM {self.source_schema}.{table_name}
            WHERE parent_package_id IN ({placeholders})
        """

        try:
            async with self.db_pool.acquire() as conn:
                rows = await conn.fetch(query, *package_ids)
                return [dict(row) for row in rows]
        except Exception as e:
            logger.warning(f"Could not fetch {table_name} data: {e}")
            return []

    async def _get_raw_granule_data_by_package_ids(
        self, package_ids: list[str]
    ) -> list[dict[str, Any]]:
        """Get raw granule data records by parent package IDs."""
        if not package_ids:
            return []

        table_name = f"{self.data_type_name}_granule_data_raw"
        placeholders = ", ".join(f"${i + 1}" for i in range(len(package_ids)))
        query = f"""
            SELECT * FROM {self.source_schema}.{table_name}
            WHERE parent_package_id IN ({placeholders})
        """

        try:
            async with self.db_pool.acquire() as conn:
                rows = await conn.fetch(query, *package_ids)
                return [dict(row) for row in rows]
        except Exception as e:
            logger.warning(f"Could not fetch {table_name} data: {e}")
            return []

    async def _get_raw_collection_data_by_package_ids(
        self, package_ids: list[str]
    ) -> list[dict[str, Any]]:
        """Get raw collection data records by parent package IDs."""
        if not package_ids:
            return []

        table_name = f"{self.data_type_name}_collection_raw"
        placeholders = ", ".join(f"${i + 1}" for i in range(len(package_ids)))
        query = f"""
            SELECT * FROM {self.source_schema}.{table_name}
            WHERE parent_package_id IN ({placeholders})
        """

        try:
            async with self.db_pool.acquire() as conn:
                rows = await conn.fetch(query, *package_ids)
                return [dict(row) for row in rows]
        except Exception as e:
            logger.warning(f"Could not fetch {table_name} data: {e}")
            return []

    # =============================================================================
    # UTILITY METHODS
    # =============================================================================

    def normalize_to_tables(
        self,
        transformed_data: dict[str, Any],
        related_data: dict[str, list[dict[str, Any]]] | None = None,
    ) -> dict[str, Any]:
        """
        Normalize transformed data to table structure.

        Args:
            transformed_data: Main transformed data
            related_data: Related data by type

        Returns:
            Normalized data ready for storage
        """
        # This method is kept for compatibility but functionality
        # is now integrated into the main processing pipeline
        return {
            "main_data": transformed_data,
            "related_data": related_data or {},
        }

    def get_table_names(self) -> list[str]:
        """Get all table names for this GovInfo data type."""
        tables = [self.get_main_table_name()]

        # Add related tables
        related_tables = self._get_related_tables_with_raw_data()
        for suffix in related_tables:
            tables.append(f"{self.data_type_name}_{suffix}")

        return tables

    def extract_item_id(self, item_data: dict[str, Any]) -> str:
        """Extract package ID from data - must be implemented by subclasses."""
        # This is a fallback - subclasses should implement their specific logic
        return item_data.get("extracted_package_id", str(uuid.uuid4()))
