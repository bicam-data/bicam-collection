"""
GovInfo-specific base cleaner.

This module provides GovInfo-specific implementations including:
- GovInfo schema defaults (bicam_staging_govinfo, bicam_govinfo)
- GovInfo progress tracking configuration
- GovInfo-specific cleaning patterns
"""

import asyncio
import logging
from datetime import UTC, datetime
from typing import Any

import asyncpg

from ....libs.checkpoint import HierarchicalProgressTracker, ProcessingStage
from ....libs.run_tracking import RunMetadata, RunType
from ...abstract import AbstractCleaner

logger = logging.getLogger(__name__)


class GovInfoBaseCleaner(AbstractCleaner):
    """
    GovInfo-specific base cleaner.

    Provides GovInfo schema defaults and GovInfo-specific
    progress tracking configuration.
    """

    def __init__(self, **kwargs):
        # Provide GovInfo-specific schema defaults
        kwargs.setdefault("staging_schema", "bicam_staging_govinfo")
        kwargs.setdefault("production_schema", "bicam_govinfo")
        super().__init__(**kwargs)

    def setup_progress_tracker(self) -> HierarchicalProgressTracker | None:
        """GovInfo-specific progress tracker setup."""
        if not self.checkpoint_manager or not self.data_type_name:
            logger.warning("Cannot setup progress tracker - missing dependencies")
            return None

        self.progress_tracker = HierarchicalProgressTracker.create_for_stage(
            self.checkpoint_manager,
            "govinfo",  # GovInfo-specific system name
            self.data_type_name,
            ProcessingStage.CLEANING,
        )
        return self.progress_tracker

    async def setup_run_tracking(
        self, run_metadata: RunMetadata | None = None
    ) -> str | None:
        """Setup run tracking for this cleaning session."""
        if not self.run_manager:
            return None

        if not run_metadata:
            run_metadata = RunMetadata(
                run_id="",
                run_type=RunType.DATA_CLEANING,
                system_name=f"{self.data_type_name}_cleaner",
                description=f"GovInfo {self.data_type_name} data cleaning to production",
                data_types=[self.data_type_name] if self.data_type_name else [],
            )

        self.current_run_id = self.run_manager.create_run(run_metadata)
        await self.run_manager.start_run(self.current_run_id)
        return self.current_run_id

    def get_data_types_to_process(self) -> list[str]:
        """Get list of GovInfo data types to clean."""
        if self.data_type_name:
            return [self.data_type_name]

        # Default GovInfo data types
        return [
            "bills_collection",
            "congressional_reports",
            "congressional_hearings",
            "federal_register",
            "statute",
            "code",
        ]

    async def process_items(
        self,
        item_ids: list[str] | None = None,
        batch_size: int = 100,
        max_concurrent: int = 10,
        **kwargs,
    ) -> dict[str, Any]:
        """
        Main GovInfo cleaning processing method.

        Args:
            item_ids: Specific package IDs to clean (optional)
            batch_size: Size of processing batches
            max_concurrent: Maximum concurrent operations
            **kwargs: Additional parameters

        Returns:
            Processing statistics
        """
        logger.info(f"Starting GovInfo cleaning for {self.data_type_name}")

        # Setup progress tracking
        self.setup_progress_tracker()

        # Setup run tracking
        await self.setup_run_tracking()

        stats = {
            "data_types_processed": 0,
            "tables_cleaned": 0,
            "records_processed": 0,
            "records_cleaned": 0,
            "errors": 0,
            "start_time": datetime.now(UTC),
        }

        try:
            data_types_to_process = self.get_data_types_to_process()

            for data_type in data_types_to_process:
                logger.info(f"Processing GovInfo data type: {data_type}")

                try:
                    data_type_stats = await self._process_data_type_cleaning(
                        data_type, item_ids, batch_size, max_concurrent
                    )

                    # Aggregate results
                    stats["tables_cleaned"] += data_type_stats.get("tables_cleaned", 0)
                    stats["records_processed"] += data_type_stats.get(
                        "records_processed", 0
                    )
                    stats["records_cleaned"] += data_type_stats.get(
                        "records_cleaned", 0
                    )
                    stats["data_types_processed"] += 1

                except Exception as e:
                    logger.error(f"Error processing {data_type}: {e}")
                    stats["errors"] += 1

        except Exception as e:
            logger.error(f"GovInfo cleaning failed: {e}")
            if self.run_manager and self.current_run_id:
                await self.run_manager.fail_run(self.current_run_id, str(e))
            raise

        finally:
            if self.run_manager and self.current_run_id:
                await self.run_manager.complete_run(self.current_run_id)

        stats["end_time"] = datetime.now(UTC)
        stats["duration"] = (stats["end_time"] - stats["start_time"]).total_seconds()

        logger.info(f"GovInfo cleaning completed: {stats}")
        return stats

    async def _process_data_type_cleaning(
        self,
        data_type: str,
        item_ids: list[str] | None,
        batch_size: int,
        max_concurrent: int,
    ) -> dict[str, Any]:
        """Process cleaning for a specific GovInfo data type."""
        stats = {
            "tables_cleaned": 0,
            "records_processed": 0,
            "records_cleaned": 0,
        }

        # Get all tables for this data type
        table_names = await self._get_staging_tables_for_data_type(data_type)

        for table_name in table_names:
            try:
                table_stats = await self._clean_table(
                    table_name, item_ids, batch_size, max_concurrent
                )

                stats["tables_cleaned"] += 1
                stats["records_processed"] += table_stats.get("records_processed", 0)
                stats["records_cleaned"] += table_stats.get("records_cleaned", 0)

            except Exception as e:
                logger.error(f"Error cleaning table {table_name}: {e}")
                raise

        return stats

    async def _get_staging_tables_for_data_type(self, data_type: str) -> list[str]:
        """Get all staging tables for a GovInfo data type."""
        async with self.db_pool.acquire() as conn:
            result = await conn.fetch(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = $1
                AND table_name LIKE $2
                ORDER BY table_name
                """,
                self.staging_schema,
                f"{data_type}_%",
            )
            return [row["table_name"] for row in result]

    async def _clean_table(
        self,
        table_name: str,
        item_ids: list[str] | None,
        batch_size: int,
        max_concurrent: int,
    ) -> dict[str, Any]:
        """Clean a specific staging table."""
        logger.info(f"Cleaning GovInfo table: {table_name}")

        # Get records to process
        records = await self._get_staging_records(table_name, item_ids)

        if not records:
            logger.info(f"No records to clean in {table_name}")
            return {"records_processed": 0, "records_cleaned": 0}

        stats = {"records_processed": len(records), "records_cleaned": 0}

        # Ensure production table exists
        await self._ensure_production_table(table_name, records[0])

        # Process in batches
        for i in range(0, len(records), batch_size):
            batch = records[i : i + batch_size]

            # Clean batch concurrently
            cleaned_batch = await self._clean_batch_concurrent(batch, max_concurrent)

            # Store cleaned records
            stored_count = await self._store_cleaned_batch(table_name, cleaned_batch)
            stats["records_cleaned"] += stored_count

        logger.info(f"Completed cleaning {table_name}: {stats}")
        return stats

    async def _get_staging_records(
        self, table_name: str, item_ids: list[str] | None
    ) -> list[dict[str, Any]]:
        """Get staging records to clean."""
        async with self.db_pool.acquire() as conn:
            if item_ids:
                # Filter by specific IDs
                placeholders = ", ".join(f"${i + 1}" for i in range(len(item_ids)))
                query = f"""
                    SELECT * FROM {self.staging_schema}.{table_name}
                    WHERE id IN ({placeholders})
                """
                rows = await conn.fetch(query, *item_ids)
            else:
                # Get all records
                query = f"SELECT * FROM {self.staging_schema}.{table_name}"
                rows = await conn.fetch(query)

            return [dict(row) for row in rows]

    async def _clean_batch_concurrent(
        self, batch: list[dict[str, Any]], max_concurrent: int
    ) -> list[dict[str, Any]]:
        """Clean a batch of records with concurrency control."""
        semaphore = asyncio.Semaphore(max_concurrent)

        async def clean_record_concurrent(record):
            async with semaphore:
                return self._clean_single_record(record)

        tasks = [clean_record_concurrent(record) for record in batch]
        cleaned_records = await asyncio.gather(*tasks)

        return [record for record in cleaned_records if record is not None]

    def _clean_single_record(self, record: dict[str, Any]) -> dict[str, Any] | None:
        """Clean a single record."""
        try:
            cleaned_record = record.copy()

            # Apply GovInfo-specific cleaning rules
            for field_name, value in record.items():
                if value is not None:
                    cleaned_value = self._clean_field_value(field_name, value)
                    cleaned_record[field_name] = cleaned_value

            # Add cleaning metadata
            cleaned_record["cleaned_at"] = datetime.now(UTC)
            cleaned_record["cleaning_version"] = "1.0"

            return cleaned_record

        except Exception as e:
            logger.error(f"Error cleaning record {record.get('id', 'UNKNOWN')}: {e}")
            return None

    def _clean_field_value(self, field_name: str, value: Any) -> Any:
        """Apply GovInfo-specific field cleaning rules."""
        if isinstance(value, str):
            return self._clean_text_field(value)
        elif isinstance(value, dict):
            return self._clean_dict_field(value)
        elif isinstance(value, list):
            return self._clean_list_field(value)
        else:
            return value

    def _clean_text_field(self, text: str) -> str | None:
        """Clean text field."""
        if not text or not text.strip():
            return None

        # Remove extra whitespace
        cleaned = " ".join(text.split())

        # Remove control characters
        cleaned = "".join(
            char for char in cleaned if ord(char) >= 32 or char in ["\n", "\t"]
        )

        # Truncate extremely long text
        if len(cleaned) > 50000:  # 50KB limit
            cleaned = cleaned[:50000] + "... [TRUNCATED]"
            logger.warning("Truncated extremely long text field")

        return cleaned if cleaned else None

    def _clean_dict_field(self, data: dict) -> dict | None:
        """Clean dictionary field."""
        if not data:
            return None

        cleaned_dict = {}
        for key, value in data.items():
            cleaned_key = self._clean_text_field(str(key)) if key else None
            if cleaned_key:
                cleaned_dict[cleaned_key] = self._clean_field_value(key, value)

        return cleaned_dict if cleaned_dict else None

    def _clean_list_field(self, data: list) -> list | None:
        """Clean list field."""
        if not data:
            return None

        cleaned_list = []
        for item in data:
            cleaned_item = self._clean_field_value("list_item", item)
            if cleaned_item is not None:
                cleaned_list.append(cleaned_item)

        return cleaned_list if cleaned_list else None

    async def _ensure_production_table(
        self, staging_table_name: str, sample_record: dict[str, Any]
    ) -> None:
        """Ensure production table exists with proper schema."""
        async with self.db_pool.acquire() as conn:
            # Create production schema if needed
            await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self.production_schema}")

            # Check if table exists
            table_exists = await conn.fetchval(
                """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = $1 AND table_name = $2
                )
                """,
                self.production_schema,
                staging_table_name,
            )

            if not table_exists:
                await self._create_production_table(
                    conn, staging_table_name, sample_record
                )
            else:
                await self._add_missing_columns_to_production(
                    conn, staging_table_name, sample_record
                )

    async def _create_production_table(
        self, conn: asyncpg.Connection, table_name: str, sample_record: dict[str, Any]
    ) -> None:
        """Create production table with schema based on sample record."""
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

        # Add cleaning metadata columns
        columns.extend(["cleaned_at TIMESTAMPTZ", "cleaning_version TEXT"])

        create_sql = f"""
            CREATE TABLE {self.production_schema}.{table_name} (
                {", ".join(columns)}
            )
        """

        await conn.execute(create_sql)
        logger.info(f"Created production table {self.production_schema}.{table_name}")

    async def _add_missing_columns_to_production(
        self, conn: asyncpg.Connection, table_name: str, sample_record: dict[str, Any]
    ) -> None:
        """Add missing columns to production table."""
        # Get existing columns
        rows = await conn.fetch(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = $1 AND table_name = $2
            """,
            self.production_schema,
            table_name,
        )
        existing_columns = {row["column_name"] for row in rows}

        # Add missing columns
        for key, value in sample_record.items():
            if key not in existing_columns:
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
                        f"ALTER TABLE {self.production_schema}.{table_name} ADD COLUMN {key} {col_type}"
                    )
                    logger.debug(f"Added column {key} to {table_name}")
                except Exception as e:
                    logger.warning(f"Could not add column {key} to {table_name}: {e}")

        # Ensure cleaning metadata columns exist
        if "cleaned_at" not in existing_columns:
            await conn.execute(
                f"ALTER TABLE {self.production_schema}.{table_name} ADD COLUMN cleaned_at TIMESTAMPTZ"
            )

        if "cleaning_version" not in existing_columns:
            await conn.execute(
                f"ALTER TABLE {self.production_schema}.{table_name} ADD COLUMN cleaning_version TEXT"
            )

    async def _store_cleaned_batch(
        self, table_name: str, cleaned_records: list[dict[str, Any]]
    ) -> int:
        """Store cleaned records to production table."""
        if not cleaned_records:
            return 0

        async with self.db_pool.acquire() as conn:
            stored_count = 0

            for record in cleaned_records:
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
                        INSERT INTO {self.production_schema}.{table_name} ({columns_str})
                        VALUES ({placeholders})
                        ON CONFLICT (id) DO UPDATE SET {update_clauses}
                    """

                    await conn.execute(insert_sql, *values)
                    stored_count += 1

                except Exception as e:
                    logger.error(f"Error storing cleaned record in {table_name}: {e}")

            return stored_count
