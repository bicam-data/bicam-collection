"""
Consolidated base cleaner.

This module provides a unified base cleaner that can handle both Congressional and GovInfo
data types with configurable schemas and processing strategies.

FEATURES:
- Configurable schema defaults (staging_schema, production_schema)
- Configurable system names for progress tracking
- Advanced batch processing with checkpoint support
- Multi-table processing support
- Flexible data cleaning patterns
- Support for both simple and complex data types
"""

import asyncio
import html
import logging
import multiprocessing
import re
from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from typing import Any

import asyncpg
from dateutil.parser import parse as parse_date

from bicam_collection.libs.checkpoint import (
    CheckpointStatus,
    HierarchicalProgressTracker,
    ProcessingPhase,
    ProcessingStage,
)
from bicam_collection.libs.data_type_registry import get_global_registry
from bicam_collection.libs.run_tracking import RunMetadata, RunType

logger = logging.getLogger(__name__)


class BaseCleaner:
    """
    Consolidated base cleaner for all data types.

    Provides configurable schema defaults, progress tracking, and advanced processing
    capabilities that work for both Congressional and GovInfo data types.
    """

    def __init__(
        self,
        data_type_name: str,
        system_name: str,
        staging_schema: str | None = None,
        production_schema: str | None = None,
        **kwargs,
    ):
        """
        Initialize the base cleaner.

        Args:
            data_type_name: Name of the data type (e.g., "bills", "members")
            system_name: System name for progress tracking (e.g., "congressional", "govinfo")
            staging_schema: Staging schema for data (auto-detected if None)
            production_schema: Production schema for data (auto-detected if None)
            **kwargs: Additional arguments passed to AbstractCleaner
        """
        # Auto-detect schemas based on system name if not provided
        if staging_schema is None:
            staging_schema = f"bicam_staging_{system_name}"
        if production_schema is None:
            production_schema = f"bicam_{system_name}"

        # Set instance attributes directly since we're not inheriting
        self.data_type_name = data_type_name
        self.staging_schema = staging_schema
        self.production_schema = production_schema
        self.system_name = system_name

        # Set other attributes from kwargs
        self.db_pool = kwargs.get("db_pool")
        self.checkpoint_manager = kwargs.get("checkpoint_manager")
        self.run_manager = kwargs.get("run_manager")
        self.progress_tracker = None
        self.current_run_id = None
        self._configs = None
        # Multi-table processing configuration - override in subclasses
        self.multi_table_data_types: dict[str, list[str]] = {}

        # Target table override system
        self._target_table_override: str | None = None

    def set_processing_resource(self, processing_resource):
        """Set the processing resource for parallel session support."""
        self.processing_resource = processing_resource
        # Get database pool from processing resource if not already set
        if self.db_pool is None and processing_resource:
            self.db_pool = processing_resource.get_db_pool_sync()
        # Get checkpoint manager from processing resource if not already set
        if self.checkpoint_manager is None and processing_resource:
            self.checkpoint_manager = processing_resource.get_checkpoint_manager()
        # Get run manager from processing resource if not already set
        if self.run_manager is None and processing_resource:
            self.run_manager = processing_resource.get_run_manager_sync()

    def setup_progress_tracker(self) -> HierarchicalProgressTracker | None:
        """Setup progress tracker with configurable system name."""
        logger.debug(
            f"Setting up progress tracker: checkpoint_manager={self.checkpoint_manager}, data_type_name={self.data_type_name}"
        )
        if not self.checkpoint_manager or not self.data_type_name:
            logger.warning(
                f"Cannot setup progress tracker - missing dependencies: checkpoint_manager={self.checkpoint_manager}, data_type_name={self.data_type_name}"
            )
            return None

        self.progress_tracker = HierarchicalProgressTracker.create_for_stage(
            self.checkpoint_manager,
            self.system_name,  # Use configurable system name
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
                description=f"{self.system_name.title()} {self.data_type_name} data cleaning from staging to production",
                data_types=[self.data_type_name] if self.data_type_name else [],
            )

        self.current_run_id = self.run_manager.create_run(run_metadata)
        await self.run_manager.start_run(self.current_run_id)
        return self.current_run_id

    def _load_configs(self):
        """Lazy load the schema configurations."""
        if self._configs is None:
            try:
                main_cfg = get_global_registry().get_data_type_config(
                    self.data_type_name
                )
                configs = [main_cfg]
                self._primary_config = main_cfg

                # Load related table configs and include them in main configs list
                self._related_configs = {}
                for suffix in main_cfg.related_tables:
                    related_name = f"{self.data_type_name}_{suffix}"
                    try:
                        related_cfg = get_global_registry().get_data_type_config(
                            related_name
                        )
                        configs.append(related_cfg)  # Include in main configs list
                        self._related_configs[suffix] = (
                            related_cfg  # Also store separately
                        )
                    except FileNotFoundError:
                        # Related table may not have its own config file yet
                        continue

                self._configs = configs

            except Exception as exc:
                logger.error("Failed to load configs via DataTypeRegistry: %s", exc)
                self._configs = []
                self._primary_config = None
                self._related_configs = {}

    # =============================================================================
    # CHECKPOINT MANAGEMENT
    # =============================================================================

    def get_incomplete_data_types_for_resume(self) -> dict[str, list[str]]:
        """
        Identify data types that need specific phases of processing for resume operations.

        Returns:
            Dictionary mapping phase names to lists of data type names that need that phase:
            {
                "cleaning": ["data_type1", "data_type2"],    # Data types needing cleaning
                "post_processing": ["data_type3", "data_type4"]  # Data types needing post-processing
            }
        """
        incomplete_phases = {"cleaning": [], "post_processing": []}
        all_types = self.get_data_types_to_process()

        for table_name in all_types:
            if self.checkpoint_manager:
                # Check cleaning phase checkpoint
                cleaning_checkpoint = self.checkpoint_manager.load_checkpoint(
                    self.system_name, f"{table_name}_cleaning"
                )
                if not cleaning_checkpoint or (
                    cleaning_checkpoint.status != CheckpointStatus.COMPLETED
                ):
                    incomplete_phases["cleaning"].append(table_name)

                # Check post-processing phase checkpoint
                post_checkpoint = self.checkpoint_manager.load_checkpoint(
                    self.system_name, f"{table_name}_post_processing"
                )
                if not post_checkpoint or (
                    post_checkpoint.status != CheckpointStatus.COMPLETED
                ):
                    incomplete_phases["post_processing"].append(table_name)

        return incomplete_phases

    def log_resume_status(self, incomplete_phases: dict[str, list[str]]) -> None:
        """
        Log the resume status showing what phases need processing.

        Args:
            incomplete_phases: Dictionary from get_incomplete_data_types_for_resume()
        """
        total_cleaning = len(incomplete_phases["cleaning"])
        total_post_processing = len(incomplete_phases["post_processing"])

        logger.info(f"Resume status for {self.data_type_name} cleaning:")
        logger.info(f"  Data types needing cleaning: {total_cleaning}")
        if total_cleaning > 0:
            logger.info(f"    {incomplete_phases['cleaning']}")
        logger.info(f"  Data types needing post-processing: {total_post_processing}")
        if total_post_processing > 0:
            logger.info(f"    {incomplete_phases['post_processing']}")

        if total_cleaning == 0 and total_post_processing == 0:
            logger.info("  All data types are fully processed!")
        else:
            logger.info(
                f"  Resume will process up to {max(total_cleaning, total_post_processing)} data types with missing phases"
            )

    def get_incomplete_records_for_data_type(
        self, data_type: str, available_record_ids: list[str]
    ) -> list[str]:
        """
        Identify records within a data type that need processing for resume operations.

        Args:
            data_type: Data type name (table name)
            available_record_ids: List of record IDs available for processing

        Returns:
            List of record IDs that need processing
        """
        if not self.checkpoint_manager:
            return available_record_ids

        # The checkpoint for cleaning operations uses the {table_name}_cleaning format
        cleaning_checkpoint_type = f"{data_type}_cleaning"
        incomplete_records = []

        for record_id in available_record_ids:
            # Check if this record is marked as processed in the cleaning phase
            is_processed = self.checkpoint_manager.is_item_processed(
                self.system_name,
                cleaning_checkpoint_type,
                record_id,
                ProcessingPhase.PHASE_1,
            )

            if not is_processed:
                incomplete_records.append(record_id)

        return incomplete_records

    # =============================================================================
    # DATA TYPE MANAGEMENT
    # =============================================================================

    def get_data_types_to_process(self) -> list[str]:
        """
        Get list of data types to clean.

        Returns a list of *table names* (not individual records) that should be
        streamed from the staging schema. It walks over the configuration
        objects associated with the primary data-type and any explicitly listed
        related tables, falling back to the original *data_type_name* when we
        cannot discover anything.
        """
        self._load_configs()
        discovered: list[str] = []

        # Process all configs (main and related)
        for config in self._configs:
            table_name = self._get_config_table_name(config)
            if table_name:
                discovered.append(table_name)

        # If we found nothing, fall back to the data type name without the "_cleaning" suffix
        if not discovered:
            base_name = self.data_type_name
            if base_name.endswith("_cleaning"):
                base_name = base_name[:-9]  # Remove "_cleaning" suffix
            discovered.append(base_name)

        # Remove duplicates while preserving order
        return list(dict.fromkeys(discovered))

    def get_post_processing_data_types(self) -> list[str]:
        """Get list of data types that need post-processing."""
        # By default, return the same as get_data_types_to_process
        # Subclasses can override this for specific post-processing needs
        return self.get_data_types_to_process()

    # =============================================================================
    # MAIN PROCESSING METHODS
    # =============================================================================

    async def process_items(
        self,
        data_types: list[str] | None = None,
        batch_id: str | None = None,
        rerun: bool = False,
        resume: bool = True,
        chunk_size: int = 1000,
        max_workers: int | None = None,
        **kwargs,
    ) -> dict[str, Any]:
        """
        Main processing method that cleans staging data to production tables.

        Args:
            data_types: List of data types to process (uses get_data_types_to_process if None)
            batch_id: Batch identifier for tracking
            rerun: Whether to reprocess existing records (ignores checkpoints)
            resume: Whether to use checkpoint-based resume logic
            chunk_size: Size of processing chunks
            max_workers: Maximum concurrent workers
            **kwargs: Additional arguments

        Returns:
            Processing statistics
        """
        logger.info(
            f"Starting cleaning for {self.data_type_name}"
            + (f" (batch: {batch_id})" if batch_id else "")
            + (f" (resume: {resume})" if resume else "")
            + (f" (rerun: {rerun})" if rerun else "")
        )

        # Setup progress tracking
        self.setup_progress_tracker()

        # Setup run tracking
        await self.setup_run_tracking()

        # Determine data types to process
        if data_types is None:
            data_types = self.get_data_types_to_process()

        stats = {
            "total_data_types": len(data_types),
            "data_types_processed": 0,
            "total_records_processed": 0,
            "errors": 0,
            "start_time": datetime.now(UTC),
            "batch_id": batch_id,
            "resume_enabled": resume,
            "rerun_enabled": rerun,
        }

        try:
            if resume and not rerun:
                # Determine which data types need which phases of processing
                incomplete_phases = self.get_incomplete_data_types_for_resume()
                self.log_resume_status(incomplete_phases)

                # Process cleaning phase
                if incomplete_phases["cleaning"]:
                    cleaning_stats = await self._parallel_process_data_types(
                        incomplete_phases["cleaning"],
                        chunk_size,
                        max_workers,
                        rerun,
                        phase="cleaning",
                    )
                    stats["data_types_processed"] += len(cleaning_stats)
                    stats["total_records_processed"] += sum(
                        s.get("records_processed", 0) for s in cleaning_stats.values()
                    )
                    stats["errors"] += sum(
                        s.get("errors", 0) for s in cleaning_stats.values()
                    )

                # Process post-processing phase
                if incomplete_phases["post_processing"]:
                    post_processing_stats = await self._run_post_processing(
                        incomplete_phases["post_processing"]
                    )
                    stats.update(post_processing_stats)

            else:
                # Traditional processing without checkpoints (rerun mode)
                cleaning_stats = await self._parallel_process_data_types(
                    data_types,
                    chunk_size,
                    max_workers,
                )
                stats["data_types_processed"] += len(cleaning_stats)
                stats["total_records_processed"] += sum(
                    s.get("records_processed", 0) for s in cleaning_stats.values()
                )
                stats["errors"] += sum(
                    s.get("errors", 0) for s in cleaning_stats.values()
                )

                # Run post-processing
                post_processing_stats = await self._run_post_processing(data_types)
                stats.update(post_processing_stats)

        except Exception as e:
            logger.error(f"Cleaning failed: {e}")
            if self.run_manager and self.current_run_id:
                await self.run_manager.fail_run(self.current_run_id, str(e))
            raise

        finally:
            if self.run_manager and self.current_run_id:
                await self.run_manager.complete_run(self.current_run_id)

        stats["end_time"] = datetime.now(UTC)
        stats["duration"] = (stats["end_time"] - stats["start_time"]).total_seconds()

        logger.info(f"Cleaning completed: {stats}")
        return stats

    # =============================================================================
    # PARALLEL PROCESSING METHODS
    # =============================================================================

    async def _parallel_process_data_types(
        self,
        data_types: list[str],
        chunk_size: int,
        max_workers: int,
        rerun: bool,
        phase: str = "cleaning",
    ) -> dict[str, dict[str, Any]]:
        """Process multiple data types in parallel."""
        if not data_types:
            return {}

        # Determine number of workers
        if max_workers is None:
            max_workers = min(multiprocessing.cpu_count(), len(data_types))

        semaphore = asyncio.Semaphore(max_workers)

        async def process_data_type_with_semaphore(data_type):
            async with semaphore:
                return await self._process_single_data_type(
                    data_type, chunk_size, rerun, phase
                )

        # Process all data types concurrently
        tasks = [
            process_data_type_with_semaphore(data_type) for data_type in data_types
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Collect results
        stats = {}
        for i, result in enumerate(results):
            data_type = data_types[i]
            if isinstance(result, Exception):
                logger.error(f"Error processing {data_type}: {result}", exc_info=True)
                stats[data_type] = {"records_processed": 0, "errors": 1}
            else:
                stats[data_type] = result

        return stats

    async def _process_single_data_type(
        self, data_type: str, chunk_size: int, rerun: bool, phase: str = "cleaning"
    ) -> dict[str, Any]:
        """Process a single data type."""
        logger.info(f"Processing {data_type} ({phase})")

        stats = {"records_processed": 0, "errors": 0}

        try:
            # Stream data in chunks
            async for chunk in self._stream_data_type_chunks(data_type, chunk_size):
                if not chunk:
                    continue

                chunk_stats = await self._process_chunk(chunk, data_type, rerun, phase)
                stats["records_processed"] += chunk_stats.get("records_processed", 0)
                stats["errors"] += chunk_stats.get("errors", 0)

        except Exception as e:
            logger.error(f"Error processing {data_type}: {e}", exc_info=True)
            stats["errors"] += 1

        logger.info(f"Completed {data_type}: {stats}")
        return stats

    # =============================================================================
    # STREAMING METHODS
    # =============================================================================

    async def _stream_data_type_chunks(
        self, data_type: str, chunk_size: int
    ) -> AsyncGenerator[list[dict[str, Any]], None]:
        """Stream data from staging table in chunks."""
        offset = 0
        while True:
            query = f"""
                SELECT * FROM {self.staging_schema}.{data_type}
                LIMIT {chunk_size} OFFSET {offset}
            """
            async with self.db_pool.acquire() as conn:
                rows = await conn.fetch(query)

            if not rows:
                break

            chunk = [dict(row) for row in rows]
            yield chunk

            offset += chunk_size

            # If we got fewer rows than requested, we're done
            if len(rows) < chunk_size:
                break

    async def _process_chunk(
        self,
        chunk: list[dict[str, Any]],
        data_type: str,
        rerun: bool,
        phase: str = "cleaning",
    ) -> dict[str, Any]:
        """Process a chunk of records."""
        if not chunk:
            return {"records_processed": 0, "errors": 0}

        stats = {"records_processed": 0, "errors": 0}

        # Filter records that need processing based on checkpoints
        if not rerun and self.progress_tracker:
            filtered_chunk = []
            for record in chunk:
                record_id = self._get_record_id(record, data_type)
                if not self.progress_tracker.should_skip_item(
                    record_id, ProcessingPhase.PHASE_1, field_name="cleaning"
                ):
                    filtered_chunk.append(record)
            chunk = filtered_chunk

        if not chunk:
            return {"records_processed": 0, "errors": 0}

        # Process each record
        records_by_table = {}  # (target_table, original_data_type) -> list of cleaned records
        for record in chunk:
            try:
                (
                    cleaned_record,
                    target_table,
                    original_data_type,
                ) = await self._clean_single_record(record, data_type)

                # Group by (target_table, original_data_type) to preserve primary key info
                table_key = (target_table, original_data_type)
                if table_key not in records_by_table:
                    records_by_table[table_key] = []
                records_by_table[table_key].append(cleaned_record)

                stats["records_processed"] += 1

            except Exception as e:
                logger.error(f"Error cleaning record in {data_type}: {e}")
                stats["errors"] += 1

        # Store cleaned records grouped by target table
        if records_by_table:
            try:
                async with self.db_pool.acquire() as conn:
                    for (
                        target_table,
                        original_data_type,
                    ), cleaned_records in records_by_table.items():
                        stored_count = await self._store_cleaned_records_batch(
                            conn,
                            cleaned_records,
                            target_table,
                            rerun,
                            original_data_type,
                        )
                        logger.debug(
                            f"Stored {stored_count} cleaned records for {target_table} (from {original_data_type})"
                        )
            except Exception as e:
                logger.error(f"Error storing chunk for {data_type}: {e}")
                total_records = sum(
                    len(records) for records in records_by_table.values()
                )
                stats["errors"] += total_records

        return stats

    # =============================================================================
    # RECORD PROCESSING METHODS
    # =============================================================================

    def _get_record_id(self, record: dict[str, Any], data_type: str) -> str:
        """Extract a unique record ID for checkpoint tracking."""
        # Try common ID fields in order of preference
        id_candidates = [
            "id",
            f"{data_type}_id",
            f"{data_type[:-1]}_id",  # Singular form
            "bill_id",
            "action_id",
            "member_id",
        ]

        for candidate in id_candidates:
            if candidate in record and record[candidate]:
                return str(record[candidate])

        # If no standard ID field, use first available field or fallback
        if record:
            first_key = next(iter(record))
            first_value = record[first_key]
            if first_value:
                return str(first_value)

        # Last resort: use a hash of the record
        import hashlib

        record_str = str(sorted(record.items())) if record else "empty_record"
        return hashlib.md5(record_str.encode()).hexdigest()[:16]

    async def _clean_single_record(
        self, record_data: dict[str, Any], data_type: str
    ) -> tuple[dict[str, Any], str, str]:
        """
        Clean a single record.

        Delegates to data type-specific method if available,
        otherwise uses generic cleaning.

        Cleaning functions can call self._register_target_table_override(target_table)
        to redirect storage to a different table.

        Returns:
            Tuple of (cleaned_record, target_table_name, original_data_type)
        """
        # Clear any previous override
        self._clear_target_table_override()

        # Call the cleaning method
        method_name = f"_clean_{data_type}_singular"
        if hasattr(self, method_name):
            cleaned_data = await getattr(self, method_name)(record_data)
        else:
            cleaned_data = self._clean_single_generic(record_data, data_type)

        # Use override if set, otherwise use original data_type
        target_table = self._target_table_override or data_type

        return cleaned_data, target_table, data_type

    # -------------------------------------------------------------------------
    # Generic record-level cleaning helpers
    # -------------------------------------------------------------------------

    def _clean_single_generic(
        self, record_data: dict[str, Any], data_type: str
    ) -> dict[str, Any]:
        """A fallback cleaning routine when no specialised method is defined."""
        cleaned = record_data.copy()

        for key, value in cleaned.items():
            if isinstance(value, str):
                # Trim and de-HTML long strings.
                value = value.strip()
                if len(value) > 100:
                    value = self.clean_long_text(value)

                if "chamber" in key.lower():
                    value = self.standardize_chamber(value)

                cleaned[key] = value

            elif value and "date" in key.lower():
                cleaned[key] = self.standardize_date(value)

            elif "number" in key.lower() or key.endswith("_number"):
                cleaned[key] = self.safe_int(value)

        return cleaned

    # =============================================================================
    # STORAGE METHODS
    # =============================================================================

    async def _store_cleaned_records_batch(
        self,
        conn: asyncpg.Connection,
        cleaned_records: list[dict[str, Any]],
        data_type: str,
        rerun: bool = False,
        original_data_type: str | None = None,
    ) -> int:
        """Store a batch of cleaned records."""
        if not cleaned_records:
            return 0

        # Ensure table exists
        sample_record = cleaned_records[0]
        await self._ensure_production_table_exists(
            conn, data_type, sample_record, original_data_type
        )

        # Store records
        stored_count = await self._store_table_records(
            conn, data_type, cleaned_records, rerun
        )

        return stored_count

    async def _ensure_production_table_exists(
        self,
        conn: asyncpg.Connection,
        data_type: str,
        sample_record: dict[str, Any],
        original_data_type: str | None = None,
    ) -> None:
        """Ensure production table exists with proper schema."""
        table_exists = await conn.fetchval(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = $1 AND table_name = $2
            )
            """,
            self.production_schema,
            data_type,
        )

        if not table_exists:
            await self._create_production_table(
                conn, data_type, sample_record, original_data_type
            )
        else:
            # Add missing columns
            await self._add_missing_production_columns(conn, data_type, sample_record)

    async def _create_production_table(
        self,
        conn: asyncpg.Connection,
        data_type: str,
        sample_record: dict[str, Any],
        original_data_type: str | None = None,
    ) -> None:
        """Create production table with proper schema."""
        # Determine primary key columns using original data type for config lookup
        config_lookup_type = original_data_type or data_type
        primary_key_columns = await self._get_primary_key_columns(
            conn, config_lookup_type, sample_record
        )

        # Build column definitions from sample record
        columns = []
        for key, value in sample_record.items():
            if isinstance(value, bool):
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

        # Add primary key constraint if we have primary key columns
        if primary_key_columns:
            primary_key_str = ", ".join(primary_key_columns)
            constraint_sql = f", PRIMARY KEY ({primary_key_str})"
        else:
            constraint_sql = ""

        create_sql = f"""
            CREATE TABLE {self.production_schema}.{data_type} (
                {", ".join(columns)}{constraint_sql}
            )
        """

        await conn.execute(create_sql)

        if primary_key_columns:
            logger.info(
                f"Created production table {self.production_schema}.{data_type} with primary key: {primary_key_columns}"
            )
        else:
            logger.warning(
                f"Table {self.production_schema}.{data_type} created without primary key - duplicates may occur"
            )

    async def _add_missing_production_columns(
        self, conn: asyncpg.Connection, data_type: str, sample_record: dict[str, Any]
    ) -> None:
        """Add missing columns to existing production table."""
        # Get existing columns
        existing_columns = await self._get_existing_columns(conn, data_type)

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
                        f"ALTER TABLE {self.production_schema}.{data_type} ADD COLUMN {key} {col_type}"
                    )
                    logger.debug(f"Added column {key} to {data_type}")
                except Exception as e:
                    logger.warning(f"Could not add column {key} to {data_type}: {e}")

    async def _get_existing_columns(
        self, conn: asyncpg.Connection, data_type: str
    ) -> set[str]:
        """Get existing column names for a table."""
        rows = await conn.fetch(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = $1 AND table_name = $2
            """,
            self.production_schema,
            data_type,
        )
        return {row["column_name"] for row in rows}

    async def _store_table_records(
        self,
        conn: asyncpg.Connection,
        table_name: str,
        records: list[dict[str, Any]],
        rerun: bool = False,
    ) -> int:
        """Store records to production table."""
        if not records:
            return 0

        # Determine primary key column(s) using original data type for config lookup
        primary_key_columns = await self._get_primary_key_columns(
            conn, table_name, records[0]
        )

        stored_count = 0

        for record in records:
            try:
                # Prepare record for storage
                columns = list(record.keys())
                # Ensure all datetime objects are timezone-aware before storage
                values = []
                for value in record.values():
                    if isinstance(value, datetime):
                        # Ensure datetime objects are timezone-aware
                        if value.tzinfo is None:
                            # Convert timezone-naive datetime to UTC
                            values.append(value.replace(tzinfo=UTC))
                        else:
                            values.append(value)
                    else:
                        values.append(value)

                # Build INSERT query with optional UPSERT
                placeholders = ", ".join(f"${i + 1}" for i in range(len(values)))
                columns_str = ", ".join(columns)

                if primary_key_columns and all(
                    pk in record for pk in primary_key_columns
                ):
                    primary_key_str = ", ".join(primary_key_columns)

                    if rerun:
                        update_clauses = ", ".join(
                            f"{col} = EXCLUDED.{col}"
                            for col in columns
                            if col not in primary_key_columns
                        )
                        insert_sql = f"""
                            INSERT INTO {self.production_schema}.{table_name} ({columns_str})
                            VALUES ({placeholders})
                            ON CONFLICT ({primary_key_str}) DO UPDATE SET {update_clauses}
                        """
                    else:
                        insert_sql = f"""
                            INSERT INTO {self.production_schema}.{table_name} ({columns_str})
                            VALUES ({placeholders})
                            ON CONFLICT ({primary_key_str}) DO NOTHING
                        """
                else:
                    # No primary key available – simple insert (may create duplicates)
                    insert_sql = f"""
                        INSERT INTO {self.production_schema}.{table_name} ({columns_str})
                        VALUES ({placeholders})
                    """

                status = await conn.execute(insert_sql, *values)
                inserted = int(status.split()[-1]) if status.startswith("INSERT") else 0
                stored_count += inserted

            except Exception as e:
                # Use primary key for error logging if available, otherwise use first available field
                if primary_key_columns:
                    record_id_parts = [
                        str(record.get(pk, "MISSING")) for pk in primary_key_columns
                    ]
                    record_id = f"({', '.join(record_id_parts)})"
                else:
                    record_id = record.get(
                        f"{table_name[:-1]}_id",
                        record.get(list(record.keys())[0] if record else "UNKNOWN"),
                    )
                logger.error(f"Error storing record {record_id} in {table_name}: {e}")

        return stored_count

    # =============================================================================
    # POST-PROCESSING METHODS
    # =============================================================================

    async def _run_post_processing(self, data_types: list[str]) -> dict[str, Any]:
        """Run post-processing for data types."""
        logger.info(f"Running post-processing for {len(data_types)} data types")

        stats = {
            "post_processing_data_types": len(data_types),
            "post_processing_errors": 0,
        }

        for data_type in data_types:
            try:
                # Subclasses can override this method for specific post-processing
                await self._post_process_data_type(data_type)
            except Exception as e:
                logger.error(f"Post-processing error for {data_type}: {e}")
                stats["post_processing_errors"] += 1

        return stats

    async def _post_process_data_type(self, data_type: str) -> None:
        """Post-process a single data type. Override in subclasses."""
        # Default implementation does nothing

    # =============================================================================
    # UTILITY METHODS
    # =============================================================================

    def standardize_date(self, date_value: Any) -> datetime | None:
        """
        Standardize date values to consistent timezone-aware datetime objects.

        Args:
            date_value: Raw date value in various formats

        Returns:
            Standardized timezone-aware datetime object (UTC) or None if invalid
        """
        if not date_value or date_value in ["", "null", "None"]:
            return None

        if isinstance(date_value, datetime):
            # Ensure existing datetime objects are timezone-aware
            if date_value.tzinfo is None:
                # Assume timezone-naive datetime is in UTC
                return date_value.replace(tzinfo=UTC)
            return date_value

        if isinstance(date_value, str):
            try:
                parsed_date = parse_date(date_value)
                # Ensure parsed datetime is timezone-aware
                if parsed_date.tzinfo is None:
                    # Assume timezone-naive datetime is in UTC
                    return parsed_date.replace(tzinfo=UTC)
                return parsed_date
            except Exception:
                logger.warning(f"Could not parse date: {date_value}")
                return None

        return None

    def clean_long_text(self, text: str | None) -> str | None:
        """
        Clean and normalize long text fields, including HTML content.

        Args:
            text: Raw text to clean (may contain HTML)

        Returns:
            Cleaned text or None
        """
        if not text:
            return None

        # Store original for fallback
        original_text = text

        try:
            # First, handle HTML content if present
            if "<" in text and ">" in text:
                # Remove DOCTYPE declarations and XML namespaces
                cleaned = re.sub(r"<!DOCTYPE[^>]*>", "", text, flags=re.IGNORECASE)
                cleaned = re.sub(r"<\?xml[^>]*\?>", "", cleaned, flags=re.IGNORECASE)

                # Convert HTML paragraph and line break tags to newlines for structure preservation
                cleaned = re.sub(r"</p>", "\n\n", cleaned, flags=re.IGNORECASE)
                cleaned = re.sub(r"<br\s*/?>", "\n", cleaned, flags=re.IGNORECASE)
                cleaned = re.sub(r"</div>", "\n", cleaned, flags=re.IGNORECASE)
                cleaned = re.sub(r"</section>", "\n\n", cleaned, flags=re.IGNORECASE)

                # Remove all remaining HTML tags
                cleaned = re.sub(r"<[^>]+>", "", cleaned)

                # Decode HTML entities (like &nbsp;, &lt;, &gt;, etc.)
                cleaned = html.unescape(cleaned)
            else:
                cleaned = text

            # Remove common artifacts
            cleaned = cleaned.replace("\x00", "")  # Null bytes
            cleaned = cleaned.replace("\ufffd", "")  # Unicode replacement character
            cleaned = cleaned.replace("\u00ad", "")  # Soft hyphens

            # Normalize line endings
            cleaned = cleaned.replace("\r\n", "\n")
            cleaned = cleaned.replace("\r", "\n")

            # Clean up excessive whitespace while preserving paragraph structure
            # First normalize multiple newlines (preserve double newlines for paragraphs)
            cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)

            # Remove spaces at the beginning/end of lines
            cleaned = re.sub(r"[ \t]+\n", "\n", cleaned)
            cleaned = re.sub(r"\n[ \t]+", "\n", cleaned)

            # Replace multiple spaces/tabs with single space
            cleaned = re.sub(r"[ \t]{2,}", " ", cleaned)

            # Clean up any remaining excessive whitespace
            cleaned = cleaned.strip()

            # Remove common legislative document artifacts
            cleaned = re.sub(r"\[\[Page\s+[^\]]+\]\]", "", cleaned)  # Page markers
            cleaned = re.sub(
                r"¿+", "", cleaned
            )  # Strange characters sometimes in congressional docs

            # Final whitespace cleanup
            cleaned = re.sub(
                r"\n\s*\n\s*\n", "\n\n", cleaned
            )  # Normalize paragraph breaks

            return cleaned if cleaned else None

        except Exception as e:
            # If cleaning fails for any reason, fall back to basic cleaning
            logger.warning(f"Advanced text cleaning failed, using basic cleaning: {e}")
            basic_cleaned = re.sub(r"\s+", " ", original_text.strip())
            basic_cleaned = basic_cleaned.replace("\x00", "")
            return basic_cleaned if basic_cleaned else None

    def standardize_chamber(self, chamber: str | None) -> str | None:
        """
        Standardize chamber values to consistent format.

        Args:
            chamber: Raw chamber value

        Returns:
            Standardized chamber value
        """
        if not chamber:
            return None

        chamber_lower = chamber.lower().strip()

        if chamber_lower in ["house", "h", "house of representatives"]:
            return "house"
        elif chamber_lower in ["senate", "s"]:
            return "senate"
        elif chamber_lower in ["joint", "both"]:
            return "joint"
        elif chamber_lower in ["nochamber", "no chamber"]:
            return "nochamber"
        else:
            return chamber  # Return original if not recognized

    def safe_int(
        self, value: Any, default: int | None = None, set_to_float: bool = False
    ) -> int | float | None:
        """
        Safely convert value to integer.

        Args:
            value: Value to convert
            default: Default value if conversion fails

        Returns:
            Integer value or default
        """
        if value is None or value == "":
            return default

        try:
            if isinstance(value, str):
                # Remove common non-numeric characters
                cleaned = re.sub(r"[^\d-]", "", value)
                if cleaned:
                    if set_to_float:
                        return float(cleaned)
                    return int(cleaned)
            else:
                if set_to_float:
                    return float(value)
                return int(value)
        except (ValueError, TypeError):
            pass

        return default

    # =============================================================================
    # CONFIG HELPER METHODS
    # =============================================================================

    def _get_config_table_name(self, config) -> str:
        """Get table name from config object."""
        from bicam_collection.libs.data_type_config import DataTypeConfig as _DC

        if isinstance(config, _DC):
            return config.table_name
        if isinstance(config, dict):
            return config.get("table_name", "")
        return getattr(config, "table_name", "")

    def _get_config_fields(self, config):
        """Get fields from config object."""
        from bicam_collection.libs.data_type_config import DataTypeConfig as _DC

        if isinstance(config, _DC):
            return config.schema.fields
        if isinstance(config, dict):
            return config.get("fields", [])
        return getattr(config, "fields", [])

    def _get_config_id_fields(self, config):
        """Get ID fields from config object."""
        from bicam_collection.libs.data_type_config import DataTypeConfig as _DC

        if isinstance(config, _DC):
            return config.schema.id_fields
        if isinstance(config, dict):
            return config.get("id_fields", [])
        return getattr(config, "id_fields", [])

    def _get_field_name(self, field) -> str:
        """Get field name from field definition."""
        if isinstance(field, dict):
            return field.get("name", "")
        return getattr(field, "name", "")

    def _get_field_type(self, field) -> str:
        """Get field type from field definition."""
        if isinstance(field, dict):
            return field.get("type", "TEXT")
        return getattr(field, "type", "TEXT")

    async def _get_primary_key_columns(
        self, conn: asyncpg.Connection, data_type: str, sample_record: dict[str, Any]
    ) -> list[str]:
        """Get primary key columns for a production table from configuration."""
        # First, try to get from configuration
        self._load_configs()

        # Check if this is a related table
        for suffix, config in self._related_configs.items():
            if data_type == f"{self.data_type_name}_{suffix}":
                id_fields = self._get_config_id_fields(config)
                if id_fields:
                    # Validate that all id_fields exist in the sample record
                    valid_id_fields = []
                    for field in id_fields:
                        if field in sample_record:
                            valid_id_fields.append(field)
                        else:
                            logger.warning(
                                f"Primary key field '{field}' from config not found in sample record for {data_type}"
                            )

                    if valid_id_fields:
                        logger.debug(
                            f"Using primary keys from related config for {data_type}: {valid_id_fields}"
                        )
                        return valid_id_fields

        # Check main table config
        if self._primary_config and data_type == self._get_config_table_name(
            self._primary_config
        ):
            id_fields = self._get_config_id_fields(self._primary_config)
            if id_fields:
                # Validate that all id_fields exist in the sample record
                valid_id_fields = []
                for field in id_fields:
                    if field in sample_record:
                        valid_id_fields.append(field)
                    else:
                        logger.warning(
                            f"Primary key field '{field}' from config not found in sample record for {data_type}"
                        )

                if valid_id_fields:
                    logger.debug(
                        f"Using primary keys from main config for {data_type}: {valid_id_fields}"
                    )
                    return valid_id_fields

        # No primary key found
        logger.warning(
            f"No primary key found for {data_type}, table will not have UPSERT capability"
        )
        return []

    # =============================================================================
    # TARGET TABLE OVERRIDE METHODS
    # =============================================================================

    def _register_target_table_override(self, target_table: str) -> None:
        """Register a target table override for the next processing operation."""
        self._target_table_override = target_table

    def _clear_target_table_override(self) -> None:
        """Clear the target table override."""
        self._target_table_override = None
