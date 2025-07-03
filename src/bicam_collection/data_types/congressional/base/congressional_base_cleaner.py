"""
Congressional-specific base cleaner.

This module provides Congressional-specific implementations including:
- Congressional schema defaults (bicam_staging_congressional, bicam_congressional)
- Congressional progress tracking configuration
- Congressional-specific cleaning patterns
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

from bicam_collection.libs.data_type_registry import get_global_registry

from ....libs.checkpoint import (
    HierarchicalProgressTracker,
    ProcessingPhase,
    ProcessingStage,
)
from ....libs.run_tracking import RunMetadata, RunType
from ...abstract import AbstractCleaner

logger = logging.getLogger(__name__)


class CongressionalBaseCleaner(AbstractCleaner):
    """
    Congressional-specific base cleaner.

    Provides Congressional schema defaults and Congressional-specific
    progress tracking configuration.
    """

    def __init__(self, **kwargs):
        # Provide Congressional-specific schema defaults
        kwargs.setdefault("staging_schema", "bicam_staging_congressional")
        kwargs.setdefault("production_schema", "bicam_congressional")
        super().__init__(**kwargs)
        self._configs = None
        # Multi-table processing configuration - override in subclasses
        self.multi_table_data_types: dict[str, list[str]] = {}

        # Target table override system
        self._target_table_override: str | None = None

    def setup_progress_tracker(self) -> HierarchicalProgressTracker | None:
        """Congressional-specific progress tracker setup."""
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
            "congressional",  # Congressional-specific system name
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
                description=f"{self.data_type_name} data cleaning from staging to production",
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

                # Attempt to load configs for related tables if YAML exists; ignore missing
                for suffix in main_cfg.related_tables:
                    related_name = f"{self.data_type_name}_{suffix}"
                    try:
                        cfg = get_global_registry().get_data_type_config(related_name)
                        configs.append(cfg)
                    except FileNotFoundError:
                        # Related table may not have its own config file yet
                        continue

                self._configs = configs
            except Exception as exc:
                logger.error("Failed to load configs via DataTypeRegistry: %s", exc)
                self._configs = []

    # =============================================================================
    # CHECKPOINT MANAGEMENT
    # =============================================================================

    def get_incomplete_data_types_for_resume(
        self, available_data_types: list[str]
    ) -> dict[str, list[str]]:
        """
        Identify data types that need specific phases of processing for resume operations.

        Args:
            available_data_types: List of data type names that are available for processing

        Returns:
            Dictionary mapping phase names to lists of data type names that need that phase:
            {
                "cleaning": ["data_type1", "data_type2"],    # Data types needing cleaning
                "post_processing": ["data_type3", "data_type4"]  # Data types needing post-processing
            }
        """
        if not self.progress_tracker:
            # If no progress tracker, assume all data types need all phases
            return {
                "cleaning": available_data_types.copy(),
                "post_processing": available_data_types.copy(),
            }

        incomplete_phases = {"cleaning": [], "post_processing": []}

        for data_type in available_data_types:
            # Check cleaning phase for this data type
            if not self.progress_tracker.should_skip_item(
                data_type, ProcessingPhase.MAIN_ITEMS, field_name="cleaning"
            ):
                incomplete_phases["cleaning"].append(data_type)

            # Check post-processing phase for this data type
            if not self.progress_tracker.should_skip_item(
                data_type,
                ProcessingPhase.RELATED_ENTITIES,
                field_name="post_processing",
            ):
                incomplete_phases["post_processing"].append(data_type)

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
            data_type: Data type name
            available_record_ids: List of record IDs available for processing

        Returns:
            List of record IDs that need processing
        """
        if not self.progress_tracker:
            # If no progress tracker, assume all records need processing
            return available_record_ids.copy()

        incomplete_records = []

        for record_id in available_record_ids:
            # Check if this record's cleaning is already completed
            if not self.progress_tracker.should_skip_item(
                f"{data_type}:{record_id}",
                ProcessingPhase.MAIN_ITEMS,
                field_name="record_cleaning",
            ):
                incomplete_records.append(record_id)

        return incomplete_records

    # =============================================================================
    # CONGRESSIONAL-SPECIFIC IMPLEMENTATIONS
    # =============================================================================

    def get_data_types_to_process(self) -> list[str]:
        """
        Get list of data types to clean.

        Override to specify custom data types, or let it auto-discover
        from configuration.

        Returns:
            List of data type table names to process
        """
        self._load_configs()
        data_types = []

        for config in self._configs:
            if isinstance(config, dict):
                table_name = config.get("table_name") or config.get("name", "")
            else:
                table_name = getattr(config, "table_name", "")

            if table_name and (
                not self.data_type_name or table_name.startswith(self.data_type_name)
            ):
                data_types.append(table_name)

        return data_types

    def get_post_processing_data_types(self) -> list[str]:
        """
        Get list of data types that need post-processing.

        Override to specify which data types need post-processing operations.

        Returns:
            List of data type names that need post-processing
        """
        post_processing_types = []
        data_types = self.get_data_types_to_process()

        for data_type in data_types:
            if hasattr(self, f"_post_process_{data_type}"):
                post_processing_types.append(data_type)

        return post_processing_types

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
        Main cleaning method that processes data types efficiently with checkpoint-based resume.

        Args:
            data_types: List of data types to process (auto-discovered if None)
            batch_id: Batch identifier for tracking
            rerun: Whether to reprocess existing records (ignores checkpoints)
            resume: Whether to use checkpoint-based resume logic
            chunk_size: Size of chunks for streaming processing
            max_workers: Maximum number of parallel workers

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
        if not data_types:
            data_types = self.get_data_types_to_process()

        if not data_types:
            logger.warning("No data types found to process")
            return {"error": "No data types to process"}

        logger.info(f"Available data types: {data_types}")

        # Set reasonable defaults for parallel processing
        if max_workers is None:
            max_workers = min(len(data_types), multiprocessing.cpu_count())

        stats = {
            "total_data_types": len(data_types),
            "data_types_processed": 0,
            "total_records_processed": 0,
            "total_errors": 0,
            "start_time": datetime.now(UTC),
            "resume_enabled": resume,
            "rerun_enabled": rerun,
        }

        try:
            if resume and not rerun:
                # Determine which data types need which phases of processing
                incomplete_phases = self.get_incomplete_data_types_for_resume(
                    data_types
                )
                self.log_resume_status(incomplete_phases)

                # Process cleaning phase with only data types that need it
                if incomplete_phases["cleaning"]:
                    logger.info(
                        f"Processing cleaning phase for {len(incomplete_phases['cleaning'])} data types"
                    )
                    cleaning_results = await self._parallel_process_data_types(
                        incomplete_phases["cleaning"],
                        chunk_size,
                        max_workers,
                        rerun,
                        "cleaning",
                    )

                    # Aggregate cleaning results
                    for _data_type, result in cleaning_results.items():
                        if result.get("success", False):
                            stats["data_types_processed"] += 1
                        stats["total_records_processed"] += result.get(
                            "records_processed", 0
                        )
                        stats["total_errors"] += result.get("errors", 0)

                # Process post-processing phase with only data types that need it
                if incomplete_phases["post_processing"]:
                    logger.info(
                        f"Processing post-processing phase for {len(incomplete_phases['post_processing'])} data types"
                    )
                    post_results = await self._run_post_processing(
                        incomplete_phases["post_processing"]
                    )
                    stats["post_processing"] = post_results

            else:
                # Traditional processing without checkpoints (rerun mode)
                logger.info("Processing all data types without checkpoint resume")
                processing_results = await self._parallel_process_data_types(
                    data_types, chunk_size, max_workers, rerun, "cleaning"
                )

                # Aggregate results
                for _data_type, result in processing_results.items():
                    if result.get("success", False):
                        stats["data_types_processed"] += 1
                    stats["total_records_processed"] += result.get(
                        "records_processed", 0
                    )
                    stats["total_errors"] += result.get("errors", 0)

                # Run post-processing if needed
                post_processing_types = self.get_post_processing_data_types()
                if post_processing_types:
                    logger.info(f"Running post-processing for: {post_processing_types}")
                    post_results = await self._run_post_processing(
                        post_processing_types
                    )
                    stats["post_processing"] = post_results

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

    async def _parallel_process_data_types(
        self,
        data_types: list[str],
        chunk_size: int,
        max_workers: int,
        rerun: bool,
        phase: str = "cleaning",
    ) -> dict[str, dict[str, Any]]:
        """Process multiple data types in parallel with checkpoint tracking."""
        semaphore = asyncio.Semaphore(max_workers)

        async def process_data_type_with_semaphore(data_type):
            async with semaphore:
                try:
                    # Check if this data type's phase is already processed (unless rerun)
                    if not rerun and self.progress_tracker:
                        processing_phase = (
                            ProcessingPhase.MAIN_ITEMS
                            if phase == "cleaning"
                            else ProcessingPhase.RELATED_ENTITIES
                        )
                        field_name = phase

                        if self.progress_tracker.should_skip_item(
                            data_type, processing_phase, field_name=field_name
                        ):
                            logger.debug(
                                f"Skipping {phase} for already processed data type {data_type}"
                            )
                            return data_type, {
                                "success": True,
                                "records_processed": 0,
                                "errors": 0,
                                "skipped": True,
                            }

                    result = await self._process_single_data_type(
                        data_type, chunk_size, rerun, phase
                    )
                    result["success"] = True

                    # Mark data type phase as complete if successful
                    if (
                        not rerun
                        and self.progress_tracker
                        and result.get("records_processed", 0) > 0
                    ):
                        processing_phase = (
                            ProcessingPhase.MAIN_ITEMS
                            if phase == "cleaning"
                            else ProcessingPhase.RELATED_ENTITIES
                        )
                        field_name = phase

                        self.progress_tracker.set_processing_phase(
                            processing_phase, current_field=field_name
                        )
                        self.progress_tracker.increment_processed(
                            data_type, processing_phase, field_name=field_name
                        )
                        logger.debug(
                            f"Marked {phase} complete for data type {data_type}"
                        )

                    return data_type, result
                except Exception as e:
                    logger.error(f"Error processing data type {data_type}: {e}")
                    return data_type, {
                        "success": False,
                        "error": str(e),
                        "records_processed": 0,
                        "errors": 1,
                    }

        tasks = [process_data_type_with_semaphore(dt) for dt in data_types]
        results = await asyncio.gather(*tasks)

        return dict(results)

    async def _process_single_data_type(
        self, data_type: str, chunk_size: int, rerun: bool
    ) -> dict[str, Any]:
        """Process a single data type with streaming (legacy method)."""
        return await self._process_single_data_type(
            data_type, chunk_size, rerun, "cleaning"
        )

    async def _process_single_data_type(
        self, data_type: str, chunk_size: int, rerun: bool, phase: str = "cleaning"
    ) -> dict[str, Any]:
        """Process a single data type with streaming and checkpoint support."""
        logger.info(f"Processing data type: {data_type} (phase: {phase})")

        stats = {
            "records_processed": 0,
            "chunks_processed": 0,
            "errors": 0,
            "start_time": datetime.now(UTC),
            "phase": phase,
        }

        try:
            # Process data in streaming chunks
            async for chunk in self._stream_data_type_chunks(data_type, chunk_size):
                chunk_result = await self._process_chunk(chunk, data_type, rerun, phase)

                stats["records_processed"] += chunk_result.get("records_processed", 0)
                stats["errors"] += chunk_result.get("errors", 0)
                stats["chunks_processed"] += 1

                # Update progress if tracker available
                if self.progress_tracker:
                    self.progress_tracker.update_progress(
                        processed_items=stats["records_processed"]
                    )

        except Exception as e:
            logger.error(f"Error in data type {data_type}: {e}")
            stats["errors"] += 1
            raise

        stats["end_time"] = datetime.now(UTC)
        stats["duration"] = (stats["end_time"] - stats["start_time"]).total_seconds()

        logger.info(f"Completed {data_type} (phase: {phase}): {stats}")
        return stats

    async def _stream_data_type_chunks(
        self, data_type: str, chunk_size: int
    ) -> AsyncGenerator[list[dict[str, Any]], None]:
        """Stream data type records in chunks for memory efficiency."""
        if data_type in self.multi_table_data_types:
            # Use multi-table streaming for complex data types
            async for chunk in self._stream_multi_table_chunks(data_type, chunk_size):
                yield chunk
        else:
            # Standard single-table streaming
            async with self.db_pool.acquire() as conn:
                # First check if table exists
                table_exists = await conn.fetchval(
                    """
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_schema = $1 AND table_name = $2
                    )
                    """,
                    self.staging_schema,
                    data_type,
                )

                if not table_exists:
                    logger.warning(
                        f"Table {self.staging_schema}.{data_type} does not exist"
                    )
                    return

                # Get available columns to determine best ordering
                column_query = """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = $1 AND table_name = $2
                    ORDER BY ordinal_position
                """
                columns = await conn.fetch(column_query, self.staging_schema, data_type)
                column_names = [col["column_name"] for col in columns]

                # Choose ordering column (prefer processed_at, then id, then first column)
                order_column = None
                for preferred_col in ["processed_at", "id"]:
                    if preferred_col in column_names:
                        order_column = preferred_col
                        break

                if not order_column and column_names:
                    order_column = column_names[0]

                if not order_column:
                    logger.error(
                        f"No columns found in table {self.staging_schema}.{data_type}"
                    )
                    return

                offset = 0
                while True:
                    query = f"""
                        SELECT * FROM {self.staging_schema}.{data_type}
                        ORDER BY {order_column}
                        LIMIT $1 OFFSET $2
                    """

                    try:
                        rows = await conn.fetch(query, chunk_size, offset)
                        if not rows:
                            break

                        chunk = [dict(row) for row in rows]
                        yield chunk

                        offset += len(chunk)

                        # If we got fewer rows than requested, we're at the end
                        if len(chunk) < chunk_size:
                            break

                    except Exception as e:
                        logger.error(
                            f"Error streaming {data_type} at offset {offset}: {e}"
                        )
                        break

    async def _process_chunk(
        self, chunk: list[dict[str, Any]], data_type: str, rerun: bool
    ) -> dict[str, Any]:
        """Process a chunk of records (legacy method)."""
        return await self._process_chunk(chunk, data_type, rerun, "cleaning")

    async def _process_chunk(
        self,
        chunk: list[dict[str, Any]],
        data_type: str,
        rerun: bool,
        phase: str = "cleaning",
    ) -> dict[str, Any]:
        """Process a chunk of records with checkpoint support."""
        stats = {"records_processed": 0, "errors": 0, "skipped": 0}

        if not chunk:
            return stats

        # Filter out already processed records (unless rerun)
        records_to_process = []

        for record in chunk:
            record_id = self._get_record_id(record, data_type)

            if (
                not rerun
                and self.progress_tracker
                and self.progress_tracker.should_skip_item(
                    f"{data_type}:{record_id}",
                    ProcessingPhase.MAIN_ITEMS,
                    field_name="record_cleaning",
                )
            ):
                logger.debug(
                    f"Skipping already processed record {record_id} in {data_type}"
                )
                stats["skipped"] += 1
                continue

            records_to_process.append(record)

        logger.debug(
            f"Processing {len(records_to_process)} records in chunk (skipped {stats['skipped']})"
        )

        # Clean records that need processing and group by target table
        records_by_table = {}  # (target_table, original_data_type) -> list of cleaned records
        for record in records_to_process:
            try:
                record_id = self._get_record_id(record, data_type)

                # Set current processing phase
                if self.progress_tracker:
                    self.progress_tracker.set_processing_phase(
                        ProcessingPhase.MAIN_ITEMS, current_field="record_cleaning"
                    )

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

                # Mark record as processed
                if not rerun and self.progress_tracker:
                    self.progress_tracker.increment_processed(
                        f"{data_type}:{record_id}",
                        ProcessingPhase.MAIN_ITEMS,
                        field_name="record_cleaning",
                    )
                    logger.debug(f"Marked record {record_id} as processed")

            except Exception as e:
                logger.error(
                    f"Error cleaning record {record.get('id', 'UNKNOWN')}: {e}"
                )
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

    def _clean_single_generic(
        self, record_data: dict[str, Any], data_type: str
    ) -> dict[str, Any]:
        """
        Generic record cleaning method.

        Override this method or implement data type-specific methods
        to customize cleaning behavior.
        """
        cleaned_record = record_data.copy()

        # Apply generic cleaning transformations
        for key, value in cleaned_record.items():
            if isinstance(value, str):
                # Clean long text fields
                if len(value) > 100:
                    cleaned_record[key] = self.clean_long_text(value)

                # Standardize chamber values
                if "chamber" in key.lower():
                    cleaned_record[key] = self.standardize_chamber(value)

                # Clean whitespace
                cleaned_record[key] = value.strip() if value else None

            elif "date" in key.lower() and value:
                # Standardize date fields
                cleaned_record[key] = self.standardize_date(value)

            elif key.endswith("_number") or "number" in key.lower():
                # Standardize numeric fields
                cleaned_record[key] = self.safe_int(value)

        return cleaned_record

    async def _store_cleaned_records_batch(
        self,
        conn: asyncpg.Connection,
        cleaned_records: list[dict[str, Any]],
        data_type: str,
        rerun: bool = False,
        original_data_type: str | None = None,
    ) -> int:
        """Store batch of cleaned records to production schema."""
        if not cleaned_records:
            return 0

        # Ensure production schema exists
        await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self.production_schema}")

        # Ensure production table exists
        await self._ensure_production_table_exists(
            conn, data_type, cleaned_records[0], original_data_type
        )

        # Determine primary key column(s) using original data type for config lookup
        config_lookup_type = original_data_type or data_type
        primary_key_columns = await self._get_primary_key_columns(
            conn, config_lookup_type, cleaned_records[0]
        )

        stored_count = 0

        for record in cleaned_records:
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
                            INSERT INTO {self.production_schema}.{data_type} ({columns_str})
                            VALUES ({placeholders})
                            ON CONFLICT ({primary_key_str}) DO UPDATE SET {update_clauses}
                        """
                    else:
                        insert_sql = f"""
                            INSERT INTO {self.production_schema}.{data_type} ({columns_str})
                            VALUES ({placeholders})
                            ON CONFLICT ({primary_key_str}) DO NOTHING
                        """
                else:
                    # No primary key available – simple insert (may create duplicates)
                    insert_sql = f"""
                        INSERT INTO {self.production_schema}.{data_type} ({columns_str})
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
                        f"{data_type[:-1]}_id",
                        record.get(list(record.keys())[0] if record else "UNKNOWN"),
                    )
                logger.error(f"Error storing record {record_id} in {data_type}: {e}")

        return stored_count

    async def _ensure_production_table_exists(
        self,
        conn: asyncpg.Connection,
        data_type: str,
        sample_record: dict[str, Any],
        original_data_type: str | None = None,
    ) -> None:
        """Ensure production table exists with proper schema."""
        # Check if table exists
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

        if table_exists:
            # Add any missing columns
            await self._add_missing_production_columns(conn, data_type, sample_record)
        else:
            # Create new table
            await self._create_production_table(
                conn, data_type, sample_record, original_data_type
            )

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
        """Get existing column names for a production table."""
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

    async def _run_post_processing(self, data_types: list[str]) -> dict[str, Any]:
        """Run post-processing operations for specified data types with checkpoint support."""
        post_processing_stats = {}

        for data_type in data_types:
            method_name = f"_post_process_{data_type}"
            if hasattr(self, method_name):
                try:
                    # Check if post-processing is already completed (unless rerun)
                    if self.progress_tracker and self.progress_tracker.should_skip_item(
                        data_type,
                        ProcessingPhase.RELATED_ENTITIES,
                        field_name="post_processing",
                    ):
                        logger.debug(
                            f"Skipping already completed post-processing for {data_type}"
                        )
                        post_processing_stats[data_type] = {
                            "success": True,
                            "result": "skipped - already completed",
                            "skipped": True,
                        }
                        continue

                    # Set current processing phase
                    if self.progress_tracker:
                        self.progress_tracker.set_processing_phase(
                            ProcessingPhase.RELATED_ENTITIES,
                            current_field="post_processing",
                        )

                    logger.info(f"Running post-processing for {data_type}")
                    result = await getattr(self, method_name)()

                    post_processing_stats[data_type] = {
                        "success": True,
                        "result": result,
                    }

                    # Mark post-processing as complete
                    if self.progress_tracker:
                        self.progress_tracker.increment_processed(
                            data_type,
                            ProcessingPhase.RELATED_ENTITIES,
                            field_name="post_processing",
                        )
                        logger.debug(f"Marked post-processing complete for {data_type}")

                except Exception as e:
                    logger.error(f"Post-processing failed for {data_type}: {e}")
                    post_processing_stats[data_type] = {
                        "success": False,
                        "error": str(e),
                    }
            else:
                logger.debug(f"No post-processing method found for {data_type}")

        return post_processing_stats

    async def _stream_multi_table_chunks(
        self, data_type: str, chunk_size: int
    ) -> AsyncGenerator[list[dict[str, Any]], None]:
        """Stream data for data types that span multiple tables."""
        # This method handles complex data types that require joining multiple tables
        # Implementation depends on the specific multi-table configuration

        if data_type not in self.multi_table_data_types:
            return

        # Check if the subclass has a specific method for this data type
        specific_method_name = f"_stream_{data_type}_joined_chunks"
        if hasattr(self, specific_method_name):
            logger.debug(f"Using specific streaming method for {data_type}")
            specific_method = getattr(self, specific_method_name)
            async for chunk in specific_method(chunk_size):
                yield chunk
        else:
            # Default fallback: stream from the first table in the list
            # This avoids the recursion issue by directly querying the table
            table_list = self.multi_table_data_types[data_type]
            main_table = table_list[0] if table_list else data_type

            logger.debug(
                f"Using default multi-table streaming for {data_type} from table {main_table}"
            )

            # Direct table streaming to avoid recursion
            async with self.db_pool.acquire() as conn:
                offset = 0
                while True:
                    # Check if table exists first
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
                        break

                    # Stream from main table
                    query = f"""
                        SELECT * FROM {self.staging_schema}.{main_table}
                        LIMIT $1 OFFSET $2
                    """

                    try:
                        rows = await conn.fetch(query, chunk_size, offset)
                        if not rows:
                            break

                        chunk = [dict(row) for row in rows]
                        yield chunk

                        offset += len(chunk)

                        # If we got fewer rows than requested, we're at the end
                        if len(chunk) < chunk_size:
                            break

                    except Exception as e:
                        logger.error(
                            f"Error streaming {main_table} at offset {offset}: {e}"
                        )
                        break

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

        # Find the config for this data type
        for config in self._configs:
            config_table_name = self._get_config_table_name(config)
            if config_table_name == data_type:
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
                            f"Using primary keys from config for {data_type}: {valid_id_fields}"
                        )
                        return valid_id_fields

        # No primary key found
        logger.warning(
            f"No primary key found for {data_type}, table will not have UPSERT capability"
        )
        return []

    # -----------------------------------------------------------------------------
    # Table-splitting helper
    # -----------------------------------------------------------------------------
    async def split_columns_to_new_table(
        self,
        conn: asyncpg.Connection,
        source_table: str,
        dest_table: str,
        columns: list[str] | dict[str, str],
        *,
        src_schema: str | None = None,
        dst_schema: str | None = None,
        create_if_missing: bool = True,
        drop_from_source: bool = False,
    ) -> int:
        """Move columns from *source_table* → *dest_table* (optionally renaming them).

        ``columns`` can be either:

        • a *list* of column names – they are copied verbatim; or
        • a *dict* mapping ``{source_col: dest_col}`` when the destination column
          name should differ from the source column name.

        Behaviour remains the same: create destination if requested, copy the data,
        and optionally drop the original columns.
        """
        src_schema = src_schema or self.staging_schema
        dst_schema = dst_schema or src_schema

        # Normalise columns parameter ------------------------------------------------
        if isinstance(columns, dict):
            src_cols = list(columns.keys())
            dest_cols = list(columns.values())
            col_pairs = list(columns.items())  # preserve order
        else:
            src_cols = columns
            dest_cols = columns
            col_pairs = [(c, c) for c in src_cols]

        # ------------------------------------------------------------------
        # Discover primary key(s) of the source table so we can carry them over
        # ------------------------------------------------------------------
        pk_cols = await conn.fetch(
            """
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = $1::regclass AND i.indisprimary
            ORDER BY a.attnum
            """,
            f"{src_schema}.{source_table}",
        )
        primary_keys = [r["attname"] for r in pk_cols] or []

        # ------------------------------------------------------------------
        # Determine which of the requested source columns actually exist
        # ------------------------------------------------------------------
        existing_cols_rows = await conn.fetch(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = $1 AND table_name = $2
            """,
            src_schema,
            source_table,
        )
        existing_src_cols = {r["column_name"] for r in existing_cols_rows}

        # ------------------------------------------------------------------
        # Abort early if none of the requested columns exist in the source
        # ------------------------------------------------------------------
        if not any(col in existing_src_cols for col in src_cols):
            logger.info(
                f"None of the requested columns {src_cols} exist in {src_schema}.{source_table}; skipping split into {dest_table}"
            )
            return 0

        # Build column lists for INSERT
        dest_insert_cols = primary_keys + dest_cols

        # Use a source alias so columns that also exist in the destination
        # table are unambiguously referenced.
        src_alias = "src"

        select_exprs: list[str] = [f"{src_alias}.{pk}" for pk in primary_keys]

        # For each requested column, if it's present use it, otherwise insert NULL
        # cast as TEXT so the SELECT list aligns with destination columns.
        for src_col, dest_col in col_pairs:
            if src_col in existing_src_cols:
                select_exprs.append(f"{src_alias}.{src_col}")
            else:
                # We already know not all are missing, but some might be. Keep NULL placeholder.
                logger.warning(
                    f"Column '{src_col}' not found in {src_schema}.{source_table}; using NULL for '{dest_col}'"
                )
                select_exprs.append(f"NULL::text AS {dest_col}")

        dest_cols_sql = ", ".join(dest_insert_cols)
        select_cols_sql = ", ".join(select_exprs)

        # Build a WHERE clause so we only bring across rows where at least one
        # of the (existing) source columns is non-NULL. This prevents inserting
        # entirely NULL link records that violate NOT-NULL constraints or are
        # simply useless.
        non_null_conds = [
            f"{src_alias}.{col} IS NOT NULL"
            for col in src_cols
            if col in existing_src_cols
        ]
        where_clause = f" WHERE {' OR '.join(non_null_conds)}" if non_null_conds else ""

        # ------------------------------------------------------------------
        # Create destination table if requested / missing
        # ------------------------------------------------------------------
        if create_if_missing:
            existing = await conn.fetchval(
                """
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = $1 AND table_name = $2
                )""",
                dst_schema,
                dest_table,
            )

            if not existing:
                # Create the table fresh
                col_defs = ", ".join(f"{c} TEXT" for c in dest_insert_cols)
                pk_clause = (
                    f", PRIMARY KEY ({', '.join(primary_keys)})" if primary_keys else ""
                )
                await conn.execute(
                    f"CREATE TABLE {dst_schema}.{dest_table} ({col_defs}{pk_clause})"
                )
                logger.info(f"Created destination table {dst_schema}.{dest_table}")
            else:
                # Ensure all expected columns exist
                existing_cols_set = {
                    r["column_name"]
                    for r in await conn.fetch(
                        """
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_schema = $1 AND table_name = $2
                        """,
                        dst_schema,
                        dest_table,
                    )
                }

                for col in dest_insert_cols:
                    if col not in existing_cols_set:
                        await conn.execute(
                            f"ALTER TABLE {dst_schema}.{dest_table} ADD COLUMN {col} TEXT"
                        )
                        logger.info(
                            f"Added missing column '{col}' to {dst_schema}.{dest_table}"
                        )

        # ------------------------------------------------------------------
        # Insert the data
        # ------------------------------------------------------------------
        insert_sql = (
            f"INSERT INTO {dst_schema}.{dest_table} ({dest_cols_sql}) "
            f"SELECT {select_cols_sql} FROM {src_schema}.{source_table} AS {src_alias}{where_clause} "
            f"ON CONFLICT DO NOTHING"
        )
        status = await conn.execute(insert_sql)  # e.g. 'INSERT 0 123'
        inserted = int(status.split()[-1]) if status.startswith("INSERT") else 0

        # ------------------------------------------------------------------
        # Optionally drop the columns from the source table
        # ------------------------------------------------------------------
        if drop_from_source and src_cols:
            for col in src_cols:
                try:
                    # Use CASCADE so that dependent indexes or constraints do not block the drop
                    await conn.execute(
                        f"ALTER TABLE {src_schema}.{source_table} "
                        f"DROP COLUMN IF EXISTS {col} CASCADE"
                    )
                except Exception as exc:
                    logger.warning(
                        f"Failed to drop column {col} from {src_schema}.{source_table}: {exc}"
                    )

        return inserted

    def _register_target_table_override(self, target_table: str) -> None:
        """Register a target table override for the current record being cleaned."""
        self._target_table_override = target_table

    def _clear_target_table_override(self) -> None:
        """Clear any target table override."""
        self._target_table_override = None
