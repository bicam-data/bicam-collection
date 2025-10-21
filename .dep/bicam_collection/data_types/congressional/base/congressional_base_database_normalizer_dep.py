"""
Congressional-specific base database normalizer.

This module provides Congressional-specific implementations including:
- Congressional schema defaults (bicam_staging_congressional, bicam_raw_congressional)
- Congressional progress tracking configuration
- Congressional-specific normalization patterns
- Phase-specific checkpointing for resumable processing
"""

import asyncio
import hashlib
import io
import json
import logging
import uuid
from datetime import UTC, datetime
from typing import Any, Literal

import asyncpg

from bicam_collection.libs.data_type_registry import get_global_registry

from ....libs.batch_accumulator import BatchAccumulator
from ....libs.checkpoint import (
    HierarchicalProgressTracker,
    ProcessingPhase,
    ProcessingStage,
)
from ....libs.record_processing_context import RecordProcessingContext
from ....libs.run_tracking import RunMetadata, RunType
from ...abstract import AbstractDatabaseNormalizer

logger = logging.getLogger(__name__)


class CongressionalBaseDatabaseNormalizer(AbstractDatabaseNormalizer):
    """
    Congressional-specific base normalizer.

    Provides Congressional schema defaults and Congressional-specific
    progress tracking configuration with phase-specific checkpointing.
    """

    # Class-level schema cache to avoid repeated table operations
    _table_schema_cache: dict[str, bool] = {}
    _cache_lock = asyncio.Lock()

    # Class-level batch accumulator for true bulk operations
    _batch_accumulator: BatchAccumulator | None = None
    _accumulator_lock = asyncio.Lock()

    def __init__(self, **kwargs):
        # Provide Congressional-specific schema defaults
        kwargs.setdefault("target_schema", "bicam_staging_congressional")
        kwargs.setdefault("source_schema", "bicam_raw_congressional")
        super().__init__(**kwargs)
        self.main_config = get_global_registry().get_data_type_config(
            self.data_type_name
        )
        self.main_table_name = self.main_config.table_name
        self.main_id_field = self.main_config.id_field
        self.main_expected_key = self.main_config.api.expected_key

        # Initialize class-level batch accumulator if needed
        if CongressionalBaseDatabaseNormalizer._batch_accumulator is None:
            CongressionalBaseDatabaseNormalizer._batch_accumulator = BatchAccumulator(
                max_batch_size=1000,
                max_memory_mb=200,  # Increased for better bulk performance
            )

    def setup_progress_tracker(
        self, checkpoint_manager=None
    ) -> HierarchicalProgressTracker | None:
        """Congressional-specific progress tracker setup."""
        if checkpoint_manager:
            self.checkpoint_manager = checkpoint_manager

        if not self.checkpoint_manager or not self.data_type_name:
            logger.warning("Cannot setup progress tracker - missing dependencies")
            return None

        self.progress_tracker = HierarchicalProgressTracker.create_for_stage(
            self.checkpoint_manager,
            "congressional",  # Congressional-specific system name
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
                description=f"{self.data_type_name} data normalization to staging",
                data_types=[self.data_type_name] if self.data_type_name else [],
            )

        self.current_run_id = self.run_manager.create_run(run_metadata)
        await self.run_manager.start_run(self.current_run_id)
        return self.current_run_id

    def _get_main_id_configs(self) -> tuple[str, str, str]:
        """Get the main ID field name from configuration."""
        if self.main_config:
            if len(self.main_config.id_fields) > 1:
                raise ValueError(f"Multiple id fields found for {self.data_type_name}")
            elif len(self.main_config.id_fields) == 1:
                id_field = self.main_config.id_fields[0]
            else:
                raise ValueError(f"No id fields found for {self.data_type_name}")

            main_table_name = self.main_config.table_name
            expected_key = self.main_config.expected_key
            return main_table_name, id_field, expected_key

    def _get_related_tables_with_raw_data(self) -> list[str]:
        """Get related table suffixes from config."""
        return self.main_config.related_tables

    # =============================================================================
    # BATCH ACCUMULATION METHODS
    # =============================================================================

    async def _store_all_table_records_batched(
        self, all_table_records: dict[str, list[dict[str, Any]]], record_type: str
    ) -> dict[str, Any]:
        """Store records using batch accumulator for better performance."""
        if not all_table_records:
            return {"tables_updated": 0, "records_stored": 0}

        async with CongressionalBaseDatabaseNormalizer._accumulator_lock:
            accumulator = CongressionalBaseDatabaseNormalizer._batch_accumulator

            # Add records to accumulator
            accumulator.add_records(all_table_records, record_type)

            # Check if we should flush
            if accumulator.should_flush():
                return await self._flush_batch_accumulator(force=False)
            else:
                # Return stats for the records we added (even though not yet flushed)
                total_records = sum(
                    len(records) for records in all_table_records.values()
                )
                return {
                    "tables_updated": len(all_table_records),
                    "records_stored": total_records,
                }

    async def _flush_batch_accumulator(self, force: bool = False) -> dict[str, Any]:
        """Flush the batch accumulator to database."""
        accumulator = CongressionalBaseDatabaseNormalizer._batch_accumulator

        if accumulator.is_empty():
            return {"tables_updated": 0, "records_stored": 0}

        if not force and not accumulator.should_flush():
            return {"tables_updated": 0, "records_stored": 0}

        # Get accumulated records
        all_table_records, record_types_by_table = accumulator.get_and_clear()

        if not all_table_records:
            return {"tables_updated": 0, "records_stored": 0}

        logger.info(
            f"Flushing batch accumulator with {sum(len(records) for records in all_table_records.values())} records across {len(all_table_records)} tables"
        )

        # Perform bulk operations
        return await self._store_all_table_records_bulk(
            all_table_records, record_types_by_table
        )

    async def _store_all_table_records_bulk(
        self,
        all_table_records: dict[str, list[dict[str, Any]]],
        record_types_by_table: dict[str, str],
    ) -> dict[str, Any]:
        """Perform true bulk database operations with minimal schema overhead."""
        stats = {"tables_updated": 0, "records_stored": 0}

        total_records = sum(len(records) for records in all_table_records.values())
        logger.info(
            f"Bulk storing {total_records} records across {len(all_table_records)} tables"
        )

        # Use a single connection for all operations to minimize overhead
        async with self.db_pool.acquire() as conn:
            # Group schema operations together - check ALL records for complete schema
            for table_name, records in all_table_records.items():
                if records:
                    record_type = record_types_by_table.get(table_name, "main")

                    # Create a combined sample record with ALL columns from ALL records
                    # This ensures schema validation catches all columns in the batch
                    combined_sample = {}
                    for record in records:
                        combined_sample.update(record)

                    logger.debug(
                        f"Schema validation for {table_name} with {len(combined_sample)} columns from {len(records)} records"
                    )

                    await self._ensure_table_with_schema_using_conn(
                        conn, table_name, combined_sample, record_type
                    )
            # Perform all bulk inserts
            for table_name, records in all_table_records.items():
                if not records:
                    continue

                record_type = record_types_by_table.get(table_name, "main")

                try:
                    stored_count = await self._store_table_records_using_conn(
                        conn, table_name, records, record_type, update_strategy="UPDATE"
                    )

                    stats["tables_updated"] += 1
                    stats["records_stored"] += stored_count

                    logger.debug(f"Bulk stored {stored_count} records in {table_name}")

                except Exception as e:
                    logger.error(
                        f"Error bulk storing {len(records)} records for {table_name}: {e}"
                    )
                    raise
        logger.info(f"Bulk stored {stats['records_stored']} records.")

        return stats

    async def flush_remaining_batches(self) -> dict[str, Any]:
        """Flush any remaining records in the batch accumulator."""
        async with CongressionalBaseDatabaseNormalizer._accumulator_lock:
            return await self._flush_batch_accumulator(force=True)

    # =============================================================================
    # CHECKPOINT MANAGEMENT
    # =============================================================================

    def get_incomplete_items_for_resume(
        self, available_item_ids: list[str]
    ) -> dict[str, list[str]]:
        """
        Identify items that need specific phases of processing for resume operations.

        Args:
            available_item_ids: List of item IDs that are available for processing

        Returns:
            Dictionary mapping phase names to lists of item IDs that need that phase:
            {
                "main_data": ["item1", "item2"],      # Items missing main data processing
                "related_data": ["item3", "item4"],   # Items missing related data processing
                "nested_data": ["item5", "item6"]     # Items missing nested data processing
            }
        """
        if not self.progress_tracker:
            # If no progress tracker, assume all items need all phases
            return {
                "main_data": available_item_ids.copy(),
                "related_data": available_item_ids.copy(),
                "nested_data": available_item_ids.copy(),
            }

        incomplete_phases = {"main_data": [], "related_data": [], "nested_data": []}

        for item_id in available_item_ids:
            # Check main data processing phase
            if not self.progress_tracker.should_skip_item(
                item_id, ProcessingPhase.MAIN_ITEMS, field_name="main_data"
            ):
                incomplete_phases["main_data"].append(item_id)

            # Check related data processing phase
            if not self.progress_tracker.should_skip_item(
                item_id, ProcessingPhase.RELATED_ENTITIES, field_name="related_data"
            ):
                incomplete_phases["related_data"].append(item_id)

            # Check nested data processing phase (use RELATED_ENTITIES with different field name)
            if not self.progress_tracker.should_skip_item(
                item_id, ProcessingPhase.RELATED_ENTITIES, field_name="nested_data"
            ):
                incomplete_phases["nested_data"].append(item_id)

        return incomplete_phases

    def log_resume_status(self, incomplete_phases: dict[str, list[str]]) -> None:
        """
        Log the resume status showing what phases need processing.

        Args:
            incomplete_phases: Dictionary from get_incomplete_items_for_resume()
        """
        total_main = len(incomplete_phases["main_data"])
        total_related = len(incomplete_phases["related_data"])
        total_nested = len(incomplete_phases["nested_data"])

        logger.info(f"Resume status for {self.data_type_name} normalization:")
        logger.info(f"  Items needing main data processing: {total_main}")
        logger.info(f"  Items needing related data processing: {total_related}")
        logger.info(f"  Items needing nested data processing: {total_nested}")

        if total_main == 0 and total_related == 0 and total_nested == 0:
            logger.info("  All items are fully processed!")
        else:
            logger.info(
                f"  Resume will process up to {max(total_main, total_related, total_nested)} items with missing phases"
            )

    # =============================================================================
    # MAIN PROCESSING METHODS
    # =============================================================================

    async def process_items(
        self,
        item_ids: list[str],
        batch_id: str | None = None,
        rerun: bool = False,
        batch_size: int = 200,  # Increased for better bulk insert efficiency
        max_concurrent: int = 20,  # Increased for better parallelization
        resume: bool = True,
    ) -> dict[str, Any]:
        """
        Main processing method that normalizes raw data to staging tables.

        Args:
            item_ids: List of item IDs to process
            batch_id: Batch identifier for tracking
            rerun: Whether to reprocess existing records (ignores checkpoints)
            batch_size: Size of processing batches
            max_concurrent: Maximum concurrent operations
            resume: Whether to use checkpoint-based resume logic

        Returns:
            Processing statistics
        """
        logger.info(
            f"Starting normalization for {len(item_ids)} {self.data_type_name} items"
            + (f" (batch: {batch_id})" if batch_id else "")
            + (f" (resume: {resume})" if resume else "")
            + (f" (rerun: {rerun})" if rerun else "")
        )

        # Setup progress tracking
        self.setup_progress_tracker()

        # Setup run tracking
        await self.setup_run_tracking()

        stats = {
            "total_items": len(item_ids),
            "main_records_processed": 0,
            "related_records_processed": 0,
            "nested_records_processed": 0,
            "errors": 0,
            "start_time": datetime.now(UTC),
            "batch_id": batch_id,
            "resume_enabled": resume,
            "rerun_enabled": rerun,
        }

        try:
            if resume and not rerun:
                # Determine which items need which phases of processing
                incomplete_phases = self.get_incomplete_items_for_resume(item_ids)
                self.log_resume_status(incomplete_phases)

                # Process each phase with only the items that need it
                main_stats = await self._process_main_records(
                    incomplete_phases["main_data"], batch_size, max_concurrent
                )
                stats["main_records_processed"] = main_stats.get("records_processed", 0)
                stats["errors"] += main_stats.get("errors", 0)

                related_stats = await self._process_related_records(
                    incomplete_phases["related_data"], batch_size, max_concurrent
                )
                stats["related_records_processed"] = related_stats.get(
                    "records_processed", 0
                )
                stats["errors"] += related_stats.get("errors", 0)

                nested_stats = await self._process_nested_records(
                    incomplete_phases["nested_data"], batch_size, max_concurrent
                )
                stats["nested_records_processed"] = nested_stats.get(
                    "records_processed", 0
                )
                stats["errors"] += nested_stats.get("errors", 0)

            else:
                # Traditional processing without checkpoints (rerun mode)
                main_stats = await self._process_main_records(
                    item_ids, batch_size, max_concurrent, rerun
                )
                stats.update(main_stats)

                related_stats = await self._process_related_records(
                    item_ids, batch_size, max_concurrent, rerun
                )
                stats["related_records_processed"] = related_stats.get(
                    "related_records_processed", 0
                )
                stats["errors"] += related_stats.get("errors", 0)

        except Exception as e:
            logger.error(f"Normalization failed: {e}")
            if self.run_manager and self.current_run_id:
                await self.run_manager.fail_run(self.current_run_id, str(e))
            raise

        finally:
            # Flush any remaining batches before completing
            try:
                flush_stats = await self.flush_remaining_batches()
                if flush_stats.get("records_stored", 0) > 0:
                    logger.info(
                        f"Flushed {flush_stats['records_stored']} remaining batched records"
                    )
            except Exception as e:
                logger.error(f"Error flushing remaining batches: {e}")

            if self.run_manager and self.current_run_id:
                await self.run_manager.complete_run(self.current_run_id)

        stats["end_time"] = datetime.now(UTC)
        stats["duration"] = (stats["end_time"] - stats["start_time"]).total_seconds()

        logger.info(f"Normalization completed: {stats}")
        return stats

    async def _process_main_records(
        self, item_ids: list[str], batch_size: int, max_concurrent: int
    ) -> dict[str, Any]:
        """Process main records with checkpoint-based resume capability."""
        if not item_ids:
            logger.info("No main records need processing (all checkpointed)")
            return {"records_processed": 0, "errors": 0}

        logger.info(f"Processing {len(item_ids)} main records with checkpoints")

        raw_data = await self._get_raw_data_by_ids(item_ids)
        logger.info(f"Retrieved {len(raw_data)} raw records")
        filtered_data = []

        # Filter out already processed items using checkpoints
        for record in raw_data:
            payload = record.get("payload", {})
            if isinstance(payload, str):
                payload = json.loads(payload)

            record_id = payload.get(self.main_id_field)
            if not record_id:
                continue

            # Check if this item's main data is already processed
            if self.progress_tracker and self.progress_tracker.should_skip_item(
                record_id, ProcessingPhase.MAIN_ITEMS, field_name="main_data"
            ):
                logger.debug(
                    f"Skipping main data for already processed item {record_id}"
                )
                continue

            filtered_data.append(record)

        logger.info(
            f"After checkpoint filtering: {len(filtered_data)} main records to process"
        )

        # Profile actual processing
        result = await self._process_data_batch(
            filtered_data, "main", batch_size, max_concurrent
        )
        logger.info("Main records processing completed.")

        return result

    async def _process_related_records(
        self, item_ids: list[str], batch_size: int, max_concurrent: int
    ) -> dict[str, Any]:
        """Process related records with checkpoint-based resume capability."""
        if not item_ids:
            logger.info("No related records need processing (all checkpointed)")
            return {"records_processed": 0, "errors": 0}

        logger.info(
            f"Processing related records for {len(item_ids)} items with checkpoints"
        )

        related_tables = self._get_related_tables_with_raw_data()
        total_stats = {"records_processed": 0, "errors": 0}

        for table_suffix in related_tables:
            try:
                raw_data = await self._get_related_raw_data_by_ids(
                    item_ids, table_suffix
                )
                filtered_data = []
                if not raw_data:
                    logger.info(f"No raw data found for {table_suffix}")
                    continue

                # Filter out already processed items using checkpoints
                for record in raw_data:
                    payload = record.get("payload", {})
                    if isinstance(payload, str):
                        payload = json.loads(payload)

                    record_id = payload.get(self.main_id_field)
                    if not record_id:
                        continue

                    # Check if this item's related data is already processed
                    if self.progress_tracker and self.progress_tracker.should_skip_item(
                        record_id,
                        ProcessingPhase.RELATED_ENTITIES,
                        field_name=f"related_{table_suffix}",
                    ):
                        logger.debug(
                            f"Skipping {table_suffix} for already processed item {record_id}"
                        )
                        continue

                    filtered_data.append(record)

                if filtered_data:
                    logger.info(
                        f"Processing {len(filtered_data)} {table_suffix} records after checkpoint filtering"
                    )
                    stats = await self._process_data_batch(
                        filtered_data, table_suffix, batch_size, max_concurrent
                    )
                    total_stats["records_processed"] += stats.get(
                        "records_processed", 0
                    )
                    total_stats["errors"] += stats.get("errors", 0)

            except Exception as e:
                logger.error(f"Error processing {table_suffix} with checkpoints: {e}")
                total_stats["errors"] += 1

        return total_stats

    async def _process_nested_records(
        self, item_ids: list[str], batch_size: int, max_concurrent: int
    ) -> dict[str, Any]:
        """Process nested records with checkpoint-based resume capability."""
        if not item_ids:
            logger.info("No nested records need processing (all checkpointed)")
            return {"records_processed": 0, "errors": 0}

        logger.info(
            f"Processing nested records for {len(item_ids)} items with checkpoints"
        )

        # For nested processing, we need to reprocess main records to extract nested data
        # but only for items that haven't had their nested data processed
        raw_data = await self._get_raw_data_by_ids(item_ids)
        filtered_data = []

        for record in raw_data:
            payload = record.get("payload", {})
            if isinstance(payload, str):
                payload = json.loads(payload)

            record_id = payload.get(self.main_id_field)
            if not record_id:
                continue

            # Check if this item's nested data is already processed
            if self.progress_tracker and self.progress_tracker.should_skip_item(
                record_id, ProcessingPhase.RELATED_ENTITIES, field_name="nested_data"
            ):
                logger.debug(
                    f"Skipping nested data for already processed item {record_id}"
                )
                continue

            filtered_data.append(record)

        logger.info(
            f"After checkpoint filtering: {len(filtered_data)} nested records to process"
        )

        return await self._process_nested_data_batch(
            filtered_data, batch_size, max_concurrent
        )

    async def _process_data_batch(
        self,
        raw_data: list[dict[str, Any]],
        record_type: str,
        batch_size: int,
        max_concurrent: int,
    ) -> dict[str, Any]:
        """Process a batch of raw data records with checkpoint tracking."""
        if not raw_data:
            return {"records_processed": 0, "errors": 0}

        stats = {"records_processed": 0, "errors": 0}
        total_batches = (len(raw_data) + batch_size - 1) // batch_size

        logger.info(
            f"Processing {len(raw_data)} {record_type} records in {total_batches} batches of {batch_size}"
        )

        # Process in batches
        for i in range(0, len(raw_data), batch_size):
            batch = raw_data[i : i + batch_size]
            batch_num = (i // batch_size) + 1

            logger.info(
                f"Processing batch {batch_num}/{total_batches} ({len(batch)} records) for {record_type}"
            )

            batch_stats = await self._run_concurrent_batch(
                batch,
                lambda rec, rt=record_type: self._process_single_record(rec, rt),
                max_concurrent,
            )
            processed = batch_stats.get("records_processed", 0)
            errors = batch_stats.get("errors", 0)

            logger.info(
                f"Batch {batch_num}/{total_batches} completed: {processed} processed, {errors} errors"
            )

            stats["records_processed"] += processed
            stats["errors"] += errors

        logger.info(
            f"All {record_type} batches completed: {stats['records_processed']} total processed, {stats['errors']} total errors"
        )

        return stats

    async def _run_concurrent_batch(
        self,
        batch: list[Any],
        handler,
        max_concurrent: int,
    ) -> dict[str, int]:
        """Run *handler* over *batch* with a bounded semaphore.

        The *handler* coroutine is expected to return either ``Exception`` or a
        mapping that – if it contains ``{"success": True}`` – is considered a
        successful processing result.
        """
        semaphore = asyncio.Semaphore(max_concurrent)

        async def _wrapped(rec):
            async with semaphore:
                return await handler(rec)

        results = await asyncio.gather(
            *[_wrapped(r) for r in batch], return_exceptions=True
        )

        stats = {"records_processed": 0, "errors": 0}
        for res in results:
            if isinstance(res, Exception):
                logger.error(f"Concurrent record processing failed: {res}")
                stats["errors"] += 1
            elif isinstance(res, dict) and res.get("success"):
                stats["records_processed"] += 1
            else:
                stats["errors"] += 1
        return stats

    async def _process_nested_data_batch(
        self, raw_data: list[dict[str, Any]], batch_size: int, max_concurrent: int
    ) -> dict[str, Any]:
        """Process nested data extraction with checkpoint tracking."""
        if not raw_data:
            return {"records_processed": 0, "errors": 0}

        stats = {"records_processed": 0, "errors": 0}

        # Process in batches
        for i in range(0, len(raw_data), batch_size):
            batch = raw_data[i : i + batch_size]
            batch_stats = await self._run_concurrent_batch(
                batch, self._process_single_nested_record, max_concurrent
            )
            stats["records_processed"] += batch_stats.get("records_processed", 0)
            stats["errors"] += batch_stats.get("errors", 0)

        return stats

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
                # Store primitive values - convert to appropriate types for database
                if value is None:
                    flattened[new_key] = None
                elif isinstance(value, int | float | bool | str):
                    # Keep numeric & boolean values – they are safe for COPY/INSERT
                    flattened[new_key] = value
                else:
                    # Fallback – stringify complex types
                    flattened[new_key] = str(value)

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
        Extract list fields to separate table records with proper hierarchical foreign keys.

        Args:
            data: Source data containing lists
            parent_id: ID of parent record
            is_nested: Whether this is a nested extraction
            parent_table: Name of parent table for nested records (suffix only)
            root_id: Root record ID (main table ID) for deeply nested structures

        Returns:
            Dictionary mapping table names to record lists
        """
        extracted = {}

        for key, value in data.items():
            if not isinstance(value, list) or not value:
                continue

            # Use original key & parent table names (keep original casing)
            current_key = key
            current_parent = parent_table

            # Determine table name
            if is_nested and current_parent:
                table_name = f"{self.data_type_name}_{current_parent}_{current_key}"
            else:
                table_name = f"{self.data_type_name}_{current_key}"

            # Process list items – use enumerate so we can build deterministic, index-based IDs.
            # Rationale: content-hash based IDs may collide when list elements are structurally
            # similar (e.g. members' `terms`).  Using the item index guarantees uniqueness under
            # the same parent while still being deterministic for idempotent re-runs.
            records: list[dict[str, Any]] = []
            for item_idx, item in enumerate(value):
                if isinstance(item, dict):
                    # Flatten the item
                    flat_item = self._flatten_dict(item)

                    # ------------------------------------------------------------------
                    # 1.  Add hierarchical foreign keys
                    # ------------------------------------------------------------------
                    if is_nested and current_parent:
                        parent_id_field = f"{current_parent.split('_')[-1]}_id"
                        flat_item[parent_id_field] = parent_id

                        # Maintain pointer back to the root/main record
                        flat_item[self.main_id_field] = (
                            root_id if root_id else parent_id
                        )
                    else:
                        # List sits directly on the main record
                        flat_item[self.main_id_field] = parent_id

                    # ------------------------------------------------------------------
                    # 2.  Generate deterministic ID **including** item index
                    # ------------------------------------------------------------------
                    flat_item["id"] = f"{parent_id}_{current_key}_{item_idx}"

                    records.append(flat_item)

                    # Recursively extract nested lists from this item
                    # Pass the new item's ID as parent_id, current key as parent_table,
                    # and preserve the root_id (main table ID)
                    nested_extracted = self._extract_lists(
                        item,
                        flat_item["id"],  # Use the new item's ID as parent
                        is_nested=True,
                        parent_table=current_key,  # Current key becomes the parent table
                        root_id=root_id or parent_id,  # Preserve or establish root ID
                    )

                    # Merge nested extractions
                    for nested_table, nested_records in nested_extracted.items():
                        if nested_table not in extracted:
                            extracted[nested_table] = []
                        extracted[nested_table].extend(nested_records)

                else:
                    # Handle primitive list items
                    record = {
                        "value": str(item),
                    }

                    # Add ALL hierarchical foreign key relationships FIRST
                    if is_nested and current_parent:
                        parent_id_field = f"{current_parent.split('_')[-1]}_id"
                        record[parent_id_field] = parent_id

                        record[self.main_id_field] = root_id if root_id else parent_id
                    else:
                        record[self.main_id_field] = parent_id

                    # Deterministic ID based on index as well
                    record["id"] = f"{parent_id}_{current_key}_{item_idx}"

                    records.append(record)

            if records:
                extracted[table_name] = records

        return extracted

    # =============================================================================
    # STORAGE METHODS
    # =============================================================================

    async def _store_all_table_records(
        self, all_table_records: dict[str, list[dict[str, Any]]], record_type: str
    ) -> dict[str, Any]:
        """Store records for all tables using batch accumulator."""
        return await self._store_all_table_records_batched(
            all_table_records, record_type
        )

    async def _ensure_table_with_schema_using_conn(
        self,
        conn: asyncpg.Connection,
        table_name: str,
        sample_record: dict[str, Any],
        record_type: str,
    ) -> bool:
        """Ensure table exists with proper schema using advisory locks for concurrency safety."""
        await self._create_schema()

        cache_key = f"{self.target_schema}.{table_name.lower()}"

        # Check if table exists (cached)
        table_exists = False
        async with CongressionalBaseDatabaseNormalizer._cache_lock:
            if cache_key in CongressionalBaseDatabaseNormalizer._table_schema_cache:
                table_exists = True
                logger.debug(f"Table existence cached: {cache_key}")

        # Use a hash of the table name as the lock ID to ensure uniqueness
        lock_id = int(
            hashlib.md5(
                f"{self.target_schema}.{table_name.lower()}".encode()
            ).hexdigest()[:8],
            16,
        )

        # Use the provided connection directly (no additional context manager)
        # Acquire advisory lock for this specific table to prevent concurrent schema modifications
        try:
            lock_acquired = await conn.fetchval(
                "SELECT pg_try_advisory_lock($1)", lock_id
            )

            if not lock_acquired:
                # Another process is modifying this table, wait for it to finish
                logger.debug(f"Waiting for table schema lock: {table_name}")
                await conn.fetchval("SELECT pg_advisory_lock($1)", lock_id)
                logger.debug(f"Acquired table schema lock: {table_name}")

            # Check table existence if not cached
            if not table_exists:
                table_exists = await self._table_exists(conn, table_name.lower())

                # Cache table existence
                if table_exists:
                    async with CongressionalBaseDatabaseNormalizer._cache_lock:
                        CongressionalBaseDatabaseNormalizer._table_schema_cache[
                            cache_key
                        ] = True

            if not table_exists:
                result = await self._create_table(
                    conn, table_name.lower(), sample_record, record_type
                )
                logger.info(f"Created table {self.target_schema}.{table_name}")

                # Cache table existence after creation
                async with CongressionalBaseDatabaseNormalizer._cache_lock:
                    CongressionalBaseDatabaseNormalizer._table_schema_cache[
                        cache_key
                    ] = True

                return result
            else:
                logger.debug(
                    f"Table {self.target_schema}.{table_name} already exists, checking schema"
                )
                # ALWAYS check for missing columns (don't cache schema completeness)
                await self._add_missing_columns(conn, table_name.lower(), sample_record)

                # Ensure proper primary key constraint exists
                expected_pk_column = (
                    self.main_id_field if record_type == "main" else "id"
                )
                has_constraint = await self._check_constraint_exists(
                    conn, table_name.lower(), expected_pk_column
                )

                if not has_constraint:
                    await self._add_primary_key_constraint(
                        conn, table_name.lower(), expected_pk_column
                    )

                return True

        finally:
            # Always release the advisory lock
            try:
                await conn.fetchval("SELECT pg_advisory_unlock($1)", lock_id)
            except Exception as e:
                logger.warning(f"Failed to release advisory lock for {table_name}: {e}")

    async def _add_primary_key_constraint(
        self, conn, table_name: str, column_name: str
    ) -> None:
        """Add primary key constraint to existing table if possible."""
        try:
            # First check if the column exists
            column_exists = await conn.fetchval(
                """
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema = $1 AND table_name = $2 AND column_name = $3
                )
                """,
                self.target_schema,
                table_name,
                column_name,
            )

            if not column_exists:
                # Add the column first
                await conn.execute(
                    f"ALTER TABLE {self.target_schema}.{table_name} ADD COLUMN {column_name} TEXT"
                )
                logger.info(f"Added missing column {column_name} to {table_name}")

            # Try to add primary key constraint
            # First need to ensure the column has no nulls and is unique
            await conn.execute(
                f"UPDATE {self.target_schema}.{table_name} SET {column_name} = gen_random_uuid()::text WHERE {column_name} IS NULL"
            )

            # Check if we can make it a primary key (no duplicates)
            duplicate_count = await conn.fetchval(
                f"""
                SELECT COUNT(*) FROM (
                    SELECT {column_name}, COUNT(*)
                    FROM {self.target_schema}.{table_name}
                    GROUP BY {column_name}
                    HAVING COUNT(*) > 1
                ) duplicates
                """
            )

            if duplicate_count > 0:
                logger.warning(
                    f"Cannot add primary key to {table_name}.{column_name} - duplicates exist"
                )
                # Add unique constraint instead if possible
                try:
                    await conn.execute(
                        f"CREATE UNIQUE INDEX IF NOT EXISTS {table_name}_{column_name}_unique ON {self.target_schema}.{table_name} ({column_name})"
                    )
                    logger.info(f"Added unique index to {table_name}.{column_name}")
                except Exception as e:
                    logger.warning(
                        f"Could not add unique index to {table_name}.{column_name}: {e}"
                    )
            else:
                # Add primary key constraint
                await conn.execute(
                    f"ALTER TABLE {self.target_schema}.{table_name} ADD PRIMARY KEY ({column_name})"
                )
                logger.info(
                    f"Added primary key constraint to {table_name}.{column_name}"
                )

        except Exception as e:
            logger.warning(
                f"Could not add primary key constraint to {table_name}.{column_name}: {e}"
            )
            # Continue without constraint - will use simple INSERT

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
            table_name.lower(),
        )
        return result

    async def _create_table(
        self,
        conn: asyncpg.Connection,
        table_name: str,
        sample_record: dict[str, Any],
        record_type: str,
    ) -> bool:
        """Create table if it doesn't exist."""
        try:
            # Build column definitions from sample record
            columns = []
            primary_key_column = None

            for key in sample_record:
                if record_type == "main" and key == self.main_id_field:
                    columns.append(f"{key} TEXT PRIMARY KEY")
                    primary_key_column = key
                elif record_type != "main" and key == "id":
                    columns.append("id TEXT PRIMARY KEY")
                    primary_key_column = key
                else:
                    # All other data types become TEXT to avoid type conversion issues
                    columns.append(f"{key} TEXT")

            # Ensure we have a primary key
            if not primary_key_column:
                if record_type == "main":
                    columns.append(f"{self.main_id_field} TEXT PRIMARY KEY")
                else:
                    columns.append("id TEXT PRIMARY KEY")

            create_sql = f"""
                CREATE TABLE IF NOT EXISTS {self.target_schema}.{table_name.lower()} (
                    {", ".join(columns)}
                )
            """

            await conn.execute(create_sql)
            # Don't log here - let the caller log only when table was actually created
            return True

        except asyncpg.DuplicateTableError:
            # Table already exists, which is fine
            logger.debug(f"Table {self.target_schema}.{table_name} already exists")
            return True
        except Exception as e:
            # Handle PostgreSQL type constraint violations (concurrent table creation)
            if "duplicate key value violates unique constraint" in str(
                e
            ) and "pg_type_typname_nsp_index" in str(e):
                # This happens when multiple processes try to create the same table type simultaneously
                logger.debug(
                    f"Table type for {table_name} already exists (concurrent creation)"
                )
                # Check if the table actually exists now
                try:
                    table_exists = await self._table_exists(conn, table_name.lower())
                    if table_exists:
                        logger.debug(
                            f"Table {self.target_schema}.{table_name} exists after type constraint error"
                        )
                        return True
                except Exception:
                    pass

            logger.error(f"Error creating table {table_name}: {e}")
            raise

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
        return {
            row["column_name"].lower() for row in rows
        }  # Convert to lowercase for comparison

    async def _add_missing_columns(
        self, conn: asyncpg.Connection, table_name: str, sample_record: dict[str, Any]
    ) -> None:
        """Add missing columns to existing table."""
        existing_columns = await self._get_existing_columns(conn, table_name)

        logger.debug(
            f"Checking schema for {table_name}: existing columns = {sorted(existing_columns)}"
        )

        missing_columns = []
        for key in sample_record:
            # Convert key to lowercase for comparison since PostgreSQL stores column names in lowercase
            key_lower = key.lower()
            if key_lower not in existing_columns:
                missing_columns.append(key)

        if missing_columns:
            logger.info(
                f"Adding {len(missing_columns)} missing columns to {table_name}: {missing_columns}"
            )

            for key in missing_columns:
                # Determine column type - use TEXT for all simple types
                try:
                    alter_sql = f"ALTER TABLE {self.target_schema}.{table_name} ADD COLUMN {key} TEXT"
                    logger.debug(f"Executing: {alter_sql}")
                    await conn.execute(alter_sql)
                    logger.info(f"Successfully added column {key} to {table_name}")
                except Exception as e:
                    if "already exists" in str(e).lower():
                        logger.debug(
                            f"Column {key} already exists in {table_name} (concurrent creation)"
                        )
                    else:
                        logger.error(f"Failed to add column {key} to {table_name}: {e}")
                        # Try to continue with other columns
                        continue
        else:
            logger.debug(f"No missing columns for {table_name}")

        # Verify the columns were actually added
        if missing_columns:
            updated_columns = await self._get_existing_columns(conn, table_name)
            still_missing = []
            for key in missing_columns:
                if key.lower() not in updated_columns:
                    still_missing.append(key)

            if still_missing:
                logger.error(
                    f"Failed to add columns to {table_name}: {still_missing} are still missing!"
                )
            else:
                logger.debug(f"Successfully verified all columns added to {table_name}")

    async def _store_table_records_using_conn(
        self,
        conn: asyncpg.Connection,
        table_name: str,
        records: list[dict[str, Any]],
        record_type: str,
        update_strategy: Literal["NOTHING", "UPDATE"],
    ) -> int:
        """Store records to table with bulk operations for better performance."""
        if not records:
            return 0

        # Determine the conflict column based on record type
        conflict_column = self.main_id_field if record_type == "main" else "id"

        # Check if the table has the expected constraint (once for all records)
        has_constraint = await self._check_constraint_exists(
            conn, table_name.lower(), conflict_column
        )

        # Process all records to get consistent columns and prepare bulk data
        processed_records = []
        all_columns = set()

        for record in records:
            try:
                # Prepare record for storage - ensure type consistency
                processed_record = {}
                for key, value in record.items():
                    if value is None:
                        processed_record[key] = None
                    elif isinstance(value, dict | list):
                        processed_record[key] = json.dumps(value)
                    elif isinstance(value, bool):
                        # Convert boolean to lowercase string for consistency
                        processed_record[key] = str(value).lower()
                    else:
                        # Convert all other non-null values to strings
                        processed_record[key] = str(value)

                processed_records.append(processed_record)
                all_columns.update(processed_record.keys())

            except Exception as e:
                logger.error(f"Error processing record for {table_name}: {e}")
                continue

        if not processed_records:
            return 0

        # Ensure all records have the same columns (fill missing with None)
        all_columns = sorted(all_columns)  # Consistent ordering
        standardized_records = []

        for record in processed_records:
            standardized_record = {col: record.get(col) for col in all_columns}
            standardized_records.append(standardized_record)

        # Deduplicate records within the batch to avoid ON CONFLICT issues
        if has_constraint and update_strategy == "UPDATE":
            seen_keys = set()
            deduplicated_records = []

            for record in standardized_records:
                key_value = record.get(conflict_column)
                if key_value and key_value not in seen_keys:
                    seen_keys.add(key_value)
                    deduplicated_records.append(record)
                elif not key_value:
                    # Keep records without conflict column (will get auto-generated ID)
                    deduplicated_records.append(record)

            if len(deduplicated_records) != len(standardized_records):
                logger.debug(
                    f"Deduplicated {len(standardized_records) - len(deduplicated_records)} duplicate records in {table_name}"
                )

            standardized_records = deduplicated_records

        # Log column information for debugging
        logger.debug(
            f"Storing {len(standardized_records)} records to {table_name} with columns: {all_columns}"
        )

        # Log column information for debugging complex nested structures
        nested_columns = [
            col
            for col in all_columns
            if "amendment" in col.lower() and col.lower().count("amendment") > 1
        ]
        if nested_columns:
            logger.debug(
                f"Found nested structure columns in {table_name}: {nested_columns}"
            )

        # Perform bulk insert using most efficient method
        try:
            # Try COPY first for maximum performance (no parameter limits)
            if not has_constraint or update_strategy != "UPDATE":
                logger.debug(
                    f"Using COPY for high-performance bulk insert to {table_name}"
                )
                stored_count = await self._bulk_copy_insert(
                    conn, table_name, standardized_records, all_columns
                )

                if stored_count > 10:  # Only log for significant batches
                    logger.debug(
                        f"COPY inserted {stored_count} records into {table_name}"
                    )

                return stored_count

            # For UPSERT operations, use chunked bulk INSERT to avoid parameter limits
            logger.debug(f"Using chunked bulk UPSERT for {table_name}")
            stored_count = await self._chunked_bulk_upsert(
                conn, table_name, standardized_records, all_columns, conflict_column
            )

            if stored_count > 10:  # Only log for significant batches
                logger.debug(
                    f"Chunked bulk upserted {stored_count} records into {table_name}"
                )

            return stored_count

        except Exception as e:
            logger.error(
                f"Bulk insert failed for {table_name}, falling back to individual inserts: {e}"
            )

            # Fallback to individual inserts if bulk fails
            stored_count = 0
            for record in standardized_records:
                try:
                    columns = list(record.keys())
                    values = list(record.values())
                    placeholders = ", ".join(f"${i + 1}" for i in range(len(values)))
                    columns_str = ", ".join(columns)

                    if has_constraint and update_strategy == "UPDATE":
                        update_clauses = ", ".join(
                            f"{col} = EXCLUDED.{col}"
                            for col in columns
                            if col != conflict_column
                        )
                        insert_sql = f"""
                            INSERT INTO {self.target_schema}.{table_name.lower()} ({columns_str})
                            VALUES ({placeholders})
                            ON CONFLICT ({conflict_column}) DO UPDATE SET {update_clauses}
                        """
                    else:
                        insert_sql = f"""
                            INSERT INTO {self.target_schema}.{table_name.lower()} ({columns_str})
                            VALUES ({placeholders})
                        """

                    await conn.execute(insert_sql, *values)
                    stored_count += 1

                except Exception as e:
                    logger.error(
                        f"Error storing individual record in {table_name}: {e}"
                    )
                    continue

            return stored_count

    async def _bulk_copy_insert(
        self,
        conn: asyncpg.Connection,
        table_name: str,
        records: list[dict[str, Any]],
        columns: list[str],
    ) -> int:
        """
        Use PostgreSQL COPY for high-performance bulk inserts.
        This method bypasses parameter limits and is much faster than INSERT.
        """
        if not records:
            return 0

        # Create a tab-separated values string
        output = io.StringIO()
        for record in records:
            row_values = []
            for col in columns:
                value = record.get(col)
                if value is None:
                    row_values.append("\\N")  # PostgreSQL NULL representation
                elif isinstance(value, bool):
                    row_values.append(
                        "t" if value else "f"
                    )  # PostgreSQL boolean representation
                elif isinstance(value, int | float):
                    row_values.append(str(value))
                else:
                    # Escape special characters for COPY format
                    str_value = str(value)
                    str_value = str_value.replace("\\", "\\\\")  # Escape backslashes
                    str_value = str_value.replace("\t", "\\t")  # Escape tabs
                    str_value = str_value.replace("\n", "\\n")  # Escape newlines
                    str_value = str_value.replace(
                        "\r", "\\r"
                    )  # Escape carriage returns
                    row_values.append(str_value)

            output.write("\t".join(row_values) + "\n")

        # Reset to beginning of StringIO
        output.seek(0)

        # Use COPY to insert the data efficiently
        columns_str = ", ".join(columns)
        copy_sql = f"COPY {self.target_schema}.{table_name.lower()} ({columns_str}) FROM STDIN WITH (FORMAT text, DELIMITER E'\\t', NULL '\\N')"

        # Use copy_from_query for raw COPY operation
        await conn.copy_from_query(
            copy_sql,
            source=output,
            timeout=300.0,  # 5 minute timeout for large batches
        )

        return len(records)

    async def _chunked_bulk_upsert(
        self,
        conn: asyncpg.Connection,
        table_name: str,
        records: list[dict[str, Any]],
        columns: list[str],
        conflict_column: str,
    ) -> int:
        """
        Perform bulk UPSERT operations in chunks to avoid parameter limits.
        PostgreSQL has a 32767 parameter limit, so we chunk the operations.
        """
        if not records:
            return 0

        # Calculate safe chunk size based on parameter limit
        # Each record uses len(columns) parameters, so max_records = 32767 / len(columns)
        max_params_per_chunk = 30000  # Leave some margin
        chunk_size = max(1, max_params_per_chunk // len(columns))

        logger.debug(
            f"Using chunk size {chunk_size} for {len(records)} records with {len(columns)} columns"
        )

        total_stored = 0
        columns_str = ", ".join(columns)

        # Build UPSERT query template
        update_clauses = ", ".join(
            f"{col} = EXCLUDED.{col}" for col in columns if col != conflict_column
        )

        for i in range(0, len(records), chunk_size):
            chunk = records[i : i + chunk_size]

            # Build VALUES clause for this chunk
            values_clauses = []
            all_values = []

            for j, record in enumerate(chunk):
                record_values = [record.get(col) for col in columns]
                all_values.extend(record_values)

                # Create parameter placeholders for this record
                start_param = j * len(columns) + 1
                placeholders = ", ".join(
                    f"${start_param + k}" for k in range(len(columns))
                )
                values_clauses.append(f"({placeholders})")

            # Execute the chunk
            insert_sql = f"""
                INSERT INTO {self.target_schema}.{table_name.lower()} ({columns_str})
                VALUES {", ".join(values_clauses)}
                ON CONFLICT ({conflict_column}) DO UPDATE SET {update_clauses}
            """

            await conn.execute(insert_sql, *all_values)
            total_stored += len(chunk)

            if len(chunk) > 10:  # Log significant chunks
                logger.debug(
                    f"Processed chunk {i // chunk_size + 1}: {len(chunk)} records"
                )

        return total_stored

    async def _check_constraint_exists(
        self, conn, table_name: str, column_name: str
    ) -> bool:
        """Check if a unique constraint or primary key exists on the given column."""
        try:
            # Check for primary key constraint
            pk_result = await conn.fetchval(
                """
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu
                    ON tc.constraint_name = kcu.constraint_name
                    WHERE tc.table_schema = $1
                    AND tc.table_name = $2
                    AND tc.constraint_type = 'PRIMARY KEY'
                    AND kcu.column_name = $3
                )
                """,
                self.target_schema,
                table_name,
                column_name,
            )

            if pk_result:
                return True

            # Check for unique constraint
            unique_result = await conn.fetchval(
                """
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu
                    ON tc.constraint_name = kcu.constraint_name
                    WHERE tc.table_schema = $1
                    AND tc.table_name = $2
                    AND tc.constraint_type = 'UNIQUE'
                    AND kcu.column_name = $3
                )
                """,
                self.target_schema,
                table_name,
                column_name,
            )

            return unique_result

        except Exception as e:
            logger.warning(
                f"Could not check constraints for {table_name}.{column_name}: {e}"
            )
            return False

    async def _create_schema(self) -> bool:
        """Ensure the target schema exists."""
        async with self.db_pool.acquire() as conn:
            await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self.target_schema}")
            return True

    # =============================================================================
    # DATA RETRIEVAL METHODS
    # =============================================================================

    async def _get_raw_data_by_ids(self, item_ids: list[str]) -> list[dict[str, Any]]:
        """Get raw data records by item IDs."""
        if not item_ids:
            return []

        placeholders = ", ".join(f"${i + 1}" for i in range(len(item_ids)))
        query = f"""
            SELECT * FROM {self.source_schema}.{self.data_type_name}_raw
            WHERE source_doc_id IN ({placeholders})
        """

        async with self.db_pool.acquire() as conn:
            rows = await conn.fetch(query, *item_ids)
            return [dict(row) for row in rows]

    async def _get_related_raw_data_by_ids(
        self, item_ids: list[str], table_suffix: str
    ) -> list[dict[str, Any]]:
        """Get related raw data records by parent item IDs."""
        if not item_ids:
            return []

        table_name = f"{self.data_type_name}_{table_suffix}_raw"
        placeholders = ", ".join(f"${i + 1}" for i in range(len(item_ids)))
        query = f"""
            SELECT * FROM {self.source_schema}.{table_name}
            WHERE source_doc_id IN ({placeholders})
        """

        try:
            async with self.db_pool.acquire() as conn:
                rows = await conn.fetch(query, *item_ids)
                return [dict(row) for row in rows]
        except Exception as e:
            logger.warning(f"Could not fetch {table_name} data: {e}")
            return []

    # -----------------------------------------------------------------------------
    # Record-level helpers (restored)
    # -----------------------------------------------------------------------------
    async def _process_single_record(
        self, raw_record: dict[str, Any], record_type: str
    ) -> dict[str, Any]:
        """Normalise a single *raw_record* (main or related) with checkpointing."""
        try:
            payload = raw_record.get("payload", {})
            if isinstance(payload, str):
                payload = json.loads(payload)

            record_id = payload.get(self.main_id_field)
            if not record_id:
                logger.warning("No main_id_field found in payload")
                return {"success": False, "error": "No main_id_field found"}
            if not payload:
                logger.warning("No payload found in raw record")
                return {"success": False, "error": "No payload found"}

            if record_type == "main":
                processing_phase = ProcessingPhase.MAIN_ITEMS
                field_name = "main_data"
            else:
                processing_phase = ProcessingPhase.RELATED_ENTITIES
                field_name = f"related_{record_type}"

            # Short-circuit when checkpoint says we are done.
            if self.progress_tracker and self.progress_tracker.should_skip_item(
                record_id, processing_phase, field_name=field_name
            ):
                logger.debug(
                    f"Double-check: Skipping {field_name} for already processed item {record_id}"
                )
                return {"success": True, "record_id": record_id, "skipped": True}

            if self.progress_tracker:
                self.progress_tracker.set_processing_phase(
                    processing_phase, current_field=field_name
                )

            # Build processing context
            if record_type == "main":
                ctx = RecordProcessingContext(
                    record_type="main",
                    data_type_name=self.data_type_name,
                    main_table_name=self.main_table_name,
                    main_id_field=self.main_id_field,
                )
            else:
                ctx = RecordProcessingContext(
                    record_type="related",
                    data_type_name=self.data_type_name,
                    main_table_name=self.main_table_name,
                    main_id_field=self.main_id_field,
                    table_suffix=record_type,
                    parent_id=record_id,
                )

            # Actual heavy lifting
            await self._process_records(payload, record_id, ctx)

            if self.progress_tracker:
                self.progress_tracker.increment_processed(
                    record_id, processing_phase, field_name=field_name
                )
            logger.debug(
                f"Processed and checkpointed {field_name} for item {record_id}"
            )
            return {"success": True, "record_id": record_id}

        except Exception as exc:
            logger.error(
                f"Error processing record {record_id if 'record_id' in locals() else 'unknown'} ({record_type}): {exc}"
            )
            return {"success": False, "error": str(exc)}

    async def _process_records(
        self,
        payload: dict[str, Any] | list[dict[str, Any]],
        base_record_id: str,
        processing_context: RecordProcessingContext,
    ) -> dict[str, Any]:
        """Normalise *payload* according to *processing_context*."""
        if processing_context.record_type == "main":
            data = payload.copy()
            temp_id = data.get(self.main_id_field, str(uuid.uuid4()))
            extracted_lists = self._extract_lists(data, temp_id)
            for key in list(data.keys()):
                if isinstance(data.get(key), list) and key in [
                    tbl.split("_")[-1] for tbl in extracted_lists
                ]:
                    del data[key]
            flat_data = self._flatten_dict(data)
            flat_data[self.main_id_field] = base_record_id

            # ----
            # Store MAIN table separately with record_type="main" so that
            # the conflict column is the primary key (bioguide_id, bill_id…).
            # Any *extracted list* tables must be stored with record_type="related"
            # so that conflict handling relies on the synthetic ``id`` column and
            # duplicates are NOT collapsed on the parent PK.  Previously we passed
            # ``record_type='main'`` for all tables which caused the accumulator
            # to deduplicate every row having the same parent key – e.g. only the
            # first entry of the *terms* list survived for members.
            # ----

            # 1.  Main (flat) record
            main_table_records = {processing_context.get_table_name(): [flat_data]}
            await self._store_all_table_records(main_table_records, "main")

            # 2.  Extracted list / nested records – treat as "related" so that
            #     the deduplication key is the synthetic ``id`` column.
            if extracted_lists:
                await self._store_all_table_records(extracted_lists, "related")

            # Nothing to *return* for now – the caller is only interested in side-effects
            return {"tables_updated": len(main_table_records) + len(extracted_lists)}

        # ---------- related (list) items ----------
        items = payload if isinstance(payload, list) else [payload]
        processed_items: list[dict[str, Any]] = []
        all_nested_lists: dict[str, list[dict[str, Any]]] = {}

        for item_data in items:
            item_copy = item_data.copy()
            temp_id = str(uuid.uuid4())
            extracted_lists = self._extract_lists(
                item_copy,
                temp_id,
                **processing_context.get_extraction_params(),
            )
            for key in list(item_copy.keys()):
                if isinstance(item_copy.get(key), list) and key in [
                    tbl.split("_")[-1] for tbl in extracted_lists
                ]:
                    del item_copy[key]
            flat_item = self._flatten_dict(item_copy)
            content_hash = processing_context.generate_content_hash(
                flat_item, base_record_id
            )
            flat_item["id"] = content_hash
            processing_context.add_relationships(flat_item, base_record_id)

            # Fix parent refs in extracted lists
            for _tbl, recs in extracted_lists.items():
                for rec in recs:
                    if "id" in rec and rec["id"] != content_hash:
                        for f_name, f_val in rec.items():
                            if f_val == temp_id:
                                rec[f_name] = content_hash

            for tbl, recs in extracted_lists.items():
                all_nested_lists.setdefault(tbl, []).extend(recs)

            processed_items.append(flat_item)

        parent_table = processing_context.get_table_name()
        all_table_records = {parent_table: processed_items}
        all_table_records.update(all_nested_lists)
        return await self._store_all_table_records(
            all_table_records, processing_context.record_type
        )

    async def _process_single_nested_record(
        self, raw_record: dict[str, Any]
    ) -> dict[str, Any]:
        """Handle *nested* data for a single main record."""
        try:
            payload = raw_record.get("payload", {})
            if isinstance(payload, str):
                payload = json.loads(payload)

            record_id = payload.get(self.main_id_field)
            if not record_id:
                logger.warning("No main_id_field found in payload")
                return {"success": False, "error": "No main_id_field found"}

            if self.progress_tracker and self.progress_tracker.should_skip_item(
                record_id, ProcessingPhase.RELATED_ENTITIES, field_name="nested_data"
            ):
                logger.debug(
                    f"Double-check: Skipping nested data for already processed item {record_id}"
                )
                return {"success": True, "record_id": record_id, "skipped": True}

            if self.progress_tracker:
                self.progress_tracker.set_processing_phase(
                    ProcessingPhase.RELATED_ENTITIES, current_field="nested_data"
                )

            nested_lists = self._extract_deeply_nested_lists(payload, record_id)
            if nested_lists:
                await self._store_all_table_records(nested_lists, "nested")
                logger.debug(
                    f"Processed {len(nested_lists)} nested tables for item {record_id}"
                )

            if self.progress_tracker:
                self.progress_tracker.increment_processed(
                    record_id,
                    ProcessingPhase.RELATED_ENTITIES,
                    field_name="nested_data",
                )
            return {"success": True, "record_id": record_id}
        except Exception as exc:
            logger.error(f"Error processing nested record with checkpoints: {exc}")
            return {"success": False, "error": str(exc)}

    def _extract_deeply_nested_lists(
        self, data: dict[str, Any], record_id: str
    ) -> dict[str, list[dict[str, Any]]]:
        """Extract lists-within-lists that previous phases didn't cover."""
        nested_tables: dict[str, list[dict[str, Any]]] = {}

        def _extract(
            items: list, parent_key: str, root_id: str, parent_id: str | None = None
        ):
            for itm in items:
                if not isinstance(itm, dict):
                    continue
                item_hash = RecordProcessingContext.generate_content_hash(
                    itm, parent_id or root_id
                )
                for key, val in itm.items():
                    if isinstance(val, list) and val:
                        tbl_name = f"{self.data_type_name}_{parent_key}_{key}"
                        nested_tables.setdefault(tbl_name, [])
                        for n_itm in val:
                            if isinstance(n_itm, dict):
                                flat = self._flatten_dict(n_itm)
                                flat[self.main_id_field] = root_id
                                parent_field = f"{parent_key.split('_')[-1]}_id"
                                flat[parent_field] = item_hash
                                flat["id"] = (
                                    RecordProcessingContext.generate_content_hash(
                                        flat, item_hash
                                    )
                                )
                                nested_tables[tbl_name].append(flat)
                                _extract([n_itm], key, root_id, flat["id"])

        for k, v in data.items():
            if isinstance(v, list) and v:
                _extract(v, k, record_id)
        return nested_tables
