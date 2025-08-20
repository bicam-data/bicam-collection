"""
Streamlined Cleaner with Full Plugin Integration

This module provides focused data cleaning with comprehensive plugin support,
checkpoint management, and production table operations.
"""

import asyncio
import logging
import time
from typing import Any

from .libs.hierarchical_checkpoint_system import (
    CleaningCheckpoint,
    CleaningPhase,
    ProcessingStage,
)
from .plugins.consolidated_registry import get_consolidated_registry
from .processing.optimized_storage_manager import OptimizedCleanerStorage
from .resources.coordinator import ResourceCoordinator

logger = logging.getLogger(__name__)


class StreamlinedCleaner:
    """
    Enhanced data cleaner with full plugin support and checkpoint management.

    This integrates comprehensive cleaning logic
    with the streamlined architecture:
    - Custom logic plugins for data type-specific cleaning
    - Checkpoint-based resume capability
    - Direct streaming from staging tables
    - Chunk-based processing
    - Production table management
    - Post-processing support
    """

    def __init__(self, resource_coordinator: ResourceCoordinator):
        """
        Initialize the streamlined cleaner.

        Args:
            resource_coordinator: Manages all resources (DB, API keys, storage, etc.)
        """
        self.coordinator = resource_coordinator
        self.plugin_registry = get_consolidated_registry()

        # Cache for custom logic plugins
        self._custom_logic_cache = {}

        # Target table override system
        self._target_table_override = None

        logger.info("StreamlinedCleaner initialized")

    async def clean_data_type(
        self,
        data_type: str,
        chunk_size: int = 1000,
        max_workers: int = None,
        rerun: bool = False,
        resume: bool = True,
        **kwargs,
    ) -> dict[str, Any]:
        """
        Clean data for a specific data type using plugin system with checkpoint support.

        Args:
            data_type: The data type to clean (e.g., "bills", "nominations")
            chunk_size: Number of items to process in each batch
            max_workers: Maximum number of parallel workers
            rerun: Whether to reprocess existing records (ignores checkpoints)
            resume: Whether to use checkpoint-based resume logic
            **kwargs: Additional parameters for specific data types

        Returns:
            Dictionary containing cleaning results and metrics
        """
        logger.info(f"Starting streamlined cleaning for {data_type}")
        start_time = time.time()

        # Get resources from coordinator
        db_pool = await self.coordinator.get_db_pool()
        checkpoint_manager = self.coordinator.get_checkpoint_manager_instance()
        storage_manager = await self.coordinator.get_storage_manager_instance()

        # Get custom logic plugin for this data type
        custom_logic = self._get_custom_logic_plugin(data_type)
        if not custom_logic:
            logger.warning(
                f"No custom logic plugin found for {data_type}, using generic cleaning"
            )

        # Get data type configuration
        try:
            config = self.plugin_registry.get_data_type_config(data_type)
            schema_names = self.plugin_registry.get_schema_names(data_type)
            staging_schema = schema_names["staging"]
            production_schema = schema_names["production"]
        except Exception as e:
            logger.error(f"Failed to get configuration for {data_type}: {e}")
            return {
                "data_type": data_type,
                "status": "failed",
                "error": f"Configuration error: {str(e)}",
                "duration": time.time() - start_time,
            }

        # Initialize cleaner storage adapter
        cleaner_storage = OptimizedCleanerStorage(
            storage_manager=storage_manager,
            staging_schema=staging_schema,
            production_schema=production_schema,
            data_type=data_type,
        )

        # Setup cleaning checkpoint
        cleaning_checkpoint = CleaningCheckpoint(checkpoint_manager, data_type)

        # Initialize custom logic with resources if needed
        if custom_logic:
            if hasattr(custom_logic, "db_pool"):
                custom_logic.db_pool = db_pool
            if hasattr(custom_logic, "staging_schema"):
                custom_logic.staging_schema = staging_schema
            if hasattr(custom_logic, "production_schema"):
                custom_logic.production_schema = production_schema

        results = {
            "data_type": data_type,
            "tables_processed": 0,
            "total_records_processed": 0,
            "total_errors": 0,
            "start_time": start_time,
            "resume_enabled": resume,
            "rerun_enabled": rerun,
            "status": "started",
        }

        try:
            # Get tables to process from custom logic or config
            tables_to_process = self._get_tables_to_process(
                data_type, custom_logic, config
            )

            if not tables_to_process:
                logger.warning(f"No tables found to process for {data_type}")
                results["status"] = "completed"
                results["duration"] = time.time() - start_time
                return results

            logger.info(
                f"Found {len(tables_to_process)} tables to process for {data_type}"
            )

            # Process each table (potentially in parallel)
            table_results = await self._process_tables(
                tables_to_process,
                data_type,
                custom_logic,
                cleaning_checkpoint,
                chunk_size,
                max_workers,
                rerun,
                resume,
                cleaner_storage,
                config,
            )

            # Aggregate results
            for _table, result in table_results.items():
                if result.get("success", False):
                    results["tables_processed"] += 1
                results["total_records_processed"] += result.get("records_processed", 0)
                results["total_errors"] += result.get("errors", 0)

            # Run post-processing if available
            if custom_logic and hasattr(custom_logic, f"_post_process_{data_type}"):
                logger.info(f"Running post-processing for {data_type}")
                try:
                    post_results = await self._run_post_processing(
                        data_type,
                        custom_logic,
                        cleaning_checkpoint,
                        rerun,
                        resume,
                        storage_manager,
                        cleaner_storage,
                    )
                    results["post_processing"] = post_results
                except Exception as e:
                    logger.error(f"Post-processing failed for {data_type}: {e}")
                    results["post_processing"] = {"status": "failed", "error": str(e)}

            results["status"] = "completed"

        except Exception as e:
            logger.error(f"Streamlined cleaning failed for {data_type}: {str(e)}")
            results["status"] = "failed"
            results["error"] = str(e)

        results["duration"] = time.time() - start_time
        logger.info(f"Streamlined cleaning completed for {data_type}: {results}")

        return results

    def _get_custom_logic_plugin(self, data_type: str) -> Any:
        """Get or cache custom logic plugin for data type."""
        if data_type in self._custom_logic_cache:
            return self._custom_logic_cache[data_type]

        plugin = self.plugin_registry.get_custom_logic_plugin(data_type, "cleaning")
        if plugin:
            self._custom_logic_cache[data_type] = plugin

        return plugin

    def _get_tables_to_process(
        self, data_type: str, custom_logic: Any, config: Any
    ) -> list[str]:
        """Get list of tables to process for this data type."""
        tables = []

        # First check if custom logic has a method to get tables
        if custom_logic and hasattr(custom_logic, "get_data_types_to_process"):
            try:
                tables = custom_logic.get_data_types_to_process()
            except Exception as e:
                logger.warning(f"Error getting tables from custom logic: {e}")

        # If no tables from custom logic, try config
        if not tables and config:
            # Main table
            table_name = config.schema.table_name or data_type
            tables.append(table_name)

            # Related tables
            for suffix in config.schema.related_tables:
                tables.append(f"{data_type}_{suffix}")

        # If still no tables, just use the data type name
        if not tables:
            tables = [data_type]

        return tables

    async def _process_tables(
        self,
        tables: list[str],
        data_type: str,
        custom_logic: Any,
        cleaning_checkpoint: CleaningCheckpoint,
        chunk_size: int,
        max_workers: int,
        rerun: bool,
        resume: bool,
        cleaner_storage: Any,
        config: Any,
    ) -> dict[str, dict[str, Any]]:
        """Process multiple tables potentially in parallel."""
        if max_workers is None:
            max_workers = min(len(tables), 4)  # Reasonable default

        semaphore = asyncio.Semaphore(max_workers)

        async def process_table_with_semaphore(table):
            async with semaphore:
                return await self._process_single_table(
                    table,
                    data_type,
                    custom_logic,
                    cleaning_checkpoint,
                    chunk_size,
                    rerun,
                    resume,
                    cleaner_storage,
                    config,
                )

        # Process all tables
        tasks = []
        skipped_results = {}

        for table in tables:
            # Skip if already processed (unless rerun)
            if not rerun and resume and cleaning_checkpoint.should_skip_table(table):
                logger.info(f"Skipping already processed table {table}")
                skipped_results[table] = {
                    "success": True,
                    "records_processed": 0,
                    "skipped": True,
                }
            else:
                tasks.append(process_table_with_semaphore(table))

        # Wait for all tasks
        if tasks:
            results = await asyncio.gather(*tasks)
            # Convert results to dict format
            for _, result in enumerate(results):
                if isinstance(result, tuple) and len(result) == 2:
                    table_name, stats = result
                    skipped_results[table_name] = stats
        else:
            results = []

        # Return the combined results
        return skipped_results

    async def _process_single_table(
        self,
        table: str,
        data_type: str,
        custom_logic: Any,
        cleaning_checkpoint: CleaningCheckpoint,
        chunk_size: int,
        rerun: bool,
        resume: bool,
        cleaner_storage: Any,
        config: Any,
    ) -> tuple[str, dict[str, Any]]:
        """Process a single table with streaming and checkpoints."""
        logger.info(f"Processing table {table} for data type {data_type}")

        stats = {
            "records_processed": 0,
            "chunks_processed": 0,
            "errors": 0,
            "start_time": time.time(),
            "success": False,
        }

        try:
            # Get checkpoint to determine offset
            checkpoint_data = cleaner_storage.get_cleaning_checkpoint(table)
            start_offset = checkpoint_data["last_offset"] if checkpoint_data else 0

            # Use optimized storage for streaming
            order_by = (
                config.schema.id_fields[0]
                if config and config.schema.id_fields
                else None
            )

            # Get multi-table configuration from custom logic if available
            multi_table_data_types = (
                getattr(custom_logic, "multi_table_data_types", None)
                if custom_logic
                else None
            )

            logger.info(
                f"Starting streaming for table {table} with chunk_size={chunk_size}, start_offset={start_offset}"
            )
            logger.info(f"Multi-table config for {table}: {multi_table_data_types}")

            async for chunk in cleaner_storage.stream_staging_data(
                table_name=table,
                batch_size=chunk_size,
                order_by=order_by,
                checkpoint_offset=start_offset if resume else 0,
                multi_table_data_types=multi_table_data_types,
                custom_logic=custom_logic,
            ):
                logger.debug(
                    f"Processing chunk for table {table}: {len(chunk)} records"
                )

                # Process chunk of records
                chunk_result = await self._process_chunk(
                    chunk,
                    table,
                    data_type,
                    custom_logic,
                    rerun,
                    cleaner_storage,
                    config,
                )

                stats["records_processed"] += chunk_result.get("records_processed", 0)
                stats["errors"] += chunk_result.get("errors", 0)
                stats["chunks_processed"] += 1

                logger.debug(
                    f"Completed chunk {stats['chunks_processed']} for table {table}: {chunk_result.get('records_processed', 0)} records processed, {chunk_result.get('errors', 0)} errors"
                )

                # Update checkpoint periodically
                if stats["chunks_processed"] % 10 == 0:
                    checkpoint = cleaning_checkpoint.cm.get_or_create_checkpoint(
                        ProcessingStage.CLEANING,
                        CleaningPhase.APPLY_RULES.value,
                        data_type,
                    )
                    checkpoint.processed_items = stats["records_processed"]
                    checkpoint.current_table = table
                    cleaning_checkpoint.save_checkpoint(checkpoint)
                    logger.info(
                        f"Updated checkpoint for table {table}: {stats['records_processed']} records processed"
                    )

            logger.info(
                f"Completed streaming for table {table}: {stats['chunks_processed']} chunks, {stats['records_processed']} records"
            )

            # Mark table as processed
            if not rerun and stats["records_processed"] > 0:
                cleaning_checkpoint.mark_table_cleaned(table)
                logger.info(f"Marked table {table} as cleaned")

            stats["success"] = True

        except Exception as e:
            logger.error(f"Error processing table {table}: {e}", exc_info=True)
            stats["error"] = str(e)

            # Log error in checkpoint system
            cleaning_checkpoint.cm.log_error(
                ProcessingStage.CLEANING,
                CleaningPhase.APPLY_RULES.value,
                data_type,
                table,
                str(e),
            )

        stats["duration"] = time.time() - stats["start_time"]
        logger.info(f"Completed processing table {table}: {stats}")

        return table, stats

    async def _process_chunk(
        self,
        chunk: list[dict[str, Any]],
        table: str,
        data_type: str,
        custom_logic: Any,
        rerun: bool,
        cleaner_storage: Any,
        config: Any,
    ) -> dict[str, Any]:
        """Process a chunk of records."""
        stats = {"records_processed": 0, "errors": 0}

        if not chunk:
            return stats

        # Group cleaned records by target table
        records_by_table = {}

        for record in chunk:
            try:
                # Clean the record using custom logic or generic cleaning
                cleaned_record, target_table = await self._clean_single_record(
                    record, table, data_type, custom_logic
                )

                if target_table not in records_by_table:
                    records_by_table[target_table] = []
                records_by_table[target_table].append(cleaned_record)

                stats["records_processed"] += 1

            except Exception as e:
                logger.error(
                    f"Error cleaning record {record.get('id', 'UNKNOWN')}: {e}"
                )
                stats["errors"] += 1

        # Store cleaned records using optimized bulk insert
        if records_by_table:
            try:
                for target_table, cleaned_records in records_by_table.items():
                    # Get primary keys from config if available
                    primary_keys = []
                    if config and hasattr(config.schema, "id_fields"):
                        primary_keys = config.schema.id_fields
                    # logger.info(f"Storing chunk: target_table: {target_table}, cleaned_records: {cleaned_records}")
                    stored_count = await cleaner_storage.bulk_insert_production(
                        table_name=target_table,
                        records=cleaned_records,
                        upsert=rerun,
                        primary_keys=primary_keys,
                    )
                    logger.debug(
                        f"Stored {stored_count} cleaned records in {target_table}"
                    )
            except Exception as e:
                logger.error(f"Error storing chunk: {e}", exc_info=True)
                stats["errors"] += len(chunk)

        return stats

    async def _clean_single_record(
        self, record: dict[str, Any], table: str, data_type: str, custom_logic: Any
    ) -> tuple[dict[str, Any], str]:
        """Clean a single record using custom logic."""
        # Clear any previous override
        self._target_table_override = None

        # Try custom logic first
        if custom_logic:
            # Clear any previous override on the custom logic instance
            if hasattr(custom_logic, "_clear_target_table_override"):
                custom_logic._clear_target_table_override()
            elif hasattr(custom_logic, "_target_table_override"):
                custom_logic._target_table_override = None

            # Look for specific cleaning method - try both table-specific and data-type-specific
            method_names = [
                f"_clean_{table}_singular",
                f"_clean_{data_type}_singular",
            ]

            for method_name in method_names:
                if hasattr(custom_logic, method_name):
                    # Call the custom cleaning method
                    cleaned_data = await getattr(custom_logic, method_name)(record)

                    # Check if custom logic set a target table override using the new pattern
                    if hasattr(custom_logic, "target_table_override"):
                        self._target_table_override = custom_logic.target_table_override
                    elif hasattr(custom_logic, "_target_table_override"):
                        # Legacy support for old pattern
                        self._target_table_override = (
                            custom_logic._target_table_override
                        )

                    target_table = self._target_table_override or table
                    return cleaned_data, target_table

        # Fallback to generic cleaning - just return the record as-is for now
        # This ensures we don't lose data even without custom logic
        cleaned_data = record.copy()
        return cleaned_data, table

    async def _run_post_processing(
        self,
        data_type: str,
        custom_logic: Any,
        cleaning_checkpoint: CleaningCheckpoint,
        rerun: bool,
        resume: bool,
        storage_manager=None,
        cleaner_storage=None,
    ) -> dict[str, Any]:
        """Run post-processing operations."""
        if not custom_logic:
            return {"status": "skipped", "reason": "no custom logic"}

        method_name = f"_post_process_{data_type}"
        if not hasattr(custom_logic, method_name):
            return {"status": "skipped", "reason": "no post-processing method"}

        # Check if already processed
        if (
            not rerun
            and resume
            and cleaning_checkpoint.should_skip_table(f"{data_type}_post_process")
        ):
            return {"status": "skipped", "reason": "already processed"}

        try:
            # Get the method and check its signature
            method = getattr(custom_logic, method_name)

            # Check if method expects storage parameters
            import inspect

            sig = inspect.signature(method)
            params = list(sig.parameters.keys())

            # Skip 'self' parameter
            if params and params[0] == "self":
                params = params[1:]

            # Call method with appropriate parameters
            if (
                len(params) >= 2
                and "storage_manager" in params
                and "cleaner_storage" in params
            ):
                result = await method(
                    storage_manager=storage_manager, cleaner_storage=cleaner_storage
                )
            elif len(params) >= 1 and "storage_manager" in params:
                result = await method(storage_manager=storage_manager)
            else:
                # Fallback to no parameters for backward compatibility
                result = await method()

            # Mark as processed
            if not rerun:
                cleaning_checkpoint.mark_table_cleaned(f"{data_type}_post_process")

            return {"status": "success", "result": result}

        except Exception as e:
            logger.error(f"Post-processing failed: {e}")
            return {"status": "failed", "error": str(e)}

    async def cleanup(self):
        """Cleanup resources."""
        # Flush checkpoint caches if we have a checkpoint manager from coordinator
        try:
            checkpoint_manager = self.coordinator.get_checkpoint_manager_instance()
            checkpoint_manager.flush_all_caches()
        except Exception as e:
            logger.warning(f"Could not flush checkpoint caches: {e}")

        logger.info("StreamlinedCleaner cleanup completed")

    async def get_cleaning_status(self, data_type: str) -> dict[str, Any]:
        """Get cleaning status with checkpoint information."""
        try:
            checkpoint_manager = self.coordinator.get_checkpoint_manager_instance()

            # Get checkpoint status
            progress = checkpoint_manager.get_progress_summary(data_type)

            # Get cleaning-specific status
            cleaning_progress = progress.get(ProcessingStage.CLEANING.value, {})

            return {
                "data_type": data_type,
                "cleaning_progress": cleaning_progress,
                "checkpoint_available": bool(cleaning_progress),
                "status": "ready",
            }
        except Exception as e:
            logger.error(f"Failed to get cleaning status: {e}")
            return {"data_type": data_type, "status": "error", "error": str(e)}
