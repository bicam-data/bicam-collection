"""
Streamlined Normalizer

This module provides data normalization from raw/staging to production tables,
using optimized normalizer storage and hierarchical checkpoint system.
"""

import asyncio
import hashlib
import json
import logging
from datetime import UTC, datetime
from typing import Any

from .libs.hierarchical_checkpoint_system import (
    ProcessingStage,
    StagingCheckpoint,
    StagingPhase,
)
from .libs.run_tracking import RunMetadata, RunType
from .plugins.consolidated_registry import get_consolidated_registry
from .processing.optimized_storage_manager import OptimizedNormalizerStorage
from .resources.coordinator import ResourceCoordinator

logger = logging.getLogger(__name__)


class StreamlinedNormalizer:
    """
    Direct implementation of data normalization from raw to staging.

    Features:
    - Uses optimized normalizer storage for bulk operations
    - Hierarchical checkpoint system for detailed progress tracking
    - Three-phase processing: main records, related records, list extraction
    - No plugin dependencies - pure data processing
    """

    def __init__(self, resource_coordinator: ResourceCoordinator):
        """
        Initialize the streamlined normalizer.

        Args:
            resource_coordinator: Manages all resources (DB, API keys, storage, etc.)
        """
        self.coordinator = resource_coordinator
        self.registry = get_consolidated_registry()
        self.normalizer_storage = None  # Will be initialized when needed

        logger.info("StreamlinedNormalizer initialized")

    def _generate_deterministic_id(
        self, data: dict[str, Any], exclude_fields: list[str] = None
    ) -> str:
        """
        Generate a deterministic ID from data by hashing the content.

        Args:
            data: Dictionary to hash
            exclude_fields: Fields to exclude from hashing (e.g., timestamps, updatedate, etc.)

        Returns:
            Deterministic hash string
        """
        if exclude_fields is None:
            exclude_fields = []

        # Create a copy of data for hashing
        hash_data = {}
        for key, value in data.items():
            # Skip excluded fields and fields ending with _at
            if (
                key.lower() in exclude_fields
                or key.lower() in ["updatedate", "lastmodified"]
                or key.lower().endswith("_at")
            ):
                continue

            # Convert value to string for consistent hashing
            if value is None:
                hash_data[key] = "null"
            elif isinstance(value, dict | list):
                hash_data[key] = json.dumps(value, sort_keys=True)
            else:
                hash_data[key] = str(value)

        # Sort keys for deterministic ordering
        sorted_items = sorted(hash_data.items())

        # Create hash string
        hash_string = json.dumps(sorted_items, sort_keys=True)

        # Generate SHA-256 hash
        hash_obj = hashlib.sha256(hash_string.encode("utf-8"))
        return hash_obj.hexdigest()[:16]  # Use first 16 characters for readability

    def _get_parent_table_suffix(self, table_name: str) -> str:
        """
        Extract the suffix from a table name to use as foreign key field name.

        Examples:
            amendments_actions -> actions
            amendments_actions_committees -> committees
            bills_notes_links -> links
            simple_table -> table

        Args:
            table_name: Full table name

        Returns:
            Suffix to use as foreign key field name
        """
        parts = table_name.split("_")
        if len(parts) >= 2:
            return parts[-1]  # Return the last part
        return table_name

    def _add_hierarchical_ids(
        self,
        flat_data: dict[str, Any],
        table_name: str,
        parent_id: str = None,
        parent_table_name: str = None,
        config: Any = None,
    ) -> dict[str, Any]:
        """
        Add hierarchical IDs to the data.

        Args:
            flat_data: Flattened data dictionary
            table_name: Current table name
            parent_id: ID of parent record (if this is an extracted table)
            parent_table_name: Name of parent table (if this is an extracted table)
            config: Configuration object to get proper id_field

        Returns:
            Data with hierarchical IDs added
        """
        # Generate deterministic ID for this record
        deterministic_id = self._generate_deterministic_id(flat_data)
        flat_data["id"] = deterministic_id

        # If this is an extracted table, add foreign key to parent
        # Only add foreign key if both parent_id and parent_table_name are provided
        if parent_id and parent_table_name:
            # Use the config's id_field if available, otherwise fall back to generic approach
            if config and hasattr(config, "id_field"):
                # For related tables, use the first id_field from the config
                # This handles cases like amendments_links where id_fields: [amendment_id, link_url]
                foreign_key_field = (
                    config.id_fields[0]
                    if hasattr(config, "id_fields")
                    else config.id_field
                )
            else:
                # Fallback to generic approach
                parent_suffix = self._get_parent_table_suffix(parent_table_name)
                foreign_key_field = f"{parent_suffix}_id"

            flat_data[foreign_key_field] = parent_id

        return flat_data

    def _get_optimal_batch_sizes(self, data_type: str) -> tuple[int, int, int]:
        """
        Get optimal batch sizes based on data type configuration and intelligent scaling.

        Returns:
            tuple: (batch_size, window_size, max_concurrent)
        """
        try:
            # Get the data type config from registry
            config = self.registry.get_data_type_config(data_type)
            if not config:
                logger.warning(f"No config found for {data_type}, using defaults")
                return (1000, 10000, 50)

            # Get base batch size and scaling factor from config
            base_batch_size = config.processing.batch_size
            scaling_factor = config.processing.scaling_factor

            # Calculate optimized batch size
            optimized_batch_size = base_batch_size * scaling_factor

            # Calculate window size (10x batch size for efficient processing)
            window_size = optimized_batch_size * 10

            # Calculate max concurrent (based on batch size, capped for memory)
            max_concurrent = min(
                optimized_batch_size // 50, 200
            )  # 1 worker per 50 items, max 200

            logger.info(f"Config-based optimization for {data_type}:")
            logger.info(f"  Base batch_size: {base_batch_size} (from config)")
            logger.info(f"  Scaling factor: {scaling_factor}x (from config)")
            logger.info(f"  Optimized batch_size: {optimized_batch_size}")
            logger.info(f"  Window size: {window_size}")
            logger.info(f"  Max concurrent: {max_concurrent}")

            return (optimized_batch_size, window_size, max_concurrent)

        except Exception as e:
            logger.warning(
                f"Error getting optimal batch sizes for {data_type}: {e}, using defaults"
            )
            return (1000, 10000, 50)

    async def normalize_data_type(
        self,
        data_type: str,
        batch_size: int = 1000,
        max_concurrent: int = 50,
        rerun: bool = False,
        resume: bool = True,
        **kwargs,
    ) -> dict[str, Any]:
        """
        Normalize data for a specific data type from raw to staging.

        Args:
            data_type: The data type to normalize (e.g., "bills", "nominations")
            batch_size: Size of processing batches (will be overridden for large data types)
            max_concurrent: Maximum concurrent operations (will be overridden for large data types)
            rerun: Whether to reprocess existing records (ignores checkpoints)
            resume: Whether to use checkpoint-based resume logic
            **kwargs: Additional parameters for specific data types

        Returns:
            Dictionary containing normalization results and metrics
        """
        logger.info(f"Starting normalization for {data_type}")

        # Get optimal batch sizes for this data type
        optimal_batch_size, optimal_window_size, optimal_max_concurrent = (
            self._get_optimal_batch_sizes(data_type)
        )

        # Always use optimal sizes for high-performance processing
        batch_size = optimal_batch_size
        max_concurrent = optimal_max_concurrent

        # Get data source from registry
        data_source = self.registry.get_data_source(data_type)

        # Get resources
        db_pool = await self.coordinator.get_db_pool()
        checkpoint_manager = self.coordinator.get_checkpoint_manager_instance()
        run_manager = await self.coordinator.get_run_manager_instance()
        storage_manager = await self.coordinator.get_storage_manager_instance()

        # Initialize normalizer storage
        schema_names = self.registry.get_schema_names(data_type)
        target_schema = schema_names["staging"]

        self.normalizer_storage = OptimizedNormalizerStorage(
            storage_manager=storage_manager,
            target_schema=target_schema,
            data_type=data_type,
        )

        # Create staging checkpoint helper
        staging_checkpoint = StagingCheckpoint(checkpoint_manager, data_type)

        # Setup run tracking
        run_metadata = RunMetadata(
            run_id="",
            run_type=RunType.DATA_NORMALIZATION,
            system_name=f"{data_type}_normalizer",
            description=f"Normalize {data_type} from {data_source} raw to staging",
            data_types=[data_type],
        )
        run_id = run_manager.create_run(run_metadata)
        await run_manager.start_run(run_id)

        results = {
            "data_type": data_type,
            "phase": "normalization",
            "status": "started",
            "tables_processed": 0,
            "main_records_processed": 0,
            "related_records_processed": 0,
            "lists_extracted": 0,
            "columns_cleaned": 0,
            "errors": 0,
            "start_time": datetime.now(UTC),
        }

        try:
            # Get configuration from registry
            config = self.registry.get_data_type_config(data_type)
            if not config:
                raise ValueError(f"No configuration found for data type: {data_type}")

            # Get schema names from registry
            source_schema = schema_names["raw"]

            logger.info(
                f"Processing {data_type} from {source_schema} to {target_schema}"
            )

            # Get checkpoints for different phases
            jsonb_checkpoint = staging_checkpoint.cm.get_or_create_checkpoint(
                ProcessingStage.STAGING, StagingPhase.JSONB_TO_STAGING.value, data_type
            )

            extract_checkpoint = staging_checkpoint.cm.get_or_create_checkpoint(
                ProcessingStage.STAGING, StagingPhase.EXTRACT_LISTS.value, data_type
            )

            # Phase 1: Process main table (JSONB to staging)
            if not rerun and staging_checkpoint.should_skip_table(config.table_name):
                logger.info(
                    f"Main table {config.table_name} already processed, skipping"
                )
            else:
                logger.info(f"Processing main table: {config.table_name}")

                # Efficient windowed processing loop
                total_processed = 0
                total_errors = 0
                window_size = optimal_window_size
                while True:
                    item_ids = await self._get_items_to_normalize(
                        db_pool,
                        data_type,
                        source_schema,
                        config,
                        jsonb_checkpoint,
                        window_size,
                    )
                    if not item_ids:
                        break
                    jsonb_checkpoint.total_items = max(
                        jsonb_checkpoint.total_items, total_processed + len(item_ids)
                    )
                    staging_checkpoint.save_checkpoint(jsonb_checkpoint)

                    main_stats = await self._process_main_records(
                        db_pool,
                        data_type,
                        source_schema,
                        config,
                        item_ids,
                        batch_size,
                        max_concurrent,
                        staging_checkpoint,
                        jsonb_checkpoint,
                    )
                    total_processed += main_stats["processed"]
                    total_errors += main_stats["errors"]

                    # If fewer than window_size returned, we're done
                    if len(item_ids) < window_size:
                        break

                results["main_records_processed"] = total_processed
                results["errors"] += total_errors

                # Mark main table as processed
                staging_checkpoint.mark_table_processed(config.table_name)
                results["tables_processed"] += 1

            # Phase 2: Process related tables (only those with create_raw: True)
            if hasattr(config, "related_tables") and config.related_tables:
                for related_table in config.related_tables:
                    # Get the specific config for this related table from the main data type's config file
                    related_config = self.registry.get_related_table_config(
                        data_type, related_table
                    )

                    # Check if this related table has a raw table (create_raw: True)
                    if (
                        related_config
                        and hasattr(related_config.schema, "create_raw")
                        and not related_config.schema.create_raw
                    ):
                        logger.info(
                            f"Skipping {related_table} - no raw table (create_raw: False)"
                        )
                        continue

                    # Check if raw table actually exists in database
                    source_table = f"{data_type}_{related_table}_raw"
                    table_exists = await self._check_raw_table_exists(
                        db_pool, source_schema, source_table
                    )

                    if not table_exists:
                        logger.info(
                            f"Skipping {related_table} - raw table {source_schema}.{source_table} does not exist"
                        )
                        continue

                    table_name = f"{data_type}_{related_table}".lower()

                    # If rerun is True, clear the checkpoint for this related table
                    if rerun:
                        logger.info(f"Rerun mode: clearing checkpoint for {table_name}")
                        self._clear_related_table_checkpoint(
                            staging_checkpoint, f"{data_type}_{related_table}".lower()
                        )
                    elif staging_checkpoint.should_skip_table(table_name):
                        logger.info(
                            f"Related table {table_name} already processed, skipping"
                        )
                        continue

                    logger.info(f"Processing related table: {table_name}")

                    if related_config is None:
                        logger.warning(
                            f"No config found for related table {related_table}, skipping."
                        )
                        continue

                    related_stats = await self._process_related_table(
                        db_pool,
                        data_type,
                        source_schema,
                        related_config,  # Always use the correct config
                        related_table,
                        batch_size,
                        max_concurrent,
                        staging_checkpoint,
                    )

                    results["related_records_processed"] += related_stats["processed"]
                    results["errors"] += related_stats["errors"]

                    # Mark related table as processed
                    staging_checkpoint.mark_table_processed(table_name)
                    results["tables_processed"] += 1

            # Phase 3: Extract lists from all tables (including recursively extracted ones)
            logger.info(
                "Extracting lists from all tables (including recursively extracted ones)"
            )

            extract_stats = await self._extract_all_lists(
                db_pool,
                data_type,
                target_schema,
                config,
                staging_checkpoint,
                extract_checkpoint,
                batch_size,
                max_concurrent,
                rerun,
            )

            results["lists_extracted"] = extract_stats["lists_extracted"]
            results["errors"] += extract_stats["errors"]

            # Phase 4: Clean up JSON columns that were extracted in Phase 3
            logger.info("Cleaning up JSON columns that were extracted in Phase 3")

            cleanup_stats = await self._cleanup_extracted_json_columns(
                db_pool,
                data_type,
                target_schema,
                config,
                staging_checkpoint,
                extract_checkpoint,
                rerun,
            )

            results["columns_cleaned"] = cleanup_stats["columns_cleaned"]
            results["errors"] += cleanup_stats["errors"]

            # Update final checkpoint state
            jsonb_checkpoint.processed_items = results["main_records_processed"]
            jsonb_checkpoint.failed_items = results["errors"]
            staging_checkpoint.save_checkpoint(jsonb_checkpoint)

            results["status"] = (
                "completed" if results["errors"] == 0 else "completed_with_errors"
            )

        except Exception as e:
            logger.error(f"Normalization failed for {data_type}: {e}")
            results["status"] = "failed"
            results["error"] = str(e)
            if run_manager and run_id:
                await run_manager.fail_run(run_id, str(e))
            raise
        finally:
            # Ensure final flush
            if self.normalizer_storage:
                await self.normalizer_storage.flush_all()

            if run_manager and run_id:
                await run_manager.complete_run(run_id)

            # Ensure checkpoint manager flushes any cached data
            checkpoint_manager.flush_all_caches()

            results["end_time"] = datetime.now(UTC)
            results["duration"] = (
                results["end_time"] - results["start_time"]
            ).total_seconds()

        logger.info(f"Normalization completed for {data_type}: {results}")
        return results

    async def _check_raw_table_exists(
        self, db_pool, schema: str, table_name: str
    ) -> bool:
        """Check if a raw table exists in the database."""
        try:
            async with db_pool.acquire() as conn:
                result = await conn.fetchval(
                    """
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_schema = $1 AND table_name = $2
                    )
                    """,
                    schema,
                    table_name.lower(),
                )
                return result
        except Exception as e:
            logger.warning(f"Error checking if table {schema}.{table_name} exists: {e}")
            return False

    async def _get_items_to_normalize(
        self,
        db_pool,
        data_type: str,
        source_schema: str,
        config,
        checkpoint,
        limit: int = 10000,
    ) -> list[str]:
        """Get list of item IDs to normalize from raw tables."""
        # If resuming, start from checkpoint offset
        offset = checkpoint.current_offset if checkpoint else 0

        query = f"""
            SELECT DISTINCT source_doc_id
            FROM {source_schema}.{data_type}_raw
            WHERE source_doc_id IS NOT NULL
            ORDER BY source_doc_id
            LIMIT {limit} OFFSET {offset}
        """

        try:
            async with db_pool.acquire() as conn:
                rows = await conn.fetch(query)
                return [row["source_doc_id"] for row in rows]
        except Exception as e:
            logger.warning(f"Could not fetch items to normalize: {e}")
            return []

    async def _process_main_records(
        self,
        db_pool,
        data_type: str,
        source_schema: str,
        config,
        item_ids: list[str],
        batch_size: int,
        max_concurrent: int,
        staging_checkpoint,
        checkpoint,
    ) -> dict[str, int]:
        """Process main records from raw to staging."""
        if not item_ids:
            return {"processed": 0, "errors": 0}

        stats = {"processed": 0, "errors": 0}
        semaphore = asyncio.Semaphore(max_concurrent)

        # Get the starting offset for this window
        global_offset = checkpoint.current_offset if checkpoint else 0

        # Process in batches
        for batch_start in range(0, len(item_ids), batch_size):
            batch_ids = item_ids[batch_start : batch_start + batch_size]

            # Get raw data for batch
            placeholders = ", ".join(f"${i + 1}" for i in range(len(batch_ids)))
            query = f"""
                SELECT * FROM {source_schema}.{data_type}_raw
                WHERE source_doc_id IN ({placeholders})
            """

            async with db_pool.acquire() as conn:
                rows = await conn.fetch(query, *batch_ids)

            # Process records concurrently
            async def process_record(record):
                async with semaphore:
                    try:
                        source_doc_id = record.get("source_doc_id")

                        # Check if already processed
                        if staging_checkpoint.cm.is_item_processed(
                            ProcessingStage.STAGING,
                            StagingPhase.JSONB_TO_STAGING.value,
                            data_type,
                            source_doc_id,
                        ):
                            return {"success": True, "skipped": True}

                        # Standard processing - all data comes from "payload" field
                        payload = record.get("payload", {})
                        if isinstance(payload, str):
                            payload = json.loads(payload)

                        if not payload:
                            logger.warning(f"Empty payload for {source_doc_id}")
                            return {"success": False, "error": "Empty payload"}

                        # # Check for wrapper key if configured
                        # if hasattr(config.api, "full_key") and config.api.full_key:
                        #     if config.api.full_key in payload:
                        #         payload = payload[config.api.full_key]
                        #     else:
                        #         logger.warning(
                        #             f"Expected wrapper key '{config.api.full_key}' not found in payload"
                        #         )
                        #         return {
                        #             "success": False,
                        #             "error": f"Missing wrapper key: {config.api.full_key}",
                        #         }

                        # Get record ID using configured field
                        record_id = payload.get(config.id_field) or source_doc_id
                        if not record_id:
                            logger.warning(f"No {config.id_field} found in payload")
                            return {
                                "success": False,
                                "error": f"No {config.id_field} found",
                            }

                        # Flatten the data
                        flat_data = self._flatten_dict(payload)
                        flat_data[config.id_field] = record_id

                        # # Add deterministic ID to the main record (no parent - this is the main table)
                        # flat_data = self._add_hierarchical_ids(
                        #     flat_data, config.table_name
                        # )

                        # Extract lists to separate tables with hierarchical IDs
                        # Use the deterministic ID of this main table row as the parent
                        extracted_lists = self._extract_lists_with_hierarchical_ids(
                            payload, record_id, config.table_name, config
                        )

                        # Add common metadata
                        flat_data["processed_at"] = datetime.now(UTC).isoformat()
                        flat_data["source_doc_id"] = source_doc_id

                        # Store main record using optimized normalizer storage
                        await self.normalizer_storage.store_normalized_records(
                            table_name=config.table_name,
                            records=[flat_data],
                            record_type="main",
                            ensure_table=True,
                            checkpoint=True,
                        )

                        # Store extracted lists
                        if extracted_lists:
                            await self.normalizer_storage.store_multiple_tables(
                                table_records=extracted_lists,
                                checkpoint=False,
                            )

                        # Mark as processed
                        staging_checkpoint.cm.mark_item_processed(
                            ProcessingStage.STAGING,
                            StagingPhase.JSONB_TO_STAGING.value,
                            data_type,
                            source_doc_id,
                        )

                        return {"success": True}

                    except Exception as e:
                        logger.error(
                            f"Error processing main record {source_doc_id}: {e}"
                        )
                        staging_checkpoint.cm.log_error(
                            ProcessingStage.STAGING,
                            StagingPhase.JSONB_TO_STAGING.value,
                            data_type,
                            record.get("source_doc_id", "unknown"),
                            str(e),
                            error_type="processing_error",
                        )
                        return {"success": False, "error": str(e)}

            # Process batch
            results = await asyncio.gather(
                *[process_record(r) for r in rows], return_exceptions=True
            )

            # Update stats
            for result in results:
                if isinstance(result, Exception):
                    stats["errors"] += 1
                elif isinstance(result, dict) and result.get("success"):
                    if not result.get("skipped"):
                        stats["processed"] += 1
                else:
                    stats["errors"] += 1

            # Update checkpoint with correct global offset
            checkpoint.current_offset = global_offset + batch_start + len(batch_ids)
            checkpoint.processed_items = stats["processed"]
            checkpoint.failed_items = stats["errors"]
            staging_checkpoint.save_checkpoint(checkpoint)

        # Final flush
        await self.normalizer_storage.flush_all()

        return stats

    async def _process_related_table(
        self,
        db_pool,
        data_type: str,
        source_schema: str,
        config,
        related_table: str,
        batch_size: int,
        max_concurrent: int,
        staging_checkpoint,
    ) -> dict[str, int]:
        """Process a related table from raw to staging."""
        stats = {"processed": 0, "errors": 0}

        # Get checkpoint for this related table
        checkpoint = staging_checkpoint.cm.get_or_create_checkpoint(
            ProcessingStage.STAGING,
            StagingPhase.JSONB_TO_STAGING.value,
            f"{data_type}_{related_table}".lower(),
        )

        source_table = f"{data_type}_{related_table}_raw"
        target_table = f"{data_type}_{related_table}".lower()

        try:
            # Get total count
            async with db_pool.acquire() as conn:
                count_result = await conn.fetchval(
                    f"SELECT COUNT(*) FROM {source_schema}.{source_table}"
                )
                logger.info(
                    f"Found {count_result} records in {source_schema}.{source_table}"
                )
                checkpoint.total_items = count_result
                staging_checkpoint.save_checkpoint(checkpoint)

                if count_result == 0:
                    logger.info(
                        f"No records found in {source_schema}.{source_table} - skipping processing"
                    )
                    return stats
        except Exception as e:
            logger.error(
                f"Could not count records in {source_schema}.{source_table}: {e}"
            )
            return stats

        # Determine starting offset based on checkpoint state
        # If we have processed items but offset is 0, we need to recalculate offset
        # If we have no processed items, start from 0
        if checkpoint.processed_items == 0:
            offset = 0
            checkpoint.current_offset = 0
            staging_checkpoint.save_checkpoint(checkpoint)
            logger.info(
                f"Starting fresh processing for {target_table}, offset reset to 0"
            )
        else:
            offset = checkpoint.current_offset
            logger.info(
                f"Resuming processing for {target_table} from offset {offset} (processed: {checkpoint.processed_items})"
            )

        # Process in batches using offset-based pagination
        while True:
            # Fetch batch
            query = f"""
                SELECT * FROM {source_schema}.{source_table}
                ORDER BY source_doc_id
                LIMIT {batch_size} OFFSET {offset}
            """

            try:
                async with db_pool.acquire() as conn:
                    rows = await conn.fetch(query)
                    logger.debug(
                        f"Fetched {len(rows)} rows from {source_schema}.{source_table} at offset {offset}"
                    )
            except Exception as e:
                logger.error(
                    f"Could not fetch records from {source_schema}.{source_table}: {e}"
                )
                stats["errors"] += 1
                break

            if not rows:
                logger.info(
                    f"No more rows to process from {source_schema}.{source_table} at offset {offset}"
                )
                break

            # Process records
            semaphore = asyncio.Semaphore(max_concurrent)

            async def process_record(record):
                async with semaphore:  # noqa: B023
                    try:
                        source_doc_id = record.get("source_doc_id")

                        # Standard processing - all data comes from "payload" field
                        payload = record.get("payload", {})
                        if isinstance(payload, str):
                            payload = json.loads(payload)

                        if not payload:
                            logger.warning(f"Empty payload for {source_doc_id}")
                            return {"success": False, "error": "Empty payload"}

                        # Get record ID using configured field
                        record_id = payload.get(config.id_field) or source_doc_id
                        if not record_id:
                            logger.warning(f"No {config.id_field} found in payload")
                            return {
                                "success": False,
                                "error": f"No {config.id_field} found",
                            }

                        # Flatten the data
                        flat_data = self._flatten_dict(payload)
                        flat_data[config.id_field] = record_id

                        # Add hierarchical IDs to the main record (no parent - this is a related table)
                        flat_data = self._add_hierarchical_ids(
                            flat_data, target_table, config=config
                        )

                        # Extract lists to separate tables with hierarchical IDs
                        # Use the deterministic ID of this related table row as the parent
                        extracted_lists = self._extract_lists_with_hierarchical_ids(
                            payload, flat_data["id"], target_table, config
                        )

                        # Add common metadata
                        flat_data["processed_at"] = datetime.now(UTC).isoformat()
                        flat_data["source_doc_id"] = source_doc_id

                        # Store main record using optimized normalizer storage
                        await self.normalizer_storage.store_normalized_records(
                            table_name=target_table,
                            records=[flat_data],
                            record_type="related",
                            ensure_table=True,
                            checkpoint=False,
                        )

                        # Store extracted lists
                        if extracted_lists:
                            await self.normalizer_storage.store_multiple_tables(
                                table_records=extracted_lists,
                                checkpoint=False,
                            )

                        return {"success": True}

                    except Exception as e:
                        logger.error(
                            f"Error processing related record {source_doc_id}: {e}"
                        )
                        staging_checkpoint.cm.log_error(
                            ProcessingStage.STAGING,
                            StagingPhase.JSONB_TO_STAGING.value,
                            f"{data_type}_{related_table}".lower(),
                            record.get("source_doc_id", "unknown"),
                            str(e),
                            error_type="processing_error",
                        )
                        return {"success": False, "error": str(e)}

            # Process batch
            results = await asyncio.gather(
                *[process_record(r) for r in rows], return_exceptions=True
            )

            # Update stats
            batch_processed = 0
            batch_errors = 0
            for result in results:
                if isinstance(result, Exception):
                    batch_errors += 1
                elif isinstance(result, dict) and result.get("success"):
                    batch_processed += 1
                else:
                    batch_errors += 1

            stats["processed"] += batch_processed
            stats["errors"] += batch_errors

            # Update checkpoint after each batch
            offset += len(rows)
            checkpoint.current_offset = offset
            checkpoint.processed_items = stats["processed"]
            checkpoint.failed_items = stats["errors"]
            staging_checkpoint.save_checkpoint(checkpoint)

            logger.info(
                f"Batch processed for {target_table}: {batch_processed} processed, {batch_errors} errors. "
                f"Total: {stats['processed']}/{checkpoint.total_items} processed, offset: {offset}"
            )

        # Final flush
        await self.normalizer_storage.flush_all()

        logger.info(
            f"Completed processing {target_table}: {stats['processed']} processed, {stats['errors']} errors"
        )
        return stats

    async def _extract_all_lists(
        self,
        db_pool,
        data_type: str,
        target_schema: str,
        config,
        staging_checkpoint,
        checkpoint,
        batch_size: int,
        max_concurrent: int,
        rerun: bool,
    ) -> dict[str, Any]:
        """Extract all list fields to separate tables, including recursively from extracted tables."""
        stats = {"lists_extracted": 0, "records_processed": 0, "errors": 0}

        # Get all tables to process lists from (including recursively extracted tables)
        tables_to_process = await self._get_all_tables_for_extraction(
            db_pool, data_type, target_schema, config
        )

        for table_name in tables_to_process:
            # Get columns that are JSON arrays
            list_columns = await self._get_list_columns(
                db_pool, target_schema, table_name
            )

            for column_name in list_columns:
                # Create target table name for this list
                target_table = f"{table_name}_{column_name}".lower()

                # Check if the target list table already exists and has data
                # This indicates lists were already extracted in Phase 1
                list_table_exists = await self._check_table_exists(
                    db_pool, target_schema, target_table
                )

                if list_table_exists:
                    # Check if table has data
                    async with db_pool.acquire() as conn:
                        count = await conn.fetchval(
                            f"SELECT COUNT(*) FROM {target_schema}.{target_table}"
                        )

                    if count > 0:
                        logger.info(
                            f"List table {target_schema}.{target_table} already exists with {count} records, skipping extraction"
                        )
                        # Mark as extracted to prevent future runs
                        staging_checkpoint.mark_list_extracted(table_name, column_name)
                        continue

                # Check if already extracted via checkpoint
                if not rerun and staging_checkpoint.should_skip_list_extraction(
                    table_name, column_name
                ):
                    logger.info(
                        f"List {table_name}.{column_name} already extracted, skipping"
                    )
                    continue

                logger.info(f"Extracting list from {table_name}.{column_name}")

                # Extract this list
                # Determine which ID field to use based on table type
                # Main tables use config.id_field, related/extracted tables use 'id'
                parent_id_field = (
                    config.id_field if table_name == config.table_name else "id"
                )

                # Get the appropriate config for this table
                table_config = None
                if table_name == config.table_name:
                    # Main table - use main config
                    table_config = config
                else:
                    # Related table - try to get related table config
                    # Extract the related table name from the full table name
                    # e.g., "amendments_links" -> "links"
                    if table_name.startswith(f"{data_type}_"):
                        related_table_name = table_name[len(f"{data_type}_") :]
                        table_config = self.registry.get_related_table_config(
                            data_type, related_table_name
                        )

                extract_stats = await self._extract_list_column(
                    db_pool,
                    target_schema,
                    table_name,
                    column_name,
                    batch_size,
                    parent_id_field,
                    config=table_config,
                )

                stats["records_processed"] += extract_stats["records"]
                stats["errors"] += extract_stats["errors"]

                if extract_stats["records"] > 0:
                    stats["lists_extracted"] += 1

                    # Mark as extracted
                    staging_checkpoint.mark_list_extracted(table_name, column_name)

                # Update checkpoint
                checkpoint.current_table = table_name
                checkpoint.current_field = column_name
                checkpoint.processed_items = stats["records_processed"]
                staging_checkpoint.save_checkpoint(checkpoint)

        # Final flush
        await self.normalizer_storage.flush_all()

        return stats

    async def _get_all_tables_for_extraction(
        self, db_pool, data_type: str, target_schema: str, config
    ) -> list[str]:
        """Get all tables that need list extraction, including recursively extracted tables."""
        tables_to_process = set()

        # Get source schema (raw schema)
        source_schema = self.registry.get_schema_names(data_type)["raw"]

        # Always process the main table if it exists
        main_table_exists = await self._check_table_exists(
            db_pool, target_schema, config.table_name
        )
        if main_table_exists:
            tables_to_process.add(config.table_name)
            logger.info(f"Added main table {config.table_name} for list extraction")
        else:
            logger.warning(
                f"Main table {target_schema}.{config.table_name} does not exist"
            )

        # Process related tables - check both raw schema and staging schema
        if hasattr(config, "related_tables") and config.related_tables:
            for rt in config.related_tables:
                table_name = f"{data_type}_{rt}".lower()

                # Check if the staging table exists (this is the primary check)
                staging_table_exists = await self._check_table_exists(
                    db_pool, target_schema, table_name
                )

                if staging_table_exists:
                    # Staging table exists, so we should process it for list extraction
                    tables_to_process.add(table_name)
                    logger.info(f"Added related table {table_name} for list extraction")
                else:
                    # Check if raw table exists as a fallback
                    raw_table_name = f"{data_type}_{rt}_raw"
                    raw_table_exists = await self._check_raw_table_exists(
                        db_pool, source_schema, raw_table_name
                    )

                    if raw_table_exists:
                        logger.info(
                            f"Raw table {source_schema}.{raw_table_name} exists but staging table {target_schema}.{table_name} does not - will be created during processing"
                        )
                        # Don't add it yet since it doesn't exist in staging
                    else:
                        logger.info(
                            f"Skipping {table_name} - neither raw nor staging table exists"
                        )

        # Discover all tables in staging that start with our data type prefix
        # This catches tables like amendments_notes that were extracted during main table processing
        logger.info(
            f"Discovering all tables in {target_schema} that start with '{data_type}_'..."
        )
        async with db_pool.acquire() as conn:
            # Get all tables in the staging schema that start with the data_type
            query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = $1 
            AND table_name LIKE $2
            AND table_name NOT LIKE '%_raw'
            ORDER BY table_name
            """
            pattern = f"{data_type}_%"
            rows = await conn.fetch(query, target_schema, pattern)

            for row in rows:
                table_name = row["table_name"]
                if table_name not in tables_to_process:
                    # Check if this table has list columns that need extraction
                    list_columns = await self._get_list_columns(
                        db_pool, target_schema, table_name
                    )
                    if list_columns:
                        tables_to_process.add(table_name)
                        logger.info(
                            f"Added discovered table {table_name} for list extraction (has list columns: {list_columns})"
                        )
                    else:
                        logger.debug(
                            f"Discovered table {table_name} but no list columns found"
                        )

        # Recursively find all extracted list tables that need further extraction
        processed_tables = set()
        tables_to_check = list(tables_to_process)

        logger.info(
            f"Starting recursive discovery with initial tables: {tables_to_process}"
        )

        while tables_to_check:
            current_table = tables_to_check.pop(0)
            if current_table in processed_tables:
                continue

            processed_tables.add(current_table)
            logger.debug(f"Processing table for list extraction: {current_table}")

            # Get list columns from current table
            list_columns = await self._get_list_columns(
                db_pool, target_schema, current_table
            )

            if list_columns:
                logger.debug(f"Found list columns in {current_table}: {list_columns}")

            # Check if any of these list columns have been extracted to separate tables
            for column_name in list_columns:
                extracted_table = f"{current_table}_{column_name}".lower()

                # Check if the extracted table exists and has data
                if await self._check_table_exists(
                    db_pool, target_schema, extracted_table
                ):
                    async with db_pool.acquire() as conn:
                        count = await conn.fetchval(
                            f"SELECT COUNT(*) FROM {target_schema}.{extracted_table}"
                        )

                    if count > 0:
                        # This table was extracted and has data, so we need to process it too
                        tables_to_process.add(extracted_table)
                        tables_to_check.append(extracted_table)
                        logger.info(
                            f"Found extracted table {extracted_table} with {count} records, will process for list extraction"
                        )
                    else:
                        logger.debug(
                            f"Extracted table {extracted_table} exists but is empty, skipping"
                        )
                else:
                    logger.debug(
                        f"Extracted table {extracted_table} does not exist yet"
                    )

        logger.info(f"Final tables to process for list extraction: {tables_to_process}")
        return list(tables_to_process)

    async def _cleanup_extracted_json_columns(
        self,
        db_pool,
        data_type: str,
        target_schema: str,
        config,
        staging_checkpoint,
        checkpoint,
        rerun: bool,
    ) -> dict[str, Any]:
        """Clean up JSON columns that were already extracted to separate tables."""
        stats = {"columns_cleaned": 0, "errors": 0}

        # Get all tables to process (including recursively extracted tables)
        tables_to_process = await self._get_all_tables_for_cleanup(
            db_pool, data_type, target_schema, config
        )

        for table_name in tables_to_process:
            # Get columns that are JSON arrays
            list_columns = await self._get_list_columns(
                db_pool, target_schema, table_name
            )

            for column_name in list_columns:
                # Create target table name for this list
                target_table = f"{table_name}_{column_name}".lower()

                # Check if the target list table exists and has data
                list_table_exists = await self._check_table_exists(
                    db_pool, target_schema, target_table
                )

                if list_table_exists:
                    # Check if table has data
                    async with db_pool.acquire() as conn:
                        count = await conn.fetchval(
                            f"SELECT COUNT(*) FROM {target_schema}.{target_table}"
                        )

                    if count > 0:
                        logger.info(
                            f"List table {target_schema}.{target_table} exists with {count} records, cleaning up JSON column {table_name}.{column_name}"
                        )

                        # Drop the JSON column since it's been extracted
                        try:
                            async with db_pool.acquire() as conn:
                                await conn.execute(
                                    f"ALTER TABLE {target_schema}.{table_name} DROP COLUMN IF EXISTS {column_name}"
                                )

                            stats["columns_cleaned"] += 1
                            logger.info(
                                f"Dropped JSON column {table_name}.{column_name}"
                            )

                            # Mark as cleaned up
                            staging_checkpoint.mark_list_extracted(
                                table_name, column_name
                            )

                        except Exception as e:
                            logger.error(
                                f"Error dropping column {table_name}.{column_name}: {e}"
                            )
                            stats["errors"] += 1
                    else:
                        logger.info(
                            f"List table {target_schema}.{target_table} exists but is empty, skipping cleanup"
                        )
                else:
                    logger.info(
                        f"List table {target_schema}.{target_table} does not exist, skipping cleanup"
                    )

                # Update checkpoint
                checkpoint.current_table = table_name
                checkpoint.current_field = column_name
                staging_checkpoint.save_checkpoint(checkpoint)

        return stats

    async def _get_all_tables_for_cleanup(
        self, db_pool, data_type: str, target_schema: str, config
    ) -> list[str]:
        """Get all tables that need cleanup, including recursively extracted tables."""
        tables_to_process = set()

        # Get source schema (raw schema)
        source_schema = self.registry.get_schema_names(data_type)["raw"]

        # Always process the main table if it exists
        main_table_exists = await self._check_table_exists(
            db_pool, target_schema, config.table_name
        )
        if main_table_exists:
            tables_to_process.add(config.table_name)
        else:
            logger.warning(
                f"Main table {target_schema}.{config.table_name} does not exist"
            )

        # Process related tables (check if they exist in raw schema first, then staging)
        if hasattr(config, "related_tables") and config.related_tables:
            for rt in config.related_tables:
                table_name = f"{data_type}_{rt}".lower()

                # First check if the raw table exists
                raw_table_name = f"{data_type}_{rt}_raw"
                raw_table_exists = await self._check_raw_table_exists(
                    db_pool, source_schema, raw_table_name
                )

                if raw_table_exists:
                    # Raw table exists, so this table should be processed
                    # Check if it already exists in staging (may have been processed in Phase 2)
                    if await self._check_table_exists(
                        db_pool, target_schema, table_name
                    ):
                        tables_to_process.add(table_name)
                        logger.debug(
                            f"Found existing staging table {table_name}, will process for cleanup"
                        )
                    else:
                        # Raw table exists but staging table doesn't - this means Phase 2 didn't process it
                        logger.warning(
                            f"Raw table {source_schema}.{raw_table_name} exists but staging table {target_schema}.{table_name} does not"
                        )
                        # Don't add it to tables_to_process since it doesn't exist in staging yet
                else:
                    logger.info(
                        f"Skipping cleanup for {table_name} - raw table {source_schema}.{raw_table_name} does not exist"
                    )

        # Recursively find all extracted list tables
        processed_tables = set()
        tables_to_check = list(tables_to_process)

        while tables_to_check:
            current_table = tables_to_check.pop(0)
            if current_table in processed_tables:
                continue

            processed_tables.add(current_table)

            # Get list columns from current table
            list_columns = await self._get_list_columns(
                db_pool, target_schema, current_table
            )

            # Check if any of these list columns have been extracted to separate tables
            for column_name in list_columns:
                extracted_table = f"{current_table}_{column_name}".lower()

                # Check if the extracted table exists and has data
                if await self._check_table_exists(
                    db_pool, target_schema, extracted_table
                ):
                    async with db_pool.acquire() as conn:
                        count = await conn.fetchval(
                            f"SELECT COUNT(*) FROM {target_schema}.{extracted_table}"
                        )

                    if count > 0:
                        # This table was extracted and has data, so we need to process it too
                        tables_to_process.add(extracted_table)
                        tables_to_check.append(extracted_table)
                        logger.debug(
                            f"Found extracted table {extracted_table} with {count} records, will process for cleanup"
                        )

        return list(tables_to_process)

    async def _check_table_exists(self, db_pool, schema: str, table_name: str) -> bool:
        """Check if a table exists in the database."""
        try:
            async with db_pool.acquire() as conn:
                result = await conn.fetchval(
                    """
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_schema = $1 AND table_name = $2
                    )
                    """,
                    schema,
                    table_name.lower(),
                )
                return result
        except Exception as e:
            logger.warning(f"Error checking if table {schema}.{table_name} exists: {e}")
            return False

    async def _get_list_columns(
        self, db_pool, schema: str, table_name: str
    ) -> list[str]:
        """Get columns that contain JSON arrays."""
        query = """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = $1 AND table_name = $2
            AND data_type IN ('json', 'jsonb', 'text')
        """

        list_columns = []

        try:
            async with db_pool.acquire() as conn:
                columns = await conn.fetch(query, schema, table_name.lower())

                if not columns:
                    logger.debug(f"No text/json columns found in {schema}.{table_name}")
                    return []

                logger.debug(
                    f"Found {len(columns)} text/json columns in {schema}.{table_name}: {[col['column_name'] for col in columns]}"
                )

                # Sample the table to check which columns have arrays
                sample_query = f"""
                    SELECT * FROM {schema}.{table_name}
                    LIMIT 100
                """

                try:
                    samples = await conn.fetch(sample_query)
                    logger.debug(
                        f"Sampled {len(samples)} rows from {schema}.{table_name}"
                    )
                except Exception as e:
                    logger.warning(f"Could not sample {schema}.{table_name}: {e}")
                    return []

                for col in columns:
                    col_name = col["column_name"]
                    col_type = col["data_type"]

                    # Check if any sample has an array in this column
                    for sample in samples:
                        value = sample.get(col_name)
                        if value:
                            try:
                                if isinstance(value, str):
                                    # Try to parse as JSON
                                    parsed = json.loads(value)
                                    if isinstance(parsed, list):
                                        list_columns.append(col_name)
                                        logger.debug(
                                            f"Found list column {col_name} (JSON string) in {schema}.{table_name}"
                                        )
                                        break
                                elif isinstance(value, list):
                                    list_columns.append(col_name)
                                    logger.debug(
                                        f"Found list column {col_name} (native list) in {schema}.{table_name}"
                                    )
                                    break
                                elif col_type in ("json", "jsonb") and isinstance(
                                    value, dict
                                ):
                                    # For JSON columns, check if it's a list-like structure
                                    # This handles cases where the JSON might be stored as a dict but represents a list
                                    pass
                            except (json.JSONDecodeError, TypeError):
                                # Not valid JSON, continue to next sample
                                continue

        except Exception as e:
            logger.error(f"Error getting list columns for {schema}.{table_name}: {e}")
            return []

        logger.debug(f"Final list columns for {schema}.{table_name}: {list_columns}")
        return list_columns

    async def _extract_list_column(
        self,
        db_pool,
        schema: str,
        table_name: str,
        column_name: str,
        batch_size: int,
        parent_id_field: str = None,
        config: Any = None,
    ) -> dict[str, int]:
        """Extract a single list column to a separate table."""
        stats = {"records": 0, "errors": 0}

        # Create target table name
        target_table = f"{table_name}_{column_name}".lower()

        # Process in batches
        offset = 0

        while True:
            # Get batch of records with non-null list values
            # Use the specified parent_id_field (config.id_field for main tables, 'id' for related/extracted tables)
            query = f"""
                SELECT {parent_id_field}, {column_name}
                FROM {schema}.{table_name}
                WHERE {column_name} IS NOT NULL
                AND {column_name} != 'null'
                AND {column_name} != '[]'
                ORDER BY {parent_id_field}
                LIMIT {batch_size} OFFSET {offset}
            """

            async with db_pool.acquire() as conn:
                rows = await conn.fetch(query)

            if not rows:
                break

            # Batch records for bulk insert
            batch_records = []

            # Process each row
            for row in rows:
                parent_id = row[parent_id_field]  # Use the specified ID field as parent
                list_data = row[column_name]

                try:
                    # Parse the list
                    if isinstance(list_data, str):
                        items = json.loads(list_data)
                    else:
                        items = list_data

                    if not isinstance(items, list):
                        continue

                    # Create records for each list item
                    for idx, item in enumerate(items):
                        if isinstance(item, dict):
                            # Flatten the item
                            flat_item = self._flatten_dict(item)
                        else:
                            # Simple value
                            flat_item = {"value": str(item)}

                        # Add hierarchical IDs
                        # Use the deterministic ID of the parent row as the foreign key
                        flat_item = self._add_hierarchical_ids(
                            flat_item,
                            target_table,
                            parent_id,
                            table_name,
                            config=config,
                        )
                        flat_item["list_index"] = idx

                        batch_records.append(flat_item)
                        stats["records"] += 1

                except json.JSONDecodeError as e:
                    # Log the problematic row data for debugging
                    logger.error(
                        f"JSON decode error extracting list from {table_name}.{column_name}: {e}"
                    )
                    logger.error(
                        f"Problematic row - parent_id: {parent_id}, list_data type: {type(list_data)}, "
                        f"list_data length: {len(str(list_data)) if list_data else 0}, "
                        f"list_data preview: {str(list_data)[:200] if list_data else 'None'}"
                    )
                    stats["errors"] += 1
                except Exception as e:
                    logger.error(
                        f"Error extracting list from {table_name}.{column_name}: {e}"
                    )
                    logger.error(
                        f"Problematic row - parent_id: {parent_id}, list_data type: {type(list_data)}, "
                        f"list_data: {list_data}"
                    )
                    stats["errors"] += 1

            # Store batch using optimized normalizer storage
            if batch_records:
                await self.normalizer_storage.store_normalized_records(
                    table_name=target_table,
                    records=batch_records,
                    record_type="nested",
                    ensure_table=True,
                    checkpoint=False,
                )

            offset += len(rows)

        return stats

    def _flatten_dict(self, data: dict[str, Any], prefix: str = "") -> dict[str, Any]:
        """Flatten nested dictionary structure."""
        flattened = {}

        for key, value in data.items():
            new_key = f"{prefix}_{key}" if prefix else key

            if isinstance(value, dict):
                flattened.update(self._flatten_dict(value, new_key))
            elif isinstance(value, list):
                # Store lists as JSON for later extraction
                flattened[new_key] = json.dumps(value) if value else None
            else:
                if value is None:
                    flattened[new_key] = None
                elif isinstance(value, int | float | bool | str):
                    flattened[new_key] = value
                else:
                    flattened[new_key] = str(value)

        return flattened

    def _extract_lists_with_hierarchical_ids(
        self, data: dict[str, Any], parent_id: str, parent_table_name: str, config
    ) -> dict[str, list[dict[str, Any]]]:
        """
        Extract list fields to separate table records with hierarchical IDs.

        Args:
            data: Data dictionary containing lists
            parent_id: ID of parent record
            parent_table_name: Name of parent table
            config: Configuration object

        Returns:
            Dictionary mapping table names to lists of records
        """
        extracted = {}

        for key, value in data.items():
            # Only extract lists, not dictionaries (dicts are flattened in _flatten_dict)
            if not isinstance(value, list) or not value:
                continue

            table_name = f"{parent_table_name}_{key}".lower()
            records = []

            for idx, item in enumerate(value):
                if isinstance(item, dict):
                    flat_item = self._flatten_dict(item)
                    # Add hierarchical IDs
                    flat_item = self._add_hierarchical_ids(
                        flat_item,
                        table_name,
                        parent_id,
                        parent_table_name,
                        config=config,
                    )
                    flat_item["list_index"] = idx
                    records.append(flat_item)
                else:
                    record = {
                        "value": str(item),
                        "list_index": idx,
                    }
                    # Add hierarchical IDs
                    record = self._add_hierarchical_ids(
                        record, table_name, parent_id, parent_table_name, config=config
                    )
                    records.append(record)

            if records:
                extracted[table_name] = records

        return extracted

    def _extract_lists(
        self, data: dict[str, Any], parent_id: str, config
    ) -> dict[str, list[dict[str, Any]]]:
        """Extract list fields to separate table records."""
        extracted = {}

        for key, value in data.items():
            # Only extract lists, not dictionaries (dicts are flattened in _flatten_dict)
            if not isinstance(value, list) or not value:
                continue

            table_name = f"{config.table_name}_{key}".lower()
            records = []

            for idx, item in enumerate(value):
                if isinstance(item, dict):
                    flat_item = self._flatten_dict(item)
                    flat_item[config.id_field] = parent_id
                    flat_item["id"] = f"{parent_id}_{key}_{idx}"
                    records.append(flat_item)
                else:
                    record = {
                        "value": str(item),
                        config.id_field: parent_id,
                        "id": f"{parent_id}_{key}_{idx}",
                    }
                    records.append(record)

            if records:
                extracted[table_name] = records

        return extracted

    async def test_normalizer(self, data_type: str) -> dict[str, Any]:
        """Test normalizer configuration for a data type."""
        try:
            config = self.registry.get_data_type_config(data_type)
            schema_names = self.registry.get_schema_names(data_type)

            return {
                "data_type": data_type,
                "config_available": config is not None,
                "main_table": config.table_name if config else None,
                "id_field": config.id_field if config else None,
                "related_tables": getattr(config, "related_tables", [])
                if config
                else [],
                "schemas": schema_names,
            }
        except Exception as e:
            return {
                "data_type": data_type,
                "config_available": False,
                "error": str(e),
            }

    def _clear_related_table_checkpoint(
        self, staging_checkpoint, related_table_key: str
    ):
        """Clear checkpoint for a related table to force reprocessing."""
        try:
            # Get the checkpoint and reset it
            checkpoint = staging_checkpoint.cm.get_or_create_checkpoint(
                ProcessingStage.STAGING,
                StagingPhase.JSONB_TO_STAGING.value,
                related_table_key,
            )

            # Reset checkpoint state
            checkpoint.processed_items = 0
            checkpoint.current_offset = 0
            checkpoint.failed_items = 0
            checkpoint.skipped_items = 0
            checkpoint.error_message = None

            # Save the reset checkpoint
            staging_checkpoint.save_checkpoint(checkpoint)

            logger.info(f"Cleared checkpoint for {related_table_key}")

        except Exception as e:
            logger.warning(f"Error clearing checkpoint for {related_table_key}: {e}")

    async def cleanup(self):
        """Clean up normalizer resources."""
        try:
            # Ensure final flush
            if self.normalizer_storage:
                await self.normalizer_storage.flush_all()
        except Exception as e:
            logger.warning(f"Error during normalizer cleanup: {e}")

        logger.info("StreamlinedNormalizer cleanup completed")
