"""
Streamlined Normalizer

This module provides data normalization from raw/staging to production tables,
using optimized normalizer storage and hierarchical checkpoint system.
"""

import asyncio
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
            batch_size: Size of processing batches
            max_concurrent: Maximum concurrent operations
            rerun: Whether to reprocess existing records (ignores checkpoints)
            resume: Whether to use checkpoint-based resume logic
            **kwargs: Additional parameters for specific data types

        Returns:
            Dictionary containing normalization results and metrics
        """
        logger.info(f"Starting normalization for {data_type}")

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

                # Get items to process
                item_ids = await self._get_items_to_normalize(
                    db_pool,
                    data_type,
                    source_schema,
                    config,
                    jsonb_checkpoint,
                    batch_size * 10,
                )

                if item_ids:
                    jsonb_checkpoint.total_items = len(item_ids)
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

                    results["main_records_processed"] = main_stats["processed"]
                    results["errors"] += main_stats["errors"]

                    # Mark main table as processed
                    staging_checkpoint.mark_table_processed(config.table_name)
                    results["tables_processed"] += 1

            # Phase 2: Process related tables (only those with create_raw: True)
            if hasattr(config, "related_tables") and config.related_tables:
                for related_table in config.related_tables:
                    # Check if this related table has a raw table (create_raw: True)
                    # Get the specific config for this related table
                    related_config = None
                    try:
                        # Try to get config for the related data type
                        related_data_type = f"{data_type}_{related_table}"
                        related_config = self.registry.get_data_type_config(
                            related_data_type
                        )
                    except Exception:
                        # If no specific config, check if it's in the main config
                        pass

                    # Skip if no raw table exists for this related table
                    if (
                        related_config
                        and hasattr(related_config, "create_raw")
                        and not related_config.create_raw
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

                    table_name = f"{data_type}_{related_table}"

                    if not rerun and staging_checkpoint.should_skip_table(table_name):
                        logger.info(
                            f"Related table {table_name} already processed, skipping"
                        )
                        continue

                    logger.info(f"Processing related table: {table_name}")

                    related_stats = await self._process_related_table(
                        db_pool,
                        data_type,
                        source_schema,
                        config,
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

            # Phase 3: Clean up JSON columns that were already extracted in Phase 1
            logger.info(
                "Cleaning up JSON columns that were already extracted in Phase 1"
            )

            cleanup_stats = await self._cleanup_extracted_json_columns(
                db_pool,
                data_type,
                target_schema,
                config,
                staging_checkpoint,
                extract_checkpoint,
                rerun,
            )

            results["lists_extracted"] = cleanup_stats["columns_cleaned"]
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

                        # Check for wrapper key if configured
                        if hasattr(config, "full_key") and config.full_key:
                            if config.full_key in payload:
                                payload = payload[config.full_key]
                            else:
                                logger.warning(
                                    f"Expected wrapper key '{config.full_key}' not found in payload"
                                )
                                return {
                                    "success": False,
                                    "error": f"Missing wrapper key: {config.full_key}",
                                }

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

                        # Extract lists to separate tables
                        extracted_lists = self._extract_lists(
                            payload, record_id, config
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

            # Update checkpoint
            checkpoint.current_offset = batch_start + len(batch_ids)
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
            f"{data_type}_{related_table}",
        )

        source_table = f"{data_type}_{related_table}_raw"
        target_table = f"{data_type}_{related_table}"

        try:
            # Get total count
            async with db_pool.acquire() as conn:
                count_result = await conn.fetchval(
                    f"SELECT COUNT(*) FROM {source_schema}.{source_table}"
                )
                checkpoint.total_items = count_result
                staging_checkpoint.save_checkpoint(checkpoint)
        except Exception as e:
            logger.error(
                f"Could not count records in {source_schema}.{source_table}: {e}"
            )
            return stats

        # Process in batches with offset for resume
        offset = checkpoint.current_offset

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
            except Exception as e:
                logger.error(
                    f"Could not fetch records from {source_schema}.{source_table}: {e}"
                )
                stats["errors"] += 1
                break

            if not rows:
                break

            # Process records
            semaphore = asyncio.Semaphore(max_concurrent)

            async def process_record(record):
                async with semaphore:  # noqa: B023
                    try:
                        payload = record.get("payload", {})
                        if isinstance(payload, str):
                            payload = json.loads(payload)

                        # For related tables, generate ID if needed
                        source_doc_id = record.get("source_doc_id")
                        record_id = (
                            f"{source_doc_id}_{related_table}_{record.get('id', '')}"
                        )

                        # Flatten and add foreign key
                        flat_data = self._flatten_dict(payload)
                        flat_data["id"] = record_id
                        flat_data[config.id_field] = source_doc_id

                        # Store using optimized normalizer storage
                        await self.normalizer_storage.store_normalized_records(
                            table_name=target_table,
                            records=[flat_data],
                            record_type="related",
                            ensure_table=True,
                            checkpoint=False,
                        )

                        return {"success": True}

                    except Exception as e:
                        logger.error(f"Error processing related record: {e}")
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
                    stats["processed"] += 1
                else:
                    stats["errors"] += 1

            # Update checkpoint
            offset += len(rows)
            checkpoint.current_offset = offset
            checkpoint.processed_items = stats["processed"]
            checkpoint.failed_items = stats["errors"]
            staging_checkpoint.save_checkpoint(checkpoint)

        # Final flush
        await self.normalizer_storage.flush_all()

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
        """Extract all list fields to separate tables."""
        stats = {"lists_extracted": 0, "records_processed": 0, "errors": 0}

        # Get all tables to process lists from
        tables_to_process = []

        # Always process the main table if it exists
        main_table_exists = await self._check_table_exists(
            db_pool, target_schema, config.table_name
        )
        if main_table_exists:
            tables_to_process.append(config.table_name)
        else:
            logger.warning(
                f"Main table {target_schema}.{config.table_name} does not exist"
            )

        # Process related tables (only those that actually exist in staging)
        if hasattr(config, "related_tables") and config.related_tables:
            for rt in config.related_tables:
                table_name = f"{data_type}_{rt}"
                if await self._check_table_exists(db_pool, target_schema, table_name):
                    tables_to_process.append(table_name)
                else:
                    logger.info(
                        f"Skipping list extraction for {table_name} - table does not exist in staging"
                    )

        for table_name in tables_to_process:
            # Get columns that are JSON arrays
            list_columns = await self._get_list_columns(
                db_pool, target_schema, table_name
            )

            for column_name in list_columns:
                # Create target table name for this list
                target_table = f"{table_name}_{column_name}"

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
                extract_stats = await self._extract_list_column(
                    db_pool,
                    target_schema,
                    table_name,
                    column_name,
                    config,
                    batch_size,
                    data_type,
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

        # Get all tables to process
        tables_to_process = []

        # Always process the main table if it exists
        main_table_exists = await self._check_table_exists(
            db_pool, target_schema, config.table_name
        )
        if main_table_exists:
            tables_to_process.append(config.table_name)
        else:
            logger.warning(
                f"Main table {target_schema}.{config.table_name} does not exist"
            )

        # Process related tables (only those that actually exist in staging)
        if hasattr(config, "related_tables") and config.related_tables:
            for rt in config.related_tables:
                table_name = f"{data_type}_{rt}"
                if await self._check_table_exists(db_pool, target_schema, table_name):
                    tables_to_process.append(table_name)
                else:
                    logger.info(
                        f"Skipping cleanup for {table_name} - table does not exist in staging"
                    )

        for table_name in tables_to_process:
            # Get columns that are JSON arrays
            list_columns = await self._get_list_columns(
                db_pool, target_schema, table_name
            )

            for column_name in list_columns:
                # Create target table name for this list
                target_table = f"{table_name}_{column_name}"

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

                # Sample the table to check which columns have arrays
                sample_query = f"""
                    SELECT * FROM {schema}.{table_name}
                    LIMIT 100
                """

                try:
                    samples = await conn.fetch(sample_query)
                except Exception as e:
                    logger.warning(f"Could not sample {schema}.{table_name}: {e}")
                    return []

                for col in columns:
                    col_name = col["column_name"]

                    # Check if any sample has an array in this column
                    for sample in samples:
                        value = sample.get(col_name)
                        if value:
                            try:
                                if isinstance(value, str):
                                    parsed = json.loads(value)
                                    if isinstance(parsed, list):
                                        list_columns.append(col_name)
                                        break
                                elif isinstance(value, list):
                                    list_columns.append(col_name)
                                    break
                            except Exception:
                                continue

        except Exception as e:
            logger.error(f"Error getting list columns for {schema}.{table_name}: {e}")
            return []

        return list_columns

    async def _extract_list_column(
        self,
        db_pool,
        schema: str,
        table_name: str,
        column_name: str,
        config,
        batch_size: int,
        data_type: str,
    ) -> dict[str, int]:
        """Extract a single list column to a separate table."""
        stats = {"records": 0, "errors": 0}

        # Create target table name
        target_table = f"{table_name}_{column_name}"

        # Process in batches
        offset = 0

        while True:
            # Get batch of records with non-null list values
            query = f"""
                SELECT {config.id_field}, {column_name}
                FROM {schema}.{table_name}
                WHERE {column_name} IS NOT NULL
                AND {column_name} != 'null'
                AND {column_name} != '[]'
                ORDER BY {config.id_field}
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
                parent_id = row[config.id_field]
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

                        # Add foreign key and ID
                        flat_item[config.id_field] = parent_id
                        flat_item["id"] = f"{parent_id}_{column_name}_{idx}"
                        flat_item["list_index"] = idx

                        batch_records.append(flat_item)
                        stats["records"] += 1

                except Exception as e:
                    logger.error(
                        f"Error extracting list from {table_name}.{column_name}: {e}"
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

    def _extract_lists(
        self, data: dict[str, Any], parent_id: str, config
    ) -> dict[str, list[dict[str, Any]]]:
        """Extract list fields to separate table records."""
        extracted = {}

        for key, value in data.items():
            if not isinstance(value, list) or not value:
                continue

            table_name = f"{config.table_name}_{key}"
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

    async def cleanup(self):
        """Clean up normalizer resources."""
        try:
            # Ensure final flush
            if self.normalizer_storage:
                await self.normalizer_storage.flush_all()
        except Exception as e:
            logger.warning(f"Error during normalizer cleanup: {e}")

        logger.info("StreamlinedNormalizer cleanup completed")
