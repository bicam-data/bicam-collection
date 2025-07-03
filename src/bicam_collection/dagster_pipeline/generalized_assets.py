"""
Generalized Asset Factory for Bicam Data Types

This module provides a factory-based approach to creating Dagster assets for any
data type from any data source (congressional, govinfo, etc.). Instead of creating
separate pipeline files for each data type, this system generates assets dynamically
based on configuration.

Key Features:
- Asset factories for raw, staging, and production phases
- Dynamic table-level asset generation
- Shared resource management with API key coordination
- Configuration-driven approach
- Consistent naming conventions across all data types
- Support for multiple data sources

Usage:
    # Create assets for a specific data type
    bills_assets = create_data_type_assets("bills")

    # Create assets for multiple data types
    all_assets = create_all_data_type_assets(["bills", "amendments", "members"])

    # Create job for specific data type
    bills_job = create_data_type_job("bills")
"""

import asyncio
import logging
from typing import Any

from dagster import (
    AssetExecutionContext,
    Config,
    asset,
    get_dagster_logger,
    job,
)

from ..libs.data_type_router import get_global_registry
from .shared_resources import ProcessingResource

logger = logging.getLogger(__name__)


class DataTypeConfig(Config):
    """Configuration for data type processing."""

    data_type: str
    from_date: str | None = None
    to_date: str | None = None
    congress: int | None = None
    batch_size: int = 100
    phases: list[int] = [1, 2, 3]  # Which phases to run


def create_raw_data_asset(data_type: str):
    """Factory function to create raw data asset for any data type."""

    @asset(
        name=f"{data_type}_raw_data",
        group_name=f"{data_type}_pipeline",
        compute_kind="fetch",
        description=f"Raw {data_type} data from API",
    )
    async def raw_data_asset(
        context: AssetExecutionContext,
        processing_resource: ProcessingResource,
    ) -> dict[str, Any]:
        """
        Stage 1: Fetch raw data from API.

        This asset fetches data using the 3-phase API approach and stores
        it in the appropriate raw schema based on the data source.
        """
        dagster_logger = get_dagster_logger()
        dagster_logger.info(f"Starting raw data fetching for {data_type}")

        # Get data type specific components
        registry = get_global_registry()
        if not registry.is_registered(data_type):
            available_types = registry.list_data_types()
            raise ValueError(
                f"Unsupported data type: {data_type}. Available: {available_types}"
            )

        # Get schema names for this data type
        schema_names = registry.get_schema_names(data_type)
        data_source = registry.get_data_source(data_type)

        dagster_logger.info(f"Processing {data_type} from {data_source} data source")
        dagster_logger.info(f"Target schema: {schema_names['raw']}")

        # Setup infrastructure
        db_pool = await processing_resource.get_db_pool()
        checkpoint_manager = processing_resource.get_checkpoint_manager()
        run_manager = await processing_resource.get_run_manager(db_pool)

        try:
            # Initialize fetcher using the new parallel processing system
            # Get API clients for parallel processing
            api_clients = (
                await processing_resource.get_api_clients_for_parallel_sessions(
                    data_type
                )
            )
            if not api_clients:
                raise RuntimeError(
                    f"No API clients available for data type: {data_type}"
                )

            # Use first client for fetcher initialization
            # The fetcher will get access to all clients through processing_resource
            primary_client = api_clients[0]

            fetcher_class = registry.get_fetcher_class(data_type)
            fetcher = fetcher_class(
                primary_client, db_pool, checkpoint_manager, run_manager
            )
            progress_tracker = fetcher.setup_progress_tracker()

            # Set the processing resource for parallel session support
            fetcher.set_processing_resource(processing_resource)
            dagster_logger.info(
                f"Processing resource set on fetcher for parallel session support. "
                f"Available clients: {len(api_clients)}"
            )

            # Coordinate checkpoint systems based on configuration flags
            from_date = processing_resource.from_date
            to_date = processing_resource.to_date
            use_checkpoint_resume = getattr(
                processing_resource, "use_checkpoint_resume", False
            )
            use_incremental_dates = getattr(
                processing_resource, "use_incremental_dates", True
            )

            # Determine processing mode
            explicit_dates_provided = from_date is not None or to_date is not None

            if explicit_dates_provided:
                dagster_logger.info(
                    f"Using explicit date range: from_date={from_date}, to_date={to_date}"
                )
                dagster_logger.info("Explicit dates override incremental processing")
            elif use_checkpoint_resume:
                dagster_logger.info("Using SQLite checkpoint system for resumption")
                # When resuming, we don't modify from_date since checkpoint system handles resumption
                if not from_date:
                    dagster_logger.info(
                        "No from_date specified for checkpoint resume - will resume from last checkpoint"
                    )
            elif (
                use_incremental_dates
                and processing_resource.incremental
                and not from_date
            ):
                # Get last processed date from database for incremental fetching
                dagster_logger.info(
                    "Using incremental processing with database last_processed_date"
                )
                last_processed = await primary_client.access_last_processed_date(
                    data_type
                )
                if last_processed:
                    from_date = last_processed
                    dagster_logger.info(
                        f"Using database incremental fetch from: {from_date}"
                    )
                else:
                    # Fallback to configured days
                    from datetime import UTC, datetime, timedelta

                    fallback_date = datetime.now(UTC) - timedelta(
                        days=processing_resource.fallback_days
                    )
                    from_date = fallback_date.strftime("%Y-%m-%dT%H:%M:%SZ")
                    dagster_logger.info(
                        f"No last processed date found, using fallback: {from_date}"
                    )
            else:
                dagster_logger.info(
                    "Using manual processing mode - no date filtering unless explicitly specified"
                )

            # Setup run tracking
            from ..libs.run_tracking import RunMetadata, RunType

            # Use the appropriate run type based on data source
            run_type = getattr(
                RunType, f"SCRAPER_{data_source.upper()}", RunType.SCRAPER_CONGRESSIONAL
            )

            run_metadata = RunMetadata(
                run_id="",
                run_type=run_type,
                system_name=f"{data_type}_fetcher_dagster",
                description=f"Dagster raw data fetching for {data_type} ({data_source})",
                data_types=[data_type],
                parameters={
                    "from_date": from_date,
                    "to_date": processing_resource.to_date,
                    "batch_size": processing_resource.batch_size,
                    "incremental": processing_resource.incremental,
                    "data_source": data_source,
                },
            )

            run_id = run_manager.create_run(run_metadata)
            await run_manager.start_run(run_id)

            # Process with checkpointing
            # For incremental processing, don't set a limit - continue until last_processed_date
            # For checkpoint resume, also don't limit - resume from exact checkpoint
            processing_params = {
                "from_date": from_date,
                "to_date": processing_resource.to_date,
                "batch_id": f"dagster_{data_type}_{context.run.run_id}",
            }

            # Check if parallelization is enabled
            enable_parallelization = bool(
                getattr(processing_resource, "parallelization_config", None)
            )
            if enable_parallelization:
                processing_params["enable_parallelization"] = True
                dagster_logger.info("Parallel session processing enabled")
            else:
                dagster_logger.info("Using single client processing")

            # Only set limit if explicitly provided and not using incremental/resume modes
            if not (use_incremental_dates or use_checkpoint_resume):
                processing_params["limit"] = processing_resource.batch_size
                dagster_logger.info(
                    f"Using batch limit: {processing_resource.batch_size}"
                )
            else:
                dagster_logger.info(
                    "No batch limit - will process all available data until cutoff"
                )

            stats = await fetcher.process_items(**processing_params)

            # Update last processed date only if using incremental system (not checkpoint resume or explicit dates)
            if (
                use_incremental_dates
                and processing_resource.incremental
                and not explicit_dates_provided
                and stats.get("processed", 0) > 0
            ):
                # Update the last processed date to current time
                from datetime import UTC, datetime

                current_time = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
                # Include total_count if available from pagination metadata
                total_count = stats.get("total_count")
                success = await primary_client.update_last_processed_date(
                    data_type, current_time, total_count
                )
                if success:
                    dagster_logger.info(
                        f"Updated database last processed date for {data_type} to: {current_time}"
                        + (f" with total_count: {total_count}" if total_count else "")
                    )
                else:
                    dagster_logger.warning(
                        f"Failed to update database last processed date for {data_type}"
                    )
            elif use_checkpoint_resume:
                dagster_logger.info(
                    "Checkpoint resume mode - not updating database last_processed_date"
                )
            elif explicit_dates_provided:
                dagster_logger.info(
                    "Explicit date mode - not updating database last_processed_date"
                )

            # Complete run and release keys
            output_summary = {
                "stage": "raw_data_fetch",
                "data_type": data_type,
                "data_source": data_source,
                "target_schema": schema_names["raw"],
                "stats": stats,
                "key_manager_status": processing_resource.get_key_manager_status(),
            }

            await run_manager.complete_run(run_id, output_summary=output_summary)
            await processing_resource.release_keys_for_data_type(data_type)

            # Report metrics
            metadata = {
                "data_type": data_type,
                "data_source": data_source,
                "target_schema": schema_names["raw"],
                "processed_items": stats.get("processed", 0),
                "failed_items": stats.get("failed", 0),
                "skipped_items": stats.get("skipped", 0),
                "run_id": run_id,
            }

            # Add total_count to metadata if available
            if stats.get("total_count") is not None:
                metadata["total_count"] = stats["total_count"]
                dagster_logger.info(f"Total count captured: {stats['total_count']}")

            context.add_output_metadata(metadata)

            return {
                "stage": "raw_data",
                "data_type": data_type,
                "data_source": data_source,
                "target_schema": schema_names["raw"],
                "stats": stats,
                "run_id": run_id,
            }

        except Exception as e:
            dagster_logger.error(f"Raw data fetching failed for {data_type}: {e}")
            await processing_resource.release_keys_for_data_type(data_type)
            if "run_id" in locals():
                await run_manager.fail_run(run_id, str(e))
            raise
        finally:
            # Don't cleanup processing_resource here as it's managed by Dagster
            # Just close our local db_pool reference with timeout
            try:
                await asyncio.wait_for(await db_pool.close(), timeout=2.0)
            except Exception as db_error:
                dagster_logger.warning(f"Error closing database pool: {db_error}")

    return raw_data_asset


def create_staging_data_asset(data_type: str):
    """Factory function to create staging data asset for any data type."""

    @asset(
        name=f"{data_type}_staging_data",
        group_name=f"{data_type}_pipeline",
        compute_kind="normalize",
        description=f"Normalized {data_type} data in staging schema",
    )
    async def staging_data_asset(
        context: AssetExecutionContext,
        processing_resource: ProcessingResource,
    ) -> dict[str, Any]:
        """
        Stage 2: Normalize raw data to staging schema.
        """
        dagster_logger = get_dagster_logger()
        dagster_logger.info(f"Starting staging data normalization for {data_type}")

        # Get data type specific components
        registry = get_global_registry()
        if not registry.is_registered(data_type):
            available_types = registry.list_data_types()
            raise ValueError(
                f"Unsupported data type: {data_type}. Available: {available_types}"
            )

        # Get schema names for this data type
        schema_names = registry.get_schema_names(data_type)
        data_source = registry.get_data_source(data_type)

        dagster_logger.info(f"Processing {data_type} from {data_source} data source")
        dagster_logger.info(
            f"Source schema: {schema_names['raw']}, Target schema: {schema_names['staging']}"
        )

        # Setup infrastructure
        db_pool = await processing_resource.get_db_pool()
        checkpoint_manager = processing_resource.get_checkpoint_manager()
        run_manager = await processing_resource.get_run_manager(db_pool)

        try:
            # Initialize normalizer
            normalizer_class = registry.get_normalizer_class(data_type)
            config_path = registry.get_config_file(data_type)
            normalizer = normalizer_class(
                config_path=config_path,
                db_pool=db_pool,
                checkpoint_manager=checkpoint_manager,
                run_manager=run_manager,
            )

            # Set processing resource type for normalizer parallelization
            processing_resource._processing_type = "normalizer"

            # Setup progress tracker for checkpointing
            progress_tracker = normalizer.setup_progress_tracker(checkpoint_manager)
            if not progress_tracker:
                dagster_logger.warning(f"No progress tracker created for {data_type}")

            # Get item IDs from raw data
            item_ids = await _get_item_ids_from_raw_data(
                db_pool, data_type, data_source
            )

            if not item_ids:
                dagster_logger.warning(f"No {data_type} IDs found in raw data")
                return {
                    "stage": "staging_data",
                    "data_type": data_type,
                    "data_source": data_source,
                    "target_schema": schema_names["staging"],
                    "stats": {"processed": 0, "failed": 0, "skipped": 0},
                    "message": "No raw data available",
                }

            # Check if this is a rerun by checking if we're processing staging or if it's specified
            # For now, we'll default to normal mode (rerun=False)
            # The rerun flag would need to be passed through the ProcessingResource
            rerun_mode = getattr(processing_resource, "rerun_mode", False)

            # Process to staging
            stats = await normalizer.process_items(
                item_ids,
                batch_id=f"dagster_staging_{data_type}_{context.run.run_id}",
                rerun=rerun_mode,
            )

            context.add_output_metadata(
                {
                    "data_type": data_type,
                    "data_source": data_source,
                    "source_schema": schema_names["raw"],
                    "target_schema": schema_names["staging"],
                    "processed_items": stats.get("processed", 0),
                    "failed_items": stats.get("failed", 0),
                    "skipped_items": stats.get("skipped", 0),
                }
            )

            return {
                "stage": "staging_data",
                "data_type": data_type,
                "data_source": data_source,
                "target_schema": schema_names["staging"],
                "stats": stats,
            }

        except Exception as e:
            dagster_logger.error(f"Staging normalization failed for {data_type}: {e}")
            raise
        finally:
            try:
                await asyncio.wait_for(db_pool.close(), timeout=2.0)
            except Exception as db_error:
                dagster_logger.warning(f"Error closing database pool: {db_error}")

    return staging_data_asset


def create_production_data_asset(data_type: str):
    """Factory function to create production data asset for any data type."""

    @asset(
        name=f"{data_type}_production_data",
        group_name=f"{data_type}_pipeline",
        compute_kind="clean",
        description=f"Clean {data_type} data in production schema",
    )
    async def production_data_asset(
        context: AssetExecutionContext,
        processing_resource: ProcessingResource,
    ) -> dict[str, Any]:
        """
        Stage 3: Clean staging data to production schema.
        """
        dagster_logger = get_dagster_logger()
        dagster_logger.info(f"Starting production data cleaning for {data_type}")

        # Get data type specific components
        registry = get_global_registry()
        if not registry.is_registered(data_type):
            available_types = registry.list_data_types()
            raise ValueError(
                f"Unsupported data type: {data_type}. Available: {available_types}"
            )

        # Get schema names for this data type
        schema_names = registry.get_schema_names(data_type)
        data_source = registry.get_data_source(data_type)

        dagster_logger.info(f"Processing {data_type} from {data_source} data source")
        dagster_logger.info(
            f"Source schema: {schema_names['staging']}, Target schema: {schema_names['production']}"
        )

        # Setup infrastructure
        db_pool = await processing_resource.get_db_pool()
        checkpoint_manager = processing_resource.get_checkpoint_manager()
        run_manager = await processing_resource.get_run_manager(db_pool)

        try:
            # Initialize cleaner
            cleaner_class = registry.get_cleaner_class(data_type)
            config_path = registry.get_config_file(data_type)
            cleaner = cleaner_class(
                config_path=config_path,
                db_pool=db_pool,
                checkpoint_manager=checkpoint_manager,
                run_manager=run_manager,
            )

            # Set processing resource type for cleaner parallelization
            processing_resource._processing_type = "cleaner"

            # Process to production
            stats = await cleaner.process_items(
                batch_id=f"dagster_production_{data_type}_{context.run.run_id}",
            )

            context.add_output_metadata(
                {
                    "data_type": data_type,
                    "data_source": data_source,
                    "source_schema": schema_names["staging"],
                    "target_schema": schema_names["production"],
                    "processed_items": stats.get("processed", 0),
                    "failed_items": stats.get("failed", 0),
                    "skipped_items": stats.get("skipped", 0),
                }
            )

            return {
                "stage": "production_data",
                "data_type": data_type,
                "data_source": data_source,
                "target_schema": schema_names["production"],
                "stats": stats,
            }

        except Exception as e:
            dagster_logger.error(f"Production cleaning failed for {data_type}: {e}")
            raise
        finally:
            try:
                await asyncio.wait_for(db_pool.close(), timeout=2.0)
            except Exception as db_error:
                dagster_logger.warning(f"Error closing database pool: {db_error}")

    return production_data_asset


def create_table_level_assets(data_type: str) -> list:
    """Create individual table-level assets for granular processing."""
    registry = get_global_registry()
    if not registry.is_registered(data_type):
        return []

    assets = []
    # For now, we'll implement table discovery dynamically when we have more data types
    # This is a placeholder that could be enhanced with schema introspection
    table_names = _get_table_names_for_data_type(data_type)

    # Create staging table assets
    for table_name in table_names:
        staging_asset = create_staging_table_asset(data_type, table_name)
        production_asset = create_production_table_asset(data_type, table_name)
        assets.extend([staging_asset, production_asset])

    return assets


def create_staging_table_asset(data_type: str, table_name: str):
    """Create asset for individual staging table."""

    @asset(
        name=f"{table_name}_staging",
        group_name=f"{data_type}_tables_staging",
        compute_kind="normalize_table",
        description=f"Staging table: {table_name}",
    )
    async def staging_table_asset(
        context: AssetExecutionContext,
        processing_resource: ProcessingResource,
    ) -> dict[str, Any]:
        """Normalize specific table to staging."""
        dagster_logger = get_dagster_logger()
        dagster_logger.info(f"Processing staging table: {table_name}")

        # Get data source info
        registry = get_global_registry()
        data_source = registry.get_data_source(data_type)
        schema_names = registry.get_schema_names(data_type)

        # Implementation similar to full staging but for specific table
        # This allows granular processing of individual tables

        return {
            "table": table_name,
            "stage": "staging",
            "data_type": data_type,
            "data_source": data_source,
            "target_schema": schema_names["staging"],
        }

    return staging_table_asset


def create_production_table_asset(data_type: str, table_name: str):
    """Create asset for individual production table."""

    @asset(
        name=f"{table_name}",  # Clean name without suffix for production
        group_name=f"{data_type}_tables_production",
        compute_kind="clean_table",
        description=f"Production table: {table_name}",
    )
    async def production_table_asset(
        context: AssetExecutionContext,
        processing_resource: ProcessingResource,
    ) -> dict[str, Any]:
        """Clean specific table to production."""
        dagster_logger = get_dagster_logger()
        dagster_logger.info(f"Processing production table: {table_name}")

        # Get data source info
        registry = get_global_registry()
        data_source = registry.get_data_source(data_type)
        schema_names = registry.get_schema_names(data_type)

        # Implementation similar to full production but for specific table

        return {
            "table": table_name,
            "stage": "production",
            "data_type": data_type,
            "data_source": data_source,
            "target_schema": schema_names["production"],
        }

    return production_table_asset


def create_data_type_assets(data_type: str) -> list:
    """Create all assets for a specific data type."""
    assets = []

    # Main pipeline assets
    assets.append(create_raw_data_asset(data_type))
    assets.append(create_staging_data_asset(data_type))
    assets.append(create_production_data_asset(data_type))

    # Table-level assets for granular control
    table_assets = create_table_level_assets(data_type)
    assets.extend(table_assets)

    # Try to load specialized assets from the data type's module
    try:
        specialized_assets = _load_specialized_assets(data_type)
        if specialized_assets:
            assets.extend(specialized_assets)
            logger.info(
                f"Added {len(specialized_assets)} specialized assets for {data_type}"
            )
    except Exception as e:
        logger.debug(f"No specialized assets found for {data_type}: {e}")

    return assets


def _load_specialized_assets(data_type: str) -> list:
    """Load specialized assets from the data type's module."""
    import importlib

    try:
        # Get the data source for this data type from the registry
        registry = get_global_registry()
        data_source = registry.get_data_source(data_type)

        # Try to import from the data type's module using the data source
        module_path = f"..data_types.{data_source}.{data_type}"
        data_type_module = importlib.import_module(module_path, package=__name__)

        # Use the lazy getter to avoid circular imports
        if hasattr(data_type_module, "get_specialized_assets"):
            specialized_assets = data_type_module.get_specialized_assets()
            return specialized_assets or []
        else:
            logger.debug(f"No get_specialized_assets function found for {data_type}")
            return []

    except ImportError as e:
        logger.debug(f"No specialized assets module for {data_type}: {e}")
        return []
    except Exception as e:
        logger.warning(f"Error loading specialized assets for {data_type}: {e}")
        return []


def create_all_data_type_assets(data_types: list[str] = None) -> list:
    """Create assets for multiple data types."""
    if data_types is None:
        registry = get_global_registry()
        data_types = registry.list_data_types()

    all_assets = []
    for data_type in data_types:
        assets = create_data_type_assets(data_type)
        all_assets.extend(assets)

    return all_assets


def _get_table_names_for_data_type(data_type: str) -> list[str]:
    """Get table names for a specific data type from its config file."""
    registry = get_global_registry()

    if not registry.is_registered(data_type):
        logger.warning(f"Data type '{data_type}' not registered")
        return [data_type]

    # Use the registry to extract table names from config
    return registry.get_table_names(data_type)


def create_data_type_job(data_type: str, phases: list[int] = None):
    """Create a job for processing a specific data type."""

    @job(name=f"{data_type}_processing_job")
    def data_type_job():
        """Job for processing specific data type through all phases."""
        if phases is None or 1 in phases:
            raw_result = globals()[f"{data_type}_raw_data"]()

        if phases is None or 2 in phases:
            staging_result = globals()[f"{data_type}_staging_data"]()

        if phases is None or 3 in phases:
            production_result = globals()[f"{data_type}_production_data"]()

    return data_type_job


# Helper functions
async def _get_item_ids_from_raw_data(
    db_pool, data_type: str, data_source: str
) -> list[str]:
    """Get item IDs from raw data for a specific data type."""
    async with db_pool.acquire() as conn:
        try:
            # Query the raw data table for unique source document IDs
            raw_table = f"bicam_raw_{data_source}.{data_type}_raw"
            rows = await conn.fetch(f"""
                SELECT DISTINCT source_doc_id
                FROM {raw_table}
                WHERE source_doc_id IS NOT NULL
                ORDER BY source_doc_id
            """)
            return [row["source_doc_id"] for row in rows]
        except Exception as e:
            logger.warning(f"Failed to get item IDs from raw data for {data_type}: {e}")
            return []


async def _get_item_ids_from_staging_data(
    db_pool, data_type: str, data_source: str
) -> list[str]:
    """Get item IDs from staging data for a specific data type."""
    async with db_pool.acquire() as conn:
        try:
            # Query the staging data table for unique IDs
            staging_table = f"bicam_staging_{data_source}.{data_type}"

            # Use data_type specific ID field (e.g., bill_id, amendment_id, etc.)
            id_field = f"{data_type.rstrip('s')}_id"  # bills -> bill_id, amendments -> amendment_id

            rows = await conn.fetch(f"""
                SELECT DISTINCT {id_field}
                FROM {staging_table}
                WHERE {id_field} IS NOT NULL
                ORDER BY {id_field}
            """)
            return [row[id_field] for row in rows]
        except Exception as e:
            logger.warning(
                f"Failed to get item IDs from staging data for {data_type}: {e}"
            )
            return []
