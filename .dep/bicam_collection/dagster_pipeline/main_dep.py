#!/usr/bin/env python3
"""
Comprehensive Main Entry Point for Bicam Data Processing

This script provides a unified command-line interface for:
- Running data processing across all data sources (congressional, govinfo, etc.)
- Database setup and management
- Status monitoring
- Configuration management

Usage:
    python -m src.bicam_collection.dagster_pipeline.main --help
"""

import argparse
import asyncio
import contextlib
import logging
import multiprocessing
import os
import signal
import sys
import threading
from pathlib import Path

from dotenv import load_dotenv

from ..libs.data_type_router import get_global_registry
from .shared_resources import ProcessingResource

# Add the project root to the path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

logger = logging.getLogger(__name__)

# Global variables for tracking current run
current_run_id = None
current_run_manager = None
current_db_pool = None
interrupted = False
cleanup_lock = threading.Lock()
cleanup_done = False


def setup_logging(debug: bool = False):
    """Set up logging configuration."""
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(), logging.FileHandler("bicam_processing.log")],
    )


def load_configuration():
    """Load configuration from environment variables for all data sources."""
    load_dotenv()

    # Load API keys for different data sources
    api_keys_by_source = {}

    # Congressional API keys (multiple variable names supported)
    congressional_keys = []
    for env_name in [
        "CONGRESSIONAL_API_KEY",
        "CONGRESS_API_KEYS",
        "CONGRESSIONAL_API_KEYS",
    ]:
        env_value = os.getenv(env_name, "")
        if env_value:
            keys = [key.strip() for key in env_value.split(",") if key.strip()]
            congressional_keys.extend(keys)

    if congressional_keys:
        # Remove duplicates while preserving order
        seen = set()
        unique_keys = []
        for key in congressional_keys:
            if key not in seen:
                seen.add(key)
                unique_keys.append(key)
        api_keys_by_source["congressional"] = unique_keys
        logger.info(f"Loaded {len(unique_keys)} Congressional API keys")

    # GovInfo API keys
    govinfo_keys = []
    for env_name in ["GOVINFO_API_KEY", "GOVINFO_API_KEYS"]:
        env_value = os.getenv(env_name, "")
        if env_value:
            keys = [key.strip() for key in env_value.split(",") if key.strip()]
            govinfo_keys.extend(keys)

    if govinfo_keys:
        # Remove duplicates while preserving order
        seen = set()
        unique_keys = []
        for key in govinfo_keys:
            if key not in seen:
                seen.add(key)
                unique_keys.append(key)
        api_keys_by_source["govinfo"] = unique_keys
        logger.info(f"Loaded {len(unique_keys)} GovInfo API keys")

    if not api_keys_by_source:
        raise ValueError(
            "No API keys found. Set one of: CONGRESSIONAL_API_KEY, GOVINFO_API_KEY"
        )

    # Calculate parallelization settings with CPU core optimization
    cpu_cores = multiprocessing.cpu_count()
    logger.info(f"System has {cpu_cores} CPU cores")

    # Fetcher parallelization (limited by API keys)
    fetcher_parallelization_config = {}
    for data_source, keys in api_keys_by_source.items():
        num_keys = len(keys)
        if num_keys >= 2:
            # Calculate optimal sessions: min(CPU cores, keys // 2)
            max_possible_sessions = num_keys // 2
            num_sessions = min(cpu_cores, max_possible_sessions)
            keys_per_session = 2
            logger.info(
                f"{data_source} fetcher: {num_keys} keys → {num_sessions} parallel sessions "
                f"(max possible: {max_possible_sessions}, CPU cores: {cpu_cores}) "
                f"with {keys_per_session} keys each"
            )
        else:
            # Single key case
            num_sessions = 1
            keys_per_session = 1
            logger.info(
                f"{data_source} fetcher: {num_keys} keys → {num_sessions} session with {keys_per_session} key"
            )

        fetcher_parallelization_config[data_source] = {
            "total_keys": num_keys,
            "num_sessions": num_sessions,
            "keys_per_session": keys_per_session,
            "effective_concurrency": num_sessions,
        }

    # Normalizer/Cleaner parallelization (CPU cores - 4, no API key constraints)
    secondary_parallel_sessions = max(
        1, cpu_cores - 4
    )  # Save 4 cores for other processes
    logger.info(
        f"Normalizer/Cleaner: {secondary_parallel_sessions} parallel sessions (CPU cores - 4)"
    )

    secondary_parallelization_config = {
        "num_sessions": secondary_parallel_sessions,
        "effective_concurrency": secondary_parallel_sessions,
        "type": "cpu_based",  # Distinguish from API key-based config
    }

    # Combined parallelization config for backward compatibility
    parallelization_config = {
        "fetcher": fetcher_parallelization_config,
        "normalizer": secondary_parallelization_config,
        "cleaner": secondary_parallelization_config,  # Same config for cleaner
        # Legacy format for backward compatibility
        **fetcher_parallelization_config,
    }

    return {
        "api_keys_by_source": api_keys_by_source,
        "parallelization_config": parallelization_config,
        "db_host": os.getenv("POSTGRESQL_HOST", "localhost"),
        "db_port": int(os.getenv("POSTGRESQL_PORT", "5432")),
        "db_name": os.getenv("POSTGRESQL_DATABASE", "bicam_collection"),
        "db_user": os.getenv("POSTGRESQL_USERNAME", "bicam_user"),
        "db_password": os.getenv("POSTGRESQL_PASSWORD", "bicam_password"),
        "api_rate_limit": float(os.getenv("API_RATE_LIMIT", "2.0")),
        "batch_size": int(os.getenv("BATCH_SIZE", "100")),
        "max_concurrent": int(os.getenv("MAX_CONCURRENT", "5")),
        "checkpoint_db_path": os.getenv(
            "CHECKPOINT_DB_PATH", "data/checkpoints/checkpoints.db"
        ),
        "run_db_path": os.getenv("RUN_DB_PATH", "data/runs"),
        "incremental": os.getenv("INCREMENTAL", "true").lower() == "true",
        "fallback_days": int(os.getenv("FALLBACK_DAYS", "0")),
    }


def create_processing_resource(
    config: dict,
    data_source: str,
    from_date: str | None = None,
    to_date: str | None = None,
    congress: int | None = None,
    processing_type: str = "fetcher",  # "fetcher", "normalizer", or "cleaner"
) -> ProcessingResource:
    """Create a processing resource with the given configuration for a specific data source."""
    # Get API keys for the specific data source
    api_keys_by_source = config["api_keys_by_source"]
    parallelization_config = config["parallelization_config"]

    if data_source not in api_keys_by_source:
        available_sources = list(api_keys_by_source.keys())
        raise ValueError(
            f"No API keys configured for data source '{data_source}'. "
            f"Available sources: {available_sources}"
        )

    api_keys = api_keys_by_source[data_source]

    # Check if parallelization is enabled
    enable_parallelization = config.get("enable_parallelization", False)

    # Select appropriate parallelization config based on processing type
    if enable_parallelization and parallelization_config:
        if processing_type in ["normalizer", "cleaner"]:
            # Use CPU-based parallelization for normalizers/cleaners
            actual_parallelization_config = parallelization_config.get(
                processing_type, {}
            )
            logger.info(
                f"Parallelization enabled for {processing_type} with config: {actual_parallelization_config}"
            )
        else:
            # Use API key-based parallelization for fetchers (legacy behavior)
            actual_parallelization_config = parallelization_config.get("fetcher", {})
            # Also check legacy format for backward compatibility
            if (
                not actual_parallelization_config
                and data_source in parallelization_config
            ):
                actual_parallelization_config = {
                    data_source: parallelization_config[data_source]
                }
            logger.info(
                f"Parallelization enabled for {processing_type} with config: {actual_parallelization_config}"
            )
    else:
        actual_parallelization_config = {}
        logger.info("Parallelization disabled - using single client processing")

    # Use the path from config or default to workspace root
    checkpoint_db_path = config.get(
        "checkpoint_db_path", "data/checkpoints/checkpoints.db"
    )

    return ProcessingResource(
        api_keys=api_keys,
        parallelization_config=actual_parallelization_config,
        checkpoint_db_path=checkpoint_db_path,
        from_date=from_date,
        to_date=to_date,
        congress=congress,
        incremental=config.get("incremental", True),
        batch_size=config.get("batch_size", 100),
        fallback_days=config.get("fallback_days", 30),
        use_checkpoint_resume=config.get("use_checkpoint_resume", False),
        use_incremental_dates=config.get("use_incremental_dates", True),
    )


def run_data_type_processing(
    data_type: str,
    config: dict,
    phases: list[str] | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
    congress: int | None = None,
    resume: bool = False,
    rerun: bool = False,
):
    """
    Run processing for a specific data type.

    Args:
        data_type: The data type to process (e.g., 'bills', 'amendments')
        config: Configuration dictionary
        phases: List of phases to run (optional)
        from_date: Override start date
        to_date: Override end date
        congress: Override congress number
        resume: If True, use SQLite checkpoint system to resume from last processed item.
                If False, use database last_processed_date for incremental fetching.
    """

    # Original implementation: separate event loops for setup and cleanup,
    # synchronous Dagster execution in between.  This avoids nested loop
    # conflicts with Dagster while still keeping our async phases separate.

    try:
        # Step 1 – async setup
        asyncio.run(
            _setup_processing(
                data_type,
                phases,
                config,
                from_date,
                to_date,
                congress,
                resume,
                rerun,
            )
        )

        # Step 2 – Dagster (synchronous)
        _execute_dagster_pipeline(
            data_type,
            config,
            phases,
            from_date,
            to_date,
            congress,
        )

        # Step 3 – async cleanup (success)
        asyncio.run(_cleanup_processing(True, None))

    except KeyboardInterrupt:
        asyncio.run(_cleanup_processing(False, "Process interrupted by user"))
        logger.warning("Processing interrupted")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error during processing: {e}")
        asyncio.run(_cleanup_processing(False, str(e)))
        sys.exit(1)


async def _setup_processing(
    data_type: str,
    phases: list[str],
    config: dict,
    from_date: str,
    to_date: str,
    congress: int,
    resume: bool,
    rerun: bool,
):
    """Async setup for processing including run tracking and validation."""
    global current_run_id

    # Start run tracking
    current_run_id = await start_run_tracking(data_type, phases, config)
    check_interrupted()

    # Validate data type
    registry = get_global_registry()
    if data_type not in registry.list_data_types():
        error_message = f"Unknown data type: {data_type}"
        logger.error(error_message)
        logger.info("Available data types:")
        for dt in registry.list_data_types():
            data_source = registry.get_data_source(dt)
            logger.info(f"  - {dt} (source: {data_source})")
        raise ValueError(error_message)

    # Get data source for this data type
    data_source = registry.get_data_source(data_type)
    logger.info(f"Processing {data_type} from {data_source} data source")

    check_interrupted()

    # Check if explicit date range is provided
    explicit_dates_provided = from_date is not None or to_date is not None

    if explicit_dates_provided:
        logger.info(
            f"Explicit date range provided: from_date={from_date}, to_date={to_date}"
        )
        logger.info("Disabling incremental processing to use explicit date range")
        config["use_checkpoint_resume"] = False
        config["use_incremental_dates"] = False
    elif resume:
        logger.info("Resume mode: enabled")
        logger.info(
            "Will use SQLite checkpoint system to resume from last processed item"
        )
        config["use_checkpoint_resume"] = True
        config["use_incremental_dates"] = False
    else:
        logger.info("Incremental mode: enabled")
        logger.info(
            "Will use database last_processed_date system for incremental fetching"
        )
        config["use_checkpoint_resume"] = False
        config["use_incremental_dates"] = config.get("incremental", True)

    # Set rerun mode
    config["rerun_mode"] = rerun
    if rerun:
        logger.info(
            "Rerun mode: enabled - will reprocess all items regardless of checkpoint status"
        )
    else:
        logger.info("Checkpoint mode: enabled - will skip already processed items")

    # Show appropriate system info - only when not using explicit dates
    if explicit_dates_provided:
        logger.info(
            f"Will process data for explicit date range: {from_date} to {to_date}"
        )
    elif config["use_incremental_dates"]:
        fallback_days = config.get("fallback_days", 30)
        await show_last_processed_date_info(data_type, config, fallback_days)
    elif config["use_checkpoint_resume"]:
        logger.info("Will check SQLite checkpoint database for resumption point")

    check_interrupted()

    return current_run_id


def _execute_dagster_pipeline(
    data_type: str,
    config: dict,
    phases: list[str],
    from_date: str,
    to_date: str,
    congress: int,
):
    """Sync Dagster pipeline execution."""
    global current_db_pool

    # Get data source
    registry = get_global_registry()
    data_source = registry.get_data_source(data_type)

    # Create processing resource with coordination flags
    processing_resource = create_processing_resource(
        config, data_source, from_date, to_date, congress
    )

    # Don't try to extract the database pool - let RunManager create its own
    # This avoids event loop coordination issues
    logger.info("Using independent database pools for pipeline and run tracking")

    # Create and execute assets using data type router
    logger.info(f"Creating assets for data type: {data_type}")

    # Use the data type router to check if the data type is registered
    if not registry.is_registered(data_type):
        available_types = registry.list_data_types()
        error_message = f"Data type '{data_type}' is not registered. Available types: {available_types}"
        raise ValueError(error_message)

    check_interrupted()

    # Create generalized assets for this data type (works for any registered type)
    from .generalized_assets import (
        create_production_data_asset,
        create_raw_data_asset,
        create_staging_data_asset,
    )

    assets = [
        create_raw_data_asset(data_type),
        create_staging_data_asset(data_type),
        create_production_data_asset(data_type),
    ]

    logger.info(
        f"Created {len(assets)} assets for data type: {data_type} ({data_source})"
    )

    # Create a simple definitions object
    from dagster import Definitions

    definitions = Definitions(
        assets=assets, resources={"processing_resource": processing_resource}
    )

    logger.info(f"Processing {data_type} data...")

    check_interrupted()

    try:
        # Run based on specified phases or all phases
        if phases:
            # Run specific phases
            for phase in phases:
                check_interrupted()

                asset_name = f"{data_type}_{phase}_data"

                logger.info(f"Running {phase} phase for {data_type}")

                from dagster import materialize

                phase_assets = [
                    asset for asset in assets if asset.key.path[-1] == asset_name
                ]

                if not phase_assets:
                    error_message = f"No asset found for phase '{phase}'. Available phases: raw, staging, production"
                    logger.error(error_message)
                    raise ValueError(f"Invalid phase: {phase}")

                check_interrupted()

                result = materialize(
                    phase_assets,
                    resources={"processing_resource": processing_resource},
                )

                if result.success:
                    logger.info(f"Successfully completed {phase} phase for {data_type}")
                else:
                    error_message = f"Failed to complete {phase} phase for {data_type}"
                    logger.error(error_message)
                    raise Exception(f"Pipeline execution failed for {phase} phase")

            logger.info(
                f"Successfully completed {len(phases)} phase(s) for {data_type}: {', '.join(phases)}"
            )

        else:
            # Run all phases in sequence
            logger.info(f"Running full pipeline for {data_type}")

            # Define phase order (simplified to core phases)
            phases = ["raw", "staging", "production"]

            for current_phase in phases:
                check_interrupted()

                asset_name = f"{data_type}_{current_phase}_data"

                logger.info(f"Running {current_phase} phase...")

                from dagster import materialize

                phase_assets = [
                    asset for asset in assets if asset.key.path[-1] == asset_name
                ]

                if not phase_assets:
                    error_message = f"No asset found for phase '{current_phase}'"
                    logger.error(error_message)
                    raise ValueError(f"Invalid phase: {current_phase}")

                check_interrupted()

                result = materialize(
                    phase_assets,
                    resources={"processing_resource": processing_resource},
                )

                if result.success:
                    logger.info(
                        f"Successfully completed {current_phase} phase for {data_type}"
                    )
                else:
                    error_message = (
                        f"Failed to complete {current_phase} phase for {data_type}"
                    )
                    logger.error(error_message)
                    raise Exception(
                        f"Pipeline execution failed for {current_phase} phase"
                    )

            logger.info(f"Successfully completed all phases for {data_type}")

    finally:
        # Ensure the processing resource is properly cleaned up
        try:
            # Create a new event loop for cleanup if the current one is closed
            try:
                current_loop = asyncio.get_running_loop()
                # If we get here, we have a running loop
                cleanup_task = current_loop.create_task(processing_resource.cleanup())
                # Don't await it here - let the loop handle it when it can
                logger.debug("Scheduled processing resource cleanup task")
            except RuntimeError:
                # No running loop, create one for cleanup
                cleanup_loop = asyncio.new_event_loop()
                asyncio.set_event_loop(cleanup_loop)
                try:
                    cleanup_loop.run_until_complete(processing_resource.cleanup())
                    logger.debug("Completed processing resource cleanup in new loop")
                finally:
                    cleanup_loop.close()
        except Exception as e:
            logger.warning(f"Error during processing resource cleanup: {e}")


async def _cleanup_processing(success: bool, error_message: str = None):
    """Async cleanup for processing including run tracking."""
    global current_run_id, current_run_manager, current_db_pool

    # Check if already interrupted
    if interrupted:
        logger.info("Cleanup already handled by interrupt handler")
        return

    # Small delay to ensure any ongoing database operations complete
    await asyncio.sleep(0.1)

    # Add timeout to the entire cleanup process
    try:
        await asyncio.wait_for(
            end_run_tracking(success, error_message),
            timeout=10.0,  # Total timeout for cleanup
        )
    except TimeoutError:
        logger.warning("Cleanup process timed out")
        # Force cleanup global variables
        current_run_id = None
        current_run_manager = None
        if current_db_pool:
            with contextlib.suppress(Exception):
                current_db_pool.terminate()  # Force terminate instead of graceful close
            current_db_pool = None
    except Exception as e:
        logger.error(f"Error during cleanup: {e}")


async def show_last_processed_date_info(
    data_type: str, config: dict, fallback_days: int
):
    """Show information about the last processed date for incremental fetching."""
    try:
        import asyncpg

        from ..api_clients import CongressionalAPIClient

        # Create database pool
        db_pool = await asyncpg.create_pool(
            host=config["db_host"],
            port=config["db_port"],
            database=config["db_name"],
            user=config["db_user"],
            password=config["db_password"],
            min_size=1,
            max_size=5,
        )

        # Get data source for this data type to get appropriate API keys
        registry = get_global_registry()
        data_source = registry.get_data_source(data_type)

        # Get API keys for the data source
        api_keys_by_source = config["api_keys_by_source"]
        if data_source not in api_keys_by_source:
            logger.warning(f"No API keys configured for data source '{data_source}'")
            return

        api_keys = api_keys_by_source[data_source]

        # Create API client to check last processed date
        client = CongressionalAPIClient(api_keys=api_keys, db_pool=db_pool)

        last_date = await client.access_last_processed_date(data_type)

        if last_date:
            logger.info(f"Last processed date for {data_type}: {last_date}")
            logger.info(f"Will fetch data updated since: {last_date}")
        else:
            if fallback_days is None or fallback_days <= 0:
                logger.info(f"No last processed date found for {data_type}")
                logger.info("Will fetch ALL available data (no fallback limit)")
            else:
                from datetime import UTC, datetime, timedelta

                fallback_date = datetime.now(UTC) - timedelta(days=fallback_days)
                fallback_str = fallback_date.strftime("%Y-%m-%d")
                logger.info(f"No last processed date found for {data_type}")
                logger.info(
                    f"Will fetch data from {fallback_days} days ago: {fallback_str}"
                )

        await db_pool.close()

    except Exception as e:
        logger.warning(f"Could not check last processed date: {e}")


async def check_last_processed_dates():
    """Check the last processed dates for all registered data types."""
    config = load_configuration()

    logger.info("Last Processed Dates:")
    logger.info("=" * 50)

    try:
        import asyncpg

        from ..api_clients import CongressionalAPIClient

        # Create database pool
        db_pool = await asyncpg.create_pool(
            host=config["db_host"],
            port=config["db_port"],
            database=config["db_name"],
            user=config["db_user"],
            password=config["db_password"],
            min_size=1,
            max_size=5,
        )

        # Get all registered data types and check by data source
        registry = get_global_registry()
        data_types = registry.list_data_types()
        api_keys_by_source = config["api_keys_by_source"]

        # Group data types by data source for efficiency
        types_by_source = {}
        for data_type in data_types:
            data_source = registry.get_data_source(data_type)
            if data_source not in types_by_source:
                types_by_source[data_source] = []
            types_by_source[data_source].append(data_type)

        for data_source, source_data_types in types_by_source.items():
            if data_source not in api_keys_by_source:
                logger.warning(
                    f"No API keys configured for data source '{data_source}', skipping..."
                )
                continue

            api_keys = api_keys_by_source[data_source]

            # Create API client for this data source
            client = CongressionalAPIClient(api_keys=api_keys, db_pool=db_pool)

            for data_type in source_data_types:
                logger.info(f"Data Type: {data_type} (source: {data_source})")
                last_date = await client.access_last_processed_date(data_type)

                if last_date:
                    logger.info(f"  Last processed: {last_date}")
                else:
                    logger.info("  No last processed date found")

        await db_pool.close()

    except Exception as e:
        logger.error(f"Error checking last processed dates: {e}")


async def reset_last_processed_dates(data_types: list[str] = None):
    """Reset the last processed dates for specified or all registered data types."""
    config = load_configuration()

    registry = get_global_registry()

    if data_types:
        # Validate provided data types
        available_types = registry.list_data_types()
        invalid_types = [dt for dt in data_types if dt not in available_types]
        if invalid_types:
            logger.error(f"Invalid data types: {invalid_types}")
            logger.info(f"Available types: {available_types}")
            return
    else:
        # Reset all data types
        data_types = registry.list_data_types()

    logger.info(f"Resetting last processed dates for: {', '.join(data_types)}")

    try:
        import asyncpg

        # Create database pool
        db_pool = await asyncpg.create_pool(
            host=config["db_host"],
            port=config["db_port"],
            database=config["db_name"],
            user=config["db_user"],
            password=config["db_password"],
            min_size=1,
            max_size=5,
        )

        async with db_pool.acquire() as conn:
            for data_type in data_types:
                await conn.execute(
                    """
                    DELETE FROM bicam_metadata.last_processed_dates
                    WHERE data_type = $1
                    """,
                    data_type,
                )
                logger.info(f"Reset last processed date for {data_type}")

        await db_pool.close()
        logger.info("Last processed dates reset successfully!")

    except Exception as e:
        logger.error(f"Error resetting last processed dates: {e}")


async def clear_checkpoints(data_type: str, stage: str = None):
    """Clear checkpoints for a specific data type and optionally specific stage."""
    config = load_configuration()

    # Validate data type
    registry = get_global_registry()
    if data_type not in registry.list_data_types():
        available_types = registry.list_data_types()
        logger.error(f"Invalid data type: {data_type}")
        logger.info(f"Available types: {available_types}")
        return

    if stage:
        if stage == "all":
            logger.info(f"Clearing all checkpoints for data type: {data_type}")
        else:
            logger.info(f"Clearing {stage} checkpoints for data type: {data_type}")
    else:
        logger.info(f"Clearing all checkpoints for data type: {data_type}")

    try:
        import sqlite3
        from pathlib import Path

        # Get checkpoint database path
        checkpoint_db_path = config.get(
            "checkpoint_db_path", "data/checkpoints/checkpoints.db"
        )
        checkpoint_path = Path(checkpoint_db_path)

        if not checkpoint_path.exists():
            logger.info(
                f"Checkpoint database {checkpoint_path} does not exist - nothing to clear"
            )
            return

        # Create list of data type variations to clear based on stage parameter
        if stage and stage != "all":
            # Clear only the specific stage - map stage names to ProcessingStage values
            stage_mapping = {
                "scraping": "scraping",
                "normalization": "normalization",
                "cleaning": "cleaning",
                "analysis": "analysis",
            }
            if stage in stage_mapping:
                # For non-scraping stages, the data_type is formatted as "{data_type}_{stage}"
                if stage == "scraping":
                    data_type_patterns = [data_type]
                else:
                    data_type_patterns = [f"{data_type}_{stage_mapping[stage]}"]
            else:
                logger.error(
                    f"Invalid stage: {stage}. Valid stages: scraping, normalization, cleaning, analysis"
                )
                return
        else:
            # Clear all stages - include base data type and all stage variations
            data_type_patterns = [
                data_type,  # For scraping stage
                f"{data_type}_normalization",  # For normalization stage
                f"{data_type}_cleaning",  # For cleaning stage
                f"{data_type}_analysis",  # For analysis stage
            ]

        total_cleared = 0

        # Get the data source (scraper_type) for this data type
        data_source = registry.get_data_source(data_type)
        scraper_type = data_source  # Use the actual data source as scraper_type
        logger.info(f"Using scraper_type '{scraper_type}' for data type '{data_type}'")

        conn = sqlite3.connect(str(checkpoint_path))
        cursor = conn.cursor()

        try:
            # Clear from each checkpoint table for matching data types
            checkpoint_tables = [
                "checkpoints",
                "checkpoint_errors",
                "processed_items",
                "processing_phases",
            ]

            for table_name in checkpoint_tables:
                # Check if table exists
                cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                    (table_name,),
                )
                if not cursor.fetchone():
                    continue

                for data_type_pattern in data_type_patterns:
                    # For the main checkpoints table, delete by scraper_type and data_type
                    if table_name == "checkpoints":
                        cursor.execute(
                            f"SELECT COUNT(*) FROM {table_name} WHERE scraper_type=? AND data_type=?",
                            (scraper_type, data_type_pattern),
                        )
                        count = cursor.fetchone()[0]

                        if count > 0:
                            # Get checkpoint IDs first for cascading deletes
                            cursor.execute(
                                f"SELECT id FROM {table_name} WHERE scraper_type=? AND data_type=?",
                                (scraper_type, data_type_pattern),
                            )
                            checkpoint_ids = [row[0] for row in cursor.fetchall()]

                            # Delete from dependent tables first
                            for checkpoint_id in checkpoint_ids:
                                for dep_table in [
                                    "checkpoint_errors",
                                    "processed_items",
                                    "processing_phases",
                                ]:
                                    cursor.execute(
                                        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                                        (dep_table,),
                                    )
                                    if cursor.fetchone():
                                        cursor.execute(
                                            f"DELETE FROM {dep_table} WHERE checkpoint_id=?",
                                            (checkpoint_id,),
                                        )
                                        deleted_deps = cursor.rowcount
                                        if deleted_deps > 0:
                                            total_cleared += deleted_deps
                                            logger.info(
                                                f"Cleared {deleted_deps} entries from {dep_table} (checkpoint_id: {checkpoint_id})"
                                            )

                            # Finally delete from main checkpoints table
                            cursor.execute(
                                f"DELETE FROM {table_name} WHERE scraper_type=? AND data_type=?",
                                (scraper_type, data_type_pattern),
                            )
                            deleted = cursor.rowcount
                            total_cleared += deleted
                            logger.info(
                                f"Cleared {deleted} checkpoint entries for {scraper_type}:{data_type_pattern}"
                            )

            conn.commit()

        finally:
            conn.close()

        if stage and stage != "all":
            logger.info(
                f"Successfully cleared {total_cleared} checkpoint entries for {data_type} {stage} stage"
            )
            if total_cleared == 0:
                logger.info(
                    f"No checkpoint entries found for {data_type} {stage} stage"
                )
        else:
            logger.info(
                f"Successfully cleared {total_cleared} checkpoint entries for {data_type} (all stages)"
            )
            if total_cleared == 0:
                logger.info(f"No checkpoint entries found for data type '{data_type}'")

    except Exception as e:
        logger.error(f"Error clearing checkpoints: {e}")


def list_data_types():
    """List all registered data types organized by data source."""
    registry = get_global_registry()
    data_sources = registry.list_data_sources()

    logger.info("Available Data Types by Source:")
    logger.info("=" * 40)

    if not data_sources:
        logger.info("No data sources registered.")
        return

    total_types = 0
    for data_source in sorted(data_sources):
        data_types = registry.list_data_types_by_source(data_source)
        logger.info(f"\n{data_source.upper()}:")
        for data_type in sorted(data_types):
            config_file = registry.get_config_file(data_type)
            logger.info(f"  - {data_type} (config: {config_file})")
        total_types += len(data_types)

    logger.info(
        f"\nTotal: {total_types} data types across {len(data_sources)} data sources"
    )
    logger.info("\nFramework Info:")
    logger.info("- All data types use generalized 3-phase processing")
    logger.info("- Phase order: raw → staging → production")
    logger.info(
        "- Data types auto-register from their fetcher/normalizer/cleaner classes"
    )
    logger.info("- Supports multiple data sources (congressional, govinfo, etc.)")


async def setup_database(recreate: bool = False):
    """Setup database schemas and tables."""
    logger.info("Setting up database schemas and tables...")
    if recreate:
        logger.info("Recreate mode enabled - will drop and recreate all schemas")

    config_dict = load_configuration()

    try:
        from ..libs.config import BicamConfig, DatabaseConfig
        from ..libs.database import setup_database as db_setup

        # Create proper BicamConfig object from dictionary
        db_config = DatabaseConfig(
            host=config_dict["db_host"],
            port=config_dict["db_port"],
            database=config_dict["db_name"],
            username=config_dict["db_user"],
            password=config_dict["db_password"],
        )

        bicam_config = BicamConfig(database=db_config)

        # Use the proper setup function with recreate support
        success = await db_setup(bicam_config, recreate=recreate)

        if success:
            logger.info("Database setup completed successfully!")
        else:
            raise Exception("Database setup failed")

    except Exception as e:
        logger.error(f"Database setup failed: {e}")
        raise


def setup_signal_handlers():
    """Set up signal handlers for graceful shutdown."""

    def signal_handler(signum, frame):
        global interrupted, cleanup_done

        with cleanup_lock:
            if cleanup_done:
                # Force exit if cleanup already attempted
                logger.warning("Force exiting...")
                os._exit(1)

            interrupted = True
            cleanup_done = True

        signal_name = signal.Signals(signum).name
        logger.warning(
            f"Received {signal_name} signal. Initiating immediate shutdown..."
        )

        # Try to run cleanup with a timeout
        try:
            # Create a new event loop for cleanup if needed
            loop = None
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)

            # Run cleanup with timeout
            cleanup_task = asyncio.create_task(handle_interrupt())
            try:
                loop.run_until_complete(asyncio.wait_for(cleanup_task, timeout=5.0))
            except TimeoutError:
                logger.warning("Cleanup timed out, forcing exit")
                os._exit(1)

        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
            os._exit(1)

    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)  # Ctrl+C
    signal.signal(signal.SIGTERM, signal_handler)  # Termination signal


async def handle_interrupt():
    """Handle process interruption by updating run status and cleaning up."""
    global current_run_id, current_run_manager, current_db_pool

    try:
        logger.info("Starting interrupt cleanup...")

        # Set a shorter timeout for all operations
        timeout = 3.0

        if current_run_manager and current_run_id:
            logger.info(
                f"Updating run {current_run_id} status to 'failed' (interrupted)..."
            )

            try:
                # Use timeout for database operations
                await asyncio.wait_for(
                    current_run_manager.fail_run(
                        current_run_id, "Process interrupted by user (SIGINT/SIGTERM)"
                    ),
                    timeout=timeout,
                )
                logger.info(
                    f"Run {current_run_id} marked as failed due to interruption"
                )
            except TimeoutError:
                logger.warning("Timeout updating run status, continuing cleanup")
            except Exception as e:
                logger.warning(f"Error updating run status: {e}")

        # Clean up run manager
        if current_run_manager:
            try:
                await asyncio.wait_for(current_run_manager.cleanup(), timeout=timeout)
                logger.info("Run manager cleaned up")
            except TimeoutError:
                logger.warning("Timeout cleaning up run manager")
            except Exception as e:
                logger.warning(f"Error cleaning up run manager: {e}")

        # Close database pool if open
        if current_db_pool:
            try:
                await asyncio.wait_for(current_db_pool.close(), timeout=timeout)
                logger.info("Database pool closed")
            except TimeoutError:
                logger.warning("Timeout closing database pool")
            except Exception as e:
                logger.warning(f"Error closing database pool: {e}")

    except Exception as e:
        logger.error(f"Error during interrupt handling: {e}")
    finally:
        logger.info("Interrupt cleanup complete, exiting")
        # Force exit after cleanup
        os._exit(1)


def check_interrupted():
    """Check if process has been interrupted and raise exception if so."""
    global interrupted
    if interrupted:
        raise KeyboardInterrupt("Process interrupted")


async def start_run_tracking(
    data_type: str, phases: list[str] = None, config: dict = None
):
    """Start tracking a new run in the database."""
    global current_run_id, current_run_manager, current_db_pool

    try:
        from ..libs.run_tracking import RunManager, RunMetadata, RunType

        # Use SQLite for run tracking to avoid database pool conflicts entirely
        # This eliminates the "another operation is in progress" issues with PostgreSQL pools
        run_db_path = config.get("run_db_path", "data/runs") if config else "data/runs"
        current_run_manager = RunManager(
            use_postgres=False, db_path=Path(run_db_path) / "bills_runs.db"
        )
        await current_run_manager.initialize()

        logger.debug(
            "RunManager initialized with SQLite database to avoid pool conflicts"
        )

        # Determine run type based on phases
        if phases and "production" in phases:
            run_type = RunType.DATA_CLEANING  # Production phase involves cleaning
        else:
            # Full pipeline - determine based on data source
            registry = get_global_registry()
            data_source = registry.get_data_source(data_type)
            if data_source == "congressional":
                run_type = RunType.SCRAPER_CONGRESSIONAL
            elif data_source == "govinfo":
                run_type = RunType.SCRAPER_GOVINFO
            else:
                run_type = RunType.OTHER

        # Create run metadata
        run_metadata = RunMetadata(
            run_id="",  # Will be set by create_run
            run_type=run_type,
            system_name="bicam_main_pipeline",
            description=f"Processing {data_type}"
            + (f" ({', '.join(phases)} phases)" if phases else " (full pipeline)"),
            parameters={
                "data_type": data_type,
                "phases": phases,
                "incremental": config.get("incremental", True) if config else True,
                "batch_size": config.get("batch_size", 100) if config else 100,
                "from_date": config.get("from_date") if config else None,
                "to_date": config.get("to_date") if config else None,
            },
            data_types=[data_type],
            priority=3,  # Medium priority
            tags=["dagster", "pipeline", data_type],
        )

        # Create and start the run
        current_run_id = current_run_manager.create_run(run_metadata)
        logger.info(f"Created run: {current_run_id}")

        # Start the run (move from pending to running) - SQLite operations are synchronous and reliable
        try:
            start_result = await current_run_manager.start_run(current_run_id)
            if start_result:
                logger.info(f"Started run tracking: {current_run_id} for {data_type}")
            else:
                logger.warning(f"Failed to start run: {current_run_id}")
        except Exception as e:
            logger.warning(f"Error starting run: {e}")

        return current_run_id

    except Exception as e:
        logger.error(f"Failed to start run tracking: {e}")
        return None


async def end_run_tracking(success: bool, error_message: str = None):
    """End the current run tracking with success/failure status."""
    global current_run_id, current_run_manager, current_db_pool

    try:
        if current_run_manager and current_run_id:
            # SQLite operations are much more reliable and don't have the pool conflicts
            try:
                if success:
                    complete_result = await current_run_manager.complete_run(
                        current_run_id
                    )
                    if complete_result:
                        logger.info(f"Run {current_run_id} completed successfully")
                    else:
                        logger.warning(
                            f"Failed to mark run {current_run_id} as completed"
                        )
                else:
                    fail_result = await current_run_manager.fail_run(
                        current_run_id, error_message or "Unknown error"
                    )
                    if fail_result:
                        logger.info(f"Run {current_run_id} failed: {error_message}")
                    else:
                        logger.warning(f"Failed to mark run {current_run_id} as failed")

            except Exception as e:
                logger.warning(f"Error updating run status: {e}")
                # Continue anyway - don't let run tracking failures block pipeline success
                logger.info(
                    "Pipeline execution was successful despite run tracking issues"
                )

        # Clean up run manager - SQLite cleanup is simple and reliable
        if current_run_manager:
            try:
                await current_run_manager.cleanup()
                logger.info("Run manager cleaned up successfully")
            except Exception as e:
                logger.warning(f"Error during run manager cleanup: {e}")

        logger.info("Run tracking cleanup completed")

    except Exception as e:
        logger.error(f"Error ending run tracking: {e}")
        # Don't let run tracking errors prevent pipeline success
        logger.info("Pipeline execution was successful despite run tracking issues")
    finally:
        # Always reset global variables to prevent resource leaks
        current_run_id = None
        current_run_manager = None
        current_db_pool = None  # Reset reference but don't close


def main():
    """Main entry point for the bicam data processing CLI."""
    # Set up signal handlers for graceful shutdown
    setup_signal_handlers()

    parser = argparse.ArgumentParser(
        description="Bicam Data Processing Pipeline - Multi-Source Support",
        epilog="""
Examples:
    python -m src.bicam_collection.dagster_pipeline.main setup
    python -m src.bicam_collection.dagster_pipeline.main process bills --resume
    python -m src.bicam_collection.dagster_pipeline.main process bills --phases raw staging --parallel
    python -m src.bicam_collection.dagster_pipeline.main clear-checkpoints bills normalization
    python -m src.bicam_collection.dagster_pipeline.main clear-checkpoints bills all
    """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Process data command
    process_parser = subparsers.add_parser("process", help="Process data by type")
    process_parser.add_argument(
        "data_type",
        help="Data type to process (e.g., bills, amendments, committees, etc.)",
    )
    process_parser.add_argument(
        "--phases",
        nargs="+",  # Allow multiple phases
        choices=["raw", "staging", "production", "summary", "quality"],
        help="Specific phase(s) to run (can specify multiple, e.g., --phase raw staging)",
    )
    process_parser.add_argument("--from-date", help="Start date (YYYY-MM-DD)")
    process_parser.add_argument("--to-date", help="End date (YYYY-MM-DD)")
    process_parser.add_argument("--congress", type=int, help="Congress number")
    process_parser.add_argument(
        "--parallel",
        action="store_true",
        help="Enable parallel processing with API key pairs",
    )
    process_parser.add_argument(
        "--no-incremental", action="store_true", help="Disable incremental processing"
    )
    process_parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from last checkpoint (uses SQLite checkpoints). If not provided, uses database last_processed_date for incremental fetching.",
    )
    process_parser.add_argument(
        "--rerun",
        action="store_true",
        help="Reprocess all items, ignoring checkpoint status. By default, already processed items are skipped.",
    )
    process_parser.set_defaults(command="process")

    # Database setup command
    setup_parser = subparsers.add_parser("setup", help="Setup database")
    setup_parser.add_argument(
        "--recreate", action="store_true", help="Drop and recreate all schemas"
    )

    # Clear checkpoints command
    clear_parser = subparsers.add_parser(
        "clear-checkpoints",
        help="Clear checkpoints for a data type and optionally specific stage",
    )
    clear_parser.add_argument(
        "data_type",
        help="Data type to clear checkpoints for (e.g., bills, amendments, committees, etc.)",
    )
    clear_parser.add_argument(
        "stage",
        nargs="?",
        choices=["scraping", "normalization", "cleaning", "analysis", "all"],
        help="Specific stage to clear (scraping=fetching, normalization=staging, cleaning=production, analysis=analysis). If not specified, clears all stages for the data type.",
    )

    # Status commands
    status_parser = subparsers.add_parser(
        "check-dates", help="Check last processed dates"
    )
    reset_parser = subparsers.add_parser(
        "reset-dates", help="Reset last processed dates"
    )
    reset_parser.add_argument(
        "--data-types", nargs="+", help="Specific data types to reset (default: all)"
    )

    # List data types command
    list_parser = subparsers.add_parser("list-types", help="List available data types")

    # Global options
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")

    args = parser.parse_args()

    # Setup logging
    setup_logging(args.debug)

    if args.command == "process":
        # Load configuration
        config = load_configuration()
        if args.no_incremental:
            config["incremental"] = False

        # Validate data type first
        from ..libs.data_type_router import get_global_registry

        registry = get_global_registry()
        if not registry.is_registered(args.data_type):
            available_types = registry.list_data_types()
            logger.error(
                f"Invalid data type: {args.data_type}. Available: {available_types}"
            )
            return

        # Set parallelization if requested
        if hasattr(args, "parallel") and args.parallel:
            config["enable_parallelization"] = True
            logger.info("Parallel processing enabled with API key pairs")
        else:
            config["enable_parallelization"] = False

        # Process data using new execution engine
        run_data_type_processing(
            data_type=args.data_type,
            config=config,
            phases=args.phases,
            from_date=args.from_date,
            to_date=args.to_date,
            congress=args.congress,
            rerun=args.rerun,
        )

    elif args.command == "setup":
        asyncio.run(setup_database(recreate=args.recreate))

    elif args.command == "check-dates":
        asyncio.run(check_last_processed_dates())

    elif args.command == "reset-dates":
        data_types_arg = getattr(args, "data_types", None)
        asyncio.run(reset_last_processed_dates(data_types_arg))

    elif args.command == "list-types":
        list_data_types()

    elif args.command == "clear-checkpoints":
        stage = getattr(args, "stage", None)
        asyncio.run(clear_checkpoints(args.data_type, stage))

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
