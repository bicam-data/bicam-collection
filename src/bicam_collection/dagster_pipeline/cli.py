"""Light-weight CLI replacing the monolithic *main.py*.

Usage examples:
    python -m bicam_collection.dagster_pipeline.cli process bills
    python -m bicam_collection.dagster_pipeline.cli process bills --phases raw staging
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import sys

from ..libs.config import BicamConfig
from .shared_resources import ProcessingResource

logging.basicConfig(level=os.getenv("LOGLEVEL", "INFO"))
logger = logging.getLogger("bicam.cli")

# Global processing resource for cleanup
_processing_resource: ProcessingResource | None = None
_interrupted = False


def setup_signal_handlers():
    """Set up signal handlers for graceful shutdown."""

    def signal_handler(signum, frame):
        global _interrupted
        signal_name = signal.Signals(signum).name
        logger.warning(f"Received {signal_name} signal. Initiating shutdown...")
        _interrupted = True

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)


def check_interrupted():
    """Check if process has been interrupted."""
    if _interrupted:
        raise KeyboardInterrupt("Process interrupted")


async def _build_processing_resource(
    config: BicamConfig,
    *,
    parallel_enabled: bool = False,
    from_date: str | None = None,
    to_date: str | None = None,
    congress: int | None = None,
    resume: bool = False,
    no_incremental: bool = False,
    rerun: bool = False,
) -> ProcessingResource:
    """Create a fully-configured ProcessingResource from config and CLI flags."""

    # Get API keys from config
    api_keys = []
    if hasattr(config, "api_keys") and config.api_keys:
        api_keys = config.api_keys
    elif hasattr(config, "congressional") and hasattr(config.congressional, "api_keys"):
        api_keys = config.congressional.api_keys
    else:
        # Fallback to environment variables
        env_keys = os.getenv(
            "CONGRESSIONAL_API_KEYS", os.getenv("CONGRESSIONAL_API_KEY", "")
        )
        govinfo_keys = os.getenv("GOVINFO_API_KEYS", "")
        if env_keys:
            api_keys.extend([key.strip() for key in env_keys.split(",") if key.strip()])
        if govinfo_keys:
            api_keys.extend(
                [key.strip() for key in govinfo_keys.split(",") if key.strip()]
            )
    if not api_keys:
        raise ValueError(
            "No API keys found. Set CONGRESSIONAL_API_KEYS or CONGRESSIONAL_API_KEY"
        )

    # Configure parallelization
    parallelization_config = {}
    if parallel_enabled:
        sessions = getattr(config.processing, "max_workers", 2)
        parallelization_config = {
            "congressional": {
                "num_sessions": sessions,
                "keys_per_session": 2,
            }
        }

    # Determine incremental settings
    explicit_dates_provided = from_date is not None or to_date is not None
    use_checkpoint_resume = resume and not explicit_dates_provided
    use_incremental_dates = (
        not explicit_dates_provided and not resume and not no_incremental
    )
    incremental_flag = not no_incremental

    return ProcessingResource(
        # Database configuration
        db_host=config.database.host,
        db_port=config.database.port,
        db_name=config.database.database,
        db_user=config.database.username,
        db_password=config.database.password,
        # API configuration
        api_keys=api_keys,
        api_rate_limit=getattr(config.scraping, "rate_limit_delay", 1.5),
        # Processing configuration
        batch_size=getattr(config.processing, "chunk_size", 100),
        parallelization_config=parallelization_config,
        # Processing parameters
        from_date=from_date,
        to_date=to_date,
        congress=congress,
        # Checkpoint configuration
        incremental=incremental_flag,
        use_checkpoint_resume=use_checkpoint_resume,
        use_incremental_dates=use_incremental_dates,
        rerun_mode=rerun,
        use_postgres_checkpoints=getattr(
            config.processing, "use_postgres_checkpoints", True
        ),
    )


async def _cmd_process(args: argparse.Namespace):
    """Process command implementation using the three-phase approach."""
    global _processing_resource

    try:
        # Phase 1: Async setup
        await _setup_processing(args)

        # Phase 2: Synchronous Dagster execution
        _execute_dagster_pipeline(args)

        # Phase 3: Async cleanup (success)
        await _cleanup_processing(success=True)

    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        await _cleanup_processing(
            success=False, error_message="Process interrupted by user"
        )
        sys.exit(1)
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        await _cleanup_processing(success=False, error_message=str(e))
        sys.exit(1)


async def _setup_processing(args: argparse.Namespace):
    """Phase 1: Async setup including configuration, validation, and resource creation."""
    global _processing_resource

    logger.info("Starting async setup phase...")

    # Load configuration
    config = BicamConfig.from_env()
    logger.debug("Loaded configuration: %s", config)

    # Validate data type
    from ..libs.data_type_router import get_global_registry

    registry = get_global_registry()

    if not registry.is_registered(args.data_type):
        available_types = registry.list_data_types()
        raise ValueError(
            f"Data type '{args.data_type}' is not registered. Available: {available_types}"
        )

    data_source = registry.get_data_source(args.data_type)
    logger.info(f"Processing {args.data_type} from {data_source} data source")

    # Create processing resource
    _processing_resource = await _build_processing_resource(
        config,
        parallel_enabled=args.parallel,
        from_date=args.from_date,
        to_date=args.to_date,
        congress=args.congress,
        resume=args.resume,
        no_incremental=args.no_incremental,
        rerun=args.rerun,
    )

    # Initialize the processing resource (async operations)
    await _processing_resource.initialize()

    logger.info("Async setup phase completed successfully")


def _execute_dagster_pipeline(args: argparse.Namespace):
    """Phase 2: Synchronous Dagster pipeline execution."""
    logger.info("Starting synchronous Dagster execution phase...")

    # Import here to avoid circular imports
    from .pipeline_runner import execute_pipeline_sync

    # Execute the pipeline synchronously
    execute_pipeline_sync(
        processing_resource=_processing_resource,
        data_type=args.data_type,
        phases=args.phases,
    )

    logger.info("Synchronous Dagster execution phase completed successfully")


async def _cleanup_processing(success: bool, error_message: str = None):
    """Phase 3: Async cleanup including resource cleanup and run tracking."""
    global _processing_resource

    logger.info("Starting async cleanup phase...")

    try:
        if _processing_resource:
            # Clean up the processing resource
            await _processing_resource.cleanup()
            logger.info("Processing resource cleaned up successfully")
    except Exception as e:
        logger.warning(f"Error during cleanup: {e}")
    finally:
        _processing_resource = None
        logger.info("Async cleanup phase completed")


def _create_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser("bicam-collection data pipeline")
    sub = p.add_subparsers(dest="command", required=True)

    # ------------------------------------------------------------------
    # process
    proc = sub.add_parser("process", help="Run pipeline for one data-type")
    proc.add_argument("data_type")
    proc.add_argument(
        "--phases",
        nargs="+",
        choices=["raw", "staging", "production"],
        help="Subset of phases to run",
    )
    proc.add_argument("--from-date", help="Start date (YYYY-MM-DD)")
    proc.add_argument("--to-date", help="End date (YYYY-MM-DD)")
    proc.add_argument("--congress", type=int, help="Override congress number")
    proc.add_argument(
        "--parallel",
        action="store_true",
        help="Enable parallel processing",
    )
    proc.add_argument(
        "--no-incremental",
        action="store_true",
        help="Disable incremental fetching",
    )
    proc.add_argument(
        "--resume",
        action="store_true",
        help="Resume from checkpoints",
    )
    proc.add_argument(
        "--rerun",
        action="store_true",
        help="Re-process items ignoring checkpoints",
    )

    # ------------------------------------------------------------------
    # setup-db
    setup_p = sub.add_parser("setup-db", help="Create database schemas/tables")
    setup_p.add_argument("--recreate", action="store_true", help="Drop & recreate")

    # ------------------------------------------------------------------
    # check-dates
    sub.add_parser("check-dates", help="Print last_processed dates")

    # reset-dates
    reset_p = sub.add_parser("reset-dates", help="Delete last_processed entries")
    reset_p.add_argument("--data-types", nargs="+", help="Subset of data-types")

    # clear-checkpoints
    cp = sub.add_parser("clear-checkpoints", help="Delete checkpoints")
    cp.add_argument("data_type")
    cp.add_argument(
        "stage",
        nargs="?",
        choices=["scraping", "normalization", "cleaning", "analysis", "all"],
        help="Stage to clear",
    )

    # list-types
    sub.add_parser("list-types", help="List registered data-types")

    return p


async def main_async(argv: list[str] | None = None):
    """Async main function."""
    parser = _create_parser()
    ns = parser.parse_args(argv)

    if ns.command == "process":
        await _cmd_process(ns)
    elif ns.command == "setup-db":
        from .commands import setup_db

        config = BicamConfig.from_env()
        resource = ProcessingResource(
            db_host=config.database.host,
            db_port=config.database.port,
            db_name=config.database.database,
            db_user=config.database.username,
            db_password=config.database.password,
        )
        try:
            await setup_db(resource, recreate=ns.recreate)
        finally:
            await resource.cleanup()
    elif ns.command == "check-dates":
        from .commands import check_dates

        config = BicamConfig.from_env()
        resource = ProcessingResource(
            db_host=config.database.host,
            db_port=config.database.port,
            db_name=config.database.database,
            db_user=config.database.username,
            db_password=config.database.password,
        )
        try:
            await check_dates(resource)
        finally:
            await resource.cleanup()
    elif ns.command == "reset-dates":
        from .commands import reset_dates

        config = BicamConfig.from_env()
        resource = ProcessingResource(
            db_host=config.database.host,
            db_port=config.database.port,
            db_name=config.database.database,
            db_user=config.database.username,
            db_password=config.database.password,
        )
        try:
            await reset_dates(resource, ns.data_types)
        finally:
            await resource.cleanup()
    elif ns.command == "clear-checkpoints":
        from .commands import clear_checkpoints

        config = BicamConfig.from_env()
        resource = ProcessingResource(
            db_host=config.database.host,
            db_port=config.database.port,
            db_name=config.database.database,
            db_user=config.database.username,
            db_password=config.database.password,
        )
        try:
            await clear_checkpoints(resource, ns.data_type, ns.stage)
        finally:
            await resource.cleanup()
    elif ns.command == "list-types":
        from .commands import list_types

        await list_types()
    else:
        parser.print_help()


def main(argv: list[str] | None = None):
    """Main entry point."""
    setup_signal_handlers()
    try:
        asyncio.run(main_async(argv))
    except KeyboardInterrupt:
        logger.info("Process interrupted")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Command failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
