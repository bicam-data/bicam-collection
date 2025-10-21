"""
Improved CLI with hierarchical checkpoint support.

This version adds comprehensive checkpoint management commands and better
progress tracking across all processing stages.

Usage examples:
    # Basic processing
    python -m bicam_collection.dagster_pipeline.cli process bills

    # View progress across all stages
    python -m bicam_collection.dagster_pipeline.cli progress bills

    # Clear specific checkpoint
    python -m bicam_collection.dagster_pipeline.cli clear-checkpoint bills fetching list_items

    # List all checkpoints
    python -m bicam_collection.dagster_pipeline.cli list-checkpoints bills

    # Retry failed items
    python -m bicam_collection.dagster_pipeline.cli retry-failed bills fetching
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal
import sys
from datetime import datetime

from ..libs.config import BicamConfig
from ..libs.hierarchical_checkpoint_system import (
    CleaningPhase,
    FetchingPhase,
    HierarchicalCheckpointManager,
    ProcessingStage,
    StagingPhase,
)
from .shared_resources import ProcessingResource

logging.basicConfig(
    level=os.getenv("LOGLEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
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


# ==============================================================================
# Enhanced Command Implementations
# ==============================================================================


async def cmd_progress(args: argparse.Namespace):
    """Show detailed progress for a data type across all stages."""
    config = BicamConfig.from_env(env_file=args.env_file)

    # Get checkpoint manager
    checkpoint_manager = _get_checkpoint_manager(config, args.checkpoint_db)

    logger.info(f"\nProcessing Progress for {args.data_type}")
    logger.info("=" * 80)

    # Get progress summary
    progress = checkpoint_manager.get_progress_summary(args.data_type)

    if not progress:
        logger.info(f"No checkpoints found for {args.data_type}")
        return

    # Display progress for each stage
    for stage_name, phases in progress.items():
        logger.info(f"\n{stage_name.upper()}")
        logger.info("-" * 40)

        for phase_name, stats in phases.items():
            # Calculate percentage
            total = stats.get("total", 0)
            processed = stats.get("processed", 0)
            failed = stats.get("failed", 0)
            percent = (processed / total * 100) if total > 0 else 0

            logger.info(f"\n  {phase_name}:")
            logger.info(f"    Progress: {processed:,}/{total:,} ({percent:.1f}%)")
            if failed > 0:
                logger.info(f"    Failed: {failed:,}")

            # Show current position
            current_item = stats.get("current_item")
            if current_item:
                logger.info(f"    Current: {current_item}")

            # Show last update time
            last_updated = stats.get("last_updated")
            if last_updated:
                logger.info(f"    Updated: {last_updated}")

    # Show overall statistics
    logger.info("\n" + "=" * 80)
    logger.info("OVERALL STATISTICS")
    logger.info("-" * 40)

    total_processed = 0
    total_failed = 0

    for phases in progress.values():
        for stats in phases.values():
            total_processed += stats.get("processed", 0)
            total_failed += stats.get("failed", 0)

    logger.info(f"Total Processed: {total_processed:,}")
    logger.info(f"Total Failed: {total_failed:,}")

    # Estimate time remaining for fetching stage
    if ProcessingStage.FETCHING.value in progress:
        fetching = progress[ProcessingStage.FETCHING.value]
        if FetchingPhase.LIST_ITEMS.value in fetching:
            list_stats = fetching[FetchingPhase.LIST_ITEMS.value]
            total = list_stats.get("total", 0)
            processed = list_stats.get("processed", 0)

            if processed > 0 and total > processed:
                # Estimate based on time elapsed
                last_updated = list_stats.get("last_updated")
                if last_updated:
                    try:
                        # Parse the timestamp
                        if isinstance(last_updated, str):
                            last_time = datetime.fromisoformat(
                                last_updated.replace("Z", "+00:00")
                            )
                        else:
                            last_time = last_updated

                        elapsed = (datetime.now() - last_time).total_seconds()
                        rate = processed / elapsed if elapsed > 0 else 0
                        remaining = total - processed
                        eta_seconds = remaining / rate if rate > 0 else 0

                        if eta_seconds > 0:
                            hours = int(eta_seconds // 3600)
                            minutes = int((eta_seconds % 3600) // 60)
                            logger.info(
                                f"\nEstimated time remaining: {hours}h {minutes}m"
                            )
                            logger.info(f"Processing rate: {rate:.0f} items/second")
                    except Exception as e:
                        logger.debug(f"Could not calculate ETA: {e}")


async def cmd_list_checkpoints(args: argparse.Namespace):
    """List all checkpoints for a data type with details."""
    config = BicamConfig.from_env(env_file=args.env_file)
    checkpoint_manager = _get_checkpoint_manager(config, args.checkpoint_db)

    logger.info(f"\nCheckpoints for {args.data_type}")
    logger.info("=" * 80)

    # Get all checkpoints for this data type
    all_stages = [
        ProcessingStage.FETCHING,
        ProcessingStage.STAGING,
        ProcessingStage.CLEANING,
    ]

    checkpoint_count = 0

    for stage in all_stages:
        # Get phases for this stage
        if stage == ProcessingStage.FETCHING:
            phases = list(FetchingPhase)
        elif stage == ProcessingStage.STAGING:
            phases = list(StagingPhase)
        elif stage == ProcessingStage.CLEANING:
            phases = list(CleaningPhase)
        else:
            continue

        stage_has_checkpoints = False

        for phase in phases:
            try:
                checkpoint = checkpoint_manager.get_or_create_checkpoint(
                    stage, phase.value, args.data_type
                )

                # Only show if there's actual progress
                if checkpoint.processed_items > 0 or checkpoint.total_items > 0:
                    if not stage_has_checkpoints:
                        logger.info(f"\n{stage.value.upper()}")
                        logger.info("-" * 40)
                        stage_has_checkpoints = True

                    checkpoint_count += 1

                    logger.info(f"\n  Phase: {phase.value}")
                    logger.info(f"    Processed: {checkpoint.processed_items:,}")
                    logger.info(f"    Failed: {checkpoint.failed_items:,}")
                    logger.info(f"    Total: {checkpoint.total_items:,}")

                    if checkpoint.current_item_id:
                        logger.info(f"    Current Item: {checkpoint.current_item_id}")
                    if checkpoint.current_table:
                        logger.info(f"    Current Table: {checkpoint.current_table}")
                    if checkpoint.current_endpoint:
                        logger.info(
                            f"    Current Endpoint: {checkpoint.current_endpoint}"
                        )

                    if checkpoint.started_at:
                        logger.info(f"    Started: {checkpoint.started_at}")
                    if checkpoint.updated_at:
                        logger.info(f"    Updated: {checkpoint.updated_at}")

                    if checkpoint.error_message:
                        logger.info(f"    Last Error: {checkpoint.error_message}")

            except Exception as e:
                logger.debug(
                    f"Error getting checkpoint for {stage.value}/{phase.value}: {e}"
                )

    if checkpoint_count == 0:
        logger.info("No active checkpoints found")
    else:
        logger.info(f"\nTotal checkpoints: {checkpoint_count}")


async def cmd_clear_checkpoint(args: argparse.Namespace):
    """Clear specific checkpoint(s) with granular control."""
    config = BicamConfig.from_env(env_file=args.env_file)
    checkpoint_manager = _get_checkpoint_manager(config, args.checkpoint_db)

    # Parse stage
    try:
        stage = ProcessingStage(args.stage)
    except ValueError:
        logger.error(f"Invalid stage: {args.stage}")
        logger.error(f"Valid stages: {[s.value for s in ProcessingStage]}")
        return

    # If phase is not specified, clear all phases for the stage
    if not args.phase:
        logger.info(
            f"\nClearing all checkpoints for {args.data_type} in {stage.value} stage"
        )

        # Get all phases for this stage
        if stage == ProcessingStage.FETCHING:
            phases = [p.value for p in FetchingPhase]
        elif stage == ProcessingStage.STAGING:
            phases = [p.value for p in StagingPhase]
        elif stage == ProcessingStage.CLEANING:
            phases = [p.value for p in CleaningPhase]
        else:
            phases = []

        if not args.yes:
            response = input(
                f"This will clear {len(phases)} phase checkpoints. Continue? [y/N]: "
            )
            if response.lower() != "y":
                logger.info("Cancelled")
                return

        # Clear each phase
        for phase in phases:
            try:
                checkpoint_manager.reset_checkpoint(
                    stage,
                    phase,
                    args.data_type,
                    clear_processed=not args.keep_processed,
                )
                logger.info(f"  Cleared {phase}")
            except Exception as e:
                logger.error(f"Error clearing {phase}: {e}")
    else:
        # Clear specific phase
        logger.info(
            f"\nClearing checkpoint for {args.data_type}/{stage.value}/{args.phase}"
        )

        if not args.yes:
            response = input("Continue? [y/N]: ")
            if response.lower() != "y":
                logger.info("Cancelled")
                return

        try:
            checkpoint_manager.reset_checkpoint(
                stage,
                args.phase,
                args.data_type,
                clear_processed=not args.keep_processed,
            )
            logger.info("Checkpoint cleared")
        except Exception as e:
            logger.error(f"Error clearing checkpoint: {e}")


async def cmd_retry_failed(args: argparse.Namespace):
    """Retry failed items for a specific stage."""
    config = BicamConfig.from_env(env_file=args.env_file)
    checkpoint_manager = _get_checkpoint_manager(config, args.checkpoint_db)

    # Parse stage
    try:
        stage = ProcessingStage(args.stage)
    except ValueError:
        logger.error(f"Invalid stage: {args.stage}")
        return

    logger.info(f"\nFinding failed items for {args.data_type} in {stage.value} stage")
    logger.info("=" * 80)

    # Get phases for this stage
    if stage == ProcessingStage.FETCHING:
        phases = [p.value for p in FetchingPhase]
    elif stage == ProcessingStage.STAGING:
        phases = [p.value for p in StagingPhase]
    elif stage == ProcessingStage.CLEANING:
        phases = [p.value for p in CleaningPhase]
    else:
        phases = []

    total_failed = 0
    failed_by_phase = {}

    # Collect failed items
    for phase in phases:
        failed_items = checkpoint_manager.get_failed_items(
            stage, phase, args.data_type, max_retries=args.max_retries
        )

        if failed_items:
            failed_by_phase[phase] = failed_items
            total_failed += len(failed_items)
            logger.info(f"\n{phase}: {len(failed_items)} failed items")

            # Show sample of failures
            for item in failed_items[:5]:
                logger.info(f"  - {item['item_id']}: {item['error_message']}")

            if len(failed_items) > 5:
                logger.info(f"  ... and {len(failed_items) - 5} more")

    if total_failed == 0:
        logger.info("\nNo failed items found")
        return

    logger.info(f"\nTotal failed items: {total_failed}")

    if not args.dry_run:
        response = input("\nRetry all failed items? [y/N]: ")
        if response.lower() != "y":
            logger.info("Cancelled")
            return

        logger.info(
            "\nNote: Actual retry logic should be implemented in your processing code"
        )
        logger.info("The failed items have been identified and can be reprocessed")
    else:
        logger.info("\n(Dry run - no items will be retried)")


async def cmd_export_checkpoint(args: argparse.Namespace):
    """Export checkpoint data to JSON for analysis."""
    config = BicamConfig.from_env(env_file=args.env_file)
    checkpoint_manager = _get_checkpoint_manager(config, args.checkpoint_db)

    logger.info(f"\nExporting checkpoint data for {args.data_type}")

    # Get all checkpoint data
    export_data = {
        "data_type": args.data_type,
        "export_time": datetime.now().isoformat(),
        "progress": checkpoint_manager.get_progress_summary(args.data_type),
        "checkpoints": {},
        "failed_items": {},
    }

    # Export detailed checkpoint data
    for stage in ProcessingStage:
        if stage == ProcessingStage.FETCHING:
            phases = list(FetchingPhase)
        elif stage == ProcessingStage.STAGING:
            phases = list(StagingPhase)
        elif stage == ProcessingStage.CLEANING:
            phases = list(CleaningPhase)
        else:
            continue

        for phase in phases:
            try:
                checkpoint = checkpoint_manager.get_or_create_checkpoint(
                    stage, phase.value, args.data_type
                )

                key = f"{stage.value}/{phase.value}"
                export_data["checkpoints"][key] = checkpoint.to_dict()

                # Get failed items for this phase
                failed_items = checkpoint_manager.get_failed_items(
                    stage, phase.value, args.data_type, max_retries=999
                )
                if failed_items:
                    export_data["failed_items"][key] = failed_items

            except Exception as e:
                logger.debug(f"Error exporting {stage.value}/{phase.value}: {e}")

    # Write to file
    output_file = (
        args.output
        or f"{args.data_type}_checkpoint_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    )

    with open(output_file, "w") as f:
        json.dump(export_data, f, indent=2, default=str)

    logger.info(f"Exported to: {output_file}")


# ==============================================================================
# Helper Functions
# ==============================================================================


def _get_checkpoint_manager(
    config: BicamConfig, checkpoint_db: str | None = None
) -> HierarchicalCheckpointManager:
    """Get checkpoint manager instance."""
    db_path = checkpoint_db or "hierarchical_checkpoints.db"
    return HierarchicalCheckpointManager(db_path)


async def _build_processing_resource(
    config: BicamConfig,
    *,
    parallel_enabled: bool = False,
    use_dynamic_pool: bool = False,
    use_optimized_storage: bool = True,
    from_date: str | None = None,
    to_date: str | None = None,
    congress: int | None = None,
    resume: bool = False,
    no_incremental: bool = False,
    rerun: bool = False,
) -> ProcessingResource:
    """Create a fully-configured ProcessingResource and ensure data-type registry is populated."""

    # Ensure all data-types are registered before any lookups happen
    try:
        from bicam_collection.libs.data_type_router import register_all_data_types

        register_all_data_types()
    except Exception as e:
        logger.debug(f"Data-type registration failed (proceeding anyway): {e}")

    """Create a fully-configured ProcessingResource from config and CLI flags."""

    # Get API keys from config
    api_keys = []

    # Get keys from the scraping config structure
    if hasattr(config, "scraping"):
        # Get congressional API keys
        congressional_keys = getattr(config.scraping, "congressional_api_key", None)
        if congressional_keys:
            api_keys.extend(
                [key.strip() for key in congressional_keys.split(",") if key.strip()]
            )

        # Get govinfo API keys
        govinfo_keys = getattr(config.scraping, "govinfo_api_key", None)
        if govinfo_keys:
            api_keys.extend(
                [key.strip() for key in govinfo_keys.split(",") if key.strip()]
            )

    if not api_keys:
        raise ValueError("No API keys found in config")

    logger.info(f"Loaded {len(api_keys)} API keys from config")

    # Configure parallelization
    parallelization_config = {}
    if parallel_enabled:
        sessions = getattr(config.processing, "max_workers", 2)
        parallelization_config = {
            "fetcher": {
                "congressional": {
                    "num_sessions": sessions,
                    "keys_per_session": 2,
                }
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
        use_dynamic_pool=use_dynamic_pool,
        use_optimized_storage=use_optimized_storage,  # New parameter
        # Processing parameters
        from_date=from_date,
        to_date=to_date,
        congress=congress,
        # Checkpoint configuration
        incremental=incremental_flag,
        use_checkpoint_resume=use_checkpoint_resume,
        use_incremental_dates=use_incremental_dates,
        rerun_mode=rerun,
        use_postgres_checkpoints=False,  # Always use SQLite for new checkpoints
    )


# ==============================================================================
# CLI Parser
# ==============================================================================


def _create_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        "bicam-collection data pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="""
Bicam Collection Pipeline CLI with Hierarchical Checkpoint Support

This CLI provides comprehensive control over the data processing pipeline
with support for resuming from any point in the processing stages:
- Fetching: List items → Full data → Related endpoints
- Staging: JSONB → Normalized tables → Extracted lists
- Cleaning: Staging tables → Production tables
        """,
    )

    # Add global --env-file argument
    p.add_argument(
        "--env-file",
        type=str,
        help="Path to .env file to load (default: .env)",
        default=None,
    )

    p.add_argument(
        "--checkpoint-db",
        type=str,
        help="Path to checkpoint database (default: hierarchical_checkpoints.db)",
        default=None,
    )

    sub = p.add_subparsers(dest="command", required=True)

    # ------------------------------------------------------------------
    # process - Enhanced with new options
    proc = sub.add_parser("process", help="Run pipeline for one data-type")
    proc.add_argument(
        "data_type", help="Data type to process (e.g., bills, amendments)"
    )
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
        "--use-dynamic-pool",
        action="store_true",
        help="Enable dynamic key pool for optimized parallel processing",
    )
    proc.add_argument(
        "--use-optimized-storage",
        action="store_true",
        default=True,
        help="Use optimized storage backend (default: True)",
    )
    proc.add_argument(
        "--no-optimized-storage",
        action="store_false",
        dest="use_optimized_storage",
        help="Disable optimized storage backend",
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
    # progress - New command
    prog = sub.add_parser("progress", help="Show detailed progress for a data type")
    prog.add_argument("data_type", help="Data type to check")

    # ------------------------------------------------------------------
    # list-checkpoints - New command
    list_cp = sub.add_parser(
        "list-checkpoints", help="List all checkpoints for a data type"
    )
    list_cp.add_argument("data_type", help="Data type to check")

    # ------------------------------------------------------------------
    # clear-checkpoint - Enhanced version
    clear_cp = sub.add_parser("clear-checkpoint", help="Clear specific checkpoint(s)")
    clear_cp.add_argument("data_type", help="Data type")
    clear_cp.add_argument(
        "stage", choices=["fetching", "staging", "cleaning"], help="Processing stage"
    )
    clear_cp.add_argument(
        "phase",
        nargs="?",
        help="Specific phase (optional, clears all phases if not specified)",
    )
    clear_cp.add_argument(
        "--keep-processed",
        action="store_true",
        help="Keep processed items tracking (only reset checkpoint state)",
    )
    clear_cp.add_argument(
        "-y", "--yes", action="store_true", help="Skip confirmation prompt"
    )

    # ------------------------------------------------------------------
    # retry-failed - New command
    retry = sub.add_parser("retry-failed", help="Retry failed items")
    retry.add_argument("data_type", help="Data type")
    retry.add_argument(
        "stage", choices=["fetching", "staging", "cleaning"], help="Processing stage"
    )
    retry.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="Maximum retries per item (default: 3)",
    )
    retry.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be retried without actually retrying",
    )

    # ------------------------------------------------------------------
    # export-checkpoint - New command
    export = sub.add_parser("export-checkpoint", help="Export checkpoint data to JSON")
    export.add_argument("data_type", help="Data type to export")
    export.add_argument(
        "-o",
        "--output",
        help="Output file path (default: {data_type}_checkpoint_{timestamp}.json)",
    )

    # ------------------------------------------------------------------
    # Existing commands (unchanged)
    setup_p = sub.add_parser("setup-db", help="Create database schemas/tables")
    setup_p.add_argument("--recreate", action="store_true", help="Drop & recreate")

    sub.add_parser("check-dates", help="Print last_processed dates")

    reset_p = sub.add_parser("reset-dates", help="Delete last_processed entries")
    reset_p.add_argument("--data-types", nargs="+", help="Subset of data-types")

    sub.add_parser("list-types", help="List registered data-types")

    return p


# ==============================================================================
# Main Entry Points
# ==============================================================================


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

    # Load configuration from the specified env file
    config = BicamConfig.from_env(env_file=args.env_file)
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
        use_dynamic_pool=args.use_dynamic_pool,
        use_optimized_storage=args.use_optimized_storage,
    )

    # Initialize the processing resource (async operations)
    await _processing_resource.initialize()

    # Check for conflicts
    logger.info(f"Checking for conflicts before processing {args.data_type}...")

    # Get run manager from processing resource
    run_manager = await _processing_resource.get_run_manager()

    # Create run metadata for conflict detection
    from ..libs.run_tracking import RunMetadata, RunType

    # Determine run type based on data source
    if data_source == "congressional":
        run_type = RunType.SCRAPER_CONGRESSIONAL
    elif data_source == "govinfo":
        run_type = RunType.SCRAPER_GOVINFO
    else:
        run_type = RunType.OTHER

    run_metadata = RunMetadata(
        run_id="",
        run_type=run_type,
        system_name=f"{args.data_type}_fetcher_dagster",
        description=f"Processing {args.data_type}"
        + (f" ({', '.join(args.phases)})" if args.phases else " (all phases)"),
        parameters={
            "data_type": args.data_type,
            "phases": args.phases,
            "from_date": args.from_date,
            "to_date": args.to_date,
            "congress": args.congress,
            "parallel": args.parallel,
            "resume": args.resume,
            "rerun": args.rerun,
            "use_dynamic_pool": args.use_dynamic_pool,
            "use_optimized_storage": args.use_optimized_storage,
        },
        data_types=[args.data_type],
        priority=3,
        tags=["dagster", "pipeline", args.data_type],
    )

    # Check for conflicts
    conflicts = await run_manager.check_conflicts(run_metadata)
    if conflicts:
        logger.error(f"Cannot start processing {args.data_type} due to conflicts:")
        for conflict in conflicts:
            logger.error(f"  - {conflict}")
        raise RuntimeError(
            f"Processing blocked by {len(conflicts)} active run(s). Stop other instances first."
        )

    logger.info(f"No conflicts detected for {args.data_type}")
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
    """Phase 3: Async cleanup including resource cleanup."""
    global _processing_resource

    logger.info("Starting async cleanup phase...")

    try:
        if _processing_resource:
            await _processing_resource.cleanup()
            logger.info("Processing resource cleaned up successfully")
    except Exception as e:
        logger.warning(f"Error during cleanup: {e}")
    finally:
        _processing_resource = None
        logger.info("Async cleanup phase completed")


async def main_async(argv: list[str] | None = None):
    """Async main function."""
    parser = _create_parser()
    ns = parser.parse_args(argv)

    # Route to appropriate command handler
    if ns.command == "process":
        await _cmd_process(ns)
    elif ns.command == "progress":
        await cmd_progress(ns)
    elif ns.command == "list-checkpoints":
        await cmd_list_checkpoints(ns)
    elif ns.command == "clear-checkpoint":
        await cmd_clear_checkpoint(ns)
    elif ns.command == "retry-failed":
        await cmd_retry_failed(ns)
    elif ns.command == "export-checkpoint":
        await cmd_export_checkpoint(ns)
    elif ns.command == "setup-db":
        from .commands import setup_db

        config = BicamConfig.from_env(env_file=ns.env_file)
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

        config = BicamConfig.from_env(env_file=ns.env_file)
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

        config = BicamConfig.from_env(env_file=ns.env_file)
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
