"""
Streamlined Architecture CLI

This module provides a command-line interface for the streamlined architecture,
allowing direct execution of data processing pipelines without the complex
8-layer architecture.
"""

import argparse
import asyncio
import logging
import sys

from .executor import StreamlinedExecutor, execute_streamlined_pipeline
from .plugins.consolidated_registry import get_consolidated_registry
from .resources.config import StreamlinedConfig
from .resources.coordinator import ResourceCoordinator

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def create_parser() -> argparse.ArgumentParser:
    """Create argument parser for the streamlined CLI."""
    parser = argparse.ArgumentParser(
        description="Streamlined Architecture CLI for Bicam Collection",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s process bills --phases raw staging production
  %(prog)s process nominations --phases raw --from-date 2024-01-01 --limit 100
  %(prog)s list-types
  %(prog)s test-plugins bills
  %(prog)s status bills
  %(prog)s list-checkpoints bills
  %(prog)s clear-checkpoints bills --phases list_items full_data
  %(prog)s resume-from-checkpoint bills
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Process command
    process_parser = subparsers.add_parser(
        "process", help="Process data for a specific type"
    )
    process_parser.add_argument(
        "data_type", help="Data type to process (e.g., bills, nominations)"
    )
    process_parser.add_argument(
        "--phases",
        nargs="+",
        default=["raw", "staging", "production"],
        help="Phases to execute (default: raw staging production)",
    )
    process_parser.add_argument(
        "--from-date", help="Start date for data fetching (YYYY-MM-DD)"
    )
    process_parser.add_argument(
        "--to-date", help="End date for data fetching (YYYY-MM-DD)"
    )
    process_parser.add_argument(
        "--limit", type=int, help="Maximum number of items to process"
    )
    process_parser.add_argument(
        "--batch-size", type=int, default=100, help="Batch size for processing"
    )
    process_parser.add_argument(
        "--no-validate", action="store_true", help="Skip validation during cleaning"
    )
    process_parser.add_argument(
        "--no-resume", action="store_true", help="Don't resume from checkpoints"
    )

    # Optional rebuild of production schema before running production phase
    process_parser.add_argument(
        "--rebuild-prod-schema",
        action="store_true",
        help=(
            "Before running the production phase, rebuild the production schema "
            "for the data source (govinfo -> build_prod_schema_govinfo.sql; "
            "congressional -> build_bicam_congressional.sql)"
        ),
    )

    # Parallelization options
    process_parser.add_argument(
        "--enable-parallelization",
        action="store_true",
        default=True,
        help="Enable parallel processing (default: True)",
    )
    process_parser.add_argument(
        "--disable-parallelization",
        action="store_true",
        help="Disable parallel processing",
    )

    # Parallel related data options
    process_parser.add_argument(
        "--enable-parallel-related-data",
        action="store_true",
        default=True,
        help="Enable parallel processing for related data pagination (default: from config)",
    )
    process_parser.add_argument(
        "--disable-parallel-related-data",
        action="store_true",
        help="Disable parallel processing for related data pagination",
    )
    process_parser.add_argument(
        "--parallel-related-data-threshold",
        type=int,
        help="Minimum pages to trigger parallel related data processing (default: from config)",
    )

    # Incremental processing options
    process_parser.add_argument(
        "--enable-incremental",
        action="store_true",
        default=True,
        help="Enable incremental processing (default: True)",
    )
    process_parser.add_argument(
        "--disable-incremental",
        action="store_true",
        help="Disable incremental processing",
    )
    process_parser.add_argument(
        "--fallback-days",
        type=int,
        default=30,
        help="Days to fall back when no previous data found (default: 30)",
    )

    # List types command
    _list_parser = subparsers.add_parser("list-types", help="List supported data types")

    # Test plugins command
    test_parser = subparsers.add_parser("test-plugins", help="Test plugin integration")
    test_parser.add_argument(
        "data_type", nargs="?", help="Data type to test (optional)"
    )

    # Status command
    status_parser = subparsers.add_parser("status", help="Get execution status")
    status_parser.add_argument("data_type", help="Data type to check status for")

    # Plugin info command
    info_parser = subparsers.add_parser("plugin-info", help="Get plugin information")
    info_parser.add_argument("data_type", help="Data type to get plugin info for")

    # Configuration command
    config_parser = subparsers.add_parser("config", help="Show configuration")
    config_parser.add_argument(
        "--validate", action="store_true", help="Validate configuration"
    )

    # Diagnostics command
    diagnostics_parser = subparsers.add_parser(
        "diagnostics", help="Debug configuration and environment"
    )
    diagnostics_parser.add_argument(
        "--check-env", action="store_true", help="Check environment variables"
    )
    diagnostics_parser.add_argument(
        "--check-dotenv", action="store_true", help="Check .env file locations"
    )

    # =============================================================================
    # CHECKPOINT MANAGEMENT COMMANDS
    # =============================================================================

    # List checkpoints command
    list_checkpoints_parser = subparsers.add_parser(
        "list-checkpoints", help="List checkpoints for a data type"
    )
    list_checkpoints_parser.add_argument(
        "data_type", help="Data type to list checkpoints for"
    )
    list_checkpoints_parser.add_argument(
        "--detailed", action="store_true", help="Show detailed checkpoint information"
    )

    # Clear checkpoints command
    clear_checkpoints_parser = subparsers.add_parser(
        "clear-checkpoints", help="Clear checkpoints for a data type"
    )
    clear_checkpoints_parser.add_argument(
        "data_type", help="Data type to clear checkpoints for"
    )
    clear_checkpoints_parser.add_argument(
        "--phases",
        nargs="+",
        choices=[
            "fetching",
            "staging",
            "cleaning",
            "list_items",
            "full_data",
            "related_data",
            "jsonb_to_staging",
            "extract_lists",
            "validate_staging",
            "apply_rules",
        ],
        help="Specific phases to clear (default: all phases)",
    )
    clear_checkpoints_parser.add_argument(
        "--confirm", action="store_true", help="Skip confirmation prompt"
    )

    # Resume from checkpoint command
    resume_parser = subparsers.add_parser(
        "resume-from-checkpoint", help="Resume processing from checkpoint"
    )
    resume_parser.add_argument("data_type", help="Data type to resume processing for")
    resume_parser.add_argument(
        "--from-date", help="Start date for data fetching (YYYY-MM-DD)"
    )
    resume_parser.add_argument(
        "--to-date", help="End date for data fetching (YYYY-MM-DD)"
    )
    resume_parser.add_argument(
        "--limit", type=int, help="Maximum number of items to process"
    )
    resume_parser.add_argument(
        "--batch-size", type=int, default=100, help="Batch size for processing"
    )

    # Checkpoint stats command
    checkpoint_stats_parser = subparsers.add_parser(
        "checkpoint-stats", help="Show checkpoint statistics"
    )
    checkpoint_stats_parser.add_argument(
        "data_type", help="Data type to show checkpoint stats for"
    )

    # Retry failed items command
    retry_parser = subparsers.add_parser(
        "retry-failed", help="Retry failed items from checkpoints"
    )
    retry_parser.add_argument("data_type", help="Data type to retry failed items for")
    retry_parser.add_argument(
        "--max-retries", type=int, default=3, help="Maximum retry attempts (default: 3)"
    )

    # Fetch related tables command
    fetch_related_parser = subparsers.add_parser(
        "fetch-related", help="Fetch related tables from existing Phase 2 data"
    )
    fetch_related_parser.add_argument(
        "data_type", help="Data type to fetch related tables for"
    )
    fetch_related_parser.add_argument(
        "--related-tables",
        nargs="+",
        required=True,
        help="Related tables to fetch (e.g., texts actions cosponsors)",
    )
    fetch_related_parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Batch size for processing (default: 100)",
    )
    fetch_related_parser.add_argument(
        "--limit", type=int, help="Maximum number of items to process"
    )
    fetch_related_parser.add_argument(
        "--from-date", help="Start date filter (YYYY-MM-DD)"
    )
    fetch_related_parser.add_argument("--to-date", help="End date filter (YYYY-MM-DD)")

    return parser


async def command_process(args) -> int:
    """Process data for a specific type."""
    logger.info(f"Processing {args.data_type} with phases: {args.phases}")

    try:
        # Create configuration and coordinator
        config = StreamlinedConfig.from_env()

        # Enable parallelization if we have multiple API keys
        # OptimizedParallelProcessor works best with ALL keys in a dynamic pool
        if len(config.api.keys) > 2:
            logger.info(
                f"Enabling parallelization with {len(config.api.keys)} API keys for OptimizedParallelProcessor"
            )
            # Enable parallelization with minimal config (OptimizedParallelProcessor ignores sessions)
            config.parallelization.enabled = True
            config.parallelization.fetcher = {
                "congressional": {
                    "num_sessions": 1,  # Ignored by OptimizedParallelProcessor
                    "keys_per_session": len(config.api.keys),  # All keys
                },
                "govinfo": {
                    "num_sessions": 1,  # Ignored by OptimizedParallelProcessor
                    "keys_per_session": len(config.api.keys),  # All keys
                },
            }
        data_source = get_consolidated_registry().get_data_source(args.data_type)
        coordinator = ResourceCoordinator(config, data_source)

        # Build kwargs for processing
        kwargs = {}
        if args.batch_size:
            kwargs["batch_size"] = args.batch_size
        if args.no_validate:
            kwargs["validate"] = False

        # Handle parallelization options
        enable_parallelization = (
            args.enable_parallelization and not args.disable_parallelization
        )
        kwargs["enable_parallelization"] = enable_parallelization

        # Handle incremental processing options
        enable_incremental = args.enable_incremental and not args.disable_incremental
        kwargs["enable_incremental"] = enable_incremental
        if args.fallback_days:
            # Update coordinator config
            coordinator.config.fallback_days = args.fallback_days

        # Handle checkpoint resume option
        resume_from_checkpoint = not args.no_resume
        kwargs["resume_from_checkpoint"] = resume_from_checkpoint

        # Handle parallel related data options
        if args.enable_parallel_related_data:
            kwargs["enable_parallel_related_data"] = True
        elif args.disable_parallel_related_data:
            kwargs["enable_parallel_related_data"] = False

        if args.parallel_related_data_threshold:
            kwargs["parallel_related_data_threshold"] = (
                args.parallel_related_data_threshold
            )

        logger.info(
            f"Parallelization: {'enabled' if enable_parallelization else 'disabled'}"
        )
        logger.info(
            f"Incremental processing: {'enabled' if enable_incremental else 'disabled'}"
        )
        logger.info(
            f"Resume from checkpoint: {'enabled' if resume_from_checkpoint else 'disabled'}"
        )
        if args.fallback_days:
            logger.info(f"Fallback days: {args.fallback_days}")

        # Log parallel related data configuration
        if "enable_parallel_related_data" in kwargs:
            logger.info(
                f"Parallel related data: {'enabled' if kwargs['enable_parallel_related_data'] else 'disabled'}"
            )
        if "parallel_related_data_threshold" in kwargs:
            logger.info(
                f"Parallel related data threshold: {kwargs['parallel_related_data_threshold']} pages"
            )

        # Optionally rebuild production schema before running production phase
        async def _maybe_rebuild_prod_schema() -> None:
            if not args.rebuild_prod_schema:
                return
            if "production" not in args.phases:
                logger.info(
                    "--rebuild-prod-schema specified but 'production' phase not requested; skipping rebuild"
                )
                return

            from pathlib import Path

            # Resolve SQL file path based on data source
            base_libs_sql = Path(__file__).parent / "libs" / "sql"
            project_root = Path(__file__).resolve().parents[2]

            sql_path = None
            if data_source == "govinfo":
                # Prefer libs/sql version; fallback to project root if present
                libs_candidate = base_libs_sql / "build_prod_schema_govinfo.sql"
                root_candidate = project_root / "build_prod_schema_govinfo.sql"
                sql_path = libs_candidate if libs_candidate.exists() else root_candidate
            elif data_source == "congressional":
                sql_path = base_libs_sql / "build_bicam_congressional.sql"

            if not sql_path or not sql_path.exists():
                logger.warning(
                    f"Production schema SQL file not found for data source '{data_source}'"
                )
                return

            logger.info(f"Rebuilding production schema using: {sql_path}")
            db_pool = await coordinator.db_manager.get_pool()
            async with db_pool.acquire() as conn:
                sql_text = sql_path.read_text()
                await conn.execute(sql_text)
            logger.info("Production schema rebuild completed")

        # Execute pipeline
        await _maybe_rebuild_prod_schema()
        results = await execute_streamlined_pipeline(
            coordinator=coordinator,
            data_type=args.data_type,
            phases=args.phases,
            from_date=args.from_date,
            to_date=args.to_date,
            limit=args.limit,
            **kwargs,
        )

        # Display results
        logger.info(f"\n{'=' * 60}")
        logger.info(f"EXECUTION RESULTS for {args.data_type}")
        logger.info(f"{'=' * 60}")
        logger.info(f"Status: {results.get('status')}")
        logger.info(f"Phases: {results.get('phases')}")
        logger.info(f"Duration: {results.get('duration', 0):.2f} seconds")

        if "metrics" in results:
            logger.info("\nPhase Metrics:")
            for phase, metrics in results["metrics"].items():
                logger.info(f"  {phase}:")
                for key, value in metrics.items():
                    if key != "phase":
                        logger.info(f"    {key}: {value}")

        if results.get("errors"):
            logger.info("\nErrors:")
            for error in results["errors"]:
                logger.info(f"  - {error}")

        return 0 if results.get("status") == "completed" else 1

    except Exception as e:
        logger.error(f"Processing failed: {e}")
        return 1


async def command_list_types(args) -> int:
    """List supported data types."""
    try:
        registry = get_consolidated_registry()
        # No need to call auto_register_plugins() as consolidated registry auto-initializes

        supported_types = registry.list_data_types()
        stats = registry.get_registry_status()

        logger.info(f"\n{'=' * 60}")
        logger.info("SUPPORTED DATA TYPES")
        logger.info(f"{'=' * 60}")

        if supported_types:
            logger.info("Available data types:")
            for data_type in sorted(supported_types):
                fetcher = "✓" if data_type in registry._data_types else "✗"
                cleaner = "✓" if data_type in registry._data_types else "✗"
                normalizer = "✓" if data_type in registry._data_types else "✗"

                logger.info(f"  {data_type}")
                logger.info(
                    f"    Fetcher: {fetcher}  Cleaner: {cleaner}  Normalizer: {normalizer}"
                )
        else:
            logger.info("No supported data types found")

        logger.info("\nPlugin Registry Statistics:")
        for key, value in stats.items():
            logger.info(f"  {key}: {value}")

        return 0

    except Exception as e:
        logger.error(f"Failed to list types: {e}")
        return 1


async def command_test_plugins(args) -> int:
    """Test plugin integration."""
    try:
        config = StreamlinedConfig.from_env()
        coordinator = ResourceCoordinator(config)

        from .cleaner import StreamlinedCleaner
        from .fetcher import StreamlinedFetcher
        from .normalizer import StreamlinedNormalizer

        fetcher = await StreamlinedFetcher.from_coordinator(coordinator)
        cleaner = StreamlinedCleaner(coordinator)
        normalizer = StreamlinedNormalizer(coordinator)

        # Test specific data type or all
        if args.data_type:
            data_types = [args.data_type]
        else:
            registry = get_consolidated_registry()
            # No need to call auto_register_plugins() as consolidated registry auto-initializes
            data_types = registry.get_supported_data_types()

        logger.info(f"\n{'=' * 60}")
        logger.info("PLUGIN INTEGRATION TESTS")
        logger.info(f"{'=' * 60}")

        for data_type in data_types:
            logger.info(f"\nTesting {data_type}:")

            # Test fetcher plugin
            fetcher_test = await fetcher.test_plugin_integration(data_type)
            logger.info(
                f"  Fetcher: {'✓' if fetcher_test.get('plugin_available') else '✗'}"
            )
            if fetcher_test.get("error"):
                logger.info(f"    Error: {fetcher_test['error']}")

            # Test cleaner plugin
            cleaner_test = await cleaner.test_plugin_integration(data_type)
            logger.info(
                f"  Cleaner: {'✓' if cleaner_test.get('plugin_available') else '✗'}"
            )
            if cleaner_test.get("error"):
                logger.info(f"    Error: {cleaner_test['error']}")

            # Test normalizer plugin
            normalizer_test = await normalizer.test_plugin_integration(data_type)
            logger.info(
                f"  Normalizer: {'✓' if normalizer_test.get('plugin_available') else '✗'}"
            )
            if normalizer_test.get("error"):
                logger.info(f"    Error: {normalizer_test['error']}")

        return 0

    except Exception as e:
        logger.error(f"Plugin testing failed: {e}")
        return 1


async def command_status(args) -> int:
    """Get execution status."""
    try:
        config = StreamlinedConfig.from_env()
        coordinator = ResourceCoordinator(config)
        executor = StreamlinedExecutor(coordinator)

        status = await executor.get_execution_status(args.data_type)
        plugin_info = executor.get_plugin_info(args.data_type)

        logger.info(f"\n{'=' * 60}")
        logger.info(f"STATUS for {args.data_type}")
        logger.info(f"{'=' * 60}")

        logger.info(f"Execution Status: {status.get('status')}")
        logger.info(f"Last Execution: {status.get('last_execution')}")

        logger.info("\nPlugin Availability:")
        logger.info(f"  Fetcher: {'✓' if plugin_info.get('fetcher_plugin') else '✗'}")
        logger.info(f"  Cleaner: {'✓' if plugin_info.get('cleaner_plugin') else '✗'}")
        logger.info(
            f"  Normalizer: {'✓' if plugin_info.get('normalizer_plugin') else '✗'}"
        )

        if status.get("metrics"):
            logger.info("\nMetrics:")
            for key, value in status["metrics"].items():
                logger.info(f"  {key}: {value}")

        return 0

    except Exception as e:
        logger.error(f"Status check failed: {e}")
        return 1


async def command_plugin_info(args) -> int:
    """Get plugin information."""
    try:
        config = StreamlinedConfig.from_env()
        coordinator = ResourceCoordinator(config)
        executor = StreamlinedExecutor(coordinator)

        plugin_info = executor.get_plugin_info(args.data_type)

        logger.info(f"\n{'=' * 60}")
        logger.info(f"PLUGIN INFO for {args.data_type}")
        logger.info(f"{'=' * 60}")

        logger.info(
            f"Fetcher Plugin: {'✓' if plugin_info.get('fetcher_plugin') else '✗'}"
        )
        logger.info(
            f"Cleaner Plugin: {'✓' if plugin_info.get('cleaner_plugin') else '✗'}"
        )
        logger.info(
            f"Normalizer Plugin: {'✓' if plugin_info.get('normalizer_plugin') else '✗'}"
        )

        return 0

    except Exception as e:
        logger.error(f"Plugin info failed: {e}")
        return 1


async def command_config(args) -> int:
    """Show configuration."""
    try:
        config = StreamlinedConfig.from_env()

        logger.info(f"\n{'=' * 60}")
        logger.info("CONFIGURATION")
        logger.info(f"{'=' * 60}")

        logger.info("Database:")
        logger.info(f"  Host: {config.database.host}")
        logger.info(f"  Port: {config.database.port}")
        logger.info(f"  Database: {config.database.database}")
        logger.info(f"  Username: {config.database.username}")

        logger.info("\nAPI:")
        logger.info(f"  Keys: {len(config.api.keys)} configured")
        logger.info(f"  Rate Limit: {config.api.rate_limit_per_second}/sec")
        logger.info(f"  Timeout: {config.api.timeout}")
        logger.info(f"  Max Retries: {config.api.max_retries}")

        logger.info("\nProcessing:")
        logger.info(f"  Batch Size: {config.processing.batch_size}")
        logger.info(f"  Max Workers: {config.processing.max_workers}")
        logger.info(f"  Chunk Size: {config.processing.chunk_size}")
        logger.info(f"  Page Size: {config.processing.page_size}")
        logger.info(f"  Max Concurrent: {config.processing.max_concurrent}")

        if args.validate:
            logger.info("\nValidation:")
            validation_errors = config.validate()
            if validation_errors:
                logger.info("  Errors found:")
                for error in validation_errors:
                    logger.info(f"    - {error}")
                return 1
            else:
                logger.info("  ✓ Configuration is valid")

        return 0

    except Exception as e:
        logger.error(f"Configuration display failed: {e}")
        return 1


async def command_diagnostics(args) -> int:
    """Debug configuration and environment."""
    try:
        import os
        from pathlib import Path

        from .resources.config import StreamlinedConfig

        logger.info(f"\n{'=' * 60}")
        logger.info("DIAGNOSTICS")
        logger.info(f"{'=' * 60}")

        # Check .env file locations
        if args.check_dotenv or not args.check_env:
            logger.info("\n.env File Locations:")
            env_paths = [
                Path.cwd() / ".env",  # Current working directory
                Path(__file__).parent.parent.parent / ".env",  # Project root
                Path.home() / ".env",  # User home directory
            ]

            for env_path in env_paths:
                exists = "✓" if env_path.exists() else "✗"
                logger.info(f"  {exists} {env_path}")
                if env_path.exists():
                    try:
                        with open(env_path) as f:
                            content = f.read()
                            db_vars = [
                                line
                                for line in content.split("\n")
                                if line.strip() and "POSTGRESQL" in line
                            ]
                            if db_vars:
                                logger.info(
                                    f"    Database variables found: {len(db_vars)}"
                                )
                                for var in db_vars:
                                    var_name = var.split("=")[0]
                                    logger.info(f"      {var_name}")
                            else:
                                logger.info("    No POSTGRESQL variables found")
                    except Exception as e:
                        logger.info(f"    Error reading file: {e}")

        # Check environment variables
        if args.check_env or not args.check_dotenv:
            logger.info("\nExpected Environment Variables:")
            expected_vars = [
                "POSTGRESQL_HOST",
                "POSTGRESQL_PORT",
                "POSTGRESQL_DATABASE",
                "POSTGRESQL_USERNAME",
                "POSTGRESQL_PASSWORD",
                "CONGRESSIONAL_API_KEY",
                "GOVINFO_API_KEY",
            ]

            for var in expected_vars:
                value = os.getenv(var)
                if value:
                    if "PASSWORD" in var or "KEY" in var:
                        logger.info(f"  ✓ {var}=***hidden***")
                    else:
                        logger.info(f"  ✓ {var}={value}")
                else:
                    logger.info(f"  ✗ {var} (not set)")

        # Test configuration loading
        logger.info("\nConfiguration Loading Test:")
        try:
            config = StreamlinedConfig.from_env()
            logger.info(f"  Database Host: {config.database.host}")
            logger.info(f"  Database Port: {config.database.port}")
            logger.info(f"  Database Name: {config.database.database}")
            logger.info(f"  Database Username: {config.database.username}")
            logger.info(f"  API Keys: {len(config.api.keys)} configured")
        except Exception as e:
            logger.error(f"  Failed to load configuration: {e}")

        # Test dotenv loading specifically
        logger.info("\nDotenv Loading Test:")
        try:
            from dotenv import load_dotenv

            logger.info("  ✓ python-dotenv is available")

            # Test loading the .env file explicitly
            env_file = Path.cwd() / ".env"
            if env_file.exists():
                logger.info(f"  Attempting to load: {env_file}")
                result = load_dotenv(env_file, override=True)
                logger.info(f"  load_dotenv result: {result}")

                # Check if variables are now loaded
                logger.info(
                    f"  After load_dotenv - POSTGRESQL_HOST: {os.getenv('POSTGRESQL_HOST')}"
                )
                logger.info(
                    f"  After load_dotenv - POSTGRESQL_USERNAME: {os.getenv('POSTGRESQL_USERNAME')}"
                )
            else:
                logger.info("  .env file not found in current directory")

        except ImportError:
            logger.error("  ✗ python-dotenv not available")
        except Exception as e:
            logger.error(f"  Dotenv loading failed: {e}")

        return 0

    except Exception as e:
        logger.error(f"Diagnostics failed: {e}")
        return 1


# =============================================================================
# CHECKPOINT MANAGEMENT COMMANDS
# =============================================================================


async def command_list_checkpoints(args) -> int:
    """List checkpoints for a data type."""
    try:
        from .libs.hierarchical_checkpoint_system import (
            HierarchicalCheckpointManager,
        )

        config = StreamlinedConfig.from_env()

        # Create checkpoint manager with proper path
        checkpoint_manager = HierarchicalCheckpointManager(
            db_path=config.infrastructure.checkpoint_db_path
        )

        logger.info(f"\n{'=' * 60}")
        logger.info(f"CHECKPOINTS for {args.data_type}")
        logger.info(f"{'=' * 60}")

        # Get checkpoint summary using the available method
        summary = checkpoint_manager.get_progress_summary(args.data_type)

        if not summary:
            logger.info("No checkpoints found for this data type")
            return 0

        for stage, phases in summary.items():
            logger.info(f"\n{stage.upper()} Stage:")

            for phase, info in phases.items():
                logger.info(f"  {phase}:")
                logger.info(f"    Processed: {info.get('processed', 0)}")
                logger.info(f"    Failed: {info.get('failed', 0)}")
                logger.info(f"    Total: {info.get('total', 0)}")

                if info.get("current_item"):
                    logger.info(f"    Current Item: {info['current_item']}")

                if info.get("last_updated"):
                    logger.info(f"    Last Updated: {info['last_updated']}")

        # Show detailed stats if requested
        if args.detailed:
            logger.info(f"\n{'=' * 40}")
            logger.info("DETAILED CHECKPOINT STATS")
            logger.info(f"{'=' * 40}")

            # Use the same progress summary for detailed stats
            for stage, phases in summary.items():
                logger.info(f"\n{stage.upper()} Stage:")
                for phase, stats in phases.items():
                    logger.info(f"  {phase}:")
                    for key, value in stats.items():
                        logger.info(f"    {key}: {value}")

        return 0

    except Exception as e:
        logger.error(f"Failed to list checkpoints: {e}")
        return 1


async def command_clear_checkpoints(args) -> int:
    """Clear checkpoints for a data type."""
    try:
        from .libs.hierarchical_checkpoint_system import (
            CleaningPhase,
            FetchingPhase,
            HierarchicalCheckpointManager,
            ProcessingStage,
            StagingPhase,
        )
        from .plugins.consolidated_registry import ConsolidatedRegistry
        from .resources.config import StreamlinedConfig

        config = StreamlinedConfig.from_env()

        # Create checkpoint manager with proper path
        checkpoint_manager = HierarchicalCheckpointManager(
            db_path=config.infrastructure.checkpoint_db_path
        )

        # Show what will be cleared
        logger.info(f"\n{'=' * 60}")
        logger.info(f"CLEAR CHECKPOINTS for {args.data_type}")
        logger.info(f"{'=' * 60}")

        # Get current checkpoint status for main data type
        current_stats = checkpoint_manager.get_progress_summary(args.data_type)

        if current_stats:
            logger.info("\nCurrent checkpoint status for main data type:")
            for stage, phases in current_stats.items():
                for phase, stats in phases.items():
                    processed = stats.get("processed", 0)
                    total = stats.get("total", 0)
                    logger.info(
                        f"  {stage}.{phase}: {processed}/{total} items processed"
                    )

        # Get related table data types that need to be cleared
        related_data_types = []
        try:
            registry = ConsolidatedRegistry()
            config_data = registry.get_data_type_config(args.data_type)

            # Check for related tables from config
            if config_data and hasattr(config_data, "related_tables"):
                for related_table in config_data.related_tables:
                    related_data_type = f"{args.data_type}_{related_table}"
                    related_data_types.append(related_data_type)

                    # Check if this related table has checkpoints
                    related_stats = checkpoint_manager.get_progress_summary(
                        related_data_type
                    )
                    if related_stats:
                        logger.info(
                            f"\nFound checkpoints for related table {related_data_type}:"
                        )
                        for stage, phases in related_stats.items():
                            for phase, stats in phases.items():
                                processed = stats.get("processed", 0)
                                total = stats.get("total", 0)
                                logger.info(
                                    f"  {stage}.{phase}: {processed}/{total} items processed"
                                )

                    # Also check for granule data
                    prefixed_granule = f"{args.data_type}_granules"
                    prefixed_stats = checkpoint_manager.get_progress_summary(
                        prefixed_granule
                    )
                    if prefixed_stats:
                        logger.info(
                            f"\nFound checkpoints for prefixed granule data type {prefixed_granule}:"
                        )
                        for stage, phases in prefixed_stats.items():
                            for phase, stats in phases.items():
                                processed = stats.get("processed", 0)
                                total = stats.get("total", 0)
                                logger.info(
                                    f"  {stage}.{phase}: {processed}/{total} items processed"
                                )
                        # Add the prefixed version to the list
                        if prefixed_granule not in related_data_types:
                            related_data_types.append(prefixed_granule)

        except Exception as e:
            logger.warning(f"Could not determine related tables: {e}")

        # Determine what to clear
        phases_to_clear = []

        if args.phases:
            # Map phase names to actual phases
            phase_map = {
                # Fetching phases
                "list_items": (
                    ProcessingStage.FETCHING,
                    FetchingPhase.LIST_ITEMS.value,
                ),
                "full_data": (ProcessingStage.FETCHING, FetchingPhase.FULL_DATA.value),
                "related_data": (
                    ProcessingStage.FETCHING,
                    FetchingPhase.RELATED_DATA.value,
                ),
                "full_related_data": (
                    ProcessingStage.FETCHING,
                    FetchingPhase.FULL_RELATED_DATA.value,
                ),
                # Staging phases
                "jsonb_to_staging": (
                    ProcessingStage.STAGING,
                    StagingPhase.JSONB_TO_STAGING.value,
                ),
                "extract_lists": (
                    ProcessingStage.STAGING,
                    StagingPhase.EXTRACT_LISTS.value,
                ),
                "validate_staging": (
                    ProcessingStage.STAGING,
                    StagingPhase.VALIDATE_STAGING.value,
                ),
                # Cleaning phases
                "apply_rules": (
                    ProcessingStage.CLEANING,
                    CleaningPhase.APPLY_RULES.value,
                ),
                # Stage-level clearing
                "fetching": "fetching_all",
                "staging": "staging_all",
                "cleaning": "cleaning_all",
            }

            for phase_name in args.phases:
                if phase_name in phase_map:
                    phases_to_clear.append((phase_name, phase_map[phase_name]))
                else:
                    logger.warning(f"Unknown phase: {phase_name}")

            if phases_to_clear:
                logger.info(f"Phases to clear: {[p[0] for p in phases_to_clear]}")
            else:
                logger.info("No valid phases specified - clearing all phases")
                phases_to_clear = None
        else:
            logger.info("All phases will be cleared")

        # Confirmation prompt
        if not args.confirm:
            if related_data_types:
                logger.info(
                    f"\nWill also clear checkpoints for related tables: {related_data_types}"
                )

            response = input(
                "\nAre you sure you want to clear these checkpoints? (y/N): "
            )
            if response.lower() not in ["y", "yes"]:
                logger.info("Operation cancelled")
                return 0

        # Clear checkpoints
        cleared_count = 0

        # Function to clear checkpoints for a data type
        def clear_data_type_checkpoints(data_type: str) -> int:
            local_cleared = 0

            if phases_to_clear:
                # Clear specific phases
                for _phase_name, phase_info in phases_to_clear:
                    if phase_info == "fetching_all":
                        # Clear all fetching phases
                        for phase in FetchingPhase:
                            checkpoint_manager.reset_checkpoint(
                                ProcessingStage.FETCHING,
                                phase.value,
                                data_type,
                                clear_processed=True,
                            )
                            local_cleared += 1
                            logger.info(
                                f"  Cleared: {data_type} - fetching.{phase.value}"
                            )
                    elif phase_info == "staging_all":
                        # Clear all staging phases
                        for phase in StagingPhase:
                            checkpoint_manager.reset_checkpoint(
                                ProcessingStage.STAGING,
                                phase.value,
                                data_type,
                                clear_processed=True,
                            )
                            local_cleared += 1
                            logger.info(
                                f"  Cleared: {data_type} - staging.{phase.value}"
                            )
                    elif phase_info == "cleaning_all":
                        # Clear all cleaning phases
                        for phase in CleaningPhase:
                            checkpoint_manager.reset_checkpoint(
                                ProcessingStage.CLEANING,
                                phase.value,
                                data_type,
                                clear_processed=True,
                            )
                            local_cleared += 1
                            logger.info(
                                f"  Cleared: {data_type} - cleaning.{phase.value}"
                            )
                    else:
                        # Clear specific phase
                        stage, phase = phase_info
                        checkpoint_manager.reset_checkpoint(
                            stage, phase, data_type, clear_processed=True
                        )
                        local_cleared += 1
                        logger.info(f"  Cleared: {data_type} - {stage.value}.{phase}")
            else:
                # Clear all phases from all stages
                all_phases = [
                    (ProcessingStage.FETCHING, FetchingPhase.LIST_ITEMS.value),
                    (ProcessingStage.FETCHING, FetchingPhase.FULL_DATA.value),
                    (ProcessingStage.FETCHING, FetchingPhase.RELATED_DATA.value),
                    (ProcessingStage.STAGING, StagingPhase.JSONB_TO_STAGING.value),
                    (ProcessingStage.STAGING, StagingPhase.EXTRACT_LISTS.value),
                    (ProcessingStage.STAGING, StagingPhase.VALIDATE_STAGING.value),
                    (ProcessingStage.CLEANING, CleaningPhase.APPLY_RULES.value),
                ]

                for stage, phase in all_phases:
                    checkpoint_manager.reset_checkpoint(
                        stage, phase, data_type, clear_processed=True
                    )
                    local_cleared += 1
                    logger.info(f"  Cleared: {data_type} - {stage.value}.{phase}")

            return local_cleared

        # Clear main data type checkpoints
        cleared_count += clear_data_type_checkpoints(args.data_type)

        # Clear related table checkpoints
        for related_data_type in related_data_types:
            cleared_count += clear_data_type_checkpoints(related_data_type)

        # Flush caches to ensure changes are persisted
        checkpoint_manager.flush_all_caches()

        logger.info(f"\n✓ Cleared {cleared_count} checkpoints total")
        logger.info(f"✓ Main data type: {args.data_type}")
        if related_data_types:
            logger.info(f"✓ Related tables: {related_data_types}")
        logger.info("✓ Flushed checkpoint caches")

        return 0

    except Exception as e:
        logger.error(f"Failed to clear checkpoints: {e}")
        return 1


async def command_resume_from_checkpoint(args) -> int:
    """Resume processing from checkpoint."""
    try:
        config = StreamlinedConfig.from_env()
        coordinator = ResourceCoordinator(config)

        logger.info(f"\n{'=' * 60}")
        logger.info(f"RESUMING FROM CHECKPOINT for {args.data_type}")
        logger.info(f"{'=' * 60}")

        # Build kwargs for processing
        kwargs = {
            "batch_size": args.batch_size,
            "resume_from_checkpoint": True,  # Force resume
            "enable_parallelization": len(config.api.keys) > 1,
        }

        # Execute only the raw phase (fetching) from checkpoint
        results = await execute_streamlined_pipeline(
            coordinator=coordinator,
            data_type=args.data_type,
            phases=["raw"],  # Only fetching phase
            from_date=args.from_date,
            to_date=args.to_date,
            limit=args.limit,
            **kwargs,
        )

        # Display results
        logger.info(f"\n{'=' * 60}")
        logger.info(f"RESUME RESULTS for {args.data_type}")
        logger.info(f"{'=' * 60}")
        logger.info(f"Status: {results.get('status')}")
        logger.info(f"Duration: {results.get('duration', 0):.2f} seconds")

        if "metrics" in results:
            for phase, metrics in results["metrics"].items():
                logger.info(f"\n{phase} metrics:")
                for key, value in metrics.items():
                    if key != "phase":
                        logger.info(f"  {key}: {value}")

        if results.get("errors"):
            logger.info("\nErrors:")
            for error in results["errors"]:
                logger.info(f"  - {error}")

        return 0 if results.get("status") == "completed" else 1

    except Exception as e:
        logger.error(f"Resume from checkpoint failed: {e}")
        return 1


async def command_checkpoint_stats(args) -> int:
    """Show checkpoint statistics."""
    try:
        from .processing.optimized_processor import OptimizedParallelProcessor

        config = StreamlinedConfig.from_env()

        # Create processor to access checkpoint manager
        processor = OptimizedParallelProcessor(
            api_keys=config.api.keys,
            client_class=None,  # Not needed for checkpoint operations
        )

        logger.info(f"\n{'=' * 60}")
        logger.info(f"CHECKPOINT STATS for {args.data_type}")
        logger.info(f"{'=' * 60}")

        # Get detailed stats
        stats = processor.get_checkpoint_stats(args.data_type)

        if not stats:
            logger.info("No checkpoint statistics found for this data type")
            return 0

        total_processed = 0
        total_failed = 0
        total_items = 0

        for phase, phase_stats in stats.items():
            logger.info(f"\n{phase.upper()}:")
            logger.info(f"  Processed Items: {phase_stats.get('processed_items', 0)}")
            logger.info(f"  Failed Items: {phase_stats.get('failed_items', 0)}")
            logger.info(f"  Total Items: {phase_stats.get('total_items', 0)}")
            logger.info(f"  Current Offset: {phase_stats.get('current_offset', 0)}")

            if phase_stats.get("current_item_id"):
                logger.info(f"  Current Item ID: {phase_stats['current_item_id']}")

            if phase_stats.get("last_updated"):
                logger.info(f"  Last Updated: {phase_stats['last_updated']}")

            # Calculate progress percentage
            processed = phase_stats.get("processed_items", 0)
            total = phase_stats.get("total_items", 0)
            if total > 0:
                progress = (processed / total) * 100
                logger.info(f"  Progress: {progress:.1f}%")

            total_processed += processed
            total_failed += phase_stats.get("failed_items", 0)
            total_items += total

        # Summary
        logger.info(f"\n{'=' * 40}")
        logger.info("SUMMARY")
        logger.info(f"{'=' * 40}")
        logger.info(f"Total Processed: {total_processed}")
        logger.info(f"Total Failed: {total_failed}")
        logger.info(f"Total Items: {total_items}")

        if total_items > 0:
            overall_progress = (total_processed / total_items) * 100
            logger.info(f"Overall Progress: {overall_progress:.1f}%")

        return 0

    except Exception as e:
        logger.error(f"Failed to get checkpoint stats: {e}")
        return 1


async def command_retry_failed(args) -> int:
    """Retry failed items from checkpoints."""
    try:
        from .processing.optimized_processor import OptimizedParallelProcessor

        config = StreamlinedConfig.from_env()

        # Create processor to access checkpoint manager
        processor = OptimizedParallelProcessor(
            api_keys=config.api.keys,
            client_class=None,  # Not needed for checkpoint operations
        )

        logger.info(f"\n{'=' * 60}")
        logger.info(f"RETRYING FAILED ITEMS for {args.data_type}")
        logger.info(f"{'=' * 60}")

        # Retry failed items
        results = await processor.retry_failed_items(args.data_type, args.max_retries)

        logger.info("Retry Results:")
        logger.info(f"  Items Retried: {results.get('retried', 0)}")
        logger.info(f"  Errors: {results.get('errors', 0)}")

        if results.get("failed_items"):
            logger.info("\nFailed Items Found:")
            for item in results["failed_items"]:
                logger.info(
                    f"  - {item.get('item_id', 'unknown')}: {item.get('error_message', 'no message')}"
                )

        return 0

    except Exception as e:
        logger.error(f"Failed to retry failed items: {e}")
        return 1


async def command_fetch_related(args) -> int:
    """Fetch related tables from existing Phase 2 data."""
    try:
        from .executor import StreamlinedExecutor
        from .plugins.consolidated_registry import get_consolidated_registry
        from .resources.config import StreamlinedConfig
        from .resources.coordinator import ResourceCoordinator

        logger.info(f"\n{'=' * 60}")
        logger.info(f"FETCHING RELATED TABLES for {args.data_type}")
        logger.info(f"{'=' * 60}")

        # Load configuration with API keys
        logger.info("Loading configuration...")
        config = StreamlinedConfig.from_env()

        # Get the data source for this data type
        registry = get_consolidated_registry()
        data_source = registry.get_data_source(args.data_type)
        logger.info(f"Data source for {args.data_type}: {data_source}")

        # Initialize coordinator with the correct data source and configuration
        coordinator = ResourceCoordinator(source=data_source, config=config)
        await coordinator.initialize()

        executor = StreamlinedExecutor(coordinator)

        # Use the executor's method to fetch related tables
        results = await executor.fetch_related_tables_from_existing_data(
            data_type=args.data_type,
            related_tables=args.related_tables,
            batch_size=args.batch_size,
            limit=args.limit,
            from_date=args.from_date,
            to_date=args.to_date,
        )

        # Display results
        logger.info(f"\n{'=' * 60}")
        logger.info("FETCH RESULTS")
        logger.info(f"{'=' * 60}")

        logger.info(f"Status: {results.get('status', 'unknown')}")
        logger.info(f"Data Type: {results.get('data_type', 'unknown')}")
        logger.info(f"Data Source: {results.get('data_source', 'unknown')}")
        logger.info(f"Related Tables: {results.get('related_tables', [])}")

        metrics = results.get("metrics", {})
        logger.info(f"Items Processed: {metrics.get('items_processed', 0)}")
        logger.info(f"Related Items Fetched: {metrics.get('related_items_fetched', 0)}")
        logger.info(f"Duration: {metrics.get('duration', 0):.2f} seconds")
        logger.info(f"Errors: {metrics.get('errors', 0)}")

        if results.get("errors"):
            logger.error("Errors encountered:")
            for error in results["errors"]:
                logger.error(f"  - {error}")

        if results.get("status") == "completed":
            logger.info("✅ Related tables fetch completed successfully")
            return 0
        else:
            logger.error("❌ Related tables fetch failed")
            return 1

    except Exception as e:
        logger.error(f"Failed to fetch related tables: {e}")
        return 1


async def main() -> int:
    """Main CLI entry point."""
    parser = create_parser()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    # Route to appropriate command handler
    command_handlers = {
        "process": command_process,
        "list-types": command_list_types,
        "test-plugins": command_test_plugins,
        "status": command_status,
        "plugin-info": command_plugin_info,
        "config": command_config,
        "diagnostics": command_diagnostics,
        "list-checkpoints": command_list_checkpoints,
        "clear-checkpoints": command_clear_checkpoints,
        "resume-from-checkpoint": command_resume_from_checkpoint,
        "checkpoint-stats": command_checkpoint_stats,
        "retry-failed": command_retry_failed,
        "fetch-related": command_fetch_related,
    }

    handler = command_handlers.get(args.command)
    if not handler:
        logger.error(f"Unknown command: {args.command}")
        return 1

    try:
        return await handler(args)
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        return 130
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return 1


def cli_main():
    """Synchronous entry point for CLI."""
    try:
        return asyncio.run(main())
    except KeyboardInterrupt:
        return 130
    except Exception as e:
        logger.error(f"CLI failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(cli_main())
