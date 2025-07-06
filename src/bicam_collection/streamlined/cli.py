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
from .plugins.consolidated_registry import get_plugin_registry
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

    # List types command
    list_parser = subparsers.add_parser("list-types", help="List supported data types")

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

    return parser


async def command_process(args) -> int:
    """Process data for a specific type."""
    logger.info(f"Processing {args.data_type} with phases: {args.phases}")

    try:
        # Create configuration and coordinator
        config = StreamlinedConfig.from_environment()
        coordinator = ResourceCoordinator(config)

        # Build kwargs for processing
        kwargs = {}
        if args.batch_size:
            kwargs["batch_size"] = args.batch_size
        if args.no_validate:
            kwargs["validate"] = False

        # Execute pipeline
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
        registry = get_plugin_registry()
        await registry.auto_register_plugins()

        supported_types = registry.list_data_types()
        stats = registry.s

        logger.info(f"\n{'=' * 60}")
        logger.info("SUPPORTED DATA TYPES")
        logger.info(f"{'=' * 60}")

        if supported_types:
            logger.info("Available data types:")
            for data_type in sorted(supported_types):
                fetcher = "✓" if registry.has_fetcher_plugin(data_type) else "✗"
                cleaner = "✓" if registry.has_cleaner_plugin(data_type) else "✗"
                normalizer = "✓" if registry.has_normalizer_plugin(data_type) else "✗"

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
        config = StreamlinedConfig.from_environment()
        coordinator = ResourceCoordinator(config)

        from .cleaner import StreamlinedCleaner
        from .fetcher import StreamlinedFetcher
        from .normalizer import StreamlinedNormalizer

        fetcher = StreamlinedFetcher(coordinator)
        cleaner = StreamlinedCleaner(coordinator)
        normalizer = StreamlinedNormalizer(coordinator)

        # Test specific data type or all
        if args.data_type:
            data_types = [args.data_type]
        else:
            registry = get_plugin_registry()
            await registry.auto_register_plugins()
            data_types = registry.get_supported_data_types()

        logger.info(f"\n{'=' * 60}")
        logger.info("PLUGIN INTEGRATION TESTS")
        logger.info(f"{'=' * 60}")

        for data_type in data_types:
            logger.info(f"\nTesting {data_type}:")

            # Test fetcher plugin
            fetcher_test = await fetcher.test_plugin_integration(data_type)
            logger.info(f"  Fetcher: {'✓' if fetcher_test.get('plugin_available') else '✗'}")
            if fetcher_test.get("error"):
                logger.info(f"    Error: {fetcher_test['error']}")

            # Test cleaner plugin
            cleaner_test = await cleaner.test_plugin_integration(data_type)
            logger.info(f"  Cleaner: {'✓' if cleaner_test.get('plugin_available') else '✗'}")
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
        config = StreamlinedConfig.from_environment()
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
        logger.info(f"  Normalizer: {'✓' if plugin_info.get('normalizer_plugin') else '✗'}")

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
        config = StreamlinedConfig.from_environment()
        coordinator = ResourceCoordinator(config)
        executor = StreamlinedExecutor(coordinator)

        plugin_info = executor.get_plugin_info(args.data_type)

        logger.info(f"\n{'=' * 60}")
        logger.info(f"PLUGIN INFO for {args.data_type}")
        logger.info(f"{'=' * 60}")

        logger.info(f"Fetcher Plugin: {'✓' if plugin_info.get('fetcher_plugin') else '✗'}")
        logger.info(f"Cleaner Plugin: {'✓' if plugin_info.get('cleaner_plugin') else '✗'}")
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
        config = StreamlinedConfig.from_environment()

        logger.info(f"\n{'=' * 60}")
        logger.info("CONFIGURATION")
        logger.info(f"{'=' * 60}")

        logger.info("Database:")
        logger.info(f"  Host: {config.database.host}")
        logger.info(f"  Port: {config.database.port}")
        logger.info(f"  Name: {config.database.name}")
        logger.info(f"  User: {config.database.user}")

        logger.info("\nAPI:")
        logger.info(f"  Base URL: {config.api.base_url}")
        logger.info(f"  Timeout: {config.api.timeout}")
        logger.info(f"  Max Retries: {config.api.max_retries}")

        logger.info("\nProcessing:")
        logger.info(f"  Batch Size: {config.processing.batch_size}")
        logger.info(f"  Max Workers: {config.processing.max_workers}")
        logger.info(f"  Retry Attempts: {config.processing.retry_attempts}")

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
