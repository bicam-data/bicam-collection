"""
Streamlined Architecture Usage Examples

This script demonstrates how to use the new streamlined architecture for
processing bicam collection data with the simplified 4-layer approach.
"""

import asyncio
import logging

# Core streamlined imports
from bicam_collection.streamlined import (
    ResourceCoordinator,
    StreamlinedConfig,
    StreamlinedExecutor,
    execute_streamlined_pipeline,
    get_plugin_registry,
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



async def example_direct_execution():
    """Example of direct execution without Dagster."""
    logger.info("=== Direct Execution Example ===")

    # Create configuration
    config = StreamlinedConfig.from_environment()

    # Create resource coordinator
    coordinator = ResourceCoordinator(config)

    # Create executor
    executor = StreamlinedExecutor(coordinator)

    try:
        # Execute pipeline for bills data type
        results = await executor.execute_data_type(
            data_type="bills",
            phases=["raw", "staging", "production"],
            from_date="2024-01-01",
            to_date="2024-01-31",
            limit=100,
        )

        logger.info(f"Execution completed: {results}")

        # Get execution status
        status = await executor.get_execution_status("bills")
        logger.info(f"Status: {status}")

    except Exception as e:
        logger.error(f"Execution failed: {e}")

    finally:
        # Cleanup is handled automatically by the executor
        pass


async def example_convenience_function():
    """Example using the convenience function."""
    logger.info("=== Convenience Function Example ===")

    # Create configuration and coordinator
    config = StreamlinedConfig.from_environment()
    coordinator = ResourceCoordinator(config)

    try:
        # Execute pipeline using convenience function
        results = await execute_streamlined_pipeline(
            coordinator=coordinator,
            data_type="nominations",
            phases=["raw", "staging"],
            from_date="2024-01-01",
            limit=50,
        )

        logger.info(f"Convenience execution completed: {results}")

    except Exception as e:
        logger.error(f"Convenience execution failed: {e}")


async def example_individual_components():
    """Example of using individual streamlined components."""
    logger.info("=== Individual Components Example ===")

    # Create configuration and coordinator
    config = StreamlinedConfig.from_environment()
    coordinator = ResourceCoordinator(config)

    try:
        # Initialize coordinator
        await coordinator.initialize()

        # Create individual components
        from bicam_collection.streamlined import (
            StreamlinedCleaner,
            StreamlinedFetcher,
            StreamlinedNormalizer,
        )

        fetcher = StreamlinedFetcher(coordinator)
        cleaner = StreamlinedCleaner(coordinator)
        normalizer = StreamlinedNormalizer(coordinator)

        # Use fetcher to get data
        fetch_results = await fetcher.fetch_data_type(
            data_type="bills", from_date="2024-01-01", limit=10
        )
        logger.info(f"Fetch results: {fetch_results}")

        # Use cleaner to clean data
        clean_results = await cleaner.clean_data_type(data_type="bills", batch_size=5)
        logger.info(f"Clean results: {clean_results}")

        # Use normalizer to normalize data
        normalize_results = await normalizer.normalize_data_type(
            data_type="bills", batch_size=5
        )
        logger.info(f"Normalize results: {normalize_results}")

    except Exception as e:
        logger.error(f"Individual components failed: {e}")

    finally:
        # Cleanup resources
        await coordinator.cleanup()


async def example_plugin_inspection():
    """Example of inspecting available plugins."""
    logger.info("=== Plugin Inspection Example ===")

    # Get plugin registry
    registry = get_plugin_registry()

    # Auto-register plugins
    await registry.auto_register_plugins()

    # Get supported data types
    supported_types = registry.get_supported_data_types()
    logger.info(f"Supported data types: {supported_types}")

    # Check plugin availability for specific data types
    for data_type in ["bills", "nominations", "amendments"]:
        has_fetcher = registry.has_fetcher_plugin(data_type)
        has_cleaner = registry.has_cleaner_plugin(data_type)
        has_normalizer = registry.has_normalizer_plugin(data_type)

        logger.info(
            f"{data_type}: fetcher={has_fetcher}, cleaner={has_cleaner}, normalizer={has_normalizer}"
        )

    # Get registry statistics
    stats = registry.get_stats()
    logger.info(f"Plugin registry stats: {stats}")


async def example_configuration_options():
    """Example of different configuration options."""
    logger.info("=== Configuration Options Example ===")

    # Load configuration from environment
    config = StreamlinedConfig.from_environment()
    logger.info(f"Environment config: {config.database.host}")

    # Create custom configuration
    from bicam_collection.streamlined.resources.config import (
        APIConfig,
        DatabaseConfig,
        ProcessingConfig,
    )

    custom_config = StreamlinedConfig(
        database=DatabaseConfig(
            host="localhost",
            port=5432,
            name="bicam_test",
            user="test_user",
            password="test_pass",
        ),
        api=APIConfig(base_url="https://api.congress.gov", timeout=30, max_retries=3),
        processing=ProcessingConfig(batch_size=100, max_workers=5, retry_attempts=3),
    )

    # Validate configuration
    validation_errors = custom_config.validate()
    if validation_errors:
        logger.error(f"Configuration validation errors: {validation_errors}")
    else:
        logger.info("Configuration is valid")

    # Create coordinator with custom config
    coordinator = ResourceCoordinator(custom_config)
    logger.info("Custom configuration coordinator created")


async def example_error_handling():
    """Example of error handling in streamlined architecture."""
    logger.info("=== Error Handling Example ===")

    # Create configuration and coordinator
    config = StreamlinedConfig.from_environment()
    coordinator = ResourceCoordinator(config)

    # Create executor
    executor = StreamlinedExecutor(coordinator)

    try:
        # Try to execute with invalid data type
        results = await executor.execute_data_type(
            data_type="invalid_type", phases=["raw"]
        )

        logger.info(f"Results: {results}")

    except ValueError as e:
        logger.info(f"Expected error caught: {e}")

    try:
        # Try to execute with invalid phase
        results = await executor.execute_data_type(
            data_type="bills", phases=["invalid_phase"]
        )

        logger.info(f"Results: {results}")

    except Exception as e:
        logger.info(f"Error in execution: {e}")


async def example_testing_plugins():
    """Example of testing plugin integration."""
    logger.info("=== Plugin Testing Example ===")

    # Create configuration and coordinator
    config = StreamlinedConfig.from_environment()
    coordinator = ResourceCoordinator(config)

    # Create components
    from bicam_collection.streamlined import (
        StreamlinedCleaner,
        StreamlinedFetcher,
        StreamlinedNormalizer,
    )

    fetcher = StreamlinedFetcher(coordinator)
    cleaner = StreamlinedCleaner(coordinator)
    normalizer = StreamlinedNormalizer(coordinator)

    # Test plugin integration for each data type
    for data_type in ["bills", "nominations"]:
        logger.info(f"\nTesting plugins for {data_type}:")

        # Test fetcher plugin
        fetcher_test = await fetcher.test_plugin_integration(data_type)
        logger.info(f"Fetcher plugin test: {fetcher_test}")

        # Test cleaner plugin
        cleaner_test = await cleaner.test_plugin_integration(data_type)
        logger.info(f"Cleaner plugin test: {cleaner_test}")

        # Test normalizer plugin
        normalizer_test = await normalizer.test_plugin_integration(data_type)
        logger.info(f"Normalizer plugin test: {normalizer_test}")


async def main():
    """Run all examples."""
    logger.info("Starting Streamlined Architecture Examples")

    examples = [
        ("Direct Execution", example_direct_execution),
        ("Convenience Function", example_convenience_function),
        ("Individual Components", example_individual_components),
        ("Plugin Inspection", example_plugin_inspection),
        ("Configuration Options", example_configuration_options),
        ("Error Handling", example_error_handling),
        ("Testing Plugins", example_testing_plugins),
    ]

    for name, example_func in examples:
        try:
            logger.info(f"\n{'=' * 50}")
            logger.info(f"Running: {name}")
            logger.info(f"{'=' * 50}")
            await example_func()
            logger.info(f"✓ {name} completed successfully")
        except Exception as e:
            logger.error(f"✗ {name} failed: {e}")

        # Small delay between examples
        await asyncio.sleep(1)

    logger.info("\nAll examples completed!")


if __name__ == "__main__":
    asyncio.run(main())
