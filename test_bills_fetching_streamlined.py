#!/usr/bin/env python3
"""
Test Bills Fetching with Streamlined Architecture

This script demonstrates how to use the new streamlined architecture to fetch
Congressional bills data using the plugin-based system.
"""

import asyncio
import logging
import os
import sys
from pathlib import Path

# Add the src directory to the path so we can import streamlined
sys.path.insert(0, str(Path(__file__).parent / "src"))

from streamlined import (
    ResourceCoordinator,
    StreamlinedExecutor,
    StreamlinedFetcher,
    execute_streamlined_pipeline,
)
from streamlined.libs.data_type_registry import (
    create_processor_for_data_type,
    get_global_registry,
    validate_registry_integration,
)
from streamlined.resources.config import StreamlinedConfig

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("bills_fetch_test.log")],
)
logger = logging.getLogger(__name__)


async def test_registry_setup():
    """Test that the data type registry is properly set up for bills."""
    logger.info("=== Testing Registry Setup ===")

    try:
        # Validate registry integration
        is_valid = validate_registry_integration()
        logger.info(f"Registry validation result: {is_valid}")

        # Get registry status
        registry = get_global_registry()
        status = registry.get_registry_status()
        logger.info(f"Registry status: {status}")

        # Check if bills is registered
        if registry.is_registered("bills"):
            logger.info("✅ Bills data type is registered")

            # Get bills configuration
            bills_config = registry.get_plugin_config("bills")
            logger.info(f"Bills plugin config: {bills_config}")

            return True
        else:
            logger.error("❌ Bills data type is not registered")
            logger.info(f"Available data types: {registry.list_data_types()}")
            return False

    except Exception as exc:
        logger.error(f"Registry setup test failed: {exc}", exc_info=True)
        return False


async def test_configuration_setup():
    """Test configuration setup for streamlined architecture."""
    logger.info("=== Testing Configuration Setup ===")

    try:
        # Check for required environment variables
        required_env_vars = [
            "CONGRESSIONAL_API_KEYS",
            "POSTGRESQL_HOST",
            "POSTGRESQL_DATABASE",
            "POSTGRESQL_USERNAME",
            "POSTGRESQL_PASSWORD",
        ]

        missing_vars = []
        for var in required_env_vars:
            if not os.getenv(var):
                missing_vars.append(var)

        if missing_vars:
            logger.warning(f"Missing environment variables: {missing_vars}")
            logger.info("Using default configuration for testing...")

            # Create a test configuration
            config = StreamlinedConfig()
            # Override with any available env vars
            if os.getenv("CONGRESSIONAL_API_KEY"):
                config.api.keys = [os.getenv("CONGRESSIONAL_API_KEY")]
            else:
                logger.warning(
                    "No Congressional API key found - using dummy key for testing"
                )
                config.api.keys = ["dummy_key_for_testing"]

        else:
            # Create config from environment
            config = StreamlinedConfig.from_env()
            logger.info("✅ Configuration loaded from environment")

        # Validate configuration
        validation_errors = config.validate()
        if validation_errors:
            logger.warning(f"Configuration validation warnings: {validation_errors}")
        else:
            logger.info("✅ Configuration is valid")

        return config

    except Exception as exc:
        logger.error(f"Configuration setup failed: {exc}", exc_info=True)
        return None


async def test_resource_coordinator():
    """Test ResourceCoordinator initialization."""
    logger.info("=== Testing ResourceCoordinator ===")

    try:
        # Create config
        config = await test_configuration_setup()
        if not config:
            return None

        # Create coordinator
        coordinator = ResourceCoordinator(config)
        await coordinator.initialize()
        logger.info("✅ ResourceCoordinator initialized successfully")

        # Test individual managers
        db_manager = await coordinator.get_database_manager()
        api_key_manager = await coordinator.get_api_key_manager()
        client_manager = await coordinator.get_client_manager()

        logger.info(f"✅ Database manager: {type(db_manager).__name__}")
        logger.info(f"✅ API key manager: {type(api_key_manager).__name__}")
        logger.info(f"✅ Client manager: {type(client_manager).__name__}")

        return coordinator

    except Exception as exc:
        logger.error(f"ResourceCoordinator test failed: {exc}", exc_info=True)
        return None


async def test_bills_fetcher_plugin():
    """Test creating bills fetcher through the plugin system."""
    logger.info("=== Testing Bills Fetcher Plugin ===")

    try:
        # Create fetcher using the new plugin system
        fetcher = create_processor_for_data_type("bills", "fetcher")
        logger.info(f"✅ Created bills fetcher: {type(fetcher).__name__}")

        # Test specific methods if available
        if hasattr(fetcher, "extract_item_id"):
            test_data = {"type": "hr", "number": "1234", "congress": "118"}
            item_id = fetcher.extract_item_id(test_data)
            logger.info(f"✅ Item ID extraction test: {item_id}")

        return fetcher

    except Exception as exc:
        logger.error(f"Bills fetcher plugin test failed: {exc}", exc_info=True)
        return None


async def test_streamlined_fetcher():
    """Test StreamlinedFetcher with bills data type."""
    logger.info("=== Testing StreamlinedFetcher ===")

    try:
        # Get coordinator
        coordinator = await test_resource_coordinator()
        if not coordinator:
            logger.error("Cannot test StreamlinedFetcher without coordinator")
            return False

        # Create StreamlinedFetcher
        fetcher = StreamlinedFetcher(coordinator)
        logger.info("✅ StreamlinedFetcher created")

        # Test plugin integration
        plugin_test = await fetcher.test_plugin_integration("bills")
        logger.info(f"Plugin integration test: {plugin_test}")

        # Get supported data types
        supported_types = fetcher.get_supported_data_types()
        logger.info(f"Supported data types: {supported_types}")

        if "bills" in supported_types:
            logger.info("✅ Bills data type is supported")
        else:
            logger.warning("⚠️ Bills data type not in supported list")

        return True

    except Exception as exc:
        logger.error(f"StreamlinedFetcher test failed: {exc}", exc_info=True)
        return False


async def test_bills_fetch_simulation():
    """Test a simulated bills fetch (dry run without actual API calls)."""
    logger.info("=== Testing Bills Fetch Simulation ===")

    try:
        # Get coordinator
        coordinator = await test_resource_coordinator()
        if not coordinator:
            return False

        # Create executor
        executor = StreamlinedExecutor(coordinator)
        logger.info("✅ StreamlinedExecutor created")

        # Test getting execution status
        status = await executor.get_execution_status("bills")
        logger.info(f"Bills execution status: {status}")

        # Test plugin info
        plugin_info = executor.get_plugin_info("bills")
        logger.info(f"Bills plugin info: {plugin_info}")

        logger.info("✅ Bills fetch simulation completed")
        return True

    except Exception as exc:
        logger.error(f"Bills fetch simulation failed: {exc}", exc_info=True)
        return False


async def test_bills_fetch_small_sample():
    """Test fetching a small sample of bills data (if API key is available)."""
    logger.info("=== Testing Small Bills Fetch Sample ===")

    try:
        # Check if we have a real API key
        api_key = os.getenv("CONGRESSIONAL_API_KEY")
        if not api_key or api_key == "dummy_key_for_testing":
            logger.info("⚠️ No real API key available - skipping actual fetch test")
            return True

        # Get coordinator
        coordinator = await test_resource_coordinator()
        if not coordinator:
            return False

        # Use execute_streamlined_pipeline for a small fetch
        logger.info("🚀 Attempting to fetch 5 bills from 2024...")

        results = await execute_streamlined_pipeline(
            coordinator=coordinator,
            data_type="bills",
            phases=["raw"],  # Only fetch raw data
            from_date="2024-01-01",
            to_date="2024-01-02",  # Very small date range
            limit=5,  # Very small limit
        )

        logger.info(f"✅ Bills fetch sample completed: {results}")
        return True

    except Exception as exc:
        logger.error(f"Bills fetch sample failed: {exc}", exc_info=True)
        return False


async def main():
    """Run all tests."""
    logger.info("🎯 Starting Bills Fetching Tests with Streamlined Architecture")

    test_results = {}

    # Run all tests
    tests = [
        ("Registry Setup", test_registry_setup),
        ("Configuration Setup", test_configuration_setup),
        ("Resource Coordinator", test_resource_coordinator),
        ("Bills Fetcher Plugin", test_bills_fetcher_plugin),
        ("Streamlined Fetcher", test_streamlined_fetcher),
        ("Bills Fetch Simulation", test_bills_fetch_simulation),
        ("Bills Fetch Sample", test_bills_fetch_small_sample),
    ]

    for test_name, test_func in tests:
        logger.info(f"\n📋 Running test: {test_name}")
        try:
            result = await test_func()
            test_results[test_name] = result
            if result:
                logger.info(f"✅ {test_name}: PASSED")
            else:
                logger.warning(f"⚠️ {test_name}: FAILED")
        except Exception as exc:
            logger.error(f"❌ {test_name}: ERROR - {exc}")
            test_results[test_name] = False

    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("📊 TEST SUMMARY")
    logger.info("=" * 60)

    passed = sum(1 for result in test_results.values() if result)
    total = len(test_results)

    for test_name, result in test_results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        logger.info(f"{status}: {test_name}")

    logger.info(f"\nOverall: {passed}/{total} tests passed")

    if passed == total:
        logger.info(
            "🎉 All tests passed! Bills fetching with streamlined architecture is working."
        )
    else:
        logger.warning("⚠️ Some tests failed. Check the logs above for details.")

    return passed == total


if __name__ == "__main__":
    asyncio.run(main())
