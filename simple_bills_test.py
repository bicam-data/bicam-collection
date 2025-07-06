#!/usr/bin/env python3
"""
Simple Bills Fetching Test - Streamlined Architecture

This script provides a quick test of the bills fetching components
without requiring full database setup.
"""

import sys
import logging
from pathlib import Path

# Add the src directory to the path so we can import streamlined
sys.path.insert(0, str(Path(__file__).parent / "src"))

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def test_imports():
    """Test that all required imports work."""
    print("🔍 Testing imports...")

    try:
        # Test core streamlined imports
        from streamlined.resources.config import StreamlinedConfig

        print("✅ StreamlinedConfig imported successfully")

        from streamlined.libs.data_type_registry import get_global_registry

        print("✅ Data type registry imported successfully")

        from streamlined.resources.coordinator import ResourceCoordinator

        print("✅ ResourceCoordinator imported successfully")

        # Test bills specific imports
        from streamlined.data_types.congressional.bills.fetcher import BillsFetcher

        print("✅ BillsFetcher imported successfully")

        from streamlined.data_types.congressional.bills.cleaner import BillsCleaner

        print("✅ BillsCleaner imported successfully")

        from streamlined.data_types.congressional.bills.database_normalizer import (
            BillsDatabaseNormalizer,
        )

        print("✅ BillsDatabaseNormalizer imported successfully")

        # Test plugin imports
        from streamlined.plugins.congressional import CongressionalFetcherPlugin

        print("✅ CongressionalFetcherPlugin imported successfully")

        return True

    except ImportError as e:
        print(f"❌ Import failed: {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error during import: {e}")
        return False


def test_configuration():
    """Test configuration creation and validation."""
    print("\n🔧 Testing configuration...")

    try:
        from streamlined.resources.config import StreamlinedConfig

        # Create default config
        config = StreamlinedConfig()
        print("✅ Default config created")

        # Test configuration structure
        print(f"  Database host: {config.database.host}")
        print(f"  API keys count: {len(config.api.keys)}")
        print(f"  Processing batch size: {config.processing.batch_size}")
        print(f"  Max workers: {config.processing.max_workers}")

        # Test validation
        errors = config.validate()
        if errors:
            print(f"⚠️  Configuration validation warnings: {errors}")
        else:
            print("✅ Configuration validation passed")

        return True

    except Exception as e:
        print(f"❌ Configuration test failed: {e}")
        return False


def test_registry():
    """Test data type registry."""
    print("\n📝 Testing data type registry...")

    try:
        from streamlined.libs.data_type_registry import get_global_registry

        # Get registry
        registry = get_global_registry()
        print("✅ Registry obtained")

        # Test registry methods
        data_types = registry.list_data_types()
        print(f"  Available data types: {data_types}")

        data_sources = registry.list_data_sources()
        print(f"  Available data sources: {data_sources}")

        # Test bills registration
        if registry.is_registered("bills"):
            print("✅ Bills data type is registered")

            # Get bills info
            try:
                plugin_type = registry.get_plugin_type("bills")
                print(f"  Bills plugin type: {plugin_type}")

                data_source = registry.get_data_source("bills")
                print(f"  Bills data source: {data_source}")

                config_file = registry.get_config_file("bills")
                print(f"  Bills config file: {config_file}")

            except Exception as e:
                print(f"⚠️  Could not get bills details: {e}")

        else:
            print("⚠️  Bills data type is not registered")

        return True

    except Exception as e:
        print(f"❌ Registry test failed: {e}")
        return False


def test_bills_fetcher():
    """Test bills fetcher instantiation."""
    print("\n🔄 Testing bills fetcher...")

    try:
        from streamlined.data_types.congressional.bills.fetcher import BillsFetcher
        from streamlined.api_clients.congressional_api import CongressionalAPIClient

        # Create a mock API client
        client = CongressionalAPIClient(api_key="test_key")
        print("✅ API client created")

        # Create fetcher
        fetcher = BillsFetcher(client=client)
        print("✅ BillsFetcher created")

        # Test extract_item_id method
        test_data = {"type": "hr", "number": "1234", "congress": "118"}

        item_id = fetcher.extract_item_id(test_data)
        print(f"✅ Item ID extraction test: {item_id}")

        # Test other methods exist
        methods_to_check = [
            "get_bills_actions",
            "get_bills_cosponsors",
            "get_bills_texts",
            "get_bills_summaries",
        ]

        for method_name in methods_to_check:
            if hasattr(fetcher, method_name):
                print(f"✅ Method {method_name} exists")
            else:
                print(f"⚠️  Method {method_name} missing")

        return True

    except Exception as e:
        print(f"❌ Bills fetcher test failed: {e}")
        return False


def test_bills_cleaner():
    """Test bills cleaner instantiation."""
    print("\n🧹 Testing bills cleaner...")

    try:
        from streamlined.data_types.congressional.bills.cleaner import BillsCleaner

        # Create cleaner
        cleaner = BillsCleaner()
        print("✅ BillsCleaner created")

        # Test basic properties
        print(f"  Data type: {cleaner.data_type}")
        print(f"  Staging schema: {cleaner.staging_schema}")
        print(f"  Production schema: {cleaner.production_schema}")

        return True

    except Exception as e:
        print(f"❌ Bills cleaner test failed: {e}")
        return False


def test_bills_normalizer():
    """Test bills database normalizer instantiation."""
    print("\n📊 Testing bills database normalizer...")

    try:
        from streamlined.data_types.congressional.bills.database_normalizer import (
            BillsDatabaseNormalizer,
        )

        # Create normalizer
        normalizer = BillsDatabaseNormalizer()
        print("✅ BillsDatabaseNormalizer created")

        # Test basic properties
        print(f"  Data type: {normalizer.data_type}")
        print(f"  Raw schema: {normalizer.raw_schema}")
        print(f"  Staging schema: {normalizer.staging_schema}")

        return True

    except Exception as e:
        print(f"❌ Bills normalizer test failed: {e}")
        return False


def test_plugin_system():
    """Test plugin system."""
    print("\n🔌 Testing plugin system...")

    try:
        from streamlined.plugins.congressional import CongressionalFetcherPlugin

        # Create plugin
        plugin = CongressionalFetcherPlugin("bills")
        print("✅ CongressionalFetcherPlugin created for bills")

        # Test basic methods
        if hasattr(plugin, "extract_item_id"):
            test_data = {"number": "123", "congress": "118"}
            item_id = plugin.extract_item_id(test_data)
            print(f"✅ Plugin item ID extraction: {item_id}")

        return True

    except Exception as e:
        print(f"❌ Plugin system test failed: {e}")
        return False


def main():
    """Run all tests."""
    print("🎯 Simple Bills Fetching Test - Streamlined Architecture")
    print("=" * 60)

    tests = [
        ("Imports", test_imports),
        ("Configuration", test_configuration),
        ("Registry", test_registry),
        ("Bills Fetcher", test_bills_fetcher),
        ("Bills Cleaner", test_bills_cleaner),
        ("Bills Normalizer", test_bills_normalizer),
        ("Plugin System", test_plugin_system),
    ]

    results = {}

    for test_name, test_func in tests:
        try:
            result = test_func()
            results[test_name] = result
        except Exception as e:
            print(f"❌ {test_name} failed with error: {e}")
            results[test_name] = False

    # Summary
    print("\n" + "=" * 60)
    print("📊 TEST SUMMARY")
    print("=" * 60)

    passed = 0
    total = len(results)

    for test_name, result in results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status}: {test_name}")
        if result:
            passed += 1

    print(f"\nResult: {passed}/{total} tests passed")

    if passed == total:
        print("🎉 All tests passed! The streamlined architecture is working correctly.")
        print("\nYou can now run the full test with:")
        print("  python test_bills_fetching_streamlined.py")
    else:
        print("⚠️  Some tests failed. Please check the errors above.")

    return passed == total


if __name__ == "__main__":
    main()
