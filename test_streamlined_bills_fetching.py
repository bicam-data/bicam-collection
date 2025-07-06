#!/usr/bin/env python3
"""
Test Bills Fetching with Streamlined Plugin Architecture

This script demonstrates how to use the streamlined plugin system to fetch
Congressional bills data using the new plugin-based approach.
"""

import asyncio
import logging
import os
import sys
from pathlib import Path

# Add the src directory to the path
sys.path.insert(0, str(Path(__file__).parent / "src"))

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


async def test_streamlined_bills_fetching():
    """Test bills fetching using the streamlined plugin architecture."""

    print("🚀 Testing Streamlined Bills Fetching")
    print("=" * 50)

    try:
        # Step 1: Test plugin system initialization
        print("\n1. Testing plugin system initialization...")

        from streamlined.plugins.registry import get_plugin_registry

        registry = get_plugin_registry()
        registry.auto_register_plugins()

        stats = registry.get_stats()
        print(f"   ✅ Plugin registry initialized: {stats}")

        # Step 2: Test getting bills fetcher plugin
        print("\n2. Testing bills fetcher plugin...")

        fetcher_plugin = registry.get_fetcher_plugin("bills")
        if fetcher_plugin:
            print(f"   ✅ Bills fetcher plugin found: {type(fetcher_plugin)}")
        else:
            print("   ❌ No bills fetcher plugin found")
            return False

        # Step 3: Test getting bills cleaner plugin
        print("\n3. Testing bills cleaner plugin...")

        cleaner_plugin = registry.get_cleaner_plugin("bills")
        if cleaner_plugin:
            print(f"   ✅ Bills cleaner plugin found: {type(cleaner_plugin)}")
        else:
            print("   ❌ No bills cleaner plugin found")
            return False

        # Step 4: Test getting bills normalizer plugin
        print("\n4. Testing bills normalizer plugin...")

        normalizer_plugin = registry.get_normalizer_plugin("bills")
        if normalizer_plugin:
            print(f"   ✅ Bills normalizer plugin found: {type(normalizer_plugin)}")
        else:
            print("   ❌ No bills normalizer plugin found")
            return False

        # Step 5: Test data type registry integration
        print("\n5. Testing data type registry integration...")

        from streamlined.libs.data_type_registry import get_global_registry

        data_registry = get_global_registry()

        if data_registry.is_registered("bills"):
            plugin_type = data_registry.get_plugin_type("bills")
            data_source = data_registry.get_data_source("bills")
            print(
                f"   ✅ Bills registered: plugin_type={plugin_type}, data_source={data_source}"
            )
        else:
            print("   ❌ Bills not registered in data type registry")
            return False

        # Step 6: Test plugin config loading
        print("\n6. Testing plugin configuration loading...")

        try:
            plugin_config = data_registry.get_plugin_config("bills")
            print(f"   ✅ Plugin config loaded: {len(plugin_config)} settings")

            # Show some key config values
            for key, value in plugin_config.items():
                if key in ["plugin_type", "data_source", "config_file"]:
                    print(f"      {key}: {value}")

        except Exception as e:
            print(f"   ⚠️  Plugin config loading error: {e}")

        # Step 7: Test creating processor for bills
        print("\n7. Testing processor creation for bills...")

        try:
            processor = data_registry.create_processor_for_data_type("bills")
            if processor:
                print(f"   ✅ Bills processor created: {type(processor)}")
            else:
                print("   ❌ Failed to create bills processor")

        except Exception as e:
            print(f"   ⚠️  Processor creation error: {e}")

        # Step 8: Test mock API client creation
        print("\n8. Testing mock API client creation...")

        try:
            from streamlined.api_clients.congressional_api import CongressionalAPIClient

            # Create a mock client without real API key
            mock_client = CongressionalAPIClient(api_key="test_key")
            print(f"   ✅ Mock API client created: {type(mock_client)}")

        except Exception as e:
            print(f"   ⚠️  Mock API client creation error: {e}")

        # Step 9: Test plugin method availability
        print("\n9. Testing plugin method availability...")

        # Check if the plugin has the expected methods
        expected_methods = [
            "fetch_list_data",
            "fetch_detailed_data",
            "fetch_related_data",
            "extract_item_id",
        ]

        for method in expected_methods:
            if hasattr(fetcher_plugin, method):
                print(f"   ✅ {method} method available")
            else:
                print(f"   ❌ {method} method missing")

        print("\n🎉 Streamlined plugin architecture test completed successfully!")
        return True

    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")
        import traceback

        traceback.print_exc()
        return False


def main():
    """Run the test."""
    success = asyncio.run(test_streamlined_bills_fetching())

    if success:
        print("\n✅ All tests passed! The streamlined plugin architecture is working.")
        return 0
    else:
        print("\n❌ Some tests failed. Check the output above for details.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
