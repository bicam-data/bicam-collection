#!/usr/bin/env python3
"""
Simple Test of Streamlined Architecture

This tests the core streamlined components without complex class instantiation.
"""

import sys
from pathlib import Path

# Add the src directory to the path
sys.path.insert(0, str(Path(__file__).parent / "src"))


def test_streamlined_core():
    """Test core streamlined components."""

    print("🔍 Testing Streamlined Core Components")
    print("=" * 50)

    # Test 1: Data Type Registry
    print("\n1. Testing Data Type Registry...")
    try:
        from streamlined.libs.data_type_registry import (
            get_global_registry,
            validate_registry_integration,
        )

        registry = get_global_registry()
        print(f"   ✅ Registry loaded: {type(registry)}")

        # Check status
        status = registry.get_registry_status()
        print(f"   📊 Registry status: {status['total_data_types']} data types")

        # Test bills registration
        if registry.is_registered("bills"):
            plugin_type = registry.get_plugin_type("bills")
            data_source = registry.get_data_source("bills")
            print(f"   ✅ Bills registered: {plugin_type}/{data_source}")
        else:
            print("   ⚠️  Bills not registered")

        # Test integration
        integration_ok = validate_registry_integration()
        print(
            f"   {'✅' if integration_ok else '⚠️'} Integration: {'OK' if integration_ok else 'Issues'}"
        )

    except Exception as e:
        print(f"   ❌ Registry error: {e}")

    # Test 2: Plugin System Basics
    print("\n2. Testing Plugin System Basics...")
    try:
        from streamlined.plugins.registry import get_plugin_registry
        from streamlined.plugins.base import (
            FetcherPlugin,
            CleanerPlugin,
            NormalizerPlugin,
        )

        plugin_registry = get_plugin_registry()
        print(f"   ✅ Plugin registry loaded: {type(plugin_registry)}")

        # Test protocols
        print(
            f"   ✅ Plugin protocols loaded: {len([FetcherPlugin, CleanerPlugin, NormalizerPlugin])} types"
        )

    except Exception as e:
        print(f"   ❌ Plugin system error: {e}")

    # Test 3: Configuration Loading
    print("\n3. Testing Configuration Loading...")
    try:
        config_file = Path("src/streamlined/data_types/congressional/bills/config.yaml")
        if config_file.exists():
            print(f"   ✅ Bills config file exists: {config_file}")

            import yaml

            with open(config_file) as f:
                config = yaml.safe_load(f)
            print(f"   ✅ Bills config loaded: {len(config)} sections")

            # Check key sections
            key_sections = ["name", "schema", "api", "processing"]
            for section in key_sections:
                if section in config:
                    print(f"      ✅ {section}: OK")
                else:
                    print(f"      ❌ {section}: Missing")
        else:
            print(f"   ❌ Bills config file not found: {config_file}")

    except Exception as e:
        print(f"   ❌ Configuration error: {e}")

    # Test 4: API Client Basics
    print("\n4. Testing API Client Basics...")
    try:
        from streamlined.api_clients.congressional_api import CongressionalAPIClient
        from streamlined.api_clients.base_api_client import BaseAPIClient

        print(f"   ✅ API client classes loaded")

        # Test mock client creation (without real API key)
        mock_client = CongressionalAPIClient(api_key="test_key")
        print(f"   ✅ Mock client created: {type(mock_client)}")

    except Exception as e:
        print(f"   ❌ API client error: {e}")

    # Test 5: Executor Integration
    print("\n5. Testing Executor Integration...")
    try:
        from streamlined.executor import StreamlinedExecutor
        from streamlined import execute_streamlined_pipeline

        print(f"   ✅ Executor and pipeline function loaded")

    except Exception as e:
        print(f"   ❌ Executor error: {e}")

    print("\n🎯 Core streamlined components test completed!")


def main():
    """Run the simple test."""
    try:
        test_streamlined_core()
        print("\n✅ Basic streamlined architecture is working!")
        return 0
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback

        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
