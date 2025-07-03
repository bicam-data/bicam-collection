"""
Test script for the new schema loading functionality.

This script demonstrates how the updated schema loader works with
the new config.yaml format that contains multiple data type configurations.
"""

import logging
from pathlib import Path

from src.bicam_collection.data_types.schema_loader import (
    get_data_type_config,
    get_all_data_type_configs,
    get_main_data_type_config,
    get_related_configs_for_data_type,
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_schema_loading():
    """Test the new schema loading functionality."""

    # Path to the new config.yaml
    config_path = Path(
        "src/bicam_collection/data_types/congressional/bills/config.yaml"
    )

    print("🧪 Testing New Schema Loading")
    print("=" * 50)

    # Test 1: Get main data type config
    print("\n1️⃣ Getting main data type config...")
    main_config = get_main_data_type_config(config_path)
    if main_config:
        print(f"✅ Main config: {main_config.name} (is_main: {main_config.is_main})")
        print(f"   Related fields: {main_config.related_fields}")
        print(f"   Nested fields: {main_config.nested_fields}")
    else:
        print("❌ No main config found")

    # Test 2: Get specific data type config
    print("\n2️⃣ Getting specific data type config...")
    bills_config = get_data_type_config("bills", config_path)
    if bills_config:
        print(f"✅ Bills config: {bills_config.name}")
        print(f"   Table name: {bills_config.table_name}")
        print(f"   ID fields: {bills_config.id_fields}")
        print(f"   Create raw: {bills_config.create_raw}")
    else:
        print("❌ Bills config not found")

    # Test 3: Get all data type configs
    print("\n3️⃣ Getting all data type configs...")
    all_configs = get_all_data_type_configs(config_path)
    print(f"✅ Found {len(all_configs)} total configurations:")
    for config in all_configs:
        print(
            f"   - {config.name} (is_main: {config.is_main}, create_raw: {config.create_raw})"
        )

    # Test 4: Get related configs for bills
    print("\n4️⃣ Getting related configs for bills...")
    related_configs = get_related_configs_for_data_type("bills", config_path)
    print(f"✅ Found {len(related_configs)} related configurations:")
    for config in related_configs:
        print(f"   - {config.name} -> {config.table_name}")

    # Test 5: Test specific related data types
    print("\n5️⃣ Testing specific related data types...")
    test_related = ["actions", "cosponsors", "summaries", "texts"]
    for related_name in test_related:
        full_name = f"bills_{related_name}"
        config = get_data_type_config(full_name, config_path)
        if config:
            print(
                f"✅ {full_name} -> {config.table_name} (create_raw: {config.create_raw})"
            )
        else:
            print(f"❌ {full_name} not found")


if __name__ == "__main__":
    test_schema_loading()
