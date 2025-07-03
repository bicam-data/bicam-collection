"""
Example: Table Names Extraction from Config Files

This example demonstrates how the enhanced data type router extracts
table names directly from YAML configuration files, eliminating the need
for hardcoded table lists.

The system now:
1. Reads table names from config files automatically
2. Provides fallback behavior if config is missing
3. Supports table-level configuration access
4. Integrates seamlessly with Dagster asset generation
"""

import logging
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path.cwd() / "src"))

from bicam_collection.libs.data_type_router import get_global_registry

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def demonstrate_table_extraction():
    """Demonstrate automatic table name extraction from config files."""

    print("=== Data Type Router - Table Names Extraction ===\n")

    # Get the global registry
    registry = get_global_registry()

    # List available data types
    available_types = registry.list_data_types()
    print(f"Available data types: {available_types}\n")

    # Extract table names for each registered data type
    for data_type in available_types:
        print(f"Data Type: {data_type}")
        print("-" * 40)

        # Get config file path
        try:
            config_file = registry.get_config_file(data_type)
            print(f"Config file: {config_file}")

            # Check if config file exists
            config_path = Path(config_file)
            if not config_path.exists():
                config_path = Path.cwd() / config_file

            exists = "✓" if config_path.exists() else "✗"
            print(f"Config exists: {exists}")

            # Extract table names
            table_names = registry.get_table_names(data_type)
            print(f"Tables ({len(table_names)}):")

            for i, table_name in enumerate(table_names, 1):
                # Get table-specific config
                table_config = registry.get_table_config(data_type, table_name)

                is_main = table_config.get("main", False)
                field_count = len(table_config.get("fields", []))

                main_indicator = " (MAIN)" if is_main else ""
                print(f"  {i:2d}. {table_name}{main_indicator}")
                print(f"      Fields: {field_count}")

                if table_config.get("related_fields"):
                    related = ", ".join(table_config["related_fields"])
                    print(f"      Related: {related}")

            print()

        except Exception as e:
            print(f"Error processing {data_type}: {e}\n")


def demonstrate_dagster_integration():
    """Show how this integrates with Dagster asset generation."""

    print("=== Dagster Integration Preview ===\n")

    registry = get_global_registry()

    for data_type in registry.list_data_types():
        table_names = registry.get_table_names(data_type)

        print(f"Assets that would be created for '{data_type}':")
        print("-" * 50)

        # Main pipeline assets
        print("Pipeline Assets:")
        print(f"  • {data_type}_raw_data")
        print(f"  • {data_type}_staging_data")
        print(f"  • {data_type}_production_data")

        # Table-level assets
        print(f"\nTable-Level Assets ({len(table_names)} tables):")
        for table_name in table_names:
            print(f"  • {table_name}_staging")
            print(f"  • {table_name}")  # Production (no suffix)

        # Asset groups
        print("\nAsset Groups:")
        print(f"  • {data_type}_pipeline")
        print(f"  • {data_type}_tables_staging")
        print(f"  • {data_type}_tables_production")

        # Jobs that would be available
        print("\nJobs:")
        print(f"  • {data_type}_full_pipeline")
        print(f"  • {data_type}_raw_only")
        print(f"  • {data_type}_staging_only")
        print(f"  • {data_type}_production_only")
        print(f"  • {data_type}_staging_tables")
        print(f"  • {data_type}_production_tables")

        print("\n" + "=" * 60 + "\n")


def demonstrate_config_access():
    """Show how to access detailed table configuration."""

    print("=== Table Configuration Access ===\n")

    registry = get_global_registry()

    # Example with bills data type
    if registry.is_registered("bills"):
        data_type = "bills"
        table_names = registry.get_table_names(data_type)

        print(f"Detailed configuration for '{data_type}' tables:\n")

        for table_name in table_names[:3]:  # Show first 3 tables
            config = registry.get_table_config(data_type, table_name)

            print(f"Table: {table_name}")
            print("-" * 30)
            print(f"Main table: {config.get('main', False)}")
            print(f"Fields: {len(config.get('fields', []))}")
            print(f"ID fields: {config.get('id_fields', [])}")

            if config.get("related_fields"):
                print(f"Related fields: {config.get('related_fields', [])}")

            # Show first few fields
            fields = config.get("fields", [])
            if fields:
                print(f"Sample fields: {fields[:5]}")
                if len(fields) > 5:
                    print(f"... and {len(fields) - 5} more")

            print()


if __name__ == "__main__":
    print("Testing Enhanced Data Type Router\n")

    try:
        demonstrate_table_extraction()
        demonstrate_dagster_integration()
        demonstrate_config_access()

        print("✓ All demonstrations completed successfully!")

    except Exception as e:
        logger.error(f"Error in demonstration: {e}")
        raise
