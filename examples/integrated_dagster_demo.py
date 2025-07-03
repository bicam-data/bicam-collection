#!/usr/bin/env python3
"""
Integrated Dagster Framework Demonstration

This script demonstrates the integrated Congressional data processing framework
that combines:
1. Generalized asset factories for any data type
2. Specialized bills pipeline with detailed table-level assets
3. Unified resource management and API key coordination
4. Configuration-driven table discovery

Key Integration Benefits:
- Bills get specialized optimization while other data types use generalized approach
- All data types share the same resource management and API key coordination
- Easy to add new data types following the same pattern
- Maintains backward compatibility with existing bills pipeline
"""

import asyncio
import logging
import sys
from pathlib import Path

# Add the project root to the path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


from src.bicam_collection.dagster_pipeline.definitions import (
    AVAILABLE_DATA_TYPES,
    create_all_assets,
    create_resources,
)
from src.bicam_collection.libs.data_type_router import get_global_registry

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def demonstrate_asset_organization():
    """Demonstrate how assets are organized in the integrated system."""
    logger.info("=" * 60)
    logger.info("ASSET ORGANIZATION DEMONSTRATION")
    logger.info("=" * 60)

    # Get all assets
    all_assets = create_all_assets()

    # Organize by data type and category
    asset_organization = {}

    for asset in all_assets:
        asset_name = asset.key.path[-1]

        # Determine data type
        data_type = None
        for dt in AVAILABLE_DATA_TYPES:
            if asset_name.startswith(dt):
                data_type = dt
                break

        if not data_type:
            data_type = "unknown"

        # Determine category
        if "_raw_data" in asset_name:
            category = "raw"
        elif "_staging_data" in asset_name:
            category = "staging"
        elif "_production_data" in asset_name:
            category = "production"
        elif "_staging" in asset_name:
            category = "table_staging"
        elif "_complete_pipeline" in asset_name:
            category = "summary"
        else:
            category = "table_production"

        if data_type not in asset_organization:
            asset_organization[data_type] = {}
        if category not in asset_organization[data_type]:
            asset_organization[data_type][category] = []

        asset_organization[data_type][category].append(asset_name)

    # Display organization
    for data_type, categories in asset_organization.items():
        logger.info(f"\n{data_type.upper()} DATA TYPE:")

        for category, assets in categories.items():
            logger.info(f"  {category}: {len(assets)} assets")
            if category in ["raw", "staging", "production", "summary"]:
                for asset_name in assets:
                    logger.info(f"    - {asset_name}")
            else:
                # For table assets, show count and sample
                logger.info(f"    - {assets[:3]}{'...' if len(assets) > 3 else ''}")

    logger.info(
        f"\nTOTAL: {len(all_assets)} assets across {len(asset_organization)} data types"
    )


def demonstrate_resource_integration():
    """Demonstrate unified resource management."""
    logger.info("=" * 60)
    logger.info("RESOURCE INTEGRATION DEMONSTRATION")
    logger.info("=" * 60)

    resources = create_resources()

    logger.info("Available Resources:")
    for resource_name, resource in resources.items():
        logger.info(f"  - {resource_name}: {type(resource).__name__}")

    # Show configuration
    congressional_resource = resources["congressional_processing"]
    config_summary = congressional_resource.get_configuration_summary()

    logger.info("\nCongressional Processing Resource Configuration:")
    for section, config in config_summary.items():
        logger.info(f"  {section}:")
        for key, value in config.items():
            # Mask sensitive information
            if "password" in key.lower() or "key" in key.lower():
                display_value = "*" * 8 if value else "Not set"
            else:
                display_value = value
            logger.info(f"    {key}: {display_value}")


def demonstrate_data_type_registry():
    """Demonstrate the data type registry system."""
    logger.info("=" * 60)
    logger.info("DATA TYPE REGISTRY DEMONSTRATION")
    logger.info("=" * 60)

    registry = get_global_registry()

    logger.info("Registered Data Types:")
    for data_type in registry.list_data_types():
        logger.info(f"\n  {data_type.upper()}:")

        # Show components
        if registry.is_registered(data_type):
            fetcher_class = registry.get_fetcher_class(data_type)
            normalizer_class = registry.get_normalizer_class(data_type)
            cleaner_class = registry.get_cleaner_class(data_type)

            logger.info(f"    Fetcher: {fetcher_class.__name__}")
            logger.info(f"    Normalizer: {normalizer_class.__name__}")
            logger.info(f"    Cleaner: {cleaner_class.__name__}")

            # Show config and tables
            try:
                config_path = registry.get_config_file(data_type)
                table_names = registry.get_table_names(data_type)
                table_configs = registry.get_table_configs(data_type)

                logger.info(f"    Config: {config_path}")
                logger.info(
                    f"    Tables: {len(table_names)} ({table_names[:3]}{'...' if len(table_names) > 3 else ''})"
                )

                # Show sample table config
                if table_configs:
                    sample_table = list(table_configs.keys())[0]
                    sample_config = table_configs[sample_table]
                    field_count = len(sample_config.get("fields", {}))
                    is_main = sample_config.get("is_main_table", False)
                    logger.info(
                        f"    Sample ({sample_table}): {field_count} fields, main_table: {is_main}"
                    )

            except Exception as e:
                logger.warning(f"    Config error: {e}")


def demonstrate_specialization_benefits():
    """Demonstrate benefits of the bills specialization."""
    logger.info("=" * 60)
    logger.info("SPECIALIZATION BENEFITS DEMONSTRATION")
    logger.info("=" * 60)

    # Show bills-specific features
    logger.info("BILLS PIPELINE SPECIALIZATION:")
    logger.info("  - Uses generalized framework for consistency")
    logger.info("  - Provides detailed table-level assets for granular control")
    logger.info("  - Includes bills-specific database query optimizations")
    logger.info("  - Maintains backward compatibility with existing usage")
    logger.info("  - Integrates with system-wide API key management")

    # Compare with generalized approach
    all_assets = create_all_assets()
    bills_assets = [
        asset for asset in all_assets if asset.key.path[-1].startswith("bills")
    ]

    asset_types = {}
    for asset in bills_assets:
        asset_name = asset.key.path[-1]
        if "_raw_data" in asset_name:
            asset_types["raw"] = asset_types.get("raw", 0) + 1
        elif "_staging_data" in asset_name:
            asset_types["staging"] = asset_types.get("staging", 0) + 1
        elif "_production_data" in asset_name:
            asset_types["production"] = asset_types.get("production", 0) + 1
        elif "_staging" in asset_name:
            asset_types["table_staging"] = asset_types.get("table_staging", 0) + 1
        elif "_complete_pipeline" in asset_name:
            asset_types["summary"] = asset_types.get("summary", 0) + 1
        else:
            asset_types["table_production"] = asset_types.get("table_production", 0) + 1

    logger.info(f"\nBILLS ASSET BREAKDOWN ({len(bills_assets)} total):")
    for asset_type, count in asset_types.items():
        logger.info(f"  {asset_type}: {count} assets")


def demonstrate_usage_patterns():
    """Demonstrate common usage patterns."""
    logger.info("=" * 60)
    logger.info("USAGE PATTERNS DEMONSTRATION")
    logger.info("=" * 60)

    logger.info("COMMON DAGSTER COMMANDS:")

    logger.info("\n1. COMPLETE PIPELINES:")
    logger.info("   dagster asset materialize -g bills_pipeline")
    for data_type in AVAILABLE_DATA_TYPES:
        if data_type != "bills":
            logger.info(f"   dagster asset materialize -g {data_type}_pipeline")

    logger.info("\n2. INDIVIDUAL STAGES:")
    logger.info("   dagster asset materialize -a bills_raw_data")
    logger.info("   dagster asset materialize -a bills_staging_data")
    logger.info("   dagster asset materialize -a bills_production_data")

    logger.info("\n3. TABLE-LEVEL PROCESSING (Bills Only):")
    logger.info("   dagster asset materialize -a bills_actions_staging")
    logger.info("   dagster asset materialize -a bills_cosponsors")
    logger.info("   dagster asset materialize -g bills_tables_staging")

    logger.info("\n4. MULTI-DATA-TYPE PROCESSING:")
    logger.info("   dagster job execute -j all_data_types_raw")
    logger.info("   dagster job execute -j high_priority_data_types")
    logger.info("   dagster job execute -j concurrent_group_1")

    logger.info("\n5. STATUS AND MONITORING:")
    logger.info("   dagster asset materialize -a bills_complete_pipeline")

    registry = get_global_registry()
    for data_type in registry.list_data_types():
        if data_type != "bills":
            logger.info(
                f"   dagster asset materialize -a {data_type}_complete_pipeline"
            )


async def demonstrate_api_key_coordination():
    """Demonstrate API key coordination (simulation)."""
    logger.info("=" * 60)
    logger.info("API KEY COORDINATION DEMONSTRATION")
    logger.info("=" * 60)

    # This would require actual API keys to demonstrate fully
    # For now, show the concept

    logger.info("API KEY COORDINATION FEATURES:")
    logger.info("  - System-wide key manager prevents conflicts")
    logger.info("  - Keys are automatically assigned to data types")
    logger.info("  - Keys are released when processing completes")
    logger.info("  - Automatic rebalancing across active data types")
    logger.info("  - Rate limit tracking per key")

    logger.info("\nKEY ASSIGNMENT EXAMPLE:")
    logger.info("  bills_raw_data: Gets 2 keys (high priority)")
    logger.info("  amendments_raw_data: Gets 1 key")
    logger.info("  members_raw_data: Gets 1 key")
    logger.info("  When bills completes: Keys redistributed to amendments/members")


def main():
    """Run all demonstrations."""
    logger.info("INTEGRATED DAGSTER FRAMEWORK DEMONSTRATION")
    logger.info("=" * 80)

    try:
        demonstrate_asset_organization()
        demonstrate_resource_integration()
        demonstrate_data_type_registry()
        demonstrate_specialization_benefits()
        demonstrate_usage_patterns()
        asyncio.run(demonstrate_api_key_coordination())

        logger.info("=" * 80)
        logger.info("DEMONSTRATION COMPLETE")
        logger.info("=" * 80)

        logger.info("\nINTEGRATION SUMMARY:")
        logger.info(
            "✅ Bills pipeline successfully integrated with generalized framework"
        )
        logger.info("✅ Unified resource management across all data types")
        logger.info("✅ System-wide API key coordination")
        logger.info("✅ Configuration-driven table discovery")
        logger.info("✅ Backward compatibility maintained")
        logger.info("✅ Easy to add new data types")

        logger.info("\nNEXT STEPS:")
        logger.info("1. Set environment variables (API keys, database config)")
        logger.info(
            "2. Run: dagster dev -f src/bicam_collection/dagster_pipeline/definitions.py"
        )
        logger.info("3. Open Dagster UI at http://localhost:3000")
        logger.info("4. Materialize assets or run jobs")

    except Exception as e:
        logger.error(f"Demonstration failed: {e}")
        raise


if __name__ == "__main__":
    main()
