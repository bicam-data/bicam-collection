#!/usr/bin/env python3
"""
Test script for dynamic schema setup using config.yaml files.

This script tests the new setup_schemas method that dynamically discovers
and processes config.yaml files to create database tables.
"""

import asyncio
import logging
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


async def test_dynamic_schema_setup():
    """Test the dynamic schema setup functionality"""
    try:
        # Import after setting up logging
        from src.bicam_collection.libs.config import BicamConfig
        from src.bicam_collection.libs.database import DatabaseManager

        # Load configuration
        config = BicamConfig()

        # Create database manager
        db_manager = DatabaseManager(config)

        # Test connection
        logger.info("Testing database connection...")
        if not await db_manager.test_connection():
            logger.error("Database connection failed")
            return False

        # Test schema setup
        logger.info("Starting dynamic schema setup...")
        success = await db_manager.setup_schemas()

        if success:
            logger.info("✅ Dynamic schema setup completed successfully!")

            # Verify some tables were created
            logger.info("Verifying table creation...")
            schemas_to_check = [
                "bicam_raw_congressional",
                "bicam_staging_congressional",
                "bicam_congressional",
            ]

            for schema in schemas_to_check:
                tables = await db_manager.get_existing_tables(schema)
                logger.info(
                    f"Schema '{schema}' has {len(tables)} tables: {sorted(tables)}"
                )

            return True
        else:
            logger.error("❌ Dynamic schema setup failed")
            return False

    except Exception as e:
        logger.error(f"Error in test: {e}")
        return False


async def test_schema_discovery():
    """Test just the config file discovery part"""
    try:
        from src.bicam_collection.data_types.schema_loader import (
            get_all_data_type_configs,
        )

        # Find config files
        data_types_path = Path("src/bicam_collection/data_types")
        config_files = list(data_types_path.rglob("config.yaml"))

        logger.info(f"Found {len(config_files)} config.yaml files:")
        for config_file in config_files:
            logger.info(f"  - {config_file}")

            # Test loading each config
            try:
                configs = get_all_data_type_configs(config_file)
                logger.info(f"    Loaded {len(configs)} table configs")
                for config in configs:
                    logger.info(
                        f"      - {config.name} (main={config.is_main}, create_raw={config.create_raw})"
                    )
            except Exception as e:
                logger.error(f"    Error loading config: {e}")

        return True

    except Exception as e:
        logger.error(f"Error in schema discovery test: {e}")
        return False


if __name__ == "__main__":
    print("🔍 Testing config file discovery...")
    asyncio.run(test_schema_discovery())

    print("\n🔧 Testing dynamic schema setup...")
    success = asyncio.run(test_dynamic_schema_setup())

    if success:
        print("\n✅ All tests passed!")
    else:
        print("\n❌ Some tests failed!")
