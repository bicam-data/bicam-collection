#!/usr/bin/env python3
"""
Streamlined Dagster Definitions

This file defines the complete Dagster setup using the new streamlined architecture:
- Dynamic asset creation for all registered data types
- StreamlinedProcessingResource instead of the old ProcessingResource
- Proper Dagster lineage and orchestration
- No duplicate boilerplate code

Usage:
    dagster dev -f dagster_streamlined.py
"""

import logging
import os
from pathlib import Path

from dagster import Definitions, load_assets_from_modules

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Streamlined imports
from streamlined.dagster_assets import ALL_ASSETS
from streamlined.resources import (
    StreamlinedProcessingResource,
    create_streamlined_dagster_resource,
)

# Load environment variables
from dotenv import load_dotenv

load_dotenv()

# Configuration
PROJECT_ROOT = Path(__file__).parent
CONFIG_FILE = PROJECT_ROOT / "config.yaml"

# Create the streamlined processing resource
processing_resource = create_streamlined_dagster_resource(
    config_file=str(CONFIG_FILE),
    # Use environment variables for database configuration
    database_url=os.getenv("DATABASE_URL"),
    api_key=os.getenv("CONGRESS_API_KEY"),
    storage_path=str(PROJECT_ROOT / "data"),
    log_level="INFO",
)

# Create Dagster definitions
defs = Definitions(
    assets=ALL_ASSETS,
    resources={
        "processing_resource": processing_resource,
    },
    # Add job definitions for different scenarios
    jobs=[
        # Define jobs here if needed for specific execution patterns
    ],
    # Add schedules if needed
    schedules=[
        # Define schedules here if needed
    ],
    # Add sensors if needed
    sensors=[
        # Define sensors here if needed
    ],
)

# Export for dagster CLI
__all__ = ["defs"]

if __name__ == "__main__":
    logger.info("Streamlined Dagster setup initialized")
    logger.info(f"Total assets available: {len(ALL_ASSETS)}")

    # Print asset summary
    from streamlined.libs.data_type_registry import get_global_registry

    registry = get_global_registry()
    data_types = registry.get_all_data_types()

    logger.info(f"Data types with assets: {data_types}")
    logger.info("Run with: dagster dev -f dagster_streamlined.py")
