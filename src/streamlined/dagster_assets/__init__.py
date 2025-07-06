"""
Streamlined Dagster Assets

This module provides dynamic asset creation for all registered data types
using the streamlined architecture. It replaces the need for individual
specialized_assets.py files in each data type folder.

Key Benefits:
- Single source of truth for asset definitions
- Automatic asset creation for all registered data types
- Proper Dagster lineage and orchestration
- No duplicate boilerplate code
- Takes advantage of Dagster's features
"""

from .dynamic_asset_factory import (
    create_all_registered_assets,
    create_configurable_asset,
    create_data_type_assets,
    system_monitoring_assets,
)

# Create all assets dynamically
ALL_ASSETS = create_all_registered_assets()

# Add system monitoring assets
ALL_ASSETS.append(system_monitoring_assets)

# Export everything
__all__ = [
    "ALL_ASSETS",
    "create_all_registered_assets",
    "create_data_type_assets",
    "create_configurable_asset",
    "system_monitoring_assets",
]
