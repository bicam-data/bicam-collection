"""
Data Types Module

This module provides the framework for processing different types of Congressional data.
The architecture follows a clean 3-phase processing pipeline:

1. **Fetching**: Retrieve data from external APIs (BaseFetcher)
2. **Normalization**: Flatten and structure raw data (BaseDatabaseNormalizer)
3. **Cleaning**: Transform and clean data for production (BaseCleaner)
4. **Assets**: Define Dagster pipeline assets (BaseSpecializedAssets)

Usage:
    # For implementing new data types
    from bicam_collection.data_types.base import (
        BaseFetcher,
        BaseDatabaseNormalizer,
        BaseCleaner,
        BaseSpecializedAssets,
    )

    # For using existing data types
    from bicam_collection.data_types.congressional.bills import BillsFetcher
"""

# Import base classes for easy access
from .abstract import (
    AbstractCleaner,
    AbstractDatabaseNormalizer,
    AbstractFetcher,
    AbstractSpecializedAssets,
)
from .congressional.base import (
    CongressionalBaseCleaner,
    CongressionalBaseDatabaseNormalizer,
    CongressionalBaseFetcher,
    CongressionalBaseSpecializedAssets,
)

# Import schema loader for configuration management
from .schema_loader import (
    get_all_data_type_configs,
    get_data_type_config,
    get_main_data_type_config,
)

__all__ = [
    # Base classes for implementing new data types
    "AbstractFetcher",
    "AbstractDatabaseNormalizer",
    "AbstractCleaner",
    "AbstractSpecializedAssets",
    # Congressional base classes
    "CongressionalBaseCleaner",
    "CongressionalBaseDatabaseNormalizer",
    "CongressionalBaseFetcher",
    "CongressionalBaseSpecializedAssets",
    # Configuration utilities
    "get_data_type_config",
    "get_all_data_type_configs",
    "get_main_data_type_config",
]
