"""
Plugin System for Streamlined Pipeline

This module provides a clean plugin system that preserves all existing custom logic
from the original complex architecture while enabling the streamlined approach.

The plugin system automatically wraps existing implementations:
- CongressionalFetcherPlugin wraps BillsFetcher, etc.
- CongressionalCleanerPlugin wraps BillsCleaner, etc.
- CongressionalNormalizerPlugin wraps BillsDatabaseNormalizer, etc.

All existing custom methods are preserved with no code changes required.
"""

from .base import (
    BaseCleaningUtilities,
    CleanerPlugin,
    CongressionalBaseFetcherLogic,
    FetcherPlugin,
    NormalizerPlugin,
)
from .congressional import (
    CongressionalFetcherPlugin,
)
from .consolidated_registry import get_consolidated_registry
from .govinfo import (
    GovInfoCleanerPlugin,
    GovInfoFetcherPlugin,
    GovInfoNormalizerPlugin,
)

__all__ = [
    "BaseCleaningUtilities",
    "CongressionalBaseFetcherLogic",
    "FetcherPlugin",
    "CleanerPlugin",
    "NormalizerPlugin",
    "get_consolidated_registry",
    "CongressionalFetcherPlugin",
    "GovInfoFetcherPlugin",
    "GovInfoCleanerPlugin",
    "GovInfoNormalizerPlugin",
]
