"""
GovInfo-specific base classes.

These classes inherit from the abstract base classes and provide
GovInfo-specific implementations including:
- 4-phase processing pattern (collection → package → granules → granule data)
- GovInfo schema defaults
- GovInfo progress tracking configuration
- GovInfo API patterns
"""

# Import shared base classes from abstract module
from ...abstract import BaseCleaner, BaseDatabaseNormalizer, BaseSpecializedAssets
from .govinfo_base_fetcher import GovInfoBaseFetcher

__all__ = [
    "GovInfoBaseFetcher",
    "BaseDatabaseNormalizer",
    "BaseCleaner",
    "BaseSpecializedAssets",
]
