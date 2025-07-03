"""
GovInfo-specific base classes.

These classes inherit from the abstract base classes and provide
GovInfo-specific implementations including:
- 4-phase processing pattern (collection → package → granules → granule data)
- GovInfo schema defaults
- GovInfo progress tracking configuration
- GovInfo API patterns
"""

from .govinfo_base_cleaner import GovInfoBaseCleaner
from .govinfo_base_database_normalizer import GovInfoBaseDatabaseNormalizer
from .govinfo_base_fetcher import GovInfoBaseFetcher
from .govinfo_base_specialized_assets import GovInfoBaseSpecializedAssets

__all__ = [
    "GovInfoBaseFetcher",
    "GovInfoBaseDatabaseNormalizer",
    "GovInfoBaseCleaner",
    "GovInfoBaseSpecializedAssets",
]
