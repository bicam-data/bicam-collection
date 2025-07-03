"""
Abstract base classes for data pipeline components.

These classes define pure interfaces with no assumptions about:
- Data sources (Congressional, GovInfo, etc.)
- Schema naming conventions
- API patterns or processing flows
- Progress tracking systems

All concrete implementations must inherit from these abstracts
through source-specific base classes.
"""

from .abstract_cleaner import AbstractCleaner
from .abstract_database_normalizer import AbstractDatabaseNormalizer
from .abstract_fetcher import AbstractFetcher
from .abstract_specialized_assets import AbstractSpecializedAssets

__all__ = [
    "AbstractFetcher",
    "AbstractDatabaseNormalizer",
    "AbstractCleaner",
    "AbstractSpecializedAssets",
]
