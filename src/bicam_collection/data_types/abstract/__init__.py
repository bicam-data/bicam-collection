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

from .base_cleaner import BaseCleaner
from .base_database_normalizer import BaseDatabaseNormalizer
from .base_fetcher import BaseFetcher
from .base_specialized_assets import BaseSpecializedAssets

__all__ = [
    "BaseFetcher",
    "BaseDatabaseNormalizer",
    "BaseCleaner",
    "BaseSpecializedAssets",
]
