"""
Congressional-specific base classes.

These classes inherit from the abstract base classes and provide
Congressional-specific implementations including:
- 3-phase processing pattern (list → full → related)
- Congressional schema defaults
- Congressional progress tracking configuration
- Congressional API patterns
"""

# Import shared base classes from abstract module
from ...abstract import BaseCleaner, BaseDatabaseNormalizer, BaseSpecializedAssets
from .congressional_base_fetcher import CongressionalBaseFetcher

__all__ = [
    "CongressionalBaseFetcher",
    "BaseDatabaseNormalizer",
    "BaseCleaner",
    "BaseSpecializedAssets",
]
