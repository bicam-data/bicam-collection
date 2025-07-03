"""
Amendments data type handlers.

This module provides specialized handling for Congressional amendments data,
including fetching, database normalization, cleaning, and Dagster assets.
"""

from .cleaner import AmendmentsCleaner
from .database_normalizer import AmendmentsDatabaseNormalizer
from .fetcher import AmendmentsFetcher


def get_specialized_assets():
    """Lazily import and return amendments specialized assets to avoid circular imports."""
    from .specialized_assets import amendments_specialized_assets

    return amendments_specialized_assets


__all__ = [
    "AmendmentsFetcher",
    "AmendmentsDatabaseNormalizer",
    "AmendmentsCleaner",
    "get_specialized_assets",
]
