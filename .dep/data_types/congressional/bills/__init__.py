"""
Bills data type handlers.

This module provides specialized handling for Congressional bills data,
including fetching, database normalization, cleaning, and Dagster assets.
"""

from .cleaner import BillsCleaner
from .database_normalizer import BillsDatabaseNormalizer
from .fetcher import BillsFetcher


def get_specialized_assets():
    """Lazily import and return bills specialized assets to avoid circular imports."""
    from .specialized_assets import bills_specialized_assets

    return bills_specialized_assets


__all__ = [
    "BillsFetcher",
    "BillsDatabaseNormalizer",
    "BillsCleaner",
    "get_specialized_assets",
]
