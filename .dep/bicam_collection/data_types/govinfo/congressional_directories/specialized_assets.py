"""
Congressional Directories GovInfo specialized assets.

This module provides Dagster assets for Congressional Directories data processing.
"""

import logging

from ..base import GovInfoBaseSpecializedAssets
from .cleaner import CongressionalDirectoriesCleaner
from .database_normalizer import CongressionalDirectoriesDatabaseNormalizer
from .fetcher import CongressionalDirectoriesFetcher

logger = logging.getLogger(__name__)


class CongressionalDirectoriesSpecializedAssets(GovInfoBaseSpecializedAssets):
    """
    Congressional Directories specialized assets.

    Provides Congressional Directories-specific asset implementations.
    """

    def __init__(self):
        super().__init__("congressional_directories")

    def get_fetcher_class(self):
        """Get the Congressional Directories fetcher class."""
        return CongressionalDirectoriesFetcher

    def get_database_normalizer_class(self):
        """Get the Congressional Directories database normalizer class."""
        return CongressionalDirectoriesDatabaseNormalizer

    def get_cleaner_class(self):
        """Get the Congressional Directories cleaner class."""
        return CongressionalDirectoriesCleaner
