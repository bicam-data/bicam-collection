"""
Abstract specialized assets interface with no source assumptions.
"""

from abc import ABC, abstractmethod


class AbstractSpecializedAssets(ABC):
    """
    Pure abstract assets with no source assumptions.
    """

    def __init__(self, data_type_name: str, source_system: str):
        self.data_type_name = data_type_name
        self.source_system = source_system  # "congressional", "govinfo", etc.

    @abstractmethod
    def get_fetcher_class(self):
        """Get the fetcher class for this data type."""

    @abstractmethod
    def get_database_normalizer_class(self):
        """Get the database normalizer class for this data type."""

    @abstractmethod
    def get_cleaner_class(self):
        """Get the cleaner class for this data type."""

    @abstractmethod
    def create_complete_pipeline_asset(self, **kwargs):
        """Create complete pipeline asset."""

    @abstractmethod
    def create_data_quality_report_asset(self, **kwargs):
        """Create data quality report asset."""
