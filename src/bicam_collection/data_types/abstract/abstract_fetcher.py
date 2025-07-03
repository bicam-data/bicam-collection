"""
Abstract fetcher interface with no data source assumptions.
"""

from abc import ABC, abstractmethod
from typing import Any


class AbstractFetcher(ABC):
    """
    Pure abstract fetcher with no data source assumptions.

    Defines only the essential interface that all fetchers must implement,
    without imposing any specific processing patterns or schema conventions.
    """

    def __init__(
        self,
        client,
        db_pool=None,
        data_type_name=None,
        checkpoint_manager=None,
        run_manager=None,
    ):
        self.client = client
        self.db_pool = db_pool
        self.data_type_name = data_type_name
        self.checkpoint_manager = checkpoint_manager
        self.run_manager = run_manager
        self.progress_tracker = None
        self.current_run_id = None

    @abstractmethod
    def extract_item_id(self, item_data: dict[str, Any]) -> str:
        """Extract standardized ID from item data."""

    @abstractmethod
    def _extract_preliminary_item_id(self, list_item: dict[str, Any]) -> str:
        """Extract preliminary ID from list item data."""

    @abstractmethod
    async def process_items(self, **kwargs) -> dict[str, Any]:
        """Main processing method - implementation completely defined by subclasses."""

    @abstractmethod
    def setup_progress_tracker(self):
        """Setup progress tracking - source-specific implementation required."""

    @abstractmethod
    async def store_raw_data(
        self, schema: str, table: str, items: list[dict], **kwargs
    ):
        """Store raw data - must specify schema explicitly, no defaults."""
