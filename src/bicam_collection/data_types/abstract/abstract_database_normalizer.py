"""
Abstract database normalizer interface with no schema assumptions.
"""

from abc import ABC, abstractmethod
from typing import Any


class AbstractDatabaseNormalizer(ABC):
    """
    Pure abstract normalizer with no schema assumptions.
    """

    def __init__(
        self,
        config_path=None,
        db_pool=None,
        checkpoint_manager=None,
        run_manager=None,
        data_type_name=None,
        target_schema=None,
        source_schema=None,
    ):
        # IMPORTANT: No default schema values
        self.config_path = config_path
        self.db_pool = db_pool
        self.checkpoint_manager = checkpoint_manager
        self.run_manager = run_manager
        self.data_type_name = data_type_name
        self.target_schema = target_schema
        self.source_schema = source_schema
        self.progress_tracker = None
        self.current_run_id = None

        # Validate required schemas
        if not target_schema or not source_schema:
            raise ValueError(
                "target_schema and source_schema must be explicitly provided"
            )

    @abstractmethod
    async def process_items(self, item_ids: list[str], **kwargs) -> dict[str, Any]:
        """Main normalization processing method."""

    @abstractmethod
    def setup_progress_tracker(self):
        """Setup progress tracking - source-specific implementation."""
