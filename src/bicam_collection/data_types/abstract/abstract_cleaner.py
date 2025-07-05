"""
Abstract cleaner interface with no schema assumptions.
"""

from abc import ABC, abstractmethod
from typing import Any


class AbstractCleaner(ABC):
    """
    Pure abstract cleaner with no schema assumptions.
    """

    def __init__(
        self,
        config_path=None,
        db_pool=None,
        checkpoint_manager=None,
        run_manager=None,
        data_type_name=None,
        staging_schema=None,
        production_schema=None,
    ):
        # IMPORTANT: No default schema values
        self.config_path = config_path
        self.db_pool = db_pool
        self.checkpoint_manager = checkpoint_manager
        self.run_manager = run_manager
        self.data_type_name = data_type_name
        self.staging_schema = staging_schema
        self.production_schema = production_schema
        self.progress_tracker = None
        self.current_run_id = None
        self.processing_resource = None  # Added for processing resource support

        # Validate required schemas
        if not staging_schema or not production_schema:
            raise ValueError(
                "staging_schema and production_schema must be explicitly provided"
            )

    @abstractmethod
    async def process_items(self, **kwargs) -> dict[str, Any]:
        """Main cleaning processing method."""

    @abstractmethod
    def setup_progress_tracker(self):
        """Setup progress tracking - source-specific implementation."""

    @abstractmethod
    def get_data_types_to_process(self) -> list[str]:
        """Get list of data types to clean."""

    def set_processing_resource(self, processing_resource) -> None:
        """
        Set the processing resource for accessing shared resources.

        This method is called by the Dagster pipeline to provide access to:
        - Shared database pools
        - Checkpoint managers
        - Other shared resources

        Args:
            processing_resource: Resource providing shared infrastructure
        """
        self.processing_resource = processing_resource
