"""
Dagster Integration for Streamlined Architecture

This module provides Dagster resources that bridge the streamlined architecture
with Dagster assets, replacing the old ProcessingResource with the new
ResourceCoordinator approach.
"""

import asyncio
import logging
from typing import Any

from dagster import ConfigurableResource, InitResourceContext, resource

from .config import StreamlinedConfig
from .coordinator import ResourceCoordinator

logger = logging.getLogger(__name__)


class StreamlinedProcessingResource(ConfigurableResource):
    """
    Dagster resource that provides the streamlined ResourceCoordinator.

    This replaces the old ProcessingResource and provides:
    - New ResourceCoordinator with focused managers
    - Integrated run tracking and checkpointing
    - Backward compatibility with existing assets
    """

    # Configuration can be overridden via Dagster config
    config_file: str | None = None
    use_postgres_runs: bool = True
    use_optimized_storage: bool = True

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._coordinator: ResourceCoordinator | None = None
        self._initialization_lock = asyncio.Lock()

    async def get_coordinator(self) -> ResourceCoordinator:
        """Get or create the resource coordinator."""
        if self._coordinator is None:
            async with self._initialization_lock:
                if self._coordinator is None:  # Double-check pattern
                    # Create config from environment or file
                    if self.config_file:
                        config = StreamlinedConfig.from_env(self.config_file)
                    else:
                        config = StreamlinedConfig.from_env()

                    # Override with resource-specific settings
                    config.infrastructure.use_postgres_runs = self.use_postgres_runs
                    config.infrastructure.use_optimized_storage = (
                        self.use_optimized_storage
                    )

                    # Create coordinator
                    self._coordinator = ResourceCoordinator(config)
                    await self._coordinator.initialize()

                    logger.info(
                        "StreamlinedProcessingResource: ResourceCoordinator initialized"
                    )

        return self._coordinator

    async def teardown(self):
        """Clean up resources."""
        if self._coordinator:
            try:
                await self._coordinator.cleanup()
                logger.info(
                    "StreamlinedProcessingResource: ResourceCoordinator cleaned up"
                )
            except Exception as e:
                logger.warning(f"Error during coordinator cleanup: {e}")
            finally:
                self._coordinator = None

    # Backward compatibility methods for existing assets
    async def get_db_pool(self):
        """Get database pool - backward compatibility."""
        coordinator = await self.get_coordinator()
        return await coordinator.db_manager.get_pool()

    def get_db_pool_sync(self):
        """Get database pool synchronously - backward compatibility."""
        # This is problematic as it's sync calling async
        # We should encourage migration away from this
        logger.warning("get_db_pool_sync is deprecated, use async get_db_pool instead")
        coordinator = asyncio.run(self.get_coordinator())
        return asyncio.run(coordinator.db_manager.get_pool())

    def get_checkpoint_manager(self):
        """Get checkpoint manager - backward compatibility."""
        if self._coordinator is None:
            logger.warning("Coordinator not initialized, returning None")
            return None
        return self._coordinator.checkpoint_manager.get_manager()

    def get_run_manager_sync(self):
        """Get run manager synchronously - backward compatibility."""
        if self._coordinator is None:
            logger.warning("Coordinator not initialized, returning None")
            return None
        return asyncio.run(self._coordinator.run_manager.get_manager())

    async def get_run_manager(self):
        """Get run manager - preferred async method."""
        coordinator = await self.get_coordinator()
        return await coordinator.run_manager.get_manager()

    async def get_storage_manager(self):
        """Get storage manager."""
        coordinator = await self.get_coordinator()
        return await coordinator.storage_manager.get_manager()

    async def get_api_key_manager(self):
        """Get API key manager."""
        coordinator = await self.get_coordinator()
        return coordinator.api_key_manager

    async def get_client_manager(self):
        """Get client manager."""
        coordinator = await self.get_coordinator()
        return coordinator.client_manager


@resource(
    description="Streamlined processing resource with integrated run tracking and checkpointing",
    config_schema={
        "config_file": str,
        "use_postgres_runs": bool,
        "use_optimized_storage": bool,
    },
)
def streamlined_processing_resource(
    context: InitResourceContext,
) -> StreamlinedProcessingResource:
    """Create streamlined processing resource from context."""
    return StreamlinedProcessingResource(
        config_file=context.resource_config.get("config_file"),
        use_postgres_runs=context.resource_config.get("use_postgres_runs", True),
        use_optimized_storage=context.resource_config.get(
            "use_optimized_storage", True
        ),
    )


def create_streamlined_dagster_resource() -> StreamlinedProcessingResource:
    """Create a streamlined processing resource with default configuration."""
    return StreamlinedProcessingResource()


# Convenience function for assets that need the new coordinator directly
async def get_streamlined_coordinator_from_context(context) -> ResourceCoordinator:
    """
    Extract ResourceCoordinator from Dagster context.

    Usage in assets:
        coordinator = await get_streamlined_coordinator_from_context(context)
        fetcher = StreamlinedFetcher(coordinator)
    """
    processing_resource = context.resources.processing_resource
    if hasattr(processing_resource, "get_coordinator"):
        return await processing_resource.get_coordinator()
    else:
        # Fallback for assets still using old ProcessingResource
        logger.warning(
            "Asset is using old ProcessingResource, consider migrating to StreamlinedProcessingResource"
        )
        # Create coordinator from old resource
        config = StreamlinedConfig.from_env()
        coordinator = ResourceCoordinator(config)
        await coordinator.initialize()
        return coordinator
