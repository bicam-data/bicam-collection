"""
Improved Shared Resources using Composition Pattern

This module provides a simplified ProcessingResource that uses the new
specialized managers internally while maintaining backwards compatibility.
"""

import logging
import os
from pathlib import Path
from typing import Any

import asyncpg
from dagster import ConfigurableResource

from ..api_clients import CongressionalAPIClient
from ..libs.hierarchical_checkpoint_system import HierarchicalCheckpointManager
from ..libs.run_tracking import RunManager
from ..processing.optimized_storage_manager import OptimizedStorageManager
from .resource_managers import (
    APIKeyManager,
    CheckpointManager,
    ClientManager,
    DatabaseManager,
    ResourceCoordinator,
    RunTrackingManager,
    StorageManager,
)

logger = logging.getLogger(__name__)


class ProcessingResource(ConfigurableResource):
    """
    Simplified ProcessingResource using composition with specialized managers.

    This version maintains backwards compatibility while internally using
    the new specialized resource managers to eliminate the God Object pattern.
    """

    # Database configuration
    db_host: str = os.getenv("POSTGRESQL_HOST", "localhost")
    db_port: int = int(os.getenv("POSTGRESQL_PORT", "5432"))
    db_name: str = os.getenv("POSTGRESQL_DATABASE", "bicam_collection")
    db_user: str = os.getenv("POSTGRESQL_USERNAME", "postgres")
    db_password: str = os.getenv("POSTGRESQL_PASSWORD", "password")

    # API configuration
    api_keys: list[str] = []
    api_rate_limit: float = 1.5

    # Parallelization configuration
    parallelization_config: dict[str, Any] = {}

    # Processing configuration
    batch_size: int = int(os.getenv("BATCH_SIZE", "100"))
    max_concurrent: int = int(os.getenv("MAX_CONCURRENT", "5"))

    # Infrastructure paths
    checkpoint_db_path: str = str(
        Path(__file__).parent.parent.parent.parent
        / "data"
        / "checkpoints"
        / "checkpoints.db"
    )
    use_postgres_runs: bool = True
    use_postgres_checkpoints: bool = True

    # Processing parameters
    from_date: str | None = None
    to_date: str | None = None
    congress: int | None = None

    # Incremental processing configuration
    incremental: bool = os.getenv("INCREMENTAL", "true").lower() == "true"
    fallback_days: int = int(os.getenv("FALLBACK_DAYS", "0"))

    # Checkpoint system coordination flags
    use_checkpoint_resume: bool = False
    use_incremental_dates: bool = True
    rerun_mode: bool = False
    use_dynamic_pool: bool = False

    # Storage backend configuration
    use_optimized_storage: bool = False

    # Internal managers (composition)
    _coordinator: ResourceCoordinator | None = None
    _processing_type: str = "fetcher"

    def set_processing_type(self, processing_type: str) -> None:
        """Set the processing type for this resource instance."""
        object.__setattr__(self, "_processing_type", processing_type)

    async def initialize(self) -> None:
        """Initialize all async components using the new managers."""
        logger.info("Initializing ProcessingResource with specialized managers...")

        # Create specialized managers
        db_manager = DatabaseManager(
            host=self.db_host,
            port=self.db_port,
            database=self.db_name,
            user=self.db_user,
            password=self.db_password,
        )

        api_key_manager = APIKeyManager(
            api_keys=self.api_keys,
            parallelization_config=self.parallelization_config,
            use_dynamic_pool=self.use_dynamic_pool,
        )

        checkpoint_manager = CheckpointManager(
            checkpoint_db_path=self.checkpoint_db_path
        )

        run_manager = RunTrackingManager(
            use_postgres=self.use_postgres_runs,
            db_manager=db_manager,
        )

        storage_manager = StorageManager(
            use_optimized_storage=self.use_optimized_storage,
            db_manager=db_manager,
            checkpoint_db_path=self.checkpoint_db_path,
            batch_size=self.batch_size * 100,
        )

        client_manager = ClientManager(
            api_rate_limit=self.api_rate_limit,
            db_manager=db_manager,
        )

        # Create coordinator
        self._coordinator = ResourceCoordinator(
            db_manager=db_manager,
            api_key_manager=api_key_manager,
            checkpoint_manager=checkpoint_manager,
            run_manager=run_manager,
            storage_manager=storage_manager,
            client_manager=client_manager,
        )

        await self._coordinator.initialize()
        logger.info("ProcessingResource initialization completed successfully")

    # Backwards compatibility methods
    async def get_db_pool(self) -> asyncpg.Pool:
        """Get database connection pool."""
        if not self._coordinator:
            raise RuntimeError("ProcessingResource not initialized")
        return await self._coordinator.db_manager.get_pool()

    def get_system_key_manager(self):
        """Get system API key manager."""
        if not self._coordinator:
            raise RuntimeError("ProcessingResource not initialized")
        return self._coordinator.api_key_manager.get_system_manager()

    def get_checkpoint_manager(self) -> HierarchicalCheckpointManager:
        """Get checkpoint manager."""
        if not self._coordinator:
            raise RuntimeError("ProcessingResource not initialized")
        return self._coordinator.checkpoint_manager.get_manager()

    async def get_run_manager(self, db_pool: asyncpg.Pool | None = None) -> RunManager:
        """Get run manager."""
        if not self._coordinator:
            raise RuntimeError("ProcessingResource not initialized")
        return await self._coordinator.run_manager.get_manager()

    async def get_parallel_sessions_for_data_type(self, data_type: str) -> list:
        """Get parallel sessions for a data type."""
        if not self._coordinator:
            raise RuntimeError("ProcessingResource not initialized")
        return await self._coordinator.api_key_manager.get_parallel_sessions(
            data_type, self._processing_type
        )

    async def get_api_clients_for_parallel_sessions(
        self, data_type: str
    ) -> list[CongressionalAPIClient]:
        """Get API clients for parallel sessions."""
        if not self._coordinator:
            raise RuntimeError("ProcessingResource not initialized")
        sessions = await self.get_parallel_sessions_for_data_type(data_type)
        return await self._coordinator.client_manager.get_clients_for_parallel_sessions(
            data_type, sessions
        )

    async def release_keys_for_data_type(self, data_type: str):
        """Release keys for a specific data type."""
        if not self._coordinator:
            return
        await self._coordinator.api_key_manager.release_keys_for_data_type(data_type)
        await self._coordinator.client_manager.release_clients_for_data_type(data_type)

    def get_key_manager_status(self) -> dict[str, Any]:
        """Get API key manager status."""
        if not self._coordinator:
            return {"status": "not_initialized"}
        return self._coordinator.api_key_manager.get_status()

    def cleanup_expired_rate_limits(self):
        """Clean up expired rate limits."""
        if self._coordinator:
            system_manager = self._coordinator.api_key_manager.get_system_manager()
            if system_manager:
                system_manager.cleanup_expired_rate_limits()

    async def get_storage_manager(self) -> OptimizedStorageManager | None:
        """Get storage manager."""
        if not self._coordinator:
            raise RuntimeError("ProcessingResource not initialized")
        return await self._coordinator.storage_manager.get_manager()

    async def cleanup(self):
        """Clean up all resources."""
        if self._coordinator:
            await self._coordinator.cleanup()

    def get_configuration_summary(self) -> dict[str, Any]:
        """Get configuration summary."""
        return {
            "database": {
                "host": self.db_host,
                "port": self.db_port,
                "database": self.db_name,
                "user": self.db_user,
            },
            "api": {
                "key_count": len(self.api_keys),
                "rate_limit": self.api_rate_limit,
            },
            "processing": {
                "batch_size": self.batch_size,
                "max_concurrent": self.max_concurrent,
                "from_date": self.from_date,
                "to_date": self.to_date,
                "congress": self.congress,
            },
            "infrastructure": {
                "checkpoint_path": self.checkpoint_db_path,
                "use_postgres_runs": self.use_postgres_runs,
            },
            "optimized_storage": {
                "enabled": self.use_optimized_storage,
            },
        }


class DataTypeSpecificResource(ConfigurableResource):
    """
    Resource for data type specific configuration.

    This remains unchanged for backwards compatibility.
    """

    data_type: str
    batch_size_override: int | None = None
    api_rate_limit_override: float | None = None
    num_api_keys: int = 2

    def get_effective_batch_size(self, base_resource: ProcessingResource) -> int:
        """Get effective batch size (override or base)."""
        return self.batch_size_override or base_resource.batch_size

    def get_effective_rate_limit(self, base_resource: ProcessingResource) -> float:
        """Get effective rate limit (override or base)."""
        return self.api_rate_limit_override or base_resource.api_rate_limit

    async def get_storage_manager(
        self, base_resource: ProcessingResource
    ) -> OptimizedStorageManager | None:
        """Get storage manager from base resource."""
        return await base_resource.get_storage_manager()
