"""
Resource Coordinator for Streamlined Pipeline

This module provides lightweight coordination of all resource managers,
replacing the God Object pattern with composition.
"""

import asyncio
import logging
from typing import Any

from .config import StreamlinedConfig
from .managers import (
    APIKeyManager,
    CheckpointManager,
    ClientManager,
    DatabaseManager,
    RunTrackingManager,
    StorageManager,
)

logger = logging.getLogger(__name__)


class ResourceCoordinator:
    """Lightweight coordinator that composes all resource managers."""

    def __init__(
        self,
        config: StreamlinedConfig | None = None,
        db_config: dict[str, Any] | None = None,
        api_keys: list[str] | None = None,
        parallelization_config: dict[str, Any] | None = None,
        use_optimized_storage: bool = True,
        use_dynamic_pool: bool = False,
    ):
        """
        Initialize resource coordinator.

        Can be initialized with either a StreamlinedConfig or individual parameters.
        """
        # Use config if provided, otherwise create from parameters
        if config:
            self.config = config
        else:
            self.config = StreamlinedConfig()
            if db_config:
                self.config.database.host = db_config.get(
                    "host", self.config.database.host
                )
                self.config.database.port = db_config.get(
                    "port", self.config.database.port
                )
                self.config.database.database = db_config.get(
                    "database", self.config.database.database
                )
                self.config.database.username = db_config.get(
                    "username", self.config.database.username
                )
                self.config.database.password = db_config.get(
                    "password", self.config.database.password
                )

            if api_keys:
                self.config.api.keys = api_keys

            if parallelization_config:
                self.config.parallelization.enabled = True
                self.config.parallelization.fetcher = parallelization_config.get(
                    "fetcher", {}
                )
                self.config.parallelization.normalizer = parallelization_config.get(
                    "normalizer", {}
                )
                self.config.parallelization.cleaner = parallelization_config.get(
                    "cleaner", {}
                )

            self.config.infrastructure.use_optimized_storage = use_optimized_storage
            self.config.infrastructure.use_dynamic_pool = use_dynamic_pool

        # Create managers
        self.db_manager = DatabaseManager(
            host=self.config.database.host,
            port=self.config.database.port,
            database=self.config.database.database,
            user=self.config.database.username,
            password=self.config.database.password,
        )

        self.api_key_manager = APIKeyManager(
            api_keys=self.config.api.keys,
            parallelization_config=self.config.get_parallelization_config(),
            use_dynamic_pool=self.config.infrastructure.use_dynamic_pool,
        )

        self.checkpoint_manager = CheckpointManager(
            checkpoint_db_path=self.config.infrastructure.checkpoint_db_path
        )

        self.run_manager = RunTrackingManager(
            use_postgres=self.config.infrastructure.use_postgres_runs,
            db_manager=self.db_manager,
        )

        self.storage_manager = StorageManager(
            use_optimized_storage=self.config.infrastructure.use_optimized_storage,
            db_manager=self.db_manager,
            checkpoint_db_path=self.config.infrastructure.checkpoint_db_path,
            batch_size=self.config.processing.batch_size
            * 100,  # Larger batches for storage
        )

        self.client_manager = ClientManager(
            api_rate_limit=self.config.api.rate_limit_per_second,
            db_manager=self.db_manager,
        )

        # Cleanup state
        self._cleanup_lock: asyncio.Lock | None = None
        self._cleanup_in_progress = False
        self._cleanup_completed = False

    @classmethod
    def from_config(cls, config: StreamlinedConfig) -> "ResourceCoordinator":
        """Create coordinator from streamlined config."""
        return cls(config=config)

    @classmethod
    def from_env(cls, env_file: str | None = None) -> "ResourceCoordinator":
        """Create coordinator from environment configuration."""
        config = StreamlinedConfig.from_env(env_file)
        return cls(config=config)

    async def initialize(self):
        """Initialize all managers."""
        self._cleanup_lock = asyncio.Lock()

        # Validate configuration
        errors = self.config.validate()
        if errors:
            raise ValueError(f"Configuration errors: {', '.join(errors)}")

        logger.info("ResourceCoordinator initialized successfully")

    async def get_database_manager(self) -> DatabaseManager:
        """Get database manager."""
        return self.db_manager

    async def get_api_key_manager(self) -> APIKeyManager:
        """Get API key manager."""
        return self.api_key_manager

    async def get_checkpoint_manager(self) -> CheckpointManager:
        """Get checkpoint manager."""
        return self.checkpoint_manager

    async def get_run_manager(self) -> RunTrackingManager:
        """Get run tracking manager."""
        return self.run_manager

    async def get_storage_manager(self) -> StorageManager:
        """Get storage manager."""
        return self.storage_manager

    async def get_client_manager(self) -> ClientManager:
        """Get client manager."""
        return self.client_manager

    # Convenience methods for common operations
    async def get_db_pool(self):
        """Get database connection pool."""
        return await self.db_manager.get_pool()

    async def get_parallel_sessions(
        self, data_type: str, processing_type: str = "fetcher"
    ):
        """Get parallel sessions for a data type."""
        return await self.api_key_manager.get_parallel_sessions(
            data_type, processing_type
        )

    async def get_api_clients(self, data_type: str):
        """Get API clients for a data type."""
        sessions = await self.get_parallel_sessions(data_type, "fetcher")
        return await self.client_manager.get_clients_for_parallel_sessions(
            data_type, sessions
        )

    def get_checkpoint_manager_instance(self):
        """Get checkpoint manager instance."""
        return self.checkpoint_manager.get_manager()

    async def get_run_manager_instance(self):
        """Get run manager instance."""
        return await self.run_manager.get_manager()

    async def get_storage_manager_instance(self):
        """Get storage manager instance."""
        return await self.storage_manager.get_manager()

    async def release_resources_for_data_type(self, data_type: str):
        """Release all resources for a specific data type."""
        await self.api_key_manager.release_keys_for_data_type(data_type)
        await self.client_manager.release_clients_for_data_type(data_type)

    def get_configuration_summary(self) -> dict[str, Any]:
        """Get configuration summary."""
        return self.config.to_dict()

    def get_status(self) -> dict[str, Any]:
        """Get overall status of all managers."""
        return {
            "config": self.config.to_dict(),
            "api_key_manager": self.api_key_manager.get_status(),
            "storage_manager": {
                "enabled": self.config.infrastructure.use_optimized_storage,
            },
        }

    async def cleanup(self):
        """Clean up all managers with proper race condition handling."""
        if self._cleanup_lock is None:
            self._cleanup_lock = asyncio.Lock()

        try:
            async with asyncio.timeout(120):
                async with self._cleanup_lock:
                    if self._cleanup_in_progress or self._cleanup_completed:
                        logger.debug("Cleanup already in progress or completed")
                        return

                    self._cleanup_in_progress = True
                    logger.info("Starting resource coordinator cleanup")

                    # Cleanup in proper order
                    cleanup_tasks = [
                        ("storage", self.storage_manager.cleanup),
                        ("clients", self.client_manager.cleanup),
                        ("run_manager", self.run_manager.cleanup),
                        ("database", self.db_manager.close),
                    ]

                    for task_name, cleanup_func in cleanup_tasks:
                        try:
                            await asyncio.wait_for(cleanup_func(), timeout=30)
                            logger.debug(f"Completed {task_name} cleanup")
                        except TimeoutError:
                            logger.warning(f"Timeout during {task_name} cleanup")
                        except Exception as e:
                            logger.warning(f"Error during {task_name} cleanup: {e}")

                    self._cleanup_completed = True
                    logger.info("Resource coordinator cleanup completed")

        except TimeoutError:
            logger.error("Overall cleanup timeout")
            self._cleanup_completed = True
        except Exception as e:
            logger.error(f"Unexpected error during cleanup: {e}")
            self._cleanup_completed = True
        finally:
            self._cleanup_in_progress = False
