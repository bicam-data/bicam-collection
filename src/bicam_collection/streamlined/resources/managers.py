"""
Specialized Resource Managers for Streamlined Pipeline

This module provides focused resource managers that replace the God Object pattern
with single-responsibility components that can be composed together.

Each manager has a clear, focused responsibility:
- DatabaseManager: Database connection pooling
- APIKeyManager: API key distribution and management
- CheckpointManager: Checkpoint operations
- RunTrackingManager: Run tracking operations
- StorageManager: Storage operations
- ClientManager: API client lifecycle
"""

import asyncio
import contextlib
import logging
from pathlib import Path
from typing import Any

import asyncpg

from ...api_clients import CongressionalAPIClient
from ...libs.api_key_manager import APIKeySession, SystemAPIKeyManager
from ...libs.hierarchical_checkpoint_system import HierarchicalCheckpointManager
from ...libs.run_tracking import RunManager
from ...processing.optimized_storage_manager import OptimizedStorageManager

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Focused manager for database connection pooling."""

    def __init__(self, host: str, port: int, database: str, user: str, password: str):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self._pool: asyncpg.Pool | None = None
        self._lock = asyncio.Lock()

    async def get_pool(self) -> asyncpg.Pool:
        """Get or create database connection pool."""
        if self._pool is None:
            async with self._lock:
                if self._pool is None:  # Double-check pattern
                    self._pool = await asyncpg.create_pool(
                        host=self.host,
                        port=self.port,
                        database=self.database,
                        user=self.user,
                        password=self.password,
                        min_size=2,
                        max_size=20,
                        command_timeout=60,
                    )
                    logger.info(
                        f"Created database pool: {self.host}:{self.port}/{self.database}"
                    )
        return self._pool

    async def close(self):
        """Close database pool."""
        if self._pool:
            try:
                await self._pool.close()
                logger.debug("Database pool closed gracefully")
            except Exception as e:
                logger.warning(f"Error closing database pool: {e}")
                with contextlib.suppress(Exception):
                    self._pool.terminate()
            finally:
                self._pool = None


class APIKeyManager:
    """Focused manager for API key distribution and management."""

    def __init__(
        self,
        api_keys: list[str],
        parallelization_config: dict[str, Any] = None,
        use_dynamic_pool: bool = False,
    ):
        self.api_keys = api_keys
        self.parallelization_config = parallelization_config or {}
        self.use_dynamic_pool = use_dynamic_pool
        self._system_manager: SystemAPIKeyManager | None = None
        self._parallel_sessions: dict[str, list] = {}

    def get_system_manager(self) -> SystemAPIKeyManager:
        """Get or create system API key manager."""
        if self._system_manager is None:
            if not self.api_keys:
                raise ValueError("No API keys configured")

            enable_parallelization = bool(self.parallelization_config)
            keys_per_session = (
                self.parallelization_config.get("keys_per_session", 2)
                if enable_parallelization
                else min(2, len(self.api_keys))
            )

            self._system_manager = SystemAPIKeyManager(
                api_keys=self.api_keys,
                default_keys_per_client=keys_per_session,
                enable_parallelization=enable_parallelization,
                use_dynamic_pool=self.use_dynamic_pool,
            )

            logger.info(f"Initialized API key manager with {len(self.api_keys)} keys")

        return self._system_manager

    async def get_parallel_sessions(self, data_type: str, processing_type: str) -> list:
        """Get or create parallel sessions for a data type."""
        if data_type in self._parallel_sessions:
            return self._parallel_sessions[data_type]

        # Handle CPU-based parallelization for normalizers and cleaners
        if processing_type in ["normalizer", "cleaner"]:
            if not self.parallelization_config:
                sessions = [
                    {"session_id": f"{data_type}_session_0", "type": "cpu_based"}
                ]
            else:
                config = self.parallelization_config.get(processing_type, {})
                num_sessions = config.get("num_sessions", 1)
                sessions = [
                    {"session_id": f"{data_type}_session_{i}", "type": "cpu_based"}
                    for i in range(num_sessions)
                ]

            self._parallel_sessions[data_type] = sessions
            logger.info(
                f"Created {len(sessions)} CPU-based parallel sessions for {data_type}"
            )
            return sessions

        # Handle API key-based parallelization for fetchers
        system_manager = self.get_system_manager()

        if not self.parallelization_config or not system_manager.enable_parallelization:
            # Single session fallback
            traditional_keys = system_manager.assign_keys_for_data_type(data_type)
            if traditional_keys:
                session = APIKeySession(f"{data_type}_session_0", traditional_keys)
                self._parallel_sessions[data_type] = [session]
                return [session]
            return []

        # Create parallel sessions
        from ...libs.data_type_router import get_global_registry

        registry = get_global_registry()

        data_source_config = {}
        try:
            data_source = registry.get_data_source(data_type)
            fetcher_config = self.parallelization_config.get("fetcher", {})
            data_source_config = fetcher_config.get(data_source, {})
        except Exception as e:
            logger.warning(f"Error getting data source for {data_type}: {e}")

        num_sessions = data_source_config.get("num_sessions", 1)
        keys_per_session = data_source_config.get("keys_per_session", 2)

        sessions = system_manager.assign_parallel_sessions_for_data_type(
            data_type, num_sessions, keys_per_session
        )

        self._parallel_sessions[data_type] = sessions
        logger.info(
            f"Created {len(sessions)} API key-based parallel sessions for {data_type}"
        )
        return sessions

    async def release_keys_for_data_type(self, data_type: str):
        """Release keys for a specific data type."""
        if data_type in self._parallel_sessions:
            del self._parallel_sessions[data_type]

        if self._system_manager:
            if hasattr(self._system_manager, "release_parallel_sessions_for_data_type"):
                released = self._system_manager.release_parallel_sessions_for_data_type(
                    data_type
                )
                logger.info(
                    f"Released {len(released)} parallel sessions from {data_type}"
                )
            else:
                released = self._system_manager.release_keys_for_data_type(data_type)
                logger.info(f"Released {len(released)} keys from {data_type}")

    def get_status(self) -> dict[str, Any]:
        """Get API key manager status."""
        if self._system_manager:
            status = self._system_manager.get_status_summary()
            status["parallelization_config"] = self.parallelization_config
            return status
        return {"status": "no_manager_initialized"}


class CheckpointManager:
    """Focused manager for checkpoint operations."""

    def __init__(self, checkpoint_db_path: str):
        self.checkpoint_db_path = checkpoint_db_path
        self._manager: HierarchicalCheckpointManager | None = None

    def get_manager(self) -> HierarchicalCheckpointManager:
        """Get or create checkpoint manager."""
        if self._manager is None:
            Path(self.checkpoint_db_path).parent.mkdir(parents=True, exist_ok=True)
            self._manager = HierarchicalCheckpointManager(
                db_path=self.checkpoint_db_path
            )
            logger.info(f"Initialized checkpoint manager: {self.checkpoint_db_path}")
        return self._manager


class RunTrackingManager:
    """Focused manager for run tracking operations."""

    def __init__(self, use_postgres: bool = True, db_manager: DatabaseManager = None):
        self.use_postgres = use_postgres
        self.db_manager = db_manager
        self._manager: RunManager | None = None

    async def get_manager(self) -> RunManager:
        """Get or create run manager."""
        if self._manager is None:
            db_pool = None
            if self.use_postgres and self.db_manager:
                db_pool = await self.db_manager.get_pool()

            self._manager = RunManager(
                use_postgres=self.use_postgres,
                external_pool=db_pool,
            )
            await self._manager.initialize()
            logger.info(f"Initialized run manager (postgres: {self.use_postgres})")

        return self._manager

    async def cleanup(self):
        """Clean up run manager."""
        if self._manager:
            try:
                await self._manager.cleanup()
                logger.debug("Run manager cleaned up successfully")
            except Exception as e:
                logger.warning(f"Error cleaning up run manager: {e}")


class StorageManager:
    """Focused manager for storage operations."""

    def __init__(
        self,
        use_optimized_storage: bool = False,
        db_manager: DatabaseManager = None,
        checkpoint_db_path: str = None,
        batch_size: int = 1000,
    ):
        self.use_optimized_storage = use_optimized_storage
        self.db_manager = db_manager
        self.checkpoint_db_path = checkpoint_db_path
        self.batch_size = batch_size
        self._manager: OptimizedStorageManager | None = None

    async def get_manager(self) -> OptimizedStorageManager | None:
        """Get or create storage manager."""
        if not self.use_optimized_storage:
            return None

        if self._manager is None and self.db_manager:
            db_pool = await self.db_manager.get_pool()
            self._manager = OptimizedStorageManager(
                pg_pool=db_pool,
                checkpoint_db_path=self.checkpoint_db_path,
                batch_size=self.batch_size,
            )
            await self._manager.start()
            logger.info("OptimizedStorageManager started")

        return self._manager

    async def cleanup(self):
        """Clean up storage manager."""
        if self._manager:
            try:
                await self._manager.flush_all()
                await self._manager.stop()
                logger.debug("Storage manager stopped successfully")
            except Exception as e:
                logger.warning(f"Error stopping storage manager: {e}")
            finally:
                self._manager = None


class ClientManager:
    """Focused manager for API client lifecycle."""

    def __init__(self, api_rate_limit: float = 1.5, db_manager: DatabaseManager = None):
        self.api_rate_limit = api_rate_limit
        self.db_manager = db_manager
        self._api_clients: dict[str, CongressionalAPIClient] = {}

    async def get_clients_for_parallel_sessions(
        self, data_type: str, sessions: list
    ) -> list[CongressionalAPIClient]:
        """Create API clients for parallel sessions."""
        clients = []
        db_pool = await self.db_manager.get_pool() if self.db_manager else None

        for i, session in enumerate(sessions):
            client_key = f"{data_type}_session_{i}"

            if client_key in self._api_clients:
                clients.append(self._api_clients[client_key])
                continue

            client = CongressionalAPIClient(
                api_keys=session.api_keys,
                rate_limit_per_second=self.api_rate_limit,
                db_pool=db_pool,
            )

            await client.__aenter__()
            self._api_clients[client_key] = client
            clients.append(client)

            masked_keys = [k[:8] + "..." for k in session.api_keys]
            logger.info(
                f"Created API client for {data_type} session {i}: {masked_keys}"
            )

        return clients

    async def release_clients_for_data_type(self, data_type: str):
        """Release clients for a specific data type."""
        clients_to_remove = []
        for client_key, client in list(self._api_clients.items()):
            if client_key.startswith(f"{data_type}_"):
                clients_to_remove.append(client_key)
                try:
                    await client.__aexit__(None, None, None)
                    logger.info(f"Closed HTTP session for {client_key}")
                except Exception as e:
                    logger.warning(f"Error closing HTTP session for {client_key}: {e}")

        for client_key in clients_to_remove:
            del self._api_clients[client_key]

    async def cleanup(self):
        """Clean up all API clients."""
        clients = list(self._api_clients.values())
        for client in clients:
            try:
                if hasattr(client, "close"):
                    await client.close()
            except Exception as e:
                logger.warning(f"Error closing API client: {e}")

        self._api_clients.clear()
        logger.debug("API clients closed successfully")
