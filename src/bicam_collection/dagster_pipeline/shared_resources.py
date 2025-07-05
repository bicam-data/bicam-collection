"""
Shared Resources for Bicam Data Processing

This module provides unified resource management across all data types and sources,
including database connections, API key management, and infrastructure services.

Key Features:
- System-wide API key coordination
- Shared database connection pooling
- Unified checkpoint and run management
- Configuration-driven setup
- Resource cleanup and lifecycle management
- Support for multiple data sources (congressional, govinfo, etc.)
"""

import asyncio
import logging
import os
from pathlib import Path
from typing import Any

import asyncpg
from dagster import ConfigurableResource

from ..api_clients import CongressionalAPIClient
from ..libs.api_key_manager import SystemAPIKeyManager
from ..libs.checkpoint import CheckpointManager
from ..libs.run_tracking import RunManager

logger = logging.getLogger(__name__)


class ProcessingResource(ConfigurableResource):
    """
    Unified resource for bicam data processing across all data sources.

    This resource provides system-wide coordination of:
    - API key distribution across data types
    - Database connection management
    - Checkpoint and run tracking
    - Infrastructure lifecycle management
    - HTTP session management for API clients
    - Parallel session management with API key pairs

    Benefits:
    - Single source of truth for configuration
    - Automatic API key coordination prevents conflicts
    - Shared connection pooling improves efficiency
    - Consistent error handling and logging
    - Supports multiple data sources (congressional, govinfo, etc.)
    - Proper resource cleanup prevents hanging connections
    - Parallelization with backup API key support
    """

    # Database configuration
    db_host: str = os.getenv("POSTGRESQL_HOST", "localhost")
    db_port: int = int(os.getenv("POSTGRESQL_PORT", "5432"))
    db_name: str = os.getenv("POSTGRESQL_DATABASE", "bicam_collection")
    db_user: str = os.getenv("POSTGRESQL_USERNAME", "postgres")
    db_password: str = os.getenv("POSTGRESQL_PASSWORD", "password")

    # API configuration (supports various API types)
    api_keys: list[
        str
    ] = []  # Will be populated at runtime, not at class definition time
    api_rate_limit: float = 1.5

    # Parallelization configuration
    parallelization_config: dict[str, Any] = {}

    # Processing configuration
    batch_size: int = int(os.getenv("BATCH_SIZE", "100"))
    max_concurrent: int = int(os.getenv("MAX_CONCURRENT", "5"))

    # Infrastructure paths (can be data source specific)
    checkpoint_db_path: str = str(
        Path(__file__).parent.parent.parent.parent
        / "data"
        / "checkpoints"
        / "checkpoints.db"
    )
    use_postgres_runs: bool = True
    use_postgres_checkpoints: bool = True

    # Processing parameters (can be overridden per run)
    from_date: str | None = None
    to_date: str | None = None
    congress: int | None = None

    # Incremental processing configuration
    incremental: bool = os.getenv("INCREMENTAL", "true").lower() == "true"
    fallback_days: int = int(os.getenv("FALLBACK_DAYS", "0"))

    # Checkpoint system coordination flags (set by CLI)
    use_checkpoint_resume: bool = False  # True when --resume flag is used
    use_incremental_dates: bool = True  # False when --resume flag is used
    rerun_mode: bool = (
        False  # True when items should be reprocessed regardless of checkpoint status
    )

    # Internal state (not configurable)
    _db_pool: asyncpg.Pool | None = None
    _system_key_manager: SystemAPIKeyManager | None = None
    _checkpoint_manager: CheckpointManager | None = None
    _run_manager: RunManager | None = None
    _api_clients: dict[str, CongressionalAPIClient] = {}  # Track created clients
    _parallel_sessions: dict[str, list] = {}  # Track parallel sessions per data type
    _processing_type: str = (
        "fetcher"  # Track the type of processing: "fetcher", "normalizer", "cleaner"
    )

    def set_processing_type(self, processing_type: str) -> None:
        """Set the processing type for this resource instance."""
        # Use object.__setattr__ to bypass the frozen model restriction
        object.__setattr__(self, "_processing_type", processing_type)

    async def initialize(self) -> None:
        """Initialize all async components during setup phase."""
        logger.info("Initializing ProcessingResource async components...")

        # Initialize database pool
        await self.get_db_pool()

        # Initialize API key manager
        self.get_system_key_manager()

        # Initialize checkpoint manager
        self.get_checkpoint_manager()

        # Initialize run manager
        await self.get_run_manager()

        logger.info("ProcessingResource initialization completed successfully")

    async def get_db_pool(self) -> asyncpg.Pool:
        """Get or create database connection pool."""
        if self._db_pool is None:
            self._db_pool = await asyncpg.create_pool(
                host=self.db_host,
                port=self.db_port,
                database=self.db_name,
                user=self.db_user,
                password=self.db_password,
                min_size=2,
                max_size=20,  # Increased for multiple data types
                command_timeout=60,
            )
            logger.info(
                f"Created database pool: {self.db_host}:{self.db_port}/{self.db_name}"
            )

        return self._db_pool

    def get_system_key_manager(self) -> SystemAPIKeyManager:
        """Get or create system-wide API key manager with parallelization support."""
        if self._system_key_manager is None:
            if not self.api_keys:
                raise ValueError(
                    "No API keys configured. Set CONGRESSIONAL_API_KEYS or CONGRESSIONAL_API_KEY"
                )

            # Check if parallelization is enabled
            enable_parallelization = bool(self.parallelization_config)

            # Use parallelization config if available
            if self.parallelization_config:
                keys_per_session = self.parallelization_config.get(
                    "keys_per_session", 2
                )
            else:
                keys_per_session = min(2, len(self.api_keys))  # Conservative default

            self._system_key_manager = SystemAPIKeyManager(
                api_keys=self.api_keys,
                default_keys_per_client=keys_per_session,
                enable_parallelization=enable_parallelization,
            )

            if enable_parallelization:
                logger.info(
                    f"Initialized system API key manager with {len(self.api_keys)} keys for parallelization "
                    f"({self.parallelization_config.get('num_sessions', 0)} sessions, {keys_per_session} keys per session)"
                )
            else:
                logger.info(
                    f"Initialized system API key manager with {len(self.api_keys)} keys (traditional mode)"
                )

        return self._system_key_manager

    def get_checkpoint_manager(self) -> CheckpointManager:
        """Get or create checkpoint manager (SQLite or Postgres)."""
        if self._checkpoint_manager is None:
            if self.use_postgres_checkpoints:
                from ..libs.pg_checkpoint import PostgresCheckpointManager

                self._checkpoint_manager = PostgresCheckpointManager(
                    host=self.db_host,
                    port=self.db_port,
                    database=self.db_name,
                    user=self.db_user,
                    password=self.db_password,
                )
                logger.info("Initialized PostgresCheckpointManager (PostgreSQL)")
            else:
                Path(self.checkpoint_db_path).parent.mkdir(parents=True, exist_ok=True)
                self._checkpoint_manager = CheckpointManager(self.checkpoint_db_path)
                logger.info(
                    f"Initialized checkpoint manager (SQLite): {self.checkpoint_db_path}"
                )

        return self._checkpoint_manager

    async def get_run_manager(self, db_pool: asyncpg.Pool | None = None) -> RunManager:
        """Get or create run manager."""
        if self._run_manager is None:
            if db_pool is None:
                db_pool = await self.get_db_pool()

            self._run_manager = RunManager(
                use_postgres=self.use_postgres_runs,
                external_pool=db_pool if self.use_postgres_runs else None,
            )
            await self._run_manager.initialize()
            logger.info(f"Initialized run manager (postgres: {self.use_postgres_runs})")

        return self._run_manager

    async def get_parallel_sessions_for_data_type(self, data_type: str) -> list:
        """
        Get or create parallel sessions for a data type using the parallelization configuration.

        For fetchers: Returns APIKeySession objects for parallel processing with API keys
        For normalizers/cleaners: Returns dummy session objects for CPU-based parallel processing

        Returns:
            List of session objects for parallel processing
        """
        # Check if we already have sessions for this data type
        if data_type in self._parallel_sessions:
            return self._parallel_sessions[data_type]

        # Handle CPU-based parallelization for normalizers and cleaners
        if self._processing_type in ["normalizer", "cleaner"]:
            if not self.parallelization_config:
                # No parallelization config, create single session
                sessions = [
                    {"session_id": f"{data_type}_session_0", "type": "cpu_based"}
                ]
            else:
                config = self.parallelization_config.get(self._processing_type, {})
                num_sessions = config.get("num_sessions", 1)

                # Create dummy sessions for CPU-based processing (no API keys needed)
                sessions = [
                    {"session_id": f"{data_type}_session_{i}", "type": "cpu_based"}
                    for i in range(num_sessions)
                ]

            logger.info(
                f"Created {len(sessions)} CPU-based parallel sessions for {data_type} {self._processing_type}"
            )
            self._parallel_sessions[data_type] = sessions
            return sessions

        # Handle API key-based parallelization for fetchers (existing logic)
        key_manager = self.get_system_key_manager()

        if not self.parallelization_config or not key_manager.enable_parallelization:
            # Fallback to traditional single session
            traditional_keys = key_manager.assign_keys_for_data_type(data_type)
            if traditional_keys:
                from ..libs.api_key_manager import APIKeySession

                session = APIKeySession(f"{data_type}_session_0", traditional_keys)
                self._parallel_sessions[data_type] = [session]
                return [session]
            return []

        # Create parallel sessions based on configuration
        # The parallelization_config contains configs for each data source
        # We need to find the right data source for this data type
        data_source_config = None

        # Map data types to their data sources
        from ..libs.data_type_router import get_global_registry

        registry = get_global_registry()
        try:
            data_source = registry.get_data_source(data_type)
            # For fetchers, look in the fetcher config or legacy format
            fetcher_config = self.parallelization_config.get("fetcher", {})
            data_source_config = fetcher_config.get(data_source, {})

            # Also check legacy format for backward compatibility
            if not data_source_config and data_source in self.parallelization_config:
                data_source_config = self.parallelization_config[data_source]
        except Exception as e:
            logger.warning(f"Error getting data source for {data_type}: {e}")
            # Fallback: try to find any config that has the right structure
            for _, config in self.parallelization_config.items():
                if (
                    isinstance(config, dict)
                    and "num_sessions" in config
                    and "keys_per_session" in config
                ):
                    data_source_config = config
                    break

        if not data_source_config:
            data_source_config = {}

        num_sessions = data_source_config.get("num_sessions", 1)
        keys_per_session = data_source_config.get("keys_per_session", 2)

        sessions = key_manager.assign_parallel_sessions_for_data_type(
            data_type, num_sessions, keys_per_session
        )

        # Store sessions for later cleanup
        self._parallel_sessions[data_type] = sessions

        logger.info(
            f"Created {len(sessions)} API key-based parallel sessions for {data_type}"
        )
        return sessions

    async def get_api_clients_for_parallel_sessions(
        self, data_type: str
    ) -> list[CongressionalAPIClient]:
        """
        Create API clients for each parallel session of a data type.

        Returns:
            List of CongressionalAPIClient objects, one per session
        """
        sessions = await self.get_parallel_sessions_for_data_type(data_type)
        clients = []

        # Get database pool for the clients
        db_pool = await self.get_db_pool()

        for i, session in enumerate(sessions):
            client_key = f"{data_type}_session_{i}"

            # Check if we already have a client for this session
            if client_key in self._api_clients:
                clients.append(self._api_clients[client_key])
                continue

            # Create new client for this session
            client = CongressionalAPIClient(
                api_keys=session.api_keys,
                rate_limit_per_second=self.api_rate_limit,
                db_pool=db_pool,
            )

            # Initialize the client using its async context manager
            await client.__aenter__()

            # Store the client for cleanup later
            self._api_clients[client_key] = client
            clients.append(client)

            # Log session assignment for monitoring
            masked_keys = [k[:8] + "..." for k in session.api_keys]
            logger.info(
                f"Created API client for {data_type} session {i}: {masked_keys}"
            )

        return clients

    async def release_keys_for_data_type(self, data_type: str):
        """Release API keys and clean up clients when processing is complete."""
        # Close all client sessions for this data type
        clients_to_remove = []
        for client_key, client in list(self._api_clients.items()):
            if client_key.startswith(f"{data_type}_"):
                clients_to_remove.append(client_key)
                try:
                    await client.__aexit__(None, None, None)
                    logger.info(f"Closed HTTP session for {client_key}")
                except Exception as e:
                    logger.warning(f"Error closing HTTP session for {client_key}: {e}")

        # Remove clients from tracking
        for client_key in clients_to_remove:
            del self._api_clients[client_key]

        # Release parallel sessions if they exist
        if data_type in self._parallel_sessions:
            del self._parallel_sessions[data_type]

        # Release keys from the key manager
        if self._system_key_manager:
            if hasattr(
                self._system_key_manager, "release_parallel_sessions_for_data_type"
            ):
                # Use parallel session release if available
                released_sessions = (
                    self._system_key_manager.release_parallel_sessions_for_data_type(
                        data_type
                    )
                )
                logger.info(
                    f"Released {len(released_sessions)} parallel sessions from {data_type}"
                )
            else:
                # Fallback to traditional key release
                released = self._system_key_manager.release_keys_for_data_type(
                    data_type
                )
                logger.info(f"Released {len(released)} keys from {data_type}")

    def get_key_manager_status(self) -> dict[str, Any]:
        """Get current status of the API key manager including parallel sessions."""
        if self._system_key_manager:
            status = self._system_key_manager.get_status_summary()

            # Add parallel session information
            if hasattr(self._system_key_manager, "parallel_sessions"):
                status["parallel_sessions"] = {
                    "total_sessions": len(self._system_key_manager.parallel_sessions),
                    "active_data_types": list(
                        self._system_key_manager.data_type_sessions.keys()
                    ),
                    "sessions_per_data_type": {
                        dt: len(sessions)
                        for dt, sessions in self._system_key_manager.data_type_sessions.items()
                    },
                }

            status["parallelization_config"] = self.parallelization_config
            return status
        return {"status": "no_manager_initialized"}

    def cleanup_expired_rate_limits(self):
        """Clean up expired rate limits in the key manager."""
        if self._system_key_manager:
            self._system_key_manager.cleanup_expired_rate_limits()

    async def cleanup(self):
        """Clean up all resources."""
        logger.info("Cleaning up bicam processing resources")

        # Release API keys for all data types
        if self._system_key_manager:
            try:
                for data_type in list(self._parallel_sessions.keys()):
                    await self.release_keys_for_data_type(data_type)
            except Exception as e:
                logger.warning(f"Error releasing API keys: {e}")

        # Close API clients
        if self._api_clients:
            try:
                for client in self._api_clients.values():
                    if hasattr(client, "close"):
                        await client.close()
                self._api_clients.clear()
            except Exception as e:
                logger.warning(f"Error closing API clients: {e}")

        # Clean up run manager (but don't close the database pool if it's shared)
        if self._run_manager:
            try:
                await self._run_manager.cleanup()
            except Exception as e:
                logger.warning(f"Error cleaning up run manager: {e}")

        # Close database pool with proper event loop handling
        if self._db_pool:
            try:
                # Check if we have a running event loop
                try:
                    asyncio.get_running_loop()
                    # We have a running loop, close gracefully
                    await self._db_pool.close()
                    logger.debug("Database pool closed gracefully")
                except RuntimeError:
                    # No running loop, force terminate
                    logger.debug("No running loop, terminating database pool")
                    self._db_pool.terminate()
            except Exception as e:
                logger.warning(f"Error closing database pool: {e}")
                # Force cleanup anyway
                try:
                    self._db_pool.terminate()
                except Exception as term_error:
                    logger.debug(f"Error terminating database pool: {term_error}")
            finally:
                self._db_pool = None

        # Clear all cached resources
        self._system_key_manager = None
        self._checkpoint_manager = None
        self._run_manager = None
        self._parallel_sessions.clear()

        logger.debug("Bicam processing resources cleanup completed")

    def get_configuration_summary(self) -> dict[str, Any]:
        """Get a summary of current configuration."""
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
                "active_clients": len(self._api_clients),
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
            "key_manager_status": self.get_key_manager_status(),
        }


class DataTypeSpecificResource(ConfigurableResource):
    """
    Resource for data type specific configuration.

    This can be used when you need different configurations
    for different data types (e.g., different batch sizes,
    rate limits, or processing parameters).
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
