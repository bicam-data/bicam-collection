"""
Resource Managers

Focused managers for specific resource types to avoid the complexity
of the original ResourceManager approach.
"""

import asyncio
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import asyncpg
import yaml

from streamlined.api_clients import CongressionalAPIClient, GovInfoAPIClient

# SystemAPIKeyManager and APIKeySession removed - using rotisserie instead
from streamlined.libs.hierarchical_checkpoint_system import (
    HierarchicalCheckpointManager,
)
from streamlined.libs.run_tracking import RunManager
from streamlined.processing.storage_manager import (
    StorageManager as ProcessingStorageManager,
)

if TYPE_CHECKING:
    from .config import Config

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Focused manager for database connection pooling and schema setup."""

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

    @property
    def connection_string(self) -> str:
        """Generate PostgreSQL connection string."""
        from urllib.parse import quote_plus

        username = quote_plus(self.user)
        password = quote_plus(self.password)
        return f"postgresql://{username}:{password}@{self.host}:{self.port}/{self.database}"

    async def test_connection(self) -> bool:
        """Test database connection."""
        try:
            pool = await self.get_pool()
            async with pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
            logger.info("Database connection successful")
            return True
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            return False

    async def setup_schemas(self) -> bool:
        """Set up essential database schemas."""
        try:
            logger.info("Setting up database schemas...")

            # Essential schema names
            schema_names = [
                "bicam_staging_congressional",
                "bicam_staging_govinfo",
                "bicam_raw_congressional",
                "bicam_raw_govinfo",
                "bicam_congressional",
                "bicam_govinfo",
                "bicam_final",
                "bicam_metadata",
                "bicam_runs",
                "bicam_checkpoints",
            ]

            pool = await self.get_pool()
            async with pool.acquire() as conn:
                for schema_name in schema_names:
                    await conn.execute(
                        f'CREATE SCHEMA IF NOT EXISTS "{schema_name}" AUTHORIZATION {self.user}'
                    )
                    logger.debug(f"Created/verified schema: {schema_name}")

            # Execute essential SQL files
            await self._setup_essential_tables()

            # Initialize metadata tables
            await self._initialize_metadata_tables()

            logger.info("Database schemas set up successfully")
            return True

        except Exception as e:
            logger.error(f"Error setting up schemas: {e}")
            return False

    async def _setup_essential_tables(self) -> None:
        """Set up essential tables from SQL files."""
        base_path = Path(__file__).parent.parent / "libs" / "sql"

        # Essential SQL files
        sql_files = {
            "bicam_metadata": base_path / "build_metadata_schema.sql",
            "bicam_runs": base_path / "build_runs_schema.sql",
            "bicam_checkpoints": base_path / "build_checkpoints_schema.sql",
        }

        pool = await self.get_pool()
        async with pool.acquire() as conn:
            for schema_name, sql_file in sql_files.items():
                if sql_file.exists():
                    try:
                        with open(sql_file) as f:
                            sql_content = f.read()
                        await conn.execute(sql_content)
                        logger.debug(f"Executed SQL file for {schema_name}")
                    except Exception as e:
                        logger.warning(f"Failed to execute {sql_file}: {e}")
                else:
                    logger.warning(f"SQL file not found: {sql_file}")

    async def _initialize_metadata_tables(self) -> None:
        """Initialize metadata tables with default data."""
        try:
            data_types = self._discover_data_types()

            if not data_types["congressional"] and not data_types["govinfo"]:
                logger.warning(
                    "No data types discovered - skipping metadata initialization"
                )
                return

            pool = await self.get_pool()
            async with pool.acquire() as conn:
                for category, dt_list in data_types.items():
                    if not dt_list:
                        continue

                    table_name = f"bicam_metadata.{category}_last_processed_dates"

                    for data_type in dt_list:
                        # Check if row exists
                        existing = await conn.fetchval(
                            f"SELECT 1 FROM {table_name} WHERE data_type = $1",
                            data_type,
                        )

                        if not existing:
                            await conn.execute(
                                f"""
                                INSERT INTO {table_name}
                                (data_type, last_processed_date, last_total_count)
                                VALUES ($1, $2, $3)
                                """,
                                data_type,
                                "1789-01-01T00:00:00Z",
                                0,
                            )
                            logger.debug(f"Initialized metadata for {data_type}")

            logger.info("Metadata tables initialized successfully")

        except Exception as e:
            logger.warning(f"Failed to initialize metadata tables: {e}")

    def _discover_data_types(self) -> dict[str, list[str]]:
        """Discover available data types from config files."""
        categories = {"congressional": [], "govinfo": []}
        data_types_path = Path(__file__).parent.parent / "data_types"

        if not data_types_path.exists():
            return categories

        for cfg_file in data_types_path.rglob("config.yaml"):
            try:
                category = cfg_file.parent.parent.name
                data_type = cfg_file.parent.name

                if category not in categories:
                    continue

                with open(cfg_file) as f:
                    cfg_data = yaml.safe_load(f)

                # Normalize to list
                if isinstance(cfg_data, dict):
                    cfg_data = [cfg_data]

                first_cfg = cfg_data[0] if cfg_data else {}
                if first_cfg.get("is_main", True):
                    categories[category].append(data_type)

            except Exception as e:
                logger.warning(f"Failed to process config file {cfg_file}: {e}")

        return categories

    async def get_database_info(self) -> dict[str, Any]:
        """Get database information."""
        try:
            pool = await self.get_pool()
            async with pool.acquire() as conn:
                # Get basic database info
                db_info = await conn.fetchrow("""
                    SELECT
                        current_database() as database_name,
                        current_user as current_user,
                        version() as postgres_version,
                        pg_size_pretty(pg_database_size(current_database())) as database_size
                """)

                # Get schema count
                schema_count = await conn.fetchval("""
                    SELECT COUNT(*) FROM information_schema.schemata
                    WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
                """)

                # Get table count
                table_count = await conn.fetchval("""
                    SELECT COUNT(*) FROM information_schema.tables
                    WHERE table_schema NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
                """)

                return {
                    "database_name": db_info["database_name"],
                    "current_user": db_info["current_user"],
                    "postgres_version": db_info["postgres_version"],
                    "database_size": db_info["database_size"],
                    "schema_count": schema_count,
                    "table_count": table_count,
                }

        except Exception as e:
            logger.error(f"Error getting database info: {e}")
            return {}

    async def create_raw_table(
        self, schema: str, table_name: str, is_main_type: bool = True
    ) -> bool:
        """Create raw data table(s) for a data type."""
        try:
            pool = await self.get_pool()
            async with pool.acquire() as conn:
                # Main raw table
                main_table = f"{table_name}_raw"
                await conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {schema}.{main_table} (
                        surrogate_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                        fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        payload JSONB NOT NULL,
                        endpoint TEXT,
                        source_doc_id TEXT,
                        etl_batch_id UUID
                    )
                """)

                # Indexes
                await conn.execute(f"""
                    CREATE INDEX IF NOT EXISTS idx_{main_table}_source_doc_id
                    ON {schema}.{main_table}(source_doc_id)
                """)

                await conn.execute(f"""
                    CREATE INDEX IF NOT EXISTS idx_{main_table}_fetched_at
                    ON {schema}.{main_table}(fetched_at)
                """)

                # List raw table for main data types
                if is_main_type:
                    list_table = f"{table_name}_list_raw"
                    await conn.execute(f"""
                        CREATE TABLE IF NOT EXISTS {schema}.{list_table} (
                            surrogate_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                            fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                            payload JSONB NOT NULL,
                            endpoint TEXT,
                            source_doc_id TEXT,
                            etl_batch_id UUID,
                            run_id TEXT
                        )
                    """)

                    # List table indexes
                    await conn.execute(f"""
                        CREATE INDEX IF NOT EXISTS idx_{list_table}_source_doc_id
                        ON {schema}.{list_table}(source_doc_id)
                    """)

                    await conn.execute(f"""
                        CREATE INDEX IF NOT EXISTS idx_{list_table}_run_id
                        ON {schema}.{list_table}(run_id)
                    """)

                logger.info(f"Created raw table(s) for {schema}.{table_name}")
                return True

        except Exception as e:
            logger.error(f"Error creating raw table {schema}.{table_name}: {e}")
            return False

    async def close(self):
        """Close database connection pool."""
        if self._pool:
            try:
                await self._pool.close()
                logger.info("Database pool closed")
            except Exception as e:
                logger.warning(f"Error closing database pool: {e}")
            finally:
                self._pool = None


class APIKeyManager:
    """
    Simplified API key manager using rotisserie.

    Rotisserie handles key rotation and rate limiting automatically,
    so this manager is now just a thin wrapper for compatibility.
    """

    def __init__(
        self,
        api_keys: list[str],
        parallelization_config: dict[str, Any] = None,
        use_dynamic_pool: bool = False,
    ):
        self.api_keys = api_keys
        self.parallelization_config = parallelization_config or {}
        self._parallel_sessions: dict[str, list] = {}

    async def get_parallel_sessions(self, data_type: str, processing_type: str) -> list:
        """
        Get parallel sessions for a data type.

        For fetchers: Returns a simple session dict with all API keys.
        Rotisserie handles the actual key management in the API clients.
        """
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

        # For fetchers, create a simple session with all API keys
        # Rotisserie in the API clients handles actual key rotation
        if processing_type == "fetcher":
            session = {
                "session_id": f"{data_type}_session_0",
                "api_keys": self.api_keys,
                "type": "api_based",
            }
            self._parallel_sessions[data_type] = [session]
            logger.info(
                f"Created API session for {data_type} with {len(self.api_keys)} keys (rotisserie handles rotation)"
            )
            return [session]

        # Default: single CPU-based session
        session = {"session_id": f"{data_type}_session_0", "type": "cpu_based"}
        self._parallel_sessions[data_type] = [session]
        return [session]

    async def release_keys_for_data_type(self, data_type: str):
        """Release sessions for a specific data type."""
        if data_type in self._parallel_sessions:
            del self._parallel_sessions[data_type]
            logger.debug(f"Released sessions for {data_type}")

    def get_status(self) -> dict[str, Any]:
        """Get API key manager status."""
        return {
            "total_keys": len(self.api_keys),
            "parallelization_config": self.parallelization_config,
            "active_sessions": len(self._parallel_sessions),
        }


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
        self._manager: ProcessingStorageManager | None = None

    async def get_manager(self) -> ProcessingStorageManager | None:
        """Get or create storage manager."""
        if not self.use_optimized_storage:
            return None

        if self._manager is None and self.db_manager:
            db_pool = await self.db_manager.get_pool()
            self._manager = ProcessingStorageManager(
                pg_pool=db_pool,
                checkpoint_db_path=self.checkpoint_db_path,
                batch_size=self.batch_size,
            )
            await self._manager.start()
            logger.info("StorageManager started")

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
        self._api_clients: dict[str, CongressionalAPIClient | GovInfoAPIClient] = {}

    async def get_clients_for_parallel_sessions(
        self, data_type: str, sessions: list, source: str
    ) -> list[CongressionalAPIClient | GovInfoAPIClient]:
        """Create API clients for parallel sessions."""
        clients = []
        db_pool = await self.db_manager.get_pool() if self.db_manager else None

        for i, session in enumerate(sessions):
            client_key = f"{data_type}_session_{i}"

            if client_key in self._api_clients:
                clients.append(self._api_clients[client_key])
                continue

            # Get API keys from session (rotisserie handles rotation in clients)
            api_keys = session.get("api_keys", [])
            if not api_keys:
                logger.warning(f"No API keys in session {client_key}, skipping")
                continue

            # Create rotisserie key pool for this client
            from rotisserie import AsyncKeyPool, KeyConfig

            key_configs = [KeyConfig(f"key_{j}", key) for j, key in enumerate(api_keys)]
            key_pool = AsyncKeyPool(key_configs, distribute=True)

            if source == "congressional":
                client = CongressionalAPIClient(
                    api_keys=api_keys,
                    rate_limit_per_second=self.api_rate_limit,
                    db_pool=db_pool,
                    key_pool=key_pool,
                )
            elif source == "govinfo":
                client = GovInfoAPIClient(
                    api_keys=api_keys,
                    rate_limit_per_second=self.api_rate_limit,
                    db_pool=db_pool,
                    key_pool=key_pool,
                )
            else:
                raise ValueError(f"Invalid source: {source}")
            await client.__aenter__()
            self._api_clients[client_key] = client
            clients.append(client)

            masked_keys = [k[:8] + "..." for k in api_keys]
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


async def setup_database(
    host: str = None,
    port: int = None,
    database: str = None,
    user: str = None,
    password: str = None,
    config: "Config" = None,
) -> bool:
    """
    Set up database schemas.

    Args:
        host: Database host (or use config)
        port: Database port (or use config)
        database: Database name (or use config)
        user: Database user (or use config)
        password: Database password (or use config)
        config: Config instance (takes precedence)

    Returns:
        True if setup successful, False otherwise
    """
    try:
        # Use config if provided, otherwise use individual parameters
        if config:
            db_manager = DatabaseManager(
                host=config.database.host,
                port=config.database.port,
                database=config.database.database,
                user=config.database.username,
                password=config.database.password,
            )
        else:
            # Use provided parameters or environment defaults
            from .config import DatabaseConfig

            db_config = DatabaseConfig()

            db_manager = DatabaseManager(
                host=host or db_config.host,
                port=port or db_config.port,
                database=database or db_config.database,
                user=user or db_config.username,
                password=password or db_config.password,
            )

        # Test connection first
        if not await db_manager.test_connection():
            logger.error("Database connection failed")
            return False

        # Set up schemas
        success = await db_manager.setup_schemas()

        # Clean up
        await db_manager.close()

        if success:
            logger.info("Streamlined database setup completed successfully")
        else:
            logger.error("Streamlined database setup failed")

        return success

    except Exception as e:
        logger.error(f"Error during streamlined database setup: {e}")
        return False
