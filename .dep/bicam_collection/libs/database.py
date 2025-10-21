"""
Database Setup and Management Utilities

This module provides utilities for setting up and managing the PostgreSQL
database for the bicam-collection pipeline.
"""

import asyncio
import logging
from pathlib import Path

import asyncpg
import yaml

from .config import BicamConfig

logger = logging.getLogger(__name__)


class DatabaseSetupError(Exception):
    """Custom exception for database setup errors"""


class DatabaseManager:
    """Database management class for setup, validation, and operations"""

    def __init__(self, config: BicamConfig):
        self.config = config
        self.db_config = config.database

    async def test_connection(self) -> bool:
        """Test database connection"""
        try:
            conn = await asyncpg.connect(self.db_config.connection_string)
            await conn.fetchval("SELECT 1")
            await conn.close()
            logger.info("Database connection successful")
            return True
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            return False

    async def get_existing_schemas(self) -> set[str]:
        """Get list of existing schemas in the database"""
        try:
            conn = await asyncpg.connect(self.db_config.connection_string)
            schemas = await conn.fetch(
                "SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')"
            )
            await conn.close()
            return {row["schema_name"] for row in schemas}
        except Exception as e:
            logger.error(f"Error getting schemas: {e}")
            return set()

    async def get_existing_tables(self, schema: str = "public") -> set[str]:
        """Get list of existing tables in a schema"""
        try:
            conn = await asyncpg.connect(self.db_config.connection_string)
            tables = await conn.fetch(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = $1",
                schema,
            )
            await conn.close()
            return {row["table_name"] for row in tables}
        except Exception as e:
            logger.error(f"Error getting tables for schema {schema}: {e}")
            return set()

    async def execute_sql_file(
        self, sql_file_path: Path, schema: str | None = None
    ) -> bool:
        """Execute SQL commands from a file"""
        if not sql_file_path.exists():
            logger.error(f"SQL file not found: {sql_file_path}")
            return False

        try:
            conn = await asyncpg.connect(self.db_config.connection_string)
            try:
                with open(sql_file_path) as f:
                    sql_content = f.read()
                await conn.execute(sql_content)
            finally:
                await conn.close()

            logger.info(f"Successfully executed SQL file: {sql_file_path}")
            return True

        except Exception as e:
            logger.error(f"Error executing SQL file {sql_file_path}: {e}")
            return False

    async def drop_schema(self, schema_name: str, cascade: bool = False) -> bool:
        """Drop a schema"""
        try:
            conn = await asyncpg.connect(self.db_config.connection_string)
            cascade_clause = "CASCADE" if cascade else "RESTRICT"
            await conn.execute(
                f'DROP SCHEMA IF EXISTS "{schema_name}" {cascade_clause}'
            )
            await conn.close()
            logger.info(f"Dropped schema: {schema_name}")
            return True
        except Exception as e:
            logger.error(f"Error dropping schema {schema_name}: {e}")
            return False

    async def setup_schemas(self) -> bool:
        """Set up all database schemas and create tables from Pydantic schema configurations."""
        try:
            logger.info("Setting up schemas from SQL files...")

            # First create the basic schemas
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

            for schema_name in schema_names:
                if not await self.create_schema(schema_name):
                    logger.error(f"Failed to create schema: {schema_name}")
                    return False

            # Execute essential SQL files for metadata and runs
            base_path = Path(__file__).parent / "sql"
            schema_sql_files = {
                "bicam_final": base_path / "build_schema_bicam.sql",
                "bicam_metadata": base_path / "build_metadata_schema.sql",
                "bicam_runs": base_path / "build_runs_schema.sql",
                "bicam_checkpoints": base_path / "build_checkpoints_schema.sql",
            }

            # Set up schemas from SQL files
            for schema, sql_file in schema_sql_files.items():
                if sql_file.exists():
                    logger.info(f"Setting up {schema} schema from {sql_file}")
                    if not await self.execute_sql_file(sql_file):
                        logger.error(f"Failed to setup {schema} schema")
                        return False
                else:
                    logger.warning(f"No SQL file found for schema: {schema}")

            # ------------------------------------------------------------------
            #  Insert default rows in metadata tables for every *main* data-type
            # ------------------------------------------------------------------
            await self._initialise_metadata_tables()

            logger.info(
                "Schemas prepared successfully (tables will be created by individual loaders at runtime)"
            )
            return True

        except Exception as e:
            logger.error(f"Error setting up schemas: {e}")
            return False

    async def _create_tables_for_config(self, config, category: str) -> bool:
        """Create tables for a single data type config"""
        try:
            import logging

            logger = logging.getLogger(__name__)

            # Determine schemas based on category
            if category == "congressional":
                raw_schema = "bicam_raw_congressional"
                staging_schema = "bicam_staging_congressional"
                prod_schema = "bicam_congressional"
            elif category == "govinfo":
                raw_schema = "bicam_raw_govinfo"
                staging_schema = "bicam_staging_govinfo"
                prod_schema = "bicam_govinfo"
            else:
                logger.warning(f"Unknown category: {category}")
                return False

            conn = await asyncpg.connect(self.db_config.connection_string)
            try:
                # 1. Create raw table if needed
                if config.create_raw:
                    await self._create_raw_table(conn, config, raw_schema)

                # 2. Create staging table
                await self._create_staging_table(conn, config, staging_schema)

                # 3. Create production table
                await self._create_production_table(conn, config, prod_schema)
            finally:
                await conn.close()

            logger.info(f"Created tables for {config.name}")
            return True

        except Exception as e:
            logger.error(f"Error creating tables for {config.name}: {e}")
            return False

    async def _create_raw_table(self, conn, config, schema: str) -> None:
        """
        Create raw data tables.

        Always creates:
        - {table_name}_raw: Main raw table for storing individual item data

        Only creates for main data types (is_main=True):
        - {table_name}_list_raw: List raw table for storing batch list data

        Related data types (is_main=False) only get the main raw table.
        """
        base_table_name = config.table_name
        is_main_type = config.is_main

        logger.info(
            f"Creating raw tables for: {base_table_name} (is_main: {is_main_type})"
        )

        # Create the main raw table (always created)
        main_table_name = f"{base_table_name}_raw"
        main_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{main_table_name} (
                surrogate_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                payload JSONB NOT NULL,
                endpoint TEXT,
                source_doc_id TEXT,
                etl_batch_id UUID
            );

            CREATE INDEX IF NOT EXISTS idx_{main_table_name}_source_doc_id
            ON {schema}.{main_table_name}(source_doc_id);

            CREATE INDEX IF NOT EXISTS idx_{main_table_name}_fetched_at
            ON {schema}.{main_table_name}(fetched_at);
        """

        try:
            await conn.execute(main_sql)
            logger.info(f"Successfully created {schema}.{main_table_name}")
        except Exception as e:
            logger.error(f"Error creating {schema}.{main_table_name}: {e}")
            raise

        # Create the list raw table ONLY for main data types
        if is_main_type:
            list_table_name = f"{base_table_name}_list_raw"
            list_sql = f"""
                CREATE TABLE IF NOT EXISTS {schema}.{list_table_name} (
                    surrogate_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    payload JSONB NOT NULL,
                    endpoint TEXT,
                    source_doc_id TEXT,
                    etl_batch_id UUID,
                    run_id TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_{list_table_name}_source_doc_id
                ON {schema}.{list_table_name}(source_doc_id);

                CREATE INDEX IF NOT EXISTS idx_{list_table_name}_fetched_at
                ON {schema}.{list_table_name}(fetched_at);

                CREATE INDEX IF NOT EXISTS idx_{list_table_name}_run_id
                ON {schema}.{list_table_name}(run_id);
            """

            try:
                await conn.execute(list_sql)
                logger.info(f"Successfully created {schema}.{list_table_name}")
            except Exception as e:
                logger.error(f"Error creating {schema}.{list_table_name}: {e}")
                raise
        else:
            logger.info(
                f"Skipping _list_raw table creation for related data type: {base_table_name}"
            )

    async def _create_staging_table(self, conn, config, schema: str) -> None:
        """Create a staging table with _batch_id and _inserted_at tracking columns"""
        table_name = f"{config.table_name}_staging"

        # Build column definitions
        columns = []
        try:
            for field in config.fields:
                sql_type = self._map_field_type_to_sql(
                    field.type,
                    field.max_length,
                )
                nullable = "" if field.required else "NULL"
                default_clause = (
                    f"DEFAULT {field.default}" if field.default is not None else ""
                )

                columns.append(
                    f"{field.name} {sql_type} {nullable} {default_clause}".strip()
                )
        except Exception as e:
            logger.error(
                f"Error processing fields for {config.name}/{config.table_name}: {e}"
            )
            logger.error(f"Config fields type: {type(config.fields)}")
            if config.fields:
                logger.error(f"First field type: {type(config.fields[0])}")
                logger.error(f"First field: {config.fields[0]}")
            raise

        # Add tracking columns
        columns.append("_batch_id UUID")
        columns.append("_inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW()")

        # Build primary key clause
        pk_clause = ""
        if config.id_fields:
            pk_fields = ", ".join(config.id_fields)
            pk_clause = f", PRIMARY KEY ({pk_fields})"

        # Create table first
        table_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
                {", ".join(columns)}{pk_clause}
            )
        """

        await conn.execute(table_sql)

        # Create indexes separately after confirming table exists
        try:
            # Verify the table and columns exist before creating indexes
            result = await conn.fetchval(f"""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema = '{schema}'
                    AND table_name = '{table_name}'
                    AND column_name = '_batch_id'
                )
            """)

            if result:
                await conn.execute(f"""
                    CREATE INDEX IF NOT EXISTS idx_{table_name}_batch_id
                    ON {schema}.{table_name}(_batch_id)
                """)

                await conn.execute(f"""
                    CREATE INDEX IF NOT EXISTS idx_{table_name}_inserted_at
                    ON {schema}.{table_name}(_inserted_at)
                """)
            else:
                logger.warning(
                    f"Column _batch_id not found in {schema}.{table_name}, skipping index creation"
                )
        except Exception as e:
            logger.warning(f"Error creating indexes for {table_name}: {e}")

    async def _create_production_table(self, conn, config, schema: str) -> None:
        """Create a production table with proper constraints and indexes"""
        table_name = config.table_name

        # Build column definitions
        columns = []
        try:
            for field in config.fields:
                sql_type = self._map_field_type_to_sql(
                    field.type,
                    field.max_length,
                )
                nullable = "NOT NULL" if field.required else ""
                default_clause = (
                    f"DEFAULT {field.default}" if field.default is not None else ""
                )

                columns.append(
                    f"{field.name} {sql_type} {nullable} {default_clause}".strip()
                )
        except Exception as e:
            logger.error(
                f"Error processing fields for {config.name}/{config.table_name}: {e}"
            )
            logger.error(f"Config fields type: {type(config.fields)}")
            if config.fields:
                logger.error(f"First field type: {type(config.fields[0])}")
                logger.error(f"First field: {config.fields[0]}")
            raise

        # Add tracking columns
        columns.append("_batch_id UUID")
        columns.append("_inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW()")

        # Build primary key clause
        pk_clause = ""
        if config.id_fields:
            pk_fields = ", ".join(config.id_fields)
            pk_clause = f", PRIMARY KEY ({pk_fields})"

        # Create table first
        table_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
                {", ".join(columns)}{pk_clause}
            )
        """

        await conn.execute(table_sql)

        # Create indexes separately after confirming table exists
        try:
            # Verify the table and columns exist before creating indexes
            result = await conn.fetchval(f"""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema = '{schema}'
                    AND table_name = '{table_name}'
                    AND column_name = '_batch_id'
                )
            """)

            if result:
                await conn.execute(f"""
                    CREATE INDEX IF NOT EXISTS idx_{table_name}_batch_id
                    ON {schema}.{table_name}(_batch_id)
                """)

                await conn.execute(f"""
                    CREATE INDEX IF NOT EXISTS idx_{table_name}_inserted_at
                    ON {schema}.{table_name}(_inserted_at)
                """)
            else:
                logger.warning(
                    f"Column _batch_id not found in {schema}.{table_name}, skipping batch_id index creation"
                )

            # Add indexes for ID fields
            if config.id_fields:
                for id_field in config.id_fields:
                    # Check if the ID field exists before creating index
                    id_result = await conn.fetchval(f"""
                        SELECT EXISTS (
                            SELECT 1 FROM information_schema.columns
                            WHERE table_schema = '{schema}'
                            AND table_name = '{table_name}'
                            AND column_name = '{id_field}'
                        )
                    """)

                    if id_result:
                        await conn.execute(f"""
                            CREATE INDEX IF NOT EXISTS idx_{table_name}_{id_field}
                            ON {schema}.{table_name}({id_field})
                        """)
                    else:
                        logger.warning(
                            f"Column {id_field} not found in {schema}.{table_name}, skipping index creation"
                        )
        except Exception as e:
            logger.warning(f"Error creating indexes for {table_name}: {e}")

    def _map_field_type_to_sql(
        self, field_type: str, max_length: int | None = None
    ) -> str:
        """Map config field types to SQL types"""
        type_mapping = {
            "string": f"VARCHAR({max_length})" if max_length else "TEXT",
            "text": "TEXT",
            "varchar": f"VARCHAR({max_length})" if max_length else "VARCHAR(255)",
            "integer": "INTEGER",
            "int": "INTEGER",
            "bigint": "BIGINT",
            "float": "REAL",
            "double": "DOUBLE PRECISION",
            "decimal": "DECIMAL",
            "boolean": "BOOLEAN",
            "bool": "BOOLEAN",
            "date": "DATE",
            "datetime": "TIMESTAMPTZ",
            "timestamp": "TIMESTAMPTZ",
            "timestamptz": "TIMESTAMPTZ",
            "json": "JSONB",
            "jsonb": "JSONB",
            "uuid": "UUID",
        }

        return type_mapping.get(field_type.lower(), "TEXT")

    async def create_schema(self, schema_name: str) -> bool:
        """Create a schema if it doesn't exist"""
        try:
            conn = await asyncpg.connect(self.db_config.connection_string)
            await conn.execute(
                f'CREATE SCHEMA IF NOT EXISTS "{schema_name}" AUTHORIZATION {self.db_config.username}'
            )
            await conn.close()
            logger.info(f"Created/verified schema: {schema_name}")
            return True
        except Exception as e:
            logger.error(f"Error creating schema {schema_name}: {e}")
            return False

    async def validate_database_setup(self) -> dict[str, bool]:
        """Validate that the database is properly set up"""
        validation_results = {
            "connection": False,
            "schemas_exist": False,
            "required_tables_exist": False,
            "indexes_exist": False,
        }

        try:
            # Test connection
            validation_results["connection"] = await self.test_connection()

            if validation_results["connection"]:
                # Check schemas
                existing_schemas = await self.get_existing_schemas()
                required_schemas = {
                    "public",
                    "bicam_raw_congressional",
                    "bicam_raw_govinfo",
                    "bicam_staging_congressional",
                    "bicam_staging_govinfo",
                    "bicam_congressional",
                    "bicam_govinfo",
                    "bicam_final",
                    "bicam_metadata",
                    "bicam_runs",
                    "bicam_checkpoints",
                }
                validation_results["schemas_exist"] = required_schemas.issubset(
                    existing_schemas
                )

                # Check tables (basic validation)
                public_tables = await self.get_existing_tables("public")
                validation_results["required_tables_exist"] = len(public_tables) > 0

                # TODO: Add index validation
                validation_results["indexes_exist"] = True

        except Exception as e:
            logger.error(f"Database validation error: {e}")

        return validation_results

    async def get_database_info(self) -> dict[str, any]:
        """Get comprehensive database information"""
        try:
            conn = await asyncpg.connect(self.db_config.connection_string)

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

            await conn.close()

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

    # ------------------------------------------------------------------
    #  Metadata bootstrap helpers
    # ------------------------------------------------------------------
    async def _collect_main_data_types(self) -> dict[str, list[str]]:
        """Return {category: [data_type, …]} for every *main* data-type found in config.yaml files."""
        categories: dict[str, list[str]] = {"congressional": [], "govinfo": []}
        data_types_path = Path(__file__).parent.parent / "data_types"

        for cfg_file in data_types_path.rglob("config.yaml"):
            category = cfg_file.parent.parent.name  # congressional / govinfo
            data_type = cfg_file.parent.name

            if category not in categories:
                continue

            try:
                with open(cfg_file) as f:
                    cfg_data = yaml.safe_load(f)

                # normalise to list
                if isinstance(cfg_data, dict):
                    cfg_data = [cfg_data]

                first_cfg = cfg_data[0] if cfg_data else {}
                if first_cfg.get("is_main", True):
                    categories[category].append(data_type)
            except Exception as exc:
                logger.warning("Failed parsing %s: %s", cfg_file, exc)

        return categories

    async def _ensure_metadata_row(self, conn, table: str, data_type: str):
        row = await conn.fetchrow(
            f"SELECT 1 FROM {table} WHERE data_type = $1", data_type
        )
        if row is None:
            await conn.execute(
                f"INSERT INTO {table} (data_type, last_processed_date, last_total_count) VALUES ($1, $2, $3)",
                data_type,
                "1789-01-01T00:00:00Z",
                0,
            )

    async def _initialise_metadata_tables(self):
        """Insert default rows (1789-01-01) for every main data-type if they are missing."""
        categories = await asyncio.to_thread(self._collect_main_data_types)

        if not categories["congressional"] and not categories["govinfo"]:
            logger.warning(
                "No data-types discovered – skipping metadata initialisation"
            )
            return

        conn = await asyncpg.connect(self.db_config.connection_string)
        try:
            for category, dts in categories.items():
                if not dts:
                    continue
                tbl = f"bicam_metadata.{category}_last_processed_dates"
                for dt in dts:
                    await self._ensure_metadata_row(conn, tbl, dt)

            logger.info("Inserted default rows into metadata tables where needed")
        finally:
            await conn.close()


async def setup_database(config: BicamConfig, recreate: bool = False) -> bool:
    """Set up the database, schemas, and tables"""
    logger.info(f"Starting database setup for '{config.database.database}'")
    db_manager = DatabaseManager(config)

    try:
        # 1. Test connection
        if not await db_manager.test_connection():
            raise DatabaseSetupError("Initial database connection failed.")

        # Schemas to manage
        schemas_to_manage = [
            "bicam_raw_congressional",
            "bicam_raw_govinfo",
            "bicam_staging_congressional",
            "bicam_staging_govinfo",
            "bicam_congressional",
            "bicam_govinfo",
            "bicam_final",
            "bicam_metadata",
            "bicam_runs",
            "bicam_checkpoints",
        ]

        if recreate:
            logger.info("Recreating schemas...")
            for schema in reversed(schemas_to_manage):
                logger.info(f"Dropping schema: {schema}")
                await db_manager.drop_schema(schema, cascade=True)

        # 2. Create schemas
        for schema in schemas_to_manage:
            if not await db_manager.create_schema(schema):
                raise DatabaseSetupError(f"Failed to create schema: {schema}")

        # 3. Set up schemas and dynamic tables from individual config.yaml files
        if not await db_manager.setup_schemas():
            raise DatabaseSetupError("Dynamic table creation from config files failed.")

        logger.info("Database setup completed successfully.")
        return True

    except (DatabaseSetupError, Exception) as e:
        logger.error(f"Database setup failed: {e}")
        return False


def setup_database_sync(config: BicamConfig, recreate: bool = False) -> bool:
    """Synchronous wrapper for database setup"""
    return asyncio.run(setup_database(config, recreate))


if __name__ == "__main__":
    # Example usage
    import sys

    from .config import load_config

    logging.basicConfig(level=logging.INFO)

    try:
        config = load_config()
        success = setup_database_sync(config)
        sys.exit(0 if success else 1)
    except Exception as e:
        logger.error(f"Database setup failed: {e}")
        sys.exit(1)
