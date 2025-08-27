import asyncio
import json
import logging
import os
import sqlite3
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Any

import asyncpg
from dotenv import load_dotenv

logger = logging.getLogger(__name__)


class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder that handles datetime and UUID objects"""

    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, timedelta):
            return obj.total_seconds()
        elif isinstance(obj, uuid.UUID):
            return str(obj)
        return super().default(obj)


def safe_json_dumps(obj, **kwargs):
    """Safely dumps JSON with datetime handling"""
    kwargs.setdefault("cls", DateTimeEncoder)
    kwargs.setdefault("ensure_ascii", False)
    return json.dumps(obj, **kwargs)


class RunStatus(Enum):
    """Status of a run"""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PAUSED = "paused"


class RunType(Enum):
    """Type of run"""

    SCRAPER_CONGRESSIONAL = "scraper_congressional"
    SCRAPER_GOVINFO = "scraper_govinfo"
    DATABASE_BUILD = "database_build"
    DATA_CLEANING = "data_cleaning"
    DATA_NORMALIZATION = "data_normalization"
    BULK_EXPORT = "bulk_export"
    SCHEMA_MIGRATION = "schema_migration"
    LOBBYIST_MATCHING = "lobbyist_matching"
    OTHER = "other"


@dataclass
class RunMetadata:
    """Metadata for a run"""

    run_id: str
    run_type: RunType
    system_name: str  # e.g., "congressional_scraper", "govinfo_scraper"
    description: str
    parameters: dict[str, Any] = field(default_factory=dict)
    data_types: list[str] = field(
        default_factory=list
    )  # Which data types being processed
    expected_duration_minutes: int | None = None
    priority: int = 1  # 1=low, 5=high
    tags: list[str] = field(default_factory=list)
    created_by: str = "system"


@dataclass
class RunRecord:
    """Complete run record with status and timing"""

    metadata: RunMetadata
    status: RunStatus = RunStatus.PENDING
    started_at: datetime | None = None
    completed_at: datetime | None = None
    error_message: str | None = None
    progress_data: dict[str, Any] = field(default_factory=dict)
    output_summary: dict[str, Any] = field(default_factory=dict)
    resource_usage: dict[str, Any] = field(default_factory=dict)
    dependencies: list[str] = field(
        default_factory=list
    )  # Other run_ids this depends on
    blocked_by: list[str] = field(default_factory=list)  # Run_ids blocking this
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class RunManager:
    """Manages run tracking across the entire system"""

    def __init__(
        self,
        db_path: Path | None = None,
        *,
        use_postgres: bool = True,
        external_pool: asyncpg.Pool | None = None,
        postgres_config: dict | None = None,
    ):
        """Create a RunManager.

        Parameters
        ----------
        external_pool
            Re-use an already-opened asyncpg pool instead of creating a new
            one.  If supplied, `use_postgres` is forced `True`.
        postgres_config
            Override the default env-based connection parameters when the
            manager has to create its own pool.
        """

        self.db_path = Path(db_path) if db_path else Path("bicam_runs.db")
        self.postgres_pool: asyncpg.Pool | None = external_pool

        # If a pool is supplied we are definitely using Postgres.
        self.use_postgres = use_postgres or external_pool is not None

        if self.use_postgres and external_pool is None:
            load_dotenv()
            self.postgres_config = postgres_config or {
                "database": os.getenv("POSTGRESQL_DATABASE")
                or os.getenv("POSTGRESQL_DB"),
                "user": os.getenv("POSTGRESQL_USERNAME")
                or os.getenv("POSTGRESQL_USER"),
                "password": os.getenv("POSTGRESQL_PASSWORD"),
                "host": os.getenv("POSTGRESQL_HOST", "localhost"),
                "port": os.getenv("POSTGRESQL_PORT", "5432"),
            }

        if not self.use_postgres:
            self._init_sqlite_database()

        # Track background tasks so we can await them before performing
        # subsequent operations that rely on the row already existing or
        # before we close the connection pool.  The mapping is
        #   run_id -> asyncio.Task
        self._pending_tasks: dict[str, asyncio.Task] = {}

        # Guard to serialize Postgres interactions coming from this RunManager
        # instance.  Even though we rely on a connection pool, we have seen
        # situations where two coroutines end up sharing the same connection
        # object concurrently, triggering asyncpg's
        # "cannot perform operation: another operation is in progress" error.
        # A single lock per RunManager is sufficient because the traffic
        # generated by a single pipeline (one global RunManager) is low and
        # the critical sections are only a single simple statement each.
        self._db_lock = asyncio.Lock()

        # Track whether we own the postgres pool (and should close it)
        self._owns_pool = False

    def _init_sqlite_database(self):
        """Initialize SQLite database for run tracking"""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY,
                    run_type TEXT NOT NULL,
                    system_name TEXT NOT NULL,
                    description TEXT NOT NULL,
                    parameters TEXT DEFAULT '{}',
                    data_types TEXT DEFAULT '[]',
                    expected_duration_minutes INTEGER,
                    priority INTEGER DEFAULT 1,
                    tags TEXT DEFAULT '[]',
                    created_by TEXT DEFAULT 'system',
                    status TEXT DEFAULT 'pending',
                    started_at TEXT,
                    completed_at TEXT,
                    error_message TEXT,
                    progress_data TEXT DEFAULT '{}',
                    output_summary TEXT DEFAULT '{}',
                    resource_usage TEXT DEFAULT '{}',
                    dependencies TEXT DEFAULT '[]',
                    blocked_by TEXT DEFAULT '[]',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
            """)

            conn.execute("""
                CREATE TABLE IF NOT EXISTS run_logs (
                    log_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    level TEXT NOT NULL,
                    message TEXT NOT NULL,
                    component TEXT,
                    data_type TEXT,
                    extra_data TEXT DEFAULT '{}',
                    FOREIGN KEY(run_id) REFERENCES runs(run_id)
                )
            """)

            conn.execute("""
                CREATE TABLE IF NOT EXISTS active_locks (
                    lock_name TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL,
                    locked_at TEXT NOT NULL,
                    expires_at TEXT,
                    lock_data TEXT DEFAULT '{}',
                    FOREIGN KEY(run_id) REFERENCES runs(run_id)
                )
            """)

            # Create indexes
            conn.execute("CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_runs_type ON runs(run_type)")
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_runs_system ON runs(system_name)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_runs_created_at ON runs(created_at)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_run_logs_run_id ON run_logs(run_id)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_run_logs_timestamp ON run_logs(timestamp)"
            )

    async def _init_postgres_database(self):
        """Initialize PostgreSQL database for run tracking"""
        async with self._db_lock, self.postgres_pool.acquire() as conn:
            await conn.execute("""
                CREATE SCHEMA IF NOT EXISTS bicam_runs;
            """)

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS bicam_runs.runs (
                    run_id TEXT PRIMARY KEY,
                    run_type TEXT NOT NULL,
                    system_name TEXT NOT NULL,
                    description TEXT NOT NULL,
                    parameters JSONB DEFAULT '{}',
                    data_types TEXT[] DEFAULT '{}',
                    expected_duration_minutes INTEGER,
                    priority INTEGER DEFAULT 1,
                    tags TEXT[] DEFAULT '{}',
                    created_by TEXT DEFAULT 'system',
                    status TEXT DEFAULT 'pending',
                    started_at TIMESTAMP WITH TIME ZONE,
                    completed_at TIMESTAMP WITH TIME ZONE,
                    error_message TEXT,
                    progress_data JSONB DEFAULT '{}',
                    output_summary JSONB DEFAULT '{}',
                    resource_usage JSONB DEFAULT '{}',
                    dependencies TEXT[] DEFAULT '{}',
                    blocked_by TEXT[] DEFAULT '{}',
                    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
                    updated_at TIMESTAMP WITH TIME ZONE NOT NULL
                );
            """)

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS bicam_runs.run_logs (
                    log_id BIGSERIAL PRIMARY KEY,
                    run_id TEXT NOT NULL,
                    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                    level TEXT NOT NULL,
                    message TEXT NOT NULL,
                    component TEXT,
                    data_type TEXT,
                    extra_data JSONB DEFAULT '{}',
                    FOREIGN KEY(run_id) REFERENCES bicam_runs.runs(run_id)
                );
            """)

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS bicam_runs.active_locks (
                    lock_name TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL,
                    locked_at TIMESTAMP WITH TIME ZONE NOT NULL,
                    expires_at TIMESTAMP WITH TIME ZONE,
                    lock_data JSONB DEFAULT '{}',
                    FOREIGN KEY(run_id) REFERENCES bicam_runs.runs(run_id)
                );
            """)

            # Create indexes
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_runs_status ON bicam_runs.runs(status)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_runs_type ON bicam_runs.runs(run_type)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_runs_system ON bicam_runs.runs(system_name)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_runs_created_at ON bicam_runs.runs(created_at)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_run_logs_run_id ON bicam_runs.run_logs(run_id)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_run_logs_timestamp ON bicam_runs.run_logs(timestamp)"
            )

    async def initialize(self):
        """Initialize the run manager"""
        if self.use_postgres and not self.postgres_pool:
            self.postgres_pool = await asyncpg.create_pool(**self.postgres_config)
            self._owns_pool = True  # Track if we created the pool
            await self._init_postgres_database()
        elif self.postgres_pool:
            self._owns_pool = False  # External pool, don't close it
            await self._init_postgres_database()

    async def cleanup(self):
        """Clean up resources"""
        # First, wait for any still-running background tasks to finish so we
        # don't terminate the pool while they are using a connection.
        if self._pending_tasks:
            try:
                # Check if we have a running event loop
                try:
                    asyncio.get_running_loop()
                    await asyncio.gather(
                        *self._pending_tasks.values(), return_exceptions=True
                    )
                except RuntimeError:
                    # Event loop is closed, can't await tasks
                    logger.debug(
                        "Event loop closed, cannot await pending tasks during cleanup"
                    )
            except Exception as e:
                logger.warning(
                    f"Error awaiting pending run save tasks during cleanup: {e}"
                )

        # Only close the pool if we created it ourselves and have a running event loop
        if self.postgres_pool and self._owns_pool:
            try:
                # Check if we have a running event loop
                asyncio.get_running_loop()
                await self.postgres_pool.close()
                logger.debug("RunManager closed its own database pool")
            except RuntimeError:
                # Event loop is closed, force terminate the pool
                logger.debug("Event loop closed, cannot gracefully close pool")
                try:
                    self.postgres_pool.terminate()
                except Exception as e:
                    logger.debug(f"Error terminating pool: {e}")
            except Exception as e:
                logger.warning(f"Error closing database pool: {e}")
        elif self.postgres_pool:
            logger.debug("RunManager keeping external database pool open")

    def create_run(self, metadata: RunMetadata) -> str:
        """Create a new run and return its ID"""
        if not metadata.run_id:
            metadata.run_id = str(uuid.uuid4())

        record = RunRecord(metadata=metadata)

        if self.use_postgres:
            # Persist the record in the background but keep track of the task.
            # We will await it later from update_run_status or cleanup to avoid
            # overlapping commands on the same connection which previously
            # manifested as "another operation is in progress" errors.
            loop = asyncio.get_running_loop()
            task = loop.create_task(self._save_run_postgres(record))
            self._pending_tasks[metadata.run_id] = task

            def _remove_task(_):
                # Remove completed tasks to avoid memory leaks
                self._pending_tasks.pop(metadata.run_id, None)

            task.add_done_callback(_remove_task)
        else:
            self._save_run_sqlite(record)

        logger.info(f"Created run {metadata.run_id}: {metadata.description}")
        return metadata.run_id

    def _save_run_sqlite(self, record: RunRecord):
        """Save run to SQLite"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO runs (
                    run_id, run_type, system_name, description, parameters, data_types,
                    expected_duration_minutes, priority, tags, created_by, status,
                    started_at, completed_at, error_message, progress_data, output_summary,
                    resource_usage, dependencies, blocked_by, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    record.metadata.run_id,
                    record.metadata.run_type.value,
                    record.metadata.system_name,
                    record.metadata.description,
                    safe_json_dumps(record.metadata.parameters),
                    safe_json_dumps(record.metadata.data_types),
                    record.metadata.expected_duration_minutes,
                    record.metadata.priority,
                    safe_json_dumps(record.metadata.tags),
                    record.metadata.created_by,
                    record.status.value,
                    record.started_at.isoformat() if record.started_at else None,
                    record.completed_at.isoformat() if record.completed_at else None,
                    record.error_message,
                    safe_json_dumps(record.progress_data),
                    safe_json_dumps(record.output_summary),
                    safe_json_dumps(record.resource_usage),
                    safe_json_dumps(record.dependencies),
                    safe_json_dumps(record.blocked_by),
                    record.created_at.isoformat(),
                    record.updated_at.isoformat(),
                ),
            )

    async def _save_run_postgres(self, record: RunRecord):
        """Save run to PostgreSQL"""
        async with self._db_lock, self.postgres_pool.acquire() as conn:
            await conn.execute(
                """
                    INSERT INTO bicam_runs.runs (
                        run_id, run_type, system_name, description, parameters, data_types,
                        expected_duration_minutes, priority, tags, created_by, status,
                        started_at, completed_at, error_message, progress_data, output_summary,
                        resource_usage, dependencies, blocked_by, created_at, updated_at
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)
                    ON CONFLICT (run_id) DO UPDATE SET
                        status = EXCLUDED.status,
                        started_at = EXCLUDED.started_at,
                        completed_at = EXCLUDED.completed_at,
                        error_message = EXCLUDED.error_message,
                        progress_data = EXCLUDED.progress_data,
                        output_summary = EXCLUDED.output_summary,
                        resource_usage = EXCLUDED.resource_usage,
                        updated_at = EXCLUDED.updated_at
                """,
                record.metadata.run_id,
                record.metadata.run_type.value,
                record.metadata.system_name,
                record.metadata.description,
                safe_json_dumps(record.metadata.parameters),
                record.metadata.data_types,
                record.metadata.expected_duration_minutes,
                record.metadata.priority,
                record.metadata.tags,
                record.metadata.created_by,
                record.status.value,
                record.started_at,
                record.completed_at,
                record.error_message,
                safe_json_dumps(record.progress_data),
                safe_json_dumps(record.output_summary),
                safe_json_dumps(record.resource_usage),
                record.dependencies,
                record.blocked_by,
                record.created_at,
                record.updated_at,
            )

    async def start_run(self, run_id: str) -> bool:
        """Mark a run as started"""
        return await self.update_run_status(
            run_id, RunStatus.RUNNING, started_at=datetime.now(timezone.utc)
        )

    async def complete_run(
        self, run_id: str, output_summary: dict[str, Any] | None = None
    ) -> bool:
        """Mark a run as completed"""
        return await self.update_run_status(
            run_id,
            RunStatus.COMPLETED,
            completed_at=datetime.now(timezone.utc),
            output_summary=output_summary or {},
        )

    async def fail_run(self, run_id: str, error_message: str) -> bool:
        """Mark a run as failed"""
        return await self.update_run_status(
            run_id,
            RunStatus.FAILED,
            completed_at=datetime.now(timezone.utc),
            error_message=error_message,
        )

    async def update_run_status(self, run_id: str, status: RunStatus, **kwargs) -> bool:
        """Update run status and other fields"""
        # Ensure any pending INSERT task for this run has completed before we
        # attempt to UPDATE the same row.  This prevents concurrent usage of
        # the same connection which resulted in asyncpg raising
        # "cannot perform operation: another operation is in progress".
        pending = self._pending_tasks.get(run_id)
        if pending and not pending.done():
            try:
                await pending
            except Exception as e:
                # Log but continue so the caller can decide how to handle it
                logger.warning(f"Background create_run task failed for {run_id}: {e}")

        if self.use_postgres:
            return await self._update_run_postgres(run_id, status, **kwargs)
        else:
            return self._update_run_sqlite(run_id, status, **kwargs)

    def _update_run_sqlite(self, run_id: str, status: RunStatus, **kwargs) -> bool:
        """Update run in SQLite"""
        updates = ["status = ?", "updated_at = ?"]
        values = [status.value, datetime.now(timezone.utc).isoformat()]

        for key, value in kwargs.items():
            if key in ["started_at", "completed_at"] and value:
                updates.append(f"{key} = ?")
                values.append(value.isoformat())
            elif key in ["error_message"]:
                updates.append(f"{key} = ?")
                values.append(value)
            elif key in ["progress_data", "output_summary", "resource_usage"]:
                updates.append(f"{key} = ?")
                values.append(safe_json_dumps(value))

        values.append(run_id)

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                f"""
                UPDATE runs SET {", ".join(updates)} WHERE run_id = ?
            """,
                values,
            )
            return cursor.rowcount > 0

    async def _update_run_postgres(
        self, run_id: str, status: RunStatus, **kwargs
    ) -> bool:
        """Update run in PostgreSQL with robust retry logic"""
        updates = ["status = $2", "updated_at = $3"]
        values = [run_id, status.value, datetime.now(timezone.utc)]
        param_count = 3

        for key, value in kwargs.items():
            param_count += 1
            if key in ["started_at", "completed_at", "error_message"]:
                updates.append(f"{key} = ${param_count}")
                values.append(value)
            elif key in ["progress_data", "output_summary", "resource_usage"]:
                updates.append(f"{key} = ${param_count}")
                values.append(safe_json_dumps(value))

        # Use retry logic for database operations
        max_retries = 3
        base_delay = 0.1

        for attempt in range(max_retries):
            try:
                async with self._db_lock:
                    # Add a small delay to reduce concurrent access conflicts
                    if attempt > 0:
                        await asyncio.sleep(base_delay * (2**attempt))

                    async with self.postgres_pool.acquire() as conn:
                        result = await conn.execute(
                            f"""
                            UPDATE bicam_runs.runs SET {", ".join(updates)} WHERE run_id = $1
                        """,
                            *values,
                        )
                        return result.split()[-1] != "0"

            except Exception as e:
                if (
                    "another operation is in progress" in str(e).lower()
                    and attempt < max_retries - 1
                ):
                    # This is the specific error we're trying to handle
                    logger.debug(
                        f"Database operation conflict on attempt {attempt + 1}, retrying..."
                    )
                    await asyncio.sleep(base_delay * (2**attempt))
                    continue
                elif attempt < max_retries - 1:
                    # Other errors, still retry
                    logger.warning(
                        f"Database operation failed on attempt {attempt + 1}: {e}"
                    )
                    await asyncio.sleep(base_delay * (2**attempt))
                    continue
                else:
                    # Final attempt failed
                    logger.error(
                        f"Failed to update run status after {max_retries} attempts: {e}"
                    )
                    raise

        return False

    async def log_message(
        self,
        run_id: str,
        level: str,
        message: str,
        component: str | None = None,
        data_type: str | None = None,
        extra_data: dict[str, Any] | None = None,
    ):
        """Log a message for a run"""
        if self.use_postgres:
            await self._log_message_postgres(
                run_id, level, message, component, data_type, extra_data
            )
        else:
            self._log_message_sqlite(
                run_id, level, message, component, data_type, extra_data
            )

    def _log_message_sqlite(
        self,
        run_id: str,
        level: str,
        message: str,
        component: str | None = None,
        data_type: str | None = None,
        extra_data: dict[str, Any] | None = None,
    ):
        """Log message to SQLite"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO run_logs (run_id, timestamp, level, message, component, data_type, extra_data)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    run_id,
                    datetime.now(timezone.utc).isoformat(),
                    level,
                    message,
                    component,
                    data_type,
                    safe_json_dumps(extra_data or {}),
                ),
            )

    async def _log_message_postgres(
        self,
        run_id: str,
        level: str,
        message: str,
        component: str | None = None,
        data_type: str | None = None,
        extra_data: dict[str, Any] | None = None,
    ):
        """Log message to PostgreSQL"""
        async with self._db_lock, self.postgres_pool.acquire() as conn:
            await conn.execute(
                """
                    INSERT INTO bicam_runs.run_logs (run_id, timestamp, level, message, component, data_type, extra_data)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                """,
                run_id,
                datetime.now(timezone.utc),
                level,
                message,
                component,
                data_type,
                extra_data or {},
            )

    async def get_active_runs(self, system_name: str | None = None) -> list[RunRecord]:
        """Get all currently active (running or pending) runs"""
        if self.use_postgres:
            return await self._get_active_runs_postgres(system_name)
        else:
            return self._get_active_runs_sqlite(system_name)

    def _get_active_runs_sqlite(
        self, system_name: str | None = None
    ) -> list[RunRecord]:
        """Get active runs from SQLite"""
        query = "SELECT * FROM runs WHERE status IN ('pending', 'running', 'paused')"
        params = []

        if system_name:
            query += " AND system_name = ?"
            params.append(system_name)

        query += " ORDER BY priority DESC, created_at ASC"

        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(query, params).fetchall()
            return [self._row_to_run_record(row) for row in rows]

    async def _get_active_runs_postgres(
        self, system_name: str | None = None
    ) -> list[RunRecord]:
        """Get active runs from PostgreSQL"""
        query = "SELECT * FROM bicam_runs.runs WHERE status IN ('pending', 'running', 'paused')"
        params = []

        if system_name:
            query += " AND system_name = $1"
            params.append(system_name)

        query += " ORDER BY priority DESC, created_at ASC"

        async with self.postgres_pool.acquire() as conn:
            rows = await conn.fetch(query, *params)
            return [self._row_to_run_record(dict(row)) for row in rows]

    def _row_to_run_record(self, row) -> RunRecord:
        """Convert database row to RunRecord"""
        if isinstance(row, sqlite3.Row):
            row = dict(row)

        # Handle JSON fields based on database type
        if self.use_postgres:
            parameters = row["parameters"]
            data_types = row["data_types"]
            tags = row["tags"]
            progress_data = row["progress_data"]
            output_summary = row["output_summary"]
            resource_usage = row["resource_usage"]
            dependencies = row["dependencies"]
            blocked_by = row["blocked_by"]
        else:
            parameters = json.loads(row["parameters"])
            data_types = json.loads(row["data_types"])
            tags = json.loads(row["tags"])
            progress_data = json.loads(row["progress_data"])
            output_summary = json.loads(row["output_summary"])
            resource_usage = json.loads(row["resource_usage"])
            dependencies = json.loads(row["dependencies"])
            blocked_by = json.loads(row["blocked_by"])

        metadata = RunMetadata(
            run_id=row["run_id"],
            run_type=RunType(row["run_type"]),
            system_name=row["system_name"],
            description=row["description"],
            parameters=parameters,
            data_types=data_types,
            expected_duration_minutes=row["expected_duration_minutes"],
            priority=row["priority"],
            tags=tags,
            created_by=row["created_by"],
        )

        # Parse datetime fields
        started_at = None
        completed_at = None
        created_at = datetime.now(timezone.utc)
        updated_at = datetime.now(timezone.utc)

        if row["started_at"]:
            started_at = (
                datetime.fromisoformat(row["started_at"].replace("Z", "+00:00"))
                if isinstance(row["started_at"], str)
                else row["started_at"]
            )
        if row["completed_at"]:
            completed_at = (
                datetime.fromisoformat(row["completed_at"].replace("Z", "+00:00"))
                if isinstance(row["completed_at"], str)
                else row["completed_at"]
            )
        if row["created_at"]:
            created_at = (
                datetime.fromisoformat(row["created_at"].replace("Z", "+00:00"))
                if isinstance(row["created_at"], str)
                else row["created_at"]
            )
        if row["updated_at"]:
            updated_at = (
                datetime.fromisoformat(row["updated_at"].replace("Z", "+00:00"))
                if isinstance(row["updated_at"], str)
                else row["updated_at"]
            )

        return RunRecord(
            metadata=metadata,
            status=RunStatus(row["status"]),
            started_at=started_at,
            completed_at=completed_at,
            error_message=row["error_message"],
            progress_data=progress_data,
            output_summary=output_summary,
            resource_usage=resource_usage,
            dependencies=dependencies,
            blocked_by=blocked_by,
            created_at=created_at,
            updated_at=updated_at,
        )

    async def check_conflicts(self, metadata: RunMetadata) -> list[str]:
        """Check if a run would conflict with active runs"""
        active_runs = await self.get_active_runs()
        conflicts = []

        for run in active_runs:
            # Check if same system is already running
            if run.metadata.system_name == metadata.system_name:
                conflicts.append(
                    f"System {metadata.system_name} is already running (run_id: {run.metadata.run_id})"
                )

            # Check for data type conflicts
            if set(metadata.data_types) & set(run.metadata.data_types):
                overlapping = set(metadata.data_types) & set(run.metadata.data_types)
                conflicts.append(
                    f"Data types {overlapping} are already being processed (run_id: {run.metadata.run_id})"
                )

        return conflicts

    async def acquire_lock(
        self, run_id: str, lock_name: str, expires_minutes: int | None = None
    ) -> bool:
        """Acquire a named lock for coordination"""
        expires_at = None
        if expires_minutes:
            expires_at = datetime.now(timezone.utc) + timedelta(minutes=expires_minutes)

        try:
            if self.use_postgres:
                async with self.postgres_pool.acquire() as conn:
                    await conn.execute(
                        """
                        INSERT INTO bicam_runs.active_locks (lock_name, run_id, locked_at, expires_at)
                        VALUES ($1, $2, $3, $4)
                    """,
                        lock_name,
                        run_id,
                        datetime.now(timezone.utc),
                        expires_at,
                    )
            else:
                with sqlite3.connect(self.db_path) as conn:
                    conn.execute(
                        """
                        INSERT INTO active_locks (lock_name, run_id, locked_at, expires_at)
                        VALUES (?, ?, ?, ?)
                    """,
                        (
                            lock_name,
                            run_id,
                            datetime.now(timezone.utc).isoformat(),
                            expires_at.isoformat() if expires_at else None,
                        ),
                    )
            return True
        except Exception as e:
            logger.error(f"Error acquiring lock: {e}")
            return False

    async def release_lock(self, run_id: str, lock_name: str) -> bool:
        """Release a named lock"""
        try:
            if self.use_postgres:
                async with self.postgres_pool.acquire() as conn:
                    result = await conn.execute(
                        """
                        DELETE FROM bicam_runs.active_locks WHERE lock_name = $1 AND run_id = $2
                    """,
                        lock_name,
                        run_id,
                    )
                    return result.split()[-1] != "0"
            else:
                with sqlite3.connect(self.db_path) as conn:
                    cursor = conn.execute(
                        """
                        DELETE FROM active_locks WHERE lock_name = ? AND run_id = ?
                    """,
                        (lock_name, run_id),
                    )
                    return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Error releasing lock: {e}")
            return False

    def get_run_summary(self, hours: int = 24) -> dict[str, Any]:
        """Get summary of runs in the last N hours"""
        # This would be implemented to provide dashboard-like info
        # For now, return a placeholder
        return {
            "total_runs": 0,
            "active_runs": 0,
            "completed_runs": 0,
            "failed_runs": 0,
            "by_system": {},
            "by_status": {},
        }


# Context manager for run tracking
class RunContext:
    """Context manager for tracking a run"""

    def __init__(self, run_manager: RunManager, metadata: RunMetadata):
        self.run_manager = run_manager
        self.metadata = metadata
        self.run_id = None

    async def __aenter__(self):
        self.run_id = self.run_manager.create_run(self.metadata)
        await self.run_manager.start_run(self.run_id)
        return self.run_id

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            await self.run_manager.fail_run(self.run_id, str(exc_val))
        else:
            await self.run_manager.complete_run(self.run_id)


# Global run manager instance
_global_run_manager: RunManager | None = None


def get_run_manager() -> RunManager:
    """Get the global run manager instance"""
    global _global_run_manager
    if _global_run_manager is None:
        _global_run_manager = RunManager()
    return _global_run_manager


async def init_run_manager(use_postgres: bool = True):
    """Initialize the global run manager"""
    global _global_run_manager
    _global_run_manager = RunManager(use_postgres=use_postgres)
    await _global_run_manager.initialize()
