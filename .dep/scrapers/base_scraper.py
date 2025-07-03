import asyncio
import csv
import json
import logging
import os
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

import aiofiles
import aiohttp
import asyncpg
import yaml
from dotenv import load_dotenv

# Import the enhanced checkpoint system
from bicam_collection.lib.checkpoint import (
    CheckpointManager,
    HierarchicalProgressTracker,
    ProcessingPhase,
)

# Import raw_key_builders
from bicam_collection.scrapers import raw_key_builders

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
# The main CLI installs its own logging/structlog configuration before our
# code runs, so subsequent ``basicConfig`` calls are ignored.  To guarantee
# that DEBUG-level diagnostics (like endpoint extraction) are visible we bump
# this module's logger level explicitly.  It still respects any handlers the
# application configured globally.

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@dataclass
class ProcessingConfig:
    """Configuration for processing a specific data type."""

    data_type: str
    main: bool = False
    fields: list[str] = field(default_factory=list)
    id_fields: list[str] = field(default_factory=list)
    nested_fields: list[str] = field(default_factory=list)
    related_fields: list[str] = field(default_factory=list)


class APIKeyManager:
    """Manages API key rotation and rate limiting."""

    def __init__(self, api_keys: list[str]):
        self.all_keys = api_keys
        self.key_status = {
            key: {"sleeping": False, "sleep_until": None} for key in api_keys
        }
        self.available_keys = api_keys.copy()

    def get_active_keys(self) -> list[str]:
        """Get list of currently active API keys."""
        now = datetime.now(UTC)
        active_keys = []

        for key, status in self.key_status.items():
            if not status["sleeping"] or (
                status["sleep_until"] and status["sleep_until"] <= now
            ):
                active_keys.append(key)
                if status["sleeping"]:
                    self.key_status[key]["sleeping"] = False
                    self.key_status[key]["sleep_until"] = None

        return active_keys

    def put_key_to_sleep(self, api_key: str, sleep_duration: int):
        """Put an API key to sleep for specified duration."""
        self.key_status[api_key]["sleeping"] = True
        self.key_status[api_key]["sleep_until"] = datetime.now(UTC) + timedelta(
            seconds=sleep_duration
        )

    def assign_keys(self, data_type: str, count: int = 2) -> list[str]:
        """Assign API keys to a data type."""
        assigned = self.available_keys[:count]
        self.available_keys = self.available_keys[count:]
        return assigned

    def release_keys(self, keys: list[str]):
        """Release keys back to available pool."""
        self.available_keys.extend(keys)

    def redistribute_key(self, data_type_clients: dict) -> bool:
        """Redistribute an available key to active data types."""
        if not self.available_keys:
            return False

        # Find data type with fewest keys
        min_keys = float("inf")
        target_data_type = None

        for dt, client in data_type_clients.items():
            key_count = len(client._adapter.api_keys)
            if key_count < min_keys:
                min_keys = key_count
                target_data_type = dt

        if target_data_type:
            extra_key = self.available_keys.pop(0)
            data_type_clients[target_data_type]._adapter.api_keys.append(extra_key)
            logger.info(f"Redistributed key to {target_data_type}")
            return True

        return False


class BaseDatabaseConnection(ABC):
    """Base database connection manager with common functionality."""

    def __init__(self, env_path: str = ".env"):
        load_dotenv(env_path)
        # Provide sensible fall-backs to the usual libpq variable names so that
        # the scraper works out-of-the-box for most Postgres setups.
        self.config: dict[str, str | int | None] = {
            "database": (
                os.getenv("POSTGRESQL_DATABASE")
                or os.getenv("PGDATABASE")
                or os.getenv("POSTGRES_DB")
            ),
            "user": (
                os.getenv("POSTGRESQL_USERNAME")
                or os.getenv("PGUSER")
                or os.getenv("POSTGRES_USER")
            ),
            "password": (
                os.getenv("POSTGRESQL_PASSWORD")
                or os.getenv("PGPASSWORD")
                or os.getenv("POSTGRES_PASSWORD")
            ),
            "host": (
                os.getenv("POSTGRESQL_HOST") or os.getenv("PGHOST") or "localhost"
            ),
            "port": int(os.getenv("POSTGRESQL_PORT", os.getenv("PGPORT", "5432"))),
        }

        # Fail fast with a helpful error if we still lack credentials.
        missing = [
            k
            for k, v in self.config.items()
            if v in (None, "") and k in ("database", "user", "password")
        ]
        if missing:
            missing_str = ", ".join(missing)
            raise RuntimeError(
                f"Database connection configuration incomplete. Missing: {missing_str}. "
                "Set the variables in your --env file or shell before running the scraper."
            )
        self.pool = None

    async def connect(self):
        """Create database connection pool."""
        self.pool = await asyncpg.create_pool(**self.config)

    async def disconnect(self):
        """Close database connection pool."""
        if self.pool:
            await self.pool.close()

    @abstractmethod
    async def access_last_processed_date(self, data_type: str) -> str | None:
        """Get the last processed date for a data type."""

    @abstractmethod
    async def update_last_processed_date(self, data_type: str, date: str):
        """Update the last processed date for a data type."""

    @abstractmethod
    async def log_error(self, url: str, error: str, data_type: str):
        """Log an error to the database."""


# Legacy ProgressTracker - now just wraps HierarchicalProgressTracker for compatibility
class ProgressTracker:
    """Legacy progress tracker - provides backward compatibility"""

    def __init__(
        self, db_path: str = "scraper_progress.db", scraper_type: str = "base"
    ):
        # Initialize the enhanced checkpoint manager
        self.checkpoint_manager = CheckpointManager(
            db_path.replace(".db", "_checkpoints.db")
        )
        self._trackers = {}  # Store trackers by data type
        self.scraper_type = scraper_type

    def _get_tracker(self, data_type: str) -> HierarchicalProgressTracker:
        """Get or create a hierarchical tracker for a data type"""
        if data_type not in self._trackers:
            self._trackers[data_type] = HierarchicalProgressTracker(
                self.checkpoint_manager, self.scraper_type, data_type
            )
        return self._trackers[data_type]


class DataExporter:
    """Handles data export to CSV files or directly into Postgres raw-JSON tables."""

    def __init__(
        self,
        output_dir: str | None,
        config: dict[str, ProcessingConfig],
        db_conn: BaseDatabaseConnection | None = None,
    ):
        # When *output_dir* is None writing CSVs is disabled.
        self.enabled = bool(output_dir)
        self.output_dir = output_dir if output_dir else ""
        self.config = config
        self.db_conn = db_conn  # Allows direct DB inserts when provided

        if self.enabled:
            os.makedirs(self.output_dir, exist_ok=True)
        self.field_cache: dict[str, list[str]] = {}

    def get_field_order(self, data_type: str, sample_data: list[dict]) -> list[str]:
        """Determine the field order for CSV export."""
        if data_type in self.field_cache:
            return self.field_cache[data_type]

        config = self.config.get(data_type)
        if not config:
            # Use all fields from sample data
            all_fields = set()
            for item in sample_data:
                all_fields.update(item.keys())
            self.field_cache[data_type] = sorted(all_fields)
            return self.field_cache[data_type]

        # Use configured field order
        ordered_fields = config.id_fields.copy()
        ordered_fields.extend([f for f in config.fields if f not in config.id_fields])

        # Add any additional fields from data
        if sample_data:
            all_fields = set()
            for item in sample_data:
                all_fields.update(item.keys())
            ordered_fields.extend(
                [f for f in sorted(all_fields) if f not in ordered_fields]
            )

        self.field_cache[data_type] = ordered_fields
        return ordered_fields

    async def export_batch(self, data_type: str, items: list[dict[str, Any]]):
        """Export a batch of items to CSV."""
        if not self.enabled or not items:
            return

        file_path = os.path.join(self.output_dir, f"{data_type}.csv")
        file_exists = os.path.exists(file_path)

        field_order = self.get_field_order(data_type, items)

        # Process items for consistent structure
        processed_items = []
        for item in items:
            processed_item = {}
            for f in field_order:
                value = item.get(f)
                if isinstance(value, (list, dict)):

                    def _safe_default(obj):
                        if isinstance(obj, (str, int, float, bool)) or obj is None:
                            return obj
                        if hasattr(obj, "dict"):
                            return obj.dict()
                        if hasattr(obj, "data"):
                            return obj.data
                        if hasattr(obj, "__dict__"):
                            return obj.__dict__
                        return str(obj)

                    processed_item[f] = (
                        json.dumps(value, default=_safe_default) if value else None
                    )
                elif value == "":
                    processed_item[f] = None
                else:
                    processed_item[f] = value
            processed_items.append(processed_item)

        async with aiofiles.open(
            file_path, "a" if file_exists else "w", newline=""
        ) as f:
            writer = csv.DictWriter(f, fieldnames=field_order, extrasaction="ignore")
            if not file_exists:
                await writer.writeheader()

            for item in processed_items:
                await writer.writerow(item)

        logger.info(f"Exported {len(items)} items to {data_type}.csv")

    # ------------------------------------------------------------------
    # RAW-JSONB WRITE SUPPORT
    # ------------------------------------------------------------------
    async def export_raw_jsonb(
        self,
        schema: str,
        table: str,
        items: list[dict[str, Any]],
        *,
        id_field: str,
        url_field: str | None = None,
        batch_id: str | None = None,
    ) -> None:
        """Insert raw payloads into a schematised jsonb staging table.

        If the target table doesn't exist, it is created on-the-fly with a
        generic structure:

            id            TEXT        PRIMARY KEY
            fetched_at    TIMESTAMPTZ NOT NULL
            payload       JSONB       NOT NULL
            endpoint    TEXT
            etl_batch_id  UUID

        Parameters
        ----------
        schema / table:
            Destination identifiers.
        items:
            List of raw JSON dictionaries *after* PyCon HTTP retrieval.
        id_field:
            Field within each dict to use as the primary key.  Rows where
            the field is missing/null are skipped.
        url_field:
            Optional field that holds the canonical URL; best-effort only.
        batch_id:
            Optional UUID string to tag this load; if omitted, one is
            generated per call.
        """

        if not self.enabled and not self.db_conn:
            # nothing to do
            return

        if self.db_conn:
            import uuid as _uuid

            batch_id = batch_id or str(_uuid.uuid4())

            schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema};"

            # Raw layer gets a surrogate UUID PK; optional source_doc_id stores the natural key when available.
            table_sql = f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table} (
                    surrogate_id UUID PRIMARY KEY,
                    fetched_at   TIMESTAMPTZ NOT NULL,
                    payload      JSONB       NOT NULL,
                    endpoint     TEXT,
                    source_doc_id TEXT,
                    etl_batch_id UUID
                );
            """

            async with self.db_conn.pool.acquire() as conn:
                # Ensure schema & table exist (cheap with IF NOT EXISTS)
                await conn.execute(schema_sql)
                await conn.execute(table_sql)

                records = []
                now = datetime.now(UTC)

                # JSON encoder that handles misc objects without leaking large
                # internal state – reused across all items below.
                def _safe_default(obj):  # noqa: D401 – simple helper
                    if isinstance(obj, (str, int, float, bool)) or obj is None:
                        return obj
                    if hasattr(obj, "dict"):
                        return obj.dict()
                    if hasattr(obj, "data"):
                        return obj.data
                    if hasattr(obj, "__dict__"):
                        return obj.__dict__
                    return str(obj)

                # Helper to normalise keys for soft matching (case/underscore insensitive)
                def _norm(s: str | None) -> str:
                    return str(s).lower().replace("_", "") if s is not None else ""

                for itm in items:
                    if not isinstance(itm, dict):
                        continue  # Skip non-dict items defensively

                    # Derive surrogate UUID primary key for raw table
                    surrogate_id = str(_uuid.uuid4())

                    source_doc_id = None
                    if id_field:
                        # best-effort extraction of natural key for reference
                        pk_val = itm.get(id_field)
                        if pk_val in (None, ""):
                            # tolerant: try insensitive + nested as earlier
                            for k, v in itm.items():
                                if _norm(k) == _norm(id_field):
                                    pk_val = v
                                    break
                            if pk_val in (None, ""):
                                for v in itm.values():
                                    if isinstance(v, dict) and id_field in v:
                                        pk_val = v[id_field]
                                        break
                        if pk_val not in (None, ""):
                            source_doc_id = str(pk_val)

                    # Fallback: derive from registered builder if still missing
                    if source_doc_id is None:
                        parent_type = table.split("_")[0] if "_" in table else table
                        source_doc_id = raw_key_builders.build(parent_type, itm)

                    # ------------------------------------------------------------------
                    # Derive endpoint (clean URL without API key) – this will be stored
                    # in `endpoint` and also added to the raw JSON under the key
                    # `endpoint` so downstream consumers can rely on a consistent name.
                    # ------------------------------------------------------------------

                    src_url = itm.get(url_field) if url_field else None

                    endpoint_url = None  # populated if URL present
                    if not src_url:
                        logger.debug(
                            "export_raw_jsonb: no URL field in payload for table %s; id=%s",
                            table,
                            source_doc_id,
                        )
                    if src_url:
                        # Strip any query-string api_key so we don't leak secrets.
                        import urllib.parse as _urlparse

                        parsed = _urlparse.urlsplit(src_url)
                        qs = _urlparse.parse_qsl(parsed.query, keep_blank_values=True)
                        qs_clean = [(k, v) for k, v in qs if k.lower() != "api_key"]
                        cleaned_query = _urlparse.urlencode(qs_clean)
                        endpoint_url = _urlparse.urlunsplit(
                            parsed._replace(query=cleaned_query)
                        )

                        logger.debug(
                            "export_raw_jsonb: derived endpoint %s for table %s id=%s",
                            endpoint_url,
                            table,
                            source_doc_id,
                        )

                        # Add/replace endpoint field in payload dict (does not mutate
                        # original PyCongress .data because we work on our local copy).
                        itm["endpoint"] = endpoint_url
                    else:
                        logger.debug(
                            "export_raw_jsonb: unable to derive endpoint for table %s; id=%s",
                            table,
                            source_doc_id,
                        )

                    records.append(
                        (
                            _uuid.UUID(surrogate_id),
                            now,
                            json.dumps(itm, default=_safe_default),
                            endpoint_url,
                            source_doc_id,
                            _uuid.UUID(batch_id),
                        )
                    )

                if not records:
                    return

                insert_sql = f"""
                    INSERT INTO {schema}.{table} (surrogate_id, fetched_at, payload, endpoint, source_doc_id, etl_batch_id)
                    VALUES ($1, $2, $3::jsonb, $4, $5, $6)
                    ON CONFLICT (surrogate_id) DO NOTHING;
                """

                await conn.executemany(insert_sql, records)


class ActivityMonitor:
    """Monitor processing activity and detect timeouts."""

    def __init__(self, timeout_minutes: int = 60):
        self.last_activity = datetime.now(UTC)
        self.timeout_duration = timedelta(minutes=timeout_minutes)
        self.sleep_start = None

    def update_activity(self):
        """Record activity."""
        self.last_activity = datetime.now(UTC)

    def start_sleep(self):
        """Mark start of sleep period."""
        if not self.sleep_start:
            self.sleep_start = datetime.now(UTC)

    def end_sleep(self):
        """Mark end of sleep period."""
        self.sleep_start = None

    def is_timed_out(self) -> bool:
        """Check if processing has timed out."""
        if self.sleep_start:
            return False  # Don't count sleep time
        return datetime.now(UTC) - self.last_activity > self.timeout_duration


class DataTransformer:
    """Handles data transformation."""

    @staticmethod
    def transform_item(item: Any, fields: list[str] = None) -> dict[str, Any]:
        """Transform a single item to dictionary format."""
        if isinstance(item, str | int | float | bool):
            return {"value": item}
        elif isinstance(item, dict):
            return item

        result = {}
        attributes = (
            fields
            if fields
            else [attr for attr in dir(item) if not attr.startswith("_")]
        )

        for attr in attributes:
            if hasattr(item, attr):
                value = getattr(item, attr)
                if isinstance(value, str | int | float | bool) or value is None:
                    result[attr] = value
                elif isinstance(value, datetime):
                    result[attr] = value.isoformat()
                elif isinstance(value, list):
                    cleaned_list = []
                    for elem in value:
                        if isinstance(elem, (str, int, float, bool)) or elem is None:
                            cleaned_list.append(elem)
                        elif isinstance(elem, dict):
                            cleaned_list.append(elem)
                        else:
                            # Attempt to extract raw data or dict representation
                            if hasattr(elem, "data"):
                                cleaned_list.append(elem.data)
                            elif hasattr(elem, "dict"):
                                cleaned_list.append(elem.dict())
                            elif hasattr(elem, "__dict__"):
                                cleaned_list.append(elem.__dict__)
                            else:
                                cleaned_list.append(str(elem))
                    result[attr] = cleaned_list
                else:
                    result[attr] = str(value)

        return result

    def process_nested_field(
        self, parent_item: Any, field_name: str, parent_config: ProcessingConfig
    ) -> list[dict[str, Any]]:
        """Process a nested field within an item."""
        nested_data = getattr(parent_item, field_name, None)
        if not nested_data:
            return []

        # Add parent ID fields to each nested item
        def add_parent_ids(item_dict: dict[str, Any]) -> dict[str, Any]:
            for id_field in parent_config.id_fields:
                if id_field not in item_dict:
                    parent_value = getattr(parent_item, id_field, None)
                    if parent_value:
                        item_dict[id_field] = parent_value
            return item_dict

        if isinstance(nested_data, list):
            result = []
            for item in nested_data:
                if isinstance(item, str | int | float | bool):
                    result.append(add_parent_ids({field_name: item}))
                else:
                    transformed = self.transform_item(item)
                    result.append(add_parent_ids(transformed))
            return result
        else:
            # Single item
            if isinstance(nested_data, str | int | float | bool):
                return [add_parent_ids({field_name: nested_data})]
            else:
                transformed = self.transform_item(nested_data)
                return [add_parent_ids(transformed)]


class BaseDataProcessor(ABC):
    """Base data processing orchestrator with shared functionality."""

    def __init__(
        self,
        config_path: str,
        output_dir: str,
        env_path: str = ".env",
        scraper_type: str = "base",
    ):
        # Load configuration
        with open(config_path) as f:
            raw_config = yaml.safe_load(f)

        self.config = {}
        for data_type, config_data in raw_config.items():
            self.config[data_type] = ProcessingConfig(
                data_type=data_type, **config_data
            )

        self.scraper_type = scraper_type
        self.env_path = env_path

        # Initialize shared components
        self.db = self._create_database_connection(env_path)
        self.progress = ProgressTracker(scraper_type=scraper_type)
        self.exporter = DataExporter(output_dir, self.config, self.db)
        self.transformer = DataTransformer()
        self.activity_monitor = ActivityMonitor()

        # Load API keys
        self._load_api_keys()
        self.api_keys = APIKeyManager(self._get_api_keys())

        # API connections
        self.sessions = {}
        self.api_clients = {}
        self.finished_data_types = set()

        # Enhanced checkpoint tracking
        self.checkpoint_manager = CheckpointManager()
        self.hierarchical_trackers = {}

    @abstractmethod
    def _create_database_connection(self, env_path: str) -> BaseDatabaseConnection:
        """Create the appropriate database connection for this scraper type."""

    @abstractmethod
    def _load_api_keys(self):
        """Load API keys specific to this scraper type."""

    @abstractmethod
    def _get_api_keys(self) -> list[str]:
        """Get the loaded API keys."""

    @abstractmethod
    def _create_api_client(
        self, data_type: str, assigned_keys: list[str], session: aiohttp.ClientSession
    ):
        """Create the appropriate API client for this scraper type."""

    def get_hierarchical_tracker(self, data_type: str) -> HierarchicalProgressTracker:
        """Get or create a hierarchical progress tracker for a data type"""
        if data_type not in self.hierarchical_trackers:
            self.hierarchical_trackers[data_type] = HierarchicalProgressTracker(
                self.checkpoint_manager, self.scraper_type, data_type
            )
        return self.hierarchical_trackers[data_type]

    async def sync_last_processed_dates(self):
        """Sync last processed dates from checkpoints to metadata table."""
        try:
            checkpoints = self.checkpoint_manager.list_checkpoints(self.scraper_type)
            for checkpoint in checkpoints:
                if checkpoint.last_processed_date:
                    await self.db.update_last_processed_date(
                        checkpoint.data_type, checkpoint.last_processed_date
                    )
                    logger.info(
                        f"Synced last processed date for {checkpoint.data_type}: {checkpoint.last_processed_date}"
                    )
        except Exception as e:
            logger.error(f"Error syncing last processed dates: {e}")

    async def update_checkpoint_from_db_date(self, data_type: str):
        """Update checkpoint's last processed date from database metadata."""
        try:
            db_date = await self.db.access_last_processed_date(data_type)
            if db_date:
                tracker = self.get_hierarchical_tracker(data_type)
                tracker.update_progress(last_processed_date=db_date)
                logger.info(
                    f"Updated checkpoint for {data_type} with date from DB: {db_date}"
                )
        except Exception as e:
            logger.error(f"Error updating checkpoint from DB date for {data_type}: {e}")

    async def initialize(self):
        """Initialize the processor."""
        await self.db.connect()

        # Sync last processed dates between checkpoints and database
        await self.sync_last_processed_dates()

        # Create API clients for main data types
        main_types = [dt for dt, config in self.config.items() if config.main]

        for data_type in main_types:
            # Update checkpoint with any existing database date
            await self.update_checkpoint_from_db_date(data_type)

            session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=300))
            self.sessions[data_type] = session

            # Assign API keys using the manager
            assigned_keys = self.api_keys.assign_keys(data_type, count=2)

            self.api_clients[data_type] = self._create_api_client(
                data_type, assigned_keys, session
            )

    async def cleanup(self):
        """Clean up resources."""
        for session in self.sessions.values():
            if not session.closed:
                await session.close()

        await self.db.disconnect()

    def get_item_date(self, item: Any) -> datetime:
        """Extract date from item for progress tracking."""
        date_fields = self._get_date_fields()

        for date_field in date_fields:
            date_value = getattr(item, date_field, None)
            if date_value:
                try:
                    # Handle different date formats
                    if isinstance(date_value, datetime):
                        parsed_date = date_value
                    elif isinstance(date_value, str):
                        # Handle various string formats
                        date_str = date_value.rstrip("Z")
                        if "T" in date_str:
                            parsed_date = datetime.fromisoformat(date_str)
                        else:
                            # Try parsing as date only
                            try:
                                parsed_date = datetime.strptime(
                                    date_str, "%Y-%m-%d"
                                ).replace(tzinfo=UTC)
                            except ValueError:
                                # Use dateutil for more flexible parsing
                                from dateutil import parser as date_parser

                                parsed_date = date_parser.parse(date_str)
                    else:
                        continue

                    # Ensure timezone info
                    if parsed_date.tzinfo is None:
                        parsed_date = parsed_date.replace(tzinfo=UTC)
                    elif parsed_date.tzinfo != UTC:
                        parsed_date = parsed_date.astimezone(UTC)

                    return parsed_date
                except (ValueError, AttributeError, TypeError) as e:
                    logger.debug(
                        f"Failed to parse date field {date_field}={date_value}: {e}"
                    )
                    continue

        return datetime.now(UTC)

    @abstractmethod
    def _get_date_fields(self) -> list[str]:
        """Get the list of date fields to check for this scraper type."""

    async def wait_for_api_keys(self):
        """Wait for API keys to become available."""
        while True:
            active_keys = self.api_keys.get_active_keys()
            if active_keys:
                self.activity_monitor.end_sleep()
                return

            # Find earliest wake time
            sleep_times = [
                status["sleep_until"]
                for status in self.api_keys.key_status.values()
                if status["sleeping"] and status["sleep_until"]
            ]

            if sleep_times:
                earliest_wake = min(sleep_times)
                wait_seconds = (earliest_wake - datetime.now(UTC)).total_seconds()
                if wait_seconds > 0:
                    logger.info(
                        f"All keys sleeping. Waiting {wait_seconds:.0f} seconds..."
                    )
                    self.activity_monitor.start_sleep()
                    await asyncio.sleep(
                        min(wait_seconds, 300)
                    )  # Check every 5 minutes max
            else:
                await asyncio.sleep(60)

    @abstractmethod
    async def get_data_stream(
        self, data_type: str, **kwargs
    ) -> AsyncIterator[list[Any]]:
        """Get data stream from API - implemented by subclasses."""

    @abstractmethod
    async def process_hierarchical_data(
        self,
        parent_item: Any,
        data_type: str,
        config: ProcessingConfig,
        tracker: HierarchicalProgressTracker,
        item_index: int,
    ):
        """Process hierarchical data for an item - implemented by subclasses."""

    async def process_nested_data(
        self,
        parent_item: Any,
        data_type: str,
        config: ProcessingConfig,
        tracker: HierarchicalProgressTracker,
        item_index: int,
    ):
        """Process nested data for an item with hierarchical tracking."""
        if not config.nested_fields:
            return

        tracker.set_processing_phase(
            ProcessingPhase.NESTED_FIELDS,
            item_index=item_index,
            item_total=tracker.checkpoint.processing_state.item_total,
            field_total=len(config.nested_fields),
        )

        for field_index, nested_field in enumerate(config.nested_fields):
            field_name = (
                nested_field
                if isinstance(nested_field, str)
                else list(nested_field.keys())[0]
            )

            tracker.set_processing_phase(
                ProcessingPhase.NESTED_FIELDS,
                item_index=item_index,
                item_total=tracker.checkpoint.processing_state.item_total,
                field_index=field_index,
                field_total=len(config.nested_fields),
                current_field=field_name,
            )

            # Check if this specific nested field was already processed
            parent_id = getattr(parent_item, config.id_fields[0], "unknown")
            if tracker.should_skip_item(
                parent_id, ProcessingPhase.NESTED_FIELDS, field_name=field_name
            ):
                continue

            try:
                nested_items = self.transformer.process_nested_field(
                    parent_item, field_name, config
                )

                if nested_items:
                    await self.exporter.export_batch(
                        f"{data_type}_{field_name}", nested_items
                    )

                    # Mark this nested field as processed for this item
                    tracker.increment_processed(
                        parent_id, ProcessingPhase.NESTED_FIELDS, field_name=field_name
                    )

            except Exception as e:
                logger.error(f"Error processing nested field {field_name}: {e}")
                tracker.increment_failed(
                    parent_id,
                    str(e),
                    phase=ProcessingPhase.NESTED_FIELDS,
                    field_name=field_name,
                )

    async def process_data_type_with_monitoring(self, data_type: str):
        """Process a data type with activity monitoring."""
        try:
            await self.process_data_type(data_type)
        except Exception as e:
            logger.error(f"Failed to process {data_type}: {e}")

    @abstractmethod
    async def process_data_type(self, data_type: str):
        """Process a complete data type - implemented by subclasses."""

    @abstractmethod
    async def run(self, specific_data_type: str | None = None):
        """Run the complete processing pipeline - implemented by subclasses."""
