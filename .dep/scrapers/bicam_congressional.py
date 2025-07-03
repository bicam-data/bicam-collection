import argparse
import asyncio
import logging
import os
import re
import time
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import aiohttp
import asyncpg
from dateutil import parser
from dotenv import load_dotenv

# Import the enhanced checkpoint system
from bicam_collection.lib.checkpoint import (
    CheckpointManager,
    CheckpointStatus,
    HierarchicalProgressTracker,
    ProcessingPhase,
)

# Import RunManager
from bicam_collection.lib.run_tracking import (
    RunManager,
    RunMetadata,
    RunType,
)

# Import the base scraper
from bicam_collection.scrapers.base_scraper import (
    BaseDataProcessor,
    ProcessingConfig,
)

# Raw fetcher registry for related payloads
from bicam_collection.scrapers.raw_fetchers import get_fetchers
from pycon.congress.abstractions import PyCongress
from pycon.models import ErrorResult

# Configure logging
logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@dataclass
class ProcessingState:
    """Legacy state class - kept for backward compatibility."""

    data_type: str
    total_items: int = 0
    processed_items: int = 0
    current_batch: int = 0
    last_processed_date: str | None = None
    is_finished: bool = False
    last_exported_item: dict[str, Any] | None = None


class CongressionalDatabaseConnection:
    """Congressional-specific database connection manager."""

    def __init__(self, env_path: str = ".env"):
        load_dotenv(env_path)
        # Mirror the variable names accepted in BaseDatabaseConnection so that
        # users can rely on either the POSTGRESQL_* or the standard PG*
        # environment variables (or both).
        self.config: dict[str, str | int | None] = {
            "database": (
                os.getenv("POSTGRESQL_DATABASE")
                or os.getenv("POSTGRESQL_DB")
                or os.getenv("PGDATABASE")
                or os.getenv("POSTGRES_DB")
            ),
            "user": (
                os.getenv("POSTGRESQL_USERNAME")
                or os.getenv("POSTGRESQL_USER")
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

        # Validate critical fields early to avoid cryptic asyncpg errors.
        missing = [
            k
            for k, v in self.config.items()
            if v in (None, "") and k in ("database", "user", "password")
        ]
        if missing:
            raise RuntimeError(
                "CongressionalDatabaseConnection: missing env vars for "
                + ", ".join(missing)
                + ". Set them in the --env file or shell."
            )
        self.pool = None

    async def connect(self):
        """Create database connection pool."""
        self.pool = await asyncpg.create_pool(**self.config)

    async def disconnect(self):
        """Close database connection pool."""
        if self.pool:
            await self.pool.close()

    async def access_last_processed_date(self, data_type: str) -> str | None:
        """Get the last processed date for a data type."""
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                "SELECT last_processed_date FROM bicam_metadata.congressional_last_processed_dates WHERE data_type = $1",
                data_type,
            )

    async def update_last_processed_date(self, data_type: str, date: str):
        """Update the last processed date for a data type."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO bicam_metadata.congressional_last_processed_dates (data_type, last_processed_date) VALUES ($1, $2) "
                "ON CONFLICT (data_type) DO UPDATE SET last_processed_date = EXCLUDED.last_processed_date",
                data_type,
                date,
            )

    async def log_error(self, url: str, error: str, data_type: str):
        """Log an error to the database."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO bicam_metadata.congressional_errors (url, error, data_type, timestamp) VALUES ($1, $2, $3, $4)",
                url,
                error,
                data_type,
                datetime.now(UTC).isoformat(),
            )


# Legacy ProgressTracker - now just wraps HierarchicalProgressTracker for compatibility
class ProgressTracker:
    """Legacy progress tracker - provides backward compatibility"""

    def __init__(self, db_path: str = "scraper_progress.db"):
        # Initialize the enhanced checkpoint manager
        self.checkpoint_manager = CheckpointManager(
            db_path.replace(".db", "_checkpoints.db")
        )
        self._trackers = {}  # Store trackers by data type

    def _get_tracker(self, data_type: str) -> HierarchicalProgressTracker:
        """Get or create a hierarchical tracker for a data type"""
        if data_type not in self._trackers:
            self._trackers[data_type] = HierarchicalProgressTracker(
                self.checkpoint_manager, "congressional", data_type
            )
        return self._trackers[data_type]

    def save_state(self, state: ProcessingState):
        """Save processing state - legacy compatibility method"""
        tracker = self._get_tracker(state.data_type)

        # Update checkpoint with legacy state data
        tracker.update_progress(
            processed_items=state.processed_items,
            last_exported_item=state.last_exported_item,
        )

        # Update processing phase based on state
        if state.is_finished:
            tracker.complete_processing()
        else:
            tracker.set_processing_phase(
                ProcessingPhase.MAIN_ITEMS,
                item_index=state.processed_items,
                item_total=state.total_items,
            )

    def load_state(self, data_type: str) -> ProcessingState | None:
        """Load processing state - legacy compatibility method"""
        tracker = self._get_tracker(data_type)
        checkpoint = tracker.checkpoint

        if not checkpoint:
            return None

        return ProcessingState(
            data_type=data_type,
            total_items=checkpoint.total_items,
            processed_items=checkpoint.processed_items,
            current_batch=checkpoint.batch_info.get("current_batch", 0),
            last_processed_date=checkpoint.last_processed_date,
            is_finished=(checkpoint.status == CheckpointStatus.COMPLETED),
            last_exported_item=checkpoint.last_exported_item,
        )

    def clear_state(self, data_type: str):
        """Clear state for a data type - legacy compatibility method"""
        self.checkpoint_manager.reset_checkpoint("congressional", data_type)


class DataProcessor(BaseDataProcessor):
    """Main data processing orchestrator with enhanced hierarchical checkpoint support."""

    def __init__(self, config_path: str, output_dir: str, env_path: str = ".env"):
        super().__init__(config_path, output_dir, env_path, "congressional")

        # Congressional-specific database connection
        self.db = CongressionalDatabaseConnection(env_path)

        # Keep DataExporter in sync with the new db connection created above
        if hasattr(self, "exporter"):
            self.exporter.db_conn = self.db

        # Legacy compatibility
        self.progress = ProgressTracker()  # Uses enhanced checkpoint system internally

        # API connections
        self.sessions = {}
        self.congress_clients = {}
        self.finished_data_types = set()

        # Enhanced checkpoint tracking
        self.checkpoint_manager = CheckpointManager()
        self.hierarchical_trackers = {}

        # ------------------------------------------------------------
        # Run tracking setup
        # ------------------------------------------------------------
        self.run_manager: RunManager | None = None
        self.run_id: str | None = None

    async def sync_last_processed_dates(self):
        """Sync last processed dates from checkpoints to metadata table."""
        try:
            checkpoints = self.checkpoint_manager.list_checkpoints("congressional")
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
                # Directly set the field because update_progress() doesn't accept this kwarg yet.
                tracker.checkpoint.last_processed_date = db_date
                tracker.checkpoint_manager.save_checkpoint(tracker.checkpoint)
                logger.info(
                    f"Updated checkpoint for {data_type} with date from DB: {db_date}"
                )
        except Exception as e:
            logger.error(f"Error updating checkpoint from DB date for {data_type}: {e}")

    def get_hierarchical_tracker(self, data_type: str) -> HierarchicalProgressTracker:
        """Get or create a hierarchical progress tracker for a data type"""
        if data_type not in self.hierarchical_trackers:
            self.hierarchical_trackers[data_type] = HierarchicalProgressTracker(
                self.checkpoint_manager, "congressional", data_type
            )
        return self.hierarchical_trackers[data_type]

    async def initialize(self):
        """Initialize the processor."""
        await self.db.connect()

        # Sync last processed dates between checkpoints and database
        await self.sync_last_processed_dates()

        # Create PyCongress clients for main data types
        main_types = [dt for dt, config in self.config.items() if config.main]

        for data_type in main_types:
            # Update checkpoint with any existing database date
            await self.update_checkpoint_from_db_date(data_type)

            session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=300))
            self.sessions[data_type] = session

            # Assign API keys using the manager
            assigned_keys = self.api_keys.assign_keys(data_type, count=2)

            self.congress_clients[data_type] = PyCongress(
                assigned_keys, session=session, db_pool=self.db.pool
            )

    async def cleanup(self):
        """Clean up resources."""
        for session in self.sessions.values():
            if not session.closed:
                await session.close()

        await self.db.disconnect()

    async def get_data_stream(
        self, data_type: str, **kwargs
    ) -> AsyncIterator[list[Any]]:
        """Get data stream from API."""
        congress = self.congress_clients[data_type]
        method = getattr(congress, f"get_bulk_{data_type}")

        while True:
            try:
                items = await method(**kwargs)

                if isinstance(items, ErrorResult):
                    logger.error(f"API error for {data_type}: {items.error_message}")
                    await self.db.log_error(
                        items.url or f"bulk_{data_type}", items.error_message, data_type
                    )
                    break

                if not items:
                    logger.info(f"No more items for {data_type}")
                    break

                # Get full item details
                full_items = []
                for item in items:
                    logger.debug("Fetching detail for %s from %s", data_type, item.url)
                    full_item = await item._adapter.retrieve(item.url, override=True)

                    if not isinstance(full_item, ErrorResult):
                        # got full payload
                        item_class = type(item)
                        # Preserve original stub URL inside the raw payload so
                        # downstream logic can propagate it to related items.
                        full_data = full_item.data or {}
                        stub_url = getattr(item, "url", None)
                        if stub_url and "url" not in full_data:
                            full_data["url"] = stub_url

                        full_obj = item_class(
                            data=full_data,
                            _pagination=item._pagination,
                            _adapter=item._adapter,
                        )
                        full_obj.raw_data = full_data
                        full_items.append(full_obj)
                    else:
                        logger.error(
                            "Detail fetch failed for %s | url=%s | error=%s",
                            data_type,
                            item.url,
                            full_item.error_message,
                        )
                        logger.debug("Stub payload: %s", item.data)

                yield full_items

                # Check for next page
                pagination_url = items[0]._pagination.get("next") if items else None
                if not pagination_url:
                    break

                kwargs["next_url"] = pagination_url
                time.sleep(10)  # Rate limiting

            except Exception as e:
                logger.error(f"Stream error for {data_type}: {e}")
                await self.db.log_error(
                    kwargs.get("next_url", f"bulk_{data_type}"), str(e), data_type
                )
                await asyncio.sleep(60)

    async def process_related_data(
        self,
        parent_item: Any,
        data_type: str,
        config: ProcessingConfig,
        tracker: HierarchicalProgressTracker,
        item_index: int,
    ):
        """Process related data for an item with hierarchical tracking."""
        if not config.related_fields:
            return

        tracker.set_processing_phase(
            ProcessingPhase.RELATED_ENTITIES,
            item_index=item_index,
            item_total=tracker.checkpoint.processing_state.item_total,
            relation_total=len(config.related_fields),
        )

        # Map of dynamic fetchers for this data_type
        fetcher_map = get_fetchers(data_type)

        for rel_index, related_type in enumerate(config.related_fields):
            tracker.set_processing_phase(
                ProcessingPhase.RELATED_ENTITIES,
                item_index=item_index,
                item_total=tracker.checkpoint.processing_state.item_total,
                relation_index=rel_index,
                relation_total=len(config.related_fields),
                current_relation=related_type,
            )

            if related_type in fetcher_map:
                # Use specialised raw fetcher
                try:
                    raw_payloads = await fetcher_map[related_type](
                        getattr(parent_item, "raw_data", None)
                        or getattr(parent_item, "data", parent_item),
                        self.congress_clients[data_type],
                    )

                    # Propagate a fallback endpoint from the parent object so that
                    # `export_raw_jsonb` can extract/clean it.  Prefer the generic
                    # `.url` attribute, but if that is not available we look for a
                    # relation-specific attribute (e.g. `actions_url`,
                    # `cosponsors_url`, `texts_url`) that PyCongress exposes on the
                    # parent amendment/bill object.  This significantly increases
                    # the likelihood that every related raw payload carries some
                    # form of endpoint metadata.

                    parent_url: str | None = getattr(parent_item, "url", None)

                    if parent_url is None:
                        # Attempt more specific attribute (e.g. actions_url)
                        specific_attr = f"{related_type}_url"
                        parent_url = getattr(parent_item, specific_attr, None)

                    if parent_url is None:
                        logger.debug(
                            "No parent URL or %s found for %s id=%s when processing %s",
                            specific_attr,
                            data_type,
                            getattr(parent_item, config.id_fields[0], "unknown"),
                            related_type,
                        )
                    else:
                        for p in raw_payloads:
                            if isinstance(p, dict) and not p.get("url"):
                                p["url"] = parent_url

                    parent_key = config.id_fields[0] if config.id_fields else None

                    # Ensure each raw payload has the parent identifier so that source_doc_id can be stored
                    if parent_key and hasattr(parent_item, parent_key):
                        parent_val = getattr(parent_item, parent_key, None)
                        if parent_val is not None:
                            for p in raw_payloads:
                                if isinstance(p, dict) and not p.get(parent_key):
                                    p[parent_key] = parent_val

                    await self.exporter.export_raw_jsonb(
                        schema="bicam_raw_congressional",
                        table=f"{data_type}_{related_type}_raw",
                        items=raw_payloads,
                        id_field=parent_key,
                        url_field="url",
                    )
                    tracker.update_progress()
                except Exception as exc:
                    logger.error("Error via fetcher for %s: %s", related_type, exc)
            else:
                # Fallback to legacy attribute iteration
                method_name = f"get_{related_type}"
                if not hasattr(parent_item, method_name):
                    continue

                try:
                    method = getattr(parent_item, method_name)
                    async for related_item in method():
                        self.activity_monitor.update_activity()

                        if isinstance(related_item, ErrorResult):
                            logger.error(
                                "Related data error: %s", related_item.error_message
                            )
                            tracker.increment_failed(
                                getattr(parent_item, config.id_fields[0], "unknown"),
                                related_item.error_message,
                                phase=ProcessingPhase.RELATED_ENTITIES,
                                relation_type=related_type,
                            )
                            continue

                        raw_payload = getattr(related_item, "raw_data", None)
                        if raw_payload is None and hasattr(related_item, "data"):
                            raw_payload = related_item.data

                        if raw_payload is not None:
                            # Use generic `.url` attribute if present, otherwise
                            # fall back to a relation-specific attribute such as
                            # `actions_url`, `cosponsors_url`, `texts_url`, etc.
                            fallback_url = getattr(parent_item, "url", None)

                            if fallback_url is None:
                                rel_attr = f"{related_type}_url"
                                fallback_url = getattr(parent_item, rel_attr, None)

                            if fallback_url:
                                raw_payload["url"] = fallback_url

                            parent_key = (
                                config.id_fields[0] if config.id_fields else None
                            )

                            if parent_key and hasattr(parent_item, parent_key):
                                parent_val = getattr(parent_item, parent_key, None)
                                if parent_val is not None and isinstance(
                                    raw_payload, dict
                                ):
                                    if not raw_payload.get(parent_key):
                                        raw_payload[parent_key] = parent_val

                            # Use parent item's URL if payload lacks one
                            if isinstance(raw_payload, dict):
                                if "url" not in raw_payload or not raw_payload["url"]:
                                    if getattr(parent_item, "url", None):
                                        raw_payload["url"] = parent_item.url

                            await self.exporter.export_raw_jsonb(
                                schema="bicam_raw_congressional",
                                table=f"{data_type}_{related_type}_raw",
                                items=[raw_payload],
                                id_field=parent_key,
                                url_field="url",
                            )

                        transformed = self.transformer.transform_item(related_item, [])
                        await self.exporter.export_batch(
                            f"{data_type}_{related_type}", [transformed]
                        )

                        tracker.increment_processed(
                            getattr(parent_item, config.id_fields[0], "unknown"),
                            phase=ProcessingPhase.RELATED_ENTITIES,
                            relation_type=related_type,
                        )

                    tracker.update_progress()

                except Exception as e:
                    logger.error("Error processing related %s: %s", related_type, e)
                    tracker.increment_failed(
                        getattr(parent_item, config.id_fields[0], "unknown"),
                        str(e),
                        phase=ProcessingPhase.RELATED_ENTITIES,
                        relation_type=related_type,
                    )

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

    async def process_data_type(self, data_type: str):
        """Process a complete data type with enhanced hierarchical tracking."""
        config = self.config[data_type]
        tracker = self.get_hierarchical_tracker(data_type)

        # Check if already completed
        if tracker.checkpoint.status == CheckpointStatus.COMPLETED:
            logger.info(f"Skipping {data_type} - already finished")
            return

        logger.info(f"Processing {data_type}")

        # Get date range for processing
        last_date = await self.db.access_last_processed_date(data_type)
        kwargs = {}

        if last_date:
            start_date = parser.isoparse(last_date) - timedelta(hours=1)
            kwargs["from_date"] = start_date.strftime("%Y-%m-%dT%H:%M:%SZ")

        end_date = datetime.now(UTC)
        kwargs["to_date"] = end_date.strftime("%Y-%m-%dT%H:%M:%SZ")

        # Store API parameters for resumption
        tracker.update_progress(api_params=kwargs)

        # Start processing
        tracker.start_processing()
        tracker.set_processing_phase(ProcessingPhase.MAIN_ITEMS)

        try:
            # Determine starting point based on checkpoint
            start_item_index = tracker.checkpoint.processing_state.item_index
            current_phase = tracker.checkpoint.processing_state.phase

            logger.info(
                f"Resuming {data_type} from item {start_item_index}, phase {current_phase.value}"
            )

            item_index = 0
            # Process data stream
            async for batch in self.get_data_stream(data_type, **kwargs):
                if not batch:
                    continue

                for item in batch:
                    # Skip items we've already processed in the main phase
                    item_id = getattr(item, config.id_fields[0], f"item_{item_index}")

                    if item_index < start_item_index:
                        item_index += 1
                        continue

                    # Set current item processing phase
                    tracker.set_processing_phase(
                        ProcessingPhase.MAIN_ITEMS,
                        item_index=item_index,
                        item_total=tracker.checkpoint.total_items
                        or 1000,  # Estimate if unknown
                    )

                    # ------------------------------------------------------------------
                    # 1. Persist raw payload to bicam_raw_congressional
                    # ------------------------------------------------------------------
                    try:
                        raw_payload = getattr(item, "raw_data", None)
                        if raw_payload is None and hasattr(item, "data"):
                            raw_payload = item.data  # Fallback to already-parsed dict

                        if raw_payload is not None:
                            # Use generic `.url` attribute if present, otherwise
                            # fall back to a relation-specific attribute such as
                            # `actions_url`, `cosponsors_url`, `texts_url`, etc.
                            fallback_url = getattr(item, "url", None)

                            if fallback_url is None:
                                rel_attr = f"{data_type}_url"
                                fallback_url = getattr(item, rel_attr, None)

                            if fallback_url:
                                raw_payload["url"] = fallback_url

                            parent_key = (
                                config.id_fields[0] if config.id_fields else None
                            )
                            await self.exporter.export_raw_jsonb(
                                schema="bicam_raw_congressional",
                                table=f"{data_type}_raw",
                                items=[raw_payload],
                                id_field=parent_key,
                                url_field="url",
                            )
                    except Exception as e:
                        logger.error(
                            f"Failed to write raw payload for {data_type}: {e}"
                        )

                    # ------------------------------------------------------------------
                    # Check if main item processing should be skipped
                    if not tracker.should_skip_item(
                        item_id, ProcessingPhase.MAIN_ITEMS
                    ):
                        # Transform main item
                        transformed = self.transformer.transform_item(
                            item, config.fields
                        )

                        # Export main item
                        await self.exporter.export_batch(data_type, [transformed])

                        # Mark main item as processed
                        tracker.increment_processed(item_id, ProcessingPhase.MAIN_ITEMS)

                        # Update last exported item
                        tracker.update_progress(last_exported_item=transformed)

                    # Process hierarchical data based on current phase
                    if tracker.can_resume_from_phase(ProcessingPhase.NESTED_FIELDS):
                        await self.process_nested_data(
                            item, data_type, config, tracker, item_index
                        )

                    if tracker.can_resume_from_phase(ProcessingPhase.RELATED_ENTITIES):
                        await self.process_related_data(
                            item, data_type, config, tracker, item_index
                        )

                    # Track latest date
                    item_date = self.get_item_date(item)
                    if item_date:
                        date_str = item_date.isoformat()
                        await self.db.update_last_processed_date(data_type, date_str)
                        # Also update the checkpoint tracker
                        tracker.update_progress(last_processed_date=date_str)

                    item_index += 1

                    # Log detailed progress periodically
                    if item_index % 10 == 0:
                        progress = tracker.detailed_progress
                        logger.info(f"Progress for {data_type}: {progress}")

            # Mark as finished and redistribute keys
            tracker.complete_processing()
            self.finished_data_types.add(data_type)

            # Release API keys from finished data type
            if data_type in self.congress_clients:
                finished_keys = self.congress_clients[data_type]._adapter.api_keys
                self.api_keys.release_keys(finished_keys)
                logger.info(
                    f"Released {len(finished_keys)} keys from completed {data_type}"
                )

                # Redistribute to remaining active data types
                active_clients = {
                    dt: client
                    for dt, client in self.congress_clients.items()
                    if dt not in self.finished_data_types
                }
                while self.api_keys.redistribute_key(active_clients):
                    pass  # Keep redistributing until no more keys available

            logger.info(f"Completed processing {data_type}")

        except Exception as e:
            logger.error(f"Error processing {data_type}: {e}")
            tracker.fail_processing(str(e))
            raise

    def get_item_date(self, item: Any) -> datetime:
        """Extract date from item for progress tracking."""
        date_fields = [
            "updated_at",
            "updateDate",
            "update_date",
            "latest_action_date",
            "introduced_at",
        ]

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

    async def process_data_type_with_monitoring(self, data_type: str):
        """Process a data type with activity monitoring."""
        try:
            await self.process_data_type(data_type)
        except Exception as e:
            logger.error(f"Failed to process {data_type}: {e}")

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

    async def run(self, requested_types: list[str] | None = None):
        """Run the complete processing pipeline with concurrent data type processing."""
        # ------------------------------------------------------------
        # 1. Initialise RunManager and create run metadata *before* any DB work
        # ------------------------------------------------------------
        if not self.run_manager:
            self.run_manager = RunManager(use_postgres=True, external_pool=self.db.pool)
            await self.run_manager.initialize()

        metadata = RunMetadata(
            run_id="",  # placeholder; set by create_run
            run_type=RunType.SCRAPER_CONGRESSIONAL,
            system_name="congressional_scraper",
            description="Congressional scraper run via DataProcessor",
            parameters={"requested_types": requested_types},
            data_types=requested_types
            or [dt for dt, cfg in self.config.items() if cfg.main],
            created_by=os.getenv("USER", "system"),
        )

        self.run_id = self.run_manager.create_run(metadata)
        await self.run_manager.start_run(self.run_id)

        try:
            await self.initialize()

            # Determine which data types to process
            if requested_types:
                unknown = [dt for dt in requested_types if dt not in self.config]
                if unknown:
                    raise ValueError(f"Unknown data type(s): {', '.join(unknown)}")
                data_types = requested_types
            else:
                data_types = [dt for dt, config in self.config.items() if config.main]

            # Filter out already finished data types
            pending_types = []
            for data_type in data_types:
                tracker = self.get_hierarchical_tracker(data_type)
                if tracker.checkpoint.status != CheckpointStatus.COMPLETED:
                    pending_types.append(data_type)
                else:
                    logger.info(f"Skipping {data_type} - already finished")

            if not pending_types:
                logger.info("All data types already finished")
                return

            # Create concurrent tasks for each data type
            tasks = []
            for data_type in pending_types:
                task = asyncio.create_task(
                    self.process_data_type_with_monitoring(data_type),
                    name=f"process_{data_type}",
                )
                tasks.append(task)

            logger.info(f"Starting concurrent processing of {len(tasks)} data types")

            # Monitor progress and handle API key availability
            while tasks:
                # Check for completed tasks
                done, pending = await asyncio.wait(
                    tasks, timeout=300, return_when=asyncio.FIRST_COMPLETED
                )

                for task in done:
                    tasks.remove(task)
                    data_type = task.get_name().replace("process_", "")
                    if task.exception():
                        logger.error(f"Task {data_type} failed: {task.exception()}")
                    else:
                        logger.info(f"Task {data_type} completed successfully")

                # Check if we need to wait for API keys
                if not self.api_keys.get_active_keys() and tasks:
                    await self.wait_for_api_keys()

                # Update activity
                self.activity_monitor.update_activity()

                # Check for timeout
                if self.activity_monitor.is_timed_out():
                    logger.warning("Processing appears to have timed out")
                    break

            # Cancel any remaining tasks
            for task in tasks:
                task.cancel()

            logger.info("Processing complete")

            # Mark run complete
            if self.run_manager and self.run_id:
                await self.run_manager.complete_run(
                    self.run_id, output_summary={"finished": True}
                )

        except KeyboardInterrupt:
            logger.info("Processing interrupted by user")
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            if self.run_manager and self.run_id:
                await self.run_manager.fail_run(self.run_id, "Interrupted by user")
        finally:
            await self.cleanup()

    # ------------------------------------------------------------------
    # Abstract-method implementations required by BaseDataProcessor
    # ------------------------------------------------------------------

    def _create_database_connection(self, env_path: str):  # type: ignore[override]
        """Return the congressional-specific DB connection."""
        return CongressionalDatabaseConnection(env_path)

    def _load_api_keys(self):  # type: ignore[override]
        """Load CONGRESS_API_KEYs from environment files.

        Order of precedence
        1. The `env_path` that `DataProcessor` was instantiated with
        2. A sibling `.env.gov` file (ignored if identical to `env_path`)

        Loading is non-destructive (`override=False`) so that earlier values
        are not clobbered by later files.  This lets developers choose to keep
        **all** secrets in a single `.env`, or split DB creds and API keys
        across separate files.
        """

        # 1. Project-wide default (self.env_path comes from BaseDataProcessor)
        load_dotenv(self.env_path, override=False)

        # 2. Optional dedicated API-key file
        if self.env_path != ".env.gov":
            load_dotenv(".env.gov", override=False)

        # Gather every CONGRESS_API_KEY* found in the merged environment
        raw_values = [
            v for k, v in os.environ.items() if k.startswith("CONGRESS_API_KEY") and v
        ]

        # Allow users to supply multiple keys in a single variable, comma- or
        # semicolon-separated.
        self._congress_api_keys = []
        for val in raw_values:
            if "," in val or ";" in val:
                for part in re.split(r"[;,]", val):
                    cleaned = part.strip()
                    if cleaned:
                        self._congress_api_keys.append(cleaned)
            else:
                self._congress_api_keys.append(val.strip())

        # Deduplicate while preserving order
        seen = set()
        self._congress_api_keys = [
            k for k in self._congress_api_keys if not (k in seen or seen.add(k))
        ]

        if not self._congress_api_keys:
            logger.warning(
                "No CONGRESS_API_KEYs found in environment.  Scraper requests "
                "will likely fail unless the API allows unauthenticated access."
            )

    def _get_api_keys(self) -> list[str]:  # type: ignore[override]
        """Return the list of previously-loaded API keys to BaseDataProcessor."""
        return getattr(self, "_congress_api_keys", [])

    def _create_api_client(  # type: ignore[override]
        self,
        data_type: str,
        assigned_keys: list[str],
        session: aiohttp.ClientSession,
    ):
        """Provide a PyCongress client for BaseDataProcessor fall-backs."""
        return PyCongress(assigned_keys, session=session, db_pool=self.db.pool)

    def _get_date_fields(self) -> list[str]:  # type: ignore[override]
        # Common date attributes in Congressional objects
        return [
            "lastModified",
            "last_modified",
            "issued_at",
            "updated_at",
            "date",
        ]

    async def process_hierarchical_data(  # type: ignore[override]
        self,
        parent_item,
        data_type: str,
        config: ProcessingConfig,
        tracker: HierarchicalProgressTracker,
        item_index: int,
    ):
        """Congressional scraper currently has no additional hierarchical layers beyond nested/related."""
        # No-op for now – extend when hierarchical relationships are introduced.
        return


async def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Enhanced Congressional ETL Scraper with Hierarchical Checkpoints"
    )
    # Single data type (legacy)
    parser.add_argument(
        "--data-type", dest="data_type_flag", help="Process a single data type (legacy)"
    )
    # Multiple data types
    parser.add_argument(
        "--data-types",
        dest="data_types_flag",
        nargs="+",
        help="Process one or more data types",
    )
    # Positional list (zero or more)
    parser.add_argument(
        "data_types", nargs="*", help="Positional list of data types to process"
    )
    parser.add_argument(
        "--clear-progress", action="store_true", help="Clear progress tracking"
    )
    parser.add_argument(
        "--config", default="configs/congressional_config.yaml", help="Config file path"
    )
    parser.add_argument(
        "--output",
        default="/mnt/big_data/database-congress/api-congress",
        help="Output directory",
    )
    parser.add_argument(
        "--env",
        default=".env",
        help="Path to environment file with DB credentials and/or API keys",
    )
    parser.add_argument(
        "--show-progress",
        action="store_true",
        help="Show detailed progress information",
    )
    parser.add_argument(
        "--reset-checkpoint", help="Reset checkpoint for specific data type"
    )
    parser.add_argument(
        "--sync-dates",
        action="store_true",
        help="Sync last processed dates between checkpoints and database",
    )
    args = parser.parse_args()

    # Resolve requested data types with precedence: --data-types › --data-type › positional list
    if args.data_types_flag:
        requested_types = args.data_types_flag
    elif args.data_type_flag:
        requested_types = [args.data_type_flag]
    elif args.data_types:
        requested_types = args.data_types
    else:
        requested_types = None  # means "all main=true" later

    # Initialize processor with custom env file
    processor = DataProcessor(args.config, args.output, env_path=args.env)

    # Handle checkpoint operations
    if args.show_progress:
        summary = processor.checkpoint_manager.get_progress_summary("congressional")
        logger.info(f"Progress Summary: {summary}")

        checkpoints = processor.checkpoint_manager.list_checkpoints("congressional")
        for checkpoint in checkpoints:
            tracker = processor.get_hierarchical_tracker(checkpoint.data_type)
            progress = tracker.detailed_progress
            logger.info(f"{checkpoint.data_type}: {progress}")
        return

    if args.reset_checkpoint:
        processor.checkpoint_manager.reset_checkpoint(
            "congressional", args.reset_checkpoint
        )
        logger.info(f"Reset checkpoint for {args.reset_checkpoint}")
        return

    if args.sync_dates:
        await processor.initialize()
        await processor.sync_last_processed_dates()
        logger.info("Synced last processed dates")
        await processor.cleanup()
        return

    # Clear progress if requested
    if args.clear_progress:
        if requested_types:
            for dt in requested_types:
                processor.checkpoint_manager.reset_checkpoint("congressional", dt)
            logger.info(f"Cleared progress for {', '.join(requested_types)}")
        else:
            # Clear all progress
            checkpoints = processor.checkpoint_manager.list_checkpoints("congressional")
            for checkpoint in checkpoints:
                processor.checkpoint_manager.reset_checkpoint(
                    "congressional", checkpoint.data_type
                )
            logger.info("Cleared all progress")

    # Run processing
    try:
        await processor.run(requested_types)
    except KeyboardInterrupt:
        logger.info("Processing interrupted by user")
    except Exception as e:
        logger.error(f"Processing failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
