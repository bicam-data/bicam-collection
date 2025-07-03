import argparse
import asyncio
import logging
import os
import time
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Any

import aiohttp
import asyncpg
from dotenv import load_dotenv

# Import the enhanced checkpoint system
from bicam_collection.lib.checkpoint import (
    CheckpointManager,
    CheckpointStatus,
    HierarchicalProgressTracker,
    ProcessingPhase,
)

# Import the base scraper
from bicam_collection.scrapers.base_scraper import (
    APIKeyManager,
    BaseDataProcessor,
    ProcessingConfig,
)
from pycon.govinfo.abstractions import GovInfoAPI
from pycon.models import ErrorResult

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class GovInfoDatabaseConnection:
    """GovInfo-specific database connection manager."""

    def __init__(self, env_path: str = ".env"):
        load_dotenv(env_path)
        self.config = {
            "database": os.getenv("POSTGRESQL_DB"),
            "user": os.getenv("POSTGRESQL_USER"),
            "password": os.getenv("POSTGRESQL_PASSWORD"),
            "host": os.getenv("POSTGRESQL_HOST"),
            "port": os.getenv("POSTGRESQL_PORT"),
        }
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
                "SELECT last_processed_date FROM bicam_metadata.govinfo_last_processed_dates WHERE data_type = $1",
                data_type,
            )

    async def update_last_processed_date(self, data_type: str, date: str):
        """Update the last processed date for a data type."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO bicam_metadata.govinfo_last_processed_dates (data_type, last_processed_date) VALUES ($1, $2) "
                "ON CONFLICT (data_type) DO UPDATE SET last_processed_date = EXCLUDED.last_processed_date",
                data_type,
                date,
            )

    async def log_error(self, url: str, error: str, data_type: str):
        """Log an error to the database."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO bicam_metadata.govinfo_errors (url, error, data_type, timestamp) VALUES ($1, $2, $3, $4)",
                url,
                error,
                data_type,
                datetime.now(UTC).isoformat(),
            )


# Legacy ProgressTracker - now just wraps HierarchicalProgressTracker for compatibility
class ProgressTracker:
    """Legacy progress tracker - provides backward compatibility"""

    def __init__(self, db_path: str = "govinfo_progress.db"):
        # Initialize the enhanced checkpoint manager
        self.checkpoint_manager = CheckpointManager(
            db_path.replace(".db", "_checkpoints.db")
        )
        self._trackers = {}  # Store trackers by data type

    def _get_tracker(self, data_type: str) -> HierarchicalProgressTracker:
        """Get or create a hierarchical tracker for a data type"""
        if data_type not in self._trackers:
            self._trackers[data_type] = HierarchicalProgressTracker(
                self.checkpoint_manager, "govinfo", data_type
            )
        return self._trackers[data_type]


class DataProcessor(BaseDataProcessor):
    """Main data processing orchestrator with enhanced hierarchical checkpoint support."""

    def __init__(self, config_path: str, output_dir: str, env_path: str = ".env"):
        super().__init__(config_path, output_dir, env_path, "govinfo")

        # GovInfo-specific database connection
        self.db = GovInfoDatabaseConnection(env_path)

        # Legacy compatibility
        self.progress = ProgressTracker()  # Uses enhanced checkpoint system internally

        # Load GovInfo API keys
        load_dotenv(".env.gov")
        api_keys = [
            value
            for key, value in os.environ.items()
            if key.startswith("GOVINFO_API_KEY")
        ]
        self.api_keys = APIKeyManager(api_keys)

        # API connections
        self.sessions = {}
        self.govinfo_clients = {}
        self.finished_data_types = set()

        # Enhanced checkpoint tracking
        self.checkpoint_manager = CheckpointManager()
        self.hierarchical_trackers = {}

    async def sync_last_processed_dates(self):
        """Sync last processed dates from checkpoints to metadata table."""
        try:
            checkpoints = self.checkpoint_manager.list_checkpoints("govinfo")
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

    def get_hierarchical_tracker(self, data_type: str) -> HierarchicalProgressTracker:
        """Get or create a hierarchical progress tracker for a data type"""
        if data_type not in self.hierarchical_trackers:
            self.hierarchical_trackers[data_type] = HierarchicalProgressTracker(
                self.checkpoint_manager, "govinfo", data_type
            )
        return self.hierarchical_trackers[data_type]

    async def initialize(self):
        """Initialize the processor."""
        await self.db.connect()

        # Sync last processed dates between checkpoints and database
        await self.sync_last_processed_dates()

        # Create GovInfo clients for main data types
        main_types = [dt for dt, config in self.config.items() if config.main]

        for data_type in main_types:
            # Update checkpoint with any existing database date
            await self.update_checkpoint_from_db_date(data_type)

            session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=300))
            self.sessions[data_type] = session

            # Assign API keys using the manager
            assigned_keys = self.api_keys.assign_keys(data_type, count=2)

            self.govinfo_clients[data_type] = GovInfoAPI(
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
        govinfo = self.govinfo_clients[data_type]
        method = getattr(govinfo, f"get_bulk_{data_type}")

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
                    try:
                        if hasattr(item, "package_url"):
                            full_item = await item._adapter.retrieve(
                                item.package_url, override=True
                            )
                            if not isinstance(full_item, ErrorResult):
                                # Create full item object
                                item_class = type(item)
                                full_obj = item_class(
                                    data=full_item.data,
                                    _pagination=item._pagination,
                                    _adapter=item._adapter,
                                )
                                # Preserve raw payload for staging
                                full_obj.raw_data = full_item.data
                                full_items.append(full_obj)
                            else:
                                logger.error(
                                    f"Error fetching full item: {full_item.error_message}"
                                )
                        else:
                            # No deep fetch required; use existing data
                            item.raw_data = item.data if hasattr(item, "data") else None
                            full_items.append(item)
                    except Exception as e:
                        logger.error(f"Error processing item: {e}")
                        await self.db.log_error(
                            getattr(item, "package_url", "unknown"), str(e), data_type
                        )

                yield full_items

                # Check for next page
                pagination_url = items[0]._pagination.get("next") if items else None
                if not pagination_url:
                    break

                kwargs["next_url"] = pagination_url
                time.sleep(5)  # Rate limiting

            except Exception as e:
                logger.error(f"Stream error for {data_type}: {e}")
                await self.db.log_error(
                    kwargs.get("next_url", f"bulk_{data_type}"), str(e), data_type
                )
                await asyncio.sleep(60)

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

    async def process_granules(
        self,
        data_type: str,
        item: Any,
        tracker: HierarchicalProgressTracker,
        item_index: int,
    ):
        """Process granules (sub-items) for a main item with hierarchical tracking."""
        granules_data_type = f"{data_type}_granules"

        if (
            hasattr(item, "get_granules")
            and hasattr(item, "granules_url")
            and item.granules_url
        ):
            # Set processing phase for granules (treated as related entities)
            tracker.set_processing_phase(
                ProcessingPhase.RELATED_ENTITIES,
                item_index=item_index,
                item_total=tracker.checkpoint.processing_state.item_total,
                current_relation="granules",
            )

            # Check if granules were already processed for this item
            parent_id = getattr(
                item,
                self.config.get(data_type, ProcessingConfig(data_type)).id_fields[0],
                "unknown",
            )
            if tracker.should_skip_item(
                parent_id, ProcessingPhase.RELATED_ENTITIES, relation_type="granules"
            ):
                return

            try:
                granule_items = []
                async for granule in item.get_granules():
                    self.activity_monitor.update_activity()

                    if isinstance(granule, ErrorResult):
                        logger.error(f"Granule error: {granule.error_message}")
                        tracker.increment_failed(
                            parent_id,
                            granule.error_message,
                            phase=ProcessingPhase.RELATED_ENTITIES,
                            relation_type="granules",
                        )
                        continue

                    # Transform granule
                    config = self.config.get(
                        granules_data_type, ProcessingConfig(granules_data_type)
                    )
                    transformed = self.transformer.transform_item(
                        granule, config.fields
                    )

                    # Link to parent
                    parent_config = self.config.get(
                        data_type, ProcessingConfig(data_type)
                    )
                    for id_field in parent_config.id_fields:
                        parent_value = getattr(item, id_field, None)
                        if parent_value:
                            transformed[f"parent_{id_field}"] = parent_value

                    granule_items.append(transformed)

                    # Process nested fields for granule with hierarchical tracking
                    await self.process_nested_data(
                        granule, granules_data_type, config, tracker, item_index
                    )

                if granule_items:
                    await self.exporter.export_batch(granules_data_type, granule_items)

                    # Mark granules as processed for this item
                    tracker.increment_processed(
                        parent_id,
                        ProcessingPhase.RELATED_ENTITIES,
                        relation_type="granules",
                    )

            except Exception as e:
                logger.error(f"Error processing granules for {data_type}: {e}")
                tracker.increment_failed(
                    parent_id,
                    str(e),
                    phase=ProcessingPhase.RELATED_ENTITIES,
                    relation_type="granules",
                )
                await self.db.log_error(
                    getattr(item, "granules_url", "unknown"), str(e), granules_data_type
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
            start_date = datetime.fromisoformat(last_date.rstrip("Z")).replace(
                tzinfo=UTC
            ) - timedelta(hours=1)
            kwargs["start_date"] = start_date.strftime("%Y-%m-%dT%H:%M:%SZ")

        end_date = datetime.now(UTC)
        kwargs["end_date"] = end_date.strftime("%Y-%m-%dT%H:%M:%SZ")

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

                    # ------------------------------------------------------------------
                    # 1. Persist raw payload to bicam_raw_govinfo
                    # ------------------------------------------------------------------
                    try:
                        raw_payload = getattr(item, "raw_data", None)
                        if raw_payload is None and hasattr(item, "data"):
                            raw_payload = item.data

                        if raw_payload is not None:
                            await self.exporter.export_raw_jsonb(
                                schema="bicam_raw_govinfo",
                                table=f"{data_type}_raw",
                                items=[raw_payload],
                                id_field=config.id_fields[0],
                                url_field="package_url",
                            )
                    except Exception as e:
                        logger.error(
                            f"Failed to write raw payload for {data_type}: {e}"
                        )

                    # ------------------------------------------------------------------
                    # Set current item processing phase
                    tracker.set_processing_phase(
                        ProcessingPhase.MAIN_ITEMS,
                        item_index=item_index,
                        item_total=tracker.checkpoint.total_items
                        or 1000,  # Estimate if unknown
                    )

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
                        await self.process_granules(
                            data_type, item, tracker, item_index
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
            if data_type in self.govinfo_clients:
                finished_keys = self.govinfo_clients[data_type]._adapter.api_keys
                self.api_keys.release_keys(finished_keys)
                logger.info(
                    f"Released {len(finished_keys)} keys from completed {data_type}"
                )

                # Redistribute to remaining active data types
                active_clients = {
                    dt: client
                    for dt, client in self.govinfo_clients.items()
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
            "last_modified",
            "dateIssued",
            "date_issued",
            "issued_at",
            "updated_at",
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

    async def run(self, specific_data_type: str | None = None):
        """Run the complete processing pipeline with concurrent data type processing."""
        try:
            await self.initialize()

            # Determine which data types to process
            if specific_data_type:
                if specific_data_type not in self.config:
                    raise ValueError(f"Unknown data type: {specific_data_type}")
                data_types = [specific_data_type]
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

        except KeyboardInterrupt:
            logger.info("Processing interrupted by user")
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            await self.cleanup()

    # ------------------------------------------------------------------
    # Abstract-method implementations required by BaseDataProcessor
    # ------------------------------------------------------------------

    def _create_database_connection(self, env_path: str):  # type: ignore[override]
        """Return the GovInfo-specific DB connection."""
        return GovInfoDatabaseConnection(env_path)

    def _load_api_keys(self):  # type: ignore[override]
        # Keys already loaded in __init__
        pass

    def _get_api_keys(self) -> list[str]:  # type: ignore[override]
        return self.api_keys.all_keys

    def _create_api_client(  # type: ignore[override]
        self,
        data_type: str,
        assigned_keys: list[str],
        session: aiohttp.ClientSession,
    ):
        return GovInfoAPI(assigned_keys, session=session, db_pool=self.db.pool)

    def _get_date_fields(self) -> list[str]:  # type: ignore[override]
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
        # No additional hierarchical layers for GovInfo yet.
        return


async def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Enhanced GovInfo ETL Scraper with Hierarchical Checkpoints"
    )
    parser.add_argument("--data-type", help="Specific data type to process")
    parser.add_argument(
        "--clear-progress", action="store_true", help="Clear progress tracking"
    )
    parser.add_argument(
        "--config", default="govinfo_config.yaml", help="Config file path"
    )
    parser.add_argument(
        "--output",
        default="/mnt/big_data/database-congress/api-govinfo",
        help="Output directory",
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

    # Initialize processor
    processor = DataProcessor(args.config, args.output)

    # Handle checkpoint operations
    if args.show_progress:
        summary = processor.checkpoint_manager.get_progress_summary("govinfo")
        logger.info(f"Progress Summary: {summary}")

        checkpoints = processor.checkpoint_manager.list_checkpoints("govinfo")
        for checkpoint in checkpoints:
            tracker = processor.get_hierarchical_tracker(checkpoint.data_type)
            progress = tracker.detailed_progress
            logger.info(f"{checkpoint.data_type}: {progress}")
        return

    if args.reset_checkpoint:
        processor.checkpoint_manager.reset_checkpoint("govinfo", args.reset_checkpoint)
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
        if args.data_type:
            processor.checkpoint_manager.reset_checkpoint("govinfo", args.data_type)
            logger.info(f"Cleared progress for {args.data_type}")
        else:
            # Clear all progress
            checkpoints = processor.checkpoint_manager.list_checkpoints("govinfo")
            for checkpoint in checkpoints:
                processor.checkpoint_manager.reset_checkpoint(
                    "govinfo", checkpoint.data_type
                )
            logger.info("Cleared all progress")

    # Run processing
    try:
        await processor.run(args.data_type)
    except KeyboardInterrupt:
        logger.info("Processing interrupted by user")
    except Exception as e:
        logger.error(f"Processing failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
