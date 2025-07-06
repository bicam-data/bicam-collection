"""
Streamlined Fetcher - Refactored to work with OptimizedParallelProcessor and checkpoint system

This fetcher focuses on:
1. Using plugins for custom logic
2. Implementing the interface expected by OptimizedParallelProcessor
3. NOT doing its own parallel processing
4. Comprehensive checkpoint management support
"""

import json
import logging
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any

from .libs.hierarchical_checkpoint_system import (
    FetchingCheckpoint,
    FetchingPhase,
    HierarchicalCheckpointManager,
    ProcessingStage,
)
from .plugins.consolidated_registry import get_consolidated_registry
from .processing.optimized_processor import OptimizedParallelProcessor

logger = logging.getLogger(__name__)


class StreamlinedFetcher:
    """
    Fetcher that uses plugins for custom logic and works with OptimizedParallelProcessor.

    This class:
    - Delegates parallel processing to OptimizedParallelProcessor
    - Uses plugins for data-source-specific logic
    - Provides the interface expected by the parallel processor
    - Supports comprehensive checkpoint management
    """

    def __init__(
        self,
        client,
        db_pool=None,
        data_type_name=None,
        checkpoint_manager=None,
        run_manager=None,
        api_keys: list[str] | None = None,
        config=None,
        resource_coordinator=None,
        checkpoint_db_path: str = "hierarchical_checkpoints.db",
    ):
        self.client = client
        self.db_pool = db_pool
        self.data_type_name = data_type_name
        self.checkpoint_manager = checkpoint_manager
        self.run_manager = run_manager
        self.api_keys = api_keys or []
        self.config = config
        self.resource_coordinator = resource_coordinator

        # Initialize hierarchical checkpoint manager
        self.hierarchical_checkpoint_manager = HierarchicalCheckpointManager(
            checkpoint_db_path
        )

        # Load plugin for this data type
        self._plugin = None
        self._load_plugin()

        # Extract API keys from client if available
        if hasattr(client, "api_keys"):
            self.api_keys = client.api_keys

        logger.info(f"StreamlinedFetcher initialized for {data_type_name}")

    @classmethod
    async def from_coordinator(cls, resource_coordinator, data_type_name=None):
        """
        Create a StreamlinedFetcher from a ResourceCoordinator.

        This is the preferred way to create a fetcher when using the new
        streamlined architecture with ResourceCoordinator.
        """
        # Get resources from coordinator
        db_pool = await resource_coordinator.get_db_pool()
        api_key_manager = await resource_coordinator.get_api_key_manager()
        checkpoint_manager = resource_coordinator.get_checkpoint_manager_instance()
        run_manager = await resource_coordinator.get_run_manager_instance()

        # Get ALL API keys from the coordinator for OptimizedParallelProcessor
        # OptimizedParallelProcessor manages keys dynamically, so we need ALL keys, not session-based keys
        system_manager = api_key_manager.get_system_manager()
        api_keys = system_manager.api_keys if system_manager else []

        logger.info(
            f"Fetcher initialized with {len(api_keys)} API keys for OptimizedParallelProcessor"
        )

        # OptimizedParallelProcessor expects ALL keys and will create len(api_keys) * 1.5 workers
        if data_type_name and api_keys:
            expected_workers = int(len(api_keys) * 1.5)
            logger.info(
                f"OptimizedParallelProcessor will create ~{expected_workers} workers from {len(api_keys)} keys"
            )

        # Create a client using the coordinator's client management
        client = None
        if data_type_name:
            try:
                clients = await resource_coordinator.get_api_clients(data_type_name)
                client = clients[0] if clients else None
            except Exception as e:
                logger.warning(f"Failed to get client for {data_type_name}: {e}")

        fetcher = cls(
            client=client,
            db_pool=db_pool,
            data_type_name=data_type_name,
            checkpoint_manager=checkpoint_manager,
            run_manager=run_manager,
            api_keys=api_keys,
            resource_coordinator=resource_coordinator,
        )

        return fetcher

    def _load_plugin(self):
        """Load the appropriate plugin for this data type."""
        if not self.data_type_name:
            return

        try:
            registry = get_consolidated_registry()
            self._plugin = registry.get_fetcher_plugin(self.data_type_name)
            if self._plugin:
                logger.info(f"Loaded plugin for {self.data_type_name}")
            else:
                logger.warning(f"No plugin found for {self.data_type_name}")
        except Exception as e:
            logger.error(f"Failed to load plugin for {self.data_type_name}: {e}")

    # =============================================================================
    # CHECKPOINT MANAGEMENT METHODS
    # =============================================================================

    def get_fetching_checkpoint(self) -> FetchingCheckpoint:
        """Get specialized fetching checkpoint manager for this data type."""
        return FetchingCheckpoint(
            self.hierarchical_checkpoint_manager, self.data_type_name
        )

    def clear_checkpoints(self, phases: list[FetchingPhase] | None = None):
        """Clear checkpoints for this data type."""
        if phases is None:
            phases = [
                FetchingPhase.LIST_ITEMS,
                FetchingPhase.FULL_DATA,
                FetchingPhase.RELATED_DATA,
            ]

        for phase in phases:
            self.hierarchical_checkpoint_manager.reset_checkpoint(
                ProcessingStage.FETCHING, phase.value, self.data_type_name
            )

        logger.info(
            f"Cleared checkpoints for {self.data_type_name}: {[p.value for p in phases]}"
        )

    def list_checkpoints(self) -> dict[str, Any]:
        """List all checkpoints for this data type."""
        return self.hierarchical_checkpoint_manager.get_progress_summary(
            self.data_type_name
        )

    def get_checkpoint_stats(self) -> dict[str, Any]:
        """Get detailed checkpoint statistics for this data type."""
        fetching_checkpoint = self.get_fetching_checkpoint()

        stats = {}
        for phase in FetchingPhase:
            checkpoint = fetching_checkpoint.get_checkpoint(phase)
            stats[phase.value] = {
                "processed_items": checkpoint.processed_items,
                "failed_items": checkpoint.failed_items,
                "total_items": checkpoint.total_items,
                "current_offset": checkpoint.current_offset,
                "current_item_id": checkpoint.current_item_id,
                "current_endpoint": checkpoint.current_endpoint,
                "last_updated": checkpoint.updated_at.isoformat()
                if checkpoint.updated_at
                else None,
            }

        return stats

    async def resume_from_checkpoint(self) -> bool:
        """Check if we can resume from an existing checkpoint."""
        fetching_checkpoint = self.get_fetching_checkpoint()

        # Check list items checkpoint
        list_checkpoint = fetching_checkpoint.get_checkpoint(FetchingPhase.LIST_ITEMS)
        if list_checkpoint.processed_items > 0:
            logger.info(
                f"Found checkpoint for {self.data_type_name}: {list_checkpoint.processed_items} items processed"
            )
            return True

        return False

    # =============================================================================
    # MAIN PROCESSING METHOD - Delegates to OptimizedParallelProcessor
    # =============================================================================

    async def process_items(
        self,
        from_date: str | None = None,
        to_date: str | None = None,
        limit: int = 250,
        max_items: int | None = None,
        batch_size: int = 50,
        enable_parallelization: bool = True,
        chunk_size: int = 5000,
        resume_from_checkpoint: bool = True,
        **kwargs,
    ) -> dict[str, Any]:
        """
        Main processing method that uses OptimizedParallelProcessor for parallel execution.
        """
        logger.info(f"Starting {self.data_type_name} processing")

        # Check for existing checkpoint
        can_resume = (
            await self.resume_from_checkpoint() if resume_from_checkpoint else False
        )

        if can_resume:
            logger.info(f"Resuming {self.data_type_name} processing from checkpoint")
        else:
            logger.info(f"Starting fresh {self.data_type_name} processing")

        if enable_parallelization and self.api_keys and len(self.api_keys) > 1:
            # Use OptimizedParallelProcessor for parallel processing
            processor = OptimizedParallelProcessor(
                api_keys=self.api_keys,
                client_class=self.client.__class__,
                db_pool=self.db_pool,
                chunk_size=chunk_size,
            )

            # Process using the optimized processor
            # It will call our phase methods below
            result = await processor.process_data_type(
                fetcher=self,  # Pass ourselves as the fetcher
                data_type=self.data_type_name,
                from_date=from_date,
                to_date=to_date,
                limit=limit,
                resume_from_checkpoint=resume_from_checkpoint,
                **kwargs,
            )

            return result
        else:
            # Fall back to sequential processing
            return await self._process_sequentially(
                from_date,
                to_date,
                limit,
                max_items,
                batch_size,
                resume_from_checkpoint,
                **kwargs,
            )

    # =============================================================================
    # INTERFACE METHODS FOR OptimizedParallelProcessor
    # =============================================================================

    async def fetch_phase_1_data_with_client(
        self,
        client,
        from_date=None,
        to_date=None,
        limit=None,
        offset=None,
        single_page_only=False,
        **kwargs,
    ) -> AsyncIterator[list[dict]]:
        """
        Phase 1: Fetch list data with specific client.
        CHECKPOINT-AWARE: Filters out already-processed items.
        """
        if not self._plugin:
            logger.error(f"No plugin available for {self.data_type_name}")
            return

        try:
            # Get checkpoint manager
            fetching_checkpoint = self.get_fetching_checkpoint()

            # Fetch data using plugin
            data_batches = await self._plugin.fetch_list_data(
                api_client=client,
                from_date=from_date,
                to_date=to_date,
                limit=limit,
                offset=offset,
                single_page_only=single_page_only,
                **kwargs,
            )

            if data_batches:
                # Filter out already-processed items
                filtered_batch = []
                skipped_count = 0

                for item in data_batches:
                    # Extract item ID
                    item_id = await self.extract_item_id(item)

                    # Check if already processed
                    if fetching_checkpoint.should_skip_list_item(item_id):
                        skipped_count += 1
                        logger.debug(f"Skipping already processed list item: {item_id}")
                        continue

                    filtered_batch.append(item)

                if skipped_count > 0:
                    logger.info(
                        f"Skipped {skipped_count} already-processed items in Phase 1"
                    )

                # Only yield if there are unprocessed items
                if filtered_batch:
                    yield filtered_batch
                else:
                    logger.debug("All items in batch were already processed")

        except Exception as e:
            logger.error(f"Phase 1 fetch failed: {e}")
            raise

    async def fetch_phase_2_data_with_client(
        self, item_url: str, client
    ) -> dict[str, Any] | None:
        """
        Phase 2: Fetch detailed data with specific client.
        CHECKPOINT-AWARE: Returns None if already processed.
        """
        if not self._plugin:
            return None

        try:
            # Extract item ID from URL to check checkpoint
            # This is a bit tricky since we don't have the full item yet
            # We could parse the URL or maintain a mapping
            item_id = item_url.split("/")[-1] if item_url else None

            if item_id:
                fetching_checkpoint = self.get_fetching_checkpoint()
                if fetching_checkpoint.should_skip_full_data(item_id):
                    logger.debug(f"Skipping already processed Phase 2 item: {item_id}")
                    return None

            # Fetch the data
            return await self._plugin.fetch_detailed_data(client, item_url)

        except Exception as e:
            logger.error(f"Phase 2 fetch failed for {item_url}: {e}")
            return None

    async def fetch_phase_3_data_with_client(
        self, detailed_data: dict[str, Any], client
    ) -> list[dict[str, Any]]:
        """
        Phase 3: Fetch related data with specific client.
        CHECKPOINT-AWARE: Only fetches unprocessed endpoints.
        """
        if not self._plugin or not detailed_data:
            return []

        try:
            # Get item ID for checkpoint checking
            item_id = await self.extract_item_id(detailed_data)
            fetching_checkpoint = self.get_fetching_checkpoint()

            # Get all available methods
            result = await self._plugin.fetch_related_data(client, detailed_data)
            if not isinstance(result, list):
                return []

            # Filter out already-processed endpoints
            filtered_result = []
            for related_item in result:
                endpoint = related_item.get("type", "unknown")

                # Check if this endpoint was already processed
                if fetching_checkpoint.should_skip_related_endpoint(item_id, endpoint):
                    logger.debug(
                        f"Skipping already processed endpoint {endpoint} for {item_id}"
                    )
                    continue

                filtered_result.append(related_item)

            if len(result) != len(filtered_result):
                logger.info(
                    f"Filtered {len(result) - len(filtered_result)} already-processed "
                    f"endpoints for {item_id}"
                )

            return filtered_result

        except Exception as e:
            logger.error(f"Phase 3 fetch failed: {e}")
            return []

    async def fetch_phase_4_data(
        self, related_data: dict[str, Any], client
    ) -> list[dict[str, Any]]:
        """Fetch Phase 4 data."""
        if not self._plugin or not related_data:
            return []

        try:
            return await self._plugin.fetch_full_related_data(client, related_data)
        except Exception as e:
            logger.error(f"Phase 4 fetch failed: {e}")
            return []

    # =============================================================================
    # STORAGE METHODS (called by OptimizedParallelProcessor)
    # =============================================================================

    async def store_phase_1_data(self, data: dict, batch_id: str | None = None) -> None:
        """Store Phase 1 list data with checkpoint marking."""
        if not self.db_pool:
            logger.warning("No database pool available for storing Phase 1 data")
            return

        # Extract item ID
        item_id = await self.extract_preliminary_item_id(data)

        # Double-check we should process this (defensive programming)
        fetching_checkpoint = self.get_fetching_checkpoint()
        if fetching_checkpoint.should_skip_list_item(item_id):
            logger.warning(
                f"Attempted to store already-processed Phase 1 item: {item_id}"
            )
            return

        schema_name = self.get_default_schema()

        # Store the data
        await self.store_raw_data(
            schema=schema_name,
            table=f"{self.data_type_name}_list_raw",
            items=[data],
            batch_id=batch_id,
        )

        # Mark as processed AFTER successful storage
        if item_id:
            fetching_checkpoint.mark_list_item_processed(item_id)
            logger.debug(f"Marked Phase 1 item as processed: {item_id}")

    async def store_phase_2_data(self, data: dict, batch_id: str | None = None) -> None:
        """Store Phase 2 full data with checkpoint marking."""
        if not self.db_pool:
            logger.warning("No database pool available for storing Phase 2 data")
            return

        # Extract item ID
        item_id = await self.extract_item_id(data)

        # Double-check we should process this
        fetching_checkpoint = self.get_fetching_checkpoint()
        if fetching_checkpoint.should_skip_full_data(item_id):
            logger.warning(
                f"Attempted to store already-processed Phase 2 item: {item_id}"
            )
            return

        schema_name = self.get_default_schema()

        # Store the data
        await self.store_raw_data(
            schema=schema_name,
            table=f"{self.data_type_name}_raw",
            items=[data],
            batch_id=batch_id,
        )

        # Mark as processed AFTER successful storage
        if item_id:
            fetching_checkpoint.mark_full_data_processed(item_id)
            logger.debug(f"Marked Phase 2 item as processed: {item_id}")

    async def store_phase_3_data(
        self, related_data_list: list[dict], parent_id: str, batch_id: str | None = None
    ) -> None:
        """Store Phase 3 related data with checkpoint marking."""
        if not self.db_pool:
            logger.warning("No database pool available for storing Phase 3 data")
            return

        if not related_data_list:
            logger.debug("No Phase 3 related data to store")
            return

        logger.info(
            f"Storing Phase 3 data: {len(related_data_list)} related items for parent {parent_id}"
        )

        fetching_checkpoint = self.get_fetching_checkpoint()

        # Process each type of related data
        for related_item in related_data_list:
            method_name = related_item.get("type", "unknown")
            data_items = related_item.get("data", [])

            if not data_items:
                continue

            # Double-check this endpoint wasn't already processed
            if fetching_checkpoint.should_skip_related_endpoint(parent_id, method_name):
                logger.warning(
                    f"Attempted to store already-processed endpoint {method_name} for {parent_id}"
                )
                continue

            # Extract table suffix from method name
            table_suffix = self._get_table_suffix_from_method(method_name)

            # Add parent reference to each item
            for record in data_items:
                if isinstance(record, dict):
                    record[self.id_field] = parent_id

            schema_name = self.get_default_schema()
            table_name = f"{self.data_type_name}_{table_suffix}_raw"

            # Store the data
            await self.store_raw_data(
                schema=schema_name,
                table=table_name,
                items=data_items,
                batch_id=batch_id,
                parent_source_doc_id=parent_id,
            )

            # Mark endpoint as processed AFTER successful storage
            fetching_checkpoint.mark_related_endpoint_processed(parent_id, method_name)
            logger.debug(f"Marked endpoint {method_name} as processed for {parent_id}")

    async def store_phase_4_data(
        self, related_data_list: list[dict], parent_id: str, batch_id: str | None = None
    ) -> None:
        """Store Phase 4 full related data."""
        if not self.db_pool or not related_data_list:
            return

        schema_name = self.get_default_schema()
        await self.store_raw_data(
            schema=schema_name,
            table=f"{self.data_type_name}_raw_granules",
            items=related_data_list,
            batch_id=batch_id,
            parent_source_doc_id=parent_id,
        )

    # =============================================================================
    # UTILITY METHODS
    # =============================================================================

    async def extract_item_id(self, item_data: dict[str, Any]) -> str:
        """Extract ID from item data using plugin."""
        if self._plugin and hasattr(self._plugin, "extract_item_id"):
            result = self._plugin.extract_item_id(item_data)
            if hasattr(result, "__await__"):
                return await result
            return result

        # Fallback
        return (
            item_data.get("packageId")
            or item_data.get("number")
            or item_data.get("id")
            or "unknown"
        )

    async def extract_preliminary_item_id(self, item_data: dict[str, Any]) -> str:
        """Extract preliminary ID from item data (for Phase 1)."""
        if self._plugin and hasattr(self._plugin, "_extract_preliminary_item_id"):
            result = self._plugin._extract_preliminary_item_id(item_data)
            if hasattr(result, "__await__"):
                return await result
            return result

        # Fallback to basic ID extraction
        return await self.extract_item_id(item_data)

    def get_default_schema(self) -> str:
        """Get the default schema name for this data type."""
        try:
            registry = get_consolidated_registry()
            schema_names = registry.get_schema_names(self.data_type_name)
            return schema_names.get("raw", "bicam_raw_unknown")
        except Exception:
            return "bicam_raw_unknown"

    async def _get_pagination_metadata(
        self, from_date: str | None, to_date: str | None, limit: int | None, **kwargs
    ) -> dict[str, Any]:
        """
        Get pagination metadata for the OptimizedParallelProcessor.
        This is called by the processor to determine work distribution.
        """
        try:
            # Fetch first page to get total count
            async for first_batch in self.fetch_phase_1_data_with_client(
                self.client,
                from_date=from_date,
                to_date=to_date,
                limit=1,  # Just need metadata
                single_page_only=True,
                **kwargs,
            ):
                # Extract pagination info from response
                if hasattr(self.client, "last_response_metadata"):
                    metadata = self.client.last_response_metadata
                    if metadata and "pagination" in metadata:
                        return {
                            "total_count": metadata["pagination"].get("count", 0),
                            "count_per_page": limit or 250,
                            "has_data": True,
                        }

                # Fallback: estimate from first batch
                return {
                    "total_count": len(first_batch) * 100,  # Rough estimate
                    "count_per_page": limit or 250,
                    "has_data": len(first_batch) > 0,
                }

        except Exception as e:
            logger.error(f"Failed to get pagination metadata: {e}")
            return {
                "total_count": 0,
                "count_per_page": limit or 250,
                "has_data": False,
                "error": str(e),
            }

    @property
    def id_field(self):
        """Get the ID field name for this data type."""
        return f"{self.data_type_name}_id" if self.data_type_name else "id"

    def _get_table_suffix_from_method(self, method_name: str) -> str:
        """Extract table suffix from method name."""
        if method_name.startswith("get_"):
            suffix = method_name[4:]
            if suffix.startswith(f"{self.data_type_name}_"):
                suffix = suffix[len(f"{self.data_type_name}_") :]
            return suffix
        return method_name

    async def store_raw_data(
        self,
        schema: str,
        table: str,
        items: list[dict],
        batch_id: str | None = None,
        parent_source_doc_id: str | None = None,
        **kwargs,
    ) -> None:
        """Store raw data in the database."""
        if not items:
            logger.debug("No items to store")
            return

        if not self.db_pool:
            logger.warning("No database pool available for storing raw data")
            return

        logger.info(f"Storing {len(items)} items to {schema}.{table}")

        async with self.db_pool.acquire() as conn:
            # Ensure table exists
            await self._ensure_table(conn, schema, table)

            # Insert data
            stored_count = 0
            for item in items:
                source_doc_id = await self._get_source_doc_id(
                    item, table, parent_source_doc_id
                )

                prepared = {
                    "id_uuid": str(uuid.uuid4()),
                    "url": item.get("url"),
                    "batch_id": batch_id or str(uuid.uuid4()),
                    "scraped_at": datetime.now(UTC),
                    "payload": json.dumps(item, default=str),
                    "source_doc_id": source_doc_id,
                    "etl_batch_id": batch_id or str(uuid.uuid4()),
                }

                await conn.execute(
                    f"""
                    INSERT INTO {schema}.{table}
                    (id_uuid, url, batch_id, scraped_at, payload, source_doc_id, etl_batch_id)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    ON CONFLICT (id_uuid) DO UPDATE SET
                        scraped_at = EXCLUDED.scraped_at,
                        payload = EXCLUDED.payload
                    """,
                    *prepared.values(),
                )
                stored_count += 1

            logger.info(f"Successfully stored {stored_count} items to {schema}.{table}")

    async def _get_source_doc_id(
        self, item: dict, table: str, parent_id: str | None
    ) -> str:
        """Get appropriate source doc ID based on phase."""
        if table.endswith("_list_raw"):
            # Phase 1: Use preliminary ID
            return await self.extract_preliminary_item_id(item)
        elif parent_id:
            # Phase 3: Use parent ID
            return parent_id
        else:
            # Phase 2: Use full item ID
            return await self.extract_item_id(item)

    async def _ensure_table(self, conn, schema: str, table: str) -> None:
        """Ensure table exists in the database."""
        await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                id_uuid TEXT PRIMARY KEY,
                url TEXT,
                batch_id TEXT,
                scraped_at TIMESTAMPTZ,
                payload JSONB,
                source_doc_id TEXT,
                etl_batch_id TEXT
            )
        """)

    # =============================================================================
    # SEQUENTIAL PROCESSING (fallback when not using parallel)
    # =============================================================================

    async def _process_sequentially(
        self,
        from_date: str | None,
        to_date: str | None,
        limit: int | None,
        max_items: int | None,
        batch_size: int,
        resume_from_checkpoint: bool = True,
        **kwargs,
    ) -> dict[str, Any]:
        """Process items sequentially (non-parallel fallback) with checkpoint support."""
        logger.info("Processing sequentially")

        stats = {
            "total_processed": 0,
            "errors": 0,
            "start_time": datetime.now(UTC),
        }

        processed_count = 0
        fetching_checkpoint = self.get_fetching_checkpoint()

        # Check if resuming from checkpoint
        if resume_from_checkpoint:
            list_checkpoint = fetching_checkpoint.get_checkpoint(
                FetchingPhase.LIST_ITEMS
            )
            processed_count = list_checkpoint.processed_items
            logger.info(
                f"Resuming from checkpoint: {processed_count} items already processed"
            )

        try:
            # Process each item through all phases
            async for batch in self.fetch_phase_1_data_with_client(
                self.client, from_date, to_date, limit, **kwargs
            ):
                for item in batch:
                    if max_items and processed_count >= max_items:
                        break

                    try:
                        # Check if already processed (for checkpoint resume)
                        item_id = await self.extract_item_id(item)
                        if (
                            resume_from_checkpoint
                            and fetching_checkpoint.should_skip_list_item(item_id)
                        ):
                            logger.debug(f"Skipping already processed item: {item_id}")
                            continue

                        # Phase 1: Store list data
                        await self.store_phase_1_data(item)

                        # Phase 2: Fetch and store detailed data
                        if url := item.get(
                            "url"
                        ) and not fetching_checkpoint.should_skip_full_data(item_id):
                            detailed = await self.fetch_phase_2_data_with_client(
                                url, self.client
                            )
                            if detailed:
                                await self.store_phase_2_data(detailed)

                                # Phase 3: Fetch and store related data
                                related = await self.fetch_phase_3_data_with_client(
                                    detailed, self.client
                                )
                                if related:
                                    full_item_id = await self.extract_item_id(detailed)
                                    await self.store_phase_3_data(related, full_item_id)

                        processed_count += 1
                        stats["total_processed"] += 1

                    except Exception as e:
                        logger.error(f"Error processing item: {e}")
                        stats["errors"] += 1

                        # Log error in checkpoint system
                        fetching_checkpoint.cm.log_error(
                            ProcessingStage.FETCHING,
                            FetchingPhase.LIST_ITEMS.value,
                            self.data_type_name,
                            item_id,
                            str(e),
                            error_type="sequential_processing",
                        )

                if max_items and processed_count >= max_items:
                    break

            stats["duration"] = (
                datetime.now(UTC) - stats["start_time"]
            ).total_seconds()

            # Save final checkpoint
            self.hierarchical_checkpoint_manager.flush_all_caches()

            return stats

        except Exception as e:
            logger.error(f"Sequential processing failed: {e}")
            stats["error"] = str(e)
            return stats


# Add periodic checkpoint flushing
async def periodic_checkpoint_flush(self):
    """Periodically flush checkpoint caches to disk."""
    if hasattr(self, "hierarchical_checkpoint_manager"):
        self.hierarchical_checkpoint_manager.flush_all_caches()
        logger.debug("Flushed checkpoint caches to disk")


# Add method to get resume information for parallel processor
async def get_resume_offset(self) -> int:
    """Get the offset to resume from based on checkpoint."""
    fetching_checkpoint = self.get_fetching_checkpoint()
    list_checkpoint = fetching_checkpoint.get_checkpoint(FetchingPhase.LIST_ITEMS)

    if list_checkpoint.processed_items > 0:
        logger.info(
            f"Found checkpoint: {list_checkpoint.processed_items} items processed, "
            f"resuming from offset {list_checkpoint.current_offset}"
        )
        return list_checkpoint.current_offset

    return 0


# Add periodic checkpoint saving
async def save_checkpoint_progress(self, phase: FetchingPhase, offset: int = None):
    """Save current checkpoint progress."""
    fetching_checkpoint = self.get_fetching_checkpoint()
    checkpoint = fetching_checkpoint.get_checkpoint(phase)

    if offset is not None:
        checkpoint.current_offset = offset

    fetching_checkpoint.save_checkpoint(checkpoint)

    # Periodically flush caches to disk
    if checkpoint.processed_items % 1000 == 0:
        fetching_checkpoint.cm.flush_all_caches()
        logger.debug(
            f"Flushed checkpoint caches after {checkpoint.processed_items} items"
        )
