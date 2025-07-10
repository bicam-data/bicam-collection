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
        data_source: str | None = None,
    ):
        self.client = client
        self.db_pool = db_pool
        self.data_type_name = data_type_name
        self.checkpoint_manager = checkpoint_manager
        self.run_manager = run_manager
        self.api_keys = api_keys or []
        self.config = config
        self.resource_coordinator = resource_coordinator
        self._data_source = data_source

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

        logger.info(
            f"StreamlinedFetcher initialized for {data_type_name} (source: {data_source})"
        )

    @classmethod
    async def from_coordinator(
        cls, resource_coordinator, data_type_name=None, data_source: str | None = None
    ):
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
            data_source=data_source,
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

        # Always get last processed date from metadata if from_date is not provided
        if not from_date and hasattr(self.client, "access_last_processed_date"):
            last_processed = await self.client.access_last_processed_date(
                self.data_type_name
            )
            logger.info(
                f"Retrieved last processed date from database: '{last_processed}'"
            )
            if last_processed:
                from_date = last_processed
                logger.info(
                    f"Using last processed date for {self.data_type_name}: {from_date}"
                )
            else:
                from datetime import datetime, timedelta

                from_date = (datetime.now(UTC) - timedelta(days=30)).strftime(
                    "%Y-%m-%d"
                )
                logger.info(
                    f"No last processed date for {self.data_type_name}, using fallback: {from_date}"
                )
        if not to_date:
            from datetime import datetime

            to_date = datetime.now(UTC).strftime("%Y-%m-%d")
            logger.info(f"Using current date as end date: {to_date}")

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
            item_id = await self.extract_item_id(
                detailed_data, granule=self.data_source == "govinfo"
            )
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
                if fetching_checkpoint.should_skip_related_endpoint(
                    item_id, endpoint, self.data_source
                ):
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

    async def fetch_phase_4_data_with_client(
        self, related_data: dict[str, Any], client
    ) -> list[dict[str, Any]]:
        if not self._plugin:
            return None

        try:
            # Extract item ID from URL to check checkpoint
            # This is a bit tricky since we don't have the full item yet
            # We could parse the URL or maintain a mapping
            item_id = await self.extract_item_id(related_data, granule=True)

            if item_id:
                fetching_checkpoint = self.get_fetching_checkpoint()
                if fetching_checkpoint.should_skip_full_related_data(
                    item_id, self.data_source
                ):
                    logger.debug(f"Skipping already processed Phase 4 item: {item_id}")
                    return None

            # Fetch the data
            return await self._plugin.fetch_full_related_data(client, related_data)

        except Exception as e:
            logger.error(f"Phase 4 fetch failed for {related_data}: {e}")
            return None

    async def fetch_specific_related_tables_from_existing_data(
        self,
        related_tables: list[str],
        batch_size: int = 100,
        limit: int = None,
        from_date: str = None,
        to_date: str = None,
    ) -> dict[str, Any]:
        """
        Fetch specific related tables from existing Phase 2 data using parallel processing.

        This method delegates to OptimizedParallelProcessor for true parallel execution:
        1. Queries the database for existing Phase 2 data
        2. Creates parallel workers to fetch related tables concurrently
        3. Uses the full optimized storage infrastructure
        4. Provides proper batching and resource management with parallel execution

        Args:
            related_tables: List of related table names to fetch (e.g., ['texts', 'actions'])
            batch_size: Number of items to process in each batch
            limit: Maximum number of items to process (optional)
            from_date: Start date filter (optional)
            to_date: End date filter (optional)

        Returns:
            Dictionary with processing results and metrics
        """
        if not self.data_type_name:
            logger.error("No data type specified for fetcher")
            return {"error": "No data type specified"}

        if not self.db_pool:
            logger.error("No database pool available")
            return {"error": "No database pool available"}

        if not self._plugin:
            logger.error("No plugin available for fetching related data")
            return {"error": "No plugin available"}

        # Ensure we have a client for API calls
        if not self.client:
            logger.info("No client available, creating one...")
            try:
                if self.resource_coordinator:
                    clients = await self.resource_coordinator.get_api_clients(
                        self.data_type_name
                    )
                    if clients:
                        self.client = clients[0]
                        logger.info("Successfully created API client")
                    else:
                        logger.error(
                            "Failed to create API client - no clients returned"
                        )
                        return {"error": "Failed to create API client"}
                else:
                    logger.error("No resource coordinator available to create client")
                    return {"error": "No resource coordinator available"}
            except Exception as e:
                logger.error(f"Failed to create API client: {e}")
                return {"error": f"Failed to create API client: {e}"}

        logger.info(
            f"Delegating related tables fetch to OptimizedParallelProcessor for {self.data_type_name}"
        )

        # Import here to avoid circular imports
        from .processing.optimized_processor import OptimizedParallelProcessor

        # Create OptimizedParallelProcessor for parallel execution
        processor = OptimizedParallelProcessor(
            api_keys=self.api_keys,
            client_class=self.client.__class__ if self.client else None,
            db_pool=self.db_pool,
        )

        # Use the processor's parallel method
        return await processor.fetch_related_tables_from_existing_data(
            fetcher=self,
            data_type=self.data_type_name,
            related_tables=related_tables,
            batch_size=batch_size,
            limit=limit,
            from_date=from_date,
            to_date=to_date,
        )

    async def _fetch_specific_related_tables_for_item(
        self, detailed_data: dict, related_tables: list[str]
    ) -> list[dict]:
        """Fetch only the specified related tables for a single item."""
        related_data = []

        # Ensure detailed_data is a dictionary (parse JSON if it's a string)
        if isinstance(detailed_data, str):
            try:
                import json

                detailed_data = json.loads(detailed_data)
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse detailed_data JSON: {e}")
                return []

        if not isinstance(detailed_data, dict):
            logger.error(f"detailed_data is not a dictionary: {type(detailed_data)}")
            return []

        # Get the custom logic plugin
        custom_logic = self._plugin._get_custom_logic_plugin(self.data_type_name)
        if not custom_logic:
            logger.error(f"No custom logic plugin available for {self.data_type_name}")
            return []

        # Call only the requested related methods
        for table in related_tables:
            method_name = f"get_{self.data_type_name}_{table}"

            if hasattr(custom_logic, method_name):
                try:
                    method = getattr(custom_logic, method_name)
                    if callable(method):
                        logger.debug(f"Calling {method_name} for {self.data_type_name}")

                        # Call the method with detailed_data and client
                        result = await method(detailed_data, self.client)

                        if result:
                            # Extract item ID for the parent record
                            item_id = custom_logic.extract_item_id(detailed_data)
                            related_data.append(
                                {
                                    "type": method_name,
                                    f"{self.data_type_name}_id": item_id,
                                    "data": result,
                                    "method": method_name,
                                }
                            )
                            logger.debug(f"{method_name} returned {len(result)} items")
                        else:
                            logger.debug(f"{method_name} returned no data")

                except Exception as e:
                    logger.error(f"Error calling {method_name}: {e}")
            else:
                logger.warning(f"Method {method_name} not found in custom logic")

        return related_data

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
            if fetching_checkpoint.should_skip_related_endpoint(
                parent_id, method_name, self.data_source
            ):
                logger.warning(
                    f"Attempted to store already-processed endpoint {method_name} for {parent_id}"
                )
                continue

            # Handle table naming for different data sources
            if self.data_source == "govinfo" and method_name.endswith("_granules"):
                # For GovInfo, the method_name is already the full granule type
                # Just add "_list_raw" for Phase 3 (granule list data)
                table_name = f"{method_name}_list_raw"
            else:
                # For Congressional data, use the existing logic
                table_suffix = self._get_table_suffix_from_method(method_name)
                table_name = f"{self.data_type_name}_{table_suffix}_raw"

            # Add parent reference to each item
            for record in data_items:
                if isinstance(record, dict):
                    record[self.id_field] = parent_id

            schema_name = self.get_default_schema()

            # Store the data
            await self.store_raw_data(
                schema=schema_name,
                table=table_name,
                items=data_items,
                batch_id=batch_id,
                parent_source_doc_id=parent_id,
            )

            # Mark endpoint as processed AFTER successful storage
            fetching_checkpoint.mark_related_endpoint_processed(
                parent_id, method_name, self.data_source
            )
            logger.debug(f"Marked endpoint {method_name} as processed for {parent_id}")

    async def store_phase_4_data(
        self, related_data_list: list[dict], parent_id: str, batch_id: str | None = None
    ) -> None:
        """Store Phase 4 full related data."""
        if not self.db_pool or not related_data_list:
            return

        # Use consistent naming scheme
        granule_table_name = f"{self.data_type_name}_granules"

        schema_name = self.get_default_schema()
        await self.store_raw_data(
            schema=schema_name,
            table=f"{granule_table_name}_raw",
            items=related_data_list,
            batch_id=batch_id,
            parent_source_doc_id=parent_id,
        )

    # =============================================================================
    # UTILITY METHODS
    # =============================================================================

    async def extract_item_id(self, item_data: dict[str, Any], **kwargs) -> str:
        """Extract ID from item data using plugin."""
        if self._plugin and hasattr(self._plugin, "extract_item_id"):
            result = self._plugin.extract_item_id(item_data, **kwargs)
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
            logger.info(
                f"Getting pagination metadata for {self.data_type_name} from {from_date} to {to_date}"
            )

            # Get doc_class from plugin config if available
            doc_class = None
            if self._plugin and hasattr(self._plugin, "_get_config"):
                try:
                    config = await self._plugin._get_config()
                    if hasattr(config, "api") and hasattr(config.api, "doc_class"):
                        doc_class = config.api.doc_class
                        if doc_class:
                            logger.info(f"Using doc_class from config: {doc_class}")
                            logger.info(
                                f"This will filter API results to only include documents with doc_class={doc_class}"
                            )
                        else:
                            logger.info(
                                "No doc_class specified in config - will fetch all documents"
                            )
                except Exception as e:
                    logger.debug(f"Could not get doc_class from plugin config: {e}")

            # Fetch first page to get total count
            async for first_batch in self.fetch_phase_1_data_with_client(
                self.client,
                from_date=from_date,
                to_date=to_date,
                limit=1,  # Just need metadata
                single_page_only=True,
                doc_class=doc_class,  # Pass doc_class to the plugin
                **kwargs,
            ):
                logger.info(
                    f"First batch received: {len(first_batch) if first_batch else 0} items"
                )

                # Extract pagination info from response
                if hasattr(self.client, "last_response_metadata"):
                    metadata = self.client.last_response_metadata
                    logger.info(f"Client metadata: {metadata}")
                    if metadata and metadata.get("pagination"):
                        result = {
                            "total_count": metadata["pagination"].get("count", 0),
                            "count_per_page": limit or 250,
                            "has_data": True,
                        }
                        logger.info(f"Returning pagination metadata: {result}")
                        return result
                    else:
                        logger.warning(f"Metadata missing pagination info: {metadata}")

                # Fallback: estimate from first batch
                result = {
                    "total_count": len(first_batch) * 100,  # Rough estimate
                    "count_per_page": limit or 250,
                    "has_data": len(first_batch) > 0,
                }
                logger.info(f"Returning fallback metadata: {result}")
                return result

        except Exception as e:
            logger.error(f"Failed to get pagination metadata: {e}")
            result = {
                "total_count": 0,
                "count_per_page": limit or 250,
                "has_data": False,
                "error": str(e),
            }
            logger.info(f"Returning error metadata: {result}")
            return result

    @property
    def id_field(self):
        """Get the ID field name for this data type."""
        return f"{self.data_type_name}_id" if self.data_type_name else "id"

    @property
    def data_source(self) -> str | None:
        """Return the data source for this fetcher."""
        if self._data_source:
            return self._data_source

        # Fallback to registry if not set
        if self.data_type_name:
            try:
                registry = get_consolidated_registry()
                return registry.get_data_source(self.data_type_name)
            except Exception as e:
                logger.warning(
                    f"Could not get data source from registry for {self.data_type_name}: {e}"
                )

        return None

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

    async def cleanup(self):
        """Cleanup resources and update metadata tables."""
        logger.info("StreamlinedFetcher cleanup started")

        try:
            # Flush checkpoint caches
            if hasattr(self, "hierarchical_checkpoint_manager"):
                self.hierarchical_checkpoint_manager.flush_all_caches()
                logger.debug("Flushed checkpoint caches to disk")

            # Update metadata tables with last processed count and total count
            if (
                self.data_type_name
                and self.client
                and hasattr(self.client, "update_last_processed_date")
            ):
                try:
                    # Get checkpoint stats to determine total count
                    checkpoint_stats = self.get_checkpoint_stats()
                    list_checkpoint = checkpoint_stats.get("list_items", {})

                    # Extract counts from checkpoint data
                    last_processed_count = list_checkpoint.get("processed_items", 0)
                    last_total_count = list_checkpoint.get("total_items", 0)

                    # If we don't have total count from checkpoint, try to get it from pagination metadata
                    if last_total_count == 0:
                        try:
                            # Get pagination metadata to determine total count
                            pagination_metadata = await self._get_pagination_metadata(
                                from_date=None, to_date=None, limit=250
                            )
                            last_total_count = pagination_metadata.get("total_count", 0)

                            # For GovInfo data types, if there's no data, we should still update the count
                            # to indicate that we've processed 0 items (no new data)
                            if last_total_count == 0 and not pagination_metadata.get(
                                "has_data"
                            ):
                                logger.info(
                                    f"No data available for {self.data_type_name}, updating count to 0"
                                )
                                last_total_count = (
                                    0  # Explicitly set to 0 to indicate no data
                                )
                        except Exception as e:
                            logger.warning(
                                f"Could not get pagination metadata for total count: {e}"
                            )

                    # Get the latest processed date from the most recent items
                    latest_date = await self._get_latest_processed_date()

                    # For GovInfo data types, if there's no data but we have a total count of 0,
                    # we should still update the metadata to indicate we checked
                    if latest_date:
                        # Update the metadata table with the latest date
                        success = await self.client.update_last_processed_date(
                            data_type=self.data_type_name,
                            date=latest_date,
                            total_count=last_total_count,
                        )

                        if success:
                            logger.info(
                                f"Updated metadata for {self.data_type_name}: "
                                f"last_processed_count={last_processed_count}, "
                                f"last_total_count={last_total_count}, "
                                f"latest_date={latest_date}"
                            )
                        else:
                            logger.warning(
                                f"Failed to update metadata for {self.data_type_name}"
                            )
                    elif last_total_count == 0 and self.data_source == "govinfo":
                        # For GovInfo data types with no data, use current date to indicate we checked
                        from datetime import UTC, datetime

                        current_date = datetime.now(UTC).strftime("%Y-%m-%d")

                        success = await self.client.update_last_processed_date(
                            data_type=self.data_type_name,
                            date=current_date,
                            total_count=0,
                        )

                        if success:
                            logger.info(
                                f"Updated metadata for {self.data_type_name} (no data found): "
                                f"last_processed_count={last_processed_count}, "
                                f"last_total_count=0, "
                                f"latest_date={current_date}"
                            )
                        else:
                            logger.warning(
                                f"Failed to update metadata for {self.data_type_name}"
                            )
                    else:
                        logger.info(
                            f"No new processed date found for {self.data_type_name}, "
                            f"keeping existing metadata unchanged"
                        )

                except Exception as e:
                    logger.warning(f"Error updating metadata during cleanup: {e}")

            logger.info("StreamlinedFetcher cleanup completed")

        except Exception as e:
            logger.error(f"Error during StreamlinedFetcher cleanup: {e}")

    async def _get_latest_processed_date(self) -> str | None:
        """
        Get the latest update_date from the most recent processed items.

        Returns:
            Latest date as a string in the format expected by the metadata table, or None if no dates found
        """
        if not self.db_pool or not self.data_type_name:
            return None

        try:
            # Determine the correct schema based on data source
            schema = (
                "bicam_raw_congressional"
                if self.data_source == "congressional"
                else "bicam_raw_govinfo"
            )

            # Query the raw data table to find the most recent update_date
            async with self.db_pool.acquire() as conn:
                # Try to find the latest date from the raw data
                # Look for common date fields in the payload
                query = f"""
                SELECT
                    payload->>'updateDate' as update_date,
                    payload->>'lastModified' as last_modified,
                    payload->>'updatedate' as updatedate,
                    payload->>'updated' as updated,
                    payload->>'date' as date
                FROM {schema}.{self.data_type_name}_list_raw
                WHERE payload IS NOT NULL
                ORDER BY
                    COALESCE(
                        payload->>'updateDate',
                        payload->>'lastModified',
                        payload->>'updatedate',
                        payload->>'updated',
                        payload->>'date'
                    ) DESC
                LIMIT 1
                """

                result = await conn.fetchrow(query)

                if result:
                    # Find the first non-null date value
                    for field in [
                        "update_date",
                        "last_modified",
                        "updatedate",
                        "updated",
                        "date",
                    ]:
                        if result[field]:
                            latest_date = result[field]
                            logger.debug(
                                f"Found latest date from {field}: {latest_date}"
                            )
                            return latest_date

                # If no date found in raw data, try the full data table
                query = f"""
                SELECT
                    payload->>'updateDate' as update_date,
                    payload->>'lastModified' as last_modified,
                    payload->>'updatedate' as updatedate,
                    payload->>'updated' as updated,
                    payload->>'date' as date
                FROM {schema}.{self.data_type_name}_raw
                WHERE payload IS NOT NULL
                ORDER BY
                    COALESCE(
                        payload->>'updateDate',
                        payload->>'lastModified',
                        payload->>'updatedate',
                        payload->>'updated',
                        payload->>'date'
                    ) DESC
                LIMIT 1
                """

                result = await conn.fetchrow(query)

                if result:
                    # Find the first non-null date value
                    for field in [
                        "update_date",
                        "last_modified",
                        "updatedate",
                        "updated",
                        "date",
                    ]:
                        if result[field]:
                            latest_date = result[field]
                            logger.debug(
                                f"Found latest date from {field}: {latest_date}"
                            )
                            return latest_date

                # No dates found in processed data
                logger.info(
                    f"No date fields found in processed data for {self.data_type_name}"
                )
                return None

        except Exception as e:
            logger.warning(
                f"Error getting latest processed date for {self.data_type_name}: {e}"
            )
            return None


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
