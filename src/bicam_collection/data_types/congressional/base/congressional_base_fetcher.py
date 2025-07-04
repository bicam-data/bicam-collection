"""
Congressional-specific base fetcher implementing the 3-phase pattern.

This module provides Congressional-specific implementations including:
- 3-phase processing pattern (list → full → related)
- Congressional schema defaults
- Congressional progress tracking configuration
- Congressional API patterns
"""

import asyncio
import json
import logging
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg

from ....libs.checkpoint import (
    HierarchicalProgressTracker,
    ProcessingPhase,
    ProcessingStage,
)
from ....libs.run_tracking import RunMetadata, RunType
from ...abstract import AbstractFetcher

logger = logging.getLogger(__name__)


class CongressionalBaseFetcher(AbstractFetcher):
    """
    Congressional-specific base fetcher.

    Implements the Congressional API 3-phase pattern:
    1. fetch_list_data() → bulk endpoints
    2. fetch_full_data() → individual item URLs
    3. fetch_related_data() → related endpoints from full data

    Provides Congressional-specific defaults for schemas and progress tracking.
    """

    def __init__(
        self,
        client,
        db_pool=None,
        data_type_name=None,
        checkpoint_manager=None,
        run_manager=None,
    ):
        super().__init__(
            client, db_pool, data_type_name, checkpoint_manager, run_manager
        )
        (
            self.expected_key,
            self.id_field,
            self.outer_api_field,
        ) = self.get_main_id_configs()

        # Keep a reference to the full typed config for advanced look-ups in
        # get_generic_related_data().  If the registry lookup fails we store
        # None so attribute access elsewhere can be guarded.
        try:
            from bicam_collection.libs.data_type_registry import get_global_registry

            self.main_config = get_global_registry().get_data_type_config(
                data_type_name
            )
        except Exception:
            self.main_config = None

    def setup_progress_tracker(self) -> HierarchicalProgressTracker | None:
        """Congressional-specific progress tracker setup."""
        if not self.checkpoint_manager or not self.data_type_name:
            logger.warning("Cannot setup progress tracker - missing dependencies")
            return None

        self.progress_tracker = HierarchicalProgressTracker.create_for_stage(
            self.checkpoint_manager,
            "congressional",  # Congressional-specific system name
            self.data_type_name,
            ProcessingStage.SCRAPING,
        )
        return self.progress_tracker

    def get_main_id_configs(self) -> tuple[str, str, str]:
        """
        Get API / ID configuration from the new typed config system.
        Returns (expected_key, id_field, outer_api_field).
        """
        # Use typed config via global registry (avoids direct config_manager dependency)
        from bicam_collection.libs.data_type_registry import get_global_registry

        try:
            cfg = get_global_registry().get_data_type_config(self.data_type_name)

            expected_key = cfg.api.expected_key
            outer_api_field = cfg.api.outer_field
            id_field = cfg.id_field

            return expected_key, id_field, outer_api_field

        except Exception as exc:
            logger.error("Config lookup failed for %s: %s", self.data_type_name, exc)
            # Fallback to old heuristic so the system still runs if config is missing
            return self.data_type_name, f"{self.data_type_name}_id", self.data_type_name

    # =============================================================================
    # CONGRESSIONAL-SPECIFIC 3-PHASE PATTERN
    # =============================================================================

    async def fetch_list_data(
        self,
        from_date: str | None = None,
        to_date: str | None = None,
        limit: int | None = 250,
        offset: int | None = None,
        **kwargs,
    ) -> AsyncIterator[list[dict[str, Any]]]:
        """
        Phase 1: Fetch list data from Congressional bulk endpoints.

        Must yield batches of items containing URLs for Phase 2.

        Args:
            from_date: Start date filter (YYYY-MM-DD)
            to_date: End date filter (YYYY-MM-DD)
            limit: Items per page
            offset: Starting offset
            **kwargs: Additional parameters

        Yields:
            Batches of item dictionaries with 'url' fields
        """

        async for batch in self.client.retrieve_data_list(
            data_type=self.outer_api_field,
            from_date=from_date,
            to_date=to_date,
            limit=limit,
            offset=offset,
            **kwargs,
        ):
            yield batch

    async def fetch_list_data_with_client(
        self,
        client,
        from_date: str | None = None,
        to_date: str | None = None,
        limit: int | None = 250,
        offset: int | None = None,
        **kwargs,
    ) -> AsyncIterator[list[dict[str, Any]]]:
        """
        Fetch list data using a specific client.

        This method can be overridden by subclasses to customize list data fetching
        while maintaining compatibility with parallel processing.

        Args:
            client: API client to use for the request
            from_date: Start date filter (YYYY-MM-DD)
            to_date: End date filter (YYYY-MM-DD)
            limit: Items per page
            offset: Starting offset
            **kwargs: Additional parameters

        Yields:
            Batches of item dictionaries with 'url' fields
        """
        async for batch in client.retrieve_data_list(
            data_type=self.outer_api_field,
            from_date=from_date,
            to_date=to_date,
            limit=limit or 250,  # Always maximize API efficiency
            offset=offset,
            **kwargs,
        ):
            yield batch

    async def fetch_full_data(self, item_url: str) -> dict[str, Any] | None:
        """
        Phase 2: Fetch complete item data from Congressional URL.

        Args:
            item_url: Individual item URL from Phase 1

        Returns:
            Complete item data or None on error
        """
        full_data = await self.fetch_full_data_with_client(item_url, self.client)
        if full_data:
            full_data[self.id_field] = self.extract_item_id(full_data)
        return full_data

    async def fetch_full_data_with_client(
        self, item_url: str, client
    ) -> dict[str, Any] | None:
        """
        Fetch complete item data using a specific client.

        This method can be overridden by subclasses to customize full data fetching
        while maintaining compatibility with parallel processing.

        Args:
            item_url: Individual item URL from Phase 1
            client: API client to use for the request

        Returns:
            Complete item data or None on error
        """
        return await client.retrieve_full_data_from_url(
            item_url, expected_key=self.expected_key
        )

    async def fetch_related_data_with_client(
        self, url: str, expected_key_list: list[str], client
    ) -> list[dict[str, Any]]:
        """
        Fetch related data from a URL using a specific client.

        This method can be overridden by subclasses to customize related data fetching
        while maintaining compatibility with parallel processing.

        Args:
            url: URL to fetch related data from
            expected_key_list: List of expected response keys
            client: API client to use for the request

        Returns:
            List of related data items
        """
        return await client.retrieve_related_data_from_url(url, expected_key_list)

    async def fetch_related_data(
        self, full_data: dict[str, Any]
    ) -> dict[str, list[dict[str, Any]]]:
        """
        Phase 3: Fetch related data (optional override).

        Override this method to fetch related data for your data type.

        Args:
            full_data: Complete item data from Phase 2

        Returns:
            Dictionary mapping relation type to list of related records
        """
        related_data = {}

        # Get all related data methods dynamically
        related_methods = self.get_related_data_methods()

        if not related_methods:
            logger.warning(f"No related data methods found for {self.data_type_name}")
            return related_data

        # Get all related data concurrently
        tasks = {}
        for method_name in related_methods:
            method = getattr(self, method_name)
            field_name = method_name.replace(f"get_{self.data_type_name}_", "")
            tasks[field_name] = method(full_data)

        if tasks:
            results = await asyncio.gather(*tasks.values(), return_exceptions=True)

            for field_name, result in zip(tasks.keys(), results, strict=False):
                if isinstance(result, Exception):
                    logger.error(f"Error fetching {field_name}: {result}")
                    related_data[field_name] = []
                else:
                    related_data[field_name] = result or []

        return related_data

    def get_related_data_methods(self) -> list[str]:
        """
        Get list of related data method names for dynamic discovery.

        Override to specify which related data methods to use.
        Default discovers methods starting with f"get_{data_type_name}_"

        Returns:
            List of method names to call for related data
        """
        if not self.data_type_name:
            return []

        methods = []
        for attr_name in dir(self):
            if attr_name.startswith(f"get_{self.data_type_name}_") and callable(
                getattr(self, attr_name)
            ):
                methods.append(attr_name)
        return methods

    # =============================================================================
    # SETUP AND CONFIGURATION
    # =============================================================================

    def set_processing_resource(self, processing_resource) -> None:
        """
        Set the processing resource for accessing parallel API clients and other resources.

        This method is called by the Dagster pipeline to provide access to:
        - Parallel API client sessions
        - Shared database pools
        - Other shared resources

        Args:
            processing_resource: Resource providing parallel clients
        """
        self.processing_resource = processing_resource
        if hasattr(processing_resource, "get_parallel_clients"):
            logger.debug("Processing resource has parallel clients capability")
        else:
            logger.debug("Processing resource does not support parallel clients")

    async def setup_run_tracking(
        self, run_metadata: RunMetadata | None = None
    ) -> str | None:
        """Setup run tracking for this processing session."""
        if not self.run_manager:
            logger.warning("No run manager provided - run tracking disabled")
            return None

        try:
            if not run_metadata:
                run_metadata = RunMetadata(
                    run_id="",
                    run_type=RunType.SCRAPER_CONGRESSIONAL,
                    system_name=f"{self.data_type_name}_fetcher",
                    description=f"Fetch {self.data_type_name} data from Congressional API",
                    data_types=[self.data_type_name] if self.data_type_name else [],
                )

            self.current_run_id = self.run_manager.create_run(run_metadata)
            await self.run_manager.start_run(self.current_run_id)
            logger.info(f"Started run tracking with ID: {self.current_run_id}")
            return self.current_run_id

        except Exception as e:
            logger.error(f"Failed to setup run tracking: {e}")
            return None

    # =============================================================================
    # MAIN PROCESSING METHOD - CONGRESSIONAL 3-PHASE IMPLEMENTATION
    # =============================================================================

    async def process_items(
        self,
        from_date: str | None = None,
        to_date: str | None = None,
        limit: int | None = 250,
        max_items: int | None = None,
        batch_size: int = 50,
        enable_parallelization: bool = False,
        max_concurrent: int = 10,
        redistribute_idle_sessions: bool = True,
        **kwargs,
    ) -> dict[str, Any]:
        """
        Congressional-specific 3-phase processing implementation.

        Args:
            from_date: Start date filter (YYYY-MM-DD)
            to_date: End date filter (YYYY-MM-DD)
            limit: Items per page
            max_items: Maximum total items to process
            batch_size: Items per storage batch
            enable_parallelization: Enable parallel processing
            max_concurrent: Maximum concurrent operations
            redistribute_idle_sessions: Use idle sessions for related data fetching
            **kwargs: Additional parameters

        Returns:
            Processing results with statistics
        """
        logger.info(f"Starting {self.data_type_name} processing")
        logger.info(f"Date range: {from_date} to {to_date}")
        logger.info(f"Parallelization: {enable_parallelization}")
        logger.info(f"Redistribute idle sessions: {redistribute_idle_sessions}")

        # Setup progress tracking
        self.setup_progress_tracker()

        # Setup run tracking
        await self.setup_run_tracking()

        try:
            if enable_parallelization:
                return await self._process_with_parallelization(
                    from_date,
                    to_date,
                    limit,
                    max_items,
                    batch_size,
                    max_concurrent,
                    redistribute_idle_sessions,
                    **kwargs,
                )
            else:
                return await self._process_sequentially(
                    from_date, to_date, limit, max_items, batch_size, **kwargs
                )

        except Exception as e:
            logger.error(f"Processing failed: {e}")
            if self.run_manager and self.current_run_id:
                await self.run_manager.fail_run(self.current_run_id, str(e))
            raise

        finally:
            if self.run_manager and self.current_run_id:
                await self.run_manager.complete_run(self.current_run_id)

    async def _process_sequentially(
        self,
        from_date: str | None,
        to_date: str | None,
        limit: int | None,
        max_items: int | None,
        batch_size: int,
        **kwargs,
    ) -> dict[str, Any]:
        """Process items sequentially using a single API client."""
        stats = {
            "total_processed": 0,
            "successful": 0,
            "errors": 0,
            "batches_stored": 0,
            "start_time": datetime.now(UTC),
            "latest_update_date": None,
        }

        batch_items = []
        processed_count = 0
        first_batch_processed = False

        async for list_batch in self.fetch_list_data(
            from_date=from_date, to_date=to_date, limit=limit, **kwargs
        ):
            # Capture total count from first response
            if not first_batch_processed and hasattr(
                self.client, "last_response_metadata"
            ):
                metadata = self.client.last_response_metadata
                if metadata and isinstance(metadata, dict) and "pagination" in metadata:
                    pagination = metadata.get("pagination")
                    if (
                        pagination
                        and isinstance(pagination, dict)
                        and "count" in pagination
                    ):
                        total_count = pagination.get("count")
                        if total_count is not None and isinstance(total_count, int):
                            stats["total_count"] = total_count
                            logger.info(
                                f"Captured total count for sequential processing: {total_count}"
                            )
                first_batch_processed = True

            for list_item in list_batch:
                if max_items and processed_count >= max_items:
                    break

                try:
                    processed_item = await self._process_single_item(list_item)
                    batch_items.append(processed_item)
                    stats["successful"] += 1

                    # Track latest updateDate for incremental processing
                    update_date = list_item.get("updateDate") or processed_item.get(
                        "full_data", {}
                    ).get("updateDate")
                    if update_date and (
                        not stats["latest_update_date"]
                        or update_date > stats["latest_update_date"]
                    ):
                        stats["latest_update_date"] = update_date

                except Exception as e:
                    logger.error(
                        f"Error processing item {list_item.get('url', 'UNKNOWN')}: {e}",
                        exc_info=True,
                    )
                    stats["errors"] += 1

                processed_count += 1
                stats["total_processed"] += 1

                # Store batch when full
                if len(batch_items) >= batch_size:
                    await self._store_batch(batch_items)
                    stats["batches_stored"] += 1
                    batch_items.clear()

                if max_items and processed_count >= max_items:
                    break

            if max_items and processed_count >= max_items:
                break

        # Store remaining items
        if batch_items:
            await self._store_batch(batch_items)
            stats["batches_stored"] += 1

        stats["end_time"] = datetime.now(UTC)
        stats["duration"] = (stats["end_time"] - stats["start_time"]).total_seconds()

        # Update last_processed_date if we processed items successfully
        if stats.get("successful", 0) > 0:
            await self._update_last_processed_tracking(stats)

        logger.info(f"Sequential processing completed: {stats}")
        return stats

    async def _process_with_parallelization(
        self,
        from_date: str | None,
        to_date: str | None,
        limit: int | None,
        max_items: int | None,
        batch_size: int,
        max_concurrent: int,
        redistribute_idle_sessions: bool = True,
        **kwargs,
    ) -> dict[str, Any]:
        """Process items with parallelization using multiple API clients."""
        logger.info("Using parallelization for processing")

        # Get parallel API clients if available
        processing_resource = getattr(self, "processing_resource", None)
        if not processing_resource or not hasattr(
            processing_resource, "get_api_clients_for_parallel_sessions"
        ):
            logger.warning(
                "No parallel clients available, falling back to sequential processing"
            )
            return await self._process_sequentially(
                from_date, to_date, limit, max_items, batch_size, **kwargs
            )

        api_clients = await processing_resource.get_api_clients_for_parallel_sessions(
            self.data_type_name
        )
        if len(api_clients) <= 1:
            logger.warning(
                "Insufficient parallel clients, falling back to sequential processing"
            )
            return await self._process_sequentially(
                from_date, to_date, limit, max_items, batch_size, **kwargs
            )

        logger.info(f"Using {len(api_clients)} parallel API clients")

        # Use keyword arguments to avoid positional mismatches in downstream calls
        return await self._process_with_concurrent_page_fetching(
            api_clients=api_clients,
            from_date=from_date,
            to_date=to_date,
            batch_size=batch_size,
            max_concurrent=max_concurrent,
            redistribute_idle_sessions=redistribute_idle_sessions,
            limit=limit,
            **kwargs,
        )

    async def _process_with_concurrent_page_fetching(
        self,
        api_clients: list,
        from_date: str | None,
        to_date: str | None,
        batch_size: int,
        max_concurrent: int,
        redistribute_idle_sessions: bool = True,
        limit: int | None = 250,
        **kwargs,
    ) -> dict[str, Any]:
        """Process with concurrent page fetching across multiple clients."""
        stats = {
            "total_processed": 0,
            "successful": 0,
            "errors": 0,
            "batches_stored": 0,
            "start_time": datetime.now(UTC),
            "redistribution_used": False,
            "latest_update_date": None,
            "total_count": None,
        }

        # Get pagination metadata for optimal distribution
        try:
            pages_to_process, pagination_metadata = await self._get_pagination_metadata(
                from_date, to_date, limit, **kwargs
            )
            if pages_to_process is None:
                logger.warning(
                    "Could not determine pagination metadata, using simple distribution"
                )
                return await self._process_with_simple_distribution(
                    api_clients, from_date, to_date, limit, batch_size, None, **kwargs
                )

            if pages_to_process == 0:
                logger.info("No new items to process based on last_processed_count")
                stats["end_time"] = datetime.now(UTC)
                stats["duration"] = (
                    stats["end_time"] - stats["start_time"]
                ).total_seconds()
                logger.info(f"Processing completed (no new items): {stats}")
                return stats

            logger.info(
                f"Incremental items to process: {pagination_metadata.get('incremental_count')} "
                f"across {pages_to_process} pages"
            )

            # Store pagination metadata in stats
            stats["total_count"] = pagination_metadata.get("total_count")
            stats["last_processed_count"] = pagination_metadata.get(
                "last_processed_count"
            )
            stats["incremental_count"] = pagination_metadata.get("incremental_count")

            # Check if we have more sessions than pages and should redistribute
            if (
                redistribute_idle_sessions
                and pages_to_process < len(api_clients)
                and pages_to_process > 0
            ):
                logger.info(
                    f"Redistributing idle sessions: {pages_to_process} pages vs {len(api_clients)} sessions"
                )
                stats["redistribution_used"] = True
                return await self._process_with_redistribution_strategy(
                    api_clients,
                    from_date,
                    to_date,
                    limit,
                    pages_to_process,
                    batch_size,
                    max_concurrent,
                    pagination_metadata,
                    **kwargs,
                )

            # Standard interleaved distribution with incremental offsets
            page_size = pagination_metadata.get("page_size", limit or 250)
            start_offset = pagination_metadata.get("start_offset", 0)
            offset_assignments = self._calculate_interleaved_offsets(
                len(api_clients), pages_to_process, page_size, start_offset
            )

            # Process with assigned offsets
            tasks = []
            for session_id, (client, assigned_offsets) in enumerate(
                zip(api_clients, offset_assignments, strict=False)
            ):
                task = self._process_assigned_offsets(
                    client,
                    from_date,
                    to_date,
                    limit,
                    assigned_offsets,
                    batch_size,
                    max_concurrent,
                    session_id,
                    **kwargs,
                )
                tasks.append(task)

            # Execute all sessions concurrently
            session_results = await asyncio.gather(*tasks, return_exceptions=True)

            # Aggregate results
            for result in session_results:
                if isinstance(result, Exception):
                    logger.error(f"Session failed: {result}")
                    stats["errors"] += 1
                elif isinstance(result, dict):
                    stats["total_processed"] += result.get("total_processed", 0)
                    stats["successful"] += result.get("successful", 0)
                    stats["errors"] += result.get("errors", 0)
                    stats["batches_stored"] += result.get("batches_stored", 0)

                    # Track latest updateDate across all sessions
                    session_latest = result.get("latest_update_date")
                    if session_latest and (
                        not stats["latest_update_date"]
                        or session_latest > stats["latest_update_date"]
                    ):
                        stats["latest_update_date"] = session_latest

        except Exception as e:
            logger.error(f"Error in concurrent processing: {e}")
            logger.info("Falling back to simple distribution")
            return await self._process_with_simple_distribution(
                api_clients, from_date, to_date, limit, batch_size, None, **kwargs
            )

        stats["end_time"] = datetime.now(UTC)
        stats["duration"] = (stats["end_time"] - stats["start_time"]).total_seconds()

        # Update last_processed_date if we processed items successfully
        if stats.get("successful", 0) > 0:
            await self._update_last_processed_tracking(stats)

        logger.info(f"Parallel processing completed: {stats}")
        return stats

    async def _process_with_redistribution_strategy(
        self,
        api_clients: list,
        from_date: str | None,
        to_date: str | None,
        limit: int | None,
        pages_to_process: int,
        batch_size: int,
        max_concurrent: int,
        pagination_metadata: dict[str, Any],
        **kwargs,
    ) -> dict[str, Any]:
        """
        Process with redistribution strategy for idle sessions.

        Strategy:
        1. Use only enough sessions for page fetching (Phase 1 & 2)
        2. Collect all full_data items
        3. Redistribute all sessions for parallel related data fetching (Phase 3)
        """
        logger.info("Using redistribution strategy for idle sessions")

        stats = {
            "total_processed": 0,
            "successful": 0,
            "errors": 0,
            "batches_stored": 0,
            "start_time": datetime.now(UTC),
            "phase_1_2_sessions": pages_to_process,
            "phase_3_sessions": len(api_clients),
            "latest_update_date": None,
            # Preserve pagination metadata for tracking
            "total_count": pagination_metadata.get("total_count"),
            "last_processed_count": pagination_metadata.get("last_processed_count"),
            "incremental_count": pagination_metadata.get("incremental_count"),
        }

        # Phase 1 & 2: Use only required sessions for page fetching
        active_clients = api_clients[:pages_to_process]
        idle_clients = api_clients[pages_to_process:]

        logger.info(f"Phase 1&2: Using {len(active_clients)} active sessions")
        logger.info(f"Phase 3: Will redistribute {len(api_clients)} total sessions")

        # Collect all items from Phase 1 & 2
        all_items = []

        page_size = pagination_metadata.get("page_size", limit or 250)
        start_offset = pagination_metadata.get("start_offset", 0)
        offset_assignments = self._calculate_interleaved_offsets(
            len(active_clients), pages_to_process, page_size, start_offset
        )

        # Process pages to get list and full data
        tasks = []
        for session_id, (client, assigned_offsets) in enumerate(
            zip(active_clients, offset_assignments, strict=False)
        ):
            task = self._collect_items_phases_1_2(
                client,
                from_date,
                to_date,
                limit,
                assigned_offsets,
                session_id,
                **kwargs,
            )
            tasks.append(task)

        # Execute Phase 1 & 2 concurrently
        session_results = await asyncio.gather(*tasks, return_exceptions=True)

        # Collect all items
        for result in session_results:
            if isinstance(result, Exception):
                logger.error(f"Phase 1&2 session failed: {result}")
                stats["errors"] += 1
            elif isinstance(result, dict) and "items" in result:
                all_items.extend(result["items"])
                stats["total_processed"] += result.get("total_processed", 0)

                # Track latest updateDate across sessions
                session_latest = result.get("latest_update_date")
                if session_latest and (
                    not stats["latest_update_date"]
                    or session_latest > stats["latest_update_date"]
                ):
                    stats["latest_update_date"] = session_latest

        logger.info(f"Collected {len(all_items)} items from Phase 1&2")

        if not all_items:
            logger.warning("No items collected from Phase 1&2")
            return stats

        # Phase 3: Redistribute all sessions for related data fetching
        logger.info(
            f"Phase 3: Redistributing related data across {len(idle_clients)} sessions"
        )

        phase_3_stats = await self._process_related_data_with_redistribution(
            api_clients,
            all_items,
            batch_size,
            max_concurrent,
        )

        # Combine stats
        stats["successful"] += phase_3_stats.get("successful", 0)
        stats["errors"] += phase_3_stats.get("errors", 0)
        stats["batches_stored"] += phase_3_stats.get("batches_stored", 0)

        stats["end_time"] = datetime.now(UTC)
        stats["duration"] = (stats["end_time"] - stats["start_time"]).total_seconds()

        # Update last_processed_date if we processed items successfully
        if stats.get("successful", 0) > 0:
            await self._update_last_processed_tracking(stats)

        logger.info(f"Redistribution strategy completed: {stats}")
        return stats

    async def _collect_items_phases_1_2(
        self,
        client,
        from_date: str | None,
        to_date: str | None,
        limit: int | None,
        assigned_offsets: list[int],
        session_id: int,
        **kwargs,
    ) -> dict[str, Any]:
        """
        Collect items from Phase 1 (list) and Phase 2 (full data) with immediate storage.
        Now stores list_data and full_data immediately as they're processed.
        """
        stats = {
            "total_processed": 0,
            "items": [],
            "session_id": session_id,
            "latest_update_date": None,
        }

        logger.info(
            f"Phase 1&2 Session {session_id}: Processing {len(assigned_offsets)} pages with immediate storage"
        )

        for offset in assigned_offsets:
            try:
                async for list_batch in self.fetch_list_data_with_client(
                    client,
                    from_date=from_date,
                    to_date=to_date,
                    limit=limit,
                    offset=offset,
                    single_page_only=True,
                    **kwargs,
                ):
                    for list_item in list_batch:
                        try:
                            # Defensive check: ensure list_item is a dictionary
                            if not isinstance(list_item, dict):
                                logger.warning(
                                    f"Session {session_id}: Skipping non-dict item: {type(list_item)} - {list_item}"
                                )
                                continue

                            # Extract preliminary item ID for checkpoint tracking
                            preliminary_item_id = self._extract_preliminary_item_id(
                                list_item
                            )

                            # Attach preliminary ID so downstream storage has proper source_doc_id
                            if (
                                preliminary_item_id
                                and preliminary_item_id != "ID_ERROR"
                            ):
                                list_item[self.id_field] = preliminary_item_id

                            # Phase 1: Store list data immediately if not already stored
                            phase_1_stored = False
                            if self.progress_tracker:
                                phase_1_stored = self.progress_tracker.should_skip_item(
                                    preliminary_item_id,
                                    ProcessingPhase.MAIN_ITEMS,
                                    field_name="list_data",
                                )

                            if not phase_1_stored:
                                batch_id = str(uuid.uuid4())
                                await self.store_list_data(list_item, batch_id)

                                if self.progress_tracker:
                                    self.progress_tracker.increment_processed(
                                        preliminary_item_id,
                                        ProcessingPhase.MAIN_ITEMS,
                                        field_name="list_data",
                                    )
                                logger.debug(
                                    f"Session {session_id}: Stored list data for {preliminary_item_id}"
                                )
                            else:
                                logger.debug(
                                    f"Session {session_id}: Skipping list data for already processed {preliminary_item_id}"
                                )

                            # Phase 2: Fetch and store full data immediately if not already stored
                            item_url = list_item.get("url")
                            if not item_url:
                                logger.warning(
                                    f"Session {session_id}: Item missing URL field"
                                )
                                continue

                            phase_2_stored = False
                            actual_item_id = preliminary_item_id
                            full_data = None

                            if self.progress_tracker:
                                phase_2_stored = self.progress_tracker.should_skip_item(
                                    preliminary_item_id,
                                    ProcessingPhase.MAIN_ITEMS,
                                    field_name="full_data",
                                )

                            if not phase_2_stored:
                                full_data = await self.fetch_full_data_with_client(
                                    item_url, client
                                )

                                if full_data:
                                    if isinstance(
                                        full_data, list
                                    ):  # committeeprints endpoint has a strange response structure
                                        full_data = full_data[0]
                                    actual_item_id = self.extract_item_id(full_data)
                                    full_data[self.id_field] = actual_item_id

                                batch_id = str(uuid.uuid4())
                                await self.store_full_data(full_data, batch_id)

                                if self.progress_tracker:
                                    self.progress_tracker.increment_processed(
                                        actual_item_id,
                                        ProcessingPhase.MAIN_ITEMS,
                                        field_name="full_data",
                                    )
                                logger.debug(
                                    f"Session {session_id}: Stored full data for {actual_item_id}"
                                )
                            else:
                                logger.debug(
                                    f"Session {session_id}: Skipping full data for already processed {preliminary_item_id}"
                                )
                                # Still need full_data for Phase 3, so fetch without storing
                                full_data = await self.fetch_full_data_with_client(
                                    item_url, client
                                )

                                if full_data:
                                    if isinstance(
                                        full_data, list
                                    ):  # committeeprints endpoint has a strange response structure
                                        full_data = full_data[0]
                                    actual_item_id = self.extract_item_id(full_data)
                                    full_data[self.id_field] = actual_item_id

                            # Extract updateDate for incremental tracking
                            update_date = list_item.get("updateDate") or (
                                full_data or {}
                            ).get("updateDate")
                            if update_date and (
                                not stats["latest_update_date"]
                                or update_date > stats["latest_update_date"]
                            ):
                                stats["latest_update_date"] = update_date

                            # Collect item data for Phase 3 (without related data yet)
                            item_data = {
                                self.id_field: actual_item_id,
                                "list_data": list_item,
                                "full_data": full_data or {},
                                "processed_at": datetime.now(UTC).isoformat(),
                                "phases_completed": {
                                    "list_data": True,  # We just stored it or it was already stored
                                    "full_data": full_data is not None,
                                    "related_data": False,  # Will be processed in Phase 3
                                },
                            }

                            stats["items"].append(item_data)
                            stats["total_processed"] += 1

                        except Exception as e:
                            logger.error(
                                f"Session {session_id} error processing item: {e}",
                                exc_info=True,
                            )
            except Exception as e:
                logger.error(
                    f"Session {session_id} error processing page offset {offset}: {e}"
                )
                stats["errors"] += 1

        logger.info(
            f"Phase 1&2 Session {session_id}: Processed {len(stats['items'])} items with immediate storage"
        )
        return stats

    async def _process_related_data_with_redistribution(
        self,
        api_clients: list,
        all_items: list[dict[str, Any]],
        batch_size: int,
        max_concurrent: int,
    ) -> dict[str, Any]:
        """
        Process related data (Phase 3) with redistribution across all available sessions.
        """
        stats = {
            "successful": 0,
            "errors": 0,
            "batches_stored": 0,
        }

        if not all_items:
            return stats

        # Distribute items across all available sessions
        items_per_session = len(all_items) // len(api_clients)
        remainder = len(all_items) % len(api_clients)

        item_assignments = []
        start_idx = 0

        for i in range(len(api_clients)):
            # Distribute remainder across first sessions
            session_count = items_per_session + (1 if i < remainder else 0)
            end_idx = start_idx + session_count
            item_assignments.append(all_items[start_idx:end_idx])
            start_idx = end_idx

        logger.info(
            f"Related data distribution: {[len(assignment) for assignment in item_assignments]}"
        )

        # Process related data in parallel across all sessions
        tasks = []
        for session_id, (client, assigned_items) in enumerate(
            zip(api_clients, item_assignments, strict=False)
        ):
            if assigned_items:  # Only create tasks for sessions with items
                task = self._process_related_data_for_session(
                    client,
                    assigned_items,
                    batch_size,
                    max_concurrent,
                    session_id,
                )
                tasks.append(task)

        # Execute all related data sessions concurrently
        if tasks:
            session_results = await asyncio.gather(*tasks, return_exceptions=True)

            # Aggregate results
            for result in session_results:
                if isinstance(result, Exception):
                    logger.error(f"Related data session failed: {result}")
                    stats["errors"] += 1
                elif isinstance(result, dict):
                    stats["successful"] += result.get("successful", 0)
                    stats["errors"] += result.get("errors", 0)
                    stats["batches_stored"] += result.get("batches_stored", 0)

        return stats

    async def _process_related_data_for_session(
        self,
        client,
        assigned_items: list[dict[str, Any]],
        batch_size: int,
        max_concurrent: int,
        session_id: int,
    ) -> dict[str, Any]:
        """
        Process related data for items assigned to a specific session with immediate storage.
        """
        stats = {
            "successful": 0,
            "errors": 0,
            "batches_stored": 0,
            "session_id": session_id,
        }

        logger.info(
            f"Related data Session {session_id}: Processing {len(assigned_items)} items with immediate storage"
        )

        semaphore = asyncio.Semaphore(max_concurrent)

        async def process_item_related_data(item_data: dict[str, Any]):
            async with semaphore:
                try:
                    item_id = item_data.get(self.id_field, "UNKNOWN")

                    # Check if related data is already processed
                    phase_3_stored = False
                    if self.progress_tracker:
                        phase_3_stored = self.progress_tracker.should_skip_item(
                            item_id, ProcessingPhase.RELATED_ENTITIES
                        )

                    if phase_3_stored:
                        logger.debug(
                            f"Session {session_id}: Skipping related data for already processed {item_id}"
                        )
                        return True

                    # Create a temporary fetcher instance with this session's client
                    # to fetch related data using the same interface
                    temp_fetcher = type(self)(
                        client=client,
                        db_pool=self.db_pool,  # Need DB for storing
                        checkpoint_manager=None,
                        run_manager=None,
                    )

                    # Phase 3: Fetch related data using the specific client
                    related_data = await temp_fetcher.fetch_related_data(
                        item_data["full_data"]
                    )

                    # Store related data immediately
                    batch_id = str(uuid.uuid4())
                    await self.store_related_data(related_data, item_id, batch_id)

                    # Mark as processed in checkpoint
                    if self.progress_tracker:
                        self.progress_tracker.increment_processed(
                            item_id, ProcessingPhase.RELATED_ENTITIES
                        )

                    logger.debug(
                        f"Session {session_id}: Stored related data for {item_id}"
                    )
                    return True

                except Exception as e:
                    logger.error(
                        f"Related data Session {session_id} error processing item {item_data.get(self.id_field, 'UNKNOWN')}: {e}"
                    )
                    raise

        # Process all items concurrently within this session
        tasks = [process_item_related_data(item) for item in assigned_items]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Handle results
        for result in results:
            if isinstance(result, Exception):
                stats["errors"] += 1
            else:
                stats["successful"] += 1

        # Note: No batch storage needed since we store immediately
        stats["batches_stored"] = stats["successful"]  # Each item stored individually

        logger.info(f"Related data Session {session_id} completed: {stats}")
        return stats

    async def _get_pagination_metadata(
        self,
        from_date: str | None,
        to_date: str | None,
        limit: int | None,
        **kwargs,
    ) -> tuple[int | None, dict[str, Any]]:
        """
        Get pagination metadata for optimal parallel distribution with incremental support.

        Returns:
            Tuple of (pages_to_process, metadata_dict) where:
            - pages_to_process: Number of pages that actually need processing (accounting for last_processed_count)
            - metadata_dict: Contains total_count, last_processed_count, incremental_count, start_offset
        """
        try:
            # Step 1: Get the TRUE total count (without date filters) if we have date filters
            true_total_count = None
            if from_date or to_date:
                async for _first_batch in self.fetch_list_data(
                    from_date=None,  # No date filters
                    to_date=None,  # No date filters
                    limit=0,  # Just get metadata, no actual items
                    offset=0,
                    **kwargs,
                ):
                    if hasattr(self.client, "last_response_metadata"):
                        metadata = self.client.last_response_metadata
                        if (
                            metadata
                            and isinstance(metadata, dict)
                            and "pagination" in metadata
                        ):
                            pagination = metadata.get("pagination")
                            if (
                                pagination
                                and isinstance(pagination, dict)
                                and "count" in pagination
                            ):
                                true_total_count = pagination.get("count")
                                if true_total_count is not None and isinstance(
                                    true_total_count, int
                                ):
                                    break
                    break

            # Step 2: Get the filtered count and pagination info (with date filters if present)
            # Fetch first page to get pagination info
            async for _first_batch in self.fetch_list_data(
                from_date=from_date,
                to_date=to_date,
                limit=limit or 250,
                offset=0,
                **kwargs,
            ):
                if hasattr(self.client, "last_response_metadata"):
                    metadata = self.client.last_response_metadata
                    if (
                        metadata
                        and isinstance(metadata, dict)
                        and "pagination" in metadata
                    ):
                        pagination = metadata.get("pagination")
                        if (
                            pagination
                            and isinstance(pagination, dict)
                            and "count" in pagination
                        ):
                            filtered_count = pagination.get("count")
                            if filtered_count is not None and isinstance(
                                filtered_count, int
                            ):
                                page_size = limit or 250

                                # Determine which total count to use for tracking
                                # If we have date filters, use the true total count for tracking
                                # Otherwise, use the filtered count (which is the same as total count)
                                total_count_for_tracking = (
                                    true_total_count
                                    if (from_date or to_date)
                                    else filtered_count
                                )

                                # Get last processed count for incremental processing
                                last_processed_count = 0
                                try:
                                    if hasattr(
                                        self.client, "access_last_processed_count"
                                    ):
                                        last_count = await self.client.access_last_processed_count(
                                            self.data_type_name
                                        )
                                        if last_count is not None:
                                            last_processed_count = last_count
                                except Exception as e:
                                    logger.debug(
                                        f"Could not get last processed count: {e}"
                                    )

                                # For incremental processing, we use the filtered count to determine pages
                                # But we track against the true total count
                                incremental_count = filtered_count  # This is what we'll actually process
                                start_offset = (
                                    0  # When using date filters, we start from offset 0
                                )

                                if incremental_count == 0:
                                    logger.info(
                                        "No new items to process (filtered count is 0)"
                                    )
                                    return 0, {
                                        "total_count": total_count_for_tracking,
                                        "last_processed_count": last_processed_count,
                                        "incremental_count": 0,
                                        "start_offset": start_offset,
                                        "page_size": page_size,
                                    }

                                # Calculate pages needed for the filtered items
                                incremental_pages = (
                                    incremental_count + page_size - 1
                                ) // page_size

                                metadata_dict = {
                                    "total_count": total_count_for_tracking,  # True total for tracking
                                    "last_processed_count": last_processed_count,
                                    "incremental_count": incremental_count,  # Filtered count for processing
                                    "start_offset": start_offset,
                                    "page_size": page_size,
                                    "incremental_pages": incremental_pages,
                                }

                                return incremental_pages, metadata_dict
                break

        except Exception as e:
            logger.warning(f"Could not get pagination metadata: {e}")

        return None, {}

    def _calculate_interleaved_offsets(
        self, num_sessions: int, total_pages: int, page_size: int, start_offset: int = 0
    ) -> list[list[int]]:
        """
        Calculate interleaved offset assignments for load balancing with incremental support.

        Args:
            num_sessions: Number of parallel sessions
            total_pages: Number of pages to process (incremental pages, not total pages)
            page_size: Items per page
            start_offset: Starting offset (based on last_processed_count)
        """
        assignments = [[] for _ in range(num_sessions)]

        for page_num in range(total_pages):
            session_id = page_num % num_sessions
            # Calculate offset starting from the incremental position
            offset = start_offset + (page_num * page_size)
            assignments[session_id].append(offset)

        logger.debug(
            f"Calculated incremental offsets: start_offset={start_offset}, "
            f"pages={total_pages}, assignments={[(i, len(a)) for i, a in enumerate(assignments)]}"
        )

        return assignments

    async def _process_with_simple_distribution(
        self,
        api_clients: list,
        from_date: str | None,
        to_date: str | None,
        limit: int | None,
        batch_size: int,
        total_pages: int | None,
        **kwargs,
    ) -> dict[str, Any]:
        """Simple distribution when pagination metadata is unavailable."""
        stats = {
            "total_processed": 0,
            "successful": 0,
            "errors": 0,
            "batches_stored": 0,
            "start_time": datetime.now(UTC),
            "latest_update_date": None,
        }

        # For simple distribution, use first client only to avoid duplication
        # This is a fallback when we can't determine proper pagination
        client = api_clients[0]

        batch_items = []
        processed_count = 0
        first_batch_processed = False

        async for list_batch in self.fetch_list_data_with_client(
            client,
            from_date=from_date,
            to_date=to_date,
            limit=limit,
            **kwargs,
        ):
            if not list_batch:
                logger.warning("Received empty batch from API")
                continue

            # Capture total count from first response
            if not first_batch_processed and hasattr(client, "last_response_metadata"):
                metadata = client.last_response_metadata
                if metadata and isinstance(metadata, dict) and "pagination" in metadata:
                    pagination = metadata.get("pagination")
                    if (
                        pagination
                        and isinstance(pagination, dict)
                        and "count" in pagination
                    ):
                        total_count = pagination.get("count")
                        if total_count is not None and isinstance(total_count, int):
                            stats["total_count"] = total_count
                            logger.info(
                                f"Captured total count for simple distribution: {total_count}"
                            )
                first_batch_processed = True

            for list_item in list_batch:
                try:
                    if not list_item or not isinstance(list_item, dict):
                        logger.warning(f"Skipping invalid list item: {type(list_item)}")
                        stats["errors"] += 1
                        continue

                    processed_item = await self._process_single_item_with_client(
                        list_item, client
                    )
                    batch_items.append(processed_item)
                    stats["successful"] += 1

                    # Track latest updateDate for incremental processing
                    update_date = list_item.get("updateDate") or processed_item.get(
                        "full_data", {}
                    ).get("updateDate")
                    if update_date and (
                        not stats["latest_update_date"]
                        or update_date > stats["latest_update_date"]
                    ):
                        stats["latest_update_date"] = update_date

                except Exception as e:
                    logger.error(
                        f"Error processing item {list_item.get('url', 'UNKNOWN') if isinstance(list_item, dict) else 'INVALID_ITEM'}: {e}",
                        exc_info=True,
                    )
                    stats["errors"] += 1

                processed_count += 1
                stats["total_processed"] += 1

                # Store batch when full
                if len(batch_items) >= batch_size:
                    await self._store_batch(batch_items)
                    stats["batches_stored"] += 1
                    batch_items.clear()

        # Store remaining items
        if batch_items:
            await self._store_batch(batch_items)
            stats["batches_stored"] += 1

        stats["end_time"] = datetime.now(UTC)
        stats["duration"] = (stats["end_time"] - stats["start_time"]).total_seconds()

        # Update last_processed_date if we processed items successfully
        if stats.get("successful", 0) > 0:
            await self._update_last_processed_tracking(stats)

        logger.info(f"Simple distribution completed: {stats}")
        return stats

    async def _process_assigned_offsets(
        self,
        client,
        from_date: str | None,
        to_date: str | None,
        limit: int | None,
        assigned_offsets: list[int],
        batch_size: int,
        max_concurrent: int,
        session_id: int,
        **kwargs,
    ) -> dict[str, Any]:
        """Process assigned offsets for a specific client session."""
        stats = {
            "total_processed": 0,
            "successful": 0,
            "errors": 0,
            "batches_stored": 0,
            "session_id": session_id,
        }

        logger.info(f"Session {session_id}: Processing {len(assigned_offsets)} pages")

        batch_items = []
        semaphore = asyncio.Semaphore(max_concurrent)

        async def process_page_offset(offset: int):
            async with semaphore:
                try:
                    async for list_batch in self.fetch_list_data_with_client(
                        client,
                        from_date=from_date,
                        to_date=to_date,
                        limit=limit,
                        offset=offset,
                        single_page_only=True,
                        **kwargs,
                    ):
                        for list_item in list_batch:
                            try:
                                # Defensive check: ensure list_item is a dictionary
                                if not isinstance(list_item, dict):
                                    logger.warning(
                                        f"Session {session_id}: Skipping non-dict item: {type(list_item)} - {list_item}"
                                    )
                                    continue

                                processed_item = (
                                    await self._process_single_item_with_client(
                                        list_item, client
                                    )
                                )
                                batch_items.append(processed_item)
                                stats["successful"] += 1
                            except Exception as e:
                                logger.error(
                                    f"Session {session_id} error processing item: {e}",
                                    exc_info=True,
                                )
                                stats["errors"] += 1

                            stats["total_processed"] += 1

                            # Store batch when full (with lock for thread safety)
                            if len(batch_items) >= batch_size:
                                await self._store_batch(batch_items[:batch_size])
                                stats["batches_stored"] += 1
                                del batch_items[:batch_size]
                except Exception as e:
                    logger.error(
                        f"Session {session_id} error processing page offset {offset}: {e}"
                    )
                    stats["errors"] += 1

        # Process all assigned offsets concurrently
        tasks = [process_page_offset(offset) for offset in assigned_offsets]
        await asyncio.gather(*tasks, return_exceptions=True)

        # Store remaining items
        if batch_items:
            await self._store_batch(batch_items)
            stats["batches_stored"] += 1

        logger.info(f"Session {session_id} completed: {stats}")
        return stats

    async def _process_single_item(self, list_item: dict[str, Any]) -> dict[str, Any]:
        """Process a single item through all phases using the default client."""
        return await self._process_single_item_with_client(list_item, self.client)

    async def _process_single_item_with_client(
        self, list_item: dict[str, Any], client
    ) -> dict[str, Any]:
        """Process a single item through all phases using a specific client with phase-specific checkpointing."""
        # Defensive check: ensure list_item is a dictionary
        if not isinstance(list_item, dict):
            raise ValueError(f"Expected dict but got {type(list_item)}: {list_item}")

        if not list_item or not isinstance(list_item, dict):
            raise ValueError(f"Invalid list item: {type(list_item)}")

        item_url = list_item.get("url")
        if not item_url:
            raise ValueError(
                f"Item missing URL field. Item keys: {list(list_item.keys())}"
            )

        # Extract item ID early for checkpoint tracking
        # We'll get the proper ID after fetching full data, but need a preliminary ID for tracking
        preliminary_item_id = self._extract_preliminary_item_id(list_item)
        list_item[self.id_field] = preliminary_item_id

        # Phase 1: Check if list data is already stored and store if needed
        phase_1_complete = False
        if self.progress_tracker:
            phase_1_complete = self.progress_tracker.should_skip_item(
                preliminary_item_id, ProcessingPhase.MAIN_ITEMS, field_name="list_data"
            )

        if not phase_1_complete:
            # Store list data immediately
            batch_id = str(uuid.uuid4())
            await self.store_list_data(list_item, batch_id)

            if self.progress_tracker:
                self.progress_tracker.set_processing_phase(
                    ProcessingPhase.MAIN_ITEMS, current_field="list_data"
                )
                self.progress_tracker.increment_processed(
                    preliminary_item_id,
                    ProcessingPhase.MAIN_ITEMS,
                    field_name="list_data",
                )

            logger.debug(f"Stored list data for item {preliminary_item_id}")
        else:
            logger.debug(
                f"Skipping list data storage for already processed item {preliminary_item_id}"
            )

        # Phase 2: Check if full data is already processed and fetch if needed
        phase_2_complete = False
        full_data = None
        actual_item_id = preliminary_item_id  # Will be updated after fetching full data

        if self.progress_tracker:
            phase_2_complete = self.progress_tracker.should_skip_item(
                preliminary_item_id, ProcessingPhase.MAIN_ITEMS, field_name="full_data"
            )

        if not phase_2_complete:
            if self.progress_tracker:
                self.progress_tracker.set_processing_phase(
                    ProcessingPhase.MAIN_ITEMS, current_field="full_data"
                )

            full_data = await self.fetch_full_data_with_client(item_url, client)
            if not full_data:
                raise ValueError(f"Failed to fetch full data from {item_url}")
            if isinstance(full_data, list):
                full_data = full_data[0]
            # Now we have the actual item ID
            actual_item_id = self.extract_item_id(full_data)
            full_data[self.id_field] = actual_item_id

            # Store full data immediately
            batch_id = str(uuid.uuid4())
            await self.store_full_data(full_data, batch_id)

            if self.progress_tracker:
                self.progress_tracker.increment_processed(
                    actual_item_id, ProcessingPhase.MAIN_ITEMS, field_name="full_data"
                )

            logger.debug(f"Stored full data for item {actual_item_id}")
        else:
            logger.debug(
                f"Skipping full data processing for already processed item {preliminary_item_id}"
            )
            # Still need full_data for Phase 3, so fetch without storing
            full_data = await self.fetch_full_data_with_client(item_url, client)

            if full_data:
                if isinstance(
                    full_data, list
                ):  # committeeprints endpoint has a strange response structure
                    full_data = full_data[0]
                actual_item_id = self.extract_item_id(full_data)
                full_data[self.id_field] = actual_item_id

        # Phase 3: Check if related data is already processed and fetch if needed
        phase_3_complete = False
        related_data = {}

        if self.progress_tracker:
            phase_3_complete = self.progress_tracker.should_skip_item(
                actual_item_id, ProcessingPhase.RELATED_ENTITIES
            )

        if not phase_3_complete and full_data:
            if self.progress_tracker:
                self.progress_tracker.set_processing_phase(
                    ProcessingPhase.RELATED_ENTITIES, current_field="related_data"
                )

            related_data = await self.fetch_related_data(full_data)

            # Store related data immediately
            batch_id = str(uuid.uuid4())
            await self.store_related_data(related_data, actual_item_id, batch_id)

            if self.progress_tracker:
                self.progress_tracker.increment_processed(
                    actual_item_id, ProcessingPhase.RELATED_ENTITIES
                )

            logger.debug(f"Stored related data for item {actual_item_id}")
        else:
            logger.debug(
                f"Skipping related data processing for already processed item {actual_item_id}"
            )

        # Combine all data for return (even if some phases were skipped)
        return {
            self.id_field: actual_item_id,
            "list_data": list_item,
            "full_data": full_data or {},
            "related_data": related_data,
            "processed_at": datetime.now(UTC).isoformat(),
            "phases_completed": {
                "list_data": phase_1_complete or True,  # True if we just processed it
                "full_data": phase_2_complete or (full_data is not None),
                "related_data": phase_3_complete or bool(related_data),
            },
        }

    async def _store_batch(self, batch_items: list[dict[str, Any]]) -> int:
        """
        Store a batch of processed items.

        NOTE: With phase-specific processing, individual items are stored
        incrementally during processing. This method is primarily for
        backward compatibility or when batch storage is explicitly needed.
        """
        if not batch_items or not self.db_pool:
            return 0

        batch_id = str(uuid.uuid4())
        stored_count = 0

        try:
            for item in batch_items:
                # Check if this item was already stored via phase-specific processing
                phases_completed = item.get("phases_completed", {})

                # If list & full are already stored, avoid re-inserting them; related_data
                # may legitimately be empty for some data types (e.g. members)
                if phases_completed.get("list_data") and phases_completed.get(
                    "full_data"
                ):
                    logger.debug(
                        f"Skipping batch storage for already processed item {item.get(self.id_field)}"
                    )
                    continue
                else:
                    # Fallback to complete item storage if phases weren't individually stored
                    if item.get("list_data"):
                        await self.store_list_data(item["list_data"], batch_id)
                        stored_count += 1
                    if item.get("full_data"):
                        await self.store_full_data(item["full_data"], batch_id)
                        stored_count += 1
                    if item.get("related_data"):
                        await self.store_related_data(
                            item["related_data"], item[self.id_field], batch_id
                        )
                        stored_count += 1

            logger.debug(f"Stored batch {batch_id}: {stored_count} items")

        except Exception as e:
            logger.error(f"Error storing batch {batch_id}: {e}")
            raise

        return stored_count

    # =============================================================================
    # STORAGE METHODS - CONGRESSIONAL DEFAULTS
    # =============================================================================

    async def store_list_data(
        self,
        list_data: dict[str, Any],
        batch_id: str | None = None,
        schema: str = "bicam_raw_congressional",
        table_suffix: str = "list_raw",
    ) -> None:
        """Store list data with Congressional defaults."""
        table_name = f"{self.data_type_name}_{table_suffix}"
        await self.store_raw_data(
            schema=schema,
            table=table_name,
            items=[list_data],
            url_field="url",
            batch_id=batch_id,
        )

    async def store_full_data(
        self,
        full_data: dict[str, Any],
        batch_id: str | None = None,
        schema: str = "bicam_raw_congressional",
        table_suffix: str = "raw",
    ) -> None:
        """Store full data with Congressional defaults."""
        table_name = f"{self.data_type_name}_{table_suffix}"

        # Extract item ID for the record
        item_id = self.extract_item_id(full_data)
        full_data_with_id = {
            **full_data,
            self.id_field: item_id,
            "batch_id": batch_id,
        }

        await self.store_raw_data(
            schema=schema,
            table=table_name,
            items=[full_data_with_id],
            url_field="url",
            batch_id=batch_id,
        )

    async def store_related_data(
        self,
        related_data: dict[str, list[dict[str, Any]]],
        item_id: str,
        batch_id: str | None = None,
        schema: str = "bicam_raw_congressional",
    ) -> None:
        """Store related data with Congressional defaults."""
        for relation_type, items in related_data.items():
            if not items:
                continue

            table_name = f"{self.data_type_name}_{relation_type}_raw"

            # Add parent item ID to each related item
            items_with_parent = []
            for item in items:
                item_with_parent = {
                    **item,
                    self.id_field: item_id,
                    "batch_id": batch_id,
                }
                items_with_parent.append(item_with_parent)

            await self.store_raw_data(
                schema=schema,
                table=table_name,
                items=items_with_parent,
                url_field="url",
                batch_id=batch_id,
            )

    async def store_raw_data(
        self,
        schema: str = "bicam_raw_congressional",
        table: str = None,
        items: list[dict[str, Any]] = None,
        url_field: str = "url",
        batch_id: str | None = None,
        conn: asyncpg.Connection | None = None,
        **kwargs,
    ) -> None:
        """Store raw data with Congressional schema defaults."""
        if not all([table, items]):
            raise ValueError("table, items, and id_field are required")

        if conn:
            await self._execute_store_raw_data(
                conn, schema, table, items, url_field, batch_id
            )
        else:
            async with self.db_pool.acquire() as conn:
                await self._execute_store_raw_data(
                    conn, schema, table, items, url_field, batch_id
                )

    async def _execute_store_raw_data(
        self,
        conn: asyncpg.Connection,
        schema: str,
        table: str,
        items: list[dict[str, Any]],
        url_field: str,
        batch_id: str | None,
    ) -> None:
        """Execute raw data storage with proper error handling."""
        if not items:
            return

        # Determine if we should attach run_id (only for _list_raw tables)
        include_run_id = (
            table.endswith("_list_raw")
            and hasattr(self, "current_run_id")
            and self.current_run_id is not None
        )

        # Prepare data for storage
        prepared_items = []
        for item in items:
            # Extract source document ID
            source_doc_id = item.get(self.id_field)

            # Extract URL properly
            item_url = item.get(url_field)
            if not item_url and "url" in item:
                item_url = item["url"]

            # Generate ETL batch ID (use batch_id if provided, otherwise generate new UUID)
            etl_batch_id = batch_id if batch_id else str(uuid.uuid4())

            # Add metadata
            item_with_metadata = {
                "id_uuid": str(uuid.uuid4()),
                "url": item_url,
                "batch_id": batch_id or str(uuid.uuid4()),
                "scraped_at": datetime.now(UTC),
                "payload": self._safe_json_dumps(item),
                "source_doc_id": source_doc_id,
                "etl_batch_id": etl_batch_id,
            }
            if include_run_id:
                item_with_metadata["run_id"] = self.current_run_id
            prepared_items.append(item_with_metadata)

        # Insert data
        try:
            # Create table if needed
            columns_ddl = """
                    id_uuid TEXT PRIMARY KEY,
                    url TEXT,
                    batch_id TEXT,
                    scraped_at TIMESTAMPTZ,
                    payload JSONB,
                    endpoint TEXT,
                    source_doc_id TEXT,
                    etl_batch_id TEXT
            """
            if include_run_id:
                columns_ddl += ",\n                    run_id TEXT"

            await conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table} (
                    {columns_ddl}
                );
            """)

            # Build dynamic insert statement
            base_cols = [
                "id_uuid",
                "url",
                "batch_id",
                "scraped_at",
                "payload",
                "source_doc_id",
                "etl_batch_id",
            ]
            if include_run_id:
                base_cols.append("run_id")
            cols_str = ", ".join(base_cols)
            placeholders = ", ".join([f"${i}" for i in range(1, len(base_cols) + 1)])

            on_conflict_set = ",\n                        ".join(
                f"{col} = EXCLUDED.{col}"
                for col in [
                    "scraped_at",
                    "payload",
                    "source_doc_id",
                    "etl_batch_id",
                ]
            )

            if include_run_id:
                on_conflict_set += ",\n                        run_id = EXCLUDED.run_id"

            for item in prepared_items:
                values = [item.get(col) for col in base_cols]
                await conn.execute(
                    f"""
                    INSERT INTO {schema}.{table} ({cols_str})
                    VALUES ({placeholders})
                    ON CONFLICT (id_uuid) DO UPDATE SET
                        {on_conflict_set}
                """,
                    *values,
                )

            logger.debug(f"Stored {len(prepared_items)} items in {schema}.{table}")

        except Exception as e:
            logger.error(f"Error storing data in {schema}.{table}: {e}")
            raise

    def _string_to_uuid(self, s: str) -> uuid.UUID:
        """Convert string to deterministic UUID."""
        # Use MD5 hash for deterministic UUID generation
        import hashlib

        hash_object = hashlib.md5(s.encode())
        return uuid.UUID(hash_object.hexdigest())

    def _safe_json_dumps(self, obj, **kwargs):
        """Safely dumps JSON with datetime and UUID handling"""

        class LocalDateTimeEncoder(json.JSONEncoder):
            def default(self, obj):
                if isinstance(obj, datetime):
                    return obj.isoformat()
                elif isinstance(obj, timedelta):
                    return obj.total_seconds()
                elif isinstance(obj, uuid.UUID):
                    return str(obj)
                return super().default(obj)

        kwargs.setdefault("cls", LocalDateTimeEncoder)
        kwargs.setdefault("ensure_ascii", False)
        return json.dumps(obj, **kwargs)

    # =============================================================================
    # HELPER METHODS
    # =============================================================================

    async def _update_last_processed_tracking(self, stats: dict[str, Any]) -> None:
        """Update the last_processed_date and total_count in the congressional_last_processed_dates table."""

        if (
            not hasattr(self, "client")
            or not hasattr(self.client, "db_pool")
            or not self.client.db_pool
        ):
            return

        latest_date = stats.get("latest_update_date")

        # Get the current total count from pagination metadata
        # This represents "what was the total count in the API when we processed"
        current_total_count = stats.get("total_count")  # From pagination metadata
        last_processed_count = stats.get(
            "last_processed_count", 0
        )  # What was already processed before this run
        successful_items = stats.get(
            "successful", 0
        )  # What we just processed successfully

        # The total count to store is the current total count from the API
        # This represents the total available when we last processed
        if current_total_count is not None and successful_items > 0:
            # Store the current API total count
            new_total_count = current_total_count
        else:
            # No items processed successfully or no total count available
            new_total_count = None

        if latest_date:
            try:
                success = await self.client.update_last_processed_date(
                    self.data_type_name, latest_date, new_total_count
                )
                if success:
                    if new_total_count is not None:
                        logger.info(
                            f"Updated last_processed_date for {self.data_type_name} to {latest_date} "
                            f"with NEW total count {new_total_count} (was {last_processed_count}, processed {successful_items} new items)"
                        )
                    else:
                        logger.info(
                            f"Updated last_processed_date for {self.data_type_name} to {latest_date} (no total count)"
                        )
                else:
                    logger.error(
                        f"Failed to update last_processed_date for {self.data_type_name}"
                    )
            except Exception as e:
                logger.error(
                    f"Exception updating last_processed_date for {self.data_type_name}: {e}"
                )
        else:
            logger.warning(
                "No latest_update_date found in stats, not updating last_processed_date"
            )

    def get_incomplete_items_for_resume(
        self, available_item_ids: list[str]
    ) -> dict[str, list[str]]:
        """
        Identify items that need specific phases of processing for resume operations.

        Args:
            available_item_ids: List of item IDs that are available for processing

        Returns:
            Dictionary mapping phase names to lists of item IDs that need that phase:
            {
                "list_data": ["item1", "item2"],  # Items missing list data
                "full_data": ["item3", "item4"],  # Items missing full data
                "related_data": ["item5", "item6"]  # Items missing related data
            }
        """
        if not self.progress_tracker:
            # If no progress tracker, assume all items need all phases
            return {
                "list_data": available_item_ids.copy(),
                "full_data": available_item_ids.copy(),
                "related_data": available_item_ids.copy(),
            }

        incomplete_phases = {"list_data": [], "full_data": [], "related_data": []}

        for item_id in available_item_ids:
            # Check each phase for this item
            if not self.progress_tracker.should_skip_item(
                item_id, ProcessingPhase.MAIN_ITEMS, field_name="list_data"
            ):
                incomplete_phases["list_data"].append(item_id)

            if not self.progress_tracker.should_skip_item(
                item_id, ProcessingPhase.MAIN_ITEMS, field_name="full_data"
            ):
                incomplete_phases["full_data"].append(item_id)

            if not self.progress_tracker.should_skip_item(
                item_id, ProcessingPhase.RELATED_ENTITIES
            ):
                incomplete_phases["related_data"].append(item_id)

        return incomplete_phases

    def log_resume_status(self, incomplete_phases: dict[str, list[str]]) -> None:
        """
        Log the resume status showing what phases need processing.

        Args:
            incomplete_phases: Dictionary from get_incomplete_items_for_resume()
        """
        total_list = len(incomplete_phases["list_data"])
        total_full = len(incomplete_phases["full_data"])
        total_related = len(incomplete_phases["related_data"])

        logger.info(f"Resume status for {self.data_type_name}:")
        logger.info(f"  Items needing list data processing: {total_list}")
        logger.info(f"  Items needing full data processing: {total_full}")
        logger.info(f"  Items needing related data processing: {total_related}")

        if total_list == 0 and total_full == 0 and total_related == 0:
            logger.info("  All items are fully processed!")
        else:
            logger.info(
                f"  Resume will process {max(total_list, total_full, total_related)} items with missing phases"
            )

    async def get_generic_related_data(
        self,
        full_data: dict[str, Any],
        field_name: str = None,
        expected_key: str | list[str] = None,
    ) -> list[dict[str, Any]]:
        """
        Generic method to fetch related data from URLs in full_data.

        Args:
            full_data: Complete item data containing URL fields
            field_name: Field name containing the URL (e.g., "actions", "amendments")
            expected_key: Expected response key or list of keys

        Returns:
            List of related data items
        """
        # Use the field_name and expected_key from method parameters
        # If not provided, use sensible defaults from the main config
        if not field_name:
            field_name = self.main_config.api.outer_field

        if not expected_key:
            expected_key = self.main_config.api.expected_key

        if field_name not in full_data:
            return []

        field_data = full_data[field_name]
        if not field_data or "url" not in field_data:
            return []

        url = field_data["url"]
        try:
            # Ensure expected_key is passed as a list
            expected_key_list = (
                [expected_key] if isinstance(expected_key, str) else expected_key
            )
            return await self.fetch_related_data_with_client(
                url, expected_key_list, self.client
            )
        except Exception as e:
            logger.error(f"Error fetching {field_name} from {url}: {e}")
            return []
