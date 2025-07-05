"""
GovInfo-specific base fetcher implementing the 4-phase pattern.

This module provides GovInfo-specific implementations including:
- 4-phase processing pattern (collection → package → granules → granule data)
- GovInfo schema defaults
- GovInfo progress tracking configuration
- GovInfo API patterns
- Sophisticated parallel processing with redistribution strategies
- Phase-specific checkpointing for resume capabilities
- Incremental processing based on last processed dates
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


class GovInfoBaseFetcher(AbstractFetcher):
    """
    GovInfo-specific base fetcher with sophisticated processing patterns.

    Implements the GovInfo API 4-phase pattern with advanced features:
    1. fetch_collection_data() → bulk collection endpoints
    2. fetch_package_data() → individual package URLs
    3. fetch_granules_data() → granule list URLs from packages (if has_granules)
    4. fetch_granule_data() → individual granule URLs

    Advanced Features:
    - Phase-specific checkpointing for resume capabilities
    - Incremental processing based on last processed dates
    - Sophisticated parallel processing with redistribution strategies
    - Comprehensive pagination metadata analysis
    - Adaptive concurrent processing based on available resources
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

        # Get configuration for this data type
        (
            self.collection_code,
            self.has_granules,
            self.granule_name,
            self.granule_doc_class,
        ) = self.get_main_id_configs()

        # Keep a reference to the full typed config
        try:
            from bicam_collection.libs.data_type_registry import get_global_registry

            self.main_config = get_global_registry().get_data_type_config(
                data_type_name
            )
        except Exception:
            self.main_config = None

    def get_main_id_configs(self) -> tuple[str, bool, str | None, str | None]:
        """
        Get GovInfo configuration from the typed config system.
        Returns (collection_code, has_granules, granule_name, granule_doc_class).
        """
        from bicam_collection.libs.data_type_registry import get_global_registry

        try:
            cfg = get_global_registry().get_data_type_config(self.data_type_name)

            collection_code = cfg.api.collection_code
            has_granules = getattr(cfg.api, "has_granules", False)
            granule_name = getattr(cfg.api, "granule_name", None)
            granule_doc_class = getattr(cfg.api, "granule_doc_class", None)

            return collection_code, has_granules, granule_name, granule_doc_class

        except Exception as exc:
            logger.error("Config lookup failed for %s: %s", self.data_type_name, exc)
            # Fallback based on data type name
            return self.data_type_name.upper(), False, None, None

    def setup_progress_tracker(self) -> HierarchicalProgressTracker | None:
        """GovInfo-specific progress tracker setup."""
        if not self.checkpoint_manager or not self.data_type_name:
            logger.warning("Cannot setup progress tracker - missing dependencies")
            return None

        self.progress_tracker = HierarchicalProgressTracker.create_for_stage(
            self.checkpoint_manager,
            "govinfo",  # GovInfo-specific system name
            self.data_type_name,
            ProcessingStage.SCRAPING,
        )
        return self.progress_tracker

    def set_processing_resource(self, processing_resource) -> None:
        """
        Set the processing resource for accessing parallel API clients and other resources.
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
                    run_type=RunType.SCRAPER_GOVINFO,
                    system_name=f"{self.data_type_name}_fetcher",
                    description=f"Fetch {self.data_type_name} data from GovInfo API",
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
    # GOVINFO-SPECIFIC 4-PHASE PATTERN
    # =============================================================================

    async def fetch_collection_data(
        self,
        from_date: str | None = None,
        to_date: str | None = None,
        limit: int | None = None,
        offset_mark: str = "*",
        **kwargs,
    ) -> AsyncIterator[list[dict[str, Any]]]:
        """
        Phase 1: Fetch collection data from GovInfo bulk endpoints.

        Must yield batches of packages containing URLs for Phase 2.

        Args:
            from_date: Start date filter (YYYY-MM-DD)
            to_date: End date filter (YYYY-MM-DD)
            limit: Maximum number of results
            offset_mark: Pagination offset mark
            **kwargs: Additional parameters

        Yields:
            Batches of package dictionaries with 'packageLink' fields
        """
        # Use current date as end date if not provided
        if not to_date:
            to_date = datetime.now(UTC).strftime("%Y-%m-%d")

        # Use reasonable default start date if not provided
        if not from_date:
            start_date = (datetime.now(UTC) - timedelta(days=30)).strftime("%Y-%m-%d")
        else:
            start_date = from_date

        async for batch in self.client.retrieve_collection_data(
            collection_code=self.collection_code,
            start_date=start_date,
            end_date=to_date,
            doc_class=self.granule_doc_class,
            limit=limit,
            offset_mark=offset_mark,
            **kwargs,
        ):
            yield batch

    async def fetch_collection_data_with_client(
        self,
        client,
        from_date: str | None = None,
        to_date: str | None = None,
        limit: int | None = None,
        offset_mark: str = "*",
        **kwargs,
    ) -> AsyncIterator[list[dict[str, Any]]]:
        """
        Fetch collection data using a specific client.

        This method can be overridden by subclasses to customize collection data fetching
        while maintaining compatibility with parallel processing.
        """
        # Use current date as end date if not provided
        if not to_date:
            to_date = datetime.now(UTC).strftime("%Y-%m-%d")

        # Use reasonable default start date if not provided
        if not from_date:
            start_date = (datetime.now(UTC) - timedelta(days=30)).strftime("%Y-%m-%d")
        else:
            start_date = from_date

        async for batch in client.retrieve_collection_data(
            collection_code=self.collection_code,
            start_date=start_date,
            end_date=to_date,
            doc_class=self.granule_doc_class,
            limit=limit,
            offset_mark=offset_mark,
            **kwargs,
        ):
            yield batch

    async def fetch_package_data(self, package_url: str) -> dict[str, Any] | None:
        """
        Phase 2: Fetch complete package data from GovInfo URL.

        Args:
            package_url: Individual package URL from Phase 1

        Returns:
            Complete package data or None on error
        """
        package_data = await self.fetch_package_data_with_client(
            package_url, self.client
        )
        if package_data:
            # Extract package ID for tracking
            package_id = self.extract_package_id(package_data)
            package_data["packageId"] = package_id
        return package_data

    async def fetch_package_data_with_client(
        self, package_url: str, client
    ) -> dict[str, Any] | None:
        """
        Fetch complete package data using a specific client.
        """
        return await client.retrieve_package_data_from_url(package_url)

    async def fetch_granules_data(
        self, package_data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """
        Phase 3: Fetch granules list data (conditional based on has_granules).

        Args:
            package_data: Complete package data from Phase 2

        Returns:
            List of granule items or empty list if no granules
        """
        if not self.has_granules:
            return []

        granules_url = package_data.get("granulesLink")
        if not granules_url:
            logger.debug("No granulesLink found in package data")
            return []

        return await self.fetch_granules_data_with_client(
            granules_url, self.granule_doc_class, self.client
        )

    async def fetch_granules_data_with_client(
        self, granules_url: str, granule_doc_class: str | None, client
    ) -> list[dict[str, Any]]:
        """
        Fetch granules list data using a specific client.
        """
        return await client.retrieve_granules_from_url(
            granules_url, granule_class=granule_doc_class
        )

    async def fetch_granule_data(self, granule_url: str) -> dict[str, Any] | None:
        """
        Phase 4: Fetch complete granule data from GovInfo URL.

        Args:
            granule_url: Individual granule URL from Phase 3

        Returns:
            Complete granule data or None on error
        """
        granule_data = await self.fetch_granule_data_with_client(
            granule_url, self.client
        )
        if granule_data:
            # Extract granule ID for tracking
            granule_id = self.extract_granule_id(granule_data)
            granule_data["granuleId"] = granule_id
        return granule_data

    async def fetch_granule_data_with_client(
        self, granule_url: str, client
    ) -> dict[str, Any] | None:
        """
        Fetch complete granule data using a specific client.
        """
        return await client.retrieve_granule_data_from_url(granule_url)

    # =============================================================================
    # MAIN PROCESSING METHOD - GOVINFO 4-PHASE IMPLEMENTATION
    # =============================================================================

    async def process_items(
        self,
        from_date: str | None = None,
        to_date: str | None = None,
        limit: int | None = None,
        max_items: int | None = None,
        batch_size: int = 50,
        enable_parallelization: bool = False,
        max_concurrent: int = 10,
        redistribute_idle_sessions: bool = True,
        **kwargs,
    ) -> dict[str, Any]:
        """
        GovInfo-specific 4-phase processing implementation with sophisticated parallel processing.

        Args:
            from_date: Start date filter (YYYY-MM-DD)
            to_date: End date filter (YYYY-MM-DD)
            limit: Items per page (GovInfo default: 1000)
            max_items: Maximum total items to process
            batch_size: Items per storage batch
            enable_parallelization: Enable parallel processing
            max_concurrent: Maximum concurrent operations
            redistribute_idle_sessions: Use idle sessions for granule processing
            **kwargs: Additional parameters

        Returns:
            Processing results with statistics
        """
        logger.info(f"Starting {self.data_type_name} GovInfo processing")
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
        """Process items sequentially with sophisticated pagination analysis."""
        stats = {
            "total_processed": 0,
            "successful": 0,
            "errors": 0,
            "batches_stored": 0,
            "start_time": datetime.now(UTC),
            "latest_date": None,
            "packages_processed": 0,
            "granules_processed": 0,
        }

        batch_items = []
        processed_count = 0
        first_batch_processed = False

        # Use the same pagination metadata analysis as parallel processing
        # This ensures sequential processing handles date filters correctly
        try:
            pages_to_process, pagination_metadata = await self._get_pagination_metadata(
                from_date, to_date, limit, **kwargs
            )

            if pages_to_process is not None:
                # We have pagination metadata, use it to guide processing
                if pages_to_process == 0:
                    logger.info("No new items to process based on pagination analysis")
                    stats["end_time"] = datetime.now(UTC)
                    stats["duration"] = (
                        stats["end_time"] - stats["start_time"]
                    ).total_seconds()
                    logger.info(
                        f"Sequential processing completed (no new items): {stats}"
                    )
                    return stats

                # Store pagination metadata in stats for tracking
                stats["total_count"] = pagination_metadata.get("total_count")
                stats["last_processed_count"] = pagination_metadata.get(
                    "last_processed_count"
                )
                stats["incremental_count"] = pagination_metadata.get(
                    "incremental_count"
                )

                logger.info(
                    f"Sequential processing: {pagination_metadata.get('incremental_count')} items "
                    f"across {pages_to_process} pages to process"
                )

                # Process with pagination awareness
                current_offset_mark = "*"
                pages_processed = 0

                while pages_processed < pages_to_process:
                    if max_items and processed_count >= max_items:
                        break

                    async for collection_batch in self.fetch_collection_data(
                        from_date=from_date,
                        to_date=to_date,
                        limit=pagination_metadata.get("page_size", limit or 1000),
                        offset_mark=current_offset_mark,
                        single_page_only=True,
                        **kwargs,
                    ):
                        # Capture total count from first response
                        if not first_batch_processed and hasattr(
                            self.client, "last_response_metadata"
                        ):
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
                                    total_count = pagination.get("count")
                                    if total_count is not None and isinstance(
                                        total_count, int
                                    ):
                                        # Update stats with actual total count if we don't have it
                                        if (
                                            "total_count" not in stats
                                            or stats["total_count"] is None
                                        ):
                                            stats["total_count"] = total_count
                                        logger.info(
                                            f"Captured total count for sequential processing: {total_count}"
                                        )

                                # Check for next page offset mark
                                next_url = pagination.get("next")
                                if next_url and "offsetMark=" in next_url:
                                    import re

                                    match = re.search(r"offsetMark=([^&]+)", next_url)
                                    if match:
                                        current_offset_mark = match.group(1)

                            first_batch_processed = True

                        for package_item in collection_batch:
                            if max_items and processed_count >= max_items:
                                break

                            try:
                                processed_item = await self._process_single_package(
                                    package_item
                                )
                                batch_items.append(processed_item)
                                stats["successful"] += 1
                                stats["packages_processed"] += 1

                                # Count granules if present
                                if "granules_data" in processed_item:
                                    stats["granules_processed"] += len(
                                        processed_item["granules_data"]
                                    )

                                # Track latest date for incremental processing
                                item_date = package_item.get(
                                    "lastModified"
                                ) or package_item.get("dateIssued")
                                if item_date and (
                                    not stats["latest_date"]
                                    or item_date > stats["latest_date"]
                                ):
                                    stats["latest_date"] = item_date

                            except Exception as e:
                                logger.error(
                                    f"Error processing package {package_item.get('packageLink', 'UNKNOWN')}: {e}",
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

                    pages_processed += 1

                    if max_items and processed_count >= max_items:
                        break

            else:
                # Fall back to simple iteration when pagination metadata is not available
                logger.warning(
                    "Pagination metadata not available, falling back to simple iteration"
                )

                async for collection_batch in self.fetch_collection_data(
                    from_date=from_date, to_date=to_date, limit=limit, **kwargs
                ):
                    # Capture total count from first response
                    if not first_batch_processed and hasattr(
                        self.client, "last_response_metadata"
                    ):
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
                                total_count = pagination.get("count")
                                if total_count is not None and isinstance(
                                    total_count, int
                                ):
                                    stats["total_count"] = total_count
                                    logger.info(
                                        f"Captured total count for sequential processing: {total_count}"
                                    )
                        first_batch_processed = True

                    for package_item in collection_batch:
                        if max_items and processed_count >= max_items:
                            break

                        try:
                            processed_item = await self._process_single_package(
                                package_item
                            )
                            batch_items.append(processed_item)
                            stats["successful"] += 1
                            stats["packages_processed"] += 1

                            # Count granules if present
                            if "granules_data" in processed_item:
                                stats["granules_processed"] += len(
                                    processed_item["granules_data"]
                                )

                            # Track latest date for incremental processing
                            item_date = package_item.get(
                                "lastModified"
                            ) or package_item.get("dateIssued")
                            if item_date and (
                                not stats["latest_date"]
                                or item_date > stats["latest_date"]
                            ):
                                stats["latest_date"] = item_date

                        except Exception as e:
                            logger.error(
                                f"Error processing package {package_item.get('packageLink', 'UNKNOWN')}: {e}",
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

        except Exception as e:
            logger.error(f"Error in sequential processing pagination analysis: {e}")
            logger.info("Falling back to simple iteration")

            # Fall back to the original simple approach if pagination analysis fails
            async for collection_batch in self.fetch_collection_data(
                from_date=from_date, to_date=to_date, limit=limit, **kwargs
            ):
                for package_item in collection_batch:
                    if max_items and processed_count >= max_items:
                        break

                    try:
                        processed_item = await self._process_single_package(
                            package_item
                        )
                        batch_items.append(processed_item)
                        stats["successful"] += 1
                        stats["packages_processed"] += 1

                        # Count granules if present
                        if "granules_data" in processed_item:
                            stats["granules_processed"] += len(
                                processed_item["granules_data"]
                            )

                        # Track latest date for incremental processing
                        item_date = package_item.get(
                            "lastModified"
                        ) or package_item.get("dateIssued")
                        if item_date and (
                            not stats["latest_date"] or item_date > stats["latest_date"]
                        ):
                            stats["latest_date"] = item_date

                    except Exception as e:
                        logger.error(
                            f"Error processing package {package_item.get('packageLink', 'UNKNOWN')}: {e}",
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
        redistribute_idle_sessions: bool,
        **kwargs,
    ) -> dict[str, Any]:
        """Process items with parallelization using multiple API clients."""
        logger.info("Using parallelization for GovInfo processing")

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

        return await self._process_with_concurrent_processing(
            api_clients=api_clients,
            from_date=from_date,
            to_date=to_date,
            batch_size=batch_size,
            max_concurrent=max_concurrent,
            redistribute_idle_sessions=redistribute_idle_sessions,
            limit=limit,
            **kwargs,
        )

    async def _process_with_concurrent_processing(
        self,
        api_clients: list,
        from_date: str | None,
        to_date: str | None,
        batch_size: int,
        max_concurrent: int,
        redistribute_idle_sessions: bool,
        limit: int | None = None,
        **kwargs,
    ) -> dict[str, Any]:
        """Process with sophisticated concurrent processing adapted for GovInfo's 4-phase pattern."""
        stats = {
            "total_processed": 0,
            "successful": 0,
            "errors": 0,
            "batches_stored": 0,
            "start_time": datetime.now(UTC),
            "redistribution_used": False,
            "latest_date": None,
            "packages_processed": 0,
            "granules_processed": 0,
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
                logger.info("No new items to process based on pagination analysis")
                stats["end_time"] = datetime.now(UTC)
                stats["duration"] = (
                    stats["end_time"] - stats["start_time"]
                ).total_seconds()
                logger.info(f"Processing completed (no new items): {stats}")
                return stats

            logger.info(
                f"Incremental items to process: {pagination_metadata.get('incremental_count')} "
                f"across {pages_to_process} estimated pages"
            )

            # Store pagination metadata in stats
            stats["total_count"] = pagination_metadata.get("total_count")
            stats["last_processed_count"] = pagination_metadata.get(
                "last_processed_count"
            )
            stats["incremental_count"] = pagination_metadata.get("incremental_count")

            # Check if we should use redistribution strategy for granule processing
            if (
                redistribute_idle_sessions
                and len(api_clients)
                > 2  # Need at least 3 clients for effective redistribution
                and self.has_granules  # Only useful if we have granules to redistribute
            ):
                logger.info(
                    f"Using redistribution strategy for GovInfo 4-phase processing: "
                    f"{len(api_clients)} sessions available"
                )
                stats["redistribution_used"] = True
                return await self._process_with_redistribution_strategy(
                    api_clients,
                    from_date,
                    to_date,
                    limit,
                    batch_size,
                    max_concurrent,
                    pagination_metadata,
                    **kwargs,
                )

            # Standard parallel processing for GovInfo
            incremental_count = pagination_metadata.get("incremental_count", 0)
            distributions = self._calculate_interleaved_distribution(
                len(api_clients), incremental_count, limit or 1000
            )

            # Process with assigned distributions
            tasks = []
            for session_id, (client, distribution) in enumerate(
                zip(api_clients, distributions, strict=False)
            ):
                task = self._process_assigned_distribution(
                    client,
                    from_date,
                    to_date,
                    limit,
                    distribution,
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
                    stats["packages_processed"] += result.get("packages_processed", 0)
                    stats["granules_processed"] += result.get("granules_processed", 0)

                    # Track latest date across all sessions
                    session_latest = result.get("latest_date")
                    if session_latest and (
                        not stats["latest_date"]
                        or session_latest > stats["latest_date"]
                    ):
                        stats["latest_date"] = session_latest

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
        batch_size: int,
        max_concurrent: int,
        pagination_metadata: dict[str, Any],
        **kwargs,
    ) -> dict[str, Any]:
        """
        Process with redistribution strategy adapted for GovInfo's 4-phase pattern.

        Strategy:
        1. Use subset of sessions for collection/package data (Phases 1 & 2)
        2. Collect all package data
        3. Redistribute all sessions for parallel granule processing (Phases 3 & 4)
        """
        logger.info("Using redistribution strategy for GovInfo 4-phase processing")

        stats = {
            "total_processed": 0,
            "successful": 0,
            "errors": 0,
            "batches_stored": 0,
            "start_time": datetime.now(UTC),
            "phase_1_2_sessions": min(
                3, len(api_clients)
            ),  # Use subset for collection/package
            "phase_3_4_sessions": len(api_clients),  # Use all for granules
            "latest_date": None,
            "packages_processed": 0,
            "granules_processed": 0,
            # Preserve pagination metadata for tracking
            "total_count": pagination_metadata.get("total_count"),
            "last_processed_count": pagination_metadata.get("last_processed_count"),
            "incremental_count": pagination_metadata.get("incremental_count"),
        }

        # Phase 1 & 2: Use subset of sessions for collection/package data
        active_clients = api_clients[: stats["phase_1_2_sessions"]]

        logger.info(
            f"Phase 1&2: Using {len(active_clients)} sessions for collection/package data"
        )
        logger.info(
            f"Phase 3&4: Will redistribute {len(api_clients)} sessions for granule processing"
        )

        # Collect all packages from Phase 1 & 2
        all_packages = []

        incremental_count = pagination_metadata.get("incremental_count", 0)
        distributions = self._calculate_interleaved_distribution(
            len(active_clients), incremental_count, limit or 1000
        )

        # Process phases 1 & 2 to get collection and package data
        tasks = []
        for session_id, (client, distribution) in enumerate(
            zip(active_clients, distributions, strict=False)
        ):
            task = self._collect_packages_phases_1_2(
                client,
                from_date,
                to_date,
                limit,
                distribution,
                session_id,
                **kwargs,
            )
            tasks.append(task)

        # Execute Phase 1 & 2 concurrently
        session_results = await asyncio.gather(*tasks, return_exceptions=True)

        # Collect all packages
        for result in session_results:
            if isinstance(result, Exception):
                logger.error(f"Phase 1&2 session failed: {result}")
                stats["errors"] += 1
            elif isinstance(result, dict) and "packages" in result:
                all_packages.extend(result["packages"])
                stats["total_processed"] += result.get("total_processed", 0)
                stats["packages_processed"] += result.get("packages_processed", 0)

                # Track latest date across sessions
                session_latest = result.get("latest_date")
                if session_latest and (
                    not stats["latest_date"] or session_latest > stats["latest_date"]
                ):
                    stats["latest_date"] = session_latest

        logger.info(f"Collected {len(all_packages)} packages from Phase 1&2")

        if not all_packages:
            logger.warning("No packages collected from Phase 1&2")
            return stats

        # Phase 3 & 4: Redistribute all sessions for granule processing (if has_granules)
        if self.has_granules:
            logger.info(
                f"Phase 3&4: Redistributing granule processing across {len(api_clients)} sessions"
            )

            phase_3_4_stats = await self._process_granules_with_redistribution(
                api_clients,
                all_packages,
                batch_size,
                max_concurrent,
            )

            # Combine stats
            stats["successful"] += phase_3_4_stats.get("successful", 0)
            stats["errors"] += phase_3_4_stats.get("errors", 0)
            stats["batches_stored"] += phase_3_4_stats.get("batches_stored", 0)
            stats["granules_processed"] += phase_3_4_stats.get("granules_processed", 0)
        else:
            logger.info("No granules configured, skipping Phase 3&4")
            stats["successful"] = stats["packages_processed"]

        stats["end_time"] = datetime.now(UTC)
        stats["duration"] = (stats["end_time"] - stats["start_time"]).total_seconds()

        # Update last_processed_date if we processed items successfully
        if stats.get("successful", 0) > 0:
            await self._update_last_processed_tracking(stats)

        logger.info(f"Redistribution strategy completed: {stats}")
        return stats

    async def _collect_packages_phases_1_2(
        self,
        client,
        from_date: str | None,
        to_date: str | None,
        limit: int | None,
        distribution: dict[str, Any],
        session_id: int,
        **kwargs,
    ) -> dict[str, Any]:
        """
        Collect packages from Phase 1 (collection) and Phase 2 (package data) with immediate storage.
        Adapted from Congressional pattern for GovInfo's collection/package structure.
        """
        stats = {
            "total_processed": 0,
            "packages_processed": 0,
            "packages": [],
            "session_id": session_id,
            "latest_date": None,
        }

        logger.info(
            f"Phase 1&2 Session {session_id}: Processing distribution with {distribution.get('item_count', 0)} items"
        )

        # For GovInfo, we'll process the collection data sequentially within this session
        # then fetch package data for each collected item
        current_offset_mark = "*"
        items_processed = 0
        target_items = distribution.get("item_count", 0)

        while items_processed < target_items:
            try:
                async for collection_batch in self.fetch_collection_data_with_client(
                    client,
                    from_date=from_date,
                    to_date=to_date,
                    limit=min(limit or 1000, target_items - items_processed),
                    offset_mark=current_offset_mark,
                    single_page_only=True,
                    **kwargs,
                ):
                    for package_item in collection_batch:
                        if items_processed >= target_items:
                            break

                        try:
                            # Extract package ID for tracking
                            package_id = self.extract_package_id(package_item)
                            package_item["packageId"] = package_id

                            # Phase 1: Store collection data immediately
                            batch_id = str(uuid.uuid4())
                            await self.store_collection_data(package_item, batch_id)

                            # Phase 2: Fetch and store package data
                            package_url = package_item.get("packageLink")
                            if package_url:
                                package_data = (
                                    await self.fetch_package_data_with_client(
                                        package_url, client
                                    )
                                )
                                if package_data:
                                    package_data["packageId"] = package_id
                                    await self.store_package_data(
                                        package_data, batch_id
                                    )

                                    # Collect package data for Phase 3&4
                                    combined_package = {
                                        "packageId": package_id,
                                        "collection_data": package_item,
                                        "package_data": package_data,
                                        "batch_id": batch_id,
                                    }
                                    stats["packages"].append(combined_package)

                            # Track latest date
                            item_date = package_item.get(
                                "lastModified"
                            ) or package_item.get("dateIssued")
                            if item_date and (
                                not stats["latest_date"]
                                or item_date > stats["latest_date"]
                            ):
                                stats["latest_date"] = item_date

                            stats["packages_processed"] += 1
                            items_processed += 1

                        except Exception as e:
                            logger.error(
                                f"Session {session_id} error processing package: {e}",
                                exc_info=True,
                            )

                    # Update offset mark for next page
                    if hasattr(client, "last_response_metadata"):
                        metadata = client.last_response_metadata
                        if metadata and "pagination" in metadata:
                            pagination = metadata["pagination"]
                            next_url = pagination.get("next")
                            if next_url and "offsetMark=" in next_url:
                                import re

                                match = re.search(r"offsetMark=([^&]+)", next_url)
                                if match:
                                    current_offset_mark = match.group(1)
                                else:
                                    break  # No more pages
                            else:
                                break  # No more pages
                        else:
                            break  # No pagination info
                    else:
                        break  # No client metadata

                if items_processed >= target_items:
                    break

            except Exception as e:
                logger.error(
                    f"Session {session_id} error processing collection batch: {e}"
                )
                break

        stats["total_processed"] = items_processed
        logger.info(
            f"Phase 1&2 Session {session_id}: Processed {len(stats['packages'])} packages"
        )
        return stats

    async def _process_granules_with_redistribution(
        self,
        api_clients: list,
        all_packages: list[dict[str, Any]],
        batch_size: int,
        max_concurrent: int,
    ) -> dict[str, Any]:
        """
        Process granules (Phase 3 & 4) with redistribution across all available sessions.
        Adapted from Congressional pattern for GovInfo's granule structure.
        """
        stats = {
            "successful": 0,
            "errors": 0,
            "batches_stored": 0,
            "granules_processed": 0,
        }

        if not all_packages:
            return stats

        # Distribute packages across all available sessions
        packages_per_session = len(all_packages) // len(api_clients)
        remainder = len(all_packages) % len(api_clients)

        package_assignments = []
        start_idx = 0

        for i in range(len(api_clients)):
            # Distribute remainder across first sessions
            session_count = packages_per_session + (1 if i < remainder else 0)
            end_idx = start_idx + session_count
            package_assignments.append(all_packages[start_idx:end_idx])
            start_idx = end_idx

        logger.info(
            f"Granule distribution: {[len(assignment) for assignment in package_assignments]}"
        )

        # Process granules in parallel across all sessions
        tasks = []
        for session_id, (client, assigned_packages) in enumerate(
            zip(api_clients, package_assignments, strict=False)
        ):
            if assigned_packages:  # Only create tasks for sessions with packages
                task = self._process_granules_for_session(
                    client,
                    assigned_packages,
                    batch_size,
                    max_concurrent,
                    session_id,
                )
                tasks.append(task)

        # Execute all granule sessions concurrently
        if tasks:
            session_results = await asyncio.gather(*tasks, return_exceptions=True)

            # Aggregate results
            for result in session_results:
                if isinstance(result, Exception):
                    logger.error(f"Granule session failed: {result}")
                    stats["errors"] += 1
                elif isinstance(result, dict):
                    stats["successful"] += result.get("successful", 0)
                    stats["errors"] += result.get("errors", 0)
                    stats["batches_stored"] += result.get("batches_stored", 0)
                    stats["granules_processed"] += result.get("granules_processed", 0)

        return stats

    async def _process_granules_for_session(
        self,
        client,
        assigned_packages: list[dict[str, Any]],
        batch_size: int,
        max_concurrent: int,
        session_id: int,
    ) -> dict[str, Any]:
        """
        Process granules for packages assigned to a specific session with immediate storage.
        """
        stats = {
            "successful": 0,
            "errors": 0,
            "batches_stored": 0,
            "granules_processed": 0,
            "session_id": session_id,
        }

        logger.info(
            f"Granule Session {session_id}: Processing {len(assigned_packages)} packages"
        )

        semaphore = asyncio.Semaphore(max_concurrent)

        async def process_package_granules(package_data: dict[str, Any]):
            async with semaphore:
                try:
                    package_id = package_data.get("packageId", "UNKNOWN")
                    full_package_data = package_data.get("package_data", {})
                    batch_id = package_data.get("batch_id")

                    # Phase 3: Fetch granules list data
                    granules_list = await self.fetch_granules_data_with_client(
                        full_package_data.get("granulesLink"),
                        self.granule_doc_class,
                        client,
                    )

                    if granules_list:
                        # Store granules list data
                        await self.store_granules_data(
                            granules_list, package_id, batch_id
                        )

                        # Phase 4: Fetch individual granule data
                        granule_data_list = []
                        for granule_item in granules_list:
                            granule_url = granule_item.get("granuleLink")
                            if granule_url:
                                granule_data = (
                                    await self.fetch_granule_data_with_client(
                                        granule_url, client
                                    )
                                )
                                if granule_data:
                                    granule_id = self.extract_granule_id(granule_data)
                                    granule_data["granuleId"] = granule_id
                                    granule_data_list.append(granule_data)

                        if granule_data_list:
                            await self.store_granule_data(
                                granule_data_list, package_id, batch_id
                            )

                        stats["granules_processed"] += len(granule_data_list)

                    logger.debug(
                        f"Session {session_id}: Processed granules for package {package_id}"
                    )
                    return True

                except Exception as e:
                    logger.error(
                        f"Granule Session {session_id} error processing package {package_data.get('packageId', 'UNKNOWN')}: {e}"
                    )
                    raise

        # Process all packages concurrently within this session
        tasks = [process_package_granules(package) for package in assigned_packages]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Handle results
        for result in results:
            if isinstance(result, Exception):
                stats["errors"] += 1
            else:
                stats["successful"] += 1

        # Note: No batch storage needed since we store immediately
        stats["batches_stored"] = stats[
            "successful"
        ]  # Each package processed individually

        logger.info(f"Granule Session {session_id} completed: {stats}")
        return stats

    async def _process_assigned_distribution(
        self,
        client,
        from_date: str | None,
        to_date: str | None,
        limit: int | None,
        distribution: dict[str, Any],
        batch_size: int,
        max_concurrent: int,
        session_id: int,
        **kwargs,
    ) -> dict[str, Any]:
        """Process assigned distribution for a specific client session."""
        stats = {
            "total_processed": 0,
            "successful": 0,
            "errors": 0,
            "batches_stored": 0,
            "packages_processed": 0,
            "granules_processed": 0,
            "latest_date": None,
            "session_id": session_id,
        }

        logger.info(
            f"Session {session_id}: Processing distribution with {distribution.get('item_count', 0)} items"
        )

        batch_items = []
        target_items = distribution.get("item_count", 0)
        items_processed = 0
        current_offset_mark = "*"

        while items_processed < target_items:
            try:
                async for collection_batch in self.fetch_collection_data_with_client(
                    client,
                    from_date=from_date,
                    to_date=to_date,
                    limit=min(limit or 1000, target_items - items_processed),
                    offset_mark=current_offset_mark,
                    single_page_only=True,
                    **kwargs,
                ):
                    for package_item in collection_batch:
                        if items_processed >= target_items:
                            break

                        try:
                            processed_item = (
                                await self._process_single_package_with_client(
                                    package_item, client
                                )
                            )
                            batch_items.append(processed_item)
                            stats["successful"] += 1
                            stats["packages_processed"] += 1

                            # Count granules if present
                            if "granules_data" in processed_item:
                                stats["granules_processed"] += len(
                                    processed_item["granules_data"]
                                )

                            # Track latest date
                            item_date = package_item.get(
                                "lastModified"
                            ) or package_item.get("dateIssued")
                            if item_date and (
                                not stats["latest_date"]
                                or item_date > stats["latest_date"]
                            ):
                                stats["latest_date"] = item_date

                        except Exception as e:
                            logger.error(
                                f"Session {session_id} error processing package: {e}",
                                exc_info=True,
                            )
                            stats["errors"] += 1

                        items_processed += 1
                        stats["total_processed"] += 1

                        # Store batch when full
                        if len(batch_items) >= batch_size:
                            await self._store_batch(batch_items[:batch_size])
                            stats["batches_stored"] += 1
                            del batch_items[:batch_size]

                        if items_processed >= target_items:
                            break

                    # Update offset mark for next page
                    if hasattr(client, "last_response_metadata"):
                        metadata = client.last_response_metadata
                        if metadata and "pagination" in metadata:
                            pagination = metadata["pagination"]
                            next_url = pagination.get("next")
                            if next_url and "offsetMark=" in next_url:
                                import re

                                match = re.search(r"offsetMark=([^&]+)", next_url)
                                if match:
                                    current_offset_mark = match.group(1)
                                else:
                                    break  # No more pages
                            else:
                                break  # No more pages
                        else:
                            break  # No pagination info
                    else:
                        break  # No client metadata

                if items_processed >= target_items:
                    break

            except Exception as e:
                logger.error(f"Session {session_id} error processing distribution: {e}")
                stats["errors"] += 1
                break

        # Store remaining items
        if batch_items:
            await self._store_batch(batch_items)
            stats["batches_stored"] += 1

        logger.info(f"Session {session_id} completed: {stats}")
        return stats

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
        """Simple distribution fallback when pagination metadata is unavailable."""
        stats = {
            "total_processed": 0,
            "successful": 0,
            "errors": 0,
            "batches_stored": 0,
            "packages_processed": 0,
            "granules_processed": 0,
            "start_time": datetime.now(UTC),
            "latest_date": None,
        }

        # For simple distribution, use first client only to avoid duplication
        # This is a fallback when we can't determine proper pagination
        client = api_clients[0]

        batch_items = []
        processed_count = 0
        first_batch_processed = False

        async for collection_batch in self.fetch_collection_data_with_client(
            client,
            from_date=from_date,
            to_date=to_date,
            limit=limit,
            **kwargs,
        ):
            if not collection_batch:
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

            for package_item in collection_batch:
                try:
                    if not package_item or not isinstance(package_item, dict):
                        logger.warning(
                            f"Skipping invalid package item: {type(package_item)}"
                        )
                        stats["errors"] += 1
                        continue

                    processed_item = await self._process_single_package_with_client(
                        package_item, client
                    )
                    batch_items.append(processed_item)
                    stats["successful"] += 1
                    stats["packages_processed"] += 1

                    # Count granules if present
                    if "granules_data" in processed_item:
                        stats["granules_processed"] += len(
                            processed_item["granules_data"]
                        )

                    # Track latest date for incremental processing
                    item_date = package_item.get("lastModified") or package_item.get(
                        "dateIssued"
                    )
                    if item_date and (
                        not stats["latest_date"] or item_date > stats["latest_date"]
                    ):
                        stats["latest_date"] = item_date

                except Exception as e:
                    logger.error(
                        f"Error processing package {package_item.get('packageLink', 'UNKNOWN') if isinstance(package_item, dict) else 'INVALID_ITEM'}: {e}",
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

    # =============================================================================
    # STORAGE METHODS - GOVINFO DEFAULTS
    # =============================================================================

    async def store_collection_data(
        self,
        collection_data: dict[str, Any],
        batch_id: str | None = None,
        schema: str = "bicam_raw_govinfo",
        table_suffix: str = "collection_raw",
    ) -> None:
        """Store collection data with GovInfo defaults."""
        table_name = f"{self.data_type_name}_{table_suffix}"
        await self.store_raw_data(
            schema=schema,
            table=table_name,
            items=[collection_data],
            url_field="packageLink",
            batch_id=batch_id,
        )

    async def store_package_data(
        self,
        package_data: dict[str, Any],
        batch_id: str | None = None,
        schema: str = "bicam_raw_govinfo",
        table_suffix: str = "package_raw",
    ) -> None:
        """Store package data with GovInfo defaults."""
        table_name = f"{self.data_type_name}_{table_suffix}"

        # Extract package ID for the record
        package_id = self.extract_package_id(package_data)
        package_data_with_id = {
            **package_data,
            "packageId": package_id,
            "batch_id": batch_id,
        }

        await self.store_raw_data(
            schema=schema,
            table=table_name,
            items=[package_data_with_id],
            url_field="packageLink",
            batch_id=batch_id,
        )

    async def store_granules_data(
        self,
        granules_list: list[dict[str, Any]],
        package_id: str,
        batch_id: str | None = None,
        schema: str = "bicam_raw_govinfo",
        table_suffix: str = "granules_raw",
    ) -> None:
        """Store granules list data with GovInfo defaults."""
        if not self.granule_name:
            logger.warning("No granule_name configured, cannot store granules data")
            return

        table_name = f"{self.granule_name}_{table_suffix}"

        # Add parent package ID to each granule item
        items_with_parent = []
        for item in granules_list:
            item_with_parent = {
                **item,
                "packageId": package_id,
                "batch_id": batch_id,
            }
            items_with_parent.append(item_with_parent)

        await self.store_raw_data(
            schema=schema,
            table=table_name,
            items=items_with_parent,
            url_field="granuleLink",
            batch_id=batch_id,
        )

    async def store_granule_data(
        self,
        granule_data_list: list[dict[str, Any]],
        package_id: str,
        batch_id: str | None = None,
        schema: str = "bicam_raw_govinfo",
        table_suffix: str = "granule_data_raw",
    ) -> None:
        """Store individual granule data with GovInfo defaults."""
        if not self.granule_name:
            logger.warning("No granule_name configured, cannot store granule data")
            return

        table_name = f"{self.granule_name}_{table_suffix}"

        # Add parent package ID to each granule data item
        items_with_parent = []
        for item in granule_data_list:
            item_with_parent = {
                **item,
                "packageId": package_id,
                "batch_id": batch_id,
            }
            items_with_parent.append(item_with_parent)

        await self.store_raw_data(
            schema=schema,
            table=table_name,
            items=items_with_parent,
            url_field="granuleLink",
            batch_id=batch_id,
        )

    async def store_raw_data(
        self,
        schema: str = "bicam_raw_govinfo",
        table: str = None,
        items: list[dict[str, Any]] = None,
        url_field: str = "packageLink",
        batch_id: str | None = None,
        conn: asyncpg.Connection | None = None,
        **kwargs,
    ) -> None:
        """Store raw data with GovInfo schema defaults."""
        if not all([table, items]):
            raise ValueError("table and items are required")

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

        # Determine ID field based on table type
        id_field = "granuleId" if "granule" in table else "packageId"

        # Determine if we should attach run_id (only for _collection_raw tables)
        include_run_id = (
            table.endswith("_collection_raw")
            and hasattr(self, "current_run_id")
            and self.current_run_id is not None
        )

        # Prepare data for storage
        prepared_items = []
        for item in items:
            # Extract source document ID
            source_doc_id = item.get(id_field)

            # Extract URL properly
            item_url = item.get(url_field)
            if not item_url and "url" in item:
                item_url = item["url"]

            # Generate ETL batch ID
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
        """Update the last_processed_date in the govinfo_last_processed_dates table."""
        if (
            not hasattr(self, "client")
            or not hasattr(self.client, "db_pool")
            or not self.client.db_pool
        ):
            return

        latest_date = stats.get("latest_date")

        if latest_date:
            try:
                success = await self.client.update_last_processed_date(
                    self.data_type_name, latest_date
                )
                if success:
                    logger.info(
                        f"Updated last_processed_date for {self.data_type_name} to {latest_date}"
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
                "No latest_date found in stats, not updating last_processed_date"
            )

    def extract_package_id(self, package_data: dict[str, Any]) -> str:
        """
        Extract package ID from package data.

        Args:
            package_data: Package data dictionary

        Returns:
            Package ID string
        """
        package_id = package_data.get("packageId")
        if package_id:
            return package_id

        # Try alternative fields
        package_id = package_data.get("package_id")
        if package_id:
            return package_id

        # Extract from packageLink if available
        package_link = package_data.get("packageLink", "")
        if package_link:
            # Extract ID from URL pattern
            import re

            match = re.search(r"/packages/([^/?]+)", package_link)
            if match:
                return match.group(1)

        return "UNKNOWN_PACKAGE_ID"

    def extract_granule_id(self, granule_data: dict[str, Any]) -> str:
        """
        Extract granule ID from granule data.

        Args:
            granule_data: Granule data dictionary

        Returns:
            Granule ID string
        """
        granule_id = granule_data.get("granuleId")
        if granule_id:
            return granule_id

        # Try alternative fields
        granule_id = granule_data.get("granule_id")
        if granule_id:
            return granule_id

        # Extract from granuleLink if available
        granule_link = granule_data.get("granuleLink", "")
        if granule_link:
            # Extract ID from URL pattern
            import re

            match = re.search(r"/granules/([^/?]+)", granule_link)
            if match:
                return match.group(1)

        return "UNKNOWN_GRANULE_ID"

    def extract_item_id(self, item_data: dict[str, Any]) -> str:
        """
        Extract item ID from item data (compatibility method).

        This method provides compatibility with the abstract base class.
        For GovInfo, we primarily work with package IDs.
        """
        return self.extract_package_id(item_data)

    async def _get_pagination_metadata(
        self,
        from_date: str | None,
        to_date: str | None,
        limit: int | None,
        **kwargs,
    ) -> tuple[int | None, dict[str, Any]]:
        """
        Get pagination metadata for optimal parallel distribution with incremental support.

        Adapted from Congressional fetcher for GovInfo's collection/package structure.
        Uses GovInfo API patterns with "count" and "nextPage" fields.

        Returns:
            Tuple of (pages_to_process, metadata_dict) where:
            - pages_to_process: Number of pages that actually need processing
            - metadata_dict: Contains total_count, last_processed_count, incremental_count, etc.
        """
        try:
            # Step 1: Get the TRUE total count (without date filters) if we have date filters
            true_total_count = None
            if from_date or to_date:
                async for _first_batch in self.fetch_collection_data(
                    from_date=None,  # No date filters
                    to_date=None,  # No date filters
                    limit=0,  # Just get metadata, no actual items
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
            async for _first_batch in self.fetch_collection_data(
                from_date=from_date,
                to_date=to_date,
                limit=limit or 1000,  # GovInfo typically uses 1000 as default
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
                                page_size = limit or 1000

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
                                        self.client, "access_last_processed_date"
                                    ):
                                        # For GovInfo, we track by data type name
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
                                # GovInfo uses offset marks rather than traditional pagination
                                # but we can estimate pages for parallel distribution
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

    def _calculate_interleaved_distribution(
        self, num_sessions: int, total_items: int, page_size: int
    ) -> list[dict[str, Any]]:
        """
        Calculate interleaved distribution for GovInfo parallel processing.

        Unlike Congressional API which uses offset-based pagination,
        GovInfo uses offset marks and nextPage URLs. We need to adapt
        the distribution strategy accordingly.

        Args:
            num_sessions: Number of parallel sessions
            total_items: Total number of items to process
            page_size: Items per page

        Returns:
            List of distribution configs for each session
        """
        distributions = []

        # For GovInfo, we'll use a simpler approach initially
        # Each session gets a portion of the total items
        items_per_session = total_items // num_sessions
        remainder = total_items % num_sessions

        start_item = 0
        for session_id in range(num_sessions):
            # Distribute remainder across first sessions
            session_count = items_per_session + (1 if session_id < remainder else 0)

            distributions.append(
                {
                    "session_id": session_id,
                    "start_item": start_item,
                    "item_count": session_count,
                    "estimated_pages": (session_count + page_size - 1) // page_size,
                }
            )

            start_item += session_count

        logger.debug(
            f"Calculated GovInfo distributions: {[(d['session_id'], d['item_count']) for d in distributions]}"
        )

        return distributions

    # =============================================================================
    # INCREMENTAL FETCHING METHODS
    # =============================================================================

    async def fetch_incremental_collection_data(
        self,
        fallback_days: int = 30,
        limit: int | None = None,
        **kwargs,
    ) -> AsyncIterator[tuple[list[dict[str, Any]], str | None]]:
        """
        Get collection data incrementally using the last processed date from database.

        This method automatically:
        1. Retrieves the last processed date from the database
        2. Fetches data updated since that date
        3. Tracks the newest date for the next run

        Args:
            fallback_days: If no last processed date, fetch data from this many days ago
            limit: Maximum number of results per page
            **kwargs: Additional parameters

        Yields:
            Tuples of (data_batch, latest_date_in_batch)
        """
        # Get the last processed date
        last_processed = None
        try:
            if hasattr(self.client, "access_last_processed_date"):
                last_processed = await self.client.access_last_processed_date(
                    self.data_type_name
                )
        except Exception as e:
            logger.debug(f"Could not get last processed date: {e}")

        if last_processed:
            from_date = last_processed
            logger.info(
                f"Fetching {self.data_type_name} data updated since: {from_date}"
            )
        else:
            if fallback_days is None or fallback_days <= 0:
                # No fallback limit – fetch a reasonable default range
                fallback_date = datetime.now(UTC) - timedelta(
                    days=365
                )  # 1 year fallback
                from_date = fallback_date.strftime("%Y-%m-%d")
                logger.info(
                    f"No last processed date found for {self.data_type_name}, fetching from 1 year ago: {from_date}"
                )
            else:
                # Fallback: get data from the last N days
                fallback_date = datetime.now(UTC) - timedelta(days=fallback_days)
                from_date = fallback_date.strftime("%Y-%m-%d")
                logger.info(
                    f"No last processed date found for {self.data_type_name}, fetching from {fallback_days} days ago: {from_date}"
                )

        # Use current date as end date
        to_date = datetime.now(UTC).strftime("%Y-%m-%d")

        latest_date = None

        async for batch in self.fetch_collection_data(
            from_date=from_date,
            to_date=to_date,
            limit=limit,
            **kwargs,
        ):
            if batch:
                # Extract the latest date from this batch
                batch_latest = self._extract_latest_date(batch)
                if batch_latest and (not latest_date or batch_latest > latest_date):
                    latest_date = batch_latest

                yield batch, latest_date
            else:
                yield batch, latest_date

    def _extract_latest_date(self, data_batch: list[dict[str, Any]]) -> str | None:
        """
        Extract the latest date from a batch of GovInfo data.

        Args:
            data_batch: List of data items

        Returns:
            Latest date string or None
        """
        latest_date = None

        for item in data_batch:
            # GovInfo typically uses 'lastModified' or 'dateIssued'
            date_value = item.get("lastModified") or item.get("dateIssued")

            if date_value and (not latest_date or date_value > latest_date):
                latest_date = date_value

        return latest_date

    async def process_items_incremental(
        self,
        fallback_days: int = 30,
        batch_size: int = 50,
        enable_parallelization: bool = False,
        max_concurrent: int = 10,
        redistribute_idle_sessions: bool = True,
        **kwargs,
    ) -> dict[str, Any]:
        """
        Process items incrementally using automatic date range detection.

        This method automatically determines the date range based on the last
        processed date stored in the database, making it ideal for scheduled runs.

        Args:
            fallback_days: If no last processed date, fetch data from this many days ago
            batch_size: Items per storage batch
            enable_parallelization: Enable parallel processing
            max_concurrent: Maximum concurrent operations
            redistribute_idle_sessions: Use idle sessions for granule processing
            **kwargs: Additional parameters

        Returns:
            Processing results with statistics
        """
        # Get the last processed date to determine incremental range
        last_processed = None
        try:
            if hasattr(self.client, "access_last_processed_date"):
                last_processed = await self.client.access_last_processed_date(
                    self.data_type_name
                )
        except Exception as e:
            logger.debug(f"Could not get last processed date: {e}")

        if last_processed:
            from_date = last_processed
            logger.info(f"Incremental processing from last processed date: {from_date}")
        else:
            if fallback_days and fallback_days > 0:
                fallback_date = datetime.now(UTC) - timedelta(days=fallback_days)
                from_date = fallback_date.strftime("%Y-%m-%d")
                logger.info(
                    f"No last processed date, using {fallback_days} day fallback: {from_date}"
                )
            else:
                from_date = None
                logger.info(
                    "No last processed date and no fallback, processing all available data"
                )

        # Use current date as end date
        to_date = datetime.now(UTC).strftime("%Y-%m-%d")

        # Call the main process_items method with determined date range
        return await self.process_items(
            from_date=from_date,
            to_date=to_date,
            batch_size=batch_size,
            enable_parallelization=enable_parallelization,
            max_concurrent=max_concurrent,
            redistribute_idle_sessions=redistribute_idle_sessions,
            **kwargs,
        )

    # =============================================================================
    # RESUME AND CHECKPOINT METHODS
    # =============================================================================

    def get_incomplete_packages_for_resume(
        self, available_package_ids: list[str]
    ) -> dict[str, list[str]]:
        """
        Identify packages that need specific phases of processing for resume operations.

        Args:
            available_package_ids: List of package IDs that are available for processing

        Returns:
            Dictionary mapping phase names to lists of package IDs that need that phase:
            {
                "collection_data": ["pkg1", "pkg2"],  # Packages missing collection data
                "package_data": ["pkg3", "pkg4"],    # Packages missing package data
                "granules_data": ["pkg5", "pkg6"],   # Packages missing granules data (if has_granules)
                "granule_data": ["pkg7", "pkg8"]     # Packages missing granule data (if has_granules)
            }
        """
        if not self.progress_tracker:
            # If no progress tracker, assume all packages need all phases
            incomplete_phases = {
                "collection_data": available_package_ids.copy(),
                "package_data": available_package_ids.copy(),
            }
            if self.has_granules:
                incomplete_phases["granules_data"] = available_package_ids.copy()
                incomplete_phases["granule_data"] = available_package_ids.copy()
            return incomplete_phases

        incomplete_phases = {
            "collection_data": [],
            "package_data": [],
        }
        if self.has_granules:
            incomplete_phases["granules_data"] = []
            incomplete_phases["granule_data"] = []

        for package_id in available_package_ids:
            # Check each phase for this package using GovInfo-specific phase structure
            if not self.progress_tracker.should_skip_item(
                package_id, ProcessingPhase.MAIN_ITEMS, field_name="collection_data"
            ):
                incomplete_phases["collection_data"].append(package_id)

            if not self.progress_tracker.should_skip_item(
                package_id, ProcessingPhase.MAIN_ITEMS, field_name="package_data"
            ):
                incomplete_phases["package_data"].append(package_id)

            if self.has_granules:
                if not self.progress_tracker.should_skip_item(
                    package_id,
                    ProcessingPhase.RELATED_ENTITIES,
                    field_name="granules_data",
                ):
                    incomplete_phases["granules_data"].append(package_id)

                if not self.progress_tracker.should_skip_item(
                    package_id,
                    ProcessingPhase.RELATED_ENTITIES,
                    field_name="granule_data",
                ):
                    incomplete_phases["granule_data"].append(package_id)

        return incomplete_phases

    def log_resume_status(self, incomplete_phases: dict[str, list[str]]) -> None:
        """
        Log the resume status showing what phases need processing for GovInfo.

        Args:
            incomplete_phases: Dictionary from get_incomplete_packages_for_resume()
        """
        total_collection = len(incomplete_phases.get("collection_data", []))
        total_package = len(incomplete_phases.get("package_data", []))
        total_granules = len(incomplete_phases.get("granules_data", []))
        total_granule_data = len(incomplete_phases.get("granule_data", []))

        logger.info(f"Resume status for GovInfo {self.data_type_name}:")
        logger.info(f"  Packages needing collection data: {total_collection}")
        logger.info(f"  Packages needing package data: {total_package}")

        if self.has_granules:
            logger.info(f"  Packages needing granules data: {total_granules}")
            logger.info(f"  Packages needing granule data: {total_granule_data}")

        if all(len(phase_list) == 0 for phase_list in incomplete_phases.values()):
            logger.info("  All packages are fully processed!")
        else:
            max_incomplete = max(
                len(phase_list) for phase_list in incomplete_phases.values()
            )
            logger.info(
                f"  Resume will process {max_incomplete} packages with missing phases"
            )
