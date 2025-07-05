"""
Sophisticated base fetcher with advanced processing patterns.

This module provides a comprehensive base fetcher that implements:
- Advanced parallel processing with redistribution strategies
- Sophisticated pagination metadata analysis
- Incremental processing based on last processed dates
- Phase-specific checkpointing for resume capabilities
- Comprehensive error handling and retry logic
- Adaptive concurrent processing based on available resources
"""

import logging
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Any

from ...libs.checkpoint import (
    HierarchicalProgressTracker,
    ProcessingPhase,
    ProcessingStage,
)
from ...libs.run_tracking import RunMetadata, RunType

logger = logging.getLogger(__name__)


class BaseFetcher(ABC):
    """
    Sophisticated base fetcher with advanced processing patterns.

    This base class implements all the sophisticated processing logic that's shared
    between different data sources, while defining abstract methods for source-specific
    phase implementations.

    Advanced Features:
    - Multi-phase processing with configurable phase patterns
    - Sophisticated parallel processing with redistribution strategies
    - Comprehensive pagination metadata analysis
    - Incremental processing based on last processed dates
    - Phase-specific checkpointing for resume capabilities
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
        self.client = client
        self.db_pool = db_pool
        self.data_type_name = data_type_name
        self.checkpoint_manager = checkpoint_manager
        self.run_manager = run_manager
        self.progress_tracker = None
        self.current_run_id = None

    # =============================================================================
    # ABSTRACT METHODS - SUBCLASSES MUST IMPLEMENT
    # =============================================================================

    @abstractmethod
    def get_source_system_name(self) -> str:
        """Get the source system name for progress tracking (e.g., 'congressional', 'govinfo')."""

    @abstractmethod
    def get_default_schema(self) -> str:
        """Get the default schema for raw data storage."""

    @abstractmethod
    def get_run_type(self) -> RunType:
        """Get the run type for tracking."""

    @abstractmethod
    async def fetch_phase_1_data(
        self,
        from_date: str | None = None,
        to_date: str | None = None,
        limit: int | None = None,
        **kwargs,
    ) -> AsyncIterator[list[dict[str, Any]]]:
        """
        Phase 1: Fetch bulk/list data from source.

        Must yield batches of items containing URLs/identifiers for Phase 2.
        """

    @abstractmethod
    async def fetch_phase_1_data_with_client(
        self,
        client,
        from_date: str | None = None,
        to_date: str | None = None,
        limit: int | None = None,
        **kwargs,
    ) -> AsyncIterator[list[dict[str, Any]]]:
        """
        Phase 1: Fetch bulk/list data using a specific client (for parallel processing).
        """

    @abstractmethod
    async def fetch_phase_2_data(self, item_url: str) -> dict[str, Any] | None:
        """
        Phase 2: Fetch complete item data from individual URL.
        """

    @abstractmethod
    async def fetch_phase_2_data_with_client(
        self, item_url: str, client
    ) -> dict[str, Any] | None:
        """
        Phase 2: Fetch complete item data using a specific client.
        """

    @abstractmethod
    def extract_item_id(self, item_data: dict[str, Any]) -> str:
        """Extract standardized ID from item data."""

    @abstractmethod
    def _extract_preliminary_item_id(self, list_item: dict[str, Any]) -> str:
        """Extract preliminary ID from list item data."""

    @abstractmethod
    async def store_phase_1_data(
        self, data: dict[str, Any], batch_id: str | None = None
    ) -> None:
        """Store Phase 1 (list/collection) data."""

    @abstractmethod
    async def store_phase_2_data(
        self, data: dict[str, Any], batch_id: str | None = None
    ) -> None:
        """Store Phase 2 (full/package) data."""

    # Optional methods for systems with additional phases
    async def fetch_phase_3_data(
        self, item_data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """
        Phase 3: Fetch related/granules data (optional).

        Override this method if your data source has a third phase.
        """
        return []

    async def fetch_phase_3_data_with_client(
        self, url: str, list_keys: list[str], client
    ) -> list[dict[str, Any]]:
        """
        Phase 3: Fetch related/granules data using a specific client (optional).
        """
        return []

    async def fetch_phase_4_data(self, item_url: str) -> dict[str, Any] | None:
        """
        Phase 4: Fetch individual granule data (optional for 4-phase systems like GovInfo).

        Override this method if your data source has a fourth phase.
        """
        return None

    async def fetch_phase_4_data_with_client(
        self, item_url: str, client
    ) -> dict[str, Any] | None:
        """
        Phase 4: Fetch individual granule data using a specific client (optional).
        """
        return None

    async def store_phase_3_data(  # noqa: B027
        self, data: list[dict[str, Any]], parent_id: str, batch_id: str | None = None
    ) -> None:
        """Store Phase 3 (related/granules) data (optional)."""

    async def store_phase_4_data(  # noqa: B027
        self, data: list[dict[str, Any]], parent_id: str, batch_id: str | None = None
    ) -> None:
        """Store Phase 4 (granule data) data (optional)."""

    @abstractmethod
    async def store_raw_data(
        self, schema: str, table: str, items: list[dict], **kwargs
    ):
        """Store raw data - must specify schema explicitly, no defaults."""

    # =============================================================================
    # SETUP AND CONFIGURATION
    # =============================================================================

    def setup_progress_tracker(self) -> HierarchicalProgressTracker | None:
        """Setup progress tracking using source-specific system name."""
        if not self.checkpoint_manager or not self.data_type_name:
            logger.warning("Cannot setup progress tracker - missing dependencies")
            return None

        self.progress_tracker = HierarchicalProgressTracker.create_for_stage(
            self.checkpoint_manager,
            self.get_source_system_name(),
            self.data_type_name,
            ProcessingStage.SCRAPING,
        )
        return self.progress_tracker

    def set_processing_resource(self, processing_resource) -> None:
        """
        Set the processing resource for accessing parallel API clients and other resources.
        """
        self.processing_resource = processing_resource
        if hasattr(processing_resource, "get_api_clients_for_parallel_sessions"):
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
                    run_type=self.get_run_type(),
                    system_name=f"{self.data_type_name}_fetcher",
                    description=f"Fetch {self.data_type_name} data from {self.get_source_system_name()} API",
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
    # MAIN PROCESSING METHOD
    # =============================================================================

    async def process_items(
        self,
        from_date: str | None = None,
        to_date: str | None = None,
        limit: int | None = 250,  # Default to 250 for better API efficiency
        max_items: int | None = None,
        batch_size: int = 50,
        enable_parallelization: bool = False,
        max_concurrent: int = 10,
        redistribute_idle_sessions: bool = True,
        **kwargs,
    ) -> dict[str, Any]:
        """
        Main processing method with sophisticated parallel processing.

        Args:
            from_date: Start date filter (YYYY-MM-DD)
            to_date: End date filter (YYYY-MM-DD)
            limit: Items per page
            max_items: Maximum total items to process
            batch_size: Items per storage batch
            enable_parallelization: Enable parallel processing
            max_concurrent: Maximum concurrent operations
            redistribute_idle_sessions: Use idle sessions for additional phases
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

    # =============================================================================
    # PAGINATION METADATA ANALYSIS
    # =============================================================================

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
            - pages_to_process: Number of pages that actually need processing
            - metadata_dict: Contains total_count, last_processed_date, etc.
        """
        logger.info("Analyzing pagination metadata for optimal distribution")

        try:
            # Get first page to analyze metadata
            first_page = None
            async for page in self.fetch_phase_1_data(
                from_date=from_date, to_date=to_date, limit=limit, **kwargs
            ):
                first_page = page
                break

            if not first_page:
                logger.warning("No data found for pagination analysis")
                return 0, {"total_count": 0, "needs_processing": 0}

            # Extract pagination info from first page
            pagination_info = self._extract_pagination_info(first_page)
            total_count = pagination_info.get("total_count", 0)
            count_per_page = pagination_info.get("count_per_page", limit or 20)

            # Calculate total pages
            total_pages = (total_count + count_per_page - 1) // count_per_page

            # Check for incremental processing
            last_processed_date = await self._get_last_processed_date()
            if last_processed_date and from_date:
                # Estimate pages needing processing based on date range
                estimated_pages = self._estimate_pages_for_date_range(
                    from_date, to_date, last_processed_date, total_pages
                )
                pages_to_process = min(estimated_pages, total_pages)
            else:
                pages_to_process = total_pages

            metadata = {
                "total_count": total_count,
                "total_pages": total_pages,
                "pages_to_process": pages_to_process,
                "count_per_page": count_per_page,
                "last_processed_date": last_processed_date,
                "incremental_processing": bool(last_processed_date),
            }

            logger.info(f"Pagination analysis: {metadata}")
            return pages_to_process, metadata

        except Exception as e:
            logger.error(f"Pagination metadata analysis failed: {e}")
            return None, {"error": str(e)}

    def _extract_pagination_info(
        self, page_data: list[dict[str, Any]]
    ) -> dict[str, Any]:
        """
        Extract pagination info from first page.

        Override this method to match your API's pagination structure.
        """
        # Default implementation - subclasses should override for specific APIs
        return {
            "total_count": len(page_data),
            "count_per_page": len(page_data),
        }

    async def _get_last_processed_date(self) -> str | None:
        """Get the last processed date from the database for incremental processing."""
        if not self.client or not hasattr(self.client, "access_last_processed_date"):
            return None

        try:
            # Use the client's method to get the last processed date from the correct table
            last_processed_date = await self.client.access_last_processed_date(
                self.data_type_name
            )
            if last_processed_date:
                logger.info(
                    f"Found last processed date for {self.data_type_name}: {last_processed_date}"
                )
            else:
                logger.info(f"No last processed date found for {self.data_type_name}")
            return last_processed_date
        except Exception as e:
            logger.debug(f"Could not get last processed date: {e}")
            return None

    def _estimate_pages_for_date_range(
        self,
        from_date: str,
        to_date: str | None,
        last_processed_date: str,
        total_pages: int,
    ) -> int:
        """
        Estimate pages needed for date range (rough approximation).
        """
        # Simple linear estimation - subclasses can override for more accuracy
        if to_date:
            # Calculate what fraction of time range needs processing
            from datetime import datetime

            start = datetime.strptime(from_date, "%Y-%m-%d")
            end = datetime.strptime(to_date, "%Y-%m-%d")
            last_proc = datetime.strptime(last_processed_date, "%Y-%m-%d")

            total_range = (end - start).days
            remaining_range = (end - last_proc).days

            if total_range > 0:
                fraction = remaining_range / total_range
                return max(1, int(total_pages * fraction))

        return max(1, total_pages // 2)  # Conservative estimate

    # =============================================================================
    # SEQUENTIAL PROCESSING
    # =============================================================================

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
        }

        batch_items = []
        processed_count = 0

        try:
            # Progress tracking
            if self.progress_tracker:
                await self.progress_tracker.start_phase(
                    ProcessingPhase.PHASE_1, "Sequential Phase 1 processing"
                )

            # Process Phase 1 data
            async for phase1_batch in self.fetch_phase_1_data(
                from_date=from_date, to_date=to_date, limit=limit, **kwargs
            ):
                for item in phase1_batch:
                    if max_items and processed_count >= max_items:
                        break

                    try:
                        # Store Phase 1 data
                        await self.store_phase_1_data(item)

                        # Process Phase 2 data
                        item_url = item.get("url")
                        if item_url:
                            phase2_data = await self.fetch_phase_2_data(item_url)
                            if phase2_data:
                                await self.store_phase_2_data(phase2_data)

                                # Process Phase 3 data if available
                                phase3_data = await self.fetch_phase_3_data(phase2_data)
                                if phase3_data:
                                    item_id = self.extract_item_id(phase2_data)
                                    await self.store_phase_3_data(phase3_data, item_id)

                                    # Process Phase 4 data if available
                                    for phase3_item in phase3_data:
                                        phase4_url = phase3_item.get("url")
                                        if phase4_url:
                                            phase4_data = await self.fetch_phase_4_data(
                                                phase4_url
                                            )
                                            if phase4_data:
                                                await self.store_phase_4_data(
                                                    [phase4_data], item_id
                                                )

                        stats["successful"] += 1
                        processed_count += 1

                        # Update progress
                        if self.progress_tracker:
                            self.progress_tracker.update_progress(
                                processed_items=processed_count
                            )

                    except Exception as e:
                        logger.error(
                            f"Error processing item {item.get('id', 'unknown')}: {e}"
                        )
                        stats["errors"] += 1

                    if max_items and processed_count >= max_items:
                        break

                if max_items and processed_count >= max_items:
                    break

            stats["total_processed"] = processed_count
            stats["end_time"] = datetime.now(UTC)
            stats["duration"] = stats["end_time"] - stats["start_time"]

            logger.info(f"Sequential processing completed: {stats}")
            return stats

        except Exception as e:
            logger.error(f"Sequential processing failed: {e}")
            stats["error"] = str(e)
            return stats

        finally:
            if self.progress_tracker:
                await self.progress_tracker.complete_phase(ProcessingPhase.PHASE_1)

    # =============================================================================
    # PARALLEL PROCESSING WITH REDISTRIBUTION
    # =============================================================================

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
        """Process with sophisticated parallel processing and redistribution strategies."""
        logger.info("Starting parallel processing with redistribution strategies")

        # Check if we should use the new optimized processor.
        # This is the corrected logic.
        if hasattr(self, "processing_resource") and getattr(
            self.processing_resource, "use_dynamic_pool", False
        ):
            logger.info(
                "--> Dynamic Pool mode DETECTED. Using OptimizedParallelProcessor."
            )
            # Late import to avoid circular dependencies and issues when not used
            from ..new_architecture.optimized_parallel_processor import OptimizedParallelProcessor

            processor = OptimizedParallelProcessor(
                api_keys=self.processing_resource.api_keys,
                client_class=self.client.__class__,
                db_pool=self.db_pool,
                num_workers=max_concurrent,
            )

            return await processor.process_data_type(
                fetcher=self,
                data_type=self.data_type_name,
                from_date=from_date,
                to_date=to_date,
                limit=limit,
                **kwargs,
            )

        # Fallback to old parallel logic
        logger.info("--> Standard Parallel mode DETECTED.")

        # Get available API clients
        api_clients = []
        if hasattr(self, "processing_resource") and hasattr(
            self.processing_resource, "get_api_clients_for_parallel_sessions"
        ):
            api_clients = (
                await self.processing_resource.get_api_clients_for_parallel_sessions(
                    self.data_type_name
                )
            )

        if not api_clients:
            logger.warning("No parallel clients available, falling back to sequential")
            return await self._process_sequentially(
                from_date, to_date, limit, max_items, batch_size, **kwargs
            )

        return await self._process_with_concurrent_processing(
            api_clients,
            from_date,
            to_date,
            batch_size,
            max_concurrent,
            redistribute_idle_sessions,
            limit,
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
        """Process with sophisticated concurrent processing."""
        stats = {
            "total_processed": 0,
            "successful": 0,
            "errors": 0,
            "batches_stored": 0,
            "start_time": datetime.now(UTC),
            "redistribution_used": False,
            "latest_date": None,
        }

        try:
            # Get pagination metadata for optimal distribution
            pages_to_process, metadata = await self._get_pagination_metadata(
                from_date, to_date, limit, **kwargs
            )

            if not pages_to_process:
                logger.info("No pages to process")
                return stats

            # Calculate optimal distribution strategy
            distribution_strategy = self._calculate_distribution_strategy(
                api_clients, pages_to_process, redistribute_idle_sessions
            )

            logger.info(f"Using distribution strategy: {distribution_strategy}")

            # Execute parallel processing
            if redistribute_idle_sessions and distribution_strategy.get(
                "use_redistribution"
            ):
                return await self._execute_redistribution_strategy(
                    api_clients,
                    distribution_strategy,
                    from_date,
                    to_date,
                    limit,
                    batch_size,
                    **kwargs,
                )
            else:
                return await self._execute_standard_parallel_processing(
                    api_clients,
                    from_date,
                    to_date,
                    limit,
                    batch_size,
                    max_concurrent,
                    **kwargs,
                )

        except Exception as e:
            logger.error(f"Concurrent processing failed: {e}")
            stats["error"] = str(e)
            return stats

    def _calculate_distribution_strategy(
        self,
        api_clients: list,
        pages_to_process: int,
        redistribute_idle_sessions: bool,
    ) -> dict[str, Any]:
        """Calculate optimal distribution strategy for parallel processing."""
        num_clients = len(api_clients)

        # Basic strategy calculation
        if pages_to_process <= num_clients:
            # More clients than pages - use redistribution if enabled
            strategy = {
                "type": "redistribution" if redistribute_idle_sessions else "standard",
                "use_redistribution": redistribute_idle_sessions
                and pages_to_process < num_clients,
                "clients_for_phase1": min(pages_to_process, num_clients),
                "clients_for_redistribution": max(0, num_clients - pages_to_process),
                "pages_per_client": 1,
            }
        else:
            # More pages than clients - standard distribution
            pages_per_client = pages_to_process // num_clients
            strategy = {
                "type": "standard",
                "use_redistribution": False,
                "clients_for_phase1": num_clients,
                "clients_for_redistribution": 0,
                "pages_per_client": pages_per_client,
            }

        return strategy

    async def _execute_redistribution_strategy(
        self,
        api_clients: list,
        distribution_strategy: dict,
        from_date: str | None,
        to_date: str | None,
        limit: int | None,
        batch_size: int,
        **kwargs,
    ) -> dict[str, Any]:
        """Execute redistribution strategy for optimal resource utilization."""
        logger.info("Executing redistribution strategy")

        # Implementation depends on the specific phase patterns
        # This is a template that subclasses can override
        return {"redistribution_strategy": "not_implemented"}

    async def _execute_standard_parallel_processing(
        self,
        api_clients: list,
        from_date: str | None,
        to_date: str | None,
        limit: int | None,
        batch_size: int,
        max_concurrent: int,
        **kwargs,
    ) -> dict[str, Any]:
        """Execute standard parallel processing across all clients."""
        logger.info("Executing standard parallel processing")

        # Implementation depends on the specific phase patterns
        # This is a template that subclasses can override
        return {"standard_processing": "not_implemented"}

    # =============================================================================
    # INCREMENTAL PROCESSING
    # =============================================================================

    async def fetch_incremental_data(
        self,
        fallback_days: int = 30,
        limit: int | None = None,
        **kwargs,
    ) -> AsyncIterator[tuple[list[dict[str, Any]], str | None]]:
        """
        Get data incrementally using the last processed date from database.

        Args:
            fallback_days: Days to look back if no last processed date found
            limit: Items per page
            **kwargs: Additional parameters

        Yields:
            Tuple of (data_batch, next_date) for incremental processing
        """
        logger.info("Starting incremental data fetching")

        # Get last processed date
        last_processed_date = await self._get_last_processed_date()

        if not last_processed_date:
            # No previous data, use fallback
            from_date = (datetime.now(UTC) - timedelta(days=fallback_days)).strftime(
                "%Y-%m-%d"
            )
            logger.info(f"No last processed date found, using fallback: {from_date}")
        else:
            # Start from last processed date
            from_date = last_processed_date
            logger.info(f"Using last processed date: {from_date}")

        # Fetch data incrementally
        async for batch in self.fetch_phase_1_data(
            from_date=from_date, limit=limit, **kwargs
        ):
            # Extract latest date from batch for next iteration
            latest_date = self._extract_latest_date_from_batch(batch)
            yield batch, latest_date

    def _extract_latest_date_from_batch(
        self, batch: list[dict[str, Any]]
    ) -> str | None:
        """
        Extract the latest date from a batch of items.

        Override this method to match your data structure.
        """
        # Default implementation - subclasses should override
        dates = []
        for item in batch:
            date_str = (
                item.get("date") or item.get("date_updated") or item.get("lastModified")
            )
            if date_str:
                dates.append(date_str)

        if dates:
            return max(dates)
        return None
