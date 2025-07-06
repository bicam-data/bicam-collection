"""
Streamlined base fetcher with only optimized parallel processing.

This version removes all inferior processing logic and tightly integrates
the OptimizedParallelProcessor as the only parallel processing option.
"""

import logging
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Any

from ...libs.hierarchical_checkpoint_system import (
    FetchingCheckpoint,
    HierarchicalCheckpointManager,
)
from ...libs.run_tracking import RunMetadata, RunType
from ...processing.optimized_parallel_processor import OptimizedParallelProcessor

logger = logging.getLogger(__name__)


class BaseFetcher(ABC):
    """
    Streamlined base fetcher with optimized parallel processing.

    This base class provides:
    - Multi-phase processing with configurable phase patterns
    - Optimized parallel processing with dynamic key pool
    - Comprehensive pagination metadata analysis
    - Incremental processing based on last processed dates
    - Phase-specific checkpointing for resume capabilities

    All parallel processing uses the OptimizedParallelProcessor for maximum efficiency.
    """

    def __init__(
        self,
        client,
        db_pool=None,
        data_type_name=None,
        checkpoint_manager: HierarchicalCheckpointManager | None = None,
        run_manager=None,
        api_keys: list[str] | None = None,
    ):
        self.client = client
        self.db_pool = db_pool
        self.data_type_name = data_type_name
        self.checkpoint_manager = checkpoint_manager
        self.run_manager = run_manager
        self.current_run_id = None
        self.api_keys = api_keys or []
        self.fetching_checkpoint: FetchingCheckpoint | None = None

        if hasattr(client, "api_keys"):
            self.api_keys = client.api_keys

        if self.checkpoint_manager:
            self.fetching_checkpoint = FetchingCheckpoint(
                self.checkpoint_manager, self.data_type_name
            )

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
        """Phase 1: Fetch bulk/list data from source."""

    @abstractmethod
    async def fetch_phase_1_data_with_client(
        self,
        client,
        from_date: str | None = None,
        to_date: str | None = None,
        limit: int | None = None,
        **kwargs,
    ) -> AsyncIterator[list[dict[str, Any]]]:
        """Phase 1: Fetch bulk/list data using a specific client."""

    @abstractmethod
    async def fetch_phase_2_data(self, item_url: str) -> dict[str, Any] | None:
        """Phase 2: Fetch complete item data from individual URL."""

    @abstractmethod
    async def fetch_phase_2_data_with_client(
        self, item_url: str, client
    ) -> dict[str, Any] | None:
        """Phase 2: Fetch complete item data using a specific client."""

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
        """Phase 3: Fetch related/granules data (optional)."""
        return []

    async def fetch_phase_3_data_with_client(
        self, item_data: dict[str, Any], client
    ) -> list[dict[str, Any]]:
        """Phase 3: Fetch related/granules data using a specific client (optional)."""
        return []

    async def store_phase_3_data(
        self, data: list[dict[str, Any]], parent_id: str, batch_id: str | None = None
    ) -> None:
        """Store Phase 3 (related/granules) data (optional)."""

    @abstractmethod
    async def store_raw_data(
        self, schema: str, table: str, items: list[dict], **kwargs
    ):
        """Store raw data - must specify schema explicitly."""

    # =============================================================================
    # SETUP AND CONFIGURATION
    # =============================================================================

    def set_processing_resource(self, processing_resource) -> None:
        """Attach a ProcessingResource so the fetcher can use shared services.

        The processing resource manages the database pool, checkpoint manager
        and run manager that are created during pipeline setup.  Fetchers often
        run in a synchronous Dagster context, so we rely on the *sync* accessors
        provided by the resource instead of the async ones.  We only fetch each
        dependency when it has not been supplied explicitly during
        instantiation so unit tests and ad-hoc scripts can still inject their
        own stubs/mocks.
        """

        # Keep a reference for downstream consumers (e.g. parallel session
        # helpers that live outside this class).
        self.processing_resource = processing_resource

        if processing_resource is None:
            return

        # --- Database pool ----------------------------------------------------
        if self.db_pool is None and hasattr(processing_resource, "get_db_pool_sync"):
            try:
                self.db_pool = processing_resource.get_db_pool_sync()
            except Exception as exc:  # pragma: no cover – defensive only
                logger.debug(
                    "Unable to obtain DB pool from processing_resource: %s", exc
                )

        # --- Checkpoint manager ---------------------------------------------
        if self.checkpoint_manager is None and hasattr(
            processing_resource, "get_checkpoint_manager"
        ):
            try:
                self.checkpoint_manager = processing_resource.get_checkpoint_manager()
                if self.checkpoint_manager and self.data_type_name:
                    # (Re-)create the FetchingCheckpoint helper now that we can.
                    self.fetching_checkpoint = FetchingCheckpoint(
                        self.checkpoint_manager, self.data_type_name
                    )
            except Exception as exc:  # pragma: no cover
                logger.debug(
                    "Unable to obtain checkpoint manager from processing_resource: %s",
                    exc,
                )

        # --- Run manager ------------------------------------------------------
        if self.run_manager is None and hasattr(
            processing_resource, "get_run_manager_sync"
        ):
            try:
                self.run_manager = processing_resource.get_run_manager_sync()
            except Exception as exc:  # pragma: no cover
                logger.debug(
                    "Unable to obtain run manager from processing_resource: %s", exc
                )

    def set_api_keys(self, api_keys: list[str]) -> None:
        """Set API keys for parallel processing."""
        self.api_keys = api_keys
        logger.info(f"Set {len(api_keys)} API keys for parallel processing")

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
        limit: int | None = 250,
        max_items: int | None = None,
        batch_size: int = 50,
        enable_parallelization: bool = True,
        num_workers: int | None = None,
        chunk_size: int = 5000,
        **kwargs,
    ) -> dict[str, Any]:
        """
        Main processing method with optimized parallel processing.

        Args:
            from_date: Start date filter (YYYY-MM-DD)
            to_date: End date filter (YYYY-MM-DD)
            limit: Items per page
            max_items: Maximum total items to process
            batch_size: Items per storage batch
            enable_parallelization: Enable parallel processing (default: True)
            num_workers: Number of workers (default: 1.5x API keys)
            chunk_size: Size of work chunks for distribution
            **kwargs: Additional parameters

        Returns:
            Processing results with statistics
        """
        logger.info(f"Starting {self.data_type_name} processing")
        logger.info(f"Date range: {from_date} to {to_date}")
        logger.info(f"Parallelization: {enable_parallelization}")

        # Setup run tracking
        await self.setup_run_tracking()

        try:
            if enable_parallelization and self.api_keys and len(self.api_keys) > 1:
                return await self._process_with_optimized_parallelization(
                    from_date,
                    to_date,
                    limit,
                    max_items,
                    batch_size,
                    num_workers,
                    chunk_size,
                    **kwargs,
                )
            else:
                if enable_parallelization and not self.api_keys:
                    logger.warning(
                        "Parallel processing requested but no API keys available"
                    )
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
    # OPTIMIZED PARALLEL PROCESSING
    # =============================================================================

    async def _process_with_optimized_parallelization(
        self,
        from_date: str | None,
        to_date: str | None,
        limit: int | None,
        max_items: int | None,
        batch_size: int,
        num_workers: int | None,
        chunk_size: int,
        **kwargs,
    ) -> dict[str, Any]:
        """Process with optimized parallel processing using dynamic key pool."""
        logger.info("=== OPTIMIZED PARALLEL PROCESSING ACTIVATED ===")
        logger.info(f"API Keys: {len(self.api_keys)}")
        logger.info(f"Workers: {num_workers or int(len(self.api_keys) * 1.5)}")
        logger.info(f"Chunk Size: {chunk_size}")

        # Create optimized processor
        processor = OptimizedParallelProcessor(
            api_keys=self.api_keys,
            client_class=self.client.__class__,
            db_pool=self.db_pool,
            num_workers=num_workers,
            chunk_size=chunk_size,
        )

        # Process using optimized strategy
        return await processor.process_data_type(
            fetcher=self,
            data_type=self.data_type_name,
            from_date=from_date,
            to_date=to_date,
            limit=limit,
            **kwargs,
        )

    # =============================================================================
    # SEQUENTIAL PROCESSING (FALLBACK)
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
        """Process items sequentially (single-threaded fallback)."""
        logger.info(
            "Processing sequentially (single API key or parallelization disabled)"
        )

        # Check if we should use optimized storage
        use_optimized_storage = False
        optimized_storage = None

        if hasattr(self, "processing_resource") and self.processing_resource:
            if getattr(self.processing_resource, "use_optimized_storage", False):
                try:
                    storage_manager = (
                        await self.processing_resource.get_storage_manager()
                    )
                    if storage_manager:
                        from ...processing.optimized_storage_manager import (
                            OptimizedFetcherStorage,
                        )

                        # Get the id_field from the fetcher's config
                        id_field = getattr(self, "id_field", "parent_id")
                        optimized_storage = OptimizedFetcherStorage(
                            storage_manager, id_field
                        )
                        use_optimized_storage = True
                        logger.info("Using optimized storage in sequential mode")
                except Exception as e:
                    logger.warning(f"Failed to get optimized storage: {e}")

        stats = {
            "total_processed": 0,
            "successful": 0,
            "errors": 0,
            "start_time": datetime.now(UTC),
            "processing_mode": "sequential",
            "storage_mode": "optimized" if use_optimized_storage else "direct",
        }

        processed_count = 0

        try:
            # Process Phase 1 data
            async for phase1_batch in self.fetch_phase_1_data(
                from_date=from_date, to_date=to_date, limit=limit, **kwargs
            ):
                for item in phase1_batch:
                    if max_items and processed_count >= max_items:
                        break

                    preliminary_item_id = self._extract_preliminary_item_id(item)
                    if (
                        self.fetching_checkpoint
                        and self.fetching_checkpoint.should_skip_list_item(
                            preliminary_item_id
                        )
                    ):
                        logger.debug(
                            f"Skipping already processed list item: {preliminary_item_id}"
                        )
                        continue

                    try:
                        # Store Phase 1 data
                        if use_optimized_storage:
                            await optimized_storage.store_phase_1_data(
                                self.get_default_schema(),
                                f"{self.data_type_name}_list_raw",
                                item,
                            )
                        else:
                            await self.store_phase_1_data(item)

                        # Process Phase 2 data
                        item_url = item.get("url")
                        if item_url:
                            phase2_data = await self.fetch_phase_2_data(item_url)
                            if phase2_data:
                                item_id = self.extract_item_id(phase2_data)
                                if (
                                    self.fetching_checkpoint
                                    and self.fetching_checkpoint.should_skip_full_data(
                                        item_id
                                    )
                                ):
                                    logger.debug(
                                        f"Skipping already processed full data for item: {item_id}"
                                    )
                                else:
                                    if use_optimized_storage:
                                        await optimized_storage.store_phase_2_data(
                                            self.get_default_schema(),
                                            f"{self.data_type_name}_raw",
                                            phase2_data,
                                        )
                                    else:
                                        await self.store_phase_2_data(phase2_data)

                                    # Process Phase 3 data if available
                                    phase3_data = await self.fetch_phase_3_data(
                                        phase2_data
                                    )
                                    if phase3_data:
                                        if use_optimized_storage:
                                            await optimized_storage.store_phase_3_data(
                                                self.get_default_schema(),
                                                self.data_type_name,
                                                phase3_data,
                                                item_id,
                                            )
                                        else:
                                            await self.store_phase_3_data(
                                                phase3_data, item_id
                                            )

                        stats["successful"] += 1
                        processed_count += 1

                    except Exception as e:
                        logger.error(f"Error processing item: {e}")
                        stats["errors"] += 1

                    if max_items and processed_count >= max_items:
                        break

                if max_items and processed_count >= max_items:
                    break

            stats["total_processed"] = processed_count
            stats["end_time"] = datetime.now(UTC)
            stats["duration"] = (
                stats["end_time"] - stats["start_time"]
            ).total_seconds()

            logger.info(f"Sequential processing completed: {stats}")
            return stats

        except Exception as e:
            logger.error(f"Sequential processing failed: {e}")
            stats["error"] = str(e)
            return stats

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
        Get pagination metadata for optimal parallel distribution.
        """
        logger.info("Analyzing pagination metadata for optimal distribution")

        try:
            # Get first page to analyze metadata
            first_page = None
            async for page in self.fetch_phase_1_data(
                from_date=from_date,
                to_date=to_date,
                limit=limit,
                pagination_request=True,
                **kwargs,
            ):
                first_page = page
                break

            if not first_page:
                logger.warning("No data found for pagination analysis")
                return 0, {"total_count": 0, "needs_processing": 0}

            # Extract pagination info from first page
            pagination_info = self._extract_pagination_info(first_page)
            total_count = pagination_info.get("total_count", 0)
            count_per_page = limit or 250

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
        """
        return {
            "total_count": len(page_data),
            "count_per_page": len(page_data),
        }

    async def _get_last_processed_date(self) -> str | None:
        """Get the last processed date from the database for incremental processing."""
        if not self.client or not hasattr(self.client, "access_last_processed_date"):
            return None

        try:
            last_processed_date = await self.client.access_last_processed_date(
                self.data_type_name
            )
            if last_processed_date:
                logger.info(f"Found last processed date: {last_processed_date}")
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
        """Estimate pages needed for date range."""
        if to_date:
            from datetime import datetime

            start = datetime.strptime(from_date, "%Y-%m-%d")
            end = datetime.strptime(to_date, "%Y-%m-%d")
            last_proc = datetime.strptime(last_processed_date, "%Y-%m-%d")

            total_range = (end - start).days
            remaining_range = (end - last_proc).days

            if total_range > 0:
                fraction = remaining_range / total_range
                return max(1, int(total_pages * fraction))

        return max(1, total_pages // 2)

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
        """
        logger.info("Starting incremental data fetching")

        # Get last processed date
        last_processed_date = await self._get_last_processed_date()

        if not last_processed_date:
            from_date = (datetime.now(UTC) - timedelta(days=fallback_days)).strftime(
                "%Y-%m-%d"
            )
            logger.info(f"No last processed date found, using fallback: {from_date}")
        else:
            from_date = last_processed_date
            logger.info(f"Using last processed date: {from_date}")

        # Fetch data incrementally
        async for batch in self.fetch_phase_1_data(
            from_date=from_date, limit=limit, **kwargs
        ):
            latest_date = self._extract_latest_date_from_batch(batch)
            yield batch, latest_date

    def _extract_latest_date_from_batch(
        self, batch: list[dict[str, Any]]
    ) -> str | None:
        """Extract the latest date from a batch of items."""
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
