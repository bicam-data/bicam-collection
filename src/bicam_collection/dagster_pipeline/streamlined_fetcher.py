"""
Streamlined Fetcher with Integrated Processing

This module combines multiple fetcher abstraction layers into a single,
efficient component that includes built-in parallel processing.

Replaces:
- BaseFetcher (abstract base)
- CongressionalBaseFetcher (congressional-specific)
- SpecificFetcher (data type specific)
- OptimizedParallelProcessor (parallel processing)

Key improvements:
- Single responsibility: data fetching with parallelism
- Integrated processing engine
- Direct resource management
- Simplified error handling
"""

import asyncio
import logging
from datetime import UTC, datetime
from typing import Any

from ..libs.data_type_router import get_global_registry
from ..processing.dynamic_key_pool_manager import DynamicKeyPool
from ..processing.work_queue_manager import AdaptiveWorkQueue
from .resource_managers import ResourceCoordinator
from .streamlined_plugins import get_plugin_registry

logger = logging.getLogger(__name__)


class StreamlinedFetcher:
    """
    Streamlined fetcher that combines multiple abstraction layers
    into a single, efficient component.

    This replaces the complex hierarchy of:
    BaseFetcher → CongressionalBaseFetcher → SpecificFetcher → OptimizedParallelProcessor

    With a single, focused implementation that includes:
    - Data type configuration
    - API client management
    - Parallel processing
    - Storage management
    - Error handling
    """

    def __init__(self, data_type: str, coordinator: ResourceCoordinator):
        self.data_type = data_type
        self.coordinator = coordinator

        # Get configuration
        registry = get_global_registry()
        self.config = registry.get_data_type_config(data_type)
        self.data_source = registry.get_data_source(data_type)

        # Get data type-specific plugin
        plugin_registry = get_plugin_registry()
        self.fetcher_plugin = plugin_registry.get_fetcher_plugin(data_type)

        if not self.fetcher_plugin:
            logger.warning(
                f"No fetcher plugin found for {data_type}, using fallback implementation"
            )

        # Processing state
        self.stats = {
            "total_processed": 0,
            "total_errors": 0,
            "start_time": None,
            "end_time": None,
        }

    async def execute_integrated_processing(
        self,
        from_date: str | None = None,
        to_date: str | None = None,
        congress: int | None = None,
        limit: int = 250,
        **kwargs,
    ) -> dict[str, Any]:
        """
        Execute integrated fetching with built-in parallel processing.

        This method replaces the complex pipeline with a single, efficient
        execution that combines all fetching phases.
        """
        self.stats["start_time"] = datetime.now(UTC)

        try:
            logger.info(f"Starting integrated processing for {self.data_type}")

            # Initialize resources
            await self._initialize_resources()

            # Get processing parameters
            params = await self._get_processing_parameters(
                from_date=from_date,
                to_date=to_date,
                congress=congress,
                limit=limit,
                **kwargs,
            )

            # Execute the three phases in parallel where possible
            results = await self._execute_parallel_phases(params)

            # Finalize and return results
            return await self._finalize_results(results)

        except Exception as e:
            logger.error(f"Integrated processing failed for {self.data_type}: {e}")
            return {
                "status": "error",
                "data_type": self.data_type,
                "error": str(e),
                "stats": self.stats,
            }
        finally:
            self.stats["end_time"] = datetime.now(UTC)

    async def _initialize_resources(self):
        """Initialize all required resources."""
        # Get managers from coordinator
        self.api_key_manager = await self.coordinator.get_api_key_manager()
        self.storage_manager = await self.coordinator.get_storage_manager()
        self.checkpoint_manager = await self.coordinator.get_checkpoint_manager()
        self.db_manager = await self.coordinator.get_database_manager()

        # Initialize key pool for parallel processing
        api_keys = await self.api_key_manager.get_available_keys()
        self.key_pool = DynamicKeyPool(
            keys=api_keys,
            rate_limit_per_key=3600,  # Congressional API limit
            rate_limit_window=3600,  # 1 hour window
            max_concurrent_per_key=1,
        )

    async def _get_processing_parameters(
        self,
        from_date: str | None = None,
        to_date: str | None = None,
        congress: int | None = None,
        limit: int = 250,
        **kwargs,
    ) -> dict[str, Any]:
        """Get processing parameters with defaults from configuration."""

        # Get API client for metadata
        api_client = await self.api_key_manager.get_api_client(self.data_source)

        # Get pagination metadata
        metadata = await self._get_pagination_metadata(
            api_client, from_date, to_date, limit, **kwargs
        )

        # Calculate processing parameters
        total_count = metadata.get("total_count", 0)
        count_per_page = metadata.get("count_per_page", limit)

        # Check for incremental processing
        last_processed = await self._get_last_processed_count()
        incremental = last_processed > 0 and total_count > last_processed

        # Calculate work distribution
        records_to_process = (
            total_count - last_processed if incremental else total_count
        )
        total_pages = (records_to_process + count_per_page - 1) // count_per_page

        return {
            "from_date": from_date,
            "to_date": to_date,
            "congress": congress,
            "limit": limit,
            "total_count": total_count,
            "count_per_page": count_per_page,
            "total_pages": total_pages,
            "records_to_process": records_to_process,
            "incremental": incremental,
            "last_processed": last_processed,
            "api_client": api_client,
            **kwargs,
        }

    async def _execute_parallel_phases(self, params: dict[str, Any]) -> dict[str, Any]:
        """Execute the three phases with optimized parallelism."""

        if params["records_to_process"] <= 0:
            return {"status": "no_data", "message": "No new records to process"}

        # Create work queue
        work_queue = AdaptiveWorkQueue(
            total_records=params["records_to_process"],
            initial_chunk_size=1000,  # Reasonable chunk size
            page_size=params["count_per_page"],
        )

        # Calculate optimal worker count
        num_workers = min(8, len(self.key_pool.keys), params["total_pages"])

        # Create workers
        workers = []
        for i in range(num_workers):
            worker = self._create_integrated_worker(
                worker_id=f"{self.data_type}_worker_{i}",
                work_queue=work_queue,
                params=params,
            )
            workers.append(worker)

        # Execute workers in parallel
        logger.info(f"Starting {num_workers} integrated workers for {self.data_type}")

        # Start storage manager
        if self.storage_manager:
            await self.storage_manager.start()

        try:
            worker_results = await asyncio.gather(*workers, return_exceptions=True)
        finally:
            # Stop storage manager
            if self.storage_manager:
                await self.storage_manager.stop()

        # Aggregate results
        return self._aggregate_worker_results(worker_results)

    async def _create_integrated_worker(
        self, worker_id: str, work_queue: AdaptiveWorkQueue, params: dict[str, Any]
    ):
        """Create a worker that integrates all three phases."""

        worker_stats = {
            "worker_id": worker_id,
            "processed": 0,
            "errors": 0,
            "chunks_completed": 0,
            "keys_used": set(),
        }

        while True:
            try:
                # Get next chunk
                chunk = await work_queue.get_next_chunk()
                if chunk is None:
                    break  # No more work

                # Process chunk with integrated phases
                chunk_processed = await self._process_integrated_chunk(
                    worker_id, chunk, params, worker_stats
                )

                worker_stats["processed"] += chunk_processed
                worker_stats["chunks_completed"] += 1

                # Mark chunk as completed
                work_queue.mark_chunk_completed(chunk)

            except Exception as e:
                logger.error(f"Worker {worker_id} error: {e}")
                worker_stats["errors"] += 1

        return worker_stats

    async def _process_integrated_chunk(
        self,
        worker_id: str,
        chunk,
        params: dict[str, Any],
        worker_stats: dict[str, Any],
    ) -> int:
        """Process a chunk with integrated Phase 1, 2, and 3 processing."""

        processed = 0
        offset = chunk.start_offset

        # Get API key
        api_key = await self.key_pool.checkout_key(worker_id, chunk.estimated_requests)
        if not api_key:
            logger.error(f"Worker {worker_id} couldn't get API key")
            return 0

        worker_stats["keys_used"].add(api_key.key[:8])

        # Create client
        api_client = await self.api_key_manager.get_api_client(
            self.data_source, api_key=api_key.key
        )

        try:
            # Process each page in the chunk
            while offset < chunk.end_offset:
                batch_size = min(params["limit"], chunk.end_offset - offset)

                # Phase 1: Fetch list data
                list_items = await self._fetch_list_data(
                    api_client, params, offset=offset, limit=batch_size
                )

                # Process each item through all phases
                for item in list_items:
                    try:
                        # Phase 1: Store list data
                        await self._store_phase_1_data(item)

                        # Phase 2: Get detailed data
                        if item.get("url"):
                            detailed_data = await self._fetch_detailed_data(
                                api_client, item["url"]
                            )

                            if detailed_data:
                                # Phase 2: Store detailed data
                                await self._store_phase_2_data(detailed_data)

                                # Phase 3: Get related data
                                related_data = await self._fetch_related_data(
                                    api_client, detailed_data
                                )

                                if related_data:
                                    # Phase 3: Store related data
                                    item_id = self._extract_item_id(detailed_data)
                                    await self._store_phase_3_data(
                                        related_data, item_id
                                    )

                        processed += 1

                    except Exception as e:
                        logger.error(f"Error processing item: {e}")
                        worker_stats["errors"] += 1

                offset += batch_size

        finally:
            # Return API key
            await self.key_pool.checkin_key(api_key.key, chunk.estimated_requests)

        return processed

    async def _fetch_list_data(
        self, api_client, params: dict[str, Any], offset: int, limit: int
    ) -> list[dict[str, Any]]:
        """Fetch Phase 1 list data using plugin or fallback."""
        if self.fetcher_plugin:
            return await self.fetcher_plugin.fetch_list_data(
                api_client=api_client,
                from_date=params.get("from_date"),
                to_date=params.get("to_date"),
                limit=limit,
                offset=offset,
                **{
                    k: v for k, v in params.items() if k not in ["from_date", "to_date"]
                },
            )
        else:
            # Fallback implementation
            logger.warning(f"Using fallback list data fetching for {self.data_type}")
            return []

    async def _fetch_detailed_data(
        self, api_client, url: str
    ) -> dict[str, Any] | None:
        """Fetch Phase 2 detailed data using plugin or fallback."""
        if self.fetcher_plugin:
            return await self.fetcher_plugin.fetch_detailed_data(api_client, url)
        else:
            # Fallback implementation
            logger.warning(
                f"Using fallback detailed data fetching for {self.data_type}"
            )
            return None

    async def _fetch_related_data(
        self, api_client, detailed_data: dict[str, Any]
    ) -> list[dict[str, Any]] | None:
        """Fetch Phase 3 related data using plugin or fallback."""
        if self.fetcher_plugin:
            return await self.fetcher_plugin.fetch_related_data(
                api_client, detailed_data
            )
        else:
            # Fallback implementation
            logger.warning(f"Using fallback related data fetching for {self.data_type}")
            return None

    async def _store_phase_1_data(self, data: dict[str, Any]):
        """Store Phase 1 data using storage manager."""
        if self.storage_manager:
            schema = self._get_schema_name()
            table = f"{self.data_type}_list_raw"
            await self.storage_manager.add_records(schema, table, [data])

    async def _store_phase_2_data(self, data: dict[str, Any]):
        """Store Phase 2 data using storage manager."""
        if self.storage_manager:
            schema = self._get_schema_name()
            table = f"{self.data_type}_raw"
            await self.storage_manager.add_records(schema, table, [data])

    async def _store_phase_3_data(self, data: list[dict[str, Any]], item_id: str):
        """Store Phase 3 data using storage manager."""
        if self.storage_manager:
            schema = self._get_schema_name()
            table = f"{self.data_type}_related"

            # Add item_id to each record
            for record in data:
                record["parent_id"] = item_id

            await self.storage_manager.add_records(schema, table, data)

    def _get_schema_name(self) -> str:
        """Get schema name based on data source."""
        return f"raw_{self.data_source}"

    def _extract_item_id(self, data: dict[str, Any]) -> str:
        """Extract item ID from data using plugin or fallback."""
        if self.fetcher_plugin:
            return self.fetcher_plugin.extract_item_id(data)
        else:
            # Fallback implementation
            return data.get("id") or data.get("url", "").split("/")[-1]

    async def _get_pagination_metadata(
        self,
        api_client,
        from_date: str | None = None,
        to_date: str | None = None,
        limit: int = 250,
        **kwargs,
    ) -> dict[str, Any]:
        """Get pagination metadata from API."""
        # Placeholder implementation
        return {"total_count": 1000, "count_per_page": limit, "has_data": True}

    async def _get_last_processed_count(self) -> int:
        """Get last processed count for incremental processing."""
        # Use checkpoint manager to get last processed count
        return 0

    def _aggregate_worker_results(self, worker_results: list[Any]) -> dict[str, Any]:
        """Aggregate results from all workers."""

        total_processed = 0
        total_errors = 0
        valid_results = []

        for result in worker_results:
            if isinstance(result, dict):
                total_processed += result.get("processed", 0)
                total_errors += result.get("errors", 0)
                valid_results.append(result)
            else:
                logger.error(f"Worker failed: {result}")
                total_errors += 1

        self.stats["total_processed"] = total_processed
        self.stats["total_errors"] = total_errors

        return {
            "status": "success",
            "total_processed": total_processed,
            "total_errors": total_errors,
            "worker_results": valid_results,
        }

    async def _finalize_results(self, results: dict[str, Any]) -> dict[str, Any]:
        """Finalize processing results."""

        duration = (
            (self.stats["end_time"] - self.stats["start_time"]).total_seconds()
            if self.stats["end_time"] and self.stats["start_time"]
            else 0
        )

        return {
            "status": results["status"],
            "data_type": self.data_type,
            "data_source": self.data_source,
            "total_processed": results.get("total_processed", 0),
            "total_errors": results.get("total_errors", 0),
            "duration_seconds": duration,
            "records_per_second": (
                results.get("total_processed", 0) / duration if duration > 0 else 0
            ),
            "worker_results": results.get("worker_results", []),
        }
