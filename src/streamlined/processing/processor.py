"""
Optimized Parallel Processor using rotisserie for key management and work queue.

This module implements the optimized processing strategy with rotisserie
key pool and work distribution.
"""

import asyncio
import logging
from datetime import UTC, datetime
from typing import Any

from rotisserie import AsyncKeyPool, KeyConfig

from ..libs.hierarchical_checkpoint_system import (
    FetchingPhase,
    ProcessingStage,
)
from ..processing.storage_adapter import StorageAdapter
from ..processing.work_queue import AdaptiveWorkQueue, WorkChunk

logger = logging.getLogger(__name__)


class ParallelProcessor:
    """
    Optimized processor that uses dynamic key pool and work queue for maximum efficiency.

    Key improvements:
    - More workers than keys for better utilization
    - Dynamic key allocation based on availability
    - Smaller work chunks for better load balancing
    - Preemptive key rotation before rate limits
    """

    def __init__(
        self,
        api_keys: list[str],
        client_class,
        db_pool=None,
        num_workers: int | None = None,
        chunk_size: int = 5000,
        key_pool: AsyncKeyPool | None = None,
    ):
        self.api_keys = api_keys  # Store API keys for access in methods
        self.client_class = client_class
        self.db_pool = db_pool

        # Initialize rotisserie key pool if not provided
        if key_pool is None:
            key_configs = [KeyConfig(f"key_{i}", key) for i, key in enumerate(api_keys)]
            self.key_pool = AsyncKeyPool(key_configs, distribute=True)
        else:
            self.key_pool = key_pool

        # Default to 1.5x keys for workers (e.g., 48 workers for 32 keys)
        self.num_workers = num_workers or int(len(api_keys) * 1.5)
        self.chunk_size = chunk_size

        # Track worker performance
        self._worker_stats = {}

        logger.info("==================================================")
        logger.info("=  ParallelProcessor INITIALIZED        =")
        logger.info(
            f"=  Workers: {self.num_workers}, Chunk Size: {self.chunk_size}          ="
        )
        logger.info(
            f"=  API Keys: {len(api_keys)}, Key Pool: Rotisserie AsyncKeyPool    ="
        )
        logger.info("==================================================")

        logger.info(
            f"Initialized optimized processor: {len(api_keys)} keys, "
            f"{self.num_workers} workers, chunk size {chunk_size}"
        )

    async def process_data_type(
        self,
        fetcher,
        data_type: str,
        from_date: str | None = None,
        to_date: str | None = None,
        limit: int = 250,
        **kwargs,
    ) -> dict[str, Any]:
        """
        Process a data type using optimized parallel strategy.

        The fetcher handles all checkpoint logic internally.
        """
        start_time = datetime.now(UTC)

        # =============================================================================
        # STEP 1: FETCH RAW PAGINATION METADATA
        # =============================================================================
        logger.info("=" * 60)
        logger.info(f"PAGINATION ANALYSIS FOR {data_type.upper()}")
        logger.info("=" * 60)

        # Check if fetcher has resume information
        resume_offset = 0
        if hasattr(fetcher, "get_resume_offset"):
            resume_offset = await fetcher.get_resume_offset()
            if resume_offset > 0:
                logger.info(f"Fetcher indicates resume from offset: {resume_offset}")

        # Get raw pagination metadata from fetcher
        logger.info("Fetching raw pagination metadata from API...")
        raw_metadata = await fetcher._get_pagination_metadata(
            from_date, to_date, limit, **kwargs
        )
        # =============================================================================
        # STEP 2: ANALYZE PAGINATION METADATA
        # =============================================================================
        logger.info("-" * 40)
        logger.info("ANALYZING PAGINATION METADATA")
        logger.info("-" * 40)

        # Extract basic info
        total_count = (raw_metadata or {}).get("total_count", 0)
        # CRITICAL: Use the intended processing limit, not the metadata fetch limit
        count_per_page = (
            limit or 250
        )  # raw_metadata count_per_page was for metadata fetch only

        # For GovInfo data types, use fixed page size of 1000 (API maximum)
        if hasattr(fetcher, "data_source") and fetcher.data_source == "govinfo":
            count_per_page = 1000
            logger.info(
                f"GovInfo data type detected: Using fixed page size of {count_per_page}"
            )

        has_data = (raw_metadata or {}).get("has_data", False)
        error = (raw_metadata or {}).get("error")

        logger.info(f"Total count: {total_count}")
        logger.info(f"Count per page: {count_per_page}")
        logger.info(f"Has data: {has_data}")
        if error:
            logger.error(f"Error in raw metadata: {error}")

        # Update checkpoint with total count from API response
        if hasattr(fetcher, "hierarchical_checkpoint_manager") and total_count > 0:
            try:
                # Get the list_items checkpoint and update its total_items
                list_checkpoint = (
                    fetcher.hierarchical_checkpoint_manager.get_or_create_checkpoint(
                        ProcessingStage.FETCHING,
                        FetchingPhase.LIST_ITEMS.value,
                        data_type,
                    )
                )
                if list_checkpoint.total_items == 0:
                    list_checkpoint.total_items = total_count
                    fetcher.hierarchical_checkpoint_manager.save_checkpoint(
                        list_checkpoint
                    )
                    logger.info(
                        f"Updated checkpoint total_items to {total_count} for {data_type}"
                    )
            except Exception as e:
                logger.warning(f"Failed to update checkpoint total_items: {e}")

        # Check if we have data to process
        if not has_data or total_count == 0:
            logger.info(f"No {data_type} data to process")
            return {
                "status": "no_data",
                "total_processed": 0,
                "pagination_analysis": {
                    "raw_metadata": raw_metadata,
                    "total_count": 0,
                    "pages_to_process": 0,
                },
            }

        # Calculate total pages
        total_pages = (total_count + count_per_page - 1) // count_per_page
        logger.info(f"Total pages calculated: {total_pages}")

        # =============================================================================
        # STEP 3: INCREMENTAL PROCESSING ANALYSIS
        # =============================================================================
        logger.info("-" * 40)
        logger.info("INCREMENTAL PROCESSING ANALYSIS")
        logger.info("-" * 40)

        # Check for incremental processing using last_total_count
        last_processed_count = await self._get_last_processed_count(fetcher)

        incremental_processing = (
            last_processed_count > 0 and total_count > last_processed_count
        )

        logger.info(f"*** DEBUG: Last processed count: {last_processed_count}")
        logger.info(f"*** DEBUG: Current total count: {total_count}")
        logger.info(
            f"*** DEBUG: Incremental processing condition: {last_processed_count} > 0 and {total_count} > {last_processed_count}"
        )
        logger.info(f"*** DEBUG: Incremental processing: {incremental_processing}")

        if incremental_processing:
            # Calculate exact pages needed based on new records
            new_records = total_count - last_processed_count
            pages_to_process = (new_records + count_per_page - 1) // count_per_page
            logger.info(f"*** DEBUG: New records since last run: {new_records}")
            logger.info(f"*** DEBUG: Pages needed for new records: {pages_to_process}")
        else:
            pages_to_process = total_pages
            logger.info(
                f"*** DEBUG: Full processing - using total_pages: {total_pages}"
            )

        logger.info(f"*** DEBUG: Final pages to process: {pages_to_process}")
        logger.info(
            f"*** DEBUG: Total records calculation: min({total_count}, {pages_to_process} * {count_per_page}) = {min(total_count, pages_to_process * count_per_page)}"
        )

        # =============================================================================
        # STEP 4: WORK DISTRIBUTION ANALYSIS
        # =============================================================================
        logger.info("-" * 40)
        logger.info("WORK DISTRIBUTION ANALYSIS")
        logger.info("-" * 40)

        # Calculate total records to process
        total_records = min(total_count, pages_to_process * count_per_page)

        logger.info(f"Total records to process: {total_records}")
        logger.info(f"Workers available: {self.num_workers}")
        logger.info(f"Chunk size: {self.chunk_size}")
        logger.info(f"Page size: {count_per_page}")

        # Calculate work distribution metrics
        estimated_chunks = (total_records + self.chunk_size - 1) // self.chunk_size
        workers_per_chunk = (
            max(1, self.num_workers // estimated_chunks)
            if estimated_chunks > 0
            else self.num_workers
        )

        logger.info(f"Estimated chunks: {estimated_chunks}")
        logger.info(f"Workers per chunk: {workers_per_chunk}")

        # =============================================================================
        # STEP 5: INITIALIZE WORK QUEUE
        # =============================================================================
        logger.info("-" * 40)
        logger.info("INITIALIZING WORK QUEUE")
        logger.info("-" * 40)

        # Initialize work queue (with resume support if available)
        work_queue = AdaptiveWorkQueue(
            total_records=total_records,
            initial_chunk_size=self.chunk_size,
            page_size=count_per_page,
        )
        logger.info(f"Work queue: {self.chunk_size} records per chunk")

        # If resuming, adjust work queue to start from resume offset
        if resume_offset > 0:
            # Remove chunks that are before the resume offset
            chunks_to_skip = []
            while not work_queue._queue.empty():
                chunk = await work_queue._queue.get()
                if chunk.end_offset <= resume_offset:
                    # This chunk is already processed
                    chunks_to_skip.append(chunk.chunk_id)
                    await work_queue.complete_work("resume", chunk.chunk_id, True)
                else:
                    # This chunk needs processing (possibly partial)
                    if chunk.start_offset < resume_offset:
                        # Adjust chunk to start from resume offset
                        chunk.start_offset = resume_offset
                        logger.info(
                            f"Adjusted chunk {chunk.chunk_id} to start from {resume_offset}"
                        )
                    # Put it back in the queue
                    await work_queue._queue.put(chunk)
                    break

            if chunks_to_skip:
                logger.info(f"Skipped {len(chunks_to_skip)} already-processed chunks")

        logger.info(f"Work queue initialized with {total_records} total records")

        # =============================================================================
        # STEP 6: INITIALIZE STORAGE MANAGER
        # =============================================================================
        logger.info("-" * 40)
        logger.info("INITIALIZING STORAGE MANAGER")
        logger.info("-" * 40)

        # Start storage manager if using optimized storage
        storage_manager = None
        if (
            hasattr(fetcher, "resource_coordinator")
            and fetcher.resource_coordinator
            and getattr(
                fetcher.resource_coordinator.config.infrastructure,
                "use_optimized_storage",
                False,
            )
        ):
            try:
                storage_manager = (
                    await fetcher.resource_coordinator.get_storage_manager_instance()
                )
                if storage_manager:
                    logger.info("Starting storage manager background workers...")
                    await storage_manager.start()
                    logger.info("Storage manager started successfully")
            except Exception as e:
                logger.error(f"Failed to initialize storage manager: {e}")
                storage_manager = None

        # =============================================================================
        # STEP 7: CREATE AND START WORKERS
        # =============================================================================
        logger.info("-" * 40)
        logger.info("STARTING WORKERS")
        logger.info("-" * 40)

        # Create workers
        workers = []
        for i in range(self.num_workers):
            worker = self._create_worker(
                worker_id=f"{data_type}_worker_{i}",
                fetcher=fetcher,
                work_queue=work_queue,
                from_date=from_date,
                to_date=to_date,
                limit=count_per_page,  # This will be 1000 for GovInfo, 250 for Congressional
                **kwargs,
            )
            workers.append(worker)

        # Start periodic status reporting
        status_task = asyncio.create_task(self._report_status(work_queue))

        # Start key pool maintenance
        maintenance_task = asyncio.create_task(self._maintain_key_pool())

        try:
            # Run all workers
            logger.info(f"Starting {len(workers)} workers for {data_type}...")
            worker_results = await asyncio.gather(*workers, return_exceptions=True)

        finally:
            # Cancel status reporting and maintenance tasks
            status_task.cancel()
            maintenance_task.cancel()

            # Wait for tasks to complete cancellation
            try:
                await asyncio.gather(
                    status_task, maintenance_task, return_exceptions=True
                )
            except Exception as e:
                logger.debug(f"Error during task cancellation: {e}")

            # CRITICAL: Stop storage manager to flush remaining data
            if storage_manager:
                try:
                    logger.info(
                        "Stopping storage manager and flushing remaining data..."
                    )
                    await storage_manager.stop()
                    logger.info("Storage manager stopped and data flushed")
                except Exception as e:
                    logger.error(f"Error stopping storage manager: {e}")

        # =============================================================================
        # STEP 8: AGGREGATE RESULTS
        # =============================================================================
        logger.info("-" * 40)
        logger.info("AGGREGATING RESULTS")
        logger.info("-" * 40)

        # Aggregate results
        stats = {
            "data_type": data_type,
            "total_workers": len(workers),
            "total_keys": len(self.api_keys),
            "duration": (datetime.now(UTC) - start_time).total_seconds(),
            "total_processed": 0,
            "total_errors": 0,
            "worker_stats": [],
            "pagination_analysis": {
                "raw_metadata": raw_metadata,
                "total_count": total_count,
                "total_pages": total_pages,
                "pages_to_process": pages_to_process,
                "count_per_page": count_per_page,
                "last_processed_count": last_processed_count,
                "incremental_processing": incremental_processing,
                "new_records": total_count - last_processed_count
                if incremental_processing
                else 0,
                "total_records": total_records,
                "estimated_chunks": estimated_chunks,
                "workers_per_chunk": workers_per_chunk,
            },
        }

        for i, result in enumerate(worker_results):
            if isinstance(result, dict):
                stats["total_processed"] += result.get("processed", 0)
                stats["total_errors"] += result.get("errors", 0)
                stats["worker_stats"].append(result)
            else:
                logger.error(f"Worker {i} failed: {result}")
                stats["total_errors"] += 1

        # Get final key pool status
        # Rotisserie manages keys automatically - no status tracking needed
        stats["final_key_status"] = {
            "note": "Rotisserie handles key management automatically"
        }

        logger.info("=" * 60)
        logger.info(f"PROCESSING COMPLETED FOR {data_type.upper()}")
        logger.info("=" * 60)
        logger.info(
            f"Results: {stats['total_processed']} records in {stats['duration']:.1f}s "
            f"({stats['total_errors']} errors)"
        )

        return stats

    async def _create_worker(
        self,
        worker_id: str,
        fetcher,
        work_queue: AdaptiveWorkQueue,
        from_date: str | None,
        to_date: str | None,
        limit: int,
        **kwargs,
    ):
        """Create and run a single worker."""
        worker_stats = {
            "worker_id": worker_id,
            "processed": 0,
            "errors": 0,
            "chunks_completed": 0,
            "keys_used": set(),
        }

        logger.info(f"Worker {worker_id} started")

        while not work_queue.is_complete():
            # Get work chunk
            chunk = await work_queue.get_work(worker_id)
            if not chunk:
                # No work available, check if we're done
                if work_queue.is_complete():
                    break
                await asyncio.sleep(1)
                continue

            chunk_start = datetime.now(UTC)

            # Process the chunk with dynamic key allocation
            try:
                processed = await self._process_chunk(
                    worker_id=worker_id,
                    fetcher=fetcher,
                    chunk=chunk,
                    from_date=from_date,
                    to_date=to_date,
                    limit=limit,
                    worker_stats=worker_stats,
                    **kwargs,
                )

                worker_stats["processed"] += processed
                worker_stats["chunks_completed"] += 1

                # Report completion with duration for adaptive sizing
                duration = (datetime.now(UTC) - chunk_start).total_seconds()
                await work_queue.complete_work(
                    worker_id, chunk.chunk_id, True, duration
                )

            except Exception as e:
                logger.error(f"Worker {worker_id} failed on {chunk.chunk_id}: {e}")
                worker_stats["errors"] += 1
                await work_queue.complete_work(worker_id, chunk.chunk_id, False)

        logger.info(
            f"Worker {worker_id} finished: "
            f"{worker_stats['processed']} processed, "
            f"{worker_stats['chunks_completed']} chunks, "
            f"{len(worker_stats['keys_used'])} keys used"
        )

        return worker_stats

    async def _process_chunk(
        self,
        worker_id: str,
        fetcher,
        chunk: WorkChunk,
        from_date: str | None,
        to_date: str | None,
        limit: int,
        worker_stats: dict,
        **kwargs,
    ) -> int:
        """Process a single work chunk with improved key management and optional Phase 4."""
        processed = 0
        offset = chunk.start_offset if chunk.start_offset is not None else 0

        # Validate offset
        if offset is None:
            logger.warning(f"Worker {worker_id} received None offset, using 0")
            offset = 0

        logger.info(
            f"Worker {worker_id} processing chunk: start_offset={chunk.start_offset}, end_offset={chunk.end_offset}, using offset={offset}"
        )

        # Check if we should use optimized storage
        optimized_storage = None

        try:
            storage_manager = (
                await fetcher.resource_coordinator.get_storage_manager_instance()
            )
            if storage_manager:
                # Get the id_field from the fetcher's config
                id_field = getattr(fetcher, "id_field", "parent_id")
                optimized_storage = StorageAdapter(
                    storage_manager,
                    ProcessingStage.FETCHING,
                    fetcher=fetcher,
                    id_field=id_field,
                )
                logger.debug(f"Worker {worker_id} using optimized storage")
        except Exception as e:
            logger.warning(f"Failed to get optimized storage: {e}")

        # Keep trying to process the chunk until it's complete
        consecutive_failures = 0
        max_consecutive_failures = 3

        while offset < chunk.end_offset:
            # Calculate requests needed for this batch
            remaining_records = chunk.end_offset - offset
            batch_size = min(limit, remaining_records)
            estimated_requests = max(1, batch_size // limit)  # Pages needed

            # Log processing information
            logger.info(
                f"Worker {worker_id} processing chunk [records {chunk.start_offset}-{chunk.end_offset}] "
                f"at offset {offset}, batch_size={batch_size}, remaining={remaining_records}"
            )

            # With rotisserie, we don't need to checkout keys - clients use auth context managers
            # Create client with key_pool - rotisserie will handle key rotation automatically
            client = self.client_class(
                api_keys=self.api_keys, key_pool=self.key_pool, db_pool=self.db_pool
            )

            async with client:
                try:
                    # Process this batch
                    batch_processed = 0
                    requests_made = 0

                    # CRITICAL: Use single_page_only=True to prevent overlapping data fetching
                    async for batch in fetcher.fetch_phase_1_data_with_client(
                        client,
                        from_date=from_date,
                        to_date=to_date,
                        limit=batch_size,
                        offset=offset,
                        single_page_only=True,
                        **kwargs,
                    ):
                        requests_made += 1

                        # Process items in batch (Phase 1, 2, 3, optional 4)
                        for item in batch:
                            try:
                                await optimized_storage.store_phase_1_data(
                                    fetcher.get_default_schema(),
                                    f"{fetcher.data_type_name}_list_raw",
                                    item,
                                )

                                # Phase 2: Get and store full data
                                url = item.get("url")
                                logger.debug(
                                    f"Worker {worker_id} item keys: {list(item.keys())}"
                                )
                                logger.debug(f"Worker {worker_id} extracted url: {url}")
                                if url:
                                    logger.debug(
                                        f"Worker {worker_id} fetching Phase 2 data from {url}"
                                    )
                                    detailed_data = (
                                        await fetcher.fetch_phase_2_data_with_client(
                                            url, client
                                        )
                                    )
                                    requests_made += 1

                                    if detailed_data:
                                        logger.debug(
                                            f"Worker {worker_id} got Phase 2 data, proceeding to Phase 3"
                                        )

                                        await optimized_storage.store_phase_2_data(
                                            fetcher.get_default_schema(),
                                            f"{fetcher.data_type_name}_raw",
                                            detailed_data,
                                        )
                                        # Phase 3: Get related data
                                        logger.debug(
                                            f"Worker {worker_id} starting Phase 3 for item {item.get('url', 'unknown')}"
                                        )

                                        # Check if this item needs parallel related data processing
                                        pagination_info = await fetcher.get_related_data_pagination_metadata(
                                            detailed_data, client
                                        )

                                        if (
                                            pagination_info
                                            and pagination_info.get(
                                                "total_parallel_work", 0
                                            )
                                            > 10
                                        ):
                                            # Use parallel processing for related data
                                            logger.info(
                                                f"Worker {worker_id} using parallel processing for related data: "
                                                f"{pagination_info['total_parallel_work']} pages to process"
                                            )

                                            related_data = await self._process_related_data_parallel(
                                                worker_id=worker_id,
                                                fetcher=fetcher,
                                                pagination_info=pagination_info,
                                                client=client,
                                                optimized_storage=optimized_storage,
                                            )
                                        else:
                                            # Use normal sequential processing
                                            related_data = await fetcher.fetch_phase_3_data_with_client(
                                                detailed_data, client
                                            )

                                        if related_data:
                                            requests_made += len(
                                                related_data
                                            )  # Approximate
                                            item_id = await fetcher.extract_item_id(
                                                detailed_data
                                            )

                                            logger.debug(
                                                f"Worker {worker_id} processing Phase 3 data: "
                                                f"{len(related_data)} related items for {item_id}"
                                            )

                                            await optimized_storage.store_phase_3_data(
                                                fetcher.get_default_schema(),
                                                fetcher.data_type_name,
                                                related_data,
                                                item_id,
                                            )

                                            # Phase 4: Optionally fetch and store full related data
                                            if hasattr(
                                                fetcher,
                                                "fetch_phase_4_data_with_client",
                                            ) and callable(
                                                fetcher.fetch_phase_4_data_with_client
                                            ):
                                                logger.debug(
                                                    f"Worker {worker_id} checking for Phase 4 (full related data) for item {item_id}"
                                                )
                                                try:
                                                    # Use consistent naming scheme for Phase 4 processing
                                                    granule_type = f"{fetcher.data_type_name}_granules"

                                                    # Process each related data item for Phase 4
                                                    for related_item in related_data:
                                                        if (
                                                            isinstance(
                                                                related_item, dict
                                                            )
                                                            and related_item.get(
                                                                "type", ""
                                                            )
                                                            == granule_type
                                                        ):
                                                            granules_data = (
                                                                related_item.get(
                                                                    "data", []
                                                                )
                                                            )
                                                            if granules_data:
                                                                logger.debug(
                                                                    f"Worker {worker_id} processing {len(granules_data)} granules for Phase 4"
                                                                )

                                                                # Process each granule
                                                                for (
                                                                    granule
                                                                ) in granules_data:
                                                                    if isinstance(
                                                                        granule, dict
                                                                    ):
                                                                        full_granule_data = await fetcher.fetch_phase_4_data_with_client(
                                                                            granule,
                                                                            client,
                                                                        )
                                                                        if full_granule_data:
                                                                            logger.debug(
                                                                                f"Worker {worker_id} storing Phase 4 granule data for item {item_id}"
                                                                            )
                                                                            await optimized_storage.store_phase_4_data(
                                                                                full_granule_data,
                                                                                item_id,
                                                                            )
                                                except Exception as e:
                                                    logger.error(
                                                        f"Worker {worker_id} error in Phase 4 for item {item_id}: {e}",
                                                        exc_info=True,
                                                    )
                                        else:
                                            logger.debug(
                                                f"Worker {worker_id} found no Phase 3 data"
                                            )
                                    else:
                                        logger.warning(
                                            f"Worker {worker_id} got no Phase 2 data from {url}"
                                        )
                                else:
                                    logger.debug(
                                        f"Worker {worker_id} item has no URL, skipping Phase 2 and 3"
                                    )

                                batch_processed += 1
                                processed += 1

                            except Exception as e:
                                logger.error(
                                    f"Error processing item: {e}", exc_info=True
                                )
                                worker_stats["errors"] += 1

                    # Successfully processed batch, reset failure counter
                    consecutive_failures = 0
                    # Rotisserie handles key management automatically - no need to checkin

                    # Update offset to next page within chunk range
                    old_offset = offset
                    # Both Congressional and GovInfo increment by actual processed count
                    offset += batch_processed
                    logger.info(
                        f"Worker {worker_id} updated offset: {old_offset} -> {offset} "
                        f"(processed {batch_processed} items)"
                    )

                except Exception as e:
                    logger.error(f"Worker {worker_id} batch processing error: {e}")

                    # Rotisserie handles rate limits automatically via Retry-After headers
                    # Check if rate limited for logging
                    rate_limited = "429" in str(e) or "rate limit" in str(e).lower()

                    if rate_limited:
                        logger.info(
                            f"Worker {worker_id} hit rate limit - rotisserie will handle retry automatically"
                        )
                        # Rotisserie will retry automatically, but we can continue with next iteration
                        # Don't increment offset on rate limit - retry the same batch
                        continue
                    else:
                        # Non-rate-limit error
                        consecutive_failures += 1
                        if consecutive_failures >= max_consecutive_failures:
                            logger.error(
                                f"Worker {worker_id} failed {consecutive_failures} times "
                                f"consecutively, abandoning chunk"
                            )
                            raise
                        else:
                            logger.warning(
                                f"Worker {worker_id} encountered error, retrying... "
                                f"(failure {consecutive_failures}/{max_consecutive_failures})"
                            )
                            await asyncio.sleep(
                                2**consecutive_failures
                            )  # Exponential backoff

        logger.info(
            f"*** SUCCESS: Worker {worker_id} completed chunk [{chunk.start_offset}, {chunk.end_offset}] "
            f"- processed {processed} items"
        )
        return processed

    async def _process_related_data_parallel(
        self,
        worker_id: str,
        fetcher,
        pagination_info: dict[str, Any],
        client,
        optimized_storage,
    ) -> list[dict[str, Any]]:
        """
        Process related data pagination in parallel using multiple workers.

        This method creates a work queue for the pagination pages and distributes
        the work across available workers to fetch related data concurrently.
        """
        try:
            total_pages = pagination_info.get("total_parallel_work", 0)
            if total_pages == 0:
                return []

            logger.info(
                f"Worker {worker_id} starting parallel related data processing: "
                f"{total_pages} pages for item {pagination_info.get('item_id')}"
            )

            # Create a work queue for the pagination pages
            # Use smaller chunks for pagination (each page is ~250 items)
            page_chunk_size = max(1, min(5, total_pages // 10))  # 1-5 pages per chunk

            related_work_queue = AdaptiveWorkQueue(
                total_records=total_pages,
                initial_chunk_size=page_chunk_size,
                page_size=page_chunk_size,
            )

            # Create workers for related data pagination
            # Use fewer workers than main processing to avoid overwhelming the API
            configured_max_workers = pagination_info.get("max_workers", 4)
            num_related_workers = min(configured_max_workers, len(self.api_keys))

            logger.info(
                f"Creating {num_related_workers} workers for related data pagination "
                f"({total_pages} pages, {page_chunk_size} pages per chunk, "
                f"configured max: {configured_max_workers})"
            )

            # Create and start related data workers
            worker_tasks = []
            worker_results = [
                {"processed": 0, "errors": 0, "data": []}
                for _ in range(num_related_workers)
            ]

            for i in range(num_related_workers):
                task = asyncio.create_task(
                    self._process_related_data_worker(
                        worker_id=f"related_{worker_id}_worker_{i}",
                        fetcher=fetcher,
                        work_queue=related_work_queue,
                        pagination_info=pagination_info,
                        worker_results=worker_results[i],
                        client_class=self.client_class,
                        key_pool=self.key_pool,
                    )
                )
                worker_tasks.append(task)

            # Wait for all related data workers to complete
            await asyncio.gather(*worker_tasks, return_exceptions=True)

            # Aggregate all related data
            all_related_data = []
            total_processed = 0
            total_errors = 0

            for result in worker_results:
                all_related_data.extend(result.get("data", []))
                total_processed += result.get("processed", 0)
                total_errors += result.get("errors", 0)

            logger.info(
                f"Worker {worker_id} completed parallel related data processing: "
                f"{len(all_related_data)} items, {total_processed} pages processed, {total_errors} errors"
            )

            return all_related_data

        except Exception as e:
            logger.error(f"Error in parallel related data processing: {e}")
            return []

    async def _process_related_data_worker(
        self,
        worker_id: str,
        fetcher,
        work_queue: AdaptiveWorkQueue,
        pagination_info: dict[str, Any],
        worker_results: dict,
        client_class,
        key_pool,
    ):
        """Worker function for processing related data pagination in parallel."""
        logger.info(f"Related data worker {worker_id} started")

        try:
            while True:
                # Get next chunk of work (page numbers)
                chunk = await work_queue.get_work(worker_id)
                if chunk is None:
                    logger.info(
                        f"Related data worker {worker_id} finished - no more work"
                    )
                    break

                chunk_id = chunk.chunk_id
                start_offset = chunk.start_offset
                end_offset = chunk.end_offset

                # Convert offset range to page numbers
                page_numbers = list(range(start_offset, end_offset))

                logger.info(
                    f"Related data worker {worker_id} processing chunk {chunk_id} "
                    f"(pages: {page_numbers})"
                )

                # With rotisserie, create client with key_pool - keys managed automatically
                client = client_class(api_keys=self.api_keys, key_pool=key_pool)

                async with client:
                    try:
                        # Fetch related data for these pages
                        page_data = await fetcher.fetch_related_data_pages_parallel(
                            pagination_info, client, page_numbers
                        )

                        if page_data:
                            worker_results["data"].extend(page_data)
                            worker_results["processed"] += len(page_numbers)
                            logger.debug(
                                f"Related data worker {worker_id} got {len(page_data)} items "
                                f"from {len(page_numbers)} pages"
                            )
                        else:
                            logger.debug(
                                f"Related data worker {worker_id} no data from {len(page_numbers)} pages"
                            )

                        # Rotisserie handles key management automatically - no need to checkin

                        # Mark chunk as completed
                        await work_queue.complete_work(
                            worker_id, chunk_id, success=True
                        )

                    except Exception as e:
                        logger.error(
                            f"Related data worker {worker_id} error processing chunk {chunk_id}: {e}"
                        )
                        worker_results["errors"] += 1
                        # Rotisserie handles key management automatically
                        await work_queue.complete_work(
                            worker_id, chunk_id, success=False
                        )

        except Exception as e:
            logger.error(
                f"Related data worker {worker_id} encountered fatal error: {e}"
            )
        finally:
            logger.info(
                f"Related data worker {worker_id} completed. "
                f"Processed: {worker_results['processed']}, Errors: {worker_results['errors']}"
            )

    async def _acquire_api_key_with_smart_backoff(
        self,
        worker_id: str,
        key_pool,
        estimated_requests: int,
        chunk: WorkChunk,
        offset: int,
    ) -> Any:
        """
        Deprecated: With rotisserie, keys are managed automatically via auth context managers.
        This method is kept for backward compatibility but always returns None.
        Clients should use rotisserie's auth context managers instead.
        """
        # Rotisserie handles key management automatically - no need to checkout keys
        logger.debug(
            f"Worker {worker_id}: Using rotisserie for automatic key management "
            f"(chunk [{chunk.start_offset}-{chunk.end_offset}] @ offset {offset})"
        )
        return None

    async def _report_status(self, work_queue: AdaptiveWorkQueue, interval: int = 30):
        """Periodically report processing status."""
        while True:
            try:
                await asyncio.sleep(interval)

                queue_status = work_queue.get_progress()
                # Rotisserie doesn't have get_pool_status() - keys are managed automatically
                # Log queue status only
                logger.info(
                    f"Progress: {queue_status['completion_percentage']:.1f}% "
                    f"({queue_status['completed_chunks']}/{queue_status['total_chunks']} chunks)"
                )

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in status reporting: {e}")

    async def _maintain_key_pool(self, interval: int = 60):
        """Deprecated: Rotisserie handles key pool maintenance automatically."""
        # Rotisserie manages keys automatically - no maintenance needed
        logger.debug("Key pool maintenance not needed with rotisserie")
        while True:
            try:
                await asyncio.sleep(interval)
                # No-op - rotisserie handles everything
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in key pool maintenance: {e}")

    # =============================================================================
    # INCREMENTAL PROCESSING ANALYSIS
    # =============================================================================

    async def _get_last_processed_count(self, fetcher) -> int:
        """Get the last processed count from the database for incremental processing."""
        if not fetcher.client or not hasattr(
            fetcher.client, "access_last_processed_count"
        ):
            return 0

        try:
            last_processed_info = await fetcher.client.access_last_processed_count(
                fetcher.data_type_name
            )

            if last_processed_info and isinstance(last_processed_info, dict):
                # Extract total count from the last processed info
                total_count = last_processed_info.get("total_count", 0)
                if isinstance(total_count, int) and total_count > 0:
                    logger.info(
                        f"Found last processed count for {fetcher.data_type_name}: {total_count}"
                    )
                    return total_count

            logger.debug(
                f"No valid last processed count found for {fetcher.data_type_name}"
            )
            return 0

        except Exception as e:
            logger.debug(
                f"Could not get last processed count for {fetcher.data_type_name}: {e}"
            )
            return 0

    async def fetch_related_tables_from_existing_data(
        self,
        fetcher,
        data_type: str,
        related_tables: list[str],
        batch_size: int = 100,
        limit: int = None,
        from_date: str = None,
        to_date: str = None,
    ) -> dict[str, Any]:
        """
        Fetch specific related tables from existing Phase 2 data using TRUE parallel processing.

        This method:
        1. Queries the database for existing Phase 2 data
        2. Creates parallel workers to fetch related tables concurrently
        3. Uses the full optimized storage infrastructure
        4. Provides proper batching and resource management with parallel execution

        Args:
            fetcher: The Fetcher instance
            data_type: The data type to process
            related_tables: List of related table names to fetch (e.g., ['texts', 'actions'])
            batch_size: Number of items to process in each batch
            limit: Maximum number of items to process (optional)
            from_date: Start date filter (optional)
            to_date: End date filter (optional)

        Returns:
            Dictionary with processing results and metrics
        """
        start_time = datetime.now(UTC)

        logger.info("=" * 60)
        logger.info("FETCHING RELATED TABLES FROM EXISTING DATA (PARALLEL)")
        logger.info(f"Data Type: {data_type}")
        logger.info(f"Related Tables: {related_tables}")
        logger.info(f"API Keys Available: {len(self.api_keys)}")
        logger.info("=" * 60)

        if not fetcher.db_pool:
            logger.error("No database pool available")
            return {"error": "No database pool available"}

        if not fetcher._plugin:
            logger.error("No plugin available for fetching related data")
            return {"error": "No plugin available"}

        # Ensure we have a client for API calls
        if not fetcher.client:
            logger.info("No client available, creating one...")
            try:
                if fetcher.resource_coordinator:
                    clients = await fetcher.resource_coordinator.get_api_clients(
                        data_type
                    )
                    if clients:
                        fetcher.client = clients[0]
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

        # Initialize storage manager
        storage_manager = None
        try:
            if fetcher.resource_coordinator:
                storage_manager = (
                    await fetcher.resource_coordinator.get_storage_manager_instance()
                )
                if storage_manager:
                    logger.info("Starting storage manager background workers...")
                    await storage_manager.start()
                    logger.info("Storage manager started successfully")
                else:
                    logger.warning("No storage manager available, using direct storage")
            else:
                logger.warning(
                    "No resource coordinator available, using direct storage"
                )
        except Exception as e:
            logger.error(f"Failed to initialize storage manager: {e}")
            storage_manager = None

        # Create optimized storage adapter if storage manager is available
        optimized_storage = None
        if storage_manager:
            id_field = getattr(fetcher, "id_field", "parent_id")
            optimized_storage = StorageAdapter(
                storage_manager,
                ProcessingStage.FETCHING,
                fetcher=fetcher,
                id_field=id_field,
            )
            logger.info("Using optimized storage for related table fetching")
        else:
            logger.info("Using direct storage for related table fetching")

        # Get schema name
        schema_name = fetcher.get_default_schema()
        table_name = f"{data_type}_raw"

        # First, get the total count of items that need related table fetching
        count_query = f"""
        SELECT COUNT(*) as total_count
        FROM {schema_name}.{table_name}
        WHERE payload IS NOT NULL
        """

        count_params = []
        param_count = 0

        # Add date filters if provided
        if from_date:
            param_count += 1
            count_query += f" AND payload->>'updatedate' >= ${param_count}"
            count_params.append(from_date)

        if to_date:
            param_count += 1
            count_query += f" AND payload->>'updatedate' <= ${param_count}"
            count_params.append(to_date)

        # Add related table filtering conditions
        for table in related_tables:
            if table == "texts":
                param_count += 1
                count_query += " AND (payload->'textVersions' IS NULL OR jsonb_typeof(payload->'textVersions') != 'array' OR jsonb_array_length(payload->'textVersions') = 0)"
            # Add more table-specific conditions as needed

        if limit:
            count_query += f" LIMIT {limit}"

        logger.info(
            f"Counting items needing related table fetching from {schema_name}.{table_name}"
        )
        logger.info(f"Related tables to fetch: {related_tables}")

        # Get total count
        async with fetcher.db_pool.acquire() as conn:
            count_result = await conn.fetchval(count_query, *count_params)

        total_items_needing_fetch = count_result or 0

        if total_items_needing_fetch == 0:
            logger.info("No items need related table fetching")
            return {
                "status": "completed",
                "data_type": data_type,
                "data_source": getattr(fetcher, "data_source", "unknown"),
                "related_tables": related_tables,
                "metrics": {
                    "items_processed": 0,
                    "related_items_fetched": 0,
                    "errors": 0,
                    "duration": 0,
                    "workers_used": 0,
                    "api_keys_used": len(self.api_keys),
                    "items_needing_fetch": 0,
                    "total_items_checked": 0,
                },
                "batch_size": batch_size,
                "limit": limit,
                "from_date": from_date,
                "to_date": to_date,
            }

        logger.info(
            f"Found {total_items_needing_fetch} items that need related table fetching"
        )

        # Create work queue for pagination-based processing
        num_workers = (
            len(self.api_keys) * 2
        )  # Use 2x the number of API keys for workers
        work_queue = AdaptiveWorkQueue(
            total_records=total_items_needing_fetch,
            initial_chunk_size=batch_size,
            page_size=batch_size,
        )

        logger.info(
            f"Created work queue with {total_items_needing_fetch} items, {work_queue._work_queue.qsize()} chunks"
        )
        logger.info(f"Will use {num_workers} parallel workers")

        # Create and start parallel workers
        worker_tasks = []
        worker_stats = [
            {"processed": 0, "errors": 0, "related_items": 0}
            for _ in range(num_workers)
        ]

        # Start status reporting task
        status_task = asyncio.create_task(self._report_status(work_queue, interval=30))

        # Start key pool maintenance task
        key_pool_maintenance_task = asyncio.create_task(
            self._maintain_key_pool(interval=60)
        )

        try:
            # Create and start workers
            for worker_id in range(num_workers):
                task = asyncio.create_task(
                    self._process_related_tables_worker(
                        worker_id=f"related_worker_{worker_id}",
                        fetcher=fetcher,
                        work_queue=work_queue,
                        related_tables=related_tables,
                        worker_stats=worker_stats[worker_id],
                        optimized_storage=optimized_storage,
                        data_type=data_type,
                    )
                )
                worker_tasks.append(task)

            # Wait for all workers to complete
            await asyncio.gather(*worker_tasks, return_exceptions=True)

            # Cancel background tasks
            status_task.cancel()
            key_pool_maintenance_task.cancel()

        except Exception as e:
            logger.error(f"Error in parallel processing: {e}")
            # Cancel all tasks
            for task in worker_tasks:
                task.cancel()
            status_task.cancel()
            key_pool_maintenance_task.cancel()

        # Stop storage manager and flush remaining data
        if storage_manager:
            try:
                logger.info("Stopping storage manager and flushing remaining data...")
                await storage_manager.stop()
                logger.info("Storage manager stopped and data flushed")
            except Exception as e:
                logger.error(f"Error stopping storage manager: {e}")

        # Calculate final statistics
        total_processed = sum(stats["processed"] for stats in worker_stats)
        total_errors = sum(stats["errors"] for stats in worker_stats)
        total_related_items = sum(stats["related_items"] for stats in worker_stats)
        duration = (datetime.now(UTC) - start_time).total_seconds()

        logger.info(f"Successfully processed {total_processed} items")
        logger.info(f"Fetched {total_related_items} related items")
        if total_errors > 0:
            logger.warning(f"Encountered {total_errors} errors")

        return {
            "status": "completed",
            "data_type": data_type,
            "data_source": getattr(fetcher, "data_source", "unknown"),
            "related_tables": related_tables,
            "metrics": {
                "items_processed": total_processed,
                "related_items_fetched": total_related_items,
                "errors": total_errors,
                "duration": duration,
                "workers_used": num_workers,
                "api_keys_used": len(self.api_keys),
                "items_needing_fetch": total_items_needing_fetch,
                "total_items_checked": total_items_needing_fetch,
            },
            "batch_size": batch_size,
            "limit": limit,
            "from_date": from_date,
            "to_date": to_date,
        }

    async def _process_related_tables_worker(
        self,
        worker_id: str,
        fetcher,
        work_queue: AdaptiveWorkQueue,
        related_tables: list[str],
        worker_stats: dict,
        optimized_storage,
        data_type: str,
    ):
        """Worker function for processing related tables in parallel using pagination."""
        logger.info(f"Worker {worker_id} started")

        # Use the shared key pool
        key_pool = self.key_pool

        try:
            while True:
                # Get next chunk of work (page)
                chunk = await work_queue.get_work(worker_id)
                if chunk is None:
                    logger.info(f"Worker {worker_id} finished - no more work")
                    break

                chunk_id = chunk.chunk_id
                start_offset = chunk.start_offset
                end_offset = chunk.end_offset
                page_size = chunk.page_size

                logger.info(
                    f"Worker {worker_id} processing chunk {chunk_id} (offset: {start_offset}, limit: {end_offset - start_offset})"
                )

                # Query the database for this page of items that need related table fetching
                schema_name = fetcher.get_default_schema()
                table_name = f"{data_type}_raw"

                # Build query to get items for this page
                query = f"""
                SELECT
                    payload,
                    source_doc_id
                FROM {schema_name}.{table_name}
                WHERE payload IS NOT NULL
                """

                params = []
                param_count = 0

                # Add date filters if provided
                if hasattr(fetcher, "from_date") and fetcher.from_date:
                    param_count += 1
                    query += f" AND payload->>'updatedate' >= ${param_count}"
                    params.append(fetcher.from_date)

                if hasattr(fetcher, "to_date") and fetcher.to_date:
                    param_count += 1
                    query += f" AND payload->>'updatedate' <= ${param_count}"
                    params.append(fetcher.to_date)

                # Add related table filtering conditions
                for table in related_tables:
                    if table == "texts":
                        param_count += 1
                        query += " AND (payload->'textVersions' IS NULL OR jsonb_typeof(payload->'textVersions') != 'array' OR jsonb_array_length(payload->'textVersions') = 0)"
                    # Add more table-specific conditions as needed

                query += " ORDER BY payload->>'updatedate' DESC"
                query += f" LIMIT {end_offset - start_offset} OFFSET {start_offset}"

                # Get items for this page
                async with fetcher.db_pool.acquire() as conn:
                    rows = await conn.fetch(query, *params)

                if not rows:
                    logger.info(
                        f"Worker {worker_id}: No items found for chunk {chunk_id}"
                    )
                    await work_queue.complete_work(worker_id, chunk_id, success=True)
                    continue

                logger.info(
                    f"Worker {worker_id}: Processing {len(rows)} items from chunk {chunk_id}"
                )

                # Process each item in the chunk
                for row in rows:
                    try:
                        payload = row["payload"]
                        source_doc_id = row["source_doc_id"]

                        # With rotisserie, create client with key_pool - keys managed automatically
                        client = self.client_class(
                            api_keys=self.api_keys, key_pool=key_pool
                        )

                        async with client:
                            # Fetch related data for this item
                            related_data = await self._fetch_specific_related_tables_for_item_with_client(
                                fetcher, payload, related_tables, client
                            )

                        if related_data:
                            # Store the related data using optimized storage if available
                            if optimized_storage:
                                await optimized_storage.store_phase_3_data(
                                    fetcher.get_default_schema(),
                                    data_type,
                                    related_data,
                                    source_doc_id,
                                )
                            else:
                                # Fallback to direct storage
                                await fetcher.store_phase_3_data(
                                    related_data, source_doc_id
                                )

                            worker_stats["processed"] += 1
                            worker_stats["related_items"] += len(related_data)
                            logger.debug(
                                f"Worker {worker_id} stored related data for {source_doc_id}"
                            )
                        else:
                            logger.debug(
                                f"Worker {worker_id} no related data found for {source_doc_id}"
                            )

                        # Rotisserie handles key management automatically - no need to checkin

                    except Exception as e:
                        logger.error(
                            f"Worker {worker_id} error processing item {source_doc_id}: {e}"
                        )
                        worker_stats["errors"] += 1
                        # Rotisserie handles key management automatically
                        continue

                # Mark chunk as completed
                await work_queue.complete_work(worker_id, chunk_id, success=True)

        except Exception as e:
            logger.error(f"Worker {worker_id} encountered fatal error: {e}")
        finally:
            logger.info(
                f"Worker {worker_id} completed. Processed: {worker_stats['processed']}, Errors: {worker_stats['errors']}"
            )

    async def _fetch_specific_related_tables_for_item_with_client(
        self, fetcher, detailed_data: dict, related_tables: list[str], client
    ) -> list[dict]:
        """Fetch only the specified related tables for a single item using a specific client."""
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
        custom_logic = fetcher._plugin._get_custom_logic_plugin(fetcher.data_type_name)
        if not custom_logic:
            logger.error(
                f"No custom logic plugin available for {fetcher.data_type_name}"
            )
            return []

        # Call only the requested related methods
        for table in related_tables:
            method_name = f"get_{fetcher.data_type_name}_{table}"

            if hasattr(custom_logic, method_name):
                try:
                    method = getattr(custom_logic, method_name)
                    if callable(method):
                        logger.debug(
                            f"Calling {method_name} for {fetcher.data_type_name}"
                        )

                        # Call the method with detailed_data and the specific client
                        result = await method(detailed_data, client)

                        if result:
                            related_data.append(
                                {
                                    "type": method_name,
                                    "data": result,
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

    def get_checkpoint_stats(self, data_type: str) -> dict[str, Any]:
        """
        Get checkpoint statistics for a data type.

        This method provides checkpoint statistics by delegating to the
        hierarchical checkpoint manager if available, or returning empty stats.

        Args:
            data_type: The data type to get stats for

        Returns:
            Dictionary containing checkpoint statistics by phase
        """
        # For now, return empty stats since this processor doesn't directly
        # manage checkpoints - the fetcher does
        logger.warning(
            f"get_checkpoint_stats called for {data_type} but ParallelProcessor doesn't manage checkpoints directly"
        )
        return {}
