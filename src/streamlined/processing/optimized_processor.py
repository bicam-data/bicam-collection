"""
OLD IMPLEMENTATION!!!! Optimized Parallel Processor using dynamic key pool and work queue.

This module implements the optimized processing strategy with dynamic
key allocation and work distribution.
"""

import asyncio
import logging
import random
from datetime import UTC, datetime
from typing import Any

from ..libs.hierarchical_checkpoint_system import (
    HierarchicalCheckpointManager,
    ProcessingStage,
    FetchingPhase,
)
from ..processing.key_pool import DynamicKeyPool
from ..processing.optimized_storage_manager import OptimizedFetcherStorage
from ..processing.work_queue import AdaptiveWorkQueue, WorkChunk

logger = logging.getLogger(__name__)


class OptimizedParallelProcessor:
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
    ):
        self.key_pool = DynamicKeyPool(api_keys)
        self.client_class = client_class
        self.db_pool = db_pool

        # Default to 1.5x keys for workers (e.g., 48 workers for 32 keys)
        self.num_workers = num_workers or int(len(api_keys) * 1.5)
        self.chunk_size = chunk_size

        # Track worker performance
        self._worker_stats = {}

        logger.info("==================================================")
        logger.info("=  OptimizedParallelProcessor INITIALIZED        =")
        logger.info(
            f"=  Workers: {self.num_workers}, Chunk Size: {self.chunk_size}          ="
        )
        logger.info(f"=  API Keys: {len(api_keys)}, Key Pool: DynamicKeyPool    =")
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
            "total_keys": len(self.key_pool._keys),
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
        stats["final_key_status"] = await self.key_pool.get_pool_status()

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
                optimized_storage = OptimizedFetcherStorage(
                    storage_manager, id_field, fetcher
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

            # Use improved key acquisition strategy
            api_key = await self._acquire_api_key_with_smart_backoff(
                worker_id=worker_id,
                key_pool=self.key_pool,
                estimated_requests=estimated_requests,
                chunk=chunk,
                offset=offset,
            )

            worker_stats["keys_used"].add(api_key.key[:8])

            # Create client with this key and use as async context manager
            client = self.client_class(api_keys=[api_key.key], db_pool=self.db_pool)

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

                        # Check if we should switch keys preemptively
                        if (
                            api_key.request_count + requests_made
                            > self.key_pool.rate_limit_threshold - 10
                        ):
                            logger.info(
                                f"Worker {worker_id} preemptively returning key "
                                f"{api_key.key[:8]}... (near limit)"
                            )
                            break

                    # Successfully processed batch, reset failure counter
                    consecutive_failures = 0

                    # Return key to pool
                    await self.key_pool.checkin_key(api_key.key, requests_made)

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

                    # Check if rate limited
                    rate_limited = "429" in str(e) or "rate limit" in str(e).lower()
                    await self.key_pool.checkin_key(
                        api_key.key, requests_made, rate_limited
                    )

                    if rate_limited:
                        logger.info(
                            f"Worker {worker_id} hit rate limit, will get new key and continue chunk"
                        )
                        # Continue with next iteration to get a new key
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

    async def _acquire_api_key_with_smart_backoff(
        self,
        worker_id: str,
        key_pool,
        estimated_requests: int,
        chunk: WorkChunk,
        offset: int,
    ) -> Any:
        """
        Acquire an API key with a smart backoff strategy that balances between:
        - Never abandoning work (must process every item)
        - Not wasting time with excessive waits
        - Minimizing overhead from constant polling

        Strategy:
        1. Start with quick retries (good for transient unavailability)
        2. Move to medium backoff with periodic quick checks
        3. Cap at reasonable maximum wait with jitter
        """

        api_key = None
        attempt = 0
        consecutive_long_waits = 0

        # Configuration
        INITIAL_WAIT = 0.5  # Start with 500ms
        QUICK_CHECK_INTERVAL = 5  # Do a quick check every N long waits
        MAX_WAIT = 30  # Cap at 30 seconds instead of 60
        JITTER_FACTOR = 0.2  # Add ±20% jitter to prevent thundering herd

        while api_key is None:
            # Try to get a key
            api_key = await key_pool.checkout_key(
                worker_id, preferred_requests=estimated_requests
            )

            if api_key is not None:
                # Success! Log if we had to wait
                if attempt > 0:
                    logger.info(
                        f"Worker {worker_id} acquired API key after {attempt} attempts"
                    )
                return api_key

            # No key available - implement smart backoff
            attempt += 1

            # Calculate base wait time with exponential backoff
            if attempt <= 3:
                # First 3 attempts: quick retries (0.5s, 1s, 2s)
                base_wait = INITIAL_WAIT * (2 ** (attempt - 1))
            else:
                # After that: slower backoff capped at MAX_WAIT
                base_wait = min(INITIAL_WAIT * (2 ** (attempt - 1)), MAX_WAIT)

                # Every QUICK_CHECK_INTERVAL long waits, do a quick check
                consecutive_long_waits += 1
                if consecutive_long_waits >= QUICK_CHECK_INTERVAL:
                    logger.info(
                        f"Worker {worker_id} doing quick availability check after "
                        f"{QUICK_CHECK_INTERVAL} long waits"
                    )
                    base_wait = INITIAL_WAIT  # Quick check
                    consecutive_long_waits = 0

            # Add jitter to prevent thundering herd
            jitter = base_wait * JITTER_FACTOR * (2 * random.random() - 1)
            wait_time = max(0.1, base_wait + jitter)  # Never wait less than 100ms

            # Log wait status with useful context
            if attempt == 1:
                logger.info(
                    f"Worker {worker_id} waiting for API key "
                    f"(chunk [{chunk.start_offset}-{chunk.end_offset}] @ offset {offset})"
                )
            elif attempt % 10 == 0:  # Log every 10 attempts to avoid spam
                logger.warning(
                    f"Worker {worker_id} still waiting for API key "
                    f"(attempt {attempt}, wait {wait_time:.1f}s, "
                    f"chunk [{chunk.start_offset}-{chunk.end_offset}] @ offset {offset})"
                )

            await asyncio.sleep(wait_time)

            # Periodically check pool status for debugging
            if attempt % 20 == 0:
                pool_status = await key_pool.get_pool_status()
                available = pool_status["status_by_state"].get("available", 0)
                total = pool_status["total_keys"]
                logger.info(
                    f"Key pool status: {available}/{total} keys available, "
                    f"{pool_status['total_requests_remaining']} requests remaining"
                )

    async def _report_status(self, work_queue: AdaptiveWorkQueue, interval: int = 30):
        """Periodically report processing status."""
        while True:
            try:
                await asyncio.sleep(interval)

                queue_status = work_queue.get_progress()
                pool_status = await self.key_pool.get_pool_status()

                # Calculate available keys from status
                available_keys = pool_status["status_by_state"].get("available", 0)
                total_keys = pool_status["total_keys"]

                logger.info(
                    f"Progress: {queue_status['completion_percentage']:.1f}% "
                    f"({queue_status['completed_chunks']}/{queue_status['total_chunks']} chunks), "
                    f"Keys: {available_keys}/{total_keys} available, "
                    f"Requests remaining: {pool_status['total_requests_remaining']}"
                )

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in status reporting: {e}")

    async def _maintain_key_pool(self, interval: int = 60):
        """Periodically check and reset expired rate limits."""
        while True:
            try:
                await asyncio.sleep(interval)

                reset_count = await self.key_pool.reset_expired_limits()
                if reset_count is not None and reset_count > 0:
                    logger.info(f"Reset {reset_count} expired rate limits")

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
