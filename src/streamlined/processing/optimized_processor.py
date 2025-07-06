"""
OLD IMPLEMENTATION!!!! Optimized Parallel Processor using dynamic key pool and work queue.

This module implements the optimized processing strategy with dynamic
key allocation and work distribution.
"""

import asyncio
import logging
from datetime import UTC, datetime
from typing import Any

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
        total_count = raw_metadata.get("total_count", 0)
        # CRITICAL: Use the intended processing limit, not the metadata fetch limit
        count_per_page = (
            limit or 250
        )  # raw_metadata count_per_page was for metadata fetch only
        has_data = raw_metadata.get("has_data", False)
        error = raw_metadata.get("error")

        logger.info(f"Total count: {total_count}")
        logger.info(f"Count per page: {count_per_page}")
        logger.info(f"Has data: {has_data}")
        if error:
            logger.error(f"Error in raw metadata: {error}")

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
                limit=count_per_page,
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
        """Process a single work chunk with dynamic key management."""
        processed = 0
        offset = chunk.start_offset

        # Check if we should use optimized storage
        use_optimized_storage = False
        optimized_storage = None

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
                    # Get the id_field from the fetcher's config
                    id_field = getattr(fetcher, "id_field", "parent_id")
                    optimized_storage = OptimizedFetcherStorage(
                        storage_manager, id_field, fetcher
                    )
                    use_optimized_storage = True
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

            # CRITICAL FIX: Keep trying to get a key instead of giving up
            api_key = None
            key_wait_attempts = 0
            max_key_wait_attempts = 10

            while api_key is None and key_wait_attempts < max_key_wait_attempts:
                api_key = await self.key_pool.checkout_key(
                    worker_id, preferred_requests=estimated_requests
                )

                if api_key is None:
                    key_wait_attempts += 1
                    if key_wait_attempts < max_key_wait_attempts:
                        wait_time = min(
                            2**key_wait_attempts, 30
                        )  # Exponential backoff, max 30s
                        logger.info(
                            f"Worker {worker_id} waiting {wait_time}s for API key "
                            f"(attempt {key_wait_attempts}/{max_key_wait_attempts})"
                        )
                        await asyncio.sleep(wait_time)
                    else:
                        logger.error(
                            f"Worker {worker_id} failed to get API key after "
                            f"{max_key_wait_attempts} attempts, abandoning chunk"
                        )
                        worker_stats["errors"] += 1
                        return processed  # Return what we've processed so far

            if not api_key:
                # This shouldn't happen with the above logic, but just in case
                break

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

                        # Process items in batch (Phase 1, 2, 3)
                        for item in batch:
                            try:
                                # Phase 1: Store list data
                                if use_optimized_storage:
                                    await optimized_storage.store_phase_1_data(
                                        fetcher.get_default_schema(),
                                        f"{fetcher.data_type_name}_list_raw",
                                        item,
                                    )
                                else:
                                    await fetcher.store_phase_1_data(item)

                                # Phase 2: Get and store full data
                                url = item.get("url")
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
                                        if use_optimized_storage:
                                            await optimized_storage.store_phase_2_data(
                                                fetcher.get_default_schema(),
                                                f"{fetcher.data_type_name}_raw",
                                                detailed_data,
                                            )
                                        else:
                                            await fetcher.store_phase_2_data(
                                                detailed_data
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

                                            if use_optimized_storage:
                                                await optimized_storage.store_phase_3_data(
                                                    fetcher.get_default_schema(),
                                                    fetcher.data_type_name,
                                                    related_data,
                                                    item_id,
                                                )
                                            else:
                                                await fetcher.store_phase_3_data(
                                                    related_data, item_id
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
                    offset += batch_size

                except Exception as e:
                    logger.error(f"Worker {worker_id} batch processing error: {e}")

                    # Check if rate limited
                    rate_limited = "429" in str(e) or "rate limit" in str(e).lower()
                    await self.key_pool.checkin_key(
                        api_key.key, requests_made, rate_limited
                    )

                    if rate_limited:
                        logger.info(
                            f"Worker {worker_id} hit rate limit, will get new key"
                        )
                        # Continue with next iteration to get a new key
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

        return processed

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
