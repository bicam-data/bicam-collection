"""
Optimized Parallel Processor using dynamic key pool and work queue.

This module implements the optimized processing strategy with dynamic
key allocation and work distribution.
"""

import asyncio
import logging
from datetime import UTC, datetime
from typing import Any

from ..processing.dynamic_key_pool_manager import DynamicKeyPool
from ..processing.work_queue_manager import AdaptiveWorkQueue, WorkChunk

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

        Args:
            fetcher: The fetcher instance (e.g., CongressionalBaseFetcher)
            data_type: Type of data to process
            from_date: Start date
            to_date: End date
            limit: Page size
            **kwargs: Additional parameters

        Returns:
            Processing statistics
        """
        start_time = datetime.now(UTC)

        # =============================================================================
        # STEP 1: FETCH RAW PAGINATION METADATA
        # =============================================================================
        logger.info("=" * 60)
        logger.info(f"PAGINATION ANALYSIS FOR {data_type.upper()}")
        logger.info("=" * 60)
        logger.info(
            f"Parameters: from_date={from_date}, to_date={to_date}, limit={limit}"
        )

        # Get raw pagination metadata from fetcher
        logger.info("Fetching raw pagination metadata from API...")
        raw_metadata = await fetcher._get_pagination_metadata(
            from_date, to_date, limit, **kwargs
        )

        logger.info(f"Raw metadata received: {raw_metadata}")

        # =============================================================================
        # STEP 2: ANALYZE PAGINATION METADATA
        # =============================================================================
        logger.info("-" * 40)
        logger.info("ANALYZING PAGINATION METADATA")
        logger.info("-" * 40)

        # Extract basic info
        total_count = raw_metadata.get("total_count", 0)
        count_per_page = raw_metadata.get("count_per_page", limit)
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

        incremental_processing = last_processed_count > 0 and total_count > last_processed_count

        logger.info(f"Last processed count: {last_processed_count}")
        logger.info(f"Current total count: {total_count}")
        logger.info(f"Incremental processing: {incremental_processing}")

        if incremental_processing:
            # Calculate exact pages needed based on new records
            new_records = total_count - last_processed_count
            pages_to_process = (new_records + count_per_page - 1) // count_per_page
            logger.info(f"New records since last run: {new_records}")
            logger.info(f"Pages needed for new records: {pages_to_process}")
        else:
            pages_to_process = total_pages
            logger.info("Full processing (no incremental data or first run)")

        logger.info(f"Final pages to process: {pages_to_process}")

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
        logger.info(f"Page size: {limit}")

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

        # Initialize work queue
        work_queue = AdaptiveWorkQueue(
            total_records=total_records,
            initial_chunk_size=self.chunk_size,
            page_size=limit,
        )

        logger.info(f"Work queue initialized with {total_records} total records")

        # =============================================================================
        # STEP 6: CREATE AND START WORKERS
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
                limit=limit,
                **kwargs,
            )
            workers.append(worker)

        # Start periodic status reporting
        status_task = asyncio.create_task(self._report_status(work_queue))

        # Start key pool maintenance
        maintenance_task = asyncio.create_task(self._maintain_key_pool())

        # Run all workers
        logger.info(f"Starting {len(workers)} workers for {data_type}...")
        worker_results = await asyncio.gather(*workers, return_exceptions=True)

        # Cancel status reporting
        status_task.cancel()
        maintenance_task.cancel()

        # =============================================================================
        # STEP 7: AGGREGATE RESULTS
        # =============================================================================
        logger.info("-" * 40)
        logger.info("AGGREGATING RESULTS")
        logger.info("-" * 40)

        # Aggregate results
        stats = {
            "data_type": data_type,
            "total_workers": len(workers),
            "total_keys": len(self.key_pool.keys),
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
        stats["final_key_status"] = self.key_pool.get_pool_status()

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

        while offset < chunk.end_offset:
            # Calculate requests needed for this batch
            remaining_records = chunk.end_offset - offset
            batch_size = min(limit, remaining_records)
            estimated_requests = max(1, batch_size // limit)  # Pages needed

            # Get API key from pool
            api_key = await self.key_pool.checkout_key(worker_id, estimated_requests)
            if not api_key:
                logger.error(f"Worker {worker_id} couldn't get API key")
                break

            worker_stats["keys_used"].add(api_key.key[:8])

            # Create client with this key
            client = self.client_class(api_keys=[api_key.key], db_pool=self.db_pool)

            try:
                # Process this batch
                batch_processed = 0
                requests_made = 0

                async for batch in fetcher.fetch_phase_1_data_with_client(
                    client,
                    from_date=from_date,
                    to_date=to_date,
                    limit=batch_size,
                    offset=offset,
                    **kwargs,
                ):
                    requests_made += 1

                    # Process items in batch (Phase 1, 2, 3)
                    for item in batch:
                        try:
                            # Phase 1: Store list data
                            await fetcher.store_phase_1_data(item)
                            print(f"Item: {item}")

                            # Phase 2: Get and store full data
                            url = item.get("url")
                            if url:
                                detailed_data = (
                                    await fetcher.fetch_phase_2_data_with_client(
                                        url, client
                                    )
                                )
                                requests_made += 1

                                if detailed_data:
                                    await fetcher.store_phase_2_data(detailed_data)

                                    # Phase 3: Get related data
                                    related_data = (
                                        await fetcher.fetch_phase_3_data_with_client(
                                            detailed_data, client
                                        )
                                    )
                                    if related_data:
                                        requests_made += len(
                                            related_data
                                        )  # Approximate
                                        item_id = fetcher.extract_item_id(detailed_data)
                                        await fetcher.store_phase_3_data(
                                            related_data, item_id
                                        )

                            batch_processed += 1
                            processed += 1

                        except Exception as e:
                            logger.error(f"Error processing item: {e}", exc_info=True)
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

                # Return key to pool
                await self.key_pool.checkin_key(api_key.key, requests_made)

                # Update offset
                offset += batch_processed

            except Exception as e:
                logger.error(f"Worker {worker_id} batch processing error: {e}")

                # Check if rate limited
                rate_limited = "429" in str(e) or "rate limit" in str(e).lower()
                await self.key_pool.checkin_key(
                    api_key.key, requests_made, rate_limited
                )

                if rate_limited:
                    # Don't break, just continue with a new key
                    logger.info(f"Worker {worker_id} hit rate limit, will get new key")
                else:
                    raise

            finally:
                # Ensure client is cleaned up
                if hasattr(client, "__aexit__"):
                    await client.__aexit__(None, None, None)

        return processed

    async def _report_status(self, work_queue: AdaptiveWorkQueue, interval: int = 30):
        """Periodically report processing status."""
        while True:
            try:
                await asyncio.sleep(interval)

                queue_status = work_queue.get_progress()
                pool_status = self.key_pool.get_pool_status()

                logger.info(
                    f"Progress: {queue_status['completion_percent']:.1f}% "
                    f"({queue_status['completed']}/{queue_status['total_chunks']} chunks), "
                    f"Keys: {pool_status['available']}/{pool_status['total_keys']} available, "
                    f"Capacity: {pool_status['capacity_used_percent']:.1f}% used"
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
                if reset_count > 0:
                    logger.info(f"Reset {reset_count} expired rate limits")

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in key pool maintenance: {e}")

    # =============================================================================
    # INCREMENTAL PROCESSING ANALYSIS
    # =============================================================================

    async def _get_last_processed_count(self, fetcher) -> dict:
        """Get the last processed date and total count from the database for incremental processing."""
        if not fetcher.client or not hasattr(
            fetcher.client, "access_last_processed_count"
        ):
            return {}

        try:
            last_processed_count = await fetcher.client.access_last_processed_count(
                fetcher.data_type_name
            )
            if last_processed_count:
                logger.info(f"Found last processed count: {last_processed_count}")
            return last_processed_count
        except Exception as e:
            logger.debug(f"Could not get last processed info: {e}")
            return {}
