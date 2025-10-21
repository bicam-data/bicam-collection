"""
Work Queue Manager for distributing work chunks to dynamic workers.

This module implements a work queue that breaks large processing jobs into
smaller chunks for better load balancing across workers.
"""

import asyncio
import logging
from dataclasses import dataclass
from datetime import UTC, datetime

logger = logging.getLogger(__name__)


@dataclass
class WorkChunk:
    """Represents a chunk of work to be processed."""

    chunk_id: str
    start_offset: int
    end_offset: int
    page_size: int = 250
    data_type: str = ""
    metadata: dict = None

    def __post_init__(self):
        """Validate work chunk parameters with enhanced validation."""
        if self.start_offset < 0:
            raise ValueError(f"start_offset must be >= 0, got {self.start_offset}")
        if self.end_offset <= self.start_offset:
            raise ValueError(
                f"end_offset ({self.end_offset}) must be > start_offset ({self.start_offset})"
            )
        if self.page_size <= 0:
            raise ValueError(f"page_size must be > 0, got {self.page_size}")

        # Initialize metadata if not provided
        if self.metadata is None:
            self.metadata = {}

        # Enhanced validation for page alignment
        chunk_size = self.end_offset - self.start_offset

        # Check if chunk is properly sized
        if chunk_size < self.page_size // 4:
            logger.warning(
                f"Chunk {self.chunk_id} is very small ({chunk_size} records, "
                f"< 25% of page size {self.page_size}). This may be inefficient."
            )

        # Check page boundary alignment for better performance
        if chunk_size % self.page_size != 0:
            remainder = chunk_size % self.page_size
            logger.debug(
                f"Chunk {self.chunk_id} has partial page: {chunk_size} records "
                f"({chunk_size // self.page_size} full pages + {remainder} records)"
            )

        # Add alignment metadata
        self.metadata.update(
            {
                "chunk_size": chunk_size,
                "is_page_aligned": chunk_size % self.page_size == 0,
                "full_pages": chunk_size // self.page_size,
                "partial_page_records": chunk_size % self.page_size,
                "alignment_efficiency": (chunk_size // self.page_size)
                * self.page_size
                / chunk_size,
            }
        )

    @property
    def num_pages(self) -> int:
        """Calculate number of pages in this chunk."""
        return max(
            1,
            (self.end_offset - self.start_offset + self.page_size - 1)
            // self.page_size,
        )

    @property
    def estimated_requests(self) -> int:
        """Estimate number of API requests needed for this chunk."""
        # Assuming 1 request per page for list data
        # Could be adjusted based on historical data
        return self.num_pages

    @property
    def total_records(self) -> int:
        """Total number of records in this chunk."""
        return self.end_offset - self.start_offset


class WorkQueue:
    """
    Manages work distribution for parallel processing.

    Features:
    - Dynamic chunk size adjustment
    - Work stealing for load balancing
    - Progress tracking per chunk
    - Failed chunk retry logic
    """

    def __init__(
        self, total_records: int, chunk_size: int = 5000, page_size: int = 250
    ):
        self.total_records = total_records
        self.chunk_size = chunk_size
        self.page_size = page_size

        self._queue = asyncio.Queue()
        self._in_progress = {}  # chunk_id -> (worker_id, start_time)
        self._completed = set()
        self._failed = {}  # chunk_id -> failure_count

        self._lock = asyncio.Lock()

        # Initialize work chunks
        self._create_work_chunks()

        logger.info(
            f"Initialized work queue: {total_records} records, "
            f"{self._queue.qsize()} chunks of ~{chunk_size} records"
        )

    def _create_work_chunks(self):
        """Create initial work chunks with proper API page boundary alignment."""
        chunk_id = 0
        offset = 0

        # Validate that chunk size is reasonable relative to page size
        if self.chunk_size < self.page_size:
            logger.warning(
                f"Chunk size ({self.chunk_size}) is smaller than page size ({self.page_size}). "
                f"Adjusting chunk size to page size for efficiency."
            )
            self.chunk_size = self.page_size

        while offset < self.total_records:
            # Calculate optimal chunk size that aligns with page boundaries
            remaining_records = self.total_records - offset
            target_chunk_size = min(self.chunk_size, remaining_records)

            # Calculate the number of complete pages needed for this chunk
            pages_needed = max(
                1, (target_chunk_size + self.page_size - 1) // self.page_size
            )

            # Calculate the actual end offset (aligned to page boundaries)
            aligned_end_offset = min(
                offset + (pages_needed * self.page_size), self.total_records
            )

            # Ensure we don't create chunks that are too small (unless it's the last chunk)
            min_chunk_size = self.page_size // 2  # Minimum 50% of page size
            if (
                aligned_end_offset - offset
            ) < min_chunk_size and aligned_end_offset < self.total_records:
                # Merge with next chunk by extending to next page boundary
                pages_needed += 1
                aligned_end_offset = min(
                    offset + (pages_needed * self.page_size), self.total_records
                )

            chunk = WorkChunk(
                chunk_id=f"chunk_{chunk_id}",
                start_offset=offset,
                end_offset=aligned_end_offset,
                page_size=self.page_size,
            )

            # Validate chunk alignment
            if not self._validate_chunk_alignment(chunk):
                logger.error(f"Chunk {chunk_id} failed alignment validation")
                raise ValueError(f"Chunk {chunk_id} has invalid alignment")

            self._queue.put_nowait(chunk)

            offset = aligned_end_offset
            chunk_id += 1

            logger.debug(
                f"Created chunk {chunk_id - 1}: offset {chunk.start_offset}-{chunk.end_offset} "
                f"({chunk.num_pages} pages, {chunk.total_records} records)"
            )

        logger.info(
            f"Created {chunk_id} work chunks, all properly aligned to page boundaries"
        )

    def _validate_chunk_alignment(self, chunk: WorkChunk) -> bool:
        """Validate that a chunk is properly aligned with API page boundaries."""
        # Check that chunk size is a multiple of page size (except for the last chunk)
        chunk_size = chunk.end_offset - chunk.start_offset

        # For chunks that don't end at the total records, they should be page-aligned
        if chunk.end_offset < self.total_records:
            if chunk_size % self.page_size != 0:
                logger.warning(
                    f"Chunk not aligned: size {chunk_size} not multiple of page size {self.page_size}"
                )
                return False

        # Check that start offset is page-aligned (except for first chunk)
        if chunk.start_offset > 0 and chunk.start_offset % self.page_size != 0:
            logger.warning(
                f"Chunk start offset {chunk.start_offset} not aligned to page size {self.page_size}"
            )
            return False

        # Check minimum chunk size
        if chunk_size < self.page_size // 2 and chunk.end_offset < self.total_records:
            logger.warning(
                f"Chunk size {chunk_size} is too small (minimum: {self.page_size // 2})"
            )
            return False

        return True

    async def get_work(self, worker_id: str) -> WorkChunk | None:
        """
        Get next work chunk for a worker.

        Args:
            worker_id: Unique identifier for the worker

        Returns:
            WorkChunk or None if no work available
        """
        try:
            # Try to get work from queue
            chunk = await asyncio.wait_for(self._queue.get(), timeout=1.0)

            async with self._lock:
                self._in_progress[chunk.chunk_id] = (worker_id, datetime.now(UTC))

            logger.debug(
                f"Worker {worker_id} got {chunk.chunk_id} "
                f"(records {chunk.start_offset}-{chunk.end_offset})"
            )
            return chunk

        except TimeoutError:
            # Check for stalled work that can be stolen
            stolen_chunk = await self._steal_stalled_work(worker_id)
            if stolen_chunk:
                return stolen_chunk

            # No work available
            return None

    async def _steal_stalled_work(
        self, worker_id: str, stall_timeout: int = 300
    ) -> WorkChunk | None:
        """Steal work from stalled workers."""
        async with self._lock:
            now = datetime.now(UTC)
            stalled_chunks = []

            for chunk_id, (_assigned_worker, start_time) in self._in_progress.items():
                if (now - start_time).total_seconds() > stall_timeout:
                    stalled_chunks.append(chunk_id)

            if not stalled_chunks:
                return None

            # Steal the oldest stalled chunk
            chunk_id = stalled_chunks[0]
            old_worker = self._in_progress[chunk_id][0]

            # Re-create the chunk (we'd need to store these for real implementation)
            # For now, parse from chunk_id
            chunk_num = int(chunk_id.split("_")[1])
            chunk = WorkChunk(
                chunk_id=chunk_id,
                start_offset=chunk_num * self.chunk_size,
                end_offset=min((chunk_num + 1) * self.chunk_size, self.total_records),
                page_size=self.page_size,
            )

            self._in_progress[chunk_id] = (worker_id, now)

            logger.warning(
                f"Worker {worker_id} stole {chunk_id} from stalled worker {old_worker}"
            )
            return chunk

    async def complete_work(self, worker_id: str, chunk_id: str, success: bool = True):
        """
        Mark work chunk as completed.

        Args:
            worker_id: Worker that completed the chunk
            chunk_id: ID of completed chunk
            success: Whether processing was successful
        """
        async with self._lock:
            if chunk_id in self._in_progress:
                del self._in_progress[chunk_id]

            if success:
                self._completed.add(chunk_id)
                logger.debug(f"Worker {worker_id} completed {chunk_id}")
            else:
                # Track failures and retry if under threshold
                failure_count = self._failed.get(chunk_id, 0) + 1
                self._failed[chunk_id] = failure_count

                if failure_count < 3:
                    # Re-queue for retry
                    chunk_num = int(chunk_id.split("_")[1])
                    chunk = WorkChunk(
                        chunk_id=chunk_id,
                        start_offset=chunk_num * self.chunk_size,
                        end_offset=min(
                            (chunk_num + 1) * self.chunk_size, self.total_records
                        ),
                        page_size=self.page_size,
                    )
                    await self._queue.put(chunk)
                    logger.warning(
                        f"Re-queued {chunk_id} after failure #{failure_count}"
                    )
                else:
                    logger.error(
                        f"Chunk {chunk_id} failed {failure_count} times, abandoning"
                    )

    def get_progress(self) -> dict:
        """Get current processing progress."""
        total_chunks = (
            self._queue.qsize() + len(self._in_progress) + len(self._completed)
        )

        return {
            "total_chunks": total_chunks,
            "queued": self._queue.qsize(),
            "in_progress": len(self._in_progress),
            "completed": len(self._completed),
            "failed": len([c for c, count in self._failed.items() if count >= 3]),
            "completion_percent": (len(self._completed) / total_chunks * 100)
            if total_chunks > 0
            else 0,
            "estimated_records_processed": len(self._completed) * self.chunk_size,
        }

    def is_complete(self) -> bool:
        """Check if all work is complete."""
        return (
            self._queue.empty()
            and len(self._in_progress) == 0
            and len(self._completed) > 0
        )


class AdaptiveWorkQueue(WorkQueue):
    """
    Enhanced work queue that adapts chunk sizes based on performance.
    """

    def __init__(
        self, total_records: int, initial_chunk_size: int = 5000, page_size: int = 250
    ):
        super().__init__(total_records, initial_chunk_size, page_size)

        self._performance_history = []  # List of (chunk_size, duration, success)
        self._current_chunk_size = initial_chunk_size

        logger.info("==================================================")
        logger.info("=      AdaptiveWorkQueue INITIALIZED             =")
        logger.info("==================================================")

    async def complete_work(
        self,
        worker_id: str,
        chunk_id: str,
        success: bool = True,
        duration: float | None = None,
    ):
        """Complete work and track performance for adaptation."""
        await super().complete_work(worker_id, chunk_id, success)

        if duration and success:
            async with self._lock:
                self._performance_history.append(
                    (self._current_chunk_size, duration, success)
                )

                # Adapt chunk size based on recent performance
                if len(self._performance_history) >= 10:
                    self._adapt_chunk_size()

    def _adapt_chunk_size(self):
        """Adapt chunk size based on recent performance."""
        recent = self._performance_history[-10:]
        avg_duration = sum(d for _, d, _ in recent) / len(recent)

        # Target: chunks should take 30-60 seconds
        if avg_duration < 30:
            # Chunks completing too fast, increase size
            self._current_chunk_size = min(self._current_chunk_size * 1.5, 20000)
            logger.info(f"Increased chunk size to {self._current_chunk_size}")
        elif avg_duration > 60:
            # Chunks taking too long, decrease size
            self._current_chunk_size = max(self._current_chunk_size * 0.7, 1000)
            logger.info(f"Decreased chunk size to {self._current_chunk_size}")
