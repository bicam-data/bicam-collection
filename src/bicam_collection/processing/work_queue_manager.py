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

    @property
    def num_pages(self) -> int:
        """Calculate number of pages in this chunk."""
        return max(1, (self.end_offset - self.start_offset) // self.page_size)

    @property
    def estimated_requests(self) -> int:
        """Estimate number of API requests needed for this chunk."""
        # Assuming 1 request per page for list data
        # Could be adjusted based on historical data
        return self.num_pages


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
        """Create initial work chunks."""
        chunk_id = 0
        offset = 0

        while offset < self.total_records:
            chunk = WorkChunk(
                chunk_id=f"chunk_{chunk_id}",
                start_offset=offset,
                end_offset=min(offset + self.chunk_size, self.total_records),
                page_size=self.page_size,
            )
            self._queue.put_nowait(chunk)

            offset += self.chunk_size
            chunk_id += 1

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
