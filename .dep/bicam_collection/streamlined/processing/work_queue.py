"""
Adaptive Work Queue for Streamlined Processing

This module provides an adaptive work queue that efficiently distributes
work across parallel workers with dynamic chunk sizing.
"""

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Any

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
        """Validate work chunk parameters."""
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

        # Calculate chunk properties
        chunk_size = self.end_offset - self.start_offset
        self.metadata.update(
            {
                "chunk_size": chunk_size,
                "is_page_aligned": chunk_size % self.page_size == 0,
                "full_pages": chunk_size // self.page_size,
                "partial_page_records": chunk_size % self.page_size,
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
        return self.num_pages

    @property
    def total_records(self) -> int:
        """Total number of records in this chunk."""
        return self.end_offset - self.start_offset


class AdaptiveWorkQueue:
    """Adaptive work queue with dynamic chunk sizing."""

    def __init__(
        self, total_records: int, initial_chunk_size: int = 5000, page_size: int = 250
    ):
        self.total_records = total_records
        self.page_size = page_size
        self.chunk_size = initial_chunk_size

        # Queue state
        self._work_queue: asyncio.Queue = asyncio.Queue()
        self._in_progress: dict[str, dict] = {}
        self._completed: set[str] = set()
        self._failed: set[str] = set()

        # Performance tracking
        self._chunk_times: list[float] = []
        self._last_adaptation = time.time()
        self._adaptation_interval = 60  # seconds

        # Initialize work chunks
        self._create_work_chunks()

        logger.info(
            f"Created adaptive work queue: {total_records} records, "
            f"{self._work_queue.qsize()} chunks, {initial_chunk_size} chunk size"
        )

    def _create_work_chunks(self):
        """Create work chunks based on total records and chunk size."""
        chunks_created = 0

        for start in range(0, self.total_records, self.chunk_size):
            end = min(start + self.chunk_size, self.total_records)

            chunk = WorkChunk(
                chunk_id=f"chunk_{chunks_created}",
                start_offset=start,
                end_offset=end,
                page_size=self.page_size,
            )

            try:
                self._work_queue.put_nowait(chunk)
                chunks_created += 1
            except asyncio.QueueFull:
                logger.warning("Work queue is full, stopping chunk creation")
                break

        logger.info(f"Created {chunks_created} work chunks")

    async def get_work(self, worker_id: str) -> WorkChunk | None:
        """Get next work chunk for a worker."""
        try:
            # Try to get work from queue
            chunk = await asyncio.wait_for(self._work_queue.get(), timeout=1.0)

            # Track as in progress
            self._in_progress[chunk.chunk_id] = {
                "worker_id": worker_id,
                "start_time": time.time(),
                "chunk": chunk,
            }

            logger.debug(f"Assigned {chunk.chunk_id} to {worker_id}")
            return chunk

        except TimeoutError:
            # No work available
            return None
        except Exception as e:
            logger.error(f"Error getting work for {worker_id}: {e}")
            return None

    async def complete_work(
        self,
        worker_id: str,
        chunk_id: str,
        success: bool = True,
        duration: float | None = None,
    ):
        """Mark work chunk as completed."""
        if chunk_id not in self._in_progress:
            logger.warning(f"Chunk {chunk_id} not found in progress for {worker_id}")
            return

        # Remove from in progress
        chunk_info = self._in_progress.pop(chunk_id)

        if success:
            self._completed.add(chunk_id)

            # Track timing for adaptation
            if duration is None:
                duration = time.time() - chunk_info["start_time"]

            self._chunk_times.append(duration)

            # Adapt chunk size if needed
            if time.time() - self._last_adaptation > self._adaptation_interval:
                self._adapt_chunk_size()

            logger.debug(f"Completed {chunk_id} by {worker_id} in {duration:.1f}s")
        else:
            self._failed.add(chunk_id)

            # Re-queue failed work for retry
            try:
                await self._work_queue.put(chunk_info["chunk"])
                logger.info(f"Re-queued failed chunk {chunk_id}")
            except Exception as e:
                logger.error(f"Error re-queueing failed chunk {chunk_id}: {e}")

    def _adapt_chunk_size(self):
        """Adapt chunk size based on performance metrics."""
        if len(self._chunk_times) < 10:
            return  # Not enough data

        # Calculate recent average time
        recent_times = self._chunk_times[-20:]  # Last 20 chunks
        avg_time = sum(recent_times) / len(recent_times)

        # Target 30-60 seconds per chunk
        _target_time = 45.0

        if avg_time < 20:
            # Chunks too fast, increase size
            new_size = min(self.chunk_size * 1.5, 20000)
            logger.info(f"Increasing chunk size: {self.chunk_size} -> {new_size}")
            self.chunk_size = int(new_size)
        elif avg_time > 90:
            # Chunks too slow, decrease size
            new_size = max(self.chunk_size * 0.7, 1000)
            logger.info(f"Decreasing chunk size: {self.chunk_size} -> {new_size}")
            self.chunk_size = int(new_size)

        self._last_adaptation = time.time()

        # Keep only recent times
        self._chunk_times = self._chunk_times[-50:]

    def get_progress(self) -> dict[str, Any]:
        """Get current progress statistics."""
        total_chunks = (
            len(self._completed)
            + len(self._failed)
            + len(self._in_progress)
            + self._work_queue.qsize()
        )

        return {
            "total_records": self.total_records,
            "total_chunks": total_chunks,
            "completed_chunks": len(self._completed),
            "failed_chunks": len(self._failed),
            "in_progress_chunks": len(self._in_progress),
            "queued_chunks": self._work_queue.qsize(),
            "completion_percentage": len(self._completed) / total_chunks * 100
            if total_chunks > 0
            else 0,
            "current_chunk_size": self.chunk_size,
            "average_chunk_time": sum(self._chunk_times[-10:])
            / len(self._chunk_times[-10:])
            if self._chunk_times
            else 0,
        }

    def is_complete(self) -> bool:
        """Check if all work is complete."""
        return (
            self._work_queue.qsize() == 0
            and len(self._in_progress) == 0
            and len(self._failed) == 0
        )

    def has_work_remaining(self) -> bool:
        """Check if there's still work to be done."""
        return self._work_queue.qsize() > 0 or len(self._in_progress) > 0
