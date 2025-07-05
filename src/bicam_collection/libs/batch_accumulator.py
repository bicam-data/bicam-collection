"""Shared BatchAccumulator utility.

This file is copied from *data_types/.abstract_dep/batch_accumulator.py* so that
callers can keep importing :pymod:`bicam_collection.libs.batch_accumulator`.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class BatchAccumulator:  # noqa: D101 – simple data holder
    """Accumulates records for bulk database operations.

    The accumulator groups records per *table* and tracks the *record_type*
    (``main``, ``related``, ``nested``…) so that the caller can decide how to
    flush the batch to the database.
    """

    max_batch_size: int = 500
    max_memory_mb: int = 100

    # Internal caches (initialised by __post_init__)
    records_by_table: dict[str, list[dict[str, Any]]] = None  # type: ignore
    record_types_by_table: dict[str, str] = None  # type: ignore

    def __post_init__(self):
        self.records_by_table = defaultdict(list)
        self.record_types_by_table = {}

    # ------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------
    def add_records(
        self, table_records: dict[str, list[dict[str, Any]]], record_type: str
    ) -> None:
        """Add *table_records* (produced during *record_type*) to accumulator."""
        for table, records in table_records.items():
            self.records_by_table[table].extend(records)
            self.record_types_by_table[table] = record_type

    def should_flush(self) -> bool:
        """Return *True* if accumulated data should be flushed."""
        if not self.records_by_table:
            return False

        if max(len(r) for r in self.records_by_table.values()) >= self.max_batch_size:
            logger.debug("BatchAccumulator: triggering flush by batch size")
            return True

        total_records = sum(len(r) for r in self.records_by_table.values())
        estimated_mb = (total_records * 2) / 1000  # ~2 kB / record heuristic
        if estimated_mb >= self.max_memory_mb:
            logger.debug("BatchAccumulator: triggering flush by memory budget")
            return True
        return False

    def get_and_clear(self) -> tuple[dict[str, list[dict[str, Any]]], dict[str, str]]:
        """Return accumulated data and clear internal structures."""
        recs, types = dict(self.records_by_table), dict(self.record_types_by_table)
        self.records_by_table.clear()
        self.record_types_by_table.clear()
        return recs, types

    def is_empty(self) -> bool:  # noqa: D401 – convenience wrapper
        return not self.records_by_table
