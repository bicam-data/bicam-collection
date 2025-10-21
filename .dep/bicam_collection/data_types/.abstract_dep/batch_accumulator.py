from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class BatchAccumulator:
    """Accumulates records across processing sessions for bulk database operations.

    The accumulator groups records per table name and keeps an indication of the
    *type* of records being stored (``main``, ``related`` or ``nested``).  When a
    certain threshold is reached – either *max_batch_size* for any single table
    or a rough *max_memory_mb* estimate – the caller should *flush* the
    accumulated data to the database and start afresh.
    """

    records_by_table: dict[str, list[dict[str, Any]]]
    record_types_by_table: dict[str, str]
    max_batch_size: int = 500
    max_memory_mb: int = 100

    def __init__(self, max_batch_size: int = 500, max_memory_mb: int = 100) -> None:
        self.records_by_table = defaultdict(list)
        self.record_types_by_table = {}
        self.max_batch_size = max_batch_size
        self.max_memory_mb = max_memory_mb

    # ---------------------------------------------------------------------
    # Public helpers
    # ---------------------------------------------------------------------
    def add_records(
        self, table_records: dict[str, list[dict[str, Any]]], record_type: str
    ) -> None:
        """Add *table_records* to the accumulator.

        *table_records* must be a mapping from table name ➔ list[record dict].
        The *record_type* argument denotes which pipeline phase the records
        originate from and is kept so that, upon flushing, the caller can make
        decisions based on this information.
        """
        for table_name, records in table_records.items():
            self.records_by_table[table_name].extend(records)
            self.record_types_by_table[table_name] = record_type

    def should_flush(self) -> bool:
        """Return *True* if the current batch should be flushed.*"""
        if not self.records_by_table:
            return False

        # Trigger flush when any individual table exceeds *max_batch_size*
        if max(len(r) for r in self.records_by_table.values()) >= self.max_batch_size:
            logger.debug("BatchAccumulator: flush triggered by batch size")
            return True

        # Estimate memory footprint very roughly (2kB per record heuristic)
        total_records = sum(len(r) for r in self.records_by_table.values())
        estimated_mb = (total_records * 2) / 1000
        if estimated_mb >= self.max_memory_mb:
            logger.debug("BatchAccumulator: flush triggered by memory budget")
            return True

        return False

    def get_and_clear(
        self,
    ) -> tuple[dict[str, list[dict[str, Any]]], dict[str, str]]:
        """Return the accumulated records and clear the internal structures."""
        records, record_types = (
            dict(self.records_by_table),
            dict(self.record_types_by_table),
        )
        self.records_by_table.clear()
        self.record_types_by_table.clear()
        return records, record_types

    def is_empty(self) -> bool:
        """Convenience wrapper."""
        return not self.records_by_table
