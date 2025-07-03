"""
Committe meetings data fetcher using the base fetcher architecture.

This module handles:
1. Phase 1: Get committe meetings list from bulk endpoints (/committee, etc.)
2. Phase 2: Get full committe meeting data from individual URLs
3. Phase 3: Get related data (committe meetings, bills, etc.) from URLs
"""

import logging
from typing import Any

import asyncpg

from ....api_clients import CongressionalAPIClient
from ..base import CongressionalBaseFetcher

logger = logging.getLogger(__name__)


class CommitteemeetingsFetcher(CongressionalBaseFetcher):
    """
    Fetcher for Congressional committees data using the 3-phase API approach.
    Inherits from CongressionalBaseFetcher for standardized processing pipeline.
    """

    def __init__(
        self,
        client: CongressionalAPIClient,
        db_pool: asyncpg.Pool | None = None,
        checkpoint_manager=None,
        run_manager=None,
    ):
        super().__init__(
            client=client,
            db_pool=db_pool,
            checkpoint_manager=checkpoint_manager,
            run_manager=run_manager,
            data_type_name="committeemeetings",
        )

    # =============================================================================
    # REQUIRED ABSTRACT METHOD IMPLEMENTATIONS
    # =============================================================================

    def extract_item_id(self, item_data: dict[str, Any]) -> str:
        """Extract standardized committe meeting ID from committe meeting data."""
        return item_data.get("eventId", "ID_ERROR")

    def _extract_preliminary_item_id(self, list_item: dict[str, Any]) -> str:
        """
        Derive a stable ID for checkpointing directly from the list-response record.
        This guarantees a non-null value so the progress-tracker can reliably
        decide whether the item was processed in a previous run.
        """
        return self.extract_item_id(list_item)

    # =============================================================================
    # COMMITTEE MEETINGS-SPECIFIC RELATED DATA METHODS
    # =============================================================================
