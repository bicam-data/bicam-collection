"""
Committees data fetcher using the base fetcher architecture.

This module handles:
1. Phase 1: Get committees list from bulk endpoints (/committee, etc.)
2. Phase 2: Get full committee data from individual URLs
3. Phase 3: Get related data (committees, bills, etc.) from URLs
"""

import logging
from typing import Any

import asyncpg

from ....api_clients import CongressionalAPIClient
from ..base import CongressionalBaseFetcher

logger = logging.getLogger(__name__)


class CommitteesFetcher(CongressionalBaseFetcher):
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
            data_type_name="committees",
        )

    # =============================================================================
    # REQUIRED ABSTRACT METHOD IMPLEMENTATIONS
    # =============================================================================

    def extract_item_id(self, item_data: dict[str, Any]) -> str:
        """Extract standardized committee ID from committee data."""
        return item_data.get("systemCode", "ID_ERROR")

    def _extract_preliminary_item_id(self, list_item: dict[str, Any]) -> str:
        """
        Derive a stable ID for checkpointing directly from the list-response record.
        This guarantees a non-null value so the progress-tracker can reliably
        decide whether the item was processed in a previous run.
        """

        return self.extract_item_id(list_item)

    # =============================================================================
    # COMMITTEES-SPECIFIC RELATED DATA METHODS
    # =============================================================================

    async def get_committees_committeereports(
        self, full_committee_data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Get committee reports from reports URL in full committee data."""
        return await self.get_generic_related_data(
            full_committee_data
        )
