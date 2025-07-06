"""
Hearings data fetcher using the base fetcher architecture.

This module handles:
1. Phase 1: Get hearings list from bulk endpoints (/hearings, etc.)
2. Phase 2: Get full hearing data from individual URLs
3. Phase 3: Get related data (committees, bills, etc.) from URLs
"""

import logging
from typing import Any

import asyncpg

from ....api_clients import CongressionalAPIClient
from ..base import CongressionalBaseFetcher

logger = logging.getLogger(__name__)


class HearingsFetcher(CongressionalBaseFetcher):
    """
    Fetcher for Congressional hearings data using the 3-phase API approach.
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
            data_type_name="hearings",
        )

    # =============================================================================
    # REQUIRED ABSTRACT METHOD IMPLEMENTATIONS
    # =============================================================================

    def extract_item_id(self, item_data: dict[str, Any]) -> str:
        """Extract standardized hearing ID from hearing data."""
        hearing_chamber = item_data.get("chamber")
        if hearing_chamber == "NoChamber":
            hearing_chamber_prefix = "j"
        else:
            hearing_chamber_prefix = hearing_chamber[0].lower()
        hearing_number = item_data.get("jacketNumber")
        hearing_congress = item_data.get("congress")
        if all([hearing_chamber_prefix, hearing_number, hearing_congress]):
            return f"{hearing_chamber_prefix}hrg{hearing_number}-{hearing_congress}"
        else:
            return "ID_ERROR"

    def _extract_preliminary_item_id(self, list_item: dict[str, Any]) -> str:
        """
        Derive a stable ID for checkpointing directly from the list-response record.
        This guarantees a non-null value so the progress-tracker can reliably
        decide whether the item was processed in a previous run.
        """

        return self.extract_item_id(list_item)

    # =============================================================================
    # HEARINGS-SPECIFIC RELATED DATA METHODS
    # =============================================================================
