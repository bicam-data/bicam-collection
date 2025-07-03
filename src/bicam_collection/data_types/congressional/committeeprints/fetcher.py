"""
Committeeprints data fetcher using the base fetcher architecture.

This module handles:
1. Phase 1: Get committeeprints list from bulk endpoints (/committeeprint, etc.)
2. Phase 2: Get full committeeprint data from individual URLs
3. Phase 3: Get related data (committees, bills, etc.) from URLs
"""

import logging
from typing import Any

import asyncpg

from ....api_clients import CongressionalAPIClient
from ..base import CongressionalBaseFetcher

logger = logging.getLogger(__name__)


class CommitteeprintsFetcher(CongressionalBaseFetcher):
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
            data_type_name="committeeprints",
        )

    # =============================================================================
    # REQUIRED ABSTRACT METHOD IMPLEMENTATIONS
    # =============================================================================

    def extract_item_id(self, item_data: dict[str, Any]) -> str:
        """Extract standardized committeeprint ID from committeeprint data."""
        item_data = item_data[0] if isinstance(item_data, list) else item_data
        jacket_number = item_data.get("jacketNumber")
        chamber = item_data.get("chamber")
        chamber_prefix = "j" if chamber == "NoChamber" else chamber[0].lower()

        congress = item_data.get("congress")
        if all([jacket_number, chamber, congress]):
            return f"{chamber_prefix}prt{jacket_number}-{congress}"
        else:
            null_values = {
                "jacket_number": jacket_number,
                "chamber": chamber,
                "congress": congress,
            }
            logger.info(
                f"Error extracting item ID: {item_data} with null values: {null_values}"
            )
            return "ID_ERROR"

    def _extract_preliminary_item_id(self, list_item: dict[str, Any]) -> str:
        """
        Derive a stable ID for checkpointing directly from the list-response record.
        This guarantees a non-null value so the progress-tracker can reliably
        decide whether the item was processed in a previous run.
        """
        return self.extract_item_id(list_item)

    # =============================================================================
    # COMMITTEEPRINTS-SPECIFIC RELATED DATA METHODS
    # =============================================================================

    async def get_committeeprints_texts(
        self, full_committeeprint_data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Get texts from texts URL in full committeeprint data."""
        return await self.get_generic_related_data(
            full_committeeprint_data
        )
