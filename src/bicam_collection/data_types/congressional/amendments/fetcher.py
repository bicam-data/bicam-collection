"""
Amendments data fetcher using the base fetcher architecture.

This module handles:
1. Phase 1: Get amendments list from bulk endpoints (/amendment, /amendment/119, etc.)
2. Phase 2: Get full amendment data from individual URLs
3. Phase 3: Get related data (actions, cosponsors, etc.) from URLs
"""

import logging
from typing import Any

import asyncpg

from ....api_clients import CongressionalAPIClient
from ..base import CongressionalBaseFetcher

logger = logging.getLogger(__name__)


class AmendmentsFetcher(CongressionalBaseFetcher):
    """
    Fetcher for Congressional amendments data using the 3-phase API approach.
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
            data_type_name="amendments",
        )

    # =============================================================================
    # REQUIRED ABSTRACT METHOD IMPLEMENTATIONS
    # =============================================================================

    def extract_item_id(self, item_data: dict[str, Any]) -> str:
        """Extract standardized amendment ID from amendment data."""
        amendment_type = item_data.get("type", "").lower()
        number = item_data.get("number", "")
        congress = item_data.get("congress", "")

        if all([amendment_type, number, congress]):
            return f"{amendment_type}{number}-{congress}"
        else:
            return "ID_ERROR"

    def _extract_preliminary_item_id(self, list_item: dict[str, Any]) -> str:
        """
        Extract a preliminary item ID from list data for checkpoint tracking.

        This is used for checkpoint tracking before we have the full data.
        The actual item ID will be extracted from full data later.

        Args:
            list_item: List item data containing URL

        Returns:
            Preliminary item ID extracted from URL or other available data
        """
        return self.extract_item_id(list_item)

    # =============================================================================
    # AMENDMENTS-SPECIFIC RELATED DATA METHODS
    # =============================================================================

    async def get_amendments_actions(
        self, full_amendment_data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Get amendment actions from actions URL in full amendment data."""
        return await self.get_generic_related_data(full_amendment_data)

    async def get_amendments_cosponsors(
        self, full_amendment_data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Get amendment cosponsors from cosponsors URL in full amendment data."""
        return await self.get_generic_related_data(full_amendment_data)

    async def get_amendments_texts(
        self, full_amendment_data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Get amendment text versions from textVersions URL in full amendment data."""
        return await self.get_generic_related_data(full_amendment_data)
