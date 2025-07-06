"""
Committeereports data fetcher using the base fetcher architecture.

This module handles:
1. Phase 1: Get committeereports list from bulk endpoints (/committeereport, etc.)
2. Phase 2: Get full committeereport data from individual URLs
3. Phase 3: Get related data (committees, bills, etc.) from URLs
"""

import logging
from collections.abc import AsyncIterator
from typing import Any

import asyncpg

from ....api_clients import CongressionalAPIClient
from ..base import CongressionalBaseFetcher

logger = logging.getLogger(__name__)


class CommitteereportsFetcher(CongressionalBaseFetcher):
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
            data_type_name="committeereports",
        )

    # =============================================================================
    # REQUIRED ABSTRACT METHOD IMPLEMENTATIONS
    # =============================================================================

    def extract_item_id(self, item_data: dict[str, Any]) -> str:
        """Extract standardized committeereport ID from committeereport data."""
        item_data = item_data[0] if isinstance(item_data, list) else item_data
        report_number = item_data.get("number")
        congress = item_data.get("congress")
        report_type = item_data.get("type")
        report_part = item_data.get("part", "1")
        if all([report_number, congress, report_type]):
            return f"{report_type.lower()}{report_number}-{report_part}-{congress}"
        else:
            logger.info(f"Error extracting item ID: {item_data}")
            return "ID_ERROR"

    def _extract_preliminary_item_id(self, list_item: dict[str, Any]) -> str:
        """
        Derive a stable ID for checkpointing directly from the list-response record.
        This guarantees a non-null value so the progress-tracker can reliably
        decide whether the item was processed in a previous run.
        """
        return self.extract_item_id(list_item)

    # =============================================================================
    # COMMITTEEREPORTS-SPECIFIC RELATED DATA METHODS
    # =============================================================================

    async def get_committeereports_texts(
        self, full_committeereport_data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Get texts from texts URL in full committeereport data."""
        return await self.get_generic_related_data(
            full_committeereport_data
        )

    # =============================================================================
    # COMMITTEEREPORTS-SPECIFIC OVERRIDES
    # =============================================================================

    async def fetch_list_data_with_client(
        self,
        client,
        from_date: str | None = None,
        to_date: str | None = None,
        limit: int | None = 250,
        offset: int | None = None,
        **kwargs,
    ) -> AsyncIterator[list[dict[str, Any]]]:
        """
        Fetch list data using a specific client.

        This method can be overridden by subclasses to customize list data fetching
        while maintaining compatibility with parallel processing.

        Args:
            client: API client to use for the request
            from_date: Start date filter (YYYY-MM-DD)
            to_date: End date filter (YYYY-MM-DD)
            limit: Items per page
            offset: Starting offset
            **kwargs: Additional parameters

        Yields:
            Batches of item dictionaries with 'url' fields
        """
        async for batch in client.retrieve_data_list(
            data_type=self.outer_api_field,
            from_date=from_date,
            to_date=to_date,
            limit=limit or 250,  # Always maximize API efficiency
            offset=offset,
            abnormal_key="reports",
            **kwargs,
        ):
            yield batch

    async def fetch_full_data_with_client(
        self, item_url: str, client: CongressionalAPIClient
    ) -> dict[str, Any] | None:
        """Fetch complete committee report data from Congressional URL."""
        return await client.retrieve_full_data_from_url(
            item_url, expected_key="committeeReports"
        )
