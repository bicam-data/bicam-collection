"""
Congresses data fetcher using the base fetcher architecture.

This module handles:
1. Phase 1: Get congresses list from bulk endpoints (/congresses, etc.)
2. Phase 2: Get full congress data from individual URLs
3. Phase 3: Get related data (congresses, bills, etc.) from URLs
"""

import logging
import re
from collections.abc import AsyncIterator
from typing import Any

import asyncpg

from ....api_clients import CongressionalAPIClient
from ..base import CongressionalBaseFetcher

logger = logging.getLogger(__name__)


class CongressesFetcher(CongressionalBaseFetcher):
    """
    Fetcher for Congressional congresses data using the 3-phase API approach.
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
            data_type_name="congresses",
        )

    # =============================================================================
    # REQUIRED ABSTRACT METHOD IMPLEMENTATIONS
    # =============================================================================

    def extract_item_id(self, item_data: dict[str, Any]) -> str:
        """Extract standardized congress ID from congress data."""
        return str(item_data.get("number", "ID_ERROR"))

    def _extract_preliminary_item_id(self, list_item: dict[str, Any]) -> str:
        """
        Derive a stable ID for checkpointing directly from the list-response record.
        This guarantees a non-null value so the progress-tracker can reliably
        decide whether the item was processed in a previous run.
        """

        # 1. direct field
        prelim_id_field = list_item.get("name")
        if prelim_id_field:
            # only take the digits before the first space
            prelim_id_prefix = prelim_id_field.split(" ")[0]
            prelim_id = re.sub(r"\D", "", prelim_id_prefix)
            return str(prelim_id)
        else:
            return "ID_ERROR"

    # =============================================================================
    # CONGRESS-SPECIFIC RELATED DATA METHODS
    # =============================================================================

    # =============================================================================
    # CONGRESS-SPECIFIC OVERRIDE FETCHING METHODS
    # =============================================================================

    async def fetch_list_data_with_client(
        self,
        client: CongressionalAPIClient,
        from_date: str | None = None,
        to_date: str | None = None,
        limit: int | None = 250,
        offset: int | None = None,
        **kwargs,
    ) -> AsyncIterator[list[dict[str, Any]]]:
        """
        Fetch list data using a specific client.
        """
        # Use the new configuration structure
        api_config = self.config.api if self.config else None
        endpoint = api_config.api_endpoint if api_config else "congress"

        async for batch in client.retrieve_data_list(
            data_type=endpoint,
            from_date=from_date,
            to_date=to_date,
            limit=limit or 250,  # Always maximize API efficiency
            offset=offset,
            **kwargs,
        ):
            yield batch

    async def fetch_full_data_with_client(
        self, item_url: str, client: CongressionalAPIClient
    ) -> dict[str, Any] | None:
        """Fetch complete congress data from Congressional URL."""
        # Use the new configuration structure
        api_config = self.config.api if self.config else None
        full_key = api_config.full_key if api_config else "congress"

        return await client.retrieve_full_data_from_url(item_url, full_key=full_key)
