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
        async for batch in client.retrieve_data_list(
            data_type=self.outer_api_field,
            from_date=from_date,
            to_date=to_date,
            limit=limit or 250,  # Always maximize API efficiency
            offset=offset,
            abnormal_key="congresses",
            **kwargs,
        ):
            yield batch

    async def fetch_full_data_with_client(
        self, item_url: str, client: CongressionalAPIClient
    ) -> dict[str, Any] | None:
        """Fetch complete congress data from Congressional URL."""
        return await client.retrieve_full_data_from_url(
            item_url, expected_key="congress"
        )
    # =============================================================================
    # UTILITY METHODS FOR BACKWARD COMPATIBILITY
    # =============================================================================

    async def sync_checkpoint_with_api_tracking(
        self, force_current_time: bool = False
    ) -> bool:
        """
        Sync the checkpoint system with the Congressional API last_processed_date tracking.
        """
        if not self.progress_tracker:
            logger.warning("No progress tracker available for syncing")
            return False

        if (
            not hasattr(self, "client")
            or not hasattr(self.client, "db_pool")
            or not self.client.db_pool
        ):
            logger.warning(
                "No Congressional API client with database pool available for syncing"
            )
            return False

        try:
            # Get the last processed date from the checkpoint system
            checkpoint_date = self.progress_tracker.checkpoint.last_processed_date

            if force_current_time or not checkpoint_date:
                from datetime import UTC, datetime

                date_to_use = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
                logger.info(f"Using current time for sync: {date_to_use}")
            else:
                date_to_use = checkpoint_date
                logger.info(f"Using checkpoint date for sync: {date_to_use}")

            # Update the Congressional API tracking table
            success = await self.client.update_last_processed_date(
                self.data_type_name, date_to_use
            )

            if success:
                logger.info(
                    f"Successfully synced {self.data_type_name} last_processed_date to: {date_to_use}"
                )
                return True
            else:
                logger.error(
                    f"Failed to sync {self.data_type_name} last_processed_date"
                )
                return False

        except Exception as e:
            logger.error(
                f"Error syncing checkpoint with API tracking: {str(e)}", exc_info=True
            )
            return False
