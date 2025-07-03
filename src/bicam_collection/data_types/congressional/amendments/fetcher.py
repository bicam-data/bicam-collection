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
        # Try the canonical extractor first
        prelim_id = self.extract_item_id(list_item)

        if prelim_id != "ID_ERROR":
            return prelim_id

        # Fallback – derive from the provided URL (pattern: /amendment/<congress>/<slug>)
        url: str | None = list_item.get("url")
        if url:
            import re

            # Example URLs:
            #   .../amendment/118/samdt2323
            #   .../amendment/117/hamdt122
            m = re.search(
                r"/amendment/(?P<cong>\d+)/(?P<slug>[a-z]+)(?P<num>\d+)", url, re.I
            )
            if m:
                congress = m.group("cong")
                am_type = m.group("slug").lower()
                number = m.group("num")
                return f"{am_type}{number}-{congress}"

        # As an absolute last-resort, fall back to a deterministic hash of the URL
        # so that we still have a unique key and avoid contaminating the DB with
        # repeated "ID_ERROR" strings.
        import hashlib

        return hashlib.sha1(str(list_item).encode()).hexdigest()[:20]

    # =============================================================================
    # AMENDMENTS-SPECIFIC RELATED DATA METHODS
    # =============================================================================

    async def get_amendments_actions(
        self, full_amendment_data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Get amendment actions from actions URL in full amendment data."""
        return await self.get_generic_related_data(
            full_amendment_data, "actions", "actions"
        )

    async def get_amendments_cosponsors(
        self, full_amendment_data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Get amendment cosponsors from cosponsors URL in full amendment data."""
        return await self.get_generic_related_data(
            full_amendment_data, "cosponsors", "cosponsors"
        )

    async def get_amendments_texts(
        self, full_amendment_data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Get amendment text versions from textVersions URL in full amendment data."""
        return await self.get_generic_related_data(
            full_amendment_data, "textVersions", "textVersions"
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
