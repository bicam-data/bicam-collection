"""
Members data fetcher using the base fetcher architecture.

This module handles:
1. Phase 1: Get members list from bulk endpoints (/member, etc.)
2. Phase 2: Get full member data from individual URLs
3. Phase 3: Get related data (committees, bills, etc.) from URLs
"""

import logging
from typing import Any

import asyncpg

from ....api_clients import CongressionalAPIClient
from ..base import CongressionalBaseFetcher

logger = logging.getLogger(__name__)


class MembersFetcher(CongressionalBaseFetcher):
    """
    Fetcher for Congressional members data using the 3-phase API approach.
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
            data_type_name="members",
        )

    # =============================================================================
    # REQUIRED ABSTRACT METHOD IMPLEMENTATIONS
    # =============================================================================

    def extract_item_id(self, item_data: dict[str, Any]) -> str:
        """Extract standardized member ID from member data."""
        return item_data.get("bioguideId", "ID_ERROR")

    def _extract_preliminary_item_id(self, list_item: dict[str, Any]) -> str:
        """
        Derive a stable ID for checkpointing directly from the list-response record.

        * primary: bioguideId if it is present (rare in list responses)
        * secondary: memberId query-param in the `url` field
        * fallback: slug at end of path

        This guarantees a non-null value so the progress-tracker can reliably
        decide whether the item was processed in a previous run.
        """

        # 1. direct field
        prelim_id = list_item.get("bioguideId")
        if prelim_id:
            return str(prelim_id)

        # 2. parse from URL
        url = list_item.get("url", "")
        if url:
            import re

            # congress API style: .../member?memberId=M001234
            m = re.search(r"memberId=([A-Za-z0-9]+)", url)
            if m:
                return m.group(1)

            # else take last path token – e.g. /member/M001234
            token = url.rstrip("/").split("/")[-1]
            if token:
                return token

        # 3. ultimate fallback – use a hash of the full url so it is still stable
        from hashlib import md5

        if url:
            return md5(url.encode()).hexdigest()

        # Should never be empty but guard anyway
        return "UNKNOWN_MEMBER_ID"

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
