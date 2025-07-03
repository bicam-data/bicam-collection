"""
Treaties data fetcher using the base fetcher architecture.

This module handles:
1. Phase 1: Get treaties list from bulk endpoints (/treaty, etc.)
2. Phase 2: Get full treaty data from individual URLs
3. Phase 3: Get related data (treaties, bills, etc.) from URLs
"""

import logging
from collections.abc import AsyncIterator
from typing import Any

import asyncpg

from ....api_clients import CongressionalAPIClient
from ..base import CongressionalBaseFetcher

logger = logging.getLogger(__name__)


class TreatiesFetcher(CongressionalBaseFetcher):
    """
    Fetcher for Congressional treaties data using the 3-phase API approach.
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
            data_type_name="treaties",
        )

    # =============================================================================
    # REQUIRED ABSTRACT METHOD IMPLEMENTATIONS
    # =============================================================================

    def extract_item_id(self, item_data: dict[str, Any]) -> str:
        """Extract standardized treaty ID from treaty data."""
        treaty_number = item_data.get("number")
        treaty_suffix = item_data.get("suffix", "")
        congress_received = item_data.get("congressReceived")

        if all([treaty_number, congress_received]):
            return f"td{congress_received}-{treaty_number}{treaty_suffix}"
        else:
            return "ID_ERROR"

    def _extract_preliminary_item_id(self, list_item: dict[str, Any]) -> str:
        """
        Derive a stable ID for checkpointing directly from the list-response record.
        This guarantees a non-null value so the progress-tracker can reliably
        decide whether the item was processed in a previous run.
        """

        # 1. direct field
        return self.extract_item_id(list_item)

    # =============================================================================
    # TREATIES-SPECIFIC OVERRIDE METHODS
    # =============================================================================
    async def fetch_list_data(
        self,
        from_date: str | None = None,
        to_date: str | None = None,
        limit: int | None = 250,
        offset: int | None = None,
        **kwargs,
    ) -> AsyncIterator[list[dict[str, Any]]]:
        """
        Phase 1: Fetch list data from Congressional bulk endpoints.

        Must yield batches of items containing URLs for Phase 2.

        Args:
            from_date: Start date filter (YYYY-MM-DD)
            to_date: End date filter (YYYY-MM-DD)
            limit: Items per page
            offset: Starting offset
            **kwargs: Additional parameters

        Yields:
            Batches of item dictionaries with 'url' fields
        """

        async for batch in self.client.retrieve_data_list(
            data_type=self.outer_api_field,
            from_date=from_date,
            to_date=to_date,
            limit=limit,
            offset=offset,
            abnormal_key="treaties",
            **kwargs,
        ):
            yield batch

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
            single_page_only=True,
            abnormal_key="treaties",
            **kwargs,
        ):
            yield batch

    # =============================================================================
    # TREATIES-SPECIFIC RELATED DATA METHODS
    # =============================================================================

    async def get_treaties_actions(
        self, full_treaty_data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Get treaty actions from actions URL in full treaty data."""
        return await self.get_generic_related_data(
            full_treaty_data, "actions", "actions"
        )

    async def get_treaties_committeeactivities(
        self, full_treaty_data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Get treaty committee activities from committeeActivities URL in full treaty data."""
        if "committees" not in full_treaty_data:
            # logger.warning("No committees field in full bill data")
            return []

        committee_info = full_treaty_data["committees"]
        if not isinstance(committee_info, dict) or "url" not in committee_info:
            logger.warning("Invalid committees info in full bill data")
            return []

        committee_url = committee_info["url"]

        # Get raw committee data
        raw_committees = await self.client.retrieve_related_data_from_url(
            committee_url, expected_key=["committees"]
        )

        if not raw_committees:
            return []

        # Process the committee data to extract activities
        committee_activities = []

        for committee in raw_committees:
            # Process main committee activities
            for activity in committee.get("activities", []):
                committee_activities.append(
                    {
                        "name": committee.get("name"),
                        "committee_code": committee.get("systemCode"),
                        "chamber": committee.get("chamber", "").lower()
                        if committee.get("chamber")
                        else None,
                        "type": committee.get("type"),
                        "activity_name": activity.get("name"),
                        "activity_date": activity.get("date"),
                    }
                )

            # Process subcommittee activities
            for subcommittee in committee.get("subcommittees", []):
                for activity in subcommittee.get("activities", []):
                    committee_activities.append(
                        {
                            "name": subcommittee.get("name"),
                            "committee_code": subcommittee.get("systemCode"),
                            "chamber": subcommittee.get("chamber", "").lower()
                            if subcommittee.get("chamber")
                            else None,
                            "type": "Subcommittee",
                            "activity_name": activity.get("name"),
                            "activity_date": activity.get("date"),
                        }
                    )

        return committee_activities

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
