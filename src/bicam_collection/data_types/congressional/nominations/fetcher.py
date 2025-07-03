"""
Nominations data fetcher using the base fetcher architecture.

This module handles:
1. Phase 1: Get nominations list from bulk endpoints (/nomination, etc.)
2. Phase 2: Get full nomination data from individual URLs
3. Phase 3: Get related data (nominations, bills, etc.) from URLs
"""

import logging
from typing import Any

import asyncpg

from ....api_clients import CongressionalAPIClient
from ..base import CongressionalBaseFetcher

logger = logging.getLogger(__name__)


class NominationsFetcher(CongressionalBaseFetcher):
    """
    Fetcher for Congressional nominations data using the 3-phase API approach.
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
            data_type_name="nominations",
        )

    # =============================================================================
    # REQUIRED ABSTRACT METHOD IMPLEMENTATIONS
    # =============================================================================

    def extract_item_id(self, item_data: dict[str, Any]) -> str:
        """Extract standardized nomination ID from nomination data."""
        nomination_number = item_data.get("number")
        part_number = item_data.get("partNumber", "00")
        congress = item_data.get("congress")
        if all([nomination_number, part_number, congress]):
            return f"PN{nomination_number}-{part_number}-{congress}"
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
    # NOMINATIONS-SPECIFIC RELATED DATA METHODS
    # =============================================================================

    async def get_nominations_actions(
        self, full_nomination_data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Get actions from actions URL in full nomination data."""
        return await self.get_generic_related_data(
            full_nomination_data, "actions", "actions"
        )

    async def get_nominations_committeeactivities(
        self, full_nomination_data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if "committees" not in full_nomination_data:
            # logger.warning("No committees field in full nomination data")
            return []

        committee_info = full_nomination_data["committees"]
        if not isinstance(committee_info, dict) or "url" not in committee_info:
            logger.warning("Invalid committees info in full nomination data")
            return []

        committee_url = committee_info["url"]

        # Get raw committee data
        raw_committees = await self.fetch_related_data_with_client(
            committee_url, ["committees"], self.client
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

    async def get_nominations_hearings(
        self, full_nomination_data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Get hearings from hearings URL in full nomination data."""
        return await self.get_generic_related_data(
            full_nomination_data, "hearings", "hearings"
        )

    async def get_nominations_individualnominees(
        self, full_nomination_data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """
        Get nominees from multiple nominee URLs in full nomination data.

        The nominees field contains an array of nominee objects, each with its own URL,
        unlike other fields that have a single URL. We need to fetch from each URL
        and combine the results.
        """
        if "nominees" not in full_nomination_data:
            return []

        nominees_array = full_nomination_data["nominees"]
        if not nominees_array or not isinstance(nominees_array, list):
            return []

        # Extract nomination_id for linking nominees back to their nomination
        nomination_id = self.extract_item_id(full_nomination_data)

        all_nominees = []

        # Fetch data from each nominee URL
        for nominee_info in nominees_array:
            if not isinstance(nominee_info, dict) or "url" not in nominee_info:
                continue

            url = nominee_info["url"]
            try:
                # Fetch nominees data from this specific URL
                nominees_data = await self.fetch_related_data_with_client(
                    url, ["nominees"], self.client
                )

                # Add metadata from the nominee_info as a nested field
                for nominee in nominees_data:
                    nominee.update(
                        {
                            "nomination_id": nomination_id,  # Link back to parent nomination
                            "position_id": nominee_info.get("ordinal")
                        }
                    )

                all_nominees.extend(nominees_data)

            except Exception as e:
                logger.error(f"Error fetching nominees from {url}: {e}")
                continue

        return all_nominees

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
