"""
Bills Custom Plugin Logic

This module contains all the custom logic for bills data type including:
- Fetching logic (from fetcher.py)
- Database normalization logic (from database_normalizer.py)
- Cleaning logic (from cleaner.py)

All logic is organized into classes that can be used by the plugin system.
"""

import logging
from typing import Any

# Import the base class that provides shared functionality
from ....base import CongressionalBaseFetcherLogic

logger = logging.getLogger(__name__)


class NominationsFetcherLogic(CongressionalBaseFetcherLogic):
    """
    Nominations-specific fetcher logic that extends the base Congressional fetcher logic.

    This class provides custom methods for fetching bills data, including:
    - Extracting standardized bill IDs
    - Handling related data from URLs
    - Implementing specialized fetching logic for bills-specific data

    Inherits shared functionality like get_generic_related_data from
    CongressionalBaseFetcherLogic.
    """

    def __init__(self, data_type: str = "nominations"):
        super().__init__(data_type)

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
        return self.extract_item_id(list_item)

    # =============================================================================
    # NOMINATIONS-SPECIFIC RELATED DATA METHODS
    # =============================================================================

    async def get_nominations_actions(
        self, full_nomination_data: dict[str, Any], client
    ) -> list[dict[str, Any]]:
        """Get actions from actions URL in full nomination data."""
        logger.debug("get_nominations_actions called for nomination")
        result = await self.get_generic_related_data(
            full_nomination_data, related_table_name="actions", client=client
        )
        logger.debug(
            f"get_nominations_actions returned {len(result) if result else 0} actions"
        )
        return result

    async def get_nominations_committeeactivities(
        self, full_nomination_data: dict[str, Any], client
    ) -> list[dict[str, Any]]:
        logger.debug("get_nominations_committeeactivities called for nomination")
        if "committees" not in full_nomination_data:
            logger.debug("No committees field in full nomination data")
            return []

        committee_info = full_nomination_data["committees"]
        if not isinstance(committee_info, dict) or "url" not in committee_info:
            logger.warning("Invalid committees info in full nomination data")
            return []

        committee_url = committee_info["url"]

        # Get raw committee data
        raw_committees = await client.retrieve_related_data_from_url(
            committee_url, list_key=["committees"]
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

        logger.debug(
            f"get_nominations_committeeactivities returned {len(committee_activities)} activities"
        )
        return committee_activities

    async def get_nominations_hearings(
        self, full_nomination_data: dict[str, Any], client
    ) -> list[dict[str, Any]]:
        """Get hearings from hearings URL in full nomination data."""
        logger.debug("get_nominations_hearings called for nomination")
        result = await self.get_generic_related_data(
            full_nomination_data, related_table_name="hearings", client=client
        )
        logger.debug(
            f"get_nominations_hearings returned {len(result) if result else 0} hearings"
        )
        return result

    async def get_nominations_individualnominees(
        self, full_nomination_data: dict[str, Any], client
    ) -> list[dict[str, Any]]:
        """
        Get nominees from multiple nominee URLs in full nomination data.

        The nominees field contains an array of nominee objects, each with its own URL,
        unlike other fields that have a single URL. We need to fetch from each URL
        and combine the results.
        """
        logger.debug("get_nominations_individualnominees called for nomination")
        if "nominees" not in full_nomination_data:
            logger.debug("No nominees field in full nomination data")
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
                nominees_data = await client.retrieve_related_data_from_url(
                    url, list_key=["nominees"]
                )

                # Add metadata from the nominee_info as a nested field
                for nominee in nominees_data:
                    nominee.update(
                        {
                            "nomination_id": nomination_id,  # Link back to parent nomination
                            "position_id": nominee_info.get("ordinal"),
                        }
                    )

                all_nominees.extend(nominees_data)

            except Exception as e:
                logger.error(f"Error fetching nominees from {url}: {e}")
                continue

        logger.debug(
            f"get_nominations_individualnominees returned {len(all_nominees)} nominees"
        )
        return all_nominees
