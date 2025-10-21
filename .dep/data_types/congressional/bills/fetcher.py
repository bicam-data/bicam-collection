"""
Bills data fetcher using the base fetcher architecture.

This module handles:
1. Phase 1: Get bills list from bulk endpoints (/bill, /bill/119, etc.)
2. Phase 2: Get full bill data from individual URLs
3. Phase 3: Get related data (actions, cosponsors, texts, summaries) from URLs
"""

import logging
from typing import Any

import asyncpg

from ....api_clients import CongressionalAPIClient
from ..base import CongressionalBaseFetcher

logger = logging.getLogger(__name__)


class BillsFetcher(CongressionalBaseFetcher):
    """
    Fetcher for Congressional bills data using the 3-phase API approach.
    Inherits from BaseFetcher for standardized processing pipeline.
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
            data_type_name="bills",
        )

    # =============================================================================
    # REQUIRED ABSTRACT METHOD IMPLEMENTATIONS
    # =============================================================================

    def extract_item_id(self, item_data: dict[str, Any]) -> str:
        """Extract standardized bill ID from bill data."""
        bill_type = item_data.get("type", "").lower()
        number = item_data.get("number", "").replace("½", ".5")
        congress = item_data.get("congress", "")

        if all([bill_type, number, congress]):
            return f"{bill_type}{number}-{congress}"
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
    # BILLS-SPECIFIC RELATED DATA METHODS
    # =============================================================================

    async def get_bills_actions(
        self, full_bill_data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Get bill actions from actions URL in full bill data."""
        return await self.get_generic_related_data(
            full_bill_data, related_table_name="actions"
        )

    async def get_bills_cosponsors(
        self, full_bill_data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Get bill cosponsors from cosponsors URL in full bill data."""
        return await self.get_generic_related_data(
            full_bill_data, related_table_name="cosponsors"
        )

    async def get_bills_texts(
        self, full_bill_data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Get bill text versions from textVersions URL in full bill data."""
        return await self.get_generic_related_data(
            full_bill_data, related_table_name="texts"
        )

    async def get_bills_summaries(
        self, full_bill_data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Get bill summaries from summaries URL in full bill data."""
        return await self.get_generic_related_data(
            full_bill_data, related_table_name="summaries"
        )

    async def get_bills_subjects(
        self, full_bill_data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """
        Get bill subjects from subjects URL in full bill data.
        Note: Congressional API returns subjects in nested structure:
        {"subjects": {"legislativeSubjects": [...], "policyArea": {...}}}
        """
        return await self.get_generic_related_data(
            full_bill_data, related_table_name="subjects"
        )

    async def get_bills_titles(
        self, full_bill_data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Get bill titles from titles URL in full bill data."""
        return await self.get_generic_related_data(
            full_bill_data, related_table_name="titles"
        )

    async def get_bills_relatedbills(
        self, full_bill_data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """
        Get bill related bills from relatedBills URL in full bill data.

        Related bills require special handling since each related bill can have
        multiple relationships, which need to be flattened into separate records.
        """
        if "relatedBills" not in full_bill_data:
            # logger.warning("No relatedBills field in full bill data")
            return []

        related_bills_info = full_bill_data["relatedBills"]
        if not isinstance(related_bills_info, dict) or "url" not in related_bills_info:
            logger.warning("Invalid relatedBills info in full bill data")
            return []

        related_bills_url = related_bills_info["url"]

        # Get raw related bills data using config
        raw_related_bills = await self.client.retrieve_related_data_from_url(
            related_bills_url, list_key=["relatedBills"]
        )

        if not raw_related_bills:
            return []

        # Process the related bills data to extract relationships
        related_bills_relationships = []
        bill_id = self.extract_item_id(full_bill_data)

        for related_bill in raw_related_bills:
            # Extract the related bill ID
            related_bill_type = related_bill.get("type", "").lower()
            related_bill_number = str(related_bill.get("number", "")).replace("½", ".5")
            related_bill_congress = str(related_bill.get("congress", ""))

            if all([related_bill_type, related_bill_number, related_bill_congress]):
                relatedbill_id = (
                    f"{related_bill_type}{related_bill_number}-{related_bill_congress}"
                )
            else:
                # Fallback to URL if we can't construct ID
                relatedbill_id = related_bill.get("url", "unknown")

            # Process each relationship for this related bill
            for relationship in related_bill.get("relationshipDetails", []):
                related_bills_relationships.append(
                    {
                        "bill_id": bill_id,
                        "relatedbill_id": relatedbill_id,
                        "relationship_identified_by": relationship.get("identifiedBy"),
                        "relationship_type": relationship.get("type"),
                    }
                )

        return related_bills_relationships

    async def get_bills_committeeactivities(
        self, full_bill_data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """
        Get bill committee activities from committees URL in full bill data.
        """
        if "committees" not in full_bill_data:
            # logger.warning("No committees field in full bill data")
            return []

        committee_info = full_bill_data["committees"]
        if not isinstance(committee_info, dict) or "url" not in committee_info:
            logger.warning("Invalid committees info in full bill data")
            return []

        committee_url = committee_info["url"]

        # Get raw committee data
        raw_committee_data = await self.client.retrieve_related_data_from_url(
            committee_url, list_key=["committees"]
        )

        if not raw_committee_data:
            return []

        # Process committee data to extract activities
        committee_activities = []
        bill_id = self.extract_item_id(full_bill_data)

        for committee in raw_committee_data:
            # Extract committee activities
            activities = committee.get("activities", [])
            for activity in activities:
                committee_activities.append(
                    {
                        "bill_id": bill_id,
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
                            "bill_id": bill_id,
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
