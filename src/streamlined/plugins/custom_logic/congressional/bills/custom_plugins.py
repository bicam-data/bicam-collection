"""
Bills Custom Plugin Logic

This module contains all the custom logic for bills data type including:
- Fetching logic (from fetcher.py)
- Database normalization logic (from database_normalizer.py)
- Cleaning logic (from cleaner.py)

All logic is organized into classes that can be used by the plugin system.
"""

import logging
import re
from collections.abc import AsyncGenerator
from typing import Any

# Import the base class that provides shared functionality
from ....base import CongressionalBaseFetcherLogic

logger = logging.getLogger(__name__)


class BillsFetcherLogic(CongressionalBaseFetcherLogic):
    """
    Bills-specific fetcher logic that extends the base Congressional fetcher logic.

    This class provides custom methods for fetching bills data, including:
    - Extracting standardized bill IDs
    - Handling related data from URLs
    - Implementing specialized fetching logic for bills-specific data

    Inherits shared functionality like get_generic_related_data from
    CongressionalBaseFetcherLogic.
    """

    def __init__(self, data_type: str = "bills"):
        super().__init__(data_type)

    def extract_item_id(self, item_data: dict[str, Any], **kwargs) -> str:
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

    async def get_bills_actions(
        self, full_bill_data: dict[str, Any], client
    ) -> list[dict[str, Any]]:
        """Get bill actions from actions URL in full bill data."""
        return await self.get_generic_related_data(
            full_bill_data, related_table_name="actions", client=client
        )

    async def get_bills_cosponsors(
        self, full_bill_data: dict[str, Any], client
    ) -> list[dict[str, Any]]:
        """Get bill cosponsors from cosponsors URL in full bill data."""
        return await self.get_generic_related_data(
            full_bill_data, related_table_name="cosponsors", client=client
        )

    async def get_bills_texts(
        self, full_bill_data: dict[str, Any], client
    ) -> list[dict[str, Any]]:
        """Get bill text versions from textVersions URL in full bill data."""
        return await self.get_generic_related_data(
            full_bill_data, related_table_name="textVersions", client=client
        )

    async def get_bills_summaries(
        self, full_bill_data: dict[str, Any], client
    ) -> list[dict[str, Any]]:
        """Get bill summaries from summaries URL in full bill data."""
        return await self.get_generic_related_data(
            full_bill_data, related_table_name="summaries", client=client
        )

    async def get_bills_subjects(
        self, full_bill_data: dict[str, Any], client
    ) -> list[dict[str, Any]]:
        """
        Get bill subjects from subjects URL in full bill data.
        Note: Congressional API returns subjects in nested structure:
        {"subjects": {"legislativeSubjects": [...], "policyArea": {...}}}
        """
        return await self.get_generic_related_data(
            full_bill_data, related_table_name="subjects", client=client
        )

    async def get_bills_titles(
        self, full_bill_data: dict[str, Any], client
    ) -> list[dict[str, Any]]:
        """Get bill titles from titles URL in full bill data."""
        return await self.get_generic_related_data(
            full_bill_data, related_table_name="titles", client=client
        )

    async def get_bills_relatedbills(
        self, full_bill_data: dict[str, Any], client
    ) -> list[dict[str, Any]]:
        """
        Get bill related bills from relatedBills URL in full bill data.

        Related bills require special handling since each related bill can have
        multiple relationships, which need to be flattened into separate records.
        """
        if "relatedBills" not in full_bill_data:
            return []

        related_bills_info = full_bill_data["relatedBills"]
        if not isinstance(related_bills_info, dict) or "url" not in related_bills_info:
            logger.warning("Invalid relatedBills info in full bill data")
            return []

        related_bills_url = related_bills_info["url"]

        # Get raw related bills data using config
        raw_related_bills = await client.retrieve_related_data_from_url(
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
        self, full_bill_data: dict[str, Any], client
    ) -> list[dict[str, Any]]:
        """
        Get bill committee activities from committees URL in full bill data.
        """
        if "committees" not in full_bill_data:
            return []

        committee_info = full_bill_data["committees"]
        if not isinstance(committee_info, dict) or "url" not in committee_info:
            logger.warning("Invalid committees info in full bill data")
            return []

        committee_url = committee_info["url"]

        # Get raw committee data
        raw_committee_data = await client.retrieve_related_data_from_url(
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


class BillsCleanerLogic:
    """
    Bills-specific cleaner logic extracted from BillsCleaner class.
    Contains all the custom cleaning methods for bills data.
    """

    def __init__(
        self,
        data_type_name: str = "bills",
        system_name: str = "congressional",
        staging_schema: str = "bicam_staging_congressional",
        production_schema: str = "bicam_congressional",
    ):
        self.data_type_name = data_type_name
        self.system_name = system_name
        self.staging_schema = staging_schema
        self.production_schema = production_schema
        self.db_pool = None

        # Set bills-specific multi-table processing configuration
        self.multi_table_data_types = {
            "bills_texts": ["bills_texts", "bills_texts_formats"],
            # Add other bills multi-table data types here as needed
        }

    async def _stream_bills_texts_joined_chunks(
        self, chunk_size: int
    ) -> AsyncGenerator[list[dict[str, Any]], None]:
        """
        Stream joined data from bills_texts_staging and bills_texts_formats_staging.
        Properly aggregates multiple formats per text record.
        """
        if not self.db_pool:
            raise ValueError("Database pool not configured")

        async with self.db_pool.acquire() as conn:
            try:
                # First, check if both tables exist
                tables_exist_query = """
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_schema = $1
                AND table_name IN ('bills_texts', 'bills_texts_formats')
                """
                tables_count = await conn.fetchval(
                    tables_exist_query, self.staging_schema
                )

                if tables_count < 2:
                    logger.warning(
                        "One or both bills_texts staging tables do not exist"
                    )
                    return

                # Get total count for logging
                count_query = f"""
                SELECT COUNT(DISTINCT bt.id)
                FROM {self.staging_schema}.bills_texts bt
                """
                total_count = await conn.fetchval(count_query)

                if total_count == 0:
                    logger.info("No bills_texts records found")
                    return

                logger.info(
                    f"Streaming {total_count} bills_texts records with formats in chunks of {chunk_size}"
                )

                # Stream using pagination with aggregated formats
                offset = 0
                while True:
                    # Use JSON aggregation to collect all formats for each text
                    query = f"""
                    SELECT
                        bt.*,
                        COALESCE(
                            json_agg(
                                json_build_object('type', btf.type, 'url', btf.url)
                                ORDER BY btf.type
                            ) FILTER (WHERE btf.type IS NOT NULL),
                            '[]'::json
                        ) as formats_json
                    FROM {self.staging_schema}.bills_texts bt
                    LEFT JOIN {self.staging_schema}.bills_texts_formats btf
                        ON bt.bill_id = btf.bill_id AND bt.list_index = btf.list_index
                    GROUP BY bt.id, bt.bill_id, bt.list_index, bt.date, bt.type,
                                bt.processed_at
                    ORDER BY bt.processed_at DESC
                    LIMIT {chunk_size} OFFSET {offset}
                    """

                    rows = await conn.fetch(query)
                    if not rows:
                        break

                    chunk = []
                    for row in rows:
                        try:
                            row_dict = dict(row)
                            chunk.append(row_dict)
                        except Exception as e:
                            logger.error(
                                f"Error converting bills_texts row to dict: {e}, row type: {type(row)}, row: {row}"
                            )
                            continue

                    if chunk:  # Only yield if we have valid records
                        yield chunk

                    offset += chunk_size

                    # Log progress periodically
                    if offset % (chunk_size * 10) == 0:
                        logger.debug(
                            f"Streamed {offset} bills_texts records with formats"
                        )

            except Exception as e:
                logger.error(f"Error streaming bills_texts with formats: {e}")
                raise

    async def _clean_bills_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual bills records.
        """
        cleaned = record_data.copy()

        # Apply bills-specific cleaning logic
        filtered_cleaned = {
            "bill_id": str(cleaned.get("bill_id", "ID_ERROR")),
            "bill_type": str(cleaned.get("type", None).lower()),
            "bill_number": float(cleaned.get("number", None)),
            "congress": self.safe_int(cleaned.get("congress", None)),
            "title": str(cleaned.get("title", None)),
            "origin_chamber": str(
                self.standardize_chamber(cleaned.get("originchamber", None))
            ),
            "policy_area": str(cleaned.get("policyarea_name", None)),
            "is_law": None,  # added via postprocessing
            "introduced_at": self.standardize_date(cleaned.get("introduceddate", None)),
            "constitutional_authority_statement": self.clean_long_text(
                cleaned.get("constitutionalauthoritystatementtext", None)
            ),
            "actions_count": self.safe_int(cleaned.get("actions_count", 0), 0),
            "amendments_count": self.safe_int(cleaned.get("amendments_count", 0), 0),
            "committees_count": self.safe_int(cleaned.get("committees_count", 0), 0),
            "cosponsors_count": self.safe_int(cleaned.get("cosponsors_count", 0), 0),
            "cosponsors_withdrawn_count": (
                self.safe_int(
                    cleaned.get("cosponsors_countincludingwithdrawncosponsors", 0), 0
                )
                - self.safe_int(cleaned.get("cosponsors_count", 0), 0)
            ),
            "relatedbills_count": self.safe_int(
                cleaned.get("relatedbills_count", 0), 0
            ),
            "subjects_count": self.safe_int(cleaned.get("subjects_count", 0), 0),
            "summaries_count": self.safe_int(cleaned.get("summaries_count", 0), 0),
            "texts_count": self.safe_int(cleaned.get("textversions_count", 0), 0),
            "titles_count": self.safe_int(cleaned.get("titles_count", 0), 0),
            "updated_at": self.standardize_date(cleaned.get("updatedate", None)),
        }

        # Validate bill_id format
        if not re.match(
            r"^(hr|hres|sres|s|hjres|hconres|sjres|sconres)\d{1,4}(\.5)?-\d{1,3}$",
            filtered_cleaned["bill_id"],
        ):
            raise ValueError(f"Invalid bill_id format: {filtered_cleaned['bill_id']}")

        return filtered_cleaned

    async def _clean_bills_actions_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """Custom cleaning logic for bills actions records."""
        cleaned = record_data.copy()

        filtered_cleaned = {
            "action_id": str(cleaned.get("id", "ID_ERROR")),
            "bill_id": str(cleaned.get("bill_id", "ID_ERROR")),
            "action_code": str(cleaned.get("actioncode", None)),
            "action_date": self.standardize_date(cleaned.get("actiondate", None)),
            "text": self.clean_long_text(cleaned.get("text", None)),
            "action_type": str(cleaned.get("type", None)),
            "source_system": str(cleaned.get("sourcesystem_name", None)),
            "source_system_code": self.safe_int(cleaned.get("sourcesystem_code", None)),
            "calendar": str(cleaned.get("calendarnumber_calendar", None)),
            "calendar_number": self.safe_int(
                cleaned.get("calendarnumber_number", None)
            ),
        }

        return filtered_cleaned

    # Add other cleaning methods for bills sub-tables
    async def _clean_bills_cosponsors_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """Custom cleaning logic for bills cosponsors records."""
        cleaned = record_data.copy()

        filtered_cleaned = {
            "bill_id": str(cleaned.get("bill_id", "ID_ERROR")),
            "bioguide_id": str(cleaned.get("bioguideid", "ID_ERROR")),
            "display_name": str(cleaned.get("fullname", None)),
            "party": str(cleaned.get("party", None)),
            "state": str(cleaned.get("state", None)),
            "district": self.safe_int(cleaned.get("district", None)),
            "is_original_cosponsor": bool(cleaned.get("isoriginalcosponsor", None)),
            "sponsorship_date": self.standardize_date(
                cleaned.get("sponsorshipdate", None)
            ),
            "sponsorship_withdrawal_date": self.standardize_date(
                cleaned.get("sponsorshipwithdrawndate", None)
            ),
        }

        return filtered_cleaned

    # Add post-processing methods
    async def _post_process_bills(self) -> dict[str, Any]:
        """Post-processing operations specific to bills data."""
        if not self.db_pool:
            raise ValueError("Database pool not configured")

        results = {
            "status": "success",
            "operations": [],
            "rows_affected": 0,
        }

        async with self.db_pool.acquire() as conn:
            # Operation 1: Populate is_law field based on bills_laws table
            try:
                async with conn.transaction():
                    sql = f"""
                        UPDATE {self.production_schema}.bills
                        SET is_law = CASE
                            WHEN EXISTS (
                                SELECT 1
                                FROM {self.production_schema}.bills_laws bl
                                WHERE bl.bill_id = bills.bill_id
                            ) THEN true
                            ELSE false
                        END
                        WHERE is_law IS NULL;
                    """

                    result = await conn.execute(sql)
                    rows_affected = int(result.split()[-1]) if result.split() else 0

                    results["operations"].append(
                        {
                            "name": "populate_is_law_field",
                            "status": "success",
                            "rows_affected": rows_affected,
                        }
                    )
                    results["rows_affected"] += rows_affected

                    logger.info(f"Updated is_law for {rows_affected} bills")

            except Exception as e:
                logger.error(f"Error populating is_law field: {e}")
                results["operations"].append(
                    {
                        "name": "populate_is_law_field",
                        "status": "error",
                        "error": str(e),
                    }
                )
                results["status"] = "partial_failure"

        return results

    # Helper methods from base cleaner
    def safe_int(self, value: Any, default: int = None) -> int | None:
        """Safely convert value to int."""
        if value is None:
            return default
        try:
            return int(value)
        except (ValueError, TypeError):
            return default

    def standardize_chamber(self, chamber: str) -> str | None:
        """Standardize chamber names."""
        if not chamber:
            return None
        chamber_lower = chamber.lower()
        if chamber_lower in ["house", "h"]:
            return "house"
        elif chamber_lower in ["senate", "s"]:
            return "senate"
        else:
            return chamber_lower

    def standardize_date(self, date_str: str) -> str | None:
        """Standardize date strings."""
        if not date_str:
            return None
        # Add date parsing logic here
        return date_str

    def clean_long_text(self, text: str) -> str | None:
        """Clean long text fields."""
        if not text:
            return None
        # Add text cleaning logic here
        return text.strip()
