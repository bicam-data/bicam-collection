"""
Bill Collections Custom Plugin Logic

This module contains all the custom logic for bill collections data type including:
- Fetching logic (from fetcher.py)
- Cleaning logic (from cleaner.py)

"""

import logging
import re
from collections.abc import AsyncGenerator
from typing import Any

logger = logging.getLogger(__name__)

class BillCollectionsFetcher:
    """
    Bill Collections fetcherogic that extends the base GovInfo fetcher logic.

    This class provides custom methods for fetching congressional directories data, including:
    - Extracting standardized congressional directories IDs
    - Handling related data from URLs
    - Implementing specialized fetching logic for congressional directories-specific data

    Inherits shared functionality like get_generic_related_data from
    GovInfoBaseFetcher.
    """

    def __init__(self, data_type: str = "billcollections"):
        self.data_type = data_type

class BillCollectionsCleaner:
    """
    Bill Collections cleaner logic extracted from BillCollectionsCleaner class.
    Contains all the custom cleaning methods for bill collections data.
    """

    def __init__(
        self,
        data_type_name: str = "billcollections",
        system_name: str = "govinfo",
        staging_schema: str = "bicam_staging_govinfo",
        production_schema: str = "bicam_govinfo",
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


