"""
Hearings Custom Plugin Logic

This module contains all the custom logic for hearings data type including:
- Fetching logic (from fetcher.py)
- Database normalization logic (from database_normalizer.py)
- Cleaning logic (from cleaner.py)

All logic is organized into classes that can be used by the plugin system.
"""

import logging
from typing import Any

# CongressionalBaseCleanerLogic provides utility methods and table override support
# Import the base class that provides shared functionality
from ....base import CongressionalBaseCleanerLogic, CongressionalBaseFetcherLogic

logger = logging.getLogger(__name__)


class HearingsFetcherLogic(CongressionalBaseFetcherLogic):
    """
    Hearings-specific fetcher logic that extends the base Congressional fetcher logic.

    This class provides custom methods for fetching hearings data, including:
    - Extracting standardized hearing IDs
    - Handling related data from URLs
    - Implementing specialized fetching logic for hearings-specific data

    Inherits shared functionality like get_generic_related_data from
    CongressionalBaseFetcherLogic.
    """

    def __init__(self, data_type: str = "hearings"):
        super().__init__(data_type)

    def extract_item_id(self, item_data: dict[str, Any]) -> str:
        """Extract standardized hearing ID from hearing data."""
        hearing_chamber = item_data.get("chamber")
        if hearing_chamber == "NoChamber":
            hearing_chamber_prefix = "j"
        else:
            hearing_chamber_prefix = hearing_chamber[0].lower()
        hearing_number = item_data.get("jacketNumber")
        hearing_congress = item_data.get("congress")
        if all([hearing_chamber_prefix, hearing_number, hearing_congress]):
            return f"{hearing_chamber_prefix}hrg{hearing_number}-{hearing_congress}"
        else:
            return "ID_ERROR"

    def _extract_preliminary_item_id(self, list_item: dict[str, Any]) -> str:
        """
        Derive a stable ID for checkpointing directly from the list-response record.
        This guarantees a non-null value so the progress-tracker can reliably
        decide whether the item was processed in a previous run.
        """

        return self.extract_item_id(list_item)

class HearingsCleanerLogic(CongressionalBaseCleanerLogic):
    """
    Hearings-specific cleaner that extends CongressionalBaseCleanerLogic with hearings-specific cleaning logic.

    Provides specialized cleaning methods for all amendments-related data types
    and handles multi-table processing for complex relationships.
    """

    def __init__(
        self,
        data_type_name: str = "hearings",
        system_name: str = "congressional",
        staging_schema: str = "bicam_staging_congressional",
        production_schema: str = "bicam_congressional",
    ):
        super().__init__()
        self.data_type_name = data_type_name
        self.system_name = system_name
        self.staging_schema = staging_schema
        self.production_schema = production_schema
        self.db_pool = None


        self.multi_table_data_types = {
            # Add hearings to enable custom joined streaming
            "hearings_texts": ["hearings_formats"],
            # Add other multi-table or alias mappings here as needed
        }

    # =============================================================================
    # HEARINGS-SPECIFIC DATA TYPE CLEANING METHODS
    # =============================================================================

    async def _clean_hearings_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual hearings records.

        FINAL COLUMNS:
        -hearing_id TEXT PRIMARY KEY,
        - hearing_jacketnumber TEXT,
        - loc_id TEXT,
        - title TEXT,
        - congress INTEGER,
        - chamber TEXT,
        - hearing_number INTEGER,
        - hearing_part INTEGER,
        - citation TEXT,
        - updated_at TIMESTAMP WITH TIME ZONE

        STAGING COLUMNS:
        Based on Congressional API hearings structure, typically includes:
        - title                       text,
        - chamber                     text,
        - batch_id                    text,
        - citation                    text,
        - congress                    text,
        - hearing_id                  text,
        - updatedate                  text,
        - jacketnumber                text,
        - libraryofcongressidentifier text,
        - number                      text,
        - associatedmeeting_url       text,
        - associatedmeeting_eventid   text,
        - part                        text

        Args:
            record_data: Raw hearings record from staging
        Returns:
            Cleaned hearings record
        """
        cleaned = record_data.copy()

        filtered_cleaned = {
            "hearing_id": cleaned.get("hearing_id", "ID_ERROR"),
            "chamber": self.standardize_chamber(cleaned.get("chamber")),
            "jacketnumber": cleaned.get("jacketnumber"),
            "hearing_part": self.safe_int(cleaned.get("part")),
            "congress": self.safe_int(cleaned.get("congress")),
            "title": cleaned.get("title"),
            "hearing_number": self.safe_int(cleaned.get("number")),
            "loc_id": cleaned.get("libraryofcongressidentifier"),
            "citation": cleaned.get("citation"),
            "meeting_id": cleaned.get("associatedmeeting_eventid"),
            "updated_at": self.standardize_date(cleaned.get("updatedate")),
        }
        return filtered_cleaned

    async def _clean_hearings_texts_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual hearings texts records.

        FINAL COLUMNS:
        - hearing_id TEXT,
        - raw_text TEXT,
        - pdf TEXT,
        - formatted_text TEXT,
        - UNIQUE (hearing_id, pdf, formatted_text)

        STAGING COLUMNS:
        - url        text,
        - type       text,
        - hearing_id text,
        - id         text not null
        """
        cleaned = record_data.copy()

        pdf_url = None
        formatted_text_url = None
        if cleaned.get("type") == "PDF":
            pdf_url = cleaned.get("url")
        elif cleaned.get("type") == "Formatted Text":
            formatted_text_url = cleaned.get("url")
        else:
            raise ValueError(f"Invalid type: {cleaned.get('type')}")

        filtered_cleaned = {
            "hearing_id": cleaned.get("hearing_id", "ID_ERROR"),
            "raw_text": None,
            "pdf": pdf_url,
            "formatted_text": formatted_text_url,
        }
        return filtered_cleaned

    async def _clean_hearings_dates_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual hearings dates records.

        FINAL COLUMNS:
        - hearing_id TEXT,
        - hearing_date TIMESTAMP WITH TIME ZONE,
        - PRIMARY KEY (hearing_id, hearing_date)

        STAGING COLUMNS:
        - date       text,
        - hearing_id text,
        - id         text not null
        - PRIMARY KEY (hearing_id, hearing_date)
        """
        cleaned = record_data.copy()

        filtered_cleaned = {
            "hearing_id": cleaned.get("hearing_id", "ID_ERROR"),
            "hearing_date": self.standardize_date(cleaned.get("date")),
        }
        return filtered_cleaned

    async def _clean_hearings_committees_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual hearings committees records.

        FINAL COLUMNS:
        - hearing_id TEXT,
        - committee_code TEXT,
        - committee_name TEXT,
        - PRIMARY KEY (hearing_id, committee_code)

        STAGING COLUMNS:
        - url        text,
        - name       text,
        - systemcode text,
        - hearing_id text,
        - id         text
        """
        cleaned = record_data.copy()

        filtered_cleaned = {
            "hearing_id": cleaned.get("hearing_id", "ID_ERROR"),
            "committee_code": cleaned.get("systemcode", "ID_ERROR"),
            "committee_name": cleaned.get("name"),
        }
        return filtered_cleaned

    # =============================================================================
    # HEARINGS-SPECIFIC POST-PROCESSING METHODS
    # =============================================================================

    async def _post_process_hearings_texts(self) -> dict[str, Any]:
        """
        Post-processing for hearings:
        - coalesce duplicate hearing_id records with complementary pdf/formatted_text
        """
        if not self.db_pool:
            raise ValueError("Database pool not configured")

        results: dict[str, Any] = {
            "status": "success",
            "operations": [],
            "rows_affected": 0,
        }

        async with self.db_pool.acquire() as conn:
            # Operation 1: Coalesce duplicate hearing_id records with complementary pdf/formatted_text
            try:
                async with conn.transaction():
                    # Create temporary view
                    await conn.execute(f"""
                        CREATE TEMPORARY VIEW coalesced AS (
                            SELECT
                                hearing_id,
                                MAX(raw_text) as raw_text,
                                MAX(pdf) as pdf,
                                MAX(formatted_text) as formatted_text
                            FROM {self.production_schema}.hearings_texts
                            GROUP BY hearing_id
                        )
                    """)

                    # Clear the table completely and capture row count
                    delete_result = await conn.execute(f"""
                        DELETE FROM {self.production_schema}.hearings_texts
                    """)

                    # Insert coalesced data from the temporary view
                    await conn.execute(f"""
                        INSERT INTO {self.production_schema}.hearings_texts (hearing_id, raw_text, pdf, formatted_text)
                        SELECT hearing_id, raw_text, pdf, formatted_text FROM coalesced
                    """)

                    # Clean up temporary view
                    await conn.execute("DROP VIEW coalesced")

                    # Parse row count from DELETE result
                    rows_affected = (
                        int(delete_result.split()[-1])
                        if delete_result and delete_result.split()
                        else 0
                    )

                    results["operations"].append(
                        {
                            "name": "coalesce_hearings_texts",
                            "status": "success",
                            "rows_affected": rows_affected,
                        }
                    )
                    results["rows_affected"] += rows_affected

                    logger.info(f"Coalesced {rows_affected} hearings_texts")

            except Exception as e:
                logger.error(f"Error coalescing hearings_texts: {e}")
                results["operations"].append(
                    {
                        "name": "coalesce_hearings_texts",
                        "status": "error",
                        "error": str(e),
                    }
                )
                results["status"] = "partial_failure"

        # Set overall status based on operations
        failed_operations = [
            op for op in results["operations"] if op["status"] == "error"
        ]
        if failed_operations:
            results["status"] = (
                "failure"
                if len(failed_operations) == len(results["operations"])
                else "partial_failure"
            )

        return results
