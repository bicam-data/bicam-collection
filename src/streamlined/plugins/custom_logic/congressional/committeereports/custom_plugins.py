"""
Committeereports Custom Plugin Logic

This module contains all the custom logic for bills data type including:
- Fetching logic (from fetcher.py)
- Database normalization logic (from database_normalizer.py)
- Cleaning logic (from cleaner.py)

All logic is organized into classes that can be used by the plugin system.
"""

import logging
from typing import Any

# Import the base class that provides shared functionality
from ....base import BaseCleanerLogic, CongressionalBaseFetcherLogic

logger = logging.getLogger(__name__)


class CommitteereportsFetcherLogic(CongressionalBaseFetcherLogic):
    """
    Committeereports-specific fetcher logic that extends the base Congressional fetcher logic.

    This class provides custom methods for fetching committeereports data, including:
    - Extracting standardized committeereport IDs
    - Handling related data from URLs
    - Implementing specialized fetching logic for committeereports-specific data

    Inherits shared functionality like get_generic_related_data from
    CongressionalBaseFetcherLogic.
    """

    def __init__(self, data_type: str = "committeereports"):
        super().__init__(data_type)

        # =============================================================================

    # REQUIRED ABSTRACT METHOD IMPLEMENTATIONS
    # =============================================================================

    def extract_item_id(self, item_data: dict[str, Any], **kwargs) -> str:
        """Extract standardized committeereport ID from committeereport data."""
        item_data = item_data[0] if isinstance(item_data, list) else item_data
        report_number = item_data.get("number")
        congress = item_data.get("congress")
        report_type = item_data.get("type")
        report_part = item_data.get("part", "1")
        if all([report_number, congress, report_type]):
            return f"{report_type.lower()}{report_number}-{report_part}-{congress}"
        else:
            logger.info(f"Error extracting item ID: {item_data}")
            return "ID_ERROR"

    def _extract_preliminary_item_id(self, list_item: dict[str, Any]) -> str:
        """
        Derive a stable ID for checkpointing directly from the list-response record.
        This guarantees a non-null value so the progress-tracker can reliably
        decide whether the item was processed in a previous run.
        """
        return self.extract_item_id(list_item)

    # =============================================================================
    # COMMITTEEREPORTS-SPECIFIC RELATED DATA METHODS
    # =============================================================================

    async def get_committeereports_texts(
        self, full_committeereport_data: dict[str, Any], client
    ) -> list[dict[str, Any]]:
        """Get texts from texts URL in full committeereport data."""
        return await self.get_generic_related_data(
            full_committeereport_data, related_table_name="text", client=client
        )


class CommitteereportsCleanerLogic(BaseCleanerLogic):
    """
    Committeereports-specific cleaner logic extracted from CommitteereportsCleaner class.
    Contains all the custom cleaning methods for committeereports data.
    """

    def __init__(
        self,
        data_type_name: str = "committeereports",
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
            "committeereports_texts": ["committeereports_texts_formats"],
            # Add other committeereports multi-table data types here as needed
        }

    # =============================================================================
    # COMMITTEEREPORTS-SPECIFIC DATA TYPE CLEANING METHODS
    # =============================================================================

    async def _clean_committeereports_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
            Custom cleaning logic for individual committeereports records.

            FINAL COLUMNS:
            - report_id TEXT PRIMARY KEY,
            - citation TEXT,
            - report_type TEXT,
            - report_number INTEGER,
            - report_part INTEGER,
            - congress INTEGER,
            - session INTEGER,
            - title TEXT,
            - chamber TEXT,
            - is_conference_report BOOLEAN,
            - issued_at TIMESTAMP WITH TIME ZONE,
            - texts_count INTEGER,
            - updated_at TIMESTAMP WITH TIME ZONE

            STAGING COLUMNS:
            Based on Congressional API committee structure, typically includes:
            - part               text,
            - text_url           text,
            - text_count         text,
            - type               text,
            - title              text,
            - number             text,
            - chamber            text,
            - batch_id           text,
            - citation           text,
            - congress           text,
            - issuedate          text,
            - report_id          text
            - reporttype         text,
        -   updatedate         text,
            - sessionnumber      text,
            - isconferencereport text,


            Args:
                record_data: Raw committeereports record from staging
            Returns:
                Cleaned committeereports record
        """
        cleaned = record_data.copy()

        filtered_cleaned = {
            "report_id": cleaned.get("report_id", "ID_ERROR"),
            "report_type": cleaned.get("type").lower(),
            "report_number": self.safe_int(cleaned.get("number")),
            "report_part": self.safe_int(cleaned.get("part", 1), 1),
            "congress": self.safe_int(cleaned.get("congress")),
            "session": self.safe_int(cleaned.get("sessionnumber")),
            "title": cleaned.get("title"),
            "chamber": self.standardize_chamber(cleaned.get("chamber")),
            "is_conference_report": cleaned.get("isconferencereport") == "True",
            "issued_at": self.standardize_date(cleaned.get("issuedate")),
            "citation": cleaned.get("citation"),
            "texts_count": self.safe_int(cleaned.get("text_count", 0), 0),
            "updated_at": self.standardize_date(cleaned.get("updatedate")),
        }
        return filtered_cleaned

    async def _clean_committeereports_committees_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual committeereports committees records.

        FINAL COLUMNS:
        - report_id
        - committee_code
        - committee_name

        STAGING COLUMNS:
        - url        text,
        - name       text,
        - systemcode text,
        - report_id  text,
        - id         text not null
        """
        cleaned = record_data.copy()

        filtered_cleaned = {
            "report_id": cleaned.get("report_id", "ID_ERROR"),
            "committee_code": cleaned.get("systemcode", "ID_ERROR"),
            "committee_name": cleaned.get("name"),
        }
        return filtered_cleaned

    async def _clean_committeereports_associatedbill_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual committeereports bills records.

        FINAL COLUMNS:
        - report_id
        - bill_id
        - bill_type
        - bill_number
        - congress

        STAGING COLUMNS:
        - url       text,
        - type      text,
        - number    text,
        - congress  text,
        - report_id text,
        - id        text not null
        """
        self._register_target_table_override("committeereports_bills")

        cleaned = record_data.copy()

        bill_type = cleaned.get("type").lower()

        bill_number = cleaned.get("number")
        if bill_number and "½" in bill_number:
            bill_number = bill_number.replace("½", ".5")

        congress = cleaned.get("congress")

        if all([bill_number, bill_type, congress]):
            bill_id = f"{bill_type}{bill_number}-{congress}"
        else:
            bill_id = "ID_ERROR"

        filtered_cleaned = {
            "report_id": cleaned.get("report_id", "ID_ERROR"),
            "bill_id": bill_id,
            "bill_type": bill_type,
            "bill_number": self.safe_int(bill_number, set_to_float=True),
            "congress": self.safe_int(congress),
        }
        return filtered_cleaned

    async def _clean_committeereports_associatedtreaties_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual committeereports treaties records.

        FINAL COLUMNS:
        - report_id
        - treaty_id
        - treaty_number
        - congress

        STAGING COLUMNS:
        - url       text,
        - number    text,
        - congress  text,
        - report_id text,
        - id        text not null
        """

        self._register_target_table_override("committeereports_treaties")
        cleaned = record_data.copy()

        treaty_number = cleaned.get("number")
        congress = cleaned.get("congress")

        treaty_id = f"td{congress}-{treaty_number}"

        filtered_cleaned = {
            "report_id": cleaned.get("report_id", "ID_ERROR"),
            "treaty_id": treaty_id,
            "treaty_number": treaty_number,
            "congress": self.safe_int(congress),
        }
        return filtered_cleaned

    async def _clean_committeereports_texts_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual committeereports texts records.

        FINAL COLUMNS:
        - report_id
        - raw_text
        - formatted_text
        - pdf
        - pdf_is_errata
        - formatted_text
        - formatted_text_is_errata

        STAGING COLUMNS:
        - url       text,
        - type      text,
        - iserrata  text,
        - texts_id  text,
        - report_id text,
        - id
        """
        cleaned = record_data.copy()

        pdf_url = None
        formatted_text_url = None
        pdf_is_errata = None
        formatted_text_is_errata = None
        if cleaned.get("type") == "PDF":
            pdf_url = cleaned.get("url")
            if cleaned.get("iserrata") not in ["Y", "N"]:
                pdf_is_errata = None
            else:
                pdf_is_errata = cleaned.get("iserrata") == "Y"
        elif cleaned.get("type") == "Formatted Text":
            formatted_text_url = cleaned.get("url")
            if cleaned.get("iserrata") not in ["Y", "N"]:
                formatted_text_is_errata = None
            else:
                formatted_text_is_errata = cleaned.get("iserrata") == "Y"

        filtered_cleaned = {
            "report_id": cleaned.get("report_id", "ID_ERROR"),
            "raw_text": None,
            "pdf": pdf_url,
            "pdf_is_errata": pdf_is_errata,
            "formatted_text": formatted_text_url,
            "formatted_text_is_errata": formatted_text_is_errata,
        }
        return filtered_cleaned
