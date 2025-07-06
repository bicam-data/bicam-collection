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
from typing import Any

# Import the base class that provides shared functionality
from ....base import BaseCleaningUtilities as utils
from ....base import CongressionalBaseFetcherLogic

logger = logging.getLogger(__name__)


class CongressesFetcherLogic(CongressionalBaseFetcherLogic):
    """
    Congresses-specific fetcher logic that extends the base Congressional fetcher logic.

    This class provides custom methods for fetching congresses data, including:
    - Extracting standardized bill IDs
    - Handling related data from URLs
    - Implementing specialized fetching logic for congresses-specific data

    Inherits shared functionality like get_generic_related_data from
    CongressionalBaseFetcherLogic.
    """

    def __init__(self, data_type: str = "congresses"):
        super().__init__(data_type)

    def extract_item_id(self, item_data: dict[str, Any]) -> str:
        """Extract standardized congress ID from congress data."""
        return str(item_data.get("number", "ID_ERROR"))

    def _extract_preliminary_item_id(self, list_item: dict[str, Any]) -> str:
        """
        Derive a stable ID for checkpointing directly from the list-response record.
        This guarantees a non-null value so the progress-tracker can reliably
        decide whether the item was processed in a previous run.
        """

        # 1. direct field
        prelim_id_field = list_item.get("name")
        if prelim_id_field:
            # only take the digits before the first space
            prelim_id_prefix = prelim_id_field.split(" ")[0]
            prelim_id = re.sub(r"\D", "", prelim_id_prefix)
            return str(prelim_id)
        else:
            return "ID_ERROR"

class CongressesCleanerLogic:
    """
    Congresses-specific cleaner logic extracted from CongressesCleaner class.
    Contains all the custom cleaning methods for congresses data.
    """

    def __init__(
        self,
        data_type_name: str = "congresses",
        system_name: str = "congressional",
        staging_schema: str = "bicam_staging_congressional",
        production_schema: str = "bicam_congressional",
    ):
        self.data_type_name = data_type_name
        self.system_name = system_name
        self.staging_schema = staging_schema
        self.production_schema = production_schema
        self.db_pool = None

        # Set congresses-specific multi-table processing configuration
        self.multi_table_data_types = {
            # Add other congresses multi-table data types here as needed
        }

    async def _clean_congresses_singular(
            self, record_data: dict[str, Any]
        ) -> dict[str, Any]:
            """
            Custom cleaning logic for individual congresses records.

            FINAL COLUMNS:
            - congress_number TEXT PRIMARY KEY,
            - name TEXT,
            - start_year INTEGER,
            - end_year INTEGER,
            - updated_at TIMESTAMP WITH TIME ZONE

            STAGING COLUMNS:
            Based on Congressional API congress structure, typically includes:
            - url             text,
            - name            text,
            - number          text,
            - endyear         text,
            - batch_id        text,
            - startyear       text,
            - updatedate      text,
            - congress_number text

            Args:
                record_data: Raw congresses record from staging
            Returns:
                Cleaned congresses record
            """
            cleaned = record_data.copy()

            filtered_cleaned = {
                "congress_number": utils.safe_int(cleaned.get("congress_number", 0), 0),
                "name": cleaned.get("name"),
                "start_year": utils.safe_int(cleaned.get("startyear")),
                "end_year": utils.safe_int(cleaned.get("endyear")),
                "updated_at": utils.standardize_date(cleaned.get("updatedate")),
            }
            return filtered_cleaned

    async def _clean_congresses_sessions_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual congresses sessions records.

        FINAL COLUMNS:
        - congress_number INTEGER,
        - session INTEGER,
        - chamber TEXT,
        - type TEXT,
        - start_date DATE,
        - end_date DATE,
        - UNIQUE (congress_number, session, chamber, type)

        STAGING COLUMNS:
        - type            text,
        - number          text,
        - chamber         text,
        - startdate       text,
        - congress_number text,
        - id              text
        - enddate         text
        """
        cleaned = record_data.copy()
        session_type = cleaned.get("type")
        if session_type == "R":
            session_type = "regular"
        elif session_type == "S":
            session_type = "special"
        else:
            session_type = session_type.lower()

        logger.info(f"Cleaned for sessions: {cleaned}")

        filtered_cleaned = {
            "congress_number": utils.safe_int(cleaned.get("congress_number", 0), 0),
            "session": utils.safe_int(cleaned.get("number")),
            "chamber": utils.standardize_chamber(cleaned.get("chamber")),
            "type": session_type,
            "start_date": utils.standardize_date(cleaned.get("startdate")),
            "end_date": utils.standardize_date(cleaned.get("enddate")),
        }
        return filtered_cleaned