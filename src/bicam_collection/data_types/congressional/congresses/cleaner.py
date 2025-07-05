"""
Congresses data cleaner.

This module handles cleaning and validation of Congressional congresses data,
extracting from staging tables, cleaning/standardizing data, and storing
to production bicam_congressional schema with proper primary keys.

ARCHITECTURE:
1. **Streaming Extraction**: Stream data types in chunks to avoid memory issues
2. **Parallel Processing**: Process data types concurrently across available cores
3. **Data Cleaning**: Apply data type-specific cleaning and standardization
4. **Production Storage**: Store to bicam_congressional schema with PKs only (no FKs)

PERFORMANCE FEATURES:
- Parallel processing across data types (max_cores - 4)
- Streaming processing to handle large datasets efficiently
- Progress tracking with checkpoints
- Transaction safety for atomic operations
"""

import logging
from typing import Any

from ...abstract.base_cleaner import BaseCleaner

logger = logging.getLogger(__name__)


class CongressesCleaner(BaseCleaner):
    """
    Congresses-specifi   cleaner that extends BaseCleaner with congresses-specific cleaning logic.

    Provides specialized cleaning methods for all congresses-related data types
    and handles multi-table processing for complex relationships.
    """

    def __init__(
        self,
        data_type_name: str = "congresses",
        system_name: str = "congressional",
        staging_schema: str = "bicam_staging_congressional",
        production_schema: str = "bicam_congressional",
        **kwargs,
    ):
        """
        Initialize congresses cleaner using base class.

        Args:
            data_type_name: Data type name (defaults to "congresses")
            staging_schema: Source staging schema name
            production_schema: Target production schema name
        """
        super().__init__(
            data_type_name=data_type_name,
            staging_schema=staging_schema,
            production_schema=production_schema,
            system_name=system_name,
            **kwargs,
        )

        # Map data types that span or alias multiple staging tables.
        # In staging, leadership-role data lives in `members_leadership`,
        # but the logical/production data-type name we expose is
        # `members_leadershiproles`.  This mapping lets the base cleaner
        # stream from the correct table without emitting missing-table warnings.
        self.multi_table_data_types = {
            # Add other multi-table or alias mappings here as needed
        }

    # =============================================================================
    # CONGRESS-SPECIFIC DATA TYPE CLEANING METHODS
    # =============================================================================

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
            "congress_number": self.safe_int(cleaned.get("congress_number", -99), -99),
            "name": cleaned.get("name"),
            "start_year": self.safe_int(cleaned.get("startyear")),
            "end_year": self.safe_int(cleaned.get("endyear")),
            "updated_at": self.standardize_date(cleaned.get("updatedate")),
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

        filtered_cleaned = {
            "congress_number": self.safe_int(cleaned.get("congress_number", -99), -99),
            "session": self.safe_int(cleaned.get("number")),
            "chamber": self.standardize_chamber(cleaned.get("chamber")),
            "type": session_type,
            "start_date": self.standardize_date(cleaned.get("startdate")),
            "end_date": self.standardize_date(cleaned.get("enddate")),
        }
        return filtered_cleaned

    # =============================================================================
    # CONGRESS-SPECIFIC POST-PROCESSING METHODS
    # =============================================================================

    # async def _post_process_congresses(self) -> dict[str, Any]:
    #     """
    #     Post-process congresses data.
    #     """
    #     return {}
