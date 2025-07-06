"""
committeereports data cleaner.

This module handles cleaning and validation of Congressional committeereports data,
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

import asyncpg

from ....libs.run_tracking import RunManager
from ..base import CongressionalBaseCleaner

logger = logging.getLogger(__name__)


class CommitteereportsCleaner(CongressionalBaseCleaner):
    """
    Committeereports-specific cleaner that extends CongressionalBaseCleaner with committeereports-specific cleaning logic.

    Provides specialized cleaning methods for all committeereports-related data types
    and handles multi-table processing for complex relationships.
    """

    def __init__(
        self,
        config_path: str | None = None,
        db_pool: asyncpg.Pool | None = None,
        checkpoint_manager=None,
        run_manager: RunManager | None = None,
        data_type_name: str = "committeereports",
        staging_schema: str = "bicam_staging_congressional",
        production_schema: str = "bicam_congressional",
    ):
        """
        Initialize committeereports cleaner using base class.

        Args:
            config_path: Path to configuration file
            db_pool: Database connection pool
            checkpoint_manager: Checkpoint manager for progress tracking
            run_manager: Run manager for run tracking
            data_type_name: Data type name (defaults to "committeereports")
            staging_schema: Source staging schema name
            production_schema: Target production schema name
        """
        super().__init__(
            config_path=config_path,
            db_pool=db_pool,
            checkpoint_manager=checkpoint_manager,
            run_manager=run_manager,
            data_type_name=data_type_name,
            staging_schema=staging_schema,
            production_schema=production_schema,
        )

        # Map data types that span or alias multiple staging tables.
        # In staging, leadership-role data lives in `reports_leadership`,
        # but the logical/production data-type name we expose is
        # `reports_leadershiproles`.  This mapping lets the base cleaner
        # stream from the correct table without emitting missing-table warnings.
        self.multi_table_data_types = {
            # Add committeereports to enable custom joined streaming
            "committeereports_texts": ["committeereports_texts_formats"],
            # Add other multi-table or alias mappings here as needed
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


    # =============================================================================
    # COMMITTEES-SPECIFIC POST-PROCESSING METHODS
    # =============================================================================

    # async def _post_process_committees(self) -> dict[str, Any]:
    #     """
    #     Post-processing for committees:
    #     - add name to committees table
    #     """
    #     if not self.db_pool:
    #         raise ValueError("Database pool not configured")

    #     results: dict[str, Any] = {
    #         "status": "success",
    #         "operations": [],
    #         "rows_affected": 0,
    #     }

    #     async with self.db_pool.acquire() as conn:
    #         # Operation 1: Populate current_party field based on reports_partyhistory table
    #         try:
    #             async with conn.transaction():
    #                 sql = f"""
    #                     UPDATE {self.production_schema}.reports
    #                     SET current_party = (SELECT party_name
    #                                 FROM {self.production_schema}.reports_partyhistory mh
    #                                 WHERE mh.bioguide_id = reports.bioguide_id
    #                                 AND mh.end_year IS NULL
    #                             )
    #                     WHERE bioguide_id IS NOT NULL;
    #                 """

    #                 result = await conn.execute(sql)
    #                 rows_affected = int(result.split()[-1]) if result.split() else 0

    #                 results["operations"].append(
    #                     {
    #                         "name": "get_current_party",
    #                         "status": "success",
    #                         "rows_affected": rows_affected,
    #                     }
    #                 )
    #                 results["rows_affected"] += rows_affected

    #                 logger.info(f"Updated current_party for {rows_affected} reports")

    #         except Exception as e:
    #             logger.error(f"Error getting current_party: {e}")
    #             results["operations"].append(
    #                 {
    #                     "name": "get_current_party",
    #                     "status": "error",
    #                     "error": str(e),
    #                 }
    #             )
    #             results["status"] = "partial_failure"

    #         # Operation 2: Add chamber to reports_leadershiproles table
    #         try:
    #             async with conn.transaction():
    #                 sql = f"""
    #                     UPDATE {self.production_schema}.reports_leadershiproles
    #                     SET chamber = (SELECT chamber
    #                                 FROM {self.production_schema}.reports_terms mt
    #                                 WHERE mt.bioguide_id = reports_leadershiproles.bioguide_id
    #                                 AND mt.congress = reports_leadershiproles.congress
    #                             )
    #                     WHERE bioguide_id IS NOT NULL
    #                     AND chamber IS NULL;
    #                 """

    #                 result = await conn.execute(sql)
    #                 rows_affected = int(result.split()[-1]) if result.split() else 0

    #                 results["operations"].append(
    #                     {
    #                         "name": "get_chamber",
    #                         "status": "success",
    #                         "rows_affected": rows_affected,
    #                     }
    #                 )
    #                 results["rows_affected"] += rows_affected

    #                 logger.info(
    #                     f"Updated chamber for {rows_affected} reports_leadershiproles"
    #                 )

    #         except Exception as e:
    #             logger.error(f"Error getting chamber: {e}")
    #             results["operations"].append(
    #                 {
    #                     "name": "get_chamber",
    #                     "status": "error",
    #                     "error": str(e),
    #                 }
    #             )
    #             results["status"] = "partial_failure"

    #     # Set overall status based on operations
    #     failed_operations = [
    #         op for op in results["operations"] if op["status"] == "error"
    #     ]
    #     if failed_operations:
    #         results["status"] = (
    #             "failure"
    #             if len(failed_operations) == len(results["operations"])
    #             else "partial_failure"
    #         )

    #     return results
