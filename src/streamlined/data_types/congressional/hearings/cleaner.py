"""
Hearings data cleaner.

This module handles cleaning and validation of Congressional hearings data,
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
from collections.abc import AsyncGenerator
from typing import Any

import asyncpg

from ....libs.run_tracking import RunManager
from ..base import CongressionalBaseCleaner

logger = logging.getLogger(__name__)


class HearingsCleaner(CongressionalBaseCleaner):
    """
    Hearings-specific cleaner that extends CongressionalBaseCleaner with hearings-specific cleaning logic.

    Provides specialized cleaning methods for all hearings-related data types
    and handles multi-table processing for complex relationships.
    """

    def __init__(
        self,
        config_path: str | None = None,
        db_pool: asyncpg.Pool | None = None,
        checkpoint_manager=None,
        run_manager: RunManager | None = None,
        data_type_name: str = "hearings",
        staging_schema: str = "bicam_staging_congressional",
        production_schema: str = "bicam_congressional",
    ):
        """
        Initialize hearings cleaner using base class.

        Args:
            config_path: Path to configuration file
            db_pool: Database connection pool
            checkpoint_manager: Checkpoint manager for progress tracking
            run_manager: Run manager for run tracking
            data_type_name: Data type name (defaults to "hearings")
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
        # In staging, leadership-role data lives in `members_leadership`,
        # but the logical/production data-type name we expose is
        # `members_leadershiproles`.  This mapping lets the base cleaner
        # stream from the correct table without emitting missing-table warnings.
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
