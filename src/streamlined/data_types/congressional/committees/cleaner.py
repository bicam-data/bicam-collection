"""
Members data cleaner.

This module handles cleaning and validation of Congressional members data,
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


class CommitteesCleaner(CongressionalBaseCleaner):
    """
    Members-specific cleaner that extends CongressionalBaseCleaner with members-specific cleaning logic.

    Provides specialized cleaning methods for all members-related data types
    and handles multi-table processing for complex relationships.
    """

    def __init__(
        self,
        config_path: str | None = None,
        db_pool: asyncpg.Pool | None = None,
        checkpoint_manager=None,
        run_manager: RunManager | None = None,
        data_type_name: str = "committees",
        staging_schema: str = "bicam_staging_congressional",
        production_schema: str = "bicam_congressional",
    ):
        """
        Initialize committees cleaner using base class.

        Args:
            config_path: Path to configuration file
            db_pool: Database connection pool
            checkpoint_manager: Checkpoint manager for progress tracking
            run_manager: Run manager for run tracking
            data_type_name: Data type name (defaults to "committees")
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
            # Add committees to enable custom joined streaming
            "committees": ["committees", "committees_list_raw"],
            # Add other multi-table or alias mappings here as needed
        }

    # =============================================================================
    # COMMITTEES-SPECIFIC DATA TYPE CLEANING METHODS
    # =============================================================================

    async def _clean_committees_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual committees records.

        FINAL COLUMNS:
        - committee_code TEXT PRIMARY KEY,
        - name TEXT,
        - chamber TEXT,
        - is_subcommittee BOOLEAN,
        - is_current BOOLEAN,
        - bills_count INTEGER,
        - reports_count INTEGER,
        - nominations_count INTEGER,
        - updated_at TIMESTAMP WITH TIME ZONE

        STAGING COLUMNS:
        Based on Congressional API committee structure, typically includes:
        - type                 text,
        - bills_url            text,
        - bills_count          text,
        - parent_url           text,
        - parent_name          text,
        - parent_systemcode    text,
        - batch_id             text,
        - iscurrent            text,
        - systemcode           text,
        - updatedate           text,
        - communications_url   text,
        - communications_count text,
        - reports_url          text,
        - reports_count        text,
        - nominations_url      text,
        - nominations_count    text

        Args:
            record_data: Raw committees record from staging
        Returns:
            Cleaned committees record
        """
        cleaned = record_data.copy()

        filtered_cleaned = {
            "committee_code": cleaned.get("systemcode", "ID_ERROR"),
            "name": cleaned.get("name"),
            "chamber": self.standardize_chamber(cleaned.get("chamber")),
            "committee_type": cleaned.get("type"),
            "is_current": cleaned.get("iscurrent") == "true",
            "bills_count": self.safe_int(cleaned.get("bills_count", 0), 0),
            "reports_count": self.safe_int(cleaned.get("reports_count", 0), 0),
            "nominations_count": self.safe_int(cleaned.get("nominations_count", 0), 0),
            "updated_at": self.standardize_date(cleaned.get("updatedate")),
        }
        return filtered_cleaned

    async def _clean_committees_committeereports_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual committees reports records.

        FINAL COLUMNS:
        - committee_code TEXT PRIMARY KEY,
        - report_id TEXT PRIMARY KEY,
        - report_type TEXT,
        - report_number FLOAT,
        - congress INTEGER,
        - chamber TEXT,
        - citation TEXT,
        - updated_at TIMESTAMP WITH TIME ZONE

        STAGING COLUMNS:
        - url            text,
        - part           text,
        - type           text,
        - number         text,
        - chamber        text,
        - batch_id       text,
        - citation       text,
        - congress       text,
        - updatedate     text,
        - committee_code text,
        - id             text not null
        """
        cleaned = record_data.copy()

        if all([cleaned.get("type"), cleaned.get("number"), cleaned.get("congress")]):
            report_id = f"{cleaned.get('type').lower()}{cleaned.get('number')}-{cleaned.get('part', '1')}-{cleaned.get('congress')}"
        else:
            report_id = "ID_ERROR"

        filtered_cleaned = {
            "committee_code": cleaned.get("committee_code", "ID_ERROR"),
            "report_id": report_id,
            "report_type": cleaned.get("type").lower(),
            "report_number": self.safe_int(cleaned.get("number", 0)),
            "congress": self.safe_int(cleaned.get("congress", 0)),
            "chamber": self.standardize_chamber(cleaned.get("chamber")),
            "citation": cleaned.get("citation"),
            "updated_at": self.standardize_date(cleaned.get("updatedate")),
        }
        return filtered_cleaned

    async def _clean_committees_subcommittees_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual committees subcommittees records.

        FINAL COLUMNS:
        - committee_code TEXT PRIMARY KEY,
        - subcommittee_code TEXT PRIMARY KEY,

        STAGING COLUMNS:
        - url            text,
        - name           text,
        - systemcode     text,
        - committee_code text,
        - id             text
        """
        cleaned = record_data.copy()

        filtered_cleaned = {
            "committee_code": cleaned.get("committee_code", "ID_ERROR"),
            "subcommittee_code": cleaned.get("systemcode", "ID_ERROR"),
        }
        return filtered_cleaned

    async def _clean_committees_history_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual committees history records.

        FINAL COLUMNS:
        - committee_code TEXT,
        - name TEXT,
        - loc_name TEXT,
        - started_at TIMESTAMP WITH TIME ZONE,
        - ended_at TIMESTAMP WITH TIME ZONE,
        - committee_type TEXT,
        - establishing_authority TEXT,
        - su_doc_class_number TEXT,
        - nara_id TEXT,
        - loc_linked_data_id TEXT,
        - updated_at TIMESTAMP WITH TIME ZONE

        STAGING COLUMNS:
        - enddate                      text,
        - startdate                    text,
        - updatedate                   text,
        - libraryofcongressname        text,
        - committee_code               text,
        - id                           text
        - officialname                 text,
        - naraid                       text,
        - loclinkeddataid              text,
        - committeetypecode            text,
        - establishingauthority        text,
        - superintendentdocumentnumber text
        """
        cleaned = record_data.copy()

        filtered_cleaned = {
            "committee_code": cleaned.get("committee_code", "ID_ERROR"),
            "name": cleaned.get("officialname"),
            "loc_name": cleaned.get("libraryofcongressname"),
            "started_at": self.standardize_date(cleaned.get("startdate")),
            "ended_at": self.standardize_date(cleaned.get("enddate")),
            "committee_type": cleaned.get("committeetypecode"),
            "establishing_authority": cleaned.get("establishingauthority"),
            "su_doc_class_number": cleaned.get("superintendentdocumentnumber"),
            "nara_id": cleaned.get("naraid"),
            "loc_linked_data_id": cleaned.get("loclinkeddataid"),
            "updated_at": self.standardize_date(cleaned.get("updatedate")),
        }
        return filtered_cleaned

    async def _stream_committees_joined_chunks(
        self, chunk_size: int
    ) -> AsyncGenerator[list[dict[str, Any]], None]:
        """
        Stream committees data with name field joined from committees_list_raw table.

        This method joins the main committees staging table with the committees_list_raw
        table to include the 'name' field that's only available in the list data.
        """
        if not self.db_pool:
            logger.error("No database pool available for streaming")
            return

        async with self.db_pool.acquire() as conn:
            # Check if both tables exist
            committees_exists = await conn.fetchval(
                """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = $1 AND table_name = $2
                )
                """,
                self.staging_schema,
                "committees",
            )

            list_raw_exists = await conn.fetchval(
                """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = $1 AND table_name = $2
                )
                """,
                "bicam_raw_congressional",
                "committees_list_raw",
            )

            if not committees_exists:
                logger.warning(f"Table {self.staging_schema}.committees does not exist")
                return

            if not list_raw_exists:
                logger.warning(
                    "Table bicam_raw_congressional.committees_list_raw does not exist"
                )
                # Fallback to single table streaming
                async for chunk in self._stream_single_table_chunks(
                    "committees", chunk_size
                ):
                    yield chunk
                return

            logger.debug("Streaming committees with joined name data from list_raw")

            offset = 0
            while True:
                # Join committees staging with committees_list_raw to get name field
                query = f"""
                    SELECT
                        c.*,
                        (lr.payload->>'name')::text as name,
                        (lr.payload->>'chamber')::text as chamber
                    FROM {self.staging_schema}.committees c
                    LEFT JOIN bicam_raw_congressional.committees_list_raw lr
                        ON c.systemcode = lr.source_doc_id
                    LIMIT $1 OFFSET $2
                """

                try:
                    rows = await conn.fetch(query, chunk_size, offset)
                    if not rows:
                        break

                    chunk = [dict(row) for row in rows]
                    yield chunk

                    offset += len(chunk)

                    # If we got fewer rows than requested, we're at the end
                    if len(chunk) < chunk_size:
                        break

                except Exception as e:
                    logger.error(
                        f"Error streaming committees with joined data at offset {offset}: {e}"
                    )
                    break

    async def _stream_single_table_chunks(
        self, table_name: str, chunk_size: int
    ) -> AsyncGenerator[list[dict[str, Any]], None]:
        """Fallback method to stream from a single table when join fails."""
        async with self.db_pool.acquire() as conn:
            offset = 0
            while True:
                query = f"""
                    SELECT * FROM {self.staging_schema}.{table_name}
                    LIMIT $1 OFFSET $2
                """

                try:
                    rows = await conn.fetch(query, chunk_size, offset)
                    if not rows:
                        break

                    chunk = [dict(row) for row in rows]
                    yield chunk

                    offset += len(chunk)

                    if len(chunk) < chunk_size:
                        break

                except Exception as e:
                    logger.error(
                        f"Error streaming {table_name} at offset {offset}: {e}"
                    )
                    break

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
    #         # Operation 1: Populate current_party field based on members_partyhistory table
    #         try:
    #             async with conn.transaction():
    #                 sql = f"""
    #                     UPDATE {self.production_schema}.members
    #                     SET current_party = (SELECT party_name
    #                                 FROM {self.production_schema}.members_partyhistory mh
    #                                 WHERE mh.bioguide_id = members.bioguide_id
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

    #                 logger.info(f"Updated current_party for {rows_affected} members")

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

    #         # Operation 2: Add chamber to members_leadershiproles table
    #         try:
    #             async with conn.transaction():
    #                 sql = f"""
    #                     UPDATE {self.production_schema}.members_leadershiproles
    #                     SET chamber = (SELECT chamber
    #                                 FROM {self.production_schema}.members_terms mt
    #                                 WHERE mt.bioguide_id = members_leadershiproles.bioguide_id
    #                                 AND mt.congress = members_leadershiproles.congress
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
    #                     f"Updated chamber for {rows_affected} members_leadershiproles"
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
