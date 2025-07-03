"""
Nominations data cleaner.

This module handles cleaning and validation of Congressional nominations data,
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


class NominationsCleaner(CongressionalBaseCleaner):
    """
    Nominations-specific cleaner that extends CongressionalBaseCleaner with nominations-specific cleaning logic.

    Provides specialized cleaning methods for all nominations-related data types
    and handles multi-table processing for complex relationships.
    """

    def __init__(
        self,
        config_path: str | None = None,
        db_pool: asyncpg.Pool | None = None,
        checkpoint_manager=None,
        run_manager: RunManager | None = None,
        data_type_name: str = "nominations",
        staging_schema: str = "bicam_staging_congressional",
        production_schema: str = "bicam_congressional",
    ):
        """
        Initialize nominations cleaner using base class.

        Args:
            config_path: Path to configuration file
            db_pool: Database connection pool
            checkpoint_manager: Checkpoint manager for progress tracking
            run_manager: Run manager for run tracking
            data_type_name: Data type name (defaults to "nominations")
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
            "nominations_positions": ["nominations_nominees"],
            # Add nominations to enable custom joined streaming
            # Add other multi-table or alias mappings here as needed
        }

    # =============================================================================
    # NOMINATIONS-SPECIFIC DATA TYPE CLEANING METHODS
    # =============================================================================

    async def _clean_nominations_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual nominations records.

        Args:
            record_data: Raw nominations record from staging
        Returns:
            Cleaned nominations record

        FINAL COLUMNS:
        - nomination_id TEXT PRIMARY KEY,
        - nomination_number TEXT,
        - part_number TEXT,
        - congress INTEGER,
        - description TEXT,
        - is_privileged BOOLEAN,
        - is_civilian BOOLEAN,
        - received_at DATE,
        - authority_date DATE,
        - executive_calendar_number TEXT,
        - citation TEXT,
        - committees_count INTEGER,
        - actions_count INTEGER,
        - updated_at TIMESTAMP WITH TIME ZONE

        STAGING COLUMNS:
        - nomination_id             text,
        - number                    text,
        - actions_url               text,
        - actions_count             text,
        - batch_id                  text,
        - citation                  text,
        - congress                  text,
        - committees_url            text,
        - committees_count          text,
        - partnumber                text,
        - updatedate                text,
        - description               text,
        - latestaction_text         text,
        - latestaction_actiondate   text,
        - receiveddate              text,
        - authoritydate             text,
        - nominationtype_iscivilian text,
        - executivecalendarnumber   text,
        - islist                    text,
        - nominationtype_ismilitary text,
        - hearings_url              text,
        - hearings_count            text,
        - isprivileged              text
        """
        cleaned = record_data.copy()
        filtered_cleaned = {
            "nomination_id": cleaned.get("nomination_id", "ID_ERROR"),
            "nomination_number": cleaned.get("number"),
            "nomination_part": cleaned.get("partnumber", "00"),
            "congress": self.safe_int(cleaned.get("congress")),
            "description": cleaned.get("description"),
            "is_privileged": cleaned.get("isprivileged") == "true",
            "is_civilian": cleaned.get("nominationtype_iscivilian") == "true",
            "is_special": cleaned.get("islist") == "true",
            "received_at": self.standardize_date(cleaned.get("receiveddate")),
            "authority_date": self.standardize_date(cleaned.get("authoritydate")),
            "executive_calendar_number": cleaned.get("executivecalendarnumber"),
            "citation": cleaned.get("citation"),
            "committees_count": self.safe_int(cleaned.get("committees_count", 0), 0),
            "actions_count": self.safe_int(cleaned.get("actions_count", 0), 0),
            "updated_at": self.standardize_date(cleaned.get("updatedate")),
        }
        return filtered_cleaned

    async def _clean_nominations_actions_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual nominations actions records.

        STAGING COLUMNS:
        - text          text,
        - type          text,
        - batch_id      text,
        - actioncode    text,
        - actiondate    text,
        - nomination_id text,
        - id            text,

        FINAL COLUMNS:
        - action_id TEXT PRIMARY KEY,
        - nomination_id TEXT,
        - action_code TEXT,
        - action_type TEXT,
        - action_date DATE,
        - text TEXT,
        - UNIQUE (action_id, nomination_id)
        """
        cleaned = record_data.copy()
        filtered_cleaned = {
            "action_id": cleaned.get("id", "ID_ERROR"),
            "nomination_id": cleaned.get("nomination_id", "ID_ERROR"),
            "action_code": cleaned.get("actioncode"),
            "action_type": cleaned.get("type"),
            "action_date": self.standardize_date(cleaned.get("actiondate")),
            "text": cleaned.get("text"),
        }
        return filtered_cleaned

    async def _clean_nominations_actions_committees_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual nominations actions committees records.

        STAGING COLUMNS:
        - url           text,
        - name          text,
        - systemcode    text,
        - actions_id    text,
        - nomination_id text,
        - id            text,

        FINAL COLUMNS:
        - action_id TEXT,
        - nomination_id TEXT,
        - committee_code TEXT,
        - committee_name TEXT,
        - PRIMARY KEY (action_id, nomination_id, committee_code)

        """
        cleaned = record_data.copy()
        filtered_cleaned = {
            "action_id": cleaned.get("actions_id", "ID_ERROR"),
            "nomination_id": cleaned.get("nomination_id", "ID_ERROR"),
            "committee_code": cleaned.get("systemcode"),
            "committee_name": cleaned.get("name"),
        }
        return filtered_cleaned

    async def _clean_nominations_hearings_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual nominations hearings records.

        STAGING COLUMNS:
        - date          text,
        - number        text,
        - chamber       text,
        - batch_id      text,
        - citation      text,
        - jacketnumber  text,
        - nomination_id text,
        - id            text
        - partnumber    text

        FINAL COLUMNS:
        - nomination_id TEXT,
        - hearing_date DATE,
        - hearing_number TEXT,
        - hearing_chamber TEXT,
        - hearing_batch_id TEXT,
        """
        cleaned = record_data.copy()

        hearing_jacketnumber = cleaned.get("jacketnumber")
        hearing_chamber = cleaned.get("chamber")
        hearing_chamber_prefix = hearing_chamber[0].lower() if hearing_chamber else None
        citation_split = cleaned.get("citation").split(".")[2]
        hearing_congress = citation_split.split("-")[0]

        if all([hearing_jacketnumber, hearing_chamber_prefix, hearing_congress]):
            hearing_id = f"{hearing_chamber_prefix}hrg{hearing_jacketnumber}-{hearing_congress}"
        else:
            hearing_id = "ID_ERROR"

        filtered_cleaned = {
            "nomination_id": cleaned.get("nomination_id", "ID_ERROR"),
            "hearing_id": hearing_id,
            "chamber": self.standardize_chamber(hearing_chamber),
            "jacketnumber": hearing_jacketnumber,
            "congress": self.safe_int(hearing_congress),
            "hearing_number": self.safe_int(cleaned.get("number")),
            "hearing_part": self.safe_int(cleaned.get("partnumber")),
            "hearing_date": self.standardize_date(cleaned.get("date")),
            "citation": cleaned.get("citation"),
        }
        return filtered_cleaned

    async def _clean_nominations_individualnominees_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual nominations individual nominees records.

        STAGING COLUMNS:
        - state           text,
        - ordinal         text,
        - batch_id        text,
        - lastname        text,
        - firstname       text,
        - position_id     text,
        - nomination_id   text,
        - id              text
        - middlename      text,
        - suffix          text,
        - prefix          text,
        - predecessorname text,
        - corpscode       text,
        - effectivedate   text

        FINAL COLUMNS:
        - nomination_id TEXT,
        - position_id TEXT,
        - nominee_ordinal INTEGER,
        - first_name TEXT,
        - middle_name TEXT,
        - last_name TEXT,
        - prefix TEXT,
        - suffix TEXT,
        - state TEXT,
        - effective_date DATE,
        - predecessor_name TEXT,
        - corps_code TEXT,
        - UNIQUE (nomination_id, position_id, nominee_ordinal)
        """
        self._register_target_table_override("nominations_nominees")
        cleaned = record_data.copy()
        filtered_cleaned = {
            "nomination_id": cleaned.get("nomination_id", "ID_ERROR"),
            "position_id": self.safe_int(cleaned.get("position_id", "ID_ERROR")),
            "nominee_ordinal": self.safe_int(cleaned.get("ordinal")),
            "first_name": cleaned.get("firstname"),
            "middle_name": cleaned.get("middlename"),
            "last_name": cleaned.get("lastname"),
            "prefix": cleaned.get("prefix"),
            "suffix": cleaned.get("suffix"),
            "state": cleaned.get("state"),
            "effective_date": self.standardize_date(cleaned.get("effectivedate")),
            "predecessor_name": cleaned.get("predecessorname"),
            "corps_code": cleaned.get("corpscode"),
        }
        return filtered_cleaned

    async def _clean_nominations_committeeactivities_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual nominations committee activities records.

        STAGING COLUMNS:
        - name           text,
        - type           text,
        - chamber        text,
        - batch_id       text,
        - activity_date  text,
        - activity_name  text,
        - nomination_id  text,
        - committee_code text,
        - id             text

        FINAL COLUMNS:
        - nomination_id TEXT,
        - committee_code TEXT,
        - committee_name TEXT,
        - committee_type TEXT,
        - chamber TEXT,
        - activity_name TEXT,
        - activity_date TIMESTAMP WITH TIME ZONE,
        - UNIQUE (nomination_id, committee_code, activity_date, activity_name)
        """
        cleaned = record_data.copy()
        filtered_cleaned = {
            "nomination_id": cleaned.get("nomination_id", "ID_ERROR"),
            "committee_code": cleaned.get("committee_code", "ID_ERROR"),
            "committee_name": cleaned.get("name"),
            "committee_type": cleaned.get("type"),
            "chamber": cleaned.get("chamber"),
            "activity_name": cleaned.get("activity_name"),
            "activity_date": self.standardize_date(cleaned.get("activity_date")),
        }
        return filtered_cleaned

    async def _clean_nominations_positions_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual nominations positions records.

        STAGING COLUMNS:
        - url           text,
        - ordinal       text,
        - introtext     text,
        - nomineecount  text,
        - organization  text,
        - positiontitle text,
        - nomination_id text,
        - id            text
        - division      text

        FINAL COLUMNS:
        - nomination_id TEXT,
        - position_id INTEGER,
        - position_title TEXT,
        - organization TEXT,
        - division TEXT,
        - intro_text TEXT,
        - nominee_count INTEGER,
        - UNIQUE (nomination_id, position_id)
        """
        cleaned = record_data.copy()
        filtered_cleaned = {
            "nomination_id": cleaned.get("nomination_id", "ID_ERROR"),
            "position_id": self.safe_int(cleaned.get("ordinal", "ID_ERROR")),
            "position_title": cleaned.get("positiontitle"),
            "organization": cleaned.get("organization"),
            "division": cleaned.get("division"),
            "intro_text": cleaned.get("introtext"),
            "nominee_count": self.safe_int(cleaned.get("nomineecount", 0), 0),
        }
        return filtered_cleaned

    # =============================================================================
    # NOMINATIONS-SPECIFIC POST-PROCESSING METHODS
    # =============================================================================

    async def _post_process_nominations_committeeactivities(self) -> dict[str, Any]:
        """
        Post-processing for nominations_committeeactivities:
        - add name to nominations_committees table
        """
        if not self.db_pool:
            raise ValueError("Database pool not configured")

        results: dict[str, Any] = {
            "status": "success",
            "operations": [],
            "rows_affected": 0,
        }

        async with self.db_pool.acquire() as conn:
            # Operation 1: Populate nominations_committees table using unique nomination_id and committee_code from nominations_committeeactivities table
            try:
                async with conn.transaction():
                    sql = f"""
                        INSERT INTO {self.production_schema}.nominations_committees (nomination_id, committee_code, committee_name)
                        SELECT DISTINCT ON (nomination_id, committee_code) nomination_id, committee_code, committee_name
                        FROM {self.production_schema}.nominations_committeeactivities
                        WHERE nomination_id IS NOT NULL
                        AND committee_code IS NOT NULL
                        ON CONFLICT DO NOTHING;
                    """

                    result = await conn.execute(sql)
                    rows_affected = int(result.split()[-1]) if result.split() else 0

                    results["operations"].append(
                        {
                            "name": "populate_nominations_committees",
                            "status": "success",
                            "rows_affected": rows_affected,
                        }
                    )
                    results["rows_affected"] += rows_affected

                    logger.info(f"Populated nominations_committees for {rows_affected} nominations")

            except Exception as e:
                logger.error(f"Error populating nominations_committees: {e}")
                results["operations"].append(
                    {
                        "name": "populate_nominations_committees",
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
