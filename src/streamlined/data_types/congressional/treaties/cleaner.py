"""
Treaties data cleaner.

This module handles cleaning and validation of Congressional treaties data,
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
import re
from typing import Any

import asyncpg

from ....libs.run_tracking import RunManager
from ..base import CongressionalBaseCleaner

logger = logging.getLogger(__name__)


class TreatiesCleaner(CongressionalBaseCleaner):
    """
    Treaties-specific cleaner that extends CongressionalBaseCleaner with treaties-specific cleaning logic.

    Provides specialized cleaning methods for all treaties-related data types
    and handles multi-table processing for complex relationships.
    """

    def __init__(
        self,
        config_path: str | None = None,
        db_pool: asyncpg.Pool | None = None,
        checkpoint_manager=None,
        run_manager: RunManager | None = None,
        data_type_name: str = "treaties",
        staging_schema: str = "bicam_staging_congressional",
        production_schema: str = "bicam_congressional",
    ):
        """
        Initialize treaties cleaner using base class.

        Args:
            config_path: Path to configuration file
            db_pool: Database connection pool
            checkpoint_manager: Checkpoint manager for progress tracking
            run_manager: Run manager for run tracking
            data_type_name: Data type name (defaults to "treaties")
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
            "treaties_countries": ["treaties_countriesparties"],
            "treaties_committeereports": ["treaties_relateddocs"],
            # Add treaties to enable custom joined streaming
            # Add other multi-table or alias mappings here as needed
        }

    # =============================================================================
    # TREATIES-SPECIFIC DATA TYPE CLEANING METHODS
    # =============================================================================

    async def _clean_treaties_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual treaties records.

        FINAL COLUMNS:
        - treaty_id TEXT PRIMARY KEY,
        - congress_received INTEGER,
        - treaty_number INTEGER,
        - suffix TEXT,
        - congress_considered INTEGER,
        - topic TEXT,
        - transmitted_at TIMESTAMP WITH TIME ZONE,
        - in_force_at TIMESTAMP WITH TIME ZONE,
        - resolution_text TEXT,
        - parts_count INTEGER,
        - actions_count INTEGER,
        - old_number TEXT,
        - old_number_display_name TEXT,
        - updated_at TIMESTAMP WITH TIME ZONE

        STAGING COLUMNS:
        Based on Congressional API treaty structure, typically includes:
        - topic                text,
        - number               text,
        - suffix               text,
        - actions_url          text,
        - actions_count        text,
        - batch_id             text,
        - oldnumber            text,
        - treaty_id            text
        - updatedate           text,
        - inforcedate          text,
        - relateddocs          text,
        - resolutiontext       text,
        - transmitteddate      text,
        - congressreceived     text,
        - congressconsidered   text,
        - oldnumberdisplayname text,
        - countriesparties     text,
        - titles               text,
        - indexterms           text,
        - parts_urls           text,
        - parts_count          text

        Args:
            record_data: Raw treaties record from staging
        Returns:
            Cleaned treaties record
        """
        cleaned = record_data.copy()

        suffix = cleaned.get("suffix", None)
        if suffix == "":
            suffix = None

        filtered_cleaned = {
            "treaty_id": cleaned.get("treaty_id", "ID_ERROR"),
            "congress_received": self.safe_int(cleaned.get("congressreceived")),
            "treaty_number": self.safe_int(cleaned.get("number")),
            "suffix": suffix,
            "congress_considered": self.safe_int(cleaned.get("congressconsidered", 0)),
            "topic": cleaned.get("topic"),
            "transmitted_at": self.standardize_date(cleaned.get("transmitteddate")),
            "in_force_at": self.standardize_date(cleaned.get("inforcedate")),
            "resolution_text": self.clean_long_text(cleaned.get("resolutiontext")),
            "parts_count": self.safe_int(cleaned.get("parts_count", 0), 0),
            "actions_count": self.safe_int(cleaned.get("actions_count", 0), 0),
            "old_number": cleaned.get("oldnumber"),
            "old_number_display_name": cleaned.get("oldnumberdisplayname"),
            "updated_at": self.standardize_date(cleaned.get("updatedate")),
        }
        return filtered_cleaned

    async def _clean_treaties_actions_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual treaties actions records.

        FINAL COLUMNS:
        - action_id TEXT PRIMARY KEY,
        - treaty_id TEXT,
        - action_code TEXT,
        - action_date DATE,
        - text TEXT,
        - action_type TEXT,
        - committee_code TEXT,
        - committee_name TEXT,

        STAGING COLUMNS:
        - text                 text,
        - type                 text,
        - batch_id             text,
        - committee_url        text,
        - committee_name       text,
        - committee_systemcode text,
        - treaty_id             text,
        - actioncode           text,
        - actiondate           text,
        - id                   text
        """
        cleaned = record_data.copy()

        filtered_cleaned = {
            "action_id": cleaned.get("id", "ID_ERROR"),
            "treaty_id": cleaned.get("treaty_id", "ID_ERROR"),
            "action_code": cleaned.get("actioncode"),
            "action_date": self.standardize_date(cleaned.get("actiondate")),
            "text": self.clean_long_text(cleaned.get("text")),
            "action_type": cleaned.get("type"),
            "committee_code": cleaned.get("committee_systemcode"),
            "committee_name": cleaned.get("committee_name"),
        }
        return filtered_cleaned

    async def _clean_treaties_committeeactivities_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual treaties committee activities records.
        """
        cleaned = record_data.copy()

        filtered_cleaned = {
            "treaty_id": cleaned.get("treaty_id", "ID_ERROR"),
            "committee_code": cleaned.get("committee_code", "ID_ERROR"),
            "committee_name": cleaned.get("name"),
            "committee_type": cleaned.get("type"),
            "chamber": cleaned.get("chamber"),
            "activity_name": cleaned.get("activity_name"),
            "activity_date": self.standardize_date(cleaned.get("activity_date")),
        }
        return filtered_cleaned

    async def _clean_treaties_countries_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual treaties countries records.

        FINAL COLUMNS:
        - treaty_id TEXT,
        - country TEXT,

        STAGING COLUMNS:
        - name      text,
        - treaty_id text,
        - id        text
        """
        cleaned = record_data.copy()

        filtered_cleaned = {
            "treaty_id": cleaned.get("treaty_id", "ID_ERROR"),
            "country": cleaned.get("name"),
        }
        return filtered_cleaned

    async def _clean_treaties_indexterms_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual treaties index terms records.

        FINAL COLUMNS:
        - treaty_id TEXT,
        - index_term TEXT,
        - updated_at TIMESTAMP WITH TIME ZONE

        STAGING COLUMNS:
        - name      text,
        - treaty_id text,
        - id        text
        """
        cleaned = record_data.copy()

        filtered_cleaned = {
            "treaty_id": cleaned.get("treaty_id", "ID_ERROR"),
            "index_term": cleaned.get("name"),
        }
        return filtered_cleaned

    async def _clean_treaties_titles_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual treaties titles records.

        FINAL COLUMNS:
        - treaty_id TEXT,
        - title TEXT,
        - title_type TEXT,
        - UNIQUE (treaty_id, title, title_type)

        STAGING COLUMNS:
        - title     text,
        - titletype text,
        - treaty_id text,
        - id        text
        """
        cleaned = record_data.copy()

        title_type = cleaned.get("titletype", None)
        cleaned_title_type = title_type.split(" ")[2].lower() if title_type else None

        filtered_cleaned = {
            "treaty_id": cleaned.get("treaty_id", "ID_ERROR"),
            "title": cleaned.get("title"),
            "title_type": cleaned_title_type,
        }
        return filtered_cleaned

    async def _clean_treaties_committeereports_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual treaties committee reports records.

        FINAL COLUMNS:
        - treaty_id TEXT,
        - report_id TEXT,
        - citation TEXT,
        - UNIQUE (treaty_id, report_id)

        STAGING COLUMNS:
        - url       text,
        - citation  text,
        - treaty_id text,
        - id        text
        """
        cleaned = record_data.copy()

        def _convert_roman_to_arabic(roman_str):
            if roman_str.isdigit():
                return roman_str
            elif re.match(r"^[IVXLC]+$", roman_str.upper()):
                # Convert roman numeral to arabic
                roman_to_arabic = {"I": 1, "V": 5, "X": 10, "L": 50, "C": 100}
                arabic_value = 0
                prev_value = 0
                for char in reversed(roman_str.upper()):
                    curr_value = roman_to_arabic[char]
                    if curr_value >= prev_value:
                        arabic_value += curr_value
                    else:
                        arabic_value -= curr_value
                    prev_value = curr_value
                return str(arabic_value)
            else:
                return roman_str

        # parse citation into report_id
        # Handle citations like "H.Rept 97-71 Part 3" or "H.Rept 97-71,Part 3"
        # Expected format: {chamber}rpt{report_number}-{part_number}-{congress}
        part_number = 1
        if any(
            part in cleaned.get("citation", "").lower()
            for part in ["part", "book", "volume"]
        ):
            if "book" in cleaned.get("citation", "").lower():
                # get the first number after "book"
                book_match = re.search(
                    r"book\s+(\d+)", cleaned.get("citation", ""), re.IGNORECASE
                )
                if book_match:
                    part_number = _convert_roman_to_arabic(book_match.group(1))
            elif "volume" in cleaned.get("citation", "").lower():
                # get the first number after "volume" (THIS CAN BE A ROMAN NUMERAL OR ARABIC NUMERAL)
                volume_match = re.search(
                    r"volume\s+([IVXLC]+|\d+)",
                    cleaned.get("citation", ""),
                    re.IGNORECASE,
                )
                if volume_match:
                    part_number = _convert_roman_to_arabic(volume_match.group(1))
            elif "part" in cleaned.get("citation", "").lower():
                # get the first number after "part"
                part_match = re.search(
                    r"part\s+([IVXLC]+|\d+)", cleaned.get("citation", ""), re.IGNORECASE
                )
                if part_match:
                    part_number = _convert_roman_to_arabic(part_match.group(1))

        # Parse citation like "H.Rept 97-71 Part 3" or "H.Rept 97-71,Part 3"
        citation = cleaned.get("citation", "").lower()

        # Extract chamber prefix (h, s, e, etc.) - take first character before first period
        chamber_prefix = (
            citation.split(".")[0].strip()[0] if citation.split(".")[0].strip() else ""
        )

        # Extract the middle part after the period
        if "." in citation:
            parts = citation.split(".")
            middle_part = ".".join(parts[1:])  # Join everything after first period
        else:
            middle_part = citation

        # Match pattern like "rept 97-71" where 97 is congress and 71 is report number
        rept_match = re.search(r"rept\.?\s*(\d+)-(\d+)", middle_part)
        if rept_match:
            congress = rept_match.group(1)
            report_number = rept_match.group(2)
            report_id = f"{chamber_prefix}rpt{report_number}-{part_number}-{congress}"
        else:
            # Fallback to original citation if parsing fails
            report_id = str(cleaned.get("citation", "ID_ERROR"))

        filtered_cleaned = {
            "treaty_id": str(cleaned.get("treaty_id", "ID_ERROR")),
            "report_id": report_id,
            "citation": cleaned.get("citation"),
        }

        return filtered_cleaned

    # =============================================================================
    # COMMITTEES-SPECIFIC POST-PROCESSING METHODS
    # =============================================================================

    async def _post_process_treaties_committeeactivities(self) -> dict[str, Any]:
        """
        Post-processing for treaties:
        - add name to treaties table
        """
        if not self.db_pool:
            raise ValueError("Database pool not configured")

        results = {
            "status": "success",
            "operations": [],
            "rows_affected": 0,
        }
        async with self.db_pool.acquire() as conn:
            # Operation 1: Update report_id to match committeereports table
            try:
                async with conn.transaction():
                    sql = f"""
                        INSERT INTO {self.production_schema}.treaties_committees
                        (treaty_id, committee_code, committee_name)
                        SELECT DISTINCT ON (treaty_id, committee_code) treaty_id, committee_code, committee_name
                        FROM {self.production_schema}.treaties_committeeactivities
                        WHERE committee_code IS NOT NULL
                        ON CONFLICT (treaty_id, committee_code) DO NOTHING;
                    """

                    result = await conn.execute(sql)
                    rows_affected = int(result.split()[-1]) if result.split() else 0

                    results["operations"].append(
                        {
                            "name": "insert_treaties_committees_table",
                            "status": "success",
                            "rows_affected": rows_affected,
                        }
                    )
                    results["rows_affected"] += rows_affected

                    logger.info(
                        f"Inserted treaties_committees table with {rows_affected} rows"
                    )

            except Exception as e:
                logger.error(f"Error inserting bills_committees table: {e}")
                results["operations"].append(
                    {
                        "name": "insert_bills_committees_table",
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
