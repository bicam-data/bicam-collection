"""
Bills data cleaner.

This module handles cleaning and validation of Congressional bills data,
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

import json
import logging
import re
from collections.abc import AsyncGenerator
from typing import Any

from ...abstract.base_cleaner import BaseCleaner

logger = logging.getLogger(__name__)


class BillsCleaner(BaseCleaner):
    """
    Bills-specific cleaner that extends BaseCleaner with bills-specific cleaning logic.

    Provides specialized cleaning methods for all bills-related data types
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


        # Set bills-specific multi-table processing configuration
        self.multi_table_data_types = {
            "bills_texts": ["bills_texts", "bills_texts_formats"],
            # Add other bills multi-table data types here as needed
        }

    # =============================================================================
    # BILLS-SPECIFIC MULTI-TABLE STREAMING IMPLEMENTATIONS
    # =============================================================================

    async def _stream_bills_texts_joined_chunks(
        self, chunk_size: int
    ) -> AsyncGenerator[list[dict[str, Any]], None]:
        """
        Stream joined data from bills_texts_staging and bills_texts_formats_staging.
        Properly aggregates multiple formats per text record.

        Args:
            chunk_size: Size of each chunk

        Yields:
            Chunks of joined text records with aggregated format information
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

    # =============================================================================
    # BILLS-SPECIFIC DATA TYPE CLEANING METHODS
    # =============================================================================

    async def _clean_bills_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual bills records.
        FINAL COLUMNS:
        - bill_id TEXT PRIMARY KEY
        - bill_type TEXT
        - bill_number TEXT
        - congress INTEGER
        - title TEXT
        - origin_chamber TEXT
        - policy_area TEXT
        - is_law BOOLEAN
        - introduced_at TIMESTAMP WITH TIME ZONE
        - constitutional_authority_statement TEXT
        - actions_count INTEGER DEFAULT 0
        - amendments_count INTEGER DEFAULT 0
        - committees_count INTEGER DEFAULT 0
        - cosponsors_count INTEGER DEFAULT 0
        - cosponsors_withdrawn_count INTEGER DEFAULT 0
        - relatedbills_count INTEGER DEFAULT 0
        - subjects_count INTEGER DEFAULT 0
        - summaries_count INTEGER DEFAULT 0
        - texts_count INTEGER DEFAULT 0
        - titles_count INTEGER DEFAULT 0
        - updated_at TIMESTAMP WITH TIME ZONE

        STAGING COLUMNS:
        - bill_id                                      text,
        - type                                         text,
        - title                                        text,
        - number                                       text,
        - titles_url                                   text,
        - titles_count                                 text,
        - actions_url                                  text,
        - actions_count                                text,
        - congress                                     text,
        - subjects_url                                 text,
        - subjects_count                               text,
        - _batch_id                                    text,
        - summaries_url                                text,
        - summaries_count                              text,
        - committees_url                               text,
        - committees_count                             text,
        - cosponsors_url                               text,
        - cosponsors_count                             text,
        - cosponsors_countincludingwithdrawncosponsors text,
        - policyarea_name                              text,
        - updatedate                                   text,
        - latestaction_text                            text,
        - latestaction_actiondate                      text,
        - textversions_url                             text,
        - textversions_count                           text,
        - originchamber                                text,
        - introduceddate                               text,
        - originchambercode                            text,
        - updatedateincludingtext                      text,
        - constitutionalauthoritystatementtext         text,
        - processed_at                                 text,
        - amendments_url                               text,
        - amendments_count                             text,
        - relatedbills_url                             text,
        - latestaction_actiontime                      text,
        - relatedbills_count                           text,

        Args:
            record_data: Raw bills record from staging
        Returns:
            Cleaned bills record
        """
        cleaned = record_data.copy()

        # cleaning logic:
        # - set to correct data types
        # - standardize the chamber
        # - standardize the dates
        # - clean long text fields
        # - set is_law to None (added via postprocessing)
        # - validate required fields (bill_id)

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

        # ensure that bill_id is in the right format of {hr, hres, sres, s, hjres, hconres, sjres, sconres}{1-4 digit number (with an optional .5)}-{1-3 digit number between 1-200}
        if not re.match(
            r"^(hr|hres|sres|s|hjres|hconres|sjres|sconres)\d{1,4}(\.5)?-\d{1,3}$",
            filtered_cleaned["bill_id"],
        ):
            raise ValueError(f"Invalid bill_id format: {filtered_cleaned['bill_id']}")

        return filtered_cleaned

    async def _clean_bills_actions_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for bills actions records.

        FINAL COLUMNS:
        - action_id UUID PRIMARY KEY,
        - bill_id TEXT,
        - action_code TEXT,
        - action_date DATE,
        - text TEXT,
        - action_type TEXT,
        - source_system TEXT,
        - source_system_code INTEGER,
        - calendar TEXT,
        - calendar_number INTEGER,


        STAGING COLUMNS:
        - id                      text,
        - processed_at            text,
        - sourcesystem_code       text,
        - actiondate              text,
        - sourcesystem_name       text,
        - calendarnumber_number   text,
        - _batch_id               text,
        - text                    text,
        - list_index              text,
        - bill_id                 text,
        - actiontime              text,
        - calendarnumber_calendar text,
        - actioncode              text,
        - type                    text

        Args:
            record_data: Raw action record from staging

        Returns:
            Cleaned action record

        """
        cleaned = record_data.copy()

        # cleaning logic:
        # - set to correct data types
        # - standardize the dates
        # - clean long text fields

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

    async def _clean_bills_actions_recordedvotes_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for bills actions recorded votes records.
        FINAL COLUMNS:

        - actions_id TEXT,
        - bill_id TEXT,
        - chamber TEXT,
        - congress INTEGER,
        - date TIMESTAMP WITH TIME ZONE,
        - roll_number INTEGER,
        - session INTEGER,
        - url TEXT

        STAGING COLUMNS:
        - id            text,
        - chamber       text,
        - congress      text,
        - list_index    text,
        - bill_id       text,
        - extracted_at  text,
        - date          text,
        - action_id     text,
        - rollnumber    text,
        - sessionnumber text,
        - url           text
        """
        cleaned = record_data.copy()

        filtered_cleaned = {
            "action_id": str(cleaned.get("actions_id", "ID_ERROR")),
            "bill_id": str(cleaned.get("bill_id", "ID_ERROR")),
            "chamber": str(self.standardize_chamber(cleaned.get("chamber", None))),
            "congress": self.safe_int(cleaned.get("congress", None)),
            "date": self.standardize_date(cleaned.get("date", None)),
            "roll_number": self.safe_int(cleaned.get("rollnumber", None)),
            "session": self.safe_int(cleaned.get("sessionnumber", None)),
            "url": str(cleaned.get("url", None)),
        }

        return filtered_cleaned

    async def _clean_bills_actions_committees_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for bills actions committees records.
        FINAL COLUMNS:
        - action_id TEXT,
        - bill_id TEXT,
        - committee_code TEXT,
        - committee_name TEXT,

        STAGING COLUMNS:
        - id           text,
        - list_index   text,
        - bill_id      text,
        - extracted_at text,
        - action_id    text,
        - url          text,
        - name         text,
        - systemcode   text

        """
        cleaned = record_data.copy()

        filtered_cleaned = {
            "action_id": str(cleaned.get("actions_id", "ID_ERROR")),
            "bill_id": str(cleaned.get("bill_id", "ID_ERROR")),
            "committee_code": str(cleaned.get("systemcode", None)),
            "committee_name": str(cleaned.get("name", None)),
        }

        return filtered_cleaned

    async def _clean_bills_cbocostestimates_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for bills cbocostestimates records.
        FINAL COLUMNS:
        - bill_id TEXT,
        - description TEXT,
        - pub_date TIMESTAMP WITH TIME ZONE,
        - title TEXT,
        - url TEXT,

        STAGING COLUMNS:
        - id           text,
        - list_index   text,
        - bill_id      text,
        - pubdate      text,
        - extracted_at text,
        - title        text,
        - description  text,
        - url          text
        """
        cleaned = record_data.copy()

        filtered_cleaned = {
            "bill_id": str(cleaned.get("bill_id", "ID_ERROR")),
            "description": self.clean_long_text(cleaned.get("description", None)),
            "pub_date": self.standardize_date(cleaned.get("pubdate", None)),
            "title": str(cleaned.get("title", None)),
            "url": str(cleaned.get("url", None)),
        }

        return filtered_cleaned

    async def _clean_bills_committeeactivities_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for bills committee activities records.

        """
        cleaned = record_data.copy()

        filtered_cleaned = {
            "bill_id": str(cleaned.get("bill_id", "ID_ERROR")),
            "committee_code": str(cleaned.get("committee_code", None)),
            "name": str(cleaned.get("name", None)),
            "type": str(cleaned.get("type", None)),
            "chamber": str(self.standardize_chamber(cleaned.get("chamber", None))),
            "activity_name": str(cleaned.get("activity_name", None)),
            "activity_date": self.standardize_date(cleaned.get("activity_date", None)),
        }

        return filtered_cleaned

    async def _clean_bills_committeereports_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for bills committeereports records.
        FINAL COLUMNS:
        - bill_id TEXT,
        - report_id TEXT,

        STAGING COLUMNS:
        - id           text,
        - list_index   text,
        - bill_id      text,
        - extracted_at text,
        - citation     text,
        - url          text
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

        # Extract chamber prefix (h, s, etc.)
        chamber_prefix = citation.split(".")[0].strip()

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
            report_id = (
                f"{chamber_prefix}rpt{report_number}-{part_number}-{congress}"
            )
        else:
            # Fallback to original citation if parsing fails
            report_id = str(cleaned.get("citation", "ID_ERROR"))

        filtered_cleaned = {
            "bill_id": str(cleaned.get("bill_id", "ID_ERROR")),
            "report_id": report_id,
            "citation": cleaned.get("citation"),
        }



        return filtered_cleaned

    async def _clean_bills_cosponsors_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for bills cosponsors records.

        Args:
            record_data: Raw cosponsor record from staging

        Returns:
            Cleaned cosponsor record

        FINAL COLUMNS:
        - bill_id TEXT,
        - bioguide_id TEXT,
        - display_name TEXT,
        - party TEXT,
        - state TEXT,
        - district INT,
        - is_original_cosponsor BOOLEAN,
        - sponsorship_date DATE,
        - sponsorship_withdrawal_date DATE,

        STAGING COLUMNS:
        - id                       text default gen_random_uuid() not null
        - primary key,
        - processed_at             text,
        - lastname                 text,
        - sponsorshipdate          text,
        - _batch_id                text,
        - list_index               text,
        - bill_id                  text,
        - firstname                text,
        - middlename               text,
        - state                    text,
        - endpoint                 text,
        - fullname                 text,
        - party                    text,
        - bioguideid               text,
        - district                 text,
        - isoriginalcosponsor      text,
        - url                      text,
        - sponsorshipwithdrawndate text
        """
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

    async def _clean_bills_laws_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for bills laws records.

        Args:
            record_data: Raw law record from staging

        Returns:
            Cleaned law record

        FINAL COLUMNS:
        - bill_id TEXT,
        - law_id TEXT,
        - law_type TEXT,
        - law_number INTEGER,
        - congress INTEGER,
        - order_number INTEGER,


        STAGING COLUMNS:
        - id           text
        - list_index   text,
        - bill_id      text,
        - extracted_at text,
        - type         text,
        - number       text
        """
        cleaned = record_data.copy()

        filtered_cleaned = {
            "bill_id": str(cleaned.get("bill_id", "ID_ERROR")),
            "law_id": str(
                f"PL{cleaned.get('number')}"
                if cleaned.get("number", None)
                else "ID_ERROR"
            ),
            "law_type": str(
                cleaned.get("type", None).split(" ")[0].lower()
            ),  # gets "public" or "private" from "Public Law" or "Private Law"
            "law_number": self.safe_int(cleaned.get("number", None).split("-")[1])
            if cleaned.get("number", None)
            else None,
            "congress": self.safe_int(cleaned.get("number", None).split("-")[0])
            if cleaned.get("number", None)
            else None,
            "order_number": self.safe_int(cleaned.get("number", None).split("-")[1])
            if cleaned.get("number", None)
            else None,
        }

        return filtered_cleaned

    async def _clean_bills_notes_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for bills notes records.
        """
        raise NotImplementedError(
            "Bills notes cleaning not implemented, check staging schema"
        )

    async def _clean_bills_notes_links_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for bills notes links records.
        """
        raise NotImplementedError(
            "Bills notes links cleaning not implemented, check staging schema"
        )

    async def _clean_bills_relatedbills_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for bills related bills records.

        Args:
            record_data: Raw related bill record from staging

        Returns:
            Cleaned related bill record

        FINAL COLUMNS:
        - bill_id TEXT,
        - relatedbill_id TEXT,
        - relationship_type TEXT,
        - relationship_identified_by TEXT,

        STAGING COLUMNS:
        - id                         text,
        - processed_at               text,
        - _batch_id                  text,
        - relationship_identified_by text,
        - list_index                 text,
        - bill_id                    text,
        - relationship_type          text,
        - relatedbill_id             text
        """
        cleaned = record_data.copy()
        filtered_cleaned = {
            "bill_id": str(cleaned.get("bill_id", "ID_ERROR")),
            "relatedbill_id": str(cleaned.get("relatedbill_id", None)),
            "relationship_type": str(cleaned.get("relationship_type", None)),
            "relationship_identified_by": str(
                cleaned.get("relationship_identified_by", None)
            ),
        }
        return filtered_cleaned

    async def _clean_bills_sponsors_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for bills sponsors records.

        Args:
            record_data: Raw sponsor record from staging

        Returns:
            Cleaned sponsor record

        FINAL COLUMNS:
        - bill_id TEXT,
        - bioguide_id TEXT,
        - display_name TEXT,
        - party TEXT,
        - state TEXT,
        - district INT,
        - is_by_external_request BOOLEAN

        STAGING COLUMNS:
        - id           text
        - url          text,
        - party        text,
        - state        text,
        - district     text,
        - fullname     text,
        - lastname     text,
        - firstname    text,
        - bioguideid   text,
        - isbyrequest  text,
        - bill_id      text,
        - list_index   text,
        - extracted_at text,
        - middlename   text
        """
        cleaned = record_data.copy()

        filtered_cleaned = {
            "bill_id": str(cleaned.get("bill_id", "ID_ERROR")),
            "bioguide_id": str(cleaned.get("bioguideid", "ID_ERROR")),
            "display_name": str(cleaned.get("fullname", None)),
            "party": str(cleaned.get("party", None)),
            "state": str(cleaned.get("state", None)),
            "district": self.safe_int(cleaned.get("district", None)),
            "is_by_external_request": cleaned.get("isbyrequest") == "Y"
            if cleaned.get("isbyrequest", None)
            else None,
        }

        return filtered_cleaned

    async def _clean_bills_subjects_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for bills subjects records.

        Args:
            record_data: Raw subject record from staging

        Returns:
            Cleaned subject record

        FINAL COLUMNS:
        - bill_id TEXT,
        - subject TEXT,
        - updated_at TIMESTAMP WITH TIME ZONE

        STAGING COLUMNS:
        - id           text
        - processed_at text,
        - _batch_id    text,
        - list_index   text,
        - bill_id      text,
        - updatedate   text,
        - name         text
        """
        cleaned = record_data.copy()
        filtered_cleaned = {
            "bill_id": str(cleaned.get("bill_id", "ID_ERROR")),
            "subject": str(cleaned.get("name", None)),
            "updated_at": self.standardize_date(cleaned.get("updatedate", None)),
        }
        return filtered_cleaned

    async def _clean_bills_summaries_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for bills summaries records.

        Args:
            record_data: Raw summary record from staging

        Returns:
            Cleaned summary record

        FINAL COLUMNS:
        - bill_id TEXT,
        - action_date DATE,
        - action_desc TEXT,
        - text TEXT,
        - version_code INTEGER,

        STAGING COLUMNS:
        - id           text
        - processed_at text,
        - actiondate   text,
        - _batch_id    text,
        - text         text,
        - list_index   text,
        - bill_id      text,
        - updatedate   text,
        - versioncode  text,
        - actiondesc   text
        """
        cleaned = record_data.copy()

        filtered_cleaned = {
            "bill_id": str(cleaned.get("bill_id", "ID_ERROR")),
            "action_date": self.standardize_date(cleaned.get("actiondate", None)),
            "action_desc": str(cleaned.get("actiondesc", None)),
            "text": str(self.clean_long_text(cleaned.get("text", None))),
            "version_code": self.safe_int(cleaned.get("versioncode", None)),
        }
        return filtered_cleaned

    async def _clean_bills_texts_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for bills texts records.
        Handles joined data from bills_texts and bills_texts_formats.

        Args:
            record_data: Raw text record from staging (with aggregated formats)

        Returns:
            Cleaned text record

        FINAL COLUMNS:
        - bill_id TEXT,
        - date TEXT,
        - type TEXT,
        - raw_text TEXT,
        - formatted_text TEXT,
        - pdf TEXT,
        - xml TEXT,

        STAGING COLUMNS (bills_texts):
        - id           text,
        - processed_at text,
        - _batch_id    text,
        - list_index   text,
        - bill_id      text,
        - date         text,
        - type         text

        STAGING COLUMNS (aggregated from bills_texts_formats):
        - id           text,
        - text_id      text,
        - list_index   text,
        - bill_id      text,
        - extracted_at text,
        - type         text,
        - url          text


        JOINED DATA:
        # From bills_texts table:
        - id           text,
        - processed_at text,
        - _batch_id    text,
        - list_index   text,
        - bill_id      text,
        - date         text,
        - type         text

        # From the JOIN aggregation:
        - formats_json: JSON array of {type, url} objects
        """
        try:
            # Avoid deep copying that might cause recursion
            cleaned = {}
            for key, value in record_data.items():
                # Skip processing complex nested objects that might cause recursion
                if isinstance(value, str | int | float | bool | type(None)):
                    cleaned[key] = value
                elif key == "formats_json":
                    # Handle formats_json specially to avoid recursion
                    cleaned[key] = value

            # Extract URLs from formats_json safely
            pdf_url = None
            xml_url = None
            html_text = None

            formats_json_str = cleaned.get("formats_json")
            if formats_json_str:
                try:
                    # Handle case where it's already a string vs already parsed
                    if isinstance(formats_json_str, str):
                        formats_json = json.loads(formats_json_str)
                    else:
                        formats_json = formats_json_str

                    if isinstance(formats_json, list):
                        for format_item in formats_json:
                            if isinstance(format_item, dict):
                                format_type = format_item.get("type", "").lower()
                                format_url = format_item.get("url", "")

                                if format_type == "pdf":
                                    pdf_url = format_url
                                elif format_type == "formatted xml":
                                    xml_url = format_url
                                elif format_type == "formatted text":
                                    html_text = format_url
                except (json.JSONDecodeError, TypeError, AttributeError) as e:
                    logger.warning(
                        f"Error parsing formats_json for bill {cleaned.get('bill_id', 'unknown')}: {e}"
                    )

            filtered_cleaned = {
                "bill_id": str(cleaned.get("bill_id", "ID_ERROR")),
                "date": self.standardize_date(cleaned.get("date")),
                "type": str(cleaned.get("type")),
                "raw_text": None,  # Would need to fetch from URLs if required
                "formatted_text": html_text,
                "pdf": pdf_url,
                "xml": xml_url,
            }

            return filtered_cleaned

        except Exception as e:
            logger.error(f"Error in _clean_bills_texts_singular: {e}")
            # Return minimal safe record to prevent total failure
            return {
                "bill_id": str(record_data.get("bill_id", "ERROR")),
                "date": None,
                "type": str(record_data.get("type")),
                "raw_text": None,
                "formatted_text": None,
                "pdf": None,
                "xml": None,
            }

    async def _clean_bills_titles_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for bills titles records.

        Args:
            record_data: Raw title record from staging

        Returns:
            Cleaned title record

        FINAL COLUMNS:
        - bill_id TEXT,
        - title TEXT,
        - title_type TEXT,
        - bill_text_version_code TEXT,
        - bill_text_version_name TEXT,
        - chamber TEXT,
        - title_type_code INTEGER,

        STAGING COLUMNS:
        - id                  text,
        - processed_at        text,
        - _batch_id           text,
        - list_index          text,
        - bill_id             text,
        - chambercode         text,
        - updatedate          text,
        - billtextversionname text,
        - billtextversioncode text,
        - titletype           text,
        - title               text,
        - titletypecode       text,
        - chambername         text
        """
        cleaned = record_data.copy()
        filtered_cleaned = {
            "bill_id": str(cleaned.get("bill_id", "ID_ERROR")),
            "title": str(cleaned.get("title", None)),
            "title_type": str(cleaned.get("titletype", None)),
            "bill_text_version_code": str(cleaned.get("billtextversioncode", None)),
            "bill_text_version_name": str(cleaned.get("billtextversionname", None)),
            "chamber": self.standardize_chamber(cleaned.get("chambername", None)),
            "title_type_code": self.safe_int(cleaned.get("titletypecode", None)),
            "updated_at": self.standardize_date(cleaned.get("updatedate", None)),
        }
        return filtered_cleaned

    # =============================================================================
    # BILLS-SPECIFIC POST-PROCESSING METHODS
    # =============================================================================

    async def _post_process_bills(self) -> dict[str, Any]:
        """
        Post-processing operations specific to bills data.

        Returns:
            Dictionary with bills post-processing results
        """
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

            # Operation 2: Create performance indexes (outside transaction for CONCURRENTLY)
            try:
                index_sqls = [
                    f"CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_bills_bill_id ON {self.production_schema}.bills (bill_id);",
                    f"CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_bills_is_law ON {self.production_schema}.bills (is_law) WHERE is_law = true;",
                    f"CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_bills_congress ON {self.production_schema}.bills (congress);",
                    f"CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_bills_introduced_at ON {self.production_schema}.bills (introduced_at);",
                    f"CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_bills_updated_at ON {self.production_schema}.bills (updated_at);",
                    f"CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_bills_chamber ON {self.production_schema}.bills (origin_chamber);",
                ]

                for sql in index_sqls:
                    await conn.execute(sql)

                results["operations"].append(
                    {
                        "name": "create_performance_indexes",
                        "status": "success",
                        "indexes_created": len(index_sqls),
                    }
                )

                logger.info(f"Created {len(index_sqls)} performance indexes")

            except Exception as e:
                logger.error(f"Error creating indexes: {e}")
                results["operations"].append(
                    {
                        "name": "create_performance_indexes",
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

    async def _post_process_bills_committeereports(self) -> dict[str, Any]:
        """
        Post-processing operations specific to bills_committeereports data.
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
                        UPDATE {self.production_schema}.bills_committeereports AS bcr
                        SET bcr.report_id = cr.report_id
                        FROM {self.production_schema}.committeereports AS cr
                        WHERE split_part(bcr.report_id, '-', 1) = split_part(cr.report_id, '-', 1)
                            AND split_part(bcr.report_id, '-', 3) = split_part(cr.report_id, '-', 3)
                            AND cr.report_id IN (
                                SELECT report_id
                                FROM {self.production_schema}.committeereports
                                GROUP BY split_part(report_id, '-', 1), split_part(report_id, '-', 3)
                                HAVING COUNT(*) = 1
                            );
                    """

                    result = await conn.execute(sql)
                    rows_affected = int(result.split()[-1]) if result.split() else 0

                    results["operations"].append(
                        {
                            "name": "update_report_id_to_match_committeereports",
                            "status": "success",
                            "rows_affected": rows_affected,
                        }
                    )
                    results["rows_affected"] += rows_affected

                    logger.info(f"Updated report_id for {rows_affected} bills")

            except Exception as e:
                logger.error(f"Error updating report_id: {e}")
                results["operations"].append(
                    {
                        "name": "update_report_id_to_match_committeereports",
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

    async def _post_process_bills_committeeactivities(self):
        """
        Post-processing operations specific to bills_committeeactivities data.
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
                        INSERT INTO {self.production_schema}.bills_committees
                        (bill_id, committee_code, committee_name)
                        SELECT DISTINCT ON (bill_id, committee_code) bill_id, committee_code, committee_name
                        FROM {self.production_schema}.bills_committeeactivities
                        WHERE committee_code IS NOT NULL
                        ON CONFLICT (bill_id, committee_code) DO NOTHING;
                    """

                    result = await conn.execute(sql)
                    rows_affected = int(result.split()[-1]) if result.split() else 0

                    results["operations"].append(
                        {
                            "name": "insert_bills_committees_table",
                            "status": "success",
                            "rows_affected": rows_affected,
                        }
                    )
                    results["rows_affected"] += rows_affected

                    logger.info(
                        f"Inserted bills_committees table with {rows_affected} rows"
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

    # Add more _post_process_{data_type} methods here as needed
    # async def _post_process_amendments(self):
    #     """Post-processing for amendments data."""
    #     pass
