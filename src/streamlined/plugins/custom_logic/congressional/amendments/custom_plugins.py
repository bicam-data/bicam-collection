"""
Amendments Custom Plugin Logic

This module contains all the custom logic for amendments data type including:
- Fetching logic (from fetcher.py)
- Database normalization logic (from database_normalizer.py)
- Cleaning logic (from cleaner.py)

All logic is organized into classes that can be used by the plugin system.
"""

import json
import logging
import re
from collections.abc import AsyncGenerator
from typing import Any

# Import the base class that provides shared functionality
from ....base import CongressionalBaseFetcherLogic

logger = logging.getLogger(__name__)


class AmendmentsFetcherLogic(CongressionalBaseFetcherLogic):
    """
    Amendments-specific fetcher logic that extends the base Congressional fetcher logic.

    This class provides custom methods for fetching amendments data, including:
    - Extracting standardized amendment IDs
    - Handling related data from URLs
    - Implementing specialized fetching logic for amendments-specific data

    Inherits shared functionality like get_generic_related_data from
    CongressionalBaseFetcherLogic.
    """

    def __init__(self, data_type: str = "amendments"):
        super().__init__(data_type)

    def extract_item_id(self, item_data: dict[str, Any]) -> str:
        """Extract standardized amendment ID from amendment data."""
        amendment_type = item_data.get("type", "").lower()
        number = item_data.get("number", "")
        congress = item_data.get("congress", "")

        if all([amendment_type, number, congress]):
            return f"{amendment_type}{number}-{congress}"
        else:
            return "ID_ERROR"

    def _extract_preliminary_item_id(self, list_item: dict[str, Any]) -> str:
        """
        Extract a preliminary item ID from list data for checkpoint tracking.

        This is used for checkpoint tracking before we have the full data.
        The actual item ID will be extracted from full data later.

        Args:
            list_item: List item data containing URL

        Returns:
            Preliminary item ID extracted from URL or other available data
        """
        return self.extract_item_id(list_item)

    async def get_amendments_actions(
        self, full_amendment_data: dict[str, Any], client
    ) -> list[dict[str, Any]]:
        """Get amendment actions from actions URL in full amendment data."""
        return await self.get_generic_related_data(
            full_amendment_data, related_table_name="actions", client=client
        )

    async def get_amendments_cosponsors(
        self, full_amendment_data: dict[str, Any], client
    ) -> list[dict[str, Any]]:
        """Get amendment cosponsors from cosponsors URL in full amendment data."""
        return await self.get_generic_related_data(
            full_amendment_data, related_table_name="cosponsors", client=client
        )

    async def get_amendments_texts(
        self, full_amendment_data: dict[str, Any], client
    ) -> list[dict[str, Any]]:
        """Get amendment text versions from textVersions URL in full amendment data."""
        return await self.get_generic_related_data(
            full_amendment_data, related_table_name="texts", client=client
        )


class AmendmentsCleanerLogic:
    """
    Amendments-specific cleaner that extends CongressionalBaseCleaner with amendments-specific cleaning logic.

    Provides specialized cleaning methods for all amendments-related data types
    and handles multi-table processing for complex relationships.
    """

    def __init__(
        self,
        data_type_name: str = "amendments",
        system_name: str = "congressional",
        staging_schema: str = "bicam_staging_congressional",
        production_schema: str = "bicam_congressional",
    ):
        self.data_type_name = data_type_name
        self.system_name = system_name
        self.staging_schema = staging_schema
        self.production_schema = production_schema
        self.db_pool = None

        # Set amendments-specific multi-table processing configuration
        self.multi_table_data_types = {
            "amendments_texts": ["amendments_texts", "amendments_texts_formats"],
            # Add other amendments multi-table data types here as needed
        }

    async def _stream_amendments_texts_joined_chunks(
        self, chunk_size: int
    ) -> AsyncGenerator[list[dict[str, Any]], None]:
        """
        Stream joined data from amendments_texts_staging and amendments_texts_formats_staging.
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
                AND table_name IN ('amendments_texts', 'amendments_texts_formats')
                """
                tables_count = await conn.fetchval(
                    tables_exist_query, self.staging_schema
                )

                if tables_count < 2:
                    logger.warning(
                        "One or both amendments_texts staging tables do not exist"
                    )
                    return

                # Get total count for logging
                count_query = f"""
                SELECT COUNT(DISTINCT at.id)
                FROM {self.staging_schema}.amendments_texts at
                """
                total_count = await conn.fetchval(count_query)

                if total_count == 0:
                    logger.info("No amendments_texts records found")
                    return

                logger.info(
                    f"Streaming {total_count} amendments_texts records with formats in chunks of {chunk_size}"
                )

                # Stream using pagination with aggregated formats
                offset = 0
                while True:
                    # Use JSON aggregation to collect all formats for each text
                    query = f"""
                    SELECT
                        at.*,
                        COALESCE(
                            json_agg(
                                json_build_object('type', atf.type, 'url', atf.url) ORDER BY atf.type
                            ) FILTER (WHERE atf.type IS NOT NULL),
                            '[]'::json
                        ) AS formats_json
                    FROM {self.staging_schema}.amendments_texts         at
                    LEFT JOIN {self.staging_schema}.amendments_texts_formats atf
                            ON at.amendment_id = atf.amendment_id
                    GROUP BY at.id
                    ORDER BY at.date DESC
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
                                f"Error converting amendments_texts row to dict: {e}, row type: {type(row)}, row: {row}"
                            )
                            continue

                    if chunk:  # Only yield if we have valid records
                        yield chunk

                    offset += chunk_size

                    # Log progress periodically
                    if offset % (chunk_size * 10) == 0:
                        logger.debug(
                            f"Streamed {offset} amendments_texts records with formats"
                        )

            except Exception as e:
                logger.error(f"Error streaming amendments_texts with formats: {e}")
                raise

    # =============================================================================
    # AMENDMENTS-SPECIFIC DATA TYPE CLEANING METHODS
    # =============================================================================

    async def _clean_amendments_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual amendments records.

        FINAL COLUMNS:
        - amendment_id TEXT PRIMARY KEY
        - amendment_type TEXT
        - amendment_number INTEGER
        - congress INTEGER
        - chamber TEXT
        - purpose TEXT
        - description TEXT
        - proposed_at TIMESTAMP WITH TIME ZONE
        - submitted_at TIMESTAMP WITH TIME ZONE
        - is_bill_amendment BOOLEAN
        - is_treaty_amendment BOOLEAN
        - is_amendment_amendment BOOLEAN
        - notes TEXT
        - actions_count INTEGER DEFAULT 0
        - cosponsors_count INTEGER DEFAULT 0
        - cosponsors_withdrawn_count INTEGER DEFAULT 0
        - texts_count INTEGER DEFAULT 0
        - amendments_to_amendment_count INTEGER DEFAULT 0
        - updated_at TIMESTAMP WITH TIME ZONE


        STAGING COLUMNS:
        Based on Congressional API amendment structure, typically includes:
        - type
        - number
        - congress
        - chamber
        - purpose
        - batch_id
        - updatedate
        - amendedbill_url
        - amendedbill_type
        - amendedbill_title
        - amendedbill_number
        - amendedbill_congress
        - amendedbill_originchamber
        - amendedbill_originchambercode
        - amendedbill_updatedateincludingtext
        - amendment_id
        - latestaction_text
        - latestaction_links
        - latestaction_actiondate
        - proposeddate
        - textversions_url
        - textversions_count
        - submitteddate
        - amendedamendment_url
        - amendedamendment_type
        - amendedamendment_number
        - amendedamendment_purpose
        - amendedamendment_congress
        - amendedamendment_updatedate
        - cosponsors_url
        - cosponsors_count
        - cosponsors_countincludingwithdrawncosponsors
        - description
        - latestaction_actiontime
        - amendmentstoamendment_url
        - amendmentstoamendment_count
        - amendedamendment_description

        Args:
            record_data: Raw amendments record from staging
        Returns:
            Cleaned amendments record
        """
        cleaned = record_data.copy()

        # Extract bill_id from amendedbill if available
        amended_bill_id = None
        amended_bill_type = None
        amended_bill_number = None
        amended_bill_congress = None
        amended_bill_originchamber = None
        amended_bill_title = None
        is_bill_amendment = False
        if cleaned.get("amendedbill_url"):
            is_bill_amendment = True
            amended_bill_type = cleaned.get("amendedbill_type", "").lower()
            amended_bill_number = cleaned.get("amendedbill_number", "")
            amended_bill_congress = cleaned.get("amendedbill_congress", "")
            amended_bill_originchamber = cleaned.get(
                "amendedbill_originchamber", ""
            )
            amended_bill_title = cleaned.get("amendedbill_title", "")
            if all([amended_bill_type, amended_bill_number, amended_bill_congress]):
                amended_bill_id = f"{amended_bill_type}{amended_bill_number}-{amended_bill_congress}"

        amended_amendment_id = None
        amended_amendment_type = None
        amended_amendment_number = None
        amended_amendment_congress = None
        amended_amendment_purpose = None
        amended_amendment_description = None
        is_amendment_amendment = False
        if cleaned.get("amendedamendment_url"):
            is_amendment_amendment = True
            amended_amendment_type = cleaned.get("amendedamendment_type", "").lower()
            amended_amendment_number = cleaned.get(
                "amendedamendment_number", ""
            )
            amended_amendment_congress = cleaned.get(
                "amendedamendment_congress", ""
            )
            amended_amendment_purpose = cleaned.get(
                    "amendedamendment_purpose", ""
            )
            amended_amendment_description = cleaned.get(
                "amendedamendment_description", ""
            )
            if all(
                    [
                        amended_amendment_type,
                        amended_amendment_number,
                        amended_amendment_congress,
                    ]
                ):
                    amended_amendment_id = f"{amended_amendment_type}{amended_amendment_number}-{amended_amendment_congress}"

        is_treaty_amendment = False
        amended_treaty_id = None
        amended_treaty_number = None
        amended_treaty_congress = None
        if cleaned.get("amendedtreaty_url"):
            is_treaty_amendment = True
            amended_treaty_number = cleaned.get("amendedtreaty_number", "")
            amended_treaty_congress = cleaned.get("amendedtreaty_congress", "")
            if all([amended_treaty_number, amended_treaty_congress]):
                amended_treaty_id = f"td{amended_treaty_congress}-{amended_treaty_number}"

        filtered_cleaned = {
            "amendment_id": str(cleaned.get("amendment_id", "ID_ERROR")),
            "amendment_type": str(cleaned.get("type", "")).lower(),
            "amendment_number": self.safe_int(cleaned.get("number", None)),
            "congress": self.safe_int(cleaned.get("congress", None)),
            "purpose": self.clean_long_text(cleaned.get("purpose", None)),
            "description": self.clean_long_text(cleaned.get("description", None)),
            "chamber": self.standardize_chamber(cleaned.get("chamber", None)),
            "proposed_at": self.standardize_date(cleaned.get("proposeddate", None)),
            "submitted_at": self.standardize_date(cleaned.get("submitteddate", None)),
            "is_bill_amendment": is_bill_amendment,
            "is_treaty_amendment": is_treaty_amendment,
            "is_amendment_amendment": is_amendment_amendment,
            "actions_count": self.safe_int(cleaned.get("actions_count", 0), 0),
            "cosponsors_count": self.safe_int(cleaned.get("cosponsors_count", 0), 0),
            "cosponsors_withdrawn_count": (
                self.safe_int(
                    cleaned.get("cosponsors_countincludingwithdrawncosponsors", 0), 0
                )
                - self.safe_int(cleaned.get("cosponsors_count", 0), 0)
            ),
            "texts_count": self.safe_int(cleaned.get("textversions_count", 0), 0),
            "amendments_to_amendment_count": self.safe_int(
                cleaned.get("amendmentstoamendment_count", 0), 0
            ),
            "updated_at": self.standardize_date(cleaned.get("updatedate", None)),
            # columns that will be dropped from the source table
            "bill_id": amended_bill_id,
            "bill_type": amended_bill_type,
            "bill_number": self.safe_int(amended_bill_number, None),
            "bill_congress": self.safe_int(amended_bill_congress, None),
            "bill_origin_chamber": self.standardize_chamber(amended_bill_originchamber),
            "bill_title": amended_bill_title,
            "amended_amendment_id": amended_amendment_id,
            "amended_amendment_type": amended_amendment_type,
            "amended_amendment_number": self.safe_int(amended_amendment_number, None),
            "amended_amendment_congress": self.safe_int(
                amended_amendment_congress, None
            ),
            "amended_amendment_purpose": amended_amendment_purpose,
            "amended_amendment_description": amended_amendment_description,
            "treaty_id": amended_treaty_id,
            "treaty_number": self.safe_int(amended_treaty_number, None),
            "treaty_congress": self.safe_int(amended_treaty_congress, None),
        }

        # Validate amendment_id format (similar to bills but for amendments)
        # Amendment IDs typically follow pattern: {type}{number}-{congress}
        if not re.match(
            r"^(h|s|hamdt|samdt|suamdt)\d+-\d{1,3}$",
            filtered_cleaned["amendment_id"],
        ):
            logger.warning(
                f"Unusual amendment_id format: {filtered_cleaned['amendment_id']}"
            )

        return filtered_cleaned

    async def _clean_amendments_actions_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for amendments actions records.

        FINAL COLUMNS:
        - action_id TEXT PRIMARY KEY
        - amendment_id TEXT
        - action_code TEXT
        - action_date DATE
        - text TEXT
        - action_type TEXT
        - source_system TEXT
        - source_system_code INTEGER

        STAGING COLUMNS:
        Similar to bills actions but for amendments:
        - text
        - type
        - batch_id
        - actioncode
        - actiondate
        - amendment_id
        - sourcesystem_code
        - sourcesystem_name
        - id

        Args:
            record_data: Raw action record from staging

        Returns:
            Cleaned action record
        """
        cleaned = record_data.copy()

        filtered_cleaned = {
            "action_id": str(cleaned.get("id", "ID_ERROR")),
            "amendment_id": str(cleaned.get("amendment_id", "ID_ERROR")),
            "action_code": str(cleaned.get("actioncode", None)),
            "action_date": self.standardize_date(cleaned.get("actiondate", None)),
            "text": self.clean_long_text(cleaned.get("text", None)),
            "action_type": str(cleaned.get("type", None)),
            "source_system": str(cleaned.get("sourcesystem_name", None)),
            "source_system_code": self.safe_int(cleaned.get("sourcesystem_code", None)),
        }

        return filtered_cleaned

    async def _clean_amendments_actions_recordedvotes_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for amendments actions recorded votes records.

        FINAL COLUMNS:
        - action_id TEXT
        - amendment_id TEXT
        - chamber TEXT
        - congress INTEGER,
        - date TIMESTAMP WITH TIME ZONE,
        - roll_number INTEGER,
        - session INTEGER,
        - url TEXT

        STAGING COLUMNS:
        - date          text,
        - chamber       text,
        - congress      text,
        - rollnumber    text,
        - sessionnumber text,
        - actions_id    text,
        - amendment_id  text,
        - id            text not null
        - url           text

        Args:
            record_data: Raw action record from staging

        """
        cleaned = record_data.copy()

        filtered_cleaned = {
            "action_id": cleaned.get("actions_id", "ID_ERROR"),
            "amendment_id": cleaned.get("amendment_id", "ID_ERROR"),
            "chamber": cleaned.get("chamber", None),
            "congress": self.safe_int(cleaned.get("congress", None)),
            "date": self.standardize_date(cleaned.get("date", None)),
            "roll_number": self.safe_int(cleaned.get("rollnumber", None)),
            "session": self.safe_int(cleaned.get("sessionnumber", None)),
            "url": cleaned.get("url", None),
        }
        return filtered_cleaned

    async def _clean_amendments_actions_committees_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for amendments actions committees records.

        FINAL COLUMNS:
        - action_id TEXT
        - amendment_id TEXT
        - committee_code TEXT
        - committee_name TEXT

        STAGING COLUMNS:
        - url          text,
        - name         text,
        - systemcode   text,
        - actions_id   text,
        - amendment_id text,
        - id           text not null
        primary key
        """
        cleaned = record_data.copy()

        filtered_cleaned = {
            "action_id": cleaned.get("actions_id", "ID_ERROR"),
            "amendment_id": cleaned.get("amendment_id", "ID_ERROR"),
            "committee_code": cleaned.get("systemcode", None),
            "committee_name": cleaned.get("name", None),
        }

        return filtered_cleaned

    async def _clean_amendments_cosponsors_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for amendments cosponsors records.

        FINAL COLUMNS:
        - amendment_id TEXT
        - bioguide_id TEXT
        - display_name TEXT
        - party TEXT
        - state TEXT
        - district INTEGER
        - sponsorship_date DATE
        - sponsorship_withdrawal_date DATE

        STAGING COLUMNS:
        - url                 text,
        - party               text,
        - state               text,
        - batch_id            text,
        - fullname            text,
        - lastname            text,
        - firstname           text,
        - bioguideid          text,
        - middlename          text,
        - amendment_id        text,
        - sponsorshipdate     text,
        - isoriginalcosponsor text,
        - id                  text not null,
        - district            text

        Args:
            record_data: Raw cosponsor record from staging

        Returns:
            Cleaned cosponsor record
        """
        cleaned = record_data.copy()

        filtered_cleaned = {
            "amendment_id": cleaned.get("amendment_id", "ID_ERROR"),
            "bioguide_id": cleaned.get("bioguideid", "ID_ERROR"),
            "display_name": cleaned.get("fullname", None),
            "party": cleaned.get("party", None),
            "state": cleaned.get("state", None),
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

    async def _clean_amendments_texts_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for amendments texts records.
        Handles joined data from amendments_texts and amendments_texts_formats.

        Args:
            record_data: Raw text record from staging (with aggregated formats)

        Returns:
            Cleaned text record

        FINAL COLUMNS:
        - amendment_id TEXT,
        - date TEXT,
        - type TEXT,
        - raw_text TEXT,
        - pdf TEXT,
        - html TEXT,

        STAGING COLUMNS (amendments_texts):
        - amendment_id text,
        - date         text,
        - type         text,
        - formats_json json,
        - processed_at text,
        - _batch_id    text,
        - id           text not null

        STAGING COLUMNS (aggregated from amendments_texts_formats):
        - id           text,
        - texts_id      text,
        - amendment_id text,
        - extracted_at text,
        - type         text,
        - url          text


        JOINED DATA:
        # From bills_texts table:
        - id           text,
        - processed_at text,
        - _batch_id    text,
        - list_index   text,
        - amendment_id text,
        - date         text,
        - type         text,
        - formats_json json,
        - id           text,
        - texts_id     text,
        - amendment_id text,
        - extracted_at text,
        - type         text,
        - url          text

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
            html_url = None

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
                                elif format_type == "html":
                                    html_url = format_url

                except (json.JSONDecodeError, TypeError, AttributeError) as e:
                    logger.warning(
                        f"Error parsing formats_json for amendment {cleaned.get('amendment_id', 'unknown')}: {e}"
                    )

            filtered_cleaned = {
                "amendment_id": cleaned.get("amendment_id", "ID_ERROR"),
                "date": self.standardize_date(cleaned.get("date")),
                "type": cleaned.get("type"),
                "raw_text": None,  # Would need to fetch from URLs if required
                "html": html_url,
                "pdf": pdf_url,
            }

            return filtered_cleaned
        except Exception as e:
            logger.error(f"Error in _clean_amendments_texts_singular: {e}")
            # Return minimal safe record to prevent total failure
            return {
                "amendment_id": cleaned.get("amendment_id", "ID_ERROR"),
                "date": None,
                "type": None,
                "raw_text": None,
                "html": None,
                "pdf": None,
            }

    async def _clean_amendments_sponsors_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for amendments sponsors records
        FINAL COLUMNS:
        - amendment_id TEXT,
        - bioguide_id TEXT,
        - display_name TEXT,
        - party TEXT,
        - state TEXT,
        - district INT,

        STAGING COLUMNS:
        - url          text,
        - party        text,
        - state        text,
        - district     text,
        - fullname     text,
        - lastname     text,
        - firstname    text,
        - bioguideid   text,
        - middlename   text,
        - amendment_id text,
        - id           text,
        - name         text
        """
        cleaned = record_data.copy()

        filtered_cleaned = {
            "amendment_id": cleaned.get("amendment_id", "ID_ERROR"),
            "bioguide_id": cleaned.get("bioguideid", "ID_ERROR"),
            "display_name": cleaned.get("fullname", None),
            "party": cleaned.get("party", None),
            "state": cleaned.get("state", None),
            "district": self.safe_int(cleaned.get("district", None)),
            "name": cleaned.get("name", None),
        }

        return filtered_cleaned

    async def _clean_amendments_on_behalf_of_singular(  # TODO
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for amendments on behalf of records.

        FINAL COLUMNS:
        - amendment_id TEXT
        - bioguide_id TEXT
        - display_name TEXT
        - party TEXT
        - state TEXT
        - type TEXT

        STAGING COLUMNS:
        - url          text,
        """
        cleaned = record_data.copy()
        filtered_cleaned = {
            "amendment_id": cleaned.get("amendment_id", "ID_ERROR"),
            "bioguide_id": cleaned.get("bioguideid", "ID_ERROR"),
            "display_name": cleaned.get("fullname", None),
            "party": cleaned.get("party", None),
            "state": cleaned.get("state", None),
            "type": cleaned.get("type", None),
        }

        return filtered_cleaned

    async def _clean_amendments_links_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for amendments links records.

        FINAL COLUMNS:
        - amendment_id TEXT
        - url TEXT
        - name TEXT

        STAGING COLUMNS:
        - url          text,
        - name         text,
        - amendment_id text
        - id           text
        """
        cleaned = record_data.copy()
        filtered_cleaned = {
            "amendment_id": cleaned.get("amendment_id", "ID_ERROR"),
            "url": cleaned.get("url", None),
            "name": cleaned.get("name", None),
        }
        return filtered_cleaned

    async def _clean_amendments_notes_singular(  # TODO
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for amendments notes records.

        FINAL COLUMNS:
        - amendment_id TEXT
        - note TEXT

        STAGING COLUMNS:
        - amendment_id text,
        - note_text    text,
        - id           text not null
        """
        cleaned = record_data.copy()
        filtered_cleaned = {
            "amendment_id": cleaned.get("amendment_id", "ID_ERROR"),
            "note": cleaned.get("note_text", None),
        }
        return filtered_cleaned

    # =============================================================================
    # AMENDMENTS-SPECIFIC POST-PROCESSING METHODS
    # =============================================================================

    async def _post_process_amendments(self) -> dict[str, Any]:
        """
        Post-processing for amendments:
          • split *_id* columns that point to other artefacts
            into separate link-tables.
        """
        if not self.db_pool:
            raise ValueError("Database pool not configured")

        results: dict[str, Any] = {"status": "success", "operations": []}

        async with self.db_pool.acquire() as conn:
            # ------------------------------------------------------------------ #
            # 1.  Amendments ↔ Bills                                             #
            # ------------------------------------------------------------------ #
            moved = await self.split_columns_to_new_table(
                conn,
                source_table="amendments",
                dest_table="amendments_amended_bills",
                columns={
                    "bill_id": "bill_id",
                    "bill_type": "bill_type",
                    "bill_number": "bill_number",
                    "bill_congress": "congress",
                    "bill_origin_chamber": "origin_chamber",
                    "bill_title": "bill_title",
                },
                src_schema=self.production_schema,
                dst_schema=self.production_schema,
                create_if_missing=True,
                drop_from_source=False,
            )
            results["operations"].append(
                {"table": "amendments_amended_bills", "rows": moved}
            )

            # ------------------------------------------------------------------ #
            # 2.  Amendments ↔ Treaties                                          #
            # ------------------------------------------------------------------ #
            moved = await self.split_columns_to_new_table(
                conn,
                source_table="amendments",
                dest_table="amendments_amended_treaties",
                columns={
                    "treaty_id": "treaty_id",
                    "treaty_congress": "congress",
                    "treaty_number": "treaty_number",
                },
                src_schema=self.production_schema,
                dst_schema=self.production_schema,
                create_if_missing=True,
                drop_from_source=False,
            )
            results["operations"].append(
                {"table": "amendments_amended_treaties", "rows": moved}
            )

            # ------------------------------------------------------------------ #
            # 3.  Amendments ↔ Amendments (self-link)                            #
            # ------------------------------------------------------------------ #
            moved = await self.split_columns_to_new_table(
                conn,
                source_table="amendments",
                dest_table="amendments_amended_amendments",
                columns={
                    "amended_amendment_id": "amended_amendment_id",
                    "amended_amendment_type": "amendment_type",
                    "amended_amendment_number": "amendment_number",
                    "amended_amendment_congress": "congress",
                    "amended_amendment_purpose": "purpose",
                    "amended_amendment_description": "description",
                },
                src_schema=self.production_schema,
                dst_schema=self.production_schema,
                create_if_missing=True,
                drop_from_source=False,
            )
            results["operations"].append(
                {"table": "amendments_amended_amendments", "rows": moved}
            )

        return results
