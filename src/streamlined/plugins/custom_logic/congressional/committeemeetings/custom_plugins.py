import json
import logging
from typing import Any

# Import the base class that provides shared functionality
from ....base import BaseCleanerLogic, CongressionalBaseFetcherLogic

logger = logging.getLogger(__name__)


class CommitteemeetingsFetcherLogic(CongressionalBaseFetcherLogic):
    """
    Committeemeetings-specific fetcher logic that extends the base Congressional fetcher logic.

    This class provides custom methods for fetching committeemeetings data, including:
    - Extracting standardized committe meeting IDs
    - Handling related data from URLs
    - Implementing specialized fetching logic for committeemeetings-specific data

    Inherits shared functionality like get_generic_related_data from
    CongressionalBaseFetcherLogic.
    """

    def __init__(self, data_type: str = "committeemeetings"):
        super().__init__(data_type)

    # =============================================================================
    # REQUIRED ABSTRACT METHOD IMPLEMENTATIONS
    # =============================================================================

    def extract_item_id(self, item_data: dict[str, Any], **kwargs) -> str:
        """Extract standardized committe meeting ID from committe meeting data."""
        return item_data.get("eventId", "ID_ERROR")

    def _extract_preliminary_item_id(self, list_item: dict[str, Any]) -> str:
        """
        Derive a stable ID for checkpointing directly from the list-response record.
        This guarantees a non-null value so the progress-tracker can reliably
        decide whether the item was processed in a previous run.
        """
        return self.extract_item_id(list_item)


class CommitteemeetingsCleanerLogic(BaseCleanerLogic):
    """
    Committeemeetings-specific cleaner logic extracted from CommitteemeetingsCleaner class.
    Contains all the custom cleaning methods for committeemeetings data.
    """

    def __init__(
        self,
        data_type_name: str = "committeemeetings",
        system_name: str = "congressional",
        staging_schema: str = "bicam_staging_congressional",
        production_schema: str = "bicam_congressional",
    ):
        super().__init__()
        self.data_type_name = data_type_name
        self.system_name = system_name
        self.staging_schema = staging_schema
        self.production_schema = production_schema
        self.db_pool = None

        # Map data types that span or alias multiple staging tables.
        # In staging, leadership-role data lives in `members_leadership`,
        # but the logical/production data-type name we expose is
        # `members_leadershiproles`.  This mapping lets the base cleaner
        # stream from the correct table without emitting missing-table warnings.
        self.multi_table_data_types = {
            # Add other multi-table or alias mappings here as needed
        }

    # =============================================================================
    # COMMITTEE MEETINGS-SPECIFIC DATA TYPE CLEANING METHODS
    # =============================================================================

    async def _clean_committeemeetings_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual committee meetings records.

        FINAL COLUMNS:
        - meeting_id TEXT PRIMARY KEY,
        - title TEXT,
        - meeting_type TEXT,
        - chamber TEXT,
        - congress INTEGER,
        - date TIMESTAMP,
        - room TEXT,
        - street_address TEXT,
        - building TEXT,
        - city TEXT,
        - state TEXT,
        - zip_code TEXT,
        - meeting_status TEXT,
        - updated_at TIMESTAMP WITH TIME ZONE

        STAGING COLUMNS:
        Based on Congressional API committee structure, typically includes:
        - date                     text,
        - type                     text,
        - title                    text,
        - chamber                  text,
        - eventid                  text,
        - batch_id                 text,
        - congress                 text,
        - location_room            text,
        - location_building        text,
        - meeting_id               text
        - updatedate               text,
        - meetingstatus            text,
        - relateditems_bills       text, # json
        - location_address         text, # json
        - relateditems_nominations text, # json
        - relateditems_treaties    text  # json

        Args:
            record_data: Raw committee meetings record from staging
        Returns:
            Cleaned committee meetings record
        """
        cleaned = record_data.copy()

        location_address = cleaned.get("location_address", {})
        location_address_json = json.loads(location_address) if location_address else {}

        street_address = location_address_json.get("street-address")
        building = location_address_json.get("building_name")
        city = location_address_json.get("city")
        state = location_address_json.get("state")
        zip_code = location_address_json.get("postal_code")

        filtered_cleaned = {
            "meeting_id": cleaned.get("meeting_id", "ID_ERROR"),
            "title": (
                cleaned.get("title").strip('"').strip("'").replace("• ", "- ").strip()
                if cleaned.get("title")
                else None
            ),
            "meeting_type": cleaned.get("type"),
            "chamber": self.standardize_chamber(cleaned.get("chamber")),
            "congress": self.safe_int(cleaned.get("congress", 0)),
            "date": self.standardize_date(cleaned.get("date")),
            "room": cleaned.get("location_room"),
            "street_address": street_address,
            "building": cleaned.get("location_building", building),
            "city": city,
            "state": state,
            "zip_code": zip_code,
            "meeting_status": cleaned.get("meetingstatus"),
            "updated_at": self.standardize_date(cleaned.get("updatedate")),
        }
        return filtered_cleaned

    async def _clean_committeemeetings_committees_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual committees records.

        FINAL COLUMNS:
        - meeting_id TEXT PRIMARY KEY,
        - committee_code TEXT PRIMARY KEY,
        - committee_name TEXT

        STAGING COLUMNS:
        - url        text,
        - systemcode text,
        - meeting_id text,
        - id         text
        - name       text
        """
        cleaned = record_data.copy()

        filtered_cleaned = {
            "meeting_id": cleaned.get("meeting_id", "ID_ERROR"),
            "committee_code": cleaned.get("committee_code", "ID_ERROR"),
            "committee_name": cleaned.get("name"),
        }
        return filtered_cleaned

    async def _clean_committeemeetings_hearingtranscript_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual hearings records.

        FINAL COLUMNS:
        - meeting_id TEXT PRIMARY KEY,
        - hearing_id TEXT PRIMARY KEY,

        STAGING COLUMNS:
        - url          text,
        - jacketnumber text,
        - meeting_id   text,
        - id           text
        primary key
        """
        # Register target table override - much simpler!
        self._register_target_table_override("committeemeetings_hearings")

        cleaned = record_data.copy()

        # Parse URL to extract hearing_id from format: https://api.congress.gov/v3/hearing/{congress}/{chamber}/{hearing_number}
        url = cleaned.get("url", "")
        hearing_id = "ID_ERROR"

        if url and "hearing/" in url:
            try:
                # Split URL by "hearing/" and get the part after it
                url_parts = url.split("hearing/")
                if len(url_parts) > 1:
                    # Extract congress/chamber/hearing_number part
                    hearing_part = url_parts[1].split("?")[0]  # Remove query params
                    hearing_components = hearing_part.split("/")

                    if len(hearing_components) >= 3:
                        congress = hearing_components[0]
                        chamber = hearing_components[1]
                        hearing_number = hearing_components[2]

                        # Construct hearing_id as congress-chamber-hearing_number
                        hearing_id = (
                            f"{chamber[0].lower()}hrg{hearing_number}-{congress}"
                        )
                    else:
                        logger.warning(f"Invalid hearing URL format: {url}")
            except Exception as e:
                logger.warning(f"Error parsing hearing URL {url}: {e}")

        filtered_cleaned = {
            "meeting_id": cleaned.get("meeting_id", "ID_ERROR"),
            "hearing_id": hearing_id,
            "jacket_number": cleaned.get("jacketnumber"),
        }

        # Just return the cleaned data - no special format needed!
        return filtered_cleaned

    async def _clean_committeemeetings_meetingdocuments_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual meeting documents records.

        FINAL COLUMNS:
        - meeting_id TEXT,
        - name TEXT,
        - document_type TEXT,
        - description TEXT,
        - url TEXT,
        - UNIQUE (meeting_id, document_name, document_type, description, url)

        STAGING COLUMNS:
        - url          text,
        - name         text,
        - format       text,
        - documenttype text,
        - meeting_id   text,
        - id           text,
        - description  text
        """
        cleaned = record_data.copy()

        filtered_cleaned = {
            "meeting_id": cleaned.get("meeting_id", "ID_ERROR"),
            "document_name": cleaned.get("name"),
            "document_type": cleaned.get("documenttype"),
            "format": cleaned.get("format"),
            "description": cleaned.get("description"),
            "url": cleaned.get("url"),
        }
        return filtered_cleaned

    async def _clean_committeemeetings_videos_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual videos records.

        FINAL COLUMNS:
        - meeting_id TEXT,
        - video_name TEXT,
        - url TEXT,
        - UNIQUE (meeting_id, video_name, url)

        STAGING COLUMNS:
        - url        text,
        - name       text,
        - meeting_id text,
        - id         text not null
        primary key
        """
        cleaned = record_data.copy()

        filtered_cleaned = {
            "meeting_id": cleaned.get("meeting_id", "ID_ERROR"),
            "video_name": cleaned.get("name"),
            "url": cleaned.get("url"),
        }
        return filtered_cleaned

    async def _clean_committeemeetings_witnessdocuments_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual witness documents records.

        FINAL COLUMNS:
        - meeting_id TEXT,
        - document_type TEXT,
        - url TEXT,
        - UNIQUE (meeting_id, document_type, url)

        STAGING COLUMNS:
        - url          text,
        - format       text,
        - documenttype text,
        - meeting_id   text,
        - id           text not null
        """
        cleaned = record_data.copy()
        filtered_cleaned = {
            "meeting_id": cleaned.get("meeting_id", "ID_ERROR"),
            "document_type": cleaned.get("documenttype"),
            "format": cleaned.get("format"),
            "url": cleaned.get("url"),
        }
        return filtered_cleaned

    async def _clean_committeemeetings_witnesses_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual witnesses records.

        FINAL COLUMNS:
        - meeting_id TEXT,
        - name TEXT,
        - position TEXT,
        - organization TEXT,
        - UNIQUE (meeting_id, name, position, organization)

        STAGING COLUMNS:
        - name         text,
        - organization text,
        - meeting_id   text,
        - id           text,
        - position     text
        """
        cleaned = record_data.copy()
        filtered_cleaned = {
            "meeting_id": cleaned.get("meeting_id", "ID_ERROR"),
            "name": cleaned.get("name"),
            "position": cleaned.get("position"),
            "organization": cleaned.get("organization"),
        }
        return filtered_cleaned

    # =============================================================================
    # COMMITTEE MEETINGS-SPECIFIC POST-PROCESSING METHODS
    # =============================================================================

    async def _post_process_committeemeetings(self) -> dict[str, Any]:
        """
        Post-processing for committee meetings:
        - insert related bills
        - insert related treaties
        - insert related nominations
        """
        if not self.db_pool:
            raise ValueError("Database pool not configured")

        results: dict[str, Any] = {
            "status": "success",
            "operations": [],
            "rows_affected": 0,
        }

        async with self.db_pool.acquire() as conn:
            # Operation 1: Insert related bills from committeemeetings relateditems_bills
            try:
                async with conn.transaction():
                    logger.info("Running insert related bills query...")
                    sql = f"""
                        INSERT INTO {self.production_schema}.committeemeetings_bills (meeting_id, bill_id, bill_type, bill_number, congress)
                        SELECT
                            cm.meeting_id,
                            CONCAT(lower(bill->>'type'), bill->>'number', '-', bill->>'congress') as bill_id,
                            bill->>'type',
                            (bill->>'number')::DOUBLE PRECISION,
                            (bill->>'congress')::INTEGER
                        FROM {self.staging_schema}.committeemeetings cm
                        CROSS JOIN LATERAL jsonb_array_elements(
                            CASE
                                WHEN cm.relateditems_bills IS NOT NULL AND cm.relateditems_bills != ''
                                THEN cm.relateditems_bills::jsonb
                                ELSE '[]'::jsonb
                            END
                        ) as bill
                        WHERE cm.relateditems_bills IS NOT NULL
                        AND cm.relateditems_bills != ''
                        ON CONFLICT (meeting_id, bill_id) DO NOTHING;
                    """

                    result = await conn.execute(sql)
                    rows_affected = int(result.split()[-1]) if result.split() else 0

                    results["operations"].append(
                        {
                            "name": "insert_related_bills",
                            "status": "success",
                            "rows_affected": rows_affected,
                        }
                    )
                    results["rows_affected"] += rows_affected

                    logger.info(
                        f"Inserted {rows_affected} related bills for committee meetings"
                    )

            except Exception as e:
                logger.error(f"Error inserting related bills: {e}")
                results["operations"].append(
                    {
                        "name": "insert_related_bills",
                        "status": "error",
                        "error": str(e),
                    }
                )
                results["status"] = "partial_failure"

            # Operation 2: Insert related nominations from committeemeetings relateditems_nominations
            try:
                async with conn.transaction():
                    logger.info("Running insert related nominations query...")
                    sql = f"""
                        INSERT INTO {self.production_schema}.committeemeetings_nominations (meeting_id, nomination_id, nomination_type, nomination_number, nomination_part, congress)
                        SELECT
                            cm.meeting_id,
                            CONCAT('PN', nomination->>'number', '-', COALESCE(nomination->>'part', '00'), '-', nomination->>'congress') as nomination_id,
                            nomination->>'type',
                            (nomination->>'number')::INTEGER,
                            nomination->>'part',
                            (nomination->>'congress')::INTEGER
                        FROM {self.staging_schema}.committeemeetings cm
                        CROSS JOIN LATERAL jsonb_array_elements(
                            CASE
                                WHEN cm.relateditems_nominations IS NOT NULL AND cm.relateditems_nominations != ''
                                THEN cm.relateditems_nominations::jsonb
                                ELSE '[]'::jsonb
                            END
                        ) as nomination
                        WHERE cm.relateditems_nominations IS NOT NULL
                        AND cm.relateditems_nominations != ''
                        ON CONFLICT (meeting_id, nomination_id) DO NOTHING;
                    """

                    result = await conn.execute(sql)
                    rows_affected = int(result.split()[-1]) if result.split() else 0

                    results["operations"].append(
                        {
                            "name": "insert_related_nominations",
                            "status": "success",
                            "rows_affected": rows_affected,
                        }
                    )
                    results["rows_affected"] += rows_affected

                    logger.info(
                        f"Inserted {rows_affected} related nominations for committee meetings"
                    )

            except Exception as e:
                logger.error(f"Error inserting related nominations: {e}")
                results["operations"].append(
                    {
                        "name": "insert_related_nominations",
                        "status": "error",
                        "error": str(e),
                    }
                )
                results["status"] = "partial_failure"

            # Operation 3: Insert related treaties from committeemeetings relateditems_treaties
            try:
                async with conn.transaction():
                    logger.info("Running insert related treaties query...")
                    sql = f"""
                        INSERT INTO {self.production_schema}.committeemeetings_treaties (meeting_id, treaty_id, treaty_number, congress)
                        SELECT
                            cm.meeting_id,
                            CONCAT('td', treaty->>'congress', '-', treaty->>'number') as treaty_id,
                            (treaty->>'number')::INTEGER,
                            (treaty->>'congress')::INTEGER
                        FROM {self.staging_schema}.committeemeetings cm
                        CROSS JOIN LATERAL jsonb_array_elements(
                            CASE
                                WHEN cm.relateditems_treaties IS NOT NULL AND cm.relateditems_treaties != ''
                                THEN cm.relateditems_treaties::jsonb
                                ELSE '[]'::jsonb
                            END
                        ) as treaty
                        WHERE cm.relateditems_treaties IS NOT NULL
                        AND cm.relateditems_treaties != ''
                        ON CONFLICT (meeting_id, treaty_id) DO NOTHING;
                    """

                    result = await conn.execute(sql)
                    rows_affected = int(result.split()[-1]) if result.split() else 0

                    results["operations"].append(
                        {
                            "name": "insert_related_treaties",
                            "status": "success",
                            "rows_affected": rows_affected,
                        }
                    )
                    results["rows_affected"] += rows_affected

                    logger.info(
                        f"Inserted {rows_affected} related treaties for committee meetings"
                    )

            except Exception as e:
                logger.error(f"Error inserting related treaties: {e}")
                results["operations"].append(
                    {
                        "name": "insert_related_treaties",
                        "status": "error",
                        "error": str(e),
                    }
                )
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
