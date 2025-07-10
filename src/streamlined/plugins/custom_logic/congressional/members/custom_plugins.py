import logging
import re
from typing import Any

# Import the base class that provides shared functionality
from ....base import BaseCleanerLogic, CongressionalBaseFetcherLogic

logger = logging.getLogger(__name__)


class MembersFetcherLogic(CongressionalBaseFetcherLogic):
    """
    Members-specific fetcher logic that extends the base Congressional fetcher logic.

    This class provides custom methods for fetching members data, including:
    - Extracting standardized member IDs
    - Handling related data from URLs
    - Implementing specialized fetching logic for members-specific data

    Inherits shared functionality like get_generic_related_data from
    CongressionalBaseFetcherLogic.
    """

    def __init__(self, data_type: str = "members"):
        super().__init__(data_type)

    def extract_item_id(self, item_data: dict[str, Any], **kwargs) -> str:
        """Extract standardized member ID from member data."""
        return item_data.get("bioguideId", "ID_ERROR")

    def _extract_preliminary_item_id(self, list_item: dict[str, Any]) -> str:
        """
        Derive a stable ID for checkpointing directly from the list-response record.

        """
        return self.extract_item_id(list_item)


class MembersCleanerLogic(BaseCleanerLogic):
    """
    Members-specific cleaner logic extracted from MembersCleaner class.
    Contains all the custom cleaning methods for members data.
    """

    def __init__(
        self,
        data_type_name: str = "members",
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
            "members_leadershiproles": ["members_leadership"],
            # Add other members multi-table or alias mappings here as needed
        }

    # =============================================================================
    # MEMBERS-SPECIFIC DATA TYPE CLEANING METHODS
    # =============================================================================

    async def _clean_members_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual members records.

        FINAL COLUMNS:
        - bioguide_id TEXT PRIMARY KEY,
        - normalized_name TEXT,
        - direct_order_name TEXT,
        - inverted_order_name TEXT,
        - honorific_prefix TEXT,
        - first_name TEXT,
        - middle_name TEXT,
        - last_name TEXT,
        - suffix TEXT,
        - nickname TEXT,
        - party TEXT,
        - state TEXT,
        - district INTEGER,
        - birth_year INTEGER,
        - death_year INTEGER,
        - official_url TEXT,
        - office_address TEXT,
        - office_city TEXT,
        - office_district TEXT,
        - office_zip TEXT,
        - office_phone TEXT,
        - sponsored_legislation_count INTEGER,
        - cosponsored_legislation_count INTEGER,
        - depiction_image_url TEXT,
        - depiction_attribution TEXT,
        - is_current_member BOOLEAN,
        - updated_at TIMESTAMP WITH TIME ZONE

        STAGING COLUMNS:
        Based on Congressional API member structure, typically includes:
        - state                            text,
        - batch_id                         text,
        - district                         text,
        - lastname                         text,
        - birthyear                        text,
        - deathyear                        text,
        - depiction_imageurl               text,
        - depiction_attribution            text,
        - firstname                        text,
        - bioguideid                       text,
        - middlename                       text,
        - updatedate                       text,
        - bioguide_id                      text
        - currentmember                    text,
        - directordername                  text,
        - invertedordername                text,
        - sponsoredlegislation_url         text,
        - sponsoredlegislation_count       text,
        - cosponsoredlegislation_url       text,
        - cosponsoredlegislation_count     text,
        - honorificname                    text,
        - addressinformation_city          text,
        - addressinformation_zipcode       text
        - addressinformation_district      text,
        - addressinformation_phonenumber   text,
        - addressinformation_officeaddress text,
        - officialwebsiteurl               text,
        - suffixname                       text,
        - nickname                         text

        Args:
            record_data: Raw members record from staging
        Returns:
            Cleaned members record
        """
        cleaned = record_data.copy()

        # Safely build a fully-upper-cased name using only non-empty parts to
        # avoid calling `.upper()` on None (which would raise an AttributeError).
        first = (cleaned.get("firstname") or "").strip()
        middle = (cleaned.get("middlename") or "").strip()
        last = (cleaned.get("lastname") or "").strip()
        suffix = (cleaned.get("suffixname") or "").strip()

        if first and last:
            parts = [first, middle, last, suffix]
            # Filter out empty strings then uppercase each part
            normalized_name = " ".join(p.upper() for p in parts if p)
        else:
            normalized_name = None

        filtered_cleaned = {
            "bioguide_id": cleaned.get("bioguide_id"),
            "normalized_name": normalized_name,
            "direct_order_name": cleaned.get("directordername"),
            "inverted_order_name": cleaned.get("invertedordername"),
            "honorific_prefix": cleaned.get("honorificname"),
            "first_name": cleaned.get("firstname"),
            "middle_name": cleaned.get("middlename"),
            "last_name": cleaned.get("lastname"),
            "suffix": cleaned.get("suffixname"),
            "nickname": cleaned.get("nickname"),
            "current_party": None,
            "state": cleaned.get("state"),
            "district": self.safe_int(cleaned.get("district")),
            "birth_year": self.safe_int(cleaned.get("birthyear")),
            "death_year": self.safe_int(cleaned.get("deathyear")),
            "official_url": cleaned.get("officialwebsiteurl"),
            "office_address": cleaned.get("addressinformation_officeaddress"),
            "office_city": cleaned.get("addressinformation_city"),
            "office_district": cleaned.get("addressinformation_district"),
            "office_zip": cleaned.get("addressinformation_zipcode"),
            "office_phone": cleaned.get("addressinformation_phonenumber"),
            "sponsored_legislation_count": self.safe_int(
                cleaned.get("sponsoredlegislation_count")
            ),
            "cosponsored_legislation_count": self.safe_int(
                cleaned.get("cosponsoredlegislation_count")
            ),
            "depiction_image_url": cleaned.get("depiction_imageurl"),
            "depiction_attribution": cleaned.get("depiction_attribution"),
            "is_current_member": cleaned.get("currentmember") == "true",
            "updated_at": self.standardize_date(cleaned.get("updatedate")),
        }

        # Validate bioguide_id format
        # Bioguide IDs typically follow one capital letter and 6 numbers: {letter}{numbers}
        if not re.match(
            r"^[A-Z]\d{6}$",
            filtered_cleaned["bioguide_id"],
        ):
            logger.warning(
                f"Unusual bioguide_id format: {filtered_cleaned['bioguide_id']}"
            )

        return filtered_cleaned

    async def _clean_members_partyhistory_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual members party history records.

        FINAL COLUMNS:
            - bioguide_id TEXT,
            - party_code TEXT,
            - party_name TEXT,
            - start_year INTEGER,
            - end_year INTEGER,

        STAGING COLUMNS:
            - endyear           text,
            - partyname         text,
            - startyear         text,
            - partyabbreviation text,
            - bioguide_id       text,
            - id                text

        Args:
            record_data: Raw members party history record from staging
        """
        cleaned = record_data.copy()

        filtered_cleaned = {
            "bioguide_id": cleaned.get("bioguide_id"),
            "party_code": cleaned.get("partyabbreviation"),
            "party_name": cleaned.get("partyname"),
            "start_year": self.safe_int(cleaned.get("startyear")),
            "end_year": self.safe_int(cleaned.get("endyear")),
        }

        return filtered_cleaned

    async def _clean_members_leadershiproles_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual members leadership roles records.

        FINAL COLUMNS:
            - bioguide_id TEXT,
            - role TEXT,
            - congress INTEGER,
            - chamber TEXT,
            - is_current BOOLEAN,

        STAGING COLUMNS:
            - type        text,
            - congress    text,
            - bioguide_id text,
            - id          text
            - current     text
        """
        cleaned = record_data.copy()

        filtered_cleaned = {
            "bioguide_id": cleaned.get("bioguide_id"),
            "role": cleaned.get("type"),
            "congress": self.safe_int(cleaned.get("congress")),
            "chamber": None,
            "is_current": cleaned.get("current") == "true",
        }

        return filtered_cleaned

    async def _clean_members_terms_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual members terms records.

        FINAL COLUMNS:
            - bioguide_id TEXT,
            - member_type TEXT,
            - chamber TEXT,
            - congress INTEGER,
            - start_year INTEGER,
            - end_year INTEGER,
            - state_name TEXT,
            - state_code TEXT,
            - district INTEGER,

        STAGING COLUMNS:

            - chamber     text,
            - endyear     text,
            - congress    text,
            - startyear   text,
            - statecode   text,
            - statename   text,
            - membertype  text,
            - bioguide_id text,
            - id
            - district    text
        """
        cleaned = record_data.copy()

        filtered_cleaned = {
            "bioguide_id": cleaned.get("bioguide_id"),
            "member_type": cleaned.get("membertype"),
            "chamber": self.standardize_chamber(cleaned.get("chamber")),
            "congress": self.safe_int(cleaned.get("congress")),
            "start_year": self.safe_int(cleaned.get("startyear")),
            "end_year": self.safe_int(cleaned.get("endyear")),
            "state_name": cleaned.get("statename"),
            "state_code": cleaned.get("statecode"),
            "district": self.safe_int(cleaned.get("district")),
        }

        return filtered_cleaned

    # =============================================================================
    # MEMBERS-SPECIFIC POST-PROCESSING METHODS
    # =============================================================================

    async def _post_process_members(self) -> dict[str, Any]:
        """
        Post-processing for members:
        - add party to members table
        - add chamber to members_leadershiproles table
        """
        if not self.db_pool:
            raise ValueError("Database pool not configured")

        results: dict[str, Any] = {
            "status": "success",
            "operations": [],
            "rows_affected": 0,
        }

        async with self.db_pool.acquire() as conn:
            # Operation 1: Populate current_party field based on members_partyhistory table
            try:
                async with conn.transaction():
                    sql = f"""
                        UPDATE {self.production_schema}.members
                        SET current_party = (SELECT party_name
                                    FROM {self.production_schema}.members_partyhistory mh
                                    WHERE mh.bioguide_id = members.bioguide_id
                                    AND mh.end_year IS NULL
                                )
                        WHERE bioguide_id IS NOT NULL;
                    """

                    result = await conn.execute(sql)
                    rows_affected = int(result.split()[-1]) if result.split() else 0

                    results["operations"].append(
                        {
                            "name": "get_current_party",
                            "status": "success",
                            "rows_affected": rows_affected,
                        }
                    )
                    results["rows_affected"] += rows_affected

                    logger.info(f"Updated current_party for {rows_affected} members")

            except Exception as e:
                logger.error(f"Error getting current_party: {e}")
                results["operations"].append(
                    {
                        "name": "get_current_party",
                        "status": "error",
                        "error": str(e),
                    }
                )
                results["status"] = "partial_failure"

            # Operation 2: Add chamber to members_leadershiproles table
            try:
                async with conn.transaction():
                    sql = f"""
                        UPDATE {self.production_schema}.members_leadershiproles
                        SET chamber = (SELECT chamber
                                    FROM {self.production_schema}.members_terms mt
                                    WHERE mt.bioguide_id = members_leadershiproles.bioguide_id
                                    AND mt.congress = members_leadershiproles.congress
                                )
                        WHERE bioguide_id IS NOT NULL
                        AND chamber IS NULL;
                    """

                    result = await conn.execute(sql)
                    rows_affected = int(result.split()[-1]) if result.split() else 0

                    results["operations"].append(
                        {
                            "name": "get_chamber",
                            "status": "success",
                            "rows_affected": rows_affected,
                        }
                    )
                    results["rows_affected"] += rows_affected

                    logger.info(
                        f"Updated chamber for {rows_affected} members_leadershiproles"
                    )

            except Exception as e:
                logger.error(f"Error getting chamber: {e}")
                results["operations"].append(
                    {
                        "name": "get_chamber",
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
