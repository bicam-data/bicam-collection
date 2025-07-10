"""
Congressional Directories Custom Plugin Logic

This module contains all the custom logic for congressional directories data type including:
- Fetching logic (from fetcher.py)
- Cleaning logic (from cleaner.py)

"""

import logging
import re
from collections.abc import AsyncGenerator
from typing import Any

from streamlined.plugins.base import BaseCleanerLogic

logger = logging.getLogger(__name__)


class CongressionalDirectoriesFetcher:
    """
    Congressional Directories fetcherogic that extends the base GovInfo fetcher logic.

    This class provides custom methods for fetching congressional directories data, including:
    - Extracting standardized congressional directories IDs
    - Handling related data from URLs
    - Implementing specialized fetching logic for congressional directories-specific data

    Inherits shared functionality like get_generic_related_data from
    GovInfoBaseFetcher.
    """

    def __init__(self, data_type: str = "congressionaldirectories"):
        self.data_type = data_type


class CongressionalDirectoriesCleaner(BaseCleanerLogic):
    """
    Congressional Directories cleaner logic extracted from CongressionalDirectoriesCleaner class.
    Contains all the custom cleaning methods for congressional directories data.
    """

    def __init__(
        self,
        data_type_name: str = "congressionaldirectories",
        system_name: str = "govinfo",
        staging_schema: str = "bicam_staging_govinfo",
        production_schema: str = "bicam_govinfo",
    ):
        self.data_type_name = data_type_name
        self.system_name = system_name
        self.staging_schema = staging_schema
        self.production_schema = production_schema
        self.db_pool = None

        # Set bills-specific multi-table processing configuration
        self.multi_table_data_types = {
            "congressionaldirectories_granules": [
                "congressionaldirectories_granules",
                "congressionaldirectories_granules_members",
                "congressionaldirectories_granules_online",
                "congressionaldirectories_granules_members_name",
            ],
        }

        # TODO: fix bioguideids, get proper metadata

    async def _stream_congressionaldirectories_granules_joined_chunks(
        self, chunk_size: int
    ) -> AsyncGenerator[list[dict[str, Any]], None]:
        """
        Stream joined data from congressional directories granules tables.
        Joins granules with granules_members, granules_online, and granules_members_name tables.
        Coalesces membername with parsed name from granules_members_name.
        """
        if not self.db_pool:
            raise ValueError("Database pool not configured")

        async with self.db_pool.acquire() as conn:
            try:
                # First, check if all required tables exist
                tables_exist_query = """
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_schema = $1
                AND table_name IN (
                    'congressionaldirectories_granules',
                    'congressionaldirectories_granules_members',
                    'congressionaldirectories_granules_online',
                    'congressionaldirectories_granules_members_name'
                )
                """
                tables_count = await conn.fetchval(
                    tables_exist_query, self.staging_schema
                )

                if tables_count < 4:
                    logger.warning(
                        f"One or more congressional directories granules staging tables do not exist. Found {tables_count}/4 tables."
                    )
                    return

                # Get total count for logging
                count_query = f"""
                SELECT COUNT(DISTINCT g.id)
                FROM {self.staging_schema}.congressionaldirectories_granules g
                WHERE g.granuleclass = 'CONGRESSMEMBERSTATE'
                AND g.subgranuleclass != 'STATEDELEGATION'
                """
                total_count = await conn.fetchval(count_query)

                if total_count == 0:
                    logger.info("No congressional directories granules records found")
                    return

                logger.info(
                    f"Streaming {total_count} congressional directories granules records with members, online data, and member names in chunks of {chunk_size}"
                )

                # Stream using pagination with flattened joined data
                offset = 0
                while True:
                    # Join granules with members, online data, and member names - flattened into one big table
                    query = f"""
                    SELECT
                        g.id as granule_id,
                        g.granuleid,
                        g.packageid,
                        g.state,
                        g.title,
                        g.category,
                        g.docclass,
                        g.dateissued,
                        g.population,
                        g.district,
                        g.year,
                        g.biography,
                        g.constituents,
                        g.zipcodes,
                        g.graphicsinpdf,
                        g.online,
                        g.download_pdflink,
                        g.download_txtlink,
                        g.download_ziplink,
                        g.download_modslink,
                        g.download_premislink,
                        g.detailslink,
                        g.packagelink,
                        g.relatedlink,
                        g.granuleclass,
                        g.granuleslink,
                        g.lastmodified,
                        g.collectioncode,
                        g.collectionname,
                        g.subgranuleclass,
                        g.processed_at,
                        g.source_doc_id,
                        -- Members data (joined)
                        gm.gpoid,
                        gm.party,
                        gm.state as member_state,
                        gm.chamber,
                        gm.congress,
                        gm.bioguideid,
                        COALESCE(gm.membername, gmn.parsed) as membername,
                        gm.authorityid,
                        gm.list_index as member_list_index,
                        -- Online data (joined)
                        go.value as online_value,
                        go.list_index as online_list_index,
                        -- Members name data (joined, for reference)
                        gmn.parsed as membername_parsed,
                        gmn.authority_fnf as membername_authority_fnf,
                        gmn.authority_lnf as membername_authority_lnf,
                        gmn.authority_other as membername_authority_other,
                        gmn.id as membername_id,
                        gmn.members_id as membername_members_id,
                        gmn.list_index as membername_list_index
                    FROM {self.staging_schema}.congressionaldirectories_granules g
                    LEFT JOIN {self.staging_schema}.congressionaldirectories_granules_members gm
                        ON g.id = gm.granule_id
                    LEFT JOIN {self.staging_schema}.congressionaldirectories_granules_members_name gmn
                        ON gm.id = gmn.members_id
                    LEFT JOIN {self.staging_schema}.congressionaldirectories_granules_online go
                        ON g.id = go.granule_id
                    WHERE g.granuleclass = 'CONGRESSMEMBERSTATE'
                    AND g.subgranuleclass != 'STATEDELEGATION'
                    ORDER BY g.processed_at DESC, gm.list_index, go.list_index
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
                                f"Error converting congressional directories granules row to dict: {e}, row type: {type(row)}, row: {row}"
                            )
                            continue

                    if chunk:  # Only yield if we have valid records
                        yield chunk

                    offset += chunk_size

                    # Log progress periodically
                    if offset % (chunk_size * 10) == 0:
                        logger.debug(
                            f"Streamed {offset} congressional directories granules records with members, online data, and member names"
                        )

            except Exception as e:
                logger.error(
                    f"Error streaming congressional directories granules with joined data: {e}"
                )
                raise

    async def _clean_congressionaldirectories_granules_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual congressional directories granules records.
        STAGING COLUMNS:
        - title                           text,
        - branch                          text,
        - session                         text,
        - category                        text,
        - congress                        text,
        - docclass                        text,
        - download_pdflink                text,
        - download_txtlink                text,
        - download_ziplink                text,
        - download_modslink               text,
        - download_premislink             text,
        - packageid                       text,
        - publisher                       text,
        - dateissued                      text,
        - detailslink                     text,
        - documenttype                    text,
        - granuleslink                    text,
        - lastmodified                    text,
        - collectioncode                  text,
        - collectionname                  text,
        - otheridentifier_ils_system_id   text,
        - otheridentifier_migrated_doc_id text,
        - sudocclassnumber                text,
        - governmentauthor1               text,
        - governmentauthor2               text,
        - package_id                      text,
        - processed_at                    text,
        - source_doc_id                   text,
        - otheridentifier_isbn            text

        FINAL COLUMNS:
        - package_id TEXT PRIMARY KEY,
        - title TEXT,
        - congress INTEGER,
        - issued_at DATE,
        - branch TEXT,
        - government_author1 TEXT,
        - government_author2 TEXT,
        - publisher TEXT,
        - collection_code TEXT,
        - ils_system_id TEXT,
        - migrated_doc_id TEXT,
        - su_doc_class_number TEXT,
        - text_url TEXT,
        - pdf_url TEXT,
        - last_modified TIMESTAMP WITH TIME ZONE

        """
        cleaned = record_data.copy()

        # Apply congressional directories-specific cleaning logic
        filtered_cleaned = {
            "package_id": str(cleaned.get("package_id", "ID_ERROR")),
            "title": str(cleaned.get("title", None)),
            "congress": self.safe_int(cleaned.get("congress", None)),
            "issued_at": self.standardize_date(cleaned.get("dateissued", None)),
            "branch": str(cleaned.get("branch", None)),
            "government_author1": str(cleaned.get("governmentauthor1", None)),
            "government_author2": str(cleaned.get("governmentauthor2", None)),
            "publisher": str(cleaned.get("publisher", None)),
            "collection_code": str(cleaned.get("collectioncode", None)),
            "ils_system_id": str(cleaned.get("otheridentifier_ils_system_id", None)),
            "migrated_doc_id": str(cleaned.get("otheridentifier_migrated_doc_id", None)),
            "su_doc_class_number": str(cleaned.get("sudocclassnumber", None)),
            "text_url": str(cleaned.get("download_txtlink", None)),
            "pdf_url": str(cleaned.get("download_pdflink", None)),
            "last_modified": self.standardize_date(cleaned.get("lastmodified", None)),
        }

        return filtered_cleaned


    # Add other cleaning methods for bills sub-tables
    async def _clean_congressionaldirectories_granules_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """Custom cleaning logic for congressional directories granules records, matching bicam_govinfo.members schema.
        g.id as granule_id,
        g.granuleid,
        g.packageid,
        g.state,
        g.title,
        g.category,
        g.docclass,
        g.dateissued,
        g.population,
        g.district,
        g.year,
        g.biography,
        g.constituents,
        g.zipcodes,
        g.graphicsinpdf,
        g.online,
        g.download_pdflink,
        g.download_txtlink,
        g.download_ziplink,
        g.download_modslink,
        g.download_premislink,
        g.detailslink,
        g.packagelink,
        g.relatedlink,
        g.granuleclass,
        g.granuleslink,
        g.lastmodified,
        g.collectioncode,
        g.collectionname,
        g.subgranuleclass,
        g.processed_at,
        g.source_doc_id,
        -- Members data (joined)
        gm.gpoid,
        gm.party,
        gm.state as member_state,
        gm.chamber,
        gm.congress,
        gm.bioguideid,
        gm.membername,
        gm.authorityid,
        gm.list_index as member_list_index,
        -- Online data (joined)
        go.value as online_value,
        go.list_index as online_list_index
        """

        cleaned = record_data.copy()
        if "writerep" not in cleaned.get("online_value"):
            if (
                ".house.gov" in cleaned.get("online_value")
                or ".senate.gov" in cleaned.get("online_value")
            ) and "@" not in cleaned.get("online_value"):
                official_url = cleaned.get("online_value")
            elif (
                ".house.gov" in cleaned.get("online_value")
                or ".senate.gov" in cleaned.get("online_value")
            ) and "@" in cleaned.get("online_value"):
                email_address = cleaned.get("online_value")
            else:
                official_url = None
                email_address = None
            if "twitter" in cleaned.get("online_value"):
                twitter_url = cleaned.get("online_value")
            else:
                twitter_url = None
            if "facebook" in cleaned.get("online_value"):
                facebook_url = cleaned.get("online_value")
            else:
                facebook_url = None
            if "youtube" in cleaned.get("online_value"):
                youtube_url = cleaned.get("online_value")
            else:
                youtube_url = None
            if "instagram" in cleaned.get("online_value"):
                instagram_url = cleaned.get("online_value")
            else:
                instagram_url = None
        else:
            official_url = None
            email_address = None
            twitter_url = None
            facebook_url = None
            youtube_url = None
            instagram_url = None
        # Map fields from the flat joined row to the members table
        filtered_cleaned = {
            "granule_id": str(
                cleaned.get("granuleid") or "ID_ERROR"
            ),
            "package_id": str(
                cleaned.get("packageid") or "ID_ERROR"
            ),
            "bioguide_id": str(
                cleaned.get("bioguideid") or None
            ),
            "membername": str(cleaned.get("membername") or None),
            "title": str(cleaned.get("title") or None),
            "biography": self.clean_long_text(cleaned.get("biography")),
            "member_type": str(cleaned.get("category") or None),
            "chamber": self.standardize_chamber(cleaned.get("chamber") or None),
            "population": self.safe_int(cleaned.get("population") or None),
            "gpo_id": str(cleaned.get("gpoid") or None),
            "authority_id": str(
                cleaned.get("authorityid") or None
            ),
            "email_address": str(email_address or None),
            "official_url": str(official_url or None),
            "twitter_url": str(twitter_url or None),
            "instagram_url": str(instagram_url or None),
            "facebook_url": str(facebook_url or None),
            "youtube_url": str(youtube_url or None),
            "last_modified": self.standardize_date(cleaned.get("lastmodified", None)),
        }

        return filtered_cleaned

    # Add post-processing methods
    async def _post_process_congressionaldirectories(self) -> dict[str, Any]:
        """
        Post-processing for congressional directories:
        - Extract ISBNs from the staging table's otheridentifiers_isbn column (JSON array)
        - Insert each ISBN into the production congressional_directories_isbn table
        """
        import json

        if not self.db_pool:
            raise ValueError("Database pool not configured")

        results = {
            "status": "success",
            "operations": [],
            "rows_affected": 0,
        }

        async with self.db_pool.acquire() as conn:
            # Operation 1: Extract and insert ISBNs
            try:
                async with conn.transaction():
                    # Fetch package_id and otheridentifiers_isbn from staging
                    fetch_sql = f"""
                        SELECT packageid, otheridentifiers_isbn
                        FROM {self.staging_schema}.congressionaldirectories
                        WHERE otheridentifiers_isbn IS NOT NULL
                    """
                    rows = await conn.fetch(fetch_sql)
                    isbn_records = []
                    for row in rows:
                        package_id = row["packageid"]
                        isbns_json = row["otheridentifiers_isbn"]
                        if not isbns_json:
                            continue
                        try:
                            isbns = json.loads(isbns_json)
                            if isinstance(isbns, str):
                                # Sometimes a single string, not a list
                                isbns = [isbns]
                            elif not isinstance(isbns, list):
                                continue
                        except Exception as e:
                            logger.error(f"Error parsing ISBN JSON for package_id {package_id}: {e}")
                            continue
                        for isbn in isbns:
                            if not isbn or not str(isbn).strip() and isbn != "\\u00a0":
                                continue
                            isbn_records.append((package_id, str(isbn).strip()))

                    # Insert into production table, avoiding duplicates
                    if isbn_records:
                        insert_sql = f"""
                            INSERT INTO {self.production_schema}.congressional_directories_isbn (package_id, isbn)
                            VALUES ($1, $2)
                            ON CONFLICT DO NOTHING
                        """
                        rows_inserted = 0
                        for rec in isbn_records:
                            try:
                                await conn.execute(insert_sql, rec[0], rec[1])
                                rows_inserted += 1
                            except Exception as e:
                                logger.error(f"Error inserting ISBN {rec[1]} for package_id {rec[0]}: {e}")
                        results["operations"].append(
                            {
                                "name": "extract_and_insert_isbns",
                                "status": "success",
                                "rows_affected": rows_inserted,
                            }
                        )
                        results["rows_affected"] += rows_inserted
                        logger.info(f"Inserted {rows_inserted} congressional directories ISBN records")
                    else:
                        results["operations"].append(
                            {
                                "name": "extract_and_insert_isbns",
                                "status": "success",
                                "rows_affected": 0,
                                "note": "No ISBNs found to insert"
                            }
                        )
            except Exception as e:
                logger.error(f"Error extracting/inserting congressional directories ISBNs: {e}")
                results["operations"].append(
                    {
                        "name": "extract_and_insert_isbns",
                        "status": "error",
                        "error": str(e),
                    }
                )
                results["status"] = "partial_failure"

        return results

    async def _post_process_congressionaldirectories_granules(self) -> dict[str, Any]:
        """
        Post-processing for congressional directories granules:
        - Extract zipcodes from the staging table's zipcodes column (space-separated string)
        - Insert each zipcode into the production members_zipcodes table
          with package_id, granule_id, bioguide_id, and the zipcode
        """
        results = {
            "status": "success",
            "operations": [],
            "rows_affected": 0,
        }

        if not self.db_pool:
            raise ValueError("Database pool not configured")

        async with self.db_pool.acquire() as conn:
            try:
                # Fetch all relevant records from the staging table
                fetch_sql = f"""
                    SELECT
                        g.packageid AS package_id,
                        g.id AS granule_id,
                        gm.bioguideid AS bioguide_id,
                        gm.zipcodes AS zipcodes
                    FROM {self.staging_schema}.congressionaldirectories_granules g
                    LEFT JOIN {self.staging_schema}.congressionaldirectories_granules_members gm
                        ON g.id = gm.granule_id
                    WHERE gm.zipcodes IS NOT NULL AND TRIM(gm.zipcodes) <> ''
                """
                rows = await conn.fetch(fetch_sql)

                zipcode_records = []
                for row in rows:
                    package_id = row["package_id"]
                    granule_id = row["granule_id"]
                    bioguide_id = row["bioguide_id"]
                    zipcodes_str = row["zipcodes"]
                    if not zipcodes_str:
                        continue
                    # Split by whitespace, filter out empty strings
                    zipcodes = [z for z in str(zipcodes_str).split() if z]
                    for zipcode in zipcodes:
                        zipcode_records.append((package_id, granule_id, bioguide_id, zipcode))

                rows_inserted = 0
                if zipcode_records:
                    insert_sql = f"""
                        INSERT INTO {self.production_schema}.members_zipcodes
                        (package_id, granule_id, bioguide_id, zipcode)
                        VALUES ($1, $2, $3, $4)
                        ON CONFLICT DO NOTHING
                    """
                    for rec in zipcode_records:
                        try:
                            await conn.execute(insert_sql, *rec)
                            rows_inserted += 1
                        except Exception as e:
                            logger.error(
                                f"Error inserting zipcode {rec[3]} for package_id {rec[0]}, granule_id {rec[1]}, bioguide_id {rec[2]}: {e}"
                            )
                    results["operations"].append(
                        {
                            "name": "extract_and_insert_zipcodes",
                            "status": "success",
                            "rows_affected": rows_inserted,
                        }
                    )
                    results["rows_affected"] += rows_inserted
                    logger.info(f"Inserted {rows_inserted} congressional directories member zipcode records")
                else:
                    results["operations"].append(
                        {
                            "name": "extract_and_insert_zipcodes",
                            "status": "success",
                            "rows_affected": 0,
                            "note": "No zipcodes found to insert"
                        }
                    )
            except Exception as e:
                logger.error(f"Error extracting/inserting congressional directories member zipcodes: {e}")
                results["operations"].append(
                    {
                        "name": "extract_and_insert_zipcodes",
                        "status": "error",
                        "error": str(e),
                    }
                )
                results["status"] = "partial_failure"

        return results
