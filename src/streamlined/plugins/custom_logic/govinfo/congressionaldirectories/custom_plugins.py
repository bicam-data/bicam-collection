"""
Congressional Directories Custom Plugin Logic

This module contains all the custom logic for congressional directories data type including:
- Fetching logic (from fetcher.py)
- Cleaning logic (from cleaner.py)

"""

import logging
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


class CongressionaldirectoriesCleanerLogic(BaseCleanerLogic):
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
                "congressionaldirectories_granules_members_name",
            ]
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
                    SELECT DISTINCT
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
                    WHERE g.granuleclass = 'CONGRESSMEMBERSTATE'
                    AND g.subgranuleclass != 'STATEDELEGATION'
                    ORDER BY g.processed_at DESC, gm.list_index
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

    async def _clean_congressionaldirectories_singular(
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
            "package_id": cleaned.get("packageid", "ID_ERROR"),
            "title": cleaned.get("title", None),
            "congress": self.safe_int(cleaned.get("congress", None)),
            "issued_at": self.standardize_date(cleaned.get("dateissued", None)),
            "branch": cleaned.get("branch", None),
            "government_author1": cleaned.get("governmentauthor1", None),
            "government_author2": cleaned.get("governmentauthor2", None),
            "publisher": cleaned.get("publisher", None),
            "collection_code": cleaned.get("collectioncode", None),
            "ils_system_id": cleaned.get("otheridentifier_ils_system_id", None),
            "migrated_doc_id": cleaned.get("otheridentifier_migrated_doc_id", None),
            "su_doc_class_number": cleaned.get("sudocclassnumber", None),
            "text_url": cleaned.get("download_txtlink", None),
            "pdf_url": cleaned.get("download_pdflink", None),
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

        self._register_target_table_override("members")

        cleaned = record_data.copy()

        granule_ids_to_fix = {
            "CDIR-1997-06-04-AS-H": "F000010",
            "CDIR-1997-06-04-CO-S-2": "0000000",
            "CDIR-1997-06-04-GU-H": "U000014",
            "CDIR-1997-06-04-MA-H-8": "K000110",
            "CDIR-1997-06-04-PR-H": "R000417",
            "CDIR-1997-06-04-VI-H": "C000380",
            "CDIR-1997-06-04-WV-H-3": "R000011",
            "CDIR-1997-06-04-WV-S-2": "R000361",
            "CDIR-1999-06-15-AS-H": "F000010",
            "CDIR-1999-06-15-GU-H": "U000014",
            "CDIR-1999-06-15-PR-H": "R000417",
            "CDIR-1999-06-15-TN-S-1": "0000000",
            "CDIR-1999-06-15-VI-H": "C000380",
            "CDIR-1999-06-15-WV-H-3": "R000011",
            "CDIR-1999-06-15-WV-S-2": "R000361",
            "CDIR-2000-02-01-AS-H": "F000010",
            "CDIR-2000-02-01-GU-H": "U000014",
            "CDIR-2000-02-01-PR-H": "R000417",
            "CDIR-2000-02-01-TN-S-1": "0000000",
            "CDIR-2000-02-01-VI-H": "C000380",
            "CDIR-2000-02-01-WV-H-3": "R000011",
            "CDIR-2000-02-01-WV-S-2": "R000361",
            "CDIR-2000-10-01-AS-H": "F000010",
            "CDIR-2000-10-01-GU-H": "U000014",
            "CDIR-2000-10-01-PR-H": "R000417",
            "CDIR-2000-10-01-TN-S-1": "0000000",
            "CDIR-2000-10-01-VI-H": "C000380",
            "CDIR-2000-10-01-WV-H-3": "R000011",
            "CDIR-2000-10-01-WV-S-2": "R000361",
            "CDIR-2001-12-07-AS-H": "F000010",
            "CDIR-2001-12-07-GU-H": "U000014",
            "CDIR-2001-12-07-PR-H": "R000417",
            "CDIR-2001-12-07-VI-H": "C000380",
            "CDIR-2001-12-07-WV-H-3": "R000011",
            "CDIR-2001-12-07-WV-S-2": "R000361",
            "CDIR-2002-10-01-AS-H": "F000010",
            "CDIR-2002-10-01-GU-H": "U000014",
            "CDIR-2002-10-01-PR-H": "A000359",
            "CDIR-2002-10-01-VI-H": "C000380",
            "CDIR-2002-10-01-WV-H-3": "R000011",
            "CDIR-2002-10-01-WV-S-2": "R000361",
            "CDIR-2003-07-11-AS-H": "F000010",
            "CDIR-2003-07-11-GU-H": "B001245",
            "CDIR-2003-07-11-PR-H": "A000359",
            "CDIR-2003-07-11-VI-H": "C000380",
            "CDIR-2003-07-11-WV-H-3": "R000011",
            "CDIR-2003-07-11-WV-S-2": "R000361",
            "CDIR-2003-11-01-AS-H": "F000010",
            "CDIR-2003-11-01-GU-H": "B001245",
            "CDIR-2003-11-01-PR-H": "A000359",
            "CDIR-2003-11-01-VI-H": "C000380",
            "CDIR-2003-11-01-WV-H-3": "R000011",
            "CDIR-2003-11-01-WV-S-2": "R000361",
            "CDIR-2004-01-01-AS-H": "F000010",
            "CDIR-2004-01-01-GU-H": "B001245",
            "CDIR-2004-01-01-PR-H": "A000359",
            "CDIR-2004-01-01-VI-H": "C000380",
            "CDIR-2004-01-01-WV-H-3": "R000011",
            "CDIR-2004-01-01-WV-S-2": "R000361",
            "CDIR-2004-08-01-AS-H": "F000010",
            "CDIR-2004-08-01-GU-H": "B001245",
            "CDIR-2004-08-01-PR-H": "A000359",
            "CDIR-2004-08-01-VI-H": "C000380",
            "CDIR-2004-08-01-WV-H-3": "R000011",
            "CDIR-2004-08-01-WV-S-2": "R000361",
            "CDIR-2005-07-11-AS-H": "F000010",
            "CDIR-2005-07-11-GU-H": "B001245",
            "CDIR-2005-07-11-MO-H-5": "C001061",
            "CDIR-2005-07-11-OH-H-2": "0000000",
            "CDIR-2005-07-11-PR-H": "F000452",
            "CDIR-2005-07-11-VI-H": "C000380",
            "CDIR-2005-07-11-WV-H-3": "R000011",
            "CDIR-2005-07-11-WV-S-2": "R000361",
            "CDIR-2006-09-01-AS-H": "F000010",
            "CDIR-2006-09-01-GU-H": "B001245",
            "CDIR-2006-09-01-MO-H-5": "C001061",
            "CDIR-2006-09-01-OH-H-2": "0000000",
            "CDIR-2006-09-01-PR-H": "F000452",
            "CDIR-2006-09-01-VI-H": "C000380",
            "CDIR-2006-09-01-WV-H-3": "R000011",
            "CDIR-2006-09-01-WV-S-2": "R000361",
            "CDIR-2007-08-09-AL-H-5": "C000868",
            "CDIR-2007-08-09-AS-H": "F000010",
            "CDIR-2007-08-09-CA-H-25": "M000508",
            "CDIR-2007-08-09-GA-H-4": "J000288",
            "CDIR-2007-08-09-GU-H": "B001245",
            "CDIR-2007-08-09-IL-H-17": "H001040",
            "CDIR-2007-08-09-MO-H-5": "C001061",
            "CDIR-2007-08-09-MO-S-1": "B000611",
            "CDIR-2007-08-09-MS-H-3": "P000323",
            "CDIR-2007-08-09-NY-H-29": "K000364",
            "CDIR-2007-08-09-OH-H-4": "J000289",
            "CDIR-2007-08-09-PR-H": "F000452",
            "CDIR-2007-08-09-TN-H-9": "C001068",
            "CDIR-2007-08-09-VA-H-3": "S000185",
            "CDIR-2007-08-09-VI-H": "C000380",
            "CDIR-2007-08-09-WV-H-3": "R000011",
            "CDIR-2007-08-09-WV-S-2": "R000361",
            "CDIR-2008-08-01-AL-H-5": "C000868",
            "CDIR-2008-08-01-AS-H": "F000010",
            "CDIR-2008-08-01-CA-H-25": "M000508",
            "CDIR-2008-08-01-GA-H-4": "J000288",
            "CDIR-2008-08-01-GU-H": "B001245",
            "CDIR-2008-08-01-IL-H-17": "H001040",
            "CDIR-2008-08-01-MO-H-5": "C001061",
            "CDIR-2008-08-01-MO-S-1": "B000611",
            "CDIR-2008-08-01-MS-H-3": "P000323",
            "CDIR-2008-08-01-NY-H-29": "K000364",
            "CDIR-2008-08-01-OH-H-4": "J000289",
            "CDIR-2008-08-01-PR-H": "F000452",
            "CDIR-2008-08-01-TN-H-9": "C001068",
            "CDIR-2008-08-01-VA-H-3": "S000185",
            "CDIR-2008-08-01-VI-H": "C000380",
            "CDIR-2008-08-01-WV-H-3": "R000011",
            "CDIR-2008-08-01-WV-S-2": "R000361",
            "CDIR-2011-12-01-AS-H": "F000010",
            "CDIR-2011-12-01-CA-H-25": "M000508",
            "CDIR-2011-12-01-FL-H-2": "S001186",
            "CDIR-2011-12-01-GA-H-4": "J000288",
            "CDIR-2011-12-01-GU-H": "B001245",
            "CDIR-2011-12-01-MO-H-5": "C001061",
            "CDIR-2011-12-01-MP-H": "S001177",
            "CDIR-2011-12-01-NJ-H-10": "P000149",
            "CDIR-2011-12-01-NY-H-29": "R000585",
            "CDIR-2011-12-01-OH-H-4": "J000289",
            "CDIR-2011-12-01-OH-H-5": "L000566",
            "CDIR-2011-12-01-OH-H-6": "J000292",
            "CDIR-2011-12-01-OR-H-1": "0000000",
            "CDIR-2011-12-01-PR-H": "P000596",
            "CDIR-2011-12-01-TN-H-9": "C001068",
            "CDIR-2011-12-01-TX-H-17": "F000461",
            "CDIR-2011-12-01-VA-H-3": "S000185",
            "CDIR-2011-12-01-VI-H": "C000380",
            "CDIR-2011-12-01-WV-H-3": "R000011",
            "CDIR-2011-12-01-WV-S-1": "R000361",
            "CDIR-2014-02-18-AS-H": "F000010",
            "CDIR-2014-02-18-CA-H-25": "M000508",
            "CDIR-2014-02-18-FL-H-2": "S001186",
            "CDIR-2014-02-18-FL-H-26": "G000573",
            "CDIR-2014-02-18-GA-H-4": "J000288",
            "CDIR-2014-02-18-GU-H": "B001245",
            "CDIR-2014-02-18-MO-H-5": "C001061",
            "CDIR-2014-02-18-MP-H": "S001177",
            "CDIR-2014-02-18-NJ-H-1": "0000000",
            "CDIR-2014-02-18-NY-H-23": "R000585",
            "CDIR-2014-02-18-OH-H-4": "J000289",
            "CDIR-2014-02-18-OH-H-5": "L000566",
            "CDIR-2014-02-18-OH-H-6": "J000292",
            "CDIR-2014-02-18-PR-H": "P000596",
            "CDIR-2014-02-18-TN-H-9": "C001068",
            "CDIR-2014-02-18-TX-H-17": "F000461",
            "CDIR-2014-02-18-VA-H-3": "S000185",
            "CDIR-2014-02-18-VI-H": "C000380",
            "CDIR-2014-02-18-WV-H-3": "R000011",
            "CDIR-2014-02-18-WV-S-1": "R000361",
            "CDIR-2016-02-12-AS-H": "R000600",
            "CDIR-2016-02-12-CO-S-1": "B001267",
            "CDIR-2016-02-12-GA-H-1": "C001103",
            "CDIR-2016-02-12-GA-H-4": "J000288",
            "CDIR-2016-02-12-GU-H": "B001245",
            "CDIR-2016-02-12-MD-S-2": "C000141",
            "CDIR-2016-02-12-MO-H-5": "C001061",
            "CDIR-2016-02-12-MP-H": "S001177",
            "CDIR-2016-02-12-OH-H-4": "J000289",
            "CDIR-2016-02-12-OH-H-5": "L000566",
            "CDIR-2016-02-12-OH-H-6": "J000292",
            "CDIR-2016-02-12-PR-H": "P000596",
            "CDIR-2016-02-12-TN-H-9": "C001068",
            "CDIR-2016-02-12-TX-H-17": "F000461",
            "CDIR-2016-02-12-VA-H-3": "S000185",
            "CDIR-2016-02-12-VI-H": "P000610",
            "CDIR-2016-02-12-WI-H-4": "M001160",
            "CDIR-2018-07-27-CO-S-1": "B001267",
            "CDIR-2018-07-27-OK-H-1": "0000000",
            "CDIR-2018-10-29-OK-H-1": "0000000",
        }
        if cleaned.get("granuleid") in granule_ids_to_fix and cleaned.get("bioguideid") is None:
            logger.info(f"Fixing bioguideid for granuleid {cleaned.get('granuleid')}")
            cleaned["bioguideid"] = granule_ids_to_fix[cleaned.get("granuleid")]

        elif cleaned.get("bioguideid") is None:
            raise ValueError(f"No bioguideid found for granuleid {cleaned.get('granuleid')}")

        # Map fields from the flat joined row to the members table
        filtered_cleaned = {
            "granule_id": str(cleaned.get("granuleid") or "ID_ERROR"),
            "package_id": str(cleaned.get("packageid") or "ID_ERROR"),
            "bioguide_id": str(cleaned.get("bioguideid") or None),
            "membername": str(cleaned.get("membername") or None),
            "title": str(cleaned.get("title")),
            "biography": self.clean_long_text(cleaned.get("biography")),
            "member_type": str(cleaned.get("category") or None),
            "chamber": self.standardize_chamber(cleaned.get("chamber")),
            "population": self.safe_int(cleaned.get("population")),
            "gpo_id": cleaned.get("gpoid"),
            "authority_id": cleaned.get("authorityid"),
            "email_address": None,
            "official_url": None,
            "twitter_url": None,
            "instagram_url": None,
            "facebook_url": None,
            "youtube_url": None,
            "last_modified": self.standardize_date(cleaned.get("lastmodified", None)),
        }

        return filtered_cleaned

    # Add post-processing methods
    async def _post_process_congressionaldirectories(self) -> dict[str, Any]:
        """
        Post-processing for congressional directories:
        - Extract ISBNs from the staging table's otheridentifiers_isbn column (JSON array)
        - Insert each ISBN into the production congressional_directories_isbn table
        - Extract zipcodes from granules and insert into members_zipcodes table
        - Extract online values and update members table
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
                        SELECT packageid, otheridentifier_isbn
                        FROM {self.staging_schema}.congressionaldirectories
                        WHERE otheridentifier_isbn IS NOT NULL
                    """
                    rows = await conn.fetch(fetch_sql)
                    isbn_records = []
                    for row in rows:
                        package_id = row["packageid"]
                        isbns_json = row["otheridentifier_isbn"]
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
                            logger.error(
                                f"Error parsing ISBN JSON for package_id {package_id}: {e}"
                            )
                            continue
                        for isbn in isbns:
                            if (
                                not isbn
                                or not str(isbn).strip()
                                and " " not in isbn
                                and "u00a0" not in isbn
                            ):
                                continue
                            isbn_records.append((package_id, str(isbn).strip()))

                    # Insert into production table, avoiding duplicates
                    if isbn_records:
                        insert_sql = f"""
                            INSERT INTO {self.production_schema}.congressionaldirectories_isbn (package_id, isbn)
                            VALUES ($1, $2)
                            ON CONFLICT DO NOTHING
                        """
                        rows_inserted = 0
                        for rec in isbn_records:
                            try:
                                await conn.execute(insert_sql, rec[0], rec[1])
                                rows_inserted += 1
                            except Exception as e:
                                logger.error(
                                    f"Error inserting ISBN {rec[1]} for package_id {rec[0]}: {e}"
                                )
                        results["operations"].append(
                            {
                                "name": "extract_and_insert_isbns",
                                "status": "success",
                                "rows_affected": rows_inserted,
                            }
                        )
                        results["rows_affected"] += rows_inserted
                        logger.info(
                            f"Inserted {rows_inserted} congressional directories ISBN records"
                        )
                    else:
                        results["operations"].append(
                            {
                                "name": "extract_and_insert_isbns",
                                "status": "success",
                                "rows_affected": 0,
                                "note": "No ISBNs found to insert",
                            }
                        )
            except Exception as e:
                logger.error(
                    f"Error extracting/inserting congressional directories ISBNs: {e}"
                )
                results["operations"].append(
                    {
                        "name": "extract_and_insert_isbns",
                        "status": "error",
                        "error": str(e),
                    }
                )
                results["status"] = "partial_failure"

            # Operation 2: Extract and insert zipcodes
            try:
                # Fetch all relevant records from the staging table
                fetch_sql = f"""
                    SELECT
                        g.packageid AS package_id,
                        g.granuleid AS granule_id,
                        gm.bioguideid AS bioguide_id,
                        g.zipcodes AS zipcodes
                    FROM {self.staging_schema}.congressionaldirectories_granules g
                    LEFT JOIN {self.staging_schema}.congressionaldirectories_granules_members gm
                        ON g.id = gm.granule_id
                    WHERE g.zipcodes IS NOT NULL AND TRIM(g.zipcodes) <> ''
                """
                rows = await conn.fetch(fetch_sql)

                zipcode_records = []
                logger.info(f"Found {len(rows)} zipcode records to insert")
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
                        zipcode_records.append(
                            (package_id, granule_id, bioguide_id, zipcode)
                        )

                rows_inserted = 0
                if zipcode_records:
                    insert_sql = f"""
                        INSERT INTO {self.production_schema}.members_zipcodes
                        (package_id, granule_id, bioguide_id, zipcode)
                        VALUES ($1, $2, $3, $4)
                        ON CONFLICT DO NOTHING
                    """
                    try:
                        # Use executemany for efficient batch insert
                        await conn.executemany(insert_sql, zipcode_records)
                        rows_inserted = len(zipcode_records)
                    except Exception as e:
                        logger.error(
                            f"Error batch inserting zipcodes: {e}"
                        )
                        # Optionally, you could fall back to single inserts here if needed

                    results["operations"].append(
                        {
                            "name": "extract_and_insert_zipcodes",
                            "status": "success",
                            "rows_affected": rows_inserted,
                        }
                    )
                    results["rows_affected"] += rows_inserted
                    logger.info(
                        f"Inserted {rows_inserted} congressional directories member zipcode records"
                    )
                else:
                    results["operations"].append(
                        {
                            "name": "extract_and_insert_zipcodes",
                            "status": "success",
                            "rows_affected": 0,
                            "note": "No zipcodes found to insert",
                        }
                    )
            except Exception as e:
                logger.error(
                    f"Error extracting/inserting congressional directories member zipcodes: {e}"
                )
                results["operations"].append(
                    {
                        "name": "extract_and_insert_zipcodes",
                        "status": "error",
                        "error": str(e),
                    }
                )
                results["status"] = "partial_failure"

            # Operation 3: Extract and insert online values
            try:
                # Fetch granule_id and value from staging
                fetch_sql = f"""
                    SELECT granule_id, value
                    FROM {self.staging_schema}.congressionaldirectories_granules_online
                    WHERE value IS NOT NULL
                    ORDER BY list_index
                """
                rows = await conn.fetch(fetch_sql)

                # Group by granule_id to collect all online values for each member
                granule_online_values = {}
                for row in rows:
                    granule_id = row["granule_id"]
                    value = row["value"]
                    if not value or "writerep" in value:
                        continue

                    if granule_id not in granule_online_values:
                        granule_online_values[granule_id] = {
                            "email_address": None,
                            "official_url": None,
                            "twitter_url": None,
                            "facebook_url": None,
                            "instagram_url": None,
                            "youtube_url": None,
                            "other_url": None,
                        }

                    # Categorize the online value
                    if ".gov" in value and "@" not in value:
                        granule_online_values[granule_id]["official_url"] = value
                    elif "@mail" in value:
                        granule_online_values[granule_id]["email_address"] = value
                    elif "twitter" in value or "x.com" in value:
                        granule_online_values[granule_id]["twitter_url"] = value
                    elif "facebook" in value:
                        granule_online_values[granule_id]["facebook_url"] = value
                    elif "instagram" in value:
                        granule_online_values[granule_id]["instagram_url"] = value
                    elif "youtube" in value:
                        granule_online_values[granule_id]["youtube_url"] = value
                    else:
                        granule_online_values[granule_id]["other_url"] = value

                # Update members table with online values
                rows_updated = 0
                if granule_online_values:
                    update_sql = f"""
                        UPDATE {self.production_schema}.members AS m
                        SET email_address = $1, official_url = $2, twitter_url = $3,
                            facebook_url = $4, instagram_url = $5, youtube_url = $6,
                            other_url = $7
                        FROM {self.staging_schema}.congressionaldirectories_granules g
                        WHERE g.granuleid = m.granule_id
                        AND g.id = $8
                    """
                    for granule_id, online_values in granule_online_values.items():
                        try:
                            await conn.execute(
                                update_sql,
                                online_values["email_address"],
                                online_values["official_url"],
                                online_values["twitter_url"],
                                online_values["facebook_url"],
                                online_values["instagram_url"],
                                online_values["youtube_url"],
                                online_values["other_url"],
                                granule_id
                            )
                            rows_updated += 1
                        except Exception as e:
                            logger.error(
                                f"Error updating online values for granule_id {granule_id}: {e}"
                            )

                    results["operations"].append(
                        {
                            "name": "extract_and_insert_online_values",
                            "status": "success",
                            "rows_affected": rows_updated,
                        }
                    )
                    results["rows_affected"] += rows_updated
                    logger.info(
                        f"Updated {rows_updated} congressional directories member online values"
                    )
                else:
                    results["operations"].append(
                        {
                            "name": "extract_and_insert_online_values",
                            "status": "success",
                            "rows_affected": 0,
                            "note": "No online values found to update",
                        }
                    )
            except Exception as e:
                logger.error(
                    f"Error extracting/inserting congressional directories member online values: {e}"
                )
                results["operations"].append(
                    {
                        "name": "extract_and_insert_online_values",
                        "status": "error",
                        "error": str(e),
                    }
                )
                results["status"] = "partial_failure"

        return results
