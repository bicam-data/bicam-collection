"""
Treaty Docs Custom Plugin Logic

This module contains all the custom logic for treaty docs data type including:
- Fetching logic (from fetcher.py)
- Cleaning logic (from cleaner.py)

"""

import json
import logging
import re
from collections.abc import AsyncGenerator
from typing import Any

from ....base import BaseCleanerLogic

logger = logging.getLogger(__name__)


class TreatyDocsFetcher:
    """
    Treaty Docs fetcherogic that extends the base GovInfo fetcher logic.

    This class provides custom methods for fetching treaty docs data, including:
    - Extracting standardized treaty docs IDs
    - Handling related data from URLs
    - Implementing specialized fetching logic for treaty docs-specific data

    Inherits shared functionality like get_generic_related_data from
    GovInfoBaseFetcher.
    """

    def __init__(self, data_type: str = "treatydocs"):
        self.data_type = data_type


class TreatydocsCleanerLogic(BaseCleanerLogic):
    """
    Treaty Docs cleaner logic extracted from TreatyDocsCleaner class.
    Contains all the custom cleaning methods for treaty docs data.
    """

    def __init__(
        self,
        data_type_name: str = "treatydocs",
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
            "treatydocs_committees": [
                "treatydocs_committees",
                "treatydocs_granules_committees",
            ],
            "treatydocs": [
                "treatydocs",
                "treatydocs_granules",
            ],
            "treatydocs_serialset": ["treatydocs"],
            "treatydocs_serialset_topics": ["treatydocs_subjects_topics"],
        }

    async def _stream_treatydocs_joined_chunks(
        self, chunk_size: int
    ) -> AsyncGenerator[list[dict[str, Any]], None]:
        """
        Stream joined data from treatydocs and treatydocs_granules.

        - Emits package-only rows for packages without granules
        - Emits joined package+granule rows where granules exist (granule-first values via COALESCE)
        """

        if not self.db_pool:
            raise ValueError("Database pool not configured")

        async with self.db_pool.acquire() as conn:
            try:
                logger.info(
                    f"Starting treatydocs streaming with chunk_size={chunk_size}"
                )

                # First, check if both tables exist
                tables_exist_query = """
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_schema = $1
                AND table_name IN ('treatydocs', 'treatydocs_granules')
                """
                tables_count = await conn.fetchval(
                    tables_exist_query, self.staging_schema
                )

                if tables_count < 2:
                    logger.warning("One or both treatydocs staging tables do not exist")
                    return

                # Get total count for logging
                packages_without_granules_query = f"""
                SELECT COUNT(DISTINCT c.packageid)
                FROM {self.staging_schema}.treatydocs c
                WHERE NOT EXISTS (
                    SELECT 1 FROM {self.staging_schema}.treatydocs_granules g
                    WHERE g.packageid = c.packageid
                )
                """
                packages_without_granules_count = await conn.fetchval(
                    packages_without_granules_query
                )

                granule_count_query = f"""
                SELECT COUNT(DISTINCT g.granuleid)
                FROM {self.staging_schema}.treatydocs_granules g
                """
                granule_count = await conn.fetchval(granule_count_query)

                total_count = packages_without_granules_count + granule_count

                if total_count == 0:
                    logger.info("No treatydocs records found")
                    return

                logger.info(
                    f"Streaming {total_count} treatydocs records in chunks of {chunk_size}"
                )

                # Stream using pagination with aggregated formats
                offset = 0
                while True:
                    logger.debug(f"Fetching treatydocs chunk at offset {offset}")

                    query = f"""
                    SELECT * FROM (
                        -- Package-only rows (only for packages that don't have granules)
                        SELECT
                            c.packageid AS packageid,
                            NULL::text AS granuleid,
                            c.title AS title,
                            c.subtitle AS subtitle,
                            c.branch AS branch,
                            c.chamber AS chamber,
                            c.session AS session,
                            c.category AS category,
                            c.congress AS congress,
                            c.docclass AS docclass,
                            c.download_ziplink AS download_ziplink,
                            c.download_modslink AS download_modslink,
                            c.download_premislink AS download_premislink,
                            c.download_pdflink AS download_pdflink,
                            c.detailslink AS detailslink,
                            c.dateissued AS dateissued,
                            c.granuleslink AS granuleslink,
                            c.lastmodified AS lastmodified,
                            c.collectioncode AS collectioncode,
                            c.collectionname AS collectionname,
                            c.pages AS pages,
                            c.publisher AS publisher,
                            c.documenttype AS documenttype,
                            c.documentnumber AS documentnumber,
                            c.otheridentifier_migrated_doc_id AS otheridentifier_migrated_doc_id,
                            c.sudocclassnumber AS sudocclassnumber,
                            c.governmentauthor1 AS governmentauthor1,
                            c.governmentauthor2 AS governmentauthor2,
                            c.processed_at AS processed_at,
                            c.source_doc_id AS source_doc_id,
                            c.agency AS agency,
                            c.volume AS volume,
                            c.parentid AS parentid,
                            c.serialset_bagid AS serialset_bagid,
                            c.serialset_docid AS serialset_docid,
                            c.serialset_isglp AS serialset_isglp,
                            c.serialset_serialsetnumber AS serialset_serialsetnumber,
                            c.documentpart AS partnumber,
                            c.otheridentifier_lccn AS otheridentifier_lccn,
                            c.otheridentifier_oclc AS otheridentifier_oclc,
                            c.otheridentifier_ils_system_id AS otheridentifier_ils_system_id,
                            c.president_id AS president_id,
                            c.president_party AS president_party,
                            c.dateissuednotspecified AS dateissuednotspecified,
                            c.otheridentifier_issn AS otheridentifier_issn,
                            NULL::text AS summary,
                            c.package_id AS package_id
                        FROM {self.staging_schema}.treatydocs c
                        WHERE NOT EXISTS (
                            SELECT 1 FROM {self.staging_schema}.treatydocs_granules g
                            WHERE g.packageid = c.packageid
                        )

                        UNION ALL

                        -- Granule rows (for packages that have granules)
                        SELECT
                            c.packageid AS packageid,
                            g.granuleid AS granuleid,
                            COALESCE(g.title, c.title) AS title,
                            c.subtitle AS subtitle,
                            c.branch AS branch,
                            c.chamber AS chamber,
                            c.session AS session,
                            COALESCE(g.category, c.category) AS category,
                            c.congress AS congress,
                            COALESCE(g.docclass, c.docclass) AS docclass,
                            COALESCE(g.download_ziplink, c.download_ziplink) AS download_ziplink,
                            COALESCE(g.download_modslink, c.download_modslink) AS download_modslink,
                            COALESCE(g.download_premislink, c.download_premislink) AS download_premislink,
                            COALESCE(g.download_pdflink, c.download_pdflink) AS download_pdflink,
                            COALESCE(g.detailslink, c.detailslink) AS detailslink,
                            COALESCE(g.dateissued, c.dateissued) AS dateissued,
                            COALESCE(g.granuleslink, c.granuleslink) AS granuleslink,
                            COALESCE(g.lastmodified, c.lastmodified) AS lastmodified,
                            COALESCE(g.collectioncode, c.collectioncode) AS collectioncode,
                            COALESCE(g.collectionname, c.collectionname) AS collectionname,
                            c.pages AS pages,
                            c.publisher AS publisher,
                            c.documenttype AS documenttype,
                            c.documentnumber AS documentnumber,
                            c.otheridentifier_migrated_doc_id AS otheridentifier_migrated_doc_id,
                            c.sudocclassnumber AS sudocclassnumber,
                            c.governmentauthor1 AS governmentauthor1,
                            c.governmentauthor2 AS governmentauthor2,
                            COALESCE(g.processed_at, c.processed_at) AS processed_at,
                            COALESCE(g.source_doc_id, c.source_doc_id) AS source_doc_id,
                            c.agency AS agency,
                            c.volume AS volume,
                            c.parentid AS parentid,
                            c.serialset_bagid AS serialset_bagid,
                            c.serialset_docid AS serialset_docid,
                            c.serialset_isglp AS serialset_isglp,
                            c.serialset_serialsetnumber AS serialset_serialsetnumber,
                            g.partnumber AS partnumber,
                            c.otheridentifier_lccn AS otheridentifier_lccn,
                            c.otheridentifier_oclc AS otheridentifier_oclc,
                            c.otheridentifier_ils_system_id AS otheridentifier_ils_system_id,
                            COALESCE(g.president_id, c.president_id) AS president_id,
                            COALESCE(g.president_party, c.president_party) AS president_party,
                            c.dateissuednotspecified AS dateissuednotspecified,
                            c.otheridentifier_issn AS otheridentifier_issn,
                            g.summary AS summary,
                            c.package_id AS package_id
                        FROM {self.staging_schema}.treatydocs c
                        JOIN {self.staging_schema}.treatydocs_granules g
                            ON c.packageid = g.packageid
                        WHERE g.granuleid IS NOT NULL
                    ) q
                    ORDER BY q.packageid, q.granuleid NULLS FIRST
                    LIMIT {chunk_size} OFFSET {offset}
                    """

                    logger.debug(f"Executing query for treatydocs at offset {offset}")
                    rows = await conn.fetch(query)
                    logger.debug(
                        f"Retrieved {len(rows)} rows for treatydocs at offset {offset}"
                    )

                    if not rows:
                        logger.info(f"No more treatydocs records at offset {offset}")
                        break

                    chunk = []
                    for row in rows:
                        try:
                            row_dict = dict(row)
                            chunk.append(row_dict)
                        except Exception as e:
                            logger.error(
                                f"Error converting treatydocs row to dict: {e}, row type: {type(row)}, row: {row}"
                            )
                            continue

                    if chunk:  # Only yield if we have valid records
                        logger.debug(f"Yielding {len(chunk)} treatydocs records")
                        yield chunk

                    offset += chunk_size

                    # Log progress periodically
                    if offset % (chunk_size * 10) == 0:
                        logger.debug(f"Streamed {offset} treatydocs records")

            except Exception as e:
                logger.error(f"Error streaming treatydocs: {e}")
                raise

    async def _stream_treatydocs_committees_joined_chunks(
        self, chunk_size: int
    ) -> AsyncGenerator[list[dict[str, Any]], None]:
        """
        Stream joined data from treatydocs_committees and treatydocs_granules_committees.
        """

        if not self.db_pool:
            raise ValueError("Database pool not configured")

        async with self.db_pool.acquire() as conn:
            try:
                logger.info(
                    f"Starting treatydocs_committees streaming with chunk_size={chunk_size}"
                )

                # First, check if both tables exist
                tables_exist_query = """
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_schema = $1
                AND table_name IN ('treatydocs_committees', 'treatydocs_granules_committees')
                """
                tables_count = await conn.fetchval(
                    tables_exist_query, self.staging_schema
                )

                if tables_count < 2:
                    logger.warning(
                        "One or both treatydocs_committees staging tables do not exist"
                    )
                    return

                # Get total count for logging
                package_count_query = f"""
                SELECT DISTINCT ON (c.package_id, c.authorityid) COUNT(*)
                FROM {self.staging_schema}.treatydocs_committees AS c
                GROUP BY c.package_id, c.authorityid
                """
                package_count = await conn.fetchval(package_count_query)

                granule_count_query = f"""
                SELECT DISTINCT ON (gc.granule_id, gc.authorityid) COUNT(*)
                FROM {self.staging_schema}.treatydocs_granules_committees gc
                GROUP BY gc.granule_id, gc.authorityid
                """
                granule_count = await conn.fetchval(granule_count_query)

                total_count = package_count + granule_count

                if total_count == 0:
                    logger.info("No treatydocs_committees records found")
                    return

                logger.info(
                    f"Streaming {total_count} treatydocs_committees records in chunks of {chunk_size}"
                )

                # Stream using pagination with aggregated formats
                offset = 0
                while True:
                    logger.debug(
                        f"Fetching treatydocs_committees chunk at offset {offset}"
                    )

                    # Use JSON aggregation to collect all formats for each text
                    query = f"""
                    SELECT
                        tdc.package_id,
                        NULL as granule_id,
                        tdc.authorityid,
                        tdc.committeename
                    FROM {self.staging_schema}.treatydocs_committees AS tdc
                    GROUP BY tdc.package_id, tdc.authorityid, tdc.committeename

                    UNION ALL

                    SELECT
                        tdg.packageid as package_id,
                        tdg.granuleid AS granule_id,
                        tdgc.authorityid,
                        tdgc.committeename
                    FROM {self.staging_schema}.treatydocs_granules_committees AS tdgc
                    JOIN {self.staging_schema}.treatydocs_granules AS tdg ON tdgc.granule_id = tdg.id
                    GROUP BY tdg.packageid, tdg.granuleid, tdgc.authorityid, tdgc.committeename
                    ORDER BY granule_id, authorityid, committeename
                    LIMIT {chunk_size} OFFSET {offset}
                    """

                    logger.debug(
                        f"Executing query for treatydocs_committees at offset {offset}"
                    )
                    rows = await conn.fetch(query)
                    logger.debug(
                        f"Retrieved {len(rows)} rows for treatydocs_committees at offset {offset}"
                    )

                    if not rows:
                        logger.info(
                            f"No more treatydocs_committees records at offset {offset}"
                        )
                        break

                    chunk = []
                    for row in rows:
                        try:
                            row_dict = dict(row)
                            chunk.append(row_dict)
                        except Exception as e:
                            logger.error(
                                f"Error converting treatydocs_committees row to dict: {e}, row type: {type(row)}, row: {row}"
                            )
                            continue

                    if chunk:  # Only yield if we have valid records
                        logger.debug(
                            f"Yielding {len(chunk)} treatydocs_committees records"
                        )
                        yield chunk

                    offset += chunk_size

                    # Log progress periodically
                    if offset % (chunk_size * 10) == 0:
                        logger.debug(f"Streamed {offset} treatydocs_committees records")

            except Exception as e:
                logger.error(f"Error streaming treatydocs_committees: {e}")
                raise

    async def _build_treatydocs_identity(
        self, cleaned: dict[str, Any]
    ) -> tuple[
        str | None,  # treaty_id
        str | None,  # parent_treaty_id
        str | None,  # granule_id
        int | None,  # part_number
        str | None,  # treaty_type
        str | None,  # treaty_number
        str | None,  # congress
        bool,  # is_errata
    ]:
        """
        Build canonical identity for treaty docs records (packages or granules).
        """

        # default: use regex to deconstruct packageid/granuleid
        # Convert "CDOC-104tdoc13" to "td104-13"
        # Extract the congress/session number and the tdoc number
        # Example: "CDOC-104tdoc13" -> "td104-13"
        # Identify the raw id and errata flag
        if cleaned.get("granuleid"):
            raw_id = str(cleaned.get("granuleid", ""))
            is_errata = bool("-err" in raw_id.lower() or raw_id.lower().endswith("-e"))
        else:
            raw_id = str(cleaned.get("packageid", ""))
            is_errata = False

        # Defaults from fields; will be refined by regex when possible
        treaty_type: str | None = (cleaned.get("documenttype") or "").lower() or None
        treaty_number: str | None = cleaned.get("documentnumber") or None
        part_number: str | None = (
            cleaned.get("partnumber", cleaned.get("documentpart")) or 0
        )
        congress: str | None = cleaned.get("congress") or None

        # Attempt to parse canonical components from typical IDs like
        #   CDOC-104tdoc13 → congress=104, type=tdoc, number=13
        if "SERIALSET" not in raw_id.upper():
            m = re.search(
                r"^[A-Z]+-(\d+)(tdoc|hdoc|hmdoc|sdoc|sedoc)(\d+)", raw_id, re.IGNORECASE
            )
            if m:
                congress = m.group(1)
                treaty_type = m.group(2).lower()
                treaty_number = m.group(3)

        # Build a simple treaty_id that does not encode part/errata
        treaty_id: str | None = None
        treaty_set_id: str | None = None
        if treaty_type and treaty_number and congress:
            part_token = str(part_number or 0)
            part_token_with_errata = f"{part_token}{'e' if is_errata else ''}"
            treaty_id = (
                f"{treaty_type}{treaty_number}-{part_token_with_errata}-{congress}"
            )
            treaty_set_id = f"{treaty_type}{treaty_number}-{congress}"

        granule_id: str | None = str(
            cleaned.get("granuleid") or cleaned.get("granule_id") or None
        )

        return (
            treaty_id,
            treaty_set_id,
            granule_id,
            part_number,
            treaty_type,
            treaty_number,
            congress,
            is_errata,
        )

    async def _clean_treatydocs_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual treaty docs records.
        STAGING COLUMNS (from _stream_treatydocs_joined_chunks):
        - packageid                       text,
        - granuleid                       text,
        - title                           text,
        - subtitle                        text,
        - branch                          text,
        - chamber                         text,
        - session                         text,
        - category                        text,
        - congress                        text,
        - docclass                        text,
        - download_ziplink                text,
        - download_modslink               text,
        - download_premislink             text,
        - download_pdflink                text,
        - detailslink                     text,
        - dateissued                      text,
        - granuleslink                    text,
        - lastmodified                    text,
        - collectioncode                  text,
        - collectionname                  text,
        - pages                           text,
        - publisher                       text,
        - documenttype                    text,
        - documentnumber                  text,
        - otheridentifier_migrated_doc_id text,
        - sudocclassnumber                text,
        - governmentauthor1               text,
        - governmentauthor2               text,
        - processed_at                    text,
        - source_doc_id                   text,
        - agency                          text,
        - volume                          text,
        - parentid                        text,
        - serialset_bagid                 text,
        - serialset_docid                 text,
        - serialset_isglp                 text,
        - serialset_serialsetnumber       text,
        - partnumber                      text,
        - otheridentifier_lccn            text,
        - otheridentifier_oclc            text,
        - otheridentifier_ils_system_id   text,
        - president_id                    text,
        - president_party                 text,
        - dateissuednotspecified          text,
        - otheridentifier_issn            text,
        - summary                         text,
        - package_id                      text,

        FINAL COLUMNS:
        - package_id TEXT PRIMARY KEY,
        - treaty_id TEXT,
        - title TEXT,
        - congress INTEGER,
        - session INTEGER,
        - chamber TEXT, -- lower
        - summary TEXT,
        - pages INTEGER,
        - issued_at DATE,
        - branch TEXT,
        - government_author1 TEXT,
        - government_author2 TEXT,
        - publisher TEXT,
        - collection_code TEXT,
        - migrated_doc_id TEXT,
        - su_doc_class_number TEXT,
        - last_modified TIMESTAMP
        """
        cleaned = record_data.copy()

        (
            treaty_id,
            treaty_set_id,
            granule_id,
            part_number,
            treaty_type,
            treaty_number,
            congress,
            is_errata,
        ) = await self._build_treatydocs_identity(cleaned)

        # Apply bills-specific cleaning logic
        filtered_cleaned = {
            "package_id": str(cleaned.get("packageid", "ID_ERROR")),
            "treaty_id": str(treaty_id),
            "granule_id": granule_id,
            "treaty_set_id": str(treaty_set_id) if treaty_set_id is not None else None,
            "title": cleaned.get("title", None),
            "subtitle": cleaned.get("subtitle", None),
            "treaty_type": str(treaty_type),
            "treaty_number": self.safe_int(treaty_number),
            "part_number": self.safe_int(part_number),
            "congress": self.safe_int(cleaned.get("congress", congress)),
            "session": self.safe_int(cleaned.get("session", None)),
            "chamber": str(self.standardize_chamber(cleaned.get("chamber", None))),
            "summary": cleaned.get("summary", None),
            "pages": self.safe_int(cleaned.get("pages", None)),
            "is_errata": is_errata,
            "issued_at": self.standardize_date(cleaned.get("dateissued", None)),
            "branch": str(cleaned.get("branch", None)),
            "government_author1": str(cleaned.get("governmentauthor1", None)),
            "government_author2": str(cleaned.get("governmentauthor2", None)),
            "publisher": str(cleaned.get("publisher", None)),
            "collection_code": str(cleaned.get("collectioncode", None)),
            "migrated_doc_id": str(
                cleaned.get("otheridentifier_migrated_doc_id", None)
            ),
            "su_doc_class_number": str(cleaned.get("sudocclassnumber", None)),
            "last_modified": self.standardize_date(cleaned.get("lastmodified", None)),
        }

        return filtered_cleaned

    async def _clean_treatydocs_committees_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual treatydocs committees records.
        STAGING COLUMNS:
        - package_id    text,
        - granuleid    text,
        - authorityid   text,
        - committeename text,

        FINAL COLUMNS:
        - package_id TEXT,
        - granule_id TEXT,
        - committee_code TEXT,
        - committee_name TEXT,
        """
        cleaned = record_data.copy()

        filtered_cleaned = {
            "package_id": cleaned.get("package_id", "ID_ERROR"),
            "granule_id": cleaned.get("granule_id", "ID_ERROR"),
            "committee_code": cleaned.get("authorityid", "ID_ERROR"),
            "committee_name": cleaned.get("committeename", None),
        }

        return filtered_cleaned

    async def _clean_treatydocs_serialset_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual treatydocs serialset records.
        STAGING COLUMNS:
        - packageid                       text,
        - granuleid                       text,
        - title                           text,
        - subtitle                        text,
        - branch                          text,
        - chamber                         text,
        - session                         text,
        - category                        text,
        - congress                        text,
        - docclass                        text,
        - download_ziplink                text,
        - download_modslink               text,
        - download_premislink             text,
        - download_pdflink                text,
        - detailslink                     text,
        - dateissued                      text,
        - granuleslink                    text,
        - lastmodified                    text,
        - collectioncode                  text,
        - collectionname                  text,
        - pages                           text,
        - publisher                       text,
        - documenttype                    text,
        - documentnumber                  text,
        - otheridentifier_migrated_doc_id text,
        - sudocclassnumber                text,
        - governmentauthor1               text,
        - governmentauthor2               text,
        - processed_at                    text,
        - source_doc_id                   text,
        - agency                          text,
        - volume                          text,
        - parentid                        text,
        - serialset_bagid                 text,
        - serialset_docid                 text,
        - serialset_isglp                 text,
        - serialset_serialsetnumber       text,
        - partnumber                      text,
        - otheridentifier_lccn            text,
        - otheridentifier_oclc            text,
        - otheridentifier_ils_system_id   text,
        - president_id                    text,
        - president_party                 text,
        - dateissuednotspecified          text,
        - otheridentifier_issn            text,
        - summary                         text,
        - package_id                      text,

        FINAL COLUMNS:
        - package_id TEXT PRIMARY KEY,
        - bag_id TEXT,
        - doc_id TEXT,
        - serialset_number TEXT,
        - agency TEXT,
        - volume TEXT,
        - parent_serialset_id TEXT,
        - oclc_number TEXT,
        - lccn_number TEXT,
        - issn_number TEXT,
        - isglp BOOLEAN,
        - last_modified TIMESTAMP WITH TIME ZONE
        """
        cleaned = record_data.copy()

        pkg = str(cleaned.get("packageid") or cleaned.get("package_id") or "")
        if "SERIALSET" not in pkg.upper():
            return None

        filtered_cleaned = {
            "package_id": cleaned.get("package_id", "ID_ERROR"),
            "bag_id": cleaned.get("serialset_bagid", None),
            "doc_id": cleaned.get("serialset_docid", None),
            "serialset_number": cleaned.get("serialset_serialsetnumber", None),
            "agency": cleaned.get("agency", None),
            "volume": cleaned.get("volume", None),
            "parent_serialset_id": cleaned.get("parentid", None),
            "oclc_number": cleaned.get("otheridentifier_oclc", None),
            "lccn_number": cleaned.get("otheridentifier_lccn", None),
            "issn_number": cleaned.get("otheridentifier_issn", None),
            "isglp": bool(cleaned.get("serialset_isglp", None)),
            "last_modified": self.standardize_date(cleaned.get("lastmodified", None)),
        }

        return filtered_cleaned

    async def _clean_treatydocs_serialset_topics_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual treatydocs serialset topics records.
        STAGING COLUMNS:
        - package_id    text,
        - value         text,

        FINAL COLUMNS:
        - package_id TEXT,
        - topic TEXT,
        """
        cleaned = record_data.copy()

        filtered_cleaned = {
            "package_id": cleaned.get("package_id", "ID_ERROR"),
            "topic": cleaned.get("value", None),
        }

        return filtered_cleaned

    # Add post-processing methods
    async def _post_process_treatydocs(self) -> dict[str, Any]:
        """Post-processing operations specific to treaty docs data."""
        if not self.db_pool:
            raise ValueError("Database pool not configured")

        results = {
            "status": "success",
            "operations": [],
            "rows_affected": 0,
        }

        # ? Operation 1: ils system id
        logger.info("Starting ils system id post-processing")
        try:
            async with self.db_pool.acquire() as conn, conn.transaction():
                # Fetch all records from staging
                fetch_sql = f"""
                    SELECT package_id, otheridentifier_ils_system_id
                    FROM {self.staging_schema}.treatydocs
                """
                rows = await conn.fetch(fetch_sql)
                logger.info(f"Found {len(rows)} records with ils_system_id data")

                # Prepare batch params
                insert_sql = f"""
                    INSERT INTO {self.production_schema}.treatydocs_ils_system_id (package_id, ils_system_id)
                    VALUES ($1, $2)
                    ON CONFLICT DO NOTHING
                """
                batch_params = []
                for row in rows:
                    ils_system_id_raw = row["otheridentifier_ils_system_id"]
                    if not ils_system_id_raw:
                        continue
                    try:
                        if isinstance(ils_system_id_raw, str):
                            ils_ids = json.loads(ils_system_id_raw)
                        elif isinstance(ils_system_id_raw, list):
                            ils_ids = ils_system_id_raw
                        elif isinstance(ils_system_id_raw, int | float):
                            # Handle case where it's a single integer/float
                            ils_ids = [str(ils_system_id_raw)]
                        else:
                            ils_ids = [str(ils_system_id_raw)]
                    except json.JSONDecodeError:
                        ils_ids = [str(ils_system_id_raw)]

                    # Ensure ils_ids is always a list
                    if not isinstance(ils_ids, list):
                        ils_ids = [str(ils_ids)]

                    for ils_id in ils_ids:
                        if ils_id:
                            batch_params.append((row["package_id"], str(ils_id)))

                total_inserted = 0
                if batch_params:
                    chunk_size = 1000
                    for i in range(0, len(batch_params), chunk_size):
                        chunk = batch_params[i : i + chunk_size]
                        await conn.executemany(insert_sql, chunk)
                        total_inserted += len(chunk)

                logger.info(f"Inserted {total_inserted} ils_system_id records")
                results["operations"].append(
                    {
                        "name": "populate_ils_system_id_field",
                        "status": "success",
                        "rows_affected": total_inserted,
                    }
                )
                results["rows_affected"] += total_inserted

        except Exception as e:
            logger.error(f"Error populating ils_system_id field: {e}", exc_info=True)
            results["operations"].append(
                {
                    "name": "populate_ils_system_id_field",
                    "status": "error",
                    "error": str(e),
                }
            )
            results["status"] = "partial_failure"
