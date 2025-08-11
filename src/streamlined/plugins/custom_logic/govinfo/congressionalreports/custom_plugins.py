"""
Congressional Reports Custom Plugin Logic

This module contains all the custom logic for congressional reports data type including:
- Fetching logic (from fetcher.py)
- Cleaning logic (from cleaner.py)

"""

# TODO TABLES:
# serialset data - congressionalreports
# serialset topics - congressionalreports
# submittedby members - congressionalreports
# references bills - congressionalreports_granules
# references other - congressionalreports
# ils_system_id table - congressionalreports
# committees - congressionalreports_committees

import hashlib
import json
import logging
from collections.abc import AsyncGenerator
from typing import Any

from streamlined.plugins.base import BaseCleanerLogic

logger = logging.getLogger(__name__)


class CongressionalReportsFetcher:
    """
    Congressional Reports fetcherogic that extends the base GovInfo fetcher logic.

    This class provides custom methods for fetching congressional reports data, including:
    - Extracting standardized congressional reports IDs
    - Handling related data from URLs
    - Implementing specialized fetching logic for congressional reports-specific data

    Inherits shared functionality like get_generic_related_data from
    GovInfoBaseFetcher.
    """

    def __init__(self, data_type: str = "congressionalreports"):
        self.data_type = data_type


class CongressionalreportsCleanerLogic(BaseCleanerLogic):
    """
    Congressional Reports cleaner logic extracted from CongressionalReportsCleaner class.
    Contains all the custom cleaning methods for congressional reports data.
    """

    def __init__(
        self,
        data_type_name: str = "congressionalreports",
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
            "congressionalreports_committees": [
                "congressionalreports_committees",
                "congressionalreports_granules_committees",
            ],
            "congressionalreports_bills": [
                "congressionalreports_granules_references_contents"
            ],
            "congressionalreports_members": [
                "congressionalreports_granules_members",
                "congressionalreports_granules_members_name",
            ],
            "congressionalreports_serialset": ["congressionalreports"],
        }

    async def _stream_congressionalreports_committees_joined_chunks(
        self, chunk_size: int
    ) -> AsyncGenerator[list[dict[str, Any]], None]:
        """
        Stream joined data from congressionalreports_committees and congressionalreports_granules_committees.
        """
        # TODO: between congressionalreports_committees and congressionalreports_granules_committees

        if not self.db_pool:
            raise ValueError("Database pool not configured")

        async with self.db_pool.acquire() as conn:
            try:
                # First, check if both tables exist
                tables_exist_query = """
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_schema = $1
                AND table_name IN ('congressionalreports_committees', 'congressionalreports_granules_committees')
                """
                tables_count = await conn.fetchval(
                    tables_exist_query, self.staging_schema
                )

                if tables_count < 2:
                    logger.warning(
                        "One or both congressionalreports_committees staging tables do not exist"
                    )
                    return

                # Get total count for logging
                package_count_query = f"""
                SELECT COUNT(DISTINCT c.package_id, c.authorityid)
                FROM {self.staging_schema}.congressionalreports_committees c
                """
                package_count = await conn.fetchval(package_count_query)

                granule_count_query = f"""
                SELECT COUNT(DISTINCT c.granule_id, c.authorityid)
                FROM {self.staging_schema}.congressionalreports_granules_committees gc
                """
                granule_count = await conn.fetchval(granule_count_query)

                total_count = package_count + granule_count

                if total_count == 0:
                    logger.info("No congressionalreports_committees records found")
                    return

                logger.info(
                    f"Streaming {total_count} congressionalreports_committees records in chunks of {chunk_size}"
                )

                # Stream using pagination with aggregated formats
                offset = 0
                while True:
                    # Use JSON aggregation to collect all formats for each text
                    query = f"""
                    SELECT
                        package_id,
                        NULL as granule_id,
                        authorityid,
                        committeename
                    FROM {self.staging_schema}.congressionalreports_committees
                    GROUP BY package_id, authorityid, committeename

                    UNION ALL

                    SELECT
                        NULL as package_id,
                        granule_id,
                        authorityid,
                        committeename
                    FROM {self.staging_schema}.congressionalreports_granules_committees
                    GROUP BY granule_id, authorityid, committeename

                    ORDER BY package_id, granule_id, authorityid, committeename
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
                                f"Error converting congressionalreports_committees row to dict: {e}, row type: {type(row)}, row: {row}"
                            )
                            continue

                    if chunk:  # Only yield if we have valid records
                        yield chunk

                    offset += chunk_size

                    # Log progress periodically
                    if offset % (chunk_size * 10) == 0:
                        logger.debug(
                            f"Streamed {offset} congressionalreports_committees records"
                        )

            except Exception as e:
                logger.error(f"Error streaming congressionalreports_committees: {e}")
                raise

    async def _stream_congressionalreports_members_joined_chunks(
        self, chunk_size: int
    ) -> AsyncGenerator[list[dict[str, Any]], None]:
        """
        Stream joined data from congressionalreports_members and congressionalreports_granules_members.
        """
        # TODO: between congressionalreports_members and congressionalreports_granules_members

        if not self.db_pool:
            raise ValueError("Database pool not configured")

        async with self.db_pool.acquire() as conn:
            try:
                # First, check if both tables exist
                tables_exist_query = """
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_schema = $1
                AND table_name IN ('congressionalreports_granules_members', 'congressionalreports_granules_members_name')
                """
                tables_count = await conn.fetchval(
                    tables_exist_query, self.staging_schema
                )

                if tables_count < 2:
                    logger.warning(
                        "One or both congressionalreports_members staging tables do not exist"
                    )
                    return

                # Get total count for logging
                package_count_query = f"""
                SELECT COUNT(DISTINCT c.package_id)
                FROM {self.staging_schema}.congressionalreports_members c
                """
                package_count = await conn.fetchval(package_count_query)

                granule_count_query = f"""
                SELECT COUNT(DISTINCT c.granule_id, c.authorityid)
                FROM {self.staging_schema}.congressionalreports_granules_committees gc
                """
                granule_count = await conn.fetchval(granule_count_query)

                total_count = package_count + granule_count

                if total_count == 0:
                    logger.info("No congressionalreports_committees records found")
                    return

                logger.info(
                    f"Streaming {total_count} congressionalreports_committees records in chunks of {chunk_size}"
                )

                # Stream using pagination with aggregated formats
                offset = 0
                while True:
                    # Use JSON aggregation to collect all formats for each text
                    query = f"""
                    SELECT
                        package_id,
                        NULL as granule_id,
                        authorityid,
                        committeename
                    FROM {self.staging_schema}.congressionalreports_committees
                    GROUP BY package_id, authorityid, committeename

                    UNION ALL

                    SELECT
                        NULL as package_id,
                        granule_id,
                        authorityid,
                        committeename
                    FROM {self.staging_schema}.congressionalreports_granules_committees
                    GROUP BY granule_id, authorityid, committeename

                    ORDER BY package_id, granule_id, authorityid, committeename
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
                                f"Error converting congressionalreports_committees row to dict: {e}, row type: {type(row)}, row: {row}"
                            )
                            continue

                    if chunk:  # Only yield if we have valid records
                        yield chunk

                    offset += chunk_size

                    # Log progress periodically
                    if offset % (chunk_size * 10) == 0:
                        logger.debug(
                            f"Streamed {offset} congressionalreports_committees records"
                        )

            except Exception as e:
                logger.error(f"Error streaming congressionalreports_committees: {e}")
                raise


    async def _clean_congressionalreports_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual congressional reports records.
        STAGING COLUMNS:
        - pages                              text,
        - title                              text,
        - branch                             text,
        - chamber                            text,
        - session                            text,
        - category                           text,
        - congress                           text,
        - docclass                           text,
        - download_ziplink                   text,
        - download_modslink                  text,
        - download_premislink                text,
        - subtitle                           text,
        - packageid                          text,
        - publisher                          text,
        - dateissued                         text,
        - detailslink                        text,
        - documenttype                       text,
        - granuleslink                       text,
        - lastmodified                       text,
        - collectioncode                     text,
        - collectionname                     text,
        - documentnumber                     text,
        - otheridentifier_migrated_doc_id    text,
        - sudocclassnumber                   text,
        - governmentauthor1                  text,
        - governmentauthor2                  text,
        - package_id                         text,
        - processed_at                       text,
        - source_doc_id                      text,
        - otheridentifier_ils_system_id      text,
        - otheridentifier_sudoc_item_number  text,
        - download_pdflink                   text,
        - otheridentifier_sudoc_class_number text,
        - agency                             text,
        - volume                             text,
        - parentid                           text,
        - subjects_topics                    text,
        - serialset_bagid                    text,
        - serialset_docid                    text,
        - serialset_isglp                    text,
        - serialset_serialsetnumber          text,
        - otheridentifier_oclc               text,
        - committees                         text,
        - dateissuednotspecified             text,
        - otheridentifier_lccn               text,
        - download_txtlink                   text,
        - download_jpeglink                  text,
        - federalpublicationname             text,
        - download_thumbnailjpeg             text,
        - references                        text,
        - president_id                       text,
        - president_names                    text,
        - president_party                    text,
        - documentpart                       text,
        - otheridentifier_issn               text

        FINAL COLUMNS:
        - package_id TEXT PRIMARY KEY,
        - report_id TEXT,
        - title TEXT,
        - subtitle TEXT,
        - chamber TEXT, -- lower
        - congress INTEGER,
        - session INTEGER,
        - pages INTEGER,
        - issued_at DATE,
        - branch TEXT,
        - government_author1 TEXT,
        - government_author2 TEXT,
        - publisher TEXT,
        - collection_code TEXT,
        - migrated_doc_id TEXT,
        - su_doc_class_number TEXT,
        - su_doc_item_number TEXT,
        - last_modified TIMESTAMP WITH TIME ZONE
        """
        cleaned = record_data.copy()

        # if J6 in packageid, return None
        if "J6" in cleaned.get("packageid", ""):
            report_type = "hrpt"
            report_number = "663"
            congress = "117"
        else:
            report_type = cleaned.get("documenttype", None)
            report_number = cleaned.get("documentnumber", None)
            congress = cleaned.get("congress", None)

        if all([report_type, report_number, congress]):
            report_id = f"{report_type.lower()}{report_number}-1-{congress}"
        else:
            raise ValueError(
                f"Invalid report_id format: report_id={report_id}, congress={congress}. Record is {cleaned}"
            )

        # Apply bills-specific cleaning logic
        filtered_cleaned = {
            "package_id": str(cleaned.get("packageid", "ID_ERROR")),
            "report_id": str(report_id),
            "title": cleaned.get("title", None),
            "subtitle": cleaned.get("subtitle", None),
            "chamber": self.standardize_chamber(cleaned.get("chamber", None)),
            "congress": self.safe_int(cleaned.get("congress", None)),
            "session": self.safe_int(cleaned.get("session", None)),
            "pages": self.safe_int(cleaned.get("pages", None)),
            "issued_at": self.standardize_date(cleaned.get("dateissued", None)),
            "branch": cleaned.get("branch", None),
            "government_author1": cleaned.get("governmentauthor1", None),
            "government_author2": cleaned.get("governmentauthor2", None),
            "publisher": cleaned.get("publisher", None),
            "collection_code": cleaned.get("collectioncode", None),
            "migrated_doc_id": cleaned.get("otheridentifier_migrated_doc_id", None),
            "su_doc_class_number": cleaned.get("sudocclassnumber", None),
            "other_su_doc_class_number": cleaned.get(
                "otheridentifier_sudoc_class_number", None
            ),
            "su_doc_item_number": cleaned.get(
                "otheridentifier_sudoc_item_number", None
            ),
            "last_modified": self.standardize_date(cleaned.get("lastmodified", None)),
        }

        return filtered_cleaned

    async def _clean_congressionalreports_serialset_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual congressional reports records.
        FINAL COLUMNS:
        - package_id TEXT PRIMARY KEY,
        - bag_id TEXT,
        - doc_id TEXT,
        - serialset_number TEXT,
        - agency TEXT,
        - volume TEXT,
        - oclc_number TEXT,
        - lccn_number TEXT,
        - issn_number TEXT,
        """
        cleaned = record_data.copy()
        return {
            "package_id": cleaned.get("package_id", "ID_ERROR"),
            "bag_id": cleaned.get("serialset_bagid", None),
            "doc_id": cleaned.get("serialset_docid", None),
            "serialset_number": cleaned.get("serialset_serialsetnumber", None),
            "agency": cleaned.get("agency", None),
            "volume": cleaned.get("volume", None),
            "oclc_number": cleaned.get("otheridentifier_oclc", None),
            "lccn_number": cleaned.get("otheridentifier_lccn", None),
            "issn_number": cleaned.get("otheridentifier_issn", None),
            "last_modified": self.standardize_date(cleaned.get("lastmodified", None)),
        }

    async def _clean_congressionalreports_committees_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual bills records.
        STAGING COLUMNS:
        - type          text,
        - chamber       text,
        - authorityid   text,
        - committeename text,
        - id            text,
        - package_id    text,
        - list_index    text

        FINAL COLUMNS:
        - package_id TEXT,
        - committee_code TEXT,
        - committee_name TEXT,
        """
        # logger.info(f"Cleaning committees: {record_data}")
        cleaned = record_data.copy()

        filtered_cleaned = {
            "package_id": cleaned.get("package_id", "ID_ERROR"),
            "committee_code": cleaned.get("authorityid", "ID_ERROR"),
            "committee_name": cleaned.get("committeename", None),
        }

        return filtered_cleaned

    async def _clean_congressionalreports_members_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual bills records.
        STAGING COLUMNS:
        - type       text,
        - title      text,
        - id         text,
        - package_id text,
        - list_index text,
        - level      text
        FINAL COLUMNS:
        - package_id TEXT,
        - short_title TEXT,
        - level TEXT,
        - type TEXT,
        """

        cleaned = record_data.copy()

        filtered_cleaned = {
            "package_id": cleaned.get("package_id", "ID_ERROR"),
            "short_title": cleaned.get("title", None),
            "level": cleaned.get("level", None),
            "type": cleaned.get("type", None),
        }

        return filtered_cleaned

    async def _clean_congressionalreports_bills_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual congressional reports records.
        """

    # Add post-processing methods
    async def _post_process_congressionalreports(self) -> dict[str, Any]:
        """Post-processing operations specific to bills data."""
        # TODO: ils system id
        # TODO: serialset topics
        # TODO: reference laws/codes/statutes
        if not self.db_pool:
            raise ValueError("Database pool not configured")

        results = {
            "status": "success",
            "operations": [],
            "rows_affected": 0,
        }

        # ? Operation 1: ils system id
        try:
            async with self.db_pool.acquire() as conn, conn.transaction():
                # Fetch all records from staging
                fetch_sql = f"""
                    SELECT package_id, otheridentifier_ils_system_id
                    FROM {self.staging_schema}.congressionalreports
                """
                rows = await conn.fetch(fetch_sql)

                # json load the otheridentifier_ils_system_id value as a list, then insert into the ils_system_id table
                for row in rows:
                    ils_system_id = json.loads(row["otheridentifier_ils_system_id"])
                    insert_sql = f"""
                            INSERT INTO {self.production_schema}.ils_system_id (package_id, ils_system_id)
                            VALUES (%s, %s)
                        """
                    for ils_id in ils_system_id:
                        await conn.execute(insert_sql, row["package_id"], ils_id)
                        results["rows_affected"] += 1
                    results["operations"].append(
                        {
                            "name": "populate_ils_system_id_field",
                            "status": "success",
                            "rows_affected": len(ils_system_id),
                        }
                    )
                    results["rows_affected"] += len(ils_system_id)

                logger.info(f"Updated ils_system_id for {results['rows_affected']} congressional reports")

        except Exception as e:
            logger.error(f"Error populating ils_system_id field: {e}", exc_info=True)
            results["operations"].append(
                {
                    "name": "populate_sponsors_field",
                    "status": "error",
                    "error": str(e),
                }
            )
            results["status"] = "partial_failure"

        # ? Operation 2: serialset data
        try:
            async with self.db_pool.acquire() as conn, conn.transaction():
                # Fetch all records from staging
                fetch_sql = f"""
                SELECT package_id, serialset_bagid, serialset_docid, serialset_serialsetnumber, agency, volume, otheridentifier_oclc, otheridentifier_lccn, otheridentifier_issn FROM {self.staging_schema}.congressionalreports_serialset
                """
                rows = await conn.fetch(fetch_sql)

                serialset_records = []
                for row in rows:
                    serialset_records.append(
                        {
                            "package_id": row["package_id"],
                            "bag_id": row["serialset_bagid"],
                            "doc_id": row["serialset_docid"],
                            "serialset_number": row["serialset_serialsetnumber"],
                            "agency": row["agency"],
                            "volume": row["volume"],
                            "oclc_number": row["otheridentifier_oclc"],
                            "lccn_number": row["otheridentifier_lccn"],
                            "issn_number": row["otheridentifier_issn"],
                        }
                    )
                if serialset_records:
                    insert_sql = f"""
                        INSERT INTO {self.production_schema}.congressionalreports_serialset (package_id, bag_id, doc_id, serialset_number, agency, volume, oclc_number, lccn_number, issn_number)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    await conn.executemany(insert_sql, serialset_records)
                    serialset_rows_affected = len(serialset_records)
                    results["operations"].append(
                        {
                            "name": "populate_serialset_field",
                            "status": "success",
                            "rows_affected": serialset_rows_affected,
                        }
                    )
                    results["rows_affected"] += serialset_rows_affected
        except Exception as e:
            logger.error(f"Error populating serialset field: {e}", exc_info=True)
            results["operations"].append(
                {
                    "name": "populate_serialset_field",
                    "status": "error",
                    "error": str(e),
                }
            )
            results["status"] = "partial_failure"
        # ? Operation 3: references
        try:
            async with self.db_pool.acquire() as conn, conn.transaction():
                # Fetch all records from staging
                fetch_sql = f"""
                SELECT package_id, collectioncode, contents FROM {self.staging_schema}.congressionalreports_references
                """
                rows = await conn.fetch(fetch_sql)

                reference_law_records = []
                reference_statute_records = []
                reference_code_records = []
                reference_statute_page_records = []
                reference_code_section_records = []

                for row in rows:
                    # Parse contents if it's a JSON string
                    contents = row.get("contents", [])
                    if isinstance(contents, str):
                        try:
                            contents = json.loads(contents)
                        except json.JSONDecodeError:
                            logger.warning(f"Failed to parse JSON contents: {contents}")
                            continue

                    if row["collectioncode"] == "PLAW" and contents:
                        for content in contents:
                            reference_law_records.append(
                                {
                                    "package_id": row["package_id"],
                                    "law_id": f"PL{content.get('congress')}-{content.get('number')}",
                                    "law_type": content.get("label", "")
                                    .split(" ")[0]
                                    .lower()
                                    if content.get("label", "")
                                    else None,
                                    "law_number": f"{content.get('congress')}-{content.get('number')}",
                                    "order_number": self.safe_int(
                                        content.get("number")
                                    ),
                                    "congress": self.safe_int(content.get("congress")),
                                }
                            )
                    elif row["collectioncode"] == "STATUTE" and contents:
                        for content in contents:
                            bill_statute_id = hashlib.sha256(
                                f"{row['package_id']}-{content['label']}-{content['pages']}-{content['title']}".encode()
                            ).hexdigest()[:16]
                            reference_statute_records.append(
                                {
                                    "bill_statute_id": bill_statute_id,
                                    "package_id": row["package_id"],
                                    "reference_statute": f"{content.get('label', '').lower()}{content.get('title', '')}",
                                }
                            )
                            for page in json.loads(content.get("pages", "[]")):
                                reference_statute_page_records.append(
                                    {
                                        "bill_statute_id": bill_statute_id,
                                        "page": page,
                                    }
                                )
                    elif row["collectioncode"] == "USCODE" and contents:
                        for content in contents:
                            bill_code_id = hashlib.sha256(
                                f"{row['package_id']}-{content['sections']}-{content['title']}".encode()
                            ).hexdigest()[:16]
                            reference_code_records.append(
                                {
                                    "bill_code_id": bill_code_id,
                                    "package_id": row["package_id"],
                                    "reference_code": f"{content.get('label', '').replace('.', '')}-{content.get('title', '')}",
                                }
                            )
                            for section in json.loads(content.get("sections", "[]")):
                                reference_code_section_records.append(
                                    {
                                        "bill_code_id": bill_code_id,
                                        "section": section,
                                    }
                                )
                if reference_law_records:
                    insert_sql = f"""
                        INSERT INTO {self.production_schema}.billcollections_reference_laws (package_id, law_id, law_type, law_number, order_number, congress)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """
                    await conn.executemany(insert_sql, reference_law_records)
                    reference_law_rows_affected = len(reference_law_records)
                    results["operations"].append(
                        {
                            "name": "populate_reference_laws_field",
                            "status": "success",
                            "rows_affected": reference_law_rows_affected,
                        }
                    )
                    results["rows_affected"] += reference_law_rows_affected
                if reference_statute_records:
                    insert_sql = f"""
                        INSERT INTO {self.production_schema}.billcollections_reference_statutes (bill_statute_id, package_id, reference_statute)
                        VALUES (%s, %s, %s)
                    """
                    await conn.executemany(insert_sql, reference_statute_records)
                    reference_statute_rows_affected = len(reference_statute_records)
                    results["operations"].append(
                        {
                            "name": "populate_reference_statutes_field",
                            "status": "success",
                            "rows_affected": reference_statute_rows_affected,
                        }
                    )
                    results["rows_affected"] += reference_statute_rows_affected
                if reference_code_records:
                    insert_sql = f"""
                        INSERT INTO {self.production_schema}.billcollections_reference_codes (bill_code_id, package_id, reference_code)
                        VALUES (%s, %s, %s)
                    """
                    await conn.executemany(insert_sql, reference_code_records)
                    reference_code_rows_affected = len(reference_code_records)
                    results["operations"].append(
                        {
                            "name": "populate_reference_codes_field",
                            "status": "success",
                            "rows_affected": reference_code_rows_affected,
                        }
                    )
                    results["rows_affected"] += reference_code_rows_affected
                if reference_statute_page_records:
                    insert_sql = f"""
                        INSERT INTO {self.production_schema}.billcollections_reference_statutes_pages (bill_statute_id, page)
                        VALUES (%s, %s)
                    """
                    await conn.executemany(insert_sql, reference_statute_page_records)
                    reference_statute_page_rows_affected = len(
                        reference_statute_page_records
                    )
                    results["operations"].append(
                        {
                            "name": "populate_reference_statutes_pages_field",
                            "status": "success",
                            "rows_affected": reference_statute_page_rows_affected,
                        }
                    )
                    results["rows_affected"] += reference_statute_page_rows_affected
                if reference_code_section_records:
                    insert_sql = f"""
                        INSERT INTO {self.production_schema}.billcollections_reference_codes_sections (bill_code_id, code_section)
                        VALUES (%s, %s)
                    """
                    await conn.executemany(insert_sql, reference_code_section_records)
                    reference_code_section_rows_affected = len(
                        reference_code_section_records
                    )
                    results["operations"].append(
                        {
                            "name": "populate_reference_codes_sections_field",
                            "status": "success",
                            "rows_affected": reference_code_section_rows_affected,
                        }
                    )
                    results["rows_affected"] += reference_code_section_rows_affected
        except Exception as e:
            logger.error(f"Error populating references field: {e}", exc_info=True)
            results["operations"].append(
                {
                    "name": "populate_references_field",
                    "status": "error",
                    "error": str(e),
                }
            )
            results["status"] = "partial_failure"

        return results
