"""
Bill Collections Custom Plugin Logic

This module contains all the custom logic for bill collections data type including:
- Fetching logic (from fetcher.py)
- Cleaning logic (from cleaner.py)

"""

import hashlib
import json
import logging
from typing import Any

from streamlined.plugins.base import BaseCleanerLogic

logger = logging.getLogger(__name__)


class BillCollectionsFetcher:
    """
    Bill Collections fetcherogic that extends the base GovInfo fetcher logic.

    This class provides custom methods for fetching congressional directories data, including:
    - Extracting standardized congressional directories IDs
    - Handling related data from URLs
    - Implementing specialized fetching logic for congressional directories-specific data

    Inherits shared functionality like get_generic_related_data from
    GovInfoBaseFetcher.
    """

    def __init__(self, data_type: str = "billcollections"):
        self.data_type = data_type


class BillcollectionsCleanerLogic(BaseCleanerLogic):
    """
    Bill Collections cleaner logic extracted from BillCollectionsCleaner class.
    Contains all the custom cleaning methods for bill collections data.
    """

    # TODO: no granules do everuthing

    def __init__(
        self,
        data_type_name: str = "billcollections",
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
        self.multi_table_data_types = {}

    async def _clean_billcollections_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual bills records.
        STAGING COLUMNS:
        - pages                                text,
        - title                                text,
        - branch                               text,
        - session                              text,
        - billtype                             text,
        - category                             text,
        - congress                             text,
        - docclass                             text,
        - download_pdflink                     text,
        - download_txtlink                     text,
        - download_ziplink                     text,
        - download_modslink                    text,
        - download_premislink                  text,
        - isprivate                            text,
        - packageid                            text,
        - publisher                            text,
        - billnumber                           text,
        - dateissued                           text,
        - billversion                          text,
        - detailslink                          text,
        - relatedlink                          text,
        - lastmodified                         text,
        - originchamber                        text,
        - collectioncode                       text,
        - collectionname                       text,
        - currentchamber                       text,
        - isappropriation                      text,
        - otheridentifier_stock_number         text,
        - otheridentifier_child_ils_title      text,
        - otheridentifier_migrated_doc_id      text,
        - otheridentifier_parent_ils_title     text,
        - otheridentifier_child_ils_system_id  text,
        - otheridentifier_parent_ils_system_id text,
        - sudocclassnumber                     text,
        - governmentauthor1                    text,
        - governmentauthor2                    text,
        - package_id                           text,
        - processed_at                         text,
        - source_doc_id                        text,
        - billversionextended                  text,
        - download_xmllink                     text,
        - related_billstatuslink               text,
        - download_uslmlink                   text,
        - otheridentifier_ils_system_id        text

        FINAL COLUMNS:
        - package_id TEXT PRIMARY KEY,
        - bill_id TEXT,
        - bill_version TEXT,
        - origin_chamber TEXT, -- needs to be lowered
        - current_chamber TEXT, -- needs to be lowered
        - is_appropriation BOOLEAN,
        - is_private BOOLEAN,
        - pages INTEGER,
        - issued_at DATE,
        - government_author1 TEXT,
        - government_author2 TEXT,
        - publisher TEXT,
        - collection_code TEXT,
        - stock_number TEXT,
        - su_doc_class_number TEXT,
        - migrated_doc_id TEXT,
        - child_ils_system_id TEXT,
        - parent_ils_system_id TEXT,
        - mods_url TEXT,
        - pdf_url TEXT,
        - premis_url TEXT,
        - txt_url TEXT,
        - xml_url TEXT,
        - zip_url TEXT,
        - last_modified TIMESTAMP WITH TIME ZONE
        """
        cleaned = record_data.copy()

        # if 'CPRT' not in packageid or if "JCS" in packageid, return None
        bill_type = cleaned.get("billtype", None)
        bill_number = cleaned.get("billnumber", None)
        congress = cleaned.get("congress", None)

        if all([bill_type, bill_number, congress]):
            bill_id = f"{bill_type}{bill_number}-{congress}"
        else:
            raise ValueError(
                f"Invalid bill_id format: bill_type={bill_type}, bill_number={bill_number}, congress={congress}. Record is {cleaned}"
            )

        # Apply bills-specific cleaning logic
        filtered_cleaned = {
            "package_id": str(cleaned.get("packageid", "ID_ERROR")),
            "bill_id": str(bill_id),
            "version_code": cleaned.get("billversion", None),
            "origin_chamber": self.standardize_chamber(cleaned.get("originchamber")),
            "current_chamber": self.standardize_chamber(cleaned.get("currentchamber")),
            "is_appropriation": cleaned.get("isappropriation") == "true"
            if cleaned.get("isappropriation")
            else None,
            "is_private": cleaned.get("isprivate") == "true"
            if cleaned.get("isprivate")
            else None,
            "pages": self.safe_int(cleaned.get("pages", None)),
            "issued_at": self.standardize_date(cleaned.get("dateissued", None)),
            "government_author1": cleaned.get("governmentauthor1"),
            "government_author2": cleaned.get("governmentauthor2"),
            "publisher": cleaned.get("publisher"),
            "collection_code": cleaned.get("collectioncode"),
            "stock_number": cleaned.get("otheridentifier_stock_number"),
            "su_doc_class_number": cleaned.get("sudocclassnumber"),
            "migrated_doc_id": cleaned.get("otheridentifier_migrated_doc_id"),
            "child_ils_system_id": cleaned.get("otheridentifier_child_ils_system_id"),
            "parent_ils_system_id": cleaned.get("otheridentifier_parent_ils_system_id"),
            "mods_url": cleaned.get("download_modslink"),
            "pdf_url": cleaned.get("download_pdflink"),
            "premis_url": cleaned.get("download_premislink"),
            "txt_url": cleaned.get("download_txtlink"),
            "xml_url": cleaned.get("download_xmllink"),
            "zip_url": cleaned.get("download_ziplink"),
            "last_modified": self.standardize_date(cleaned.get("lastmodified", None)),
        }

        return filtered_cleaned

    async def _clean_billcollections_committees_singular(
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

    async def _clean_billcollections_shorttitle_singular(
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

        # logger.info(f"Cleaning shorttitle: {record_data}")

        self._register_target_table_override("billcollections_shorttitles")

        cleaned = record_data.copy()

        filtered_cleaned = {
            "package_id": cleaned.get("package_id", "ID_ERROR"),
            "short_title": cleaned.get("title", None),
            "level": cleaned.get("level", None),
            "type": cleaned.get("type", None),
        }

        return filtered_cleaned

    # Add post-processing methods
    async def _post_process_billcollections(self) -> dict[str, Any]:
        """Post-processing operations specific to bills data."""
        if not self.db_pool:
            raise ValueError("Database pool not configured")

        results = {
            "status": "success",
            "operations": [],
            "rows_affected": 0,
        }

        # ? Operation 1: population sponsors/cosponsors
        try:
            async with self.db_pool.acquire() as conn, conn.transaction():
                # Process in batches to avoid timeout
                batch_size = 10000
                offset = 0
                sponsor_records = []
                cosponsor_records = []

                while True:
                    # Fetch batch of records from staging
                    fetch_sql = f"""
                        SELECT m.package_id, m.bioguideid AS bioguide_id, COALESCE(m.membername, mn.parsed) AS name, m.role
                        FROM {self.staging_schema}.billcollections_members AS m
                        LEFT JOIN {self.staging_schema}.billcollections_members_name AS mn
                        ON m.id = mn.members_id
                        ORDER BY m.package_id
                        LIMIT {batch_size} OFFSET {offset}
                    """
                    rows = await conn.fetch(fetch_sql, timeout=60.0)

                    if not rows:
                        break

                    # Process batch
                    for row in rows:
                        if row["role"] == "SPONSOR":
                            sponsor_records.append(
                                {
                                    "package_id": row["package_id"],
                                    "bioguide_id": row["bioguide_id"],
                                    "name": row["name"],
                                }
                            )
                        elif row["role"] == "COSPONSOR":
                            cosponsor_records.append(
                                {
                                    "package_id": row["package_id"],
                                    "bioguide_id": row["bioguide_id"],
                                    "name": row["name"],
                                }
                            )

                    offset += batch_size
                    logger.info(
                        f"Processed {len(rows)} sponsor/cosponsor records (offset: {offset})"
                    )

                # Insert sponsor records
                if sponsor_records:
                    insert_sql = f"""
                        INSERT INTO {self.production_schema}.billcollections_sponsors (package_id, bioguide_id, name)
                        VALUES (%s, %s, %s)
                    """
                    await conn.executemany(insert_sql, sponsor_records)
                sponsor_rows_affected = len(sponsor_records)

                if cosponsor_records:
                    insert_sql = f"""
                        INSERT INTO {self.production_schema}.billcollections_cosponsors (package_id, bioguide_id, name)
                        VALUES (%s, %s, %s)
                    """
                    await conn.executemany(insert_sql, cosponsor_records)
                cosponsor_rows_affected = len(cosponsor_records)

                results["operations"].append(
                    {
                        "name": "populate_sponsors_field",
                        "status": "success",
                        "rows_affected": sponsor_rows_affected,
                    }
                )
                results["rows_affected"] += sponsor_rows_affected

                logger.info(f"Updated sponsors for {sponsor_rows_affected} bills")

                results["operations"].append(
                    {
                        "name": "populate_cosponsors_field",
                        "status": "success",
                        "rows_affected": cosponsor_rows_affected,
                    }
                )
                results["rows_affected"] += cosponsor_rows_affected
                logger.info(f"Updated cosponsors for {cosponsor_rows_affected} bills")

        except Exception as e:
            logger.error(f"Error populating sponsors field: {e}", exc_info=True)
            results["operations"].append(
                {
                    "name": "populate_sponsors_field",
                    "status": "error",
                    "error": str(e),
                }
            )
            results["status"] = "partial_failure"

        # ? Operation 2: populate references
        try:
            async with self.db_pool.acquire() as conn, conn.transaction():
                # Fetch all records from staging with timeout
                fetch_sql = f"""
                SELECT package_id, collectioncode, contents FROM {self.staging_schema}.billcollections_references
                """
                rows = await conn.fetch(fetch_sql, timeout=300.0)

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
                            pages = content.get("pages", [])
                            if isinstance(pages, str):
                                try:
                                    pages = json.loads(pages)
                                except json.JSONDecodeError:
                                    pages = []

                            for page in pages:
                                reference_statute_page_records.append(
                                    {
                                        "bill_statute_id": bill_statute_id,
                                        "page": page,
                                    }
                                )
                    elif row["collectioncode"] == "USCODE" and contents:
                        for content in contents:
                            bill_code_id = hashlib.sha256(
                                f"{row['package_id']}-{content.get('sections', '')}-{content.get('title', '')}".encode()
                            ).hexdigest()[:16]
                            reference_code_records.append(
                                {
                                    "bill_code_id": bill_code_id,
                                    "package_id": row["package_id"],
                                    "reference_code": f"{content.get('label', '').replace('.', '')}-{content.get('title', '')}",
                                }
                            )
                            sections = content.get("sections", [])
                            if isinstance(sections, str):
                                try:
                                    sections = json.loads(sections)
                                except json.JSONDecodeError:
                                    sections = []

                            for section in sections:
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
