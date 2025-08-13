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
import re
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
                "congressionalreports_granules_references",
                "congressionalreports_granules_references_contents",
            ],
            "congressionalreports_members": [
                "congressionalreports_granules_members",
                "congressionalreports_granules_members_name",
            ],
            "congressionalreports_serialset": ["congressionalreports"],
            "congressionalreports": [
                "congressionalreports",
                "congressionalreports_granules",
            ],
        }

    # =========================
    # Shared helpers
    # =========================

    def _build_report_identity(
        self, cleaned: dict[str, Any]
    ) -> tuple[str, str | None, str | None, int, str, str, str]:
        """
        Build canonical identity for congressional report rows (packages or granules).

        Returns tuple:
        - report_id
        - parent_report_id (nullable)
        - granule_id (nullable)
        - part_number (int)
        - report_type (str)
        - report_number (str)
        - congress (str)

        Input must contain: documenttype, documentnumber, congress. Part comes from:
        - For granules: partnumber if present; else parse via parse_part_from_fields
        - For packages: parse via parse_part_from_fields
        Parent part for granules is derived from documentpart or packageid; else 1.
        """
        # Support multiple inbound shapes for ids
        packageid = str(cleaned.get("packageid") or cleaned.get("package_id"))
        granuleid = cleaned.get("granuleid") or cleaned.get("granule_id")

        report_type = cleaned.get("documenttype")
        report_type = report_type.lower() if isinstance(report_type, str) else None
        report_number = cleaned.get("documentnumber") or None
        congress = str(cleaned.get("congress") or "") or None

        if not all([report_type, report_number, congress]):
            # Rare fallback: J6 package-only
            if granuleid is None and "J6" in (packageid or ""):
                report_type = "hrpt"
                report_number = "663"
                congress = "117"
            else:
                raise ValueError(
                    f"Invalid report_id inputs: report_type={report_type}, report_number={report_number}, congress={congress}. Record is {cleaned}"
                )

        if granuleid:
            part_field = cleaned.get("partnumber")
            if part_field is None:
                part_number = self.parse_part_from_fields(cleaned) or 1
            else:
                part_str = str(part_field).strip()
                part_number = (
                    int(part_str)
                    if part_str.isdigit()
                    else (self.roman_to_int(part_str) or 1)
                )

            # Parent part number from documentpart or packageid; else default 1
            parent_part_number: int | None = None
            doc_part_field = cleaned.get("documentpart")
            if doc_part_field:
                doc_part_str = str(doc_part_field).strip()
                if doc_part_str.isdigit():
                    doc_part_val = int(doc_part_str)
                else:
                    doc_part_val = self.roman_to_int(doc_part_str) or None
                if doc_part_val is not None and doc_part_val != part_number:
                    parent_part_number = doc_part_val
            if parent_part_number is None:
                pkg_lower = (packageid or "").lower()
                m = re.search(
                    r"(?:^|[-_.])pt([ivx]+|\d+)(?:$|[-_.])", pkg_lower, re.IGNORECASE
                )
                if m:
                    tok = m.group(1)
                    parent_part_number = (
                        int(tok) if tok.isdigit() else (self.roman_to_int(tok) or None)
                    )
                if parent_part_number is None:
                    m = re.search(
                        r"(?:^|[-_.])vol(?:ume)?([ivx]+|\d+)(?:$|[-_.])",
                        pkg_lower,
                        re.IGNORECASE,
                    )
                    if m:
                        tok = m.group(1)
                        parent_part_number = (
                            int(tok)
                            if tok.isdigit()
                            else (self.roman_to_int(tok) or None)
                        )
            if parent_part_number is None:
                parent_part_number = 1

            parent_report_id = (
                f"{report_type}{report_number}-{parent_part_number}-{congress}"
            )
        else:
            part_number = self.parse_part_from_fields(cleaned) or 1
            parent_report_id = None

        report_id = f"{report_type}{report_number}-{part_number}-{congress}"
        granule_id = str(granuleid) if granuleid else None
        return (
            report_id,
            parent_report_id,
            granule_id,
            part_number,
            report_type,
            report_number,
            congress,
        )

    def roman_to_int(self, roman: str) -> int | None:
        mapping = {
            "I": 1,
            "II": 2,
            "III": 3,
            "IV": 4,
            "V": 5,
            "VI": 6,
            "VII": 7,
            "VIII": 8,
            "IX": 9,
            "X": 10,
            "XI": 11,
            "XII": 12,
            "XIII": 13,
            "XIV": 14,
            "XV": 15,
            "XVI": 16,
            "XVII": 17,
            "XVIII": 18,
            "XIX": 19,
            "XX": 20,
        }
        return mapping.get(roman.upper())

    def parse_part_from_fields(self, cleaned: dict[str, Any]) -> int:
        # Priority: explicit partnumber -> title/subtitle -> packageid suffix
        part_field = cleaned.get("partnumber", cleaned.get("documentpart"))
        if part_field:
            part_field_str = str(part_field).strip()
            if part_field_str.isdigit():
                return int(part_field_str)
            roman_val = self.roman_to_int(part_field_str)
            if roman_val is not None:
                return roman_val

        # As a last resort, inspect packageid for suffix hints like volII/ptII/-pt2
        pkg = str(cleaned.get("packageid", ""))
        m = re.search(r"(?:vol|pt)[-_.]?([ivx]+|\d+)\b", pkg, re.IGNORECASE)
        if m:
            token = m.group(1)
            if token.isdigit():
                return int(token)
            roman_val = self.roman_to_int(token)
            if roman_val is not None:
                return roman_val

        return 1

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
                        crc.package_id,
                        NULL as granuleid,
                        crc.authorityid,
                        crc.committeename,
                        cr.documenttype,
                        cr.documentnumber,
                        cr.congress,
                        cr.documentpart AS partnumber
                    FROM {self.staging_schema}.congressionalreports_committees AS crc
                    JOIN {self.staging_schema}.congressionalreports AS cr ON crc.package_id = cr.packageid
                    GROUP BY crc.package_id, crc.authorityid, crc.committeename, cr.documenttype, cr.documentnumber, cr.congress, cr.documentpart

                    UNION ALL

                    SELECT
                        NULL as package_id,
                        crg.granuleid,
                        crgc.authorityid,
                        crgc.committeename,
                        cr.documenttype,
                        cr.documentnumber,
                        cr.congress,
                        crg.partnumber
                    FROM {self.staging_schema}.congressionalreports_granules_committees AS crgc
                    JOIN {self.staging_schema}.congressionalreports_granules AS crg ON crgc.granule_id = crg.id
                    JOIN {self.staging_schema}.congressionalreports AS cr ON crg.packageid = cr.packageid
                    GROUP BY crg.granuleid, crgc.authorityid, crgc.committeename, cr.documenttype, cr.documentnumber, cr.congress, crg.partnumber

                    ORDER BY package_id, granuleid, authorityid, committeename
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
                SELECT COUNT(DISTINCT c.id)
                FROM {self.staging_schema}.congressionalreports_granules_members c
                """
                package_count = await conn.fetchval(package_count_query)

                granule_count_query = f"""
                SELECT COUNT(DISTINCT gc.id)
                FROM {self.staging_schema}.congressionalreports_granules_members_name gc
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
                        crg.packageid,
                        crg.granuleid,
                        crgm.bioguideid,
                        crgm.membername,
                        crgm.authorityid,
                        crgm.gpoid,
                        crgmn.authority_fnf,
                        crgmn.authority_other,
                        crg.partnumber,
                        cr.documenttype,
                        cr.documentnumber,
                        cr.congress,
                        cr.documentpart
                    FROM {self.staging_schema}.congressionalreports_granules_members AS crgm
                    JOIN {self.staging_schema}.congressionalreports_granules_members_name AS crgmn ON crgm.id = crgmn.members_id
                    JOIN {self.staging_schema}.congressionalreports_granules AS crg ON crgm.granule_id = crg.id
                    JOIN {self.staging_schema}.congressionalreports AS cr ON crg.packageid = cr.packageid
                    GROUP BY crg.packageid, crg.granuleid, crgm.bioguideid, crgm.membername, crgm.authorityid, crgm.gpoid, crgmn.authority_fnf, crgmn.authority_other, crg.partnumber, cr.documenttype, cr.documentnumber, cr.congress, cr.documentpart
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
                                f"Error converting congressionalreports_members row to dict: {e}, row type: {type(row)}, row: {row}"
                            )
                            continue

                    if chunk:  # Only yield if we have valid records
                        yield chunk

                    offset += chunk_size

                    # Log progress periodically
                    if offset % (chunk_size * 10) == 0:
                        logger.debug(
                            f"Streamed {offset} congressionalreports_members records"
                        )

            except Exception as e:
                logger.error(f"Error streaming congressionalreports_members: {e}")
                raise

    async def _stream_congressionalreports_joined_chunks(
        self, chunk_size: int
    ) -> AsyncGenerator[list[dict[str, Any]], None]:
        """
        Stream joined data from congressionalreports and congressionalreports_granules.
        """
        # TODO: between congressionalreports and congressionalreports_granules

        if not self.db_pool:
            raise ValueError("Database pool not configured")

        async with self.db_pool.acquire() as conn:
            try:
                # First, check if both tables exist
                tables_exist_query = """
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_schema = $1
                AND table_name IN ('congressionalreports', 'congressionalreports_granules')
                """
                tables_count = await conn.fetchval(
                    tables_exist_query, self.staging_schema
                )

                if tables_count < 2:
                    logger.warning(
                        "One or both congressionalreports staging tables do not exist"
                    )
                    return

                # Get total count for logging
                package_count_query = f"""
                SELECT COUNT(DISTINCT c.packageid)
                FROM {self.staging_schema}.congressionalreports c
                """
                package_count = await conn.fetchval(package_count_query)

                granule_count_query = f"""
                SELECT COUNT(DISTINCT g.granuleid)
                FROM {self.staging_schema}.congressionalreports_granules g
                """
                granule_count = await conn.fetchval(granule_count_query)

                total_count = package_count + granule_count

                if total_count == 0:
                    logger.info("No congressionalreports records found")
                    return

                logger.info(
                    f"Streaming {total_count} congressionalreports records in chunks of {chunk_size}"
                )

                # Stream using pagination with aggregated formats
                offset = 0
                while True:
                    # Use JSON aggregation to collect all formats for each text
                    query = f"""
                    SELECT * FROM (
                        -- Package-only row (always include)
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
                            c.download_txtlink AS download_txtlink,
                            c.detailslink AS detailslink,
                            c.lastmodified AS lastmodified,
                            c.documenttype AS documenttype,
                            c.documentnumber AS documentnumber,
                            c.publisher AS publisher,
                            c.dateissued AS dateissued,
                            c.granuleslink AS granuleslink,
                            c.collectioncode AS collectioncode,
                            c.collectionname AS collectionname,
                            c.pages AS pages,
                            c.otheridentifier_migrated_doc_id AS otheridentifier_migrated_doc_id,
                            c.sudocclassnumber AS sudocclassnumber,
                            c.governmentauthor1 AS governmentauthor1,
                            c.governmentauthor2 AS governmentauthor2,
                            c.processed_at AS processed_at,
                            c.source_doc_id AS source_doc_id,
                            c.otheridentifier_ils_system_id AS otheridentifier_ils_system_id,
                            c.otheridentifier_sudoc_item_number AS otheridentifier_sudoc_item_number,
                            c.otheridentifier_sudoc_class_number AS otheridentifier_sudoc_class_number,
                            c.agency AS agency,
                            c.volume AS volume,
                            c.parentid AS parentid,
                            c.subjects_topics AS subjects_topics,
                            c.serialset_bagid AS serialset_bagid,
                            c.serialset_docid AS serialset_docid,
                            c.serialset_isglp AS serialset_isglp,
                            c.serialset_serialsetnumber AS serialset_serialsetnumber,
                            c.otheridentifier_oclc AS otheridentifier_oclc,
                            c.committees AS committees,
                            c.dateissuednotspecified AS dateissuednotspecified,
                            c.otheridentifier_lccn AS otheridentifier_lccn,
                            c.download_jpeglink AS download_jpeglink,
                            c.federalpublicationname AS federalpublicationname,
                            c.download_thumbnailjpeg AS download_thumbnailjpeg,
                            c.references AS references,
                            c.president_id AS president_id,
                            c.president_names AS president_names,
                            c.president_party AS president_party,
                            c.documentpart AS documentpart,
                            c.otheridentifier_issn AS otheridentifier_issn,
                            NULL::text AS heading,
                            NULL::text AS partnumber,
                            NULL::text AS volumenumber
                        FROM {self.staging_schema}.congressionalreports c

                        UNION ALL

                        -- Granule rows
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
                            COALESCE(g.download_txtlink, c.download_txtlink) AS download_txtlink,
                            COALESCE(g.detailslink, c.detailslink) AS detailslink,
                            COALESCE(g.lastmodified, c.lastmodified) AS lastmodified,
                            c.documenttype AS documenttype,
                            c.documentnumber AS documentnumber,
                            c.publisher AS publisher,
                            c.dateissued AS dateissued,
                            c.granuleslink AS granuleslink,
                            c.collectioncode AS collectioncode,
                            c.collectionname AS collectionname,
                            c.pages AS pages,
                            c.otheridentifier_migrated_doc_id AS otheridentifier_migrated_doc_id,
                            c.sudocclassnumber AS sudocclassnumber,
                            c.governmentauthor1 AS governmentauthor1,
                            c.governmentauthor2 AS governmentauthor2,
                            c.processed_at AS processed_at,
                            c.source_doc_id AS source_doc_id,
                            c.otheridentifier_ils_system_id AS otheridentifier_ils_system_id,
                            c.otheridentifier_sudoc_item_number AS otheridentifier_sudoc_item_number,
                            c.otheridentifier_sudoc_class_number AS otheridentifier_sudoc_class_number,
                            c.agency AS agency,
                            c.volume AS volume,
                            c.parentid AS parentid,
                            c.subjects_topics AS subjects_topics,
                            c.serialset_bagid AS serialset_bagid,
                            c.serialset_docid AS serialset_docid,
                            c.serialset_isglp AS serialset_isglp,
                            c.serialset_serialsetnumber AS serialset_serialsetnumber,
                            c.otheridentifier_oclc AS otheridentifier_oclc,
                            c.committees AS committees,
                            c.dateissuednotspecified AS dateissuednotspecified,
                            c.otheridentifier_lccn AS otheridentifier_lccn,
                            c.download_jpeglink AS download_jpeglink,
                            c.federalpublicationname AS federalpublicationname,
                            c.download_thumbnailjpeg AS download_thumbnailjpeg,
                            c.references AS references,
                            c.president_id AS president_id,
                            c.president_names AS president_names,
                            c.president_party AS president_party,
                            COALESCE(c.documentpart, g.partnumber, g.volumenumber) AS documentpart,
                            c.otheridentifier_issn AS otheridentifier_issn,
                            g.heading AS heading,
                            g.partnumber AS partnumber,
                            g.volumenumber AS volumenumber
                        FROM {self.staging_schema}.congressionalreports c
                        JOIN {self.staging_schema}.congressionalreports_granules g
                            ON c.packageid = g.packageid
                    ) q
                    ORDER BY q.packageid, q.granuleid NULLS FIRST
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
                                f"Error converting congressionalreports row to dict: {e}, row type: {type(row)}, row: {row}"
                            )
                            continue

                    if chunk:  # Only yield if we have valid records
                        yield chunk

                    offset += chunk_size

                    # Log progress periodically
                    if offset % (chunk_size * 10) == 0:
                        logger.debug(f"Streamed {offset} congressionalreports records")

            except Exception as e:
                logger.error(f"Error streaming congressionalreports: {e}")
                raise

    async def _stream_congressionalreports_bills_joined_chunks(
        self, chunk_size: int
    ) -> AsyncGenerator[list[dict[str, Any]], None]:
        """
        Stream congressional reports bills joined chunks.
        """
        if not self.db_pool:
            raise ValueError("Database pool not configured")

        logger.info(
            f"Streaming congressionalreports_bills records in chunks of {chunk_size}"
        )

        # Check if tables exist
        async with self.db_pool.acquire() as conn, conn.transaction():
            tables_exist_query = f"""
                SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '{self.staging_schema}' AND table_name IN ('congressionalreports_granules_references_contents', 'congressionalreports_granules_references', 'congressionalreports_granules', 'congressionalreports')
            """
            tables_count = await conn.fetchval(tables_exist_query)

            if tables_count < 4:
                logger.warning(
                    f"Missing required tables in {self.staging_schema} schema: "
                    "congressionalreports_granules_references_contents, "
                    "congressionalreports_granules_references, "
                    "congressionalreports_granules, "
                    "congressionalreports"
                )
                return

        try:
            offset = 0
            while True:
                async with self.db_pool.acquire() as conn, conn.transaction():
                    query = f"""
                            SELECT crgrc.*, crg.granuleid, crg.partnumber, cr.packageid,cr.documenttype, cr.documentnumber, cr.congress, cr.documentpart FROM {self.staging_schema}.congressionalreports_granules_references_contents AS crgrc
                            JOIN {self.staging_schema}.congressionalreports_granules_references AS crgr ON crgr.id = crgrc.references_id
                            JOIN {self.staging_schema}.congressionalreports_granules AS crg ON crg.id = crgr.granule_id
                            JOIN {self.staging_schema}.congressionalreports AS cr ON crg.packageid = cr.packageid
                            ORDER BY cr.packageid, crg.granuleid NULLS FIRST
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
                                f"Error converting congressionalreports_bills row to dict: {e}, row type: {type(row)}, row: {row}"
                            )
                            continue

                    if chunk:
                        yield chunk

                    offset += chunk_size

                    if offset % (chunk_size * 10) == 0:
                        logger.debug(
                            f"Streamed {offset} congressionalreports_bills records"
                        )

        except Exception as e:
            logger.error(f"Error streaming congressionalreports_bills: {e}")
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
        - granule_id TEXT,
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

        (
            report_id,
            parent_report_id,
            granule_id,
            part_number,
            report_type,
            report_number,
            congress,
        ) = self._build_report_identity(cleaned)

        # Compute errata flag based on id heuristics
        is_errata: bool | None = None
        try:
            if "err" in cleaned.get("packageid", "").lower():
                is_errata = True
            if granule_id is not None:
                heading_val = str(cleaned.get("heading", "")).strip().lower()
                if heading_val == "errata":
                    is_errata = True
        except Exception:
            # Best-effort flag; ignore parsing issues
            pass

        # Apply bills-specific cleaning logic
        filtered_cleaned = {
            "package_id": str(cleaned.get("packageid", "ID_ERROR")),
            "report_id": str(report_id),
            "granule_id": str(cleaned.get("granuleid"))
            if cleaned.get("granuleid")
            else None,
            # Note: parent_report_id is returned for completeness; add column if you want it persisted
            "parent_report_id": parent_report_id,
            "title": cleaned.get("title", None),
            "subtitle": cleaned.get("subtitle", None),
            "chamber": self.standardize_chamber(cleaned.get("chamber", None)),
            "congress": self.safe_int(cleaned.get("congress", None)),
            "session": self.safe_int(cleaned.get("session", None)),
            "pages": self.safe_int(cleaned.get("pages", None)),
            "is_errata": is_errata,
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
        report_id, _, _, _, _, _, _ = self._build_report_identity(cleaned)

        return {
            "package_id": cleaned.get("package_id", "ID_ERROR"),
            "report_id": report_id,
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
        - package_id    text,
        - granuleid    text,
        - authorityid   text,
        - committeename text,
        - documenttype  text,
        - documentnumber text,
        - congress      text,
        - documentpart  text,

        FINAL COLUMNS:
        - package_id TEXT,
        - granule_id TEXT,
        - report_id TEXT,
        - committee_code TEXT,
        - committee_name TEXT,
        """
        # logger.info(f"Cleaning committees: {record_data}")
        cleaned = record_data.copy()
        report_id, _, granule_id, _, _, _, _ = self._build_report_identity(cleaned)

        filtered_cleaned = {
            "package_id": cleaned.get("package_id", "ID_ERROR"),
            "granule_id": granule_id,
            "report_id": report_id,
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
        crg.packageid,
        crg.granuleid,
        crgm.bioguideid,
        crgm.membername
        crgm.authorityid,
        crgm.gpoid,
        crgmn.authority_fnf,
        crgmn.authority_other
        FINAL COLUMNS:
        - package_id TEXT,
        - granule_id TEXT,
        - report_id TEXT,
        - bioguide_id TEXT,
        - membername TEXT,
        - authorityid TEXT,
        - gpoid TEXT,
        """

        cleaned = record_data.copy()
        report_id, _, granule_id, _, _, _, _ = self._build_report_identity(cleaned)

        filtered_cleaned = {
            "package_id": cleaned.get("package_id", "ID_ERROR"),
            "granule_id": granule_id,
            "report_id": report_id,
            "bioguide_id": cleaned.get("bioguideid", "ID_ERROR"),
            "membername": cleaned.get("membername", None),
            "authorityid": cleaned.get("authorityid", "ID_ERROR"),
            "gpoid": cleaned.get("gpoid", None),
        }

        return filtered_cleaned

    async def _clean_congressionalreports_bills_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual congressional reports records.
        STAGING COLUMNS:
        - type          text,
        - number        text,
        - congress      text,
        - id            text,
        - references_id text,
        - list_index    text
        - granuleid     text,
        - partnumber    text,
        - packageid     text,
        - documenttype  text,
        - documentnumber text,
        - congress      text,
        - documentpart  text,

        """
        logger.info(f"Cleaning congressionalreports_bills: {record_data}")

        cleaned = record_data.copy()
        report_id, _, granule_id, _, _, _, _ = self._build_report_identity(cleaned)

        bill_type = cleaned.get("type", "").lower()
        bill_number = cleaned.get("number", "")
        bill_congress = cleaned.get("congress", "")

        if all([bill_type, bill_number, bill_congress]):
            bill_id = f"{bill_type}{bill_number}-{bill_congress}"
        else:
            bill_id = None

        filtered_cleaned = {
            "package_id": cleaned.get("package_id", "ID_ERROR"),
            "granule_id": granule_id,
            "report_id": report_id,
            "bill_id": bill_id,
            "bill_type": bill_type,
            "bill_number": bill_number,
            "congress": bill_congress,
        }
        return filtered_cleaned

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
                    ils_system_id_raw = row["otheridentifier_ils_system_id"]
                    if ils_system_id_raw:
                        try:
                            if isinstance(ils_system_id_raw, str):
                                ils_system_id = json.loads(ils_system_id_raw)
                            elif isinstance(ils_system_id_raw, list):
                                ils_system_id = ils_system_id_raw
                            else:
                                ils_system_id = [str(ils_system_id_raw)]
                        except json.JSONDecodeError:
                            ils_system_id = [str(ils_system_id_raw)]

                        insert_sql = f"""
                                INSERT INTO {self.production_schema}.congressionalreports_ils_system_id (package_id, ils_system_id)
                                VALUES ($1, $2)
                            """
                        for ils_id in ils_system_id:
                            if ils_id:  # Skip empty values
                                await conn.execute(
                                    insert_sql, row["package_id"], ils_id
                                )
                                results["rows_affected"] += 1
                    results["operations"].append(
                        {
                            "name": "populate_ils_system_id_field",
                            "status": "success",
                            "rows_affected": len(ils_system_id),
                        }
                    )
                    results["rows_affected"] += len(ils_system_id)

                logger.info(
                    f"Updated ils_system_id for {results['rows_affected']} congressional reports"
                )

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

        # ? Operation 2: serialset data
        try:
            async with self.db_pool.acquire() as conn, conn.transaction():
                # Fetch all records from staging
                fetch_sql = f"""
                SELECT packageid as package_id, serialset_bagid, serialset_docid, serialset_serialsetnumber, agency, volume, otheridentifier_oclc, otheridentifier_lccn, otheridentifier_issn, lastmodified, documenttype, documentnumber, congress, documentpart FROM {self.staging_schema}.congressionalreports
                WHERE serialset_bagid IS NOT NULL OR serialset_docid IS NOT NULL OR serialset_serialsetnumber IS NOT NULL
                """
                rows = await conn.fetch(fetch_sql)

                serialset_records = []
                for row in rows:
                    cleaned_row = (
                        await self._clean_congressionalreports_serialset_singular(
                            dict(row)
                        )
                    )
                    serialset_records.append(
                        (
                            cleaned_row["package_id"],
                            cleaned_row["report_id"],
                            cleaned_row["bag_id"],
                            cleaned_row["doc_id"],
                            cleaned_row["serialset_number"],
                            cleaned_row["agency"],
                            cleaned_row["volume"],
                            cleaned_row["oclc_number"],
                            cleaned_row["lccn_number"],
                            cleaned_row["issn_number"],
                            cleaned_row["last_modified"],
                        )
                    )
                if serialset_records:
                    insert_sql = f"""
                        INSERT INTO {self.production_schema}.congressionalreports_serialset (package_id, report_id, bag_id, doc_id, serialset_number, agency, volume, oclc_number, lccn_number, issn_number, last_modified)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                    """
                    for record in serialset_records:
                        await conn.execute(insert_sql, *record)
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
                SELECT package_id, collectioncode, contents FROM {self.staging_schema}.congressionalreports_granules_references_contents AS cgrc
                JOIN {self.staging_schema}.congressionalreports_granules_references AS cgr ON cgrc.references_id = cgr.id
                JOIN {self.staging_schema}.congressionalreports_granules AS cg ON cgr.granule_id = cg.id
                JOIN {self.staging_schema}.congressionalreports AS c ON cg.packageid = c.packageid
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
                            report_statute_id = hashlib.sha256(
                                f"{row['package_id']}-{content.get('label', '')}-{content.get('pages', '')}-{content.get('title', '')}".encode()
                            ).hexdigest()[:16]
                            reference_statute_records.append(
                                {
                                    "bill_statute_id": report_statute_id,
                                    "package_id": row["package_id"],
                                    "reference_statute": f"{content.get('label', '').lower()}{content.get('title', '')}",
                                }
                            )
                            pages_data = content.get("pages", "[]")
                            if isinstance(pages_data, str):
                                try:
                                    pages_list = json.loads(pages_data)
                                except json.JSONDecodeError:
                                    pages_list = [pages_data]
                            else:
                                pages_list = (
                                    pages_data if isinstance(pages_data, list) else []
                                )
                            for page in pages_list:
                                reference_statute_page_records.append(
                                    {
                                        "bill_statute_id": report_statute_id,
                                        "page": page,
                                    }
                                )
                    elif row["collectioncode"] == "USCODE" and contents:
                        for content in contents:
                            report_code_id = hashlib.sha256(
                                f"{row['package_id']}-{content.get('sections', '')}-{content.get('title', '')}".encode()
                            ).hexdigest()[:16]
                            reference_code_records.append(
                                {
                                    "bill_code_id": report_code_id,
                                    "package_id": row["package_id"],
                                    "reference_code": f"{content.get('label', '').replace('.', '')}-{content.get('title', '')}",
                                }
                            )
                            sections_data = content.get("sections", "[]")
                            if isinstance(sections_data, str):
                                try:
                                    sections_list = json.loads(sections_data)
                                except json.JSONDecodeError:
                                    sections_list = [sections_data]
                            else:
                                sections_list = (
                                    sections_data
                                    if isinstance(sections_data, list)
                                    else []
                                )
                            for section in sections_list:
                                reference_code_section_records.append(
                                    {
                                        "bill_code_id": report_code_id,
                                        "section": section,
                                    }
                                )
                if reference_law_records:
                    insert_sql = f"""
                        INSERT INTO {self.production_schema}.congressionalreports_reference_laws (package_id, law_id, law_type, law_number, order_number, congress)
                        VALUES ($1, $2, $3, $4, $5, $6)
                    """
                    for record in reference_law_records:
                        await conn.execute(
                            insert_sql,
                            record["package_id"],
                            record["law_id"],
                            record["law_type"],
                            record["law_number"],
                            record["order_number"],
                            record["congress"],
                        )
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
                        INSERT INTO {self.production_schema}.congressionalreports_reference_statutes (report_statute_id, package_id, reference_statute)
                        VALUES ($1, $2, $3)
                    """
                    for record in reference_statute_records:
                        await conn.execute(
                            insert_sql,
                            record["bill_statute_id"],
                            record["package_id"],
                            record["reference_statute"],
                        )
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
                        INSERT INTO {self.production_schema}.congressionalreports_reference_codes (report_code_id, package_id, reference_code)
                        VALUES ($1, $2, $3)
                    """
                    for record in reference_code_records:
                        await conn.execute(
                            insert_sql,
                            record["bill_code_id"],
                            record["package_id"],
                            record["reference_code"],
                        )
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
                        INSERT INTO {self.production_schema}.congressionalreports_reference_statutes_pages (report_statute_id, page)
                        VALUES ($1, $2)
                    """
                    for record in reference_statute_page_records:
                        await conn.execute(
                            insert_sql, record["bill_statute_id"], record["page"]
                        )
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
                        INSERT INTO {self.production_schema}.congressionalreports_reference_codes_sections (report_code_id, code_section)
                        VALUES ($1, $2)
                    """
                    for record in reference_code_section_records:
                        await conn.execute(
                            insert_sql, record["bill_code_id"], record["section"]
                        )
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
