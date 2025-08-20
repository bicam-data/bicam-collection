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
            "congressionalreports_reference_bills": [
                "congressionalreports_granules_references",
                "congressionalreports_granules_references_contents",
            ],
            "congressionalreports_members": [
                "congressionalreports_granules_members",
                "congressionalreports_granules_members_name",
            ],
            # "congressionalreports_serialset": ["congressionalreports"],
            "congressionalreports": [
                "congressionalreports",
                "congressionalreports_granules",
            ],
        }

    # =========================
    # Shared helpers
    # =========================

    def _detect_is_errata(self, cleaned: dict[str, Any]) -> bool:
        """Best-effort detection of errata records from available fields.

        Heuristics:
        - Package-level: package id contains "err"
        - Granule-level: heading equals "Errata"
        """
        try:
            package_id_val = str(
                cleaned.get("packageid") or cleaned.get("package_id") or ""
            )
            if "err" in package_id_val.lower():
                return True

            heading_val = cleaned.get("heading")
            if heading_val is not None and str(heading_val).strip().lower() == "errata":
                return True
        except Exception:
            # Non-fatal; treat as non-errata if parsing fails
            pass
        return False

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
            if "J6" in (packageid or ""):
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

        # Append errata suffix to the part token when applicable so errata sorts
        # immediately after the main part (e.g., "2" < "2e" < "3").
        is_errata = self._detect_is_errata(cleaned)

        # Use placeholder 'x' when package shows trailing numeric tokens but no explicit part in fields
        part_token_base: str
        if self._should_use_placeholder_part(cleaned, packageid):
            part_token_base = "x"
        else:
            part_token_base = str(part_number)

        part_token = f"{part_token_base}{'e' if is_errata else ''}"
        report_id = f"{report_type}{report_number}-{part_token}-{congress}"
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

    def _should_use_placeholder_part(
        self, cleaned: dict[str, Any], packageid: str | None
    ) -> bool:
        """Generalized check for when to emit placeholder part 'x'.

        True when there are no explicit part indicators (heading, partnumber/documentpart, volumenumber)
        and the package id ends with numeric token(s) like "...-4" or "...-4-3".
        """
        try:
            pkg = str(
                packageid or cleaned.get("packageid") or cleaned.get("package_id") or ""
            )
            heading = cleaned.get("heading")
            partnumber = cleaned.get("partnumber") or cleaned.get("documentpart")
            volumenumber = cleaned.get("volumenumber")
            if heading or partnumber or volumenumber:
                return False
            return bool(re.search(r"\d+(?:-\d+)?$", pkg))
        except Exception:
            return False

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
                logger.info(
                    f"Starting congressionalreports_committees streaming with chunk_size={chunk_size}"
                )

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
                SELECT DISTINCT ON (c.package_id, c.authorityid) COUNT(*)
                FROM {self.staging_schema}.congressionalreports_committees AS c
                GROUP BY c.package_id, c.authorityid
                """
                package_count = await conn.fetchval(package_count_query)

                granule_count_query = f"""
                SELECT DISTINCT ON (gc.granule_id, gc.authorityid) COUNT(*)
                FROM {self.staging_schema}.congressionalreports_granules_committees gc
                GROUP BY gc.granule_id, gc.authorityid
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
                    logger.debug(
                        f"Fetching congressionalreports_committees chunk at offset {offset}"
                    )

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
                        cr.documentpart AS partnumber,
                        NULL::text AS heading
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
                        crg.partnumber,
                        crg.heading
                    FROM {self.staging_schema}.congressionalreports_granules_committees AS crgc
                    JOIN {self.staging_schema}.congressionalreports_granules AS crg ON crgc.granule_id = crg.id
                    JOIN {self.staging_schema}.congressionalreports AS cr ON crg.packageid = cr.packageid
                    GROUP BY crg.granuleid, crgc.authorityid, crgc.committeename, cr.documenttype, cr.documentnumber, cr.congress, crg.partnumber, crg.heading

                    ORDER BY granuleid, authorityid, committeename
                    LIMIT {chunk_size} OFFSET {offset}
                    """

                    logger.debug(
                        f"Executing query for congressionalreports_committees at offset {offset}"
                    )
                    rows = await conn.fetch(query)
                    logger.debug(
                        f"Retrieved {len(rows)} rows for congressionalreports_committees at offset {offset}"
                    )

                    if not rows:
                        logger.info(
                            f"No more congressionalreports_committees records at offset {offset}"
                        )
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
                        logger.debug(
                            f"Yielding {len(chunk)} congressionalreports_committees records"
                        )
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
                logger.info(
                    f"Starting congressionalreports_members streaming with chunk_size={chunk_size}"
                )

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
                    logger.debug(
                        f"Fetching congressionalreports_members chunk at offset {offset}"
                    )

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
                        cr.documentpart,
                        crg.heading
                    FROM {self.staging_schema}.congressionalreports_granules_members AS crgm
                    JOIN {self.staging_schema}.congressionalreports_granules_members_name AS crgmn ON crgm.id = crgmn.members_id
                    JOIN {self.staging_schema}.congressionalreports_granules AS crg ON crgm.granule_id = crg.id
                    JOIN {self.staging_schema}.congressionalreports AS cr ON crg.packageid = cr.packageid
                    GROUP BY crg.packageid, crg.granuleid, crgm.bioguideid, crgm.membername, crgm.authorityid, crgm.gpoid, crgmn.authority_fnf, crgmn.authority_other, crg.partnumber, cr.documenttype, cr.documentnumber, cr.congress, cr.documentpart, crg.heading
                    ORDER BY crg.packageid, crg.granuleid NULLS FIRST
                    LIMIT {chunk_size} OFFSET {offset}
                    """

                    logger.debug(
                        f"Executing query for congressionalreports_members at offset {offset}"
                    )
                    rows = await conn.fetch(query)
                    logger.debug(
                        f"Retrieved {len(rows)} rows for congressionalreports_members at offset {offset}"
                    )

                    if not rows:
                        logger.info(
                            f"No more congressionalreports_members records at offset {offset}"
                        )
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
                        logger.debug(
                            f"Yielding {len(chunk)} congressionalreports_members records"
                        )
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
                logger.info(
                    f"Starting congressionalreports streaming with chunk_size={chunk_size}"
                )

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
                    logger.debug(
                        f"Fetching congressionalreports chunk at offset {offset}"
                    )

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
                            c._references AS _references,
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
                            c._references AS _references,
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

                    logger.debug(
                        f"Executing query for congressionalreports at offset {offset}"
                    )
                    rows = await conn.fetch(query)
                    logger.debug(
                        f"Retrieved {len(rows)} rows for congressionalreports at offset {offset}"
                    )

                    if not rows:
                        logger.info(
                            f"No more congressionalreports records at offset {offset}"
                        )
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
                        logger.debug(
                            f"Yielding {len(chunk)} congressionalreports records"
                        )
                        yield chunk

                    offset += chunk_size

                    # Log progress periodically
                    if offset % (chunk_size * 10) == 0:
                        logger.debug(f"Streamed {offset} congressionalreports records")

            except Exception as e:
                logger.error(f"Error streaming congressionalreports: {e}")
                raise

    async def _stream_congressionalreports_reference_bills_joined_chunks(
        self, chunk_size: int
    ) -> AsyncGenerator[list[dict[str, Any]], None]:
        """
        Stream congressional reports bills joined chunks.
        """
        if not self.db_pool:
            raise ValueError("Database pool not configured")

        logger.info(
            f"Streaming congressionalreports_reference_bills records in chunks of {chunk_size}"
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
                logger.debug(
                    f"Fetching congressionalreports_reference_bills chunk at offset {offset}"
                )

                async with self.db_pool.acquire() as conn, conn.transaction():
                    query = f"""
                            SELECT crgrc.*, crg.granuleid, crg.partnumber, crg.heading, cr.packageid, cr.documenttype, cr.documentnumber, cr.congress, cr.documentpart
                            FROM {self.staging_schema}.congressionalreports_granules_references_contents AS crgrc
                            JOIN {self.staging_schema}.congressionalreports_granules_references AS crgr ON crgr.id = crgrc.references_id
                            JOIN {self.staging_schema}.congressionalreports_granules AS crg ON crg.id = crgr.granule_id
                            JOIN {self.staging_schema}.congressionalreports AS cr ON crg.packageid = cr.packageid
                            ORDER BY cr.packageid, crg.granuleid NULLS FIRST
                            LIMIT {chunk_size} OFFSET {offset}
                            """

                    logger.debug(
                        f"Executing query for congressionalreports_reference_bills at offset {offset}"
                    )
                    rows = await conn.fetch(query)
                    logger.debug(
                        f"Retrieved {len(rows)} rows for congressionalreports_reference_bills at offset {offset}"
                    )

                    if not rows:
                        logger.info(
                            f"No more congressionalreports_reference_bills records at offset {offset}"
                        )
                        break

                    chunk = []
                    for row in rows:
                        try:
                            row_dict = dict(row)
                            chunk.append(row_dict)
                        except Exception as e:
                            logger.error(
                                f"Error converting congressionalreports_reference_bills row to dict: {e}, row type: {type(row)}, row: {row}"
                            )
                            continue

                    if chunk:
                        logger.debug(
                            f"Yielding {len(chunk)} congressionalreports_reference_bills records"
                        )
                        yield chunk

                    offset += chunk_size

                    if offset % (chunk_size * 10) == 0:
                        logger.debug(
                            f"Streamed {offset} congressionalreports_reference_bills records"
                        )

        except Exception as e:
            logger.error(f"Error streaming congressionalreports_reference_bills: {e}")
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
        - _references                        text,
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
            _,
            _,
            _,
            _,
            _,
        ) = self._build_report_identity(cleaned)

        # Compute errata flag using shared heuristics
        errata_detected = self._detect_is_errata(cleaned)

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
            "is_errata": errata_detected,
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

    # async def _clean_congressionalreports_serialset_singular(
    #     self, record_data: dict[str, Any]
    # ) -> dict[str, Any]:
    #     """
    #     Custom cleaning logic for individual congressional reports records.
    #     FINAL COLUMNS:
    #     - package_id TEXT PRIMARY KEY,
    #     - bag_id TEXT,
    #     - doc_id TEXT,
    #     - serialset_number TEXT,
    #     - agency TEXT,
    #     - volume TEXT,
    #     - oclc_number TEXT,
    #     - lccn_number TEXT,
    #     - issn_number TEXT,
    #     """

    #     cleaned = record_data.copy()
    #     report_id, _, _, _, _, _, _ = self._build_report_identity(cleaned)

    #     return {
    #         "package_id": cleaned.get("package_id", "ID_ERROR"),
    #         "report_id": report_id,
    #         "bag_id": cleaned.get("serialset_bagid", None),
    #         "doc_id": cleaned.get("serialset_docid", None),
    #         "serialset_number": cleaned.get("serialset_serialsetnumber", None),
    #         "agency": cleaned.get("agency", None),
    #         "volume": cleaned.get("volume", None),
    #         "oclc_number": cleaned.get("otheridentifier_oclc", None),
    #         "lccn_number": cleaned.get("otheridentifier_lccn", None),
    #         "issn_number": cleaned.get("otheridentifier_issn", None),
    #         "last_modified": self.standardize_date(cleaned.get("lastmodified", None)),
    #     }

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

    async def _clean_congressionalreports_reference_bills_singular(
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

        logger.info("Starting post-processing operations")
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
                    FROM {self.staging_schema}.congressionalreports
                """
                rows = await conn.fetch(fetch_sql)
                logger.info(f"Found {len(rows)} records with ils_system_id data")

                # Prepare batch params
                insert_sql = f"""
                    INSERT INTO {self.production_schema}.congressionalreports_ils_system_id (package_id, ils_system_id)
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

        # ? Operation 2: serialset data
        logger.info("Starting serialset post-processing")
        try:
            async with self.db_pool.acquire() as conn, conn.transaction():
                # Fetch all records from staging
                fetch_sql = f"""
                SELECT packageid as package_id, serialset_bagid, serialset_docid, serialset_serialsetnumber, agency, volume, otheridentifier_oclc, otheridentifier_lccn, otheridentifier_issn, lastmodified, documenttype, documentnumber, congress, documentpart FROM {self.staging_schema}.congressionalreports
                WHERE serialset_bagid IS NOT NULL OR serialset_docid IS NOT NULL OR serialset_serialsetnumber IS NOT NULL
                """
                rows = await conn.fetch(fetch_sql)
                logger.info(f"Found {len(rows)} records with serialset data")

                serialset_records = []
                for row in rows:
                    report_id, _, _, _, _, _, _ = self._build_report_identity(row)
                    serialset_records.append(
                        (
                            row["package_id"],
                            report_id,
                            row["serialset_bagid"],
                            row["serialset_docid"],
                            row["serialset_serialsetnumber"],
                            row["agency"],
                            row["volume"],
                            row[
                                "otheridentifier_oclc"
                            ],  # Fixed: use correct column name
                            row[
                                "otheridentifier_lccn"
                            ],  # Fixed: use correct column name
                            row[
                                "otheridentifier_issn"
                            ],  # Fixed: use correct column name
                            self.standardize_date(
                                row["lastmodified"]
                            ),  # Fixed: use correct column name and standardize date
                        )
                    )
                if serialset_records:
                    insert_sql = f"""
                        INSERT INTO {self.production_schema}.congressionalreports_serialset (package_id, report_id, bag_id, doc_id, serialset_number, agency, volume, oclc_number, lccn_number, issn_number, last_modified)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                        ON CONFLICT (package_id) DO NOTHING
                    """
                    chunk_size = 1000
                    serialset_rows_affected = 0
                    for i in range(0, len(serialset_records), chunk_size):
                        chunk = serialset_records[i : i + chunk_size]
                        await conn.executemany(insert_sql, chunk)
                        serialset_rows_affected += len(chunk)
                    logger.info(f"Inserted {serialset_rows_affected} serialset records")
                    results["operations"].append(
                        {
                            "name": "populate_serialset_field",
                            "status": "success",
                            "rows_affected": serialset_rows_affected,
                        }
                    )
                    results["rows_affected"] += serialset_rows_affected
                else:
                    logger.info("No serialset records to insert")
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
        logger.info("Starting reference post-processing")
        try:
            async with self.db_pool.acquire() as conn, conn.transaction():
                # Chunked processing to avoid huge memory and long transactions
                offset = 0
                limit = 5000
                total_counts = {
                    "laws": 0,
                    "statutes": 0,
                    "codes": 0,
                    "statute_pages": 0,
                    "code_sections": 0,
                }

                while True:
                    fetch_sql = f"""
                    SELECT c.packageid AS package_id, cg.collectioncode, cgr.contents
                    FROM {self.staging_schema}.congressionalreports_granules_references_contents AS cgrc
                    JOIN {self.staging_schema}.congressionalreports_granules_references AS cgr ON cgrc.references_id = cgr.id
                    JOIN {self.staging_schema}.congressionalreports_granules AS cg ON cgr.granule_id = cg.id
                    JOIN {self.staging_schema}.congressionalreports AS c ON cg.packageid = c.packageid
                    ORDER BY c.packageid
                    LIMIT {limit} OFFSET {offset}
                    """
                    rows = await conn.fetch(fetch_sql)
                    logger.debug(
                        f"Processing references chunk at offset {offset}: {len(rows)} rows"
                    )

                    if not rows:
                        break

                    law_params = []
                    statute_params = []
                    code_params = []
                    statute_page_params = []
                    code_section_params = []

                    for row in rows:
                        contents = row["contents"]
                        if isinstance(contents, str):
                            try:
                                contents = json.loads(contents)
                            except json.JSONDecodeError:
                                continue
                        if not contents:
                            continue

                        if row["collectioncode"] == "PLAW":
                            for content in contents:
                                law_params.append(
                                    (
                                        row["package_id"],
                                        f"PL{content.get('congress')}-{content.get('number')}",
                                        content.get("label", "").split(" ")[0].lower()
                                        if content.get("label", "")
                                        else None,
                                        f"{content.get('congress')}-{content.get('number')}",
                                        self.safe_int(content.get("number")),
                                        self.safe_int(content.get("congress")),
                                    )
                                )
                        elif row["collectioncode"] == "STATUTE":
                            for content in contents:
                                report_statute_id = hashlib.sha256(
                                    f"{row['package_id']}-{content.get('label', '')}-{content.get('pages', '')}-{content.get('title', '')}".encode()
                                ).hexdigest()[:16]
                                statute_params.append(
                                    (
                                        report_statute_id,
                                        row["package_id"],
                                        f"{content.get('label', '').lower()}{content.get('title', '')}",
                                    )
                                )
                                pages_data = content.get("pages", "[]")
                                if isinstance(pages_data, str):
                                    try:
                                        pages_list = json.loads(pages_data)
                                    except json.JSONDecodeError:
                                        pages_list = [pages_data]
                                else:
                                    pages_list = (
                                        pages_data
                                        if isinstance(pages_data, list)
                                        else []
                                    )
                                for page in pages_list:
                                    statute_page_params.append(
                                        (report_statute_id, page)
                                    )
                        elif row["collectioncode"] == "USCODE":
                            for content in contents:
                                report_code_id = hashlib.sha256(
                                    f"{row['package_id']}-{content.get('sections', '')}-{content.get('title', '')}".encode()
                                ).hexdigest()[:16]
                                code_params.append(
                                    (
                                        report_code_id,
                                        row["package_id"],
                                        f"{content.get('label', '').replace('.', '')}-{content.get('title', '')}",
                                    )
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
                                    code_section_params.append(
                                        (report_code_id, section)
                                    )

                    # Batch insert
                    if law_params:
                        insert_sql = f"""
                            INSERT INTO {self.production_schema}.congressionalreports_reference_laws (package_id, law_id, law_type, law_number, order_number, congress)
                            VALUES ($1, $2, $3, $4, $5, $6)
                            ON CONFLICT DO NOTHING
                        """
                        await conn.executemany(insert_sql, law_params)
                        total_counts["laws"] += len(law_params)
                    if statute_params:
                        insert_sql = f"""
                            INSERT INTO {self.production_schema}.congressionalreports_reference_statutes (report_statute_id, package_id, reference_statute)
                            VALUES ($1, $2, $3)
                            ON CONFLICT DO NOTHING
                        """
                        await conn.executemany(insert_sql, statute_params)
                        total_counts["statutes"] += len(statute_params)
                    if code_params:
                        insert_sql = f"""
                            INSERT INTO {self.production_schema}.congressionalreports_reference_codes (report_code_id, package_id, reference_code)
                            VALUES ($1, $2, $3)
                            ON CONFLICT DO NOTHING
                        """
                        await conn.executemany(insert_sql, code_params)
                        total_counts["codes"] += len(code_params)
                    if statute_page_params:
                        insert_sql = f"""
                            INSERT INTO {self.production_schema}.congressionalreports_reference_statutes_pages (report_statute_id, page)
                            VALUES ($1, $2)
                            ON CONFLICT DO NOTHING
                        """
                        await conn.executemany(insert_sql, statute_page_params)
                        total_counts["statute_pages"] += len(statute_page_params)
                    if code_section_params:
                        insert_sql = f"""
                            INSERT INTO {self.production_schema}.congressionalreports_reference_codes_sections (report_code_id, code_section)
                            VALUES ($1, $2)
                            ON CONFLICT DO NOTHING
                        """
                        await conn.executemany(insert_sql, code_section_params)
                        total_counts["code_sections"] += len(code_section_params)

                    offset += limit

                logger.info(f"References processing completed: {total_counts}")
                results["operations"].append(
                    {
                        "name": "populate_references_field",
                        "status": "success",
                        "rows_affected": sum(total_counts.values()),
                        "details": total_counts,
                    }
                )
                results["rows_affected"] += sum(total_counts.values())
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

        # ? Operation 4: Resolve placeholder 'x' part numbers across all reports
        logger.info("Starting placeholder part resolution post-processing")
        try:
            async with self.db_pool.acquire() as conn, conn.transaction():
                # Fetch all production rows with placeholder 'x' (including errata 'xe')
                prod_rows = await conn.fetch(
                    f"""
                    SELECT package_id, report_id, is_errata
                    FROM {self.production_schema}.congressionalreports
                    WHERE report_id LIKE '%-x-%' OR report_id LIKE '%-xe-%'
                    """
                )

                if not prod_rows:
                    results["operations"].append(
                        {
                            "name": "resolve_placeholder_parts",
                            "status": "skipped",
                            "reason": "no placeholder reports found",
                        }
                    )
                else:
                    logger.info(f"Found {len(prod_rows)} records to process")

                    # Process in chunks to handle large datasets efficiently
                    chunk_size = 10000  # Process 10k records at a time
                    total_chunks = (len(prod_rows) + chunk_size - 1) // chunk_size
                    updated_count = 0

                    for chunk_idx in range(total_chunks):
                        start_idx = chunk_idx * chunk_size
                        end_idx = min(start_idx + chunk_size, len(prod_rows))
                        chunk_rows = prod_rows[start_idx:end_idx]

                        logger.info(
                            f"Processing chunk {chunk_idx + 1}/{total_chunks} ({len(chunk_rows)} records)"
                        )

                        # Group by (type, number, congress) for this chunk
                        groups = {}
                        for row in chunk_rows:
                            m = re.match(
                                r"^([a-z]+)(\d+)-([^-]+)-(\d+)$",
                                str(row["report_id"]),
                                re.IGNORECASE,
                            )
                            if not m:
                                continue
                            key = (m.group(1).lower(), m.group(2), m.group(4))
                            groups.setdefault(key, []).append(dict(row))

                        def parse_order(pkg_id: str) -> tuple[int, int]:
                            try:
                                m = re.search(r"(\d+)(?:-(\d+))?$", pkg_id)
                                if m:
                                    a = int(m.group(1))
                                    b = int(m.group(2)) if m.group(2) else 0
                                    return a, b
                            except Exception:
                                pass
                            return (1 << 30, 1 << 30)

                        # Prepare batch updates for this chunk
                        main_table_updates = []
                        committees_updates = []
                        members_updates = []
                        serialset_updates = []

                        total_groups = len(groups)
                        logger.debug(
                            f"Processing {total_groups} groups in chunk {chunk_idx + 1}"
                        )

                        for group_idx, (_key, rows) in enumerate(groups.items()):
                            if group_idx % 1000 == 0 and total_groups > 1000:
                                logger.debug(
                                    f"Processing group {group_idx}/{total_groups} in chunk {chunk_idx + 1}"
                                )

                            # Order within group by trailing numeric tokens of package_id
                            rows_with_order = []
                            for r in rows:
                                pkg = r.get("package_id") or ""
                                rows_with_order.append((parse_order(str(pkg)), r))
                            rows_with_order.sort(key=lambda x: x[0])

                            # Assign parts sequentially starting at 1
                            for idx, (_ord, r) in enumerate(rows_with_order, start=1):
                                package_id = r["package_id"]
                                old_report_id = r["report_id"]
                                is_errata_flag = (
                                    bool(r["is_errata"])
                                    if r["is_errata"] is not None
                                    else False
                                )

                                m = re.match(
                                    r"^([a-z]+)(\d+)-([^-]+)-(\d+)$",
                                    str(old_report_id),
                                    re.IGNORECASE,
                                )
                                if not m:
                                    continue
                                rtype, rnum, part_comp, cong = (
                                    m.group(1).lower(),
                                    m.group(2),
                                    m.group(3),
                                    m.group(4),
                                )

                                has_errata_suffix = (
                                    part_comp.endswith("e") or is_errata_flag
                                )
                                new_part_token = (
                                    f"{idx}{'e' if has_errata_suffix else ''}"
                                )
                                new_report_id = f"{rtype}{rnum}-{new_part_token}-{cong}"

                                if new_report_id == old_report_id:
                                    continue

                                # Collect updates for batch processing
                                main_table_updates.append((new_report_id, package_id))
                                committees_updates.append((new_report_id, package_id))
                                members_updates.append((new_report_id, package_id))
                                serialset_updates.append((new_report_id, package_id))

                        # Execute batch updates for this chunk
                        if main_table_updates:
                            logger.info(
                                f"Executing batch updates for chunk {chunk_idx + 1}: {len(main_table_updates)} records"
                            )

                            # Update main table in batches
                            update_batch_size = 1000
                            for i in range(
                                0, len(main_table_updates), update_batch_size
                            ):
                                batch = main_table_updates[i : i + update_batch_size]
                                await conn.executemany(
                                    f"""
                                    UPDATE {self.production_schema}.congressionalreports
                                    SET report_id = $1
                                    WHERE package_id = $2
                                    """,
                                    batch,
                                )
                                updated_count += len(batch)

                            # Update dependent tables in batches
                            for i in range(
                                0, len(committees_updates), update_batch_size
                            ):
                                batch = committees_updates[i : i + update_batch_size]
                                await conn.executemany(
                                    f"""
                                    UPDATE {self.production_schema}.congressionalreports_committees
                                    SET report_id = $1
                                    WHERE package_id = $2
                                    """,
                                    batch,
                                )

                            for i in range(0, len(members_updates), update_batch_size):
                                batch = members_updates[i : i + update_batch_size]
                                await conn.executemany(
                                    f"""
                                    UPDATE {self.production_schema}.congressionalreports_members
                                    SET report_id = $1
                                    WHERE package_id = $2
                                    """,
                                    batch,
                                )

                            for i in range(
                                0, len(serialset_updates), update_batch_size
                            ):
                                batch = serialset_updates[i : i + update_batch_size]
                                await conn.executemany(
                                    f"""
                                    UPDATE {self.production_schema}.congressionalreports_serialset
                                    SET report_id = $1
                                    WHERE package_id = $2
                                    """,
                                    batch,
                                )

                    logger.info(
                        f"Completed placeholder part resolution: {updated_count} records updated across {total_chunks} chunks"
                    )
                    results["operations"].append(
                        {
                            "name": "resolve_placeholder_parts",
                            "status": "success",
                            "rows_affected": updated_count,
                        }
                    )
                    results["rows_affected"] += updated_count

        except Exception as e:
            logger.error(f"Error resolving placeholder parts: {e}", exc_info=True)
            results["operations"].append(
                {
                    "name": "resolve_placeholder_parts",
                    "status": "error",
                    "error": str(e),
                }
            )
            results["status"] = "partial_failure"

        return results
