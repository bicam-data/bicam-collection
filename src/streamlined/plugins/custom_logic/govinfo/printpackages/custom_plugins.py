"""
Print Packages Custom Plugin Logic

This module contains all the custom logic for print packages data type including:
- Fetching logic (from fetcher.py)
- Cleaning logic (from cleaner.py)

"""

import logging
import re
from collections.abc import AsyncGenerator
from typing import Any

from streamlined.plugins.base import BaseCleanerLogic

logger = logging.getLogger(__name__)


class PrintPackagesFetcher:
    """
    Print Packages fetcherogic that extends the base GovInfo fetcher logic.

    This class provides custom methods for fetching print packages data, including:
    - Extracting standardized print packages IDs
    - Handling related data from URLs
    - Implementing specialized fetching logic for print packages-specific data

    Inherits shared functionality like get_generic_related_data from
    GovInfoBaseFetcher.
    """

    def __init__(self, data_type: str = "printpackages"):
        self.data_type = data_type


class PrintPackagesCleaner(BaseCleanerLogic):
    """
    Treaty Docs cleaner logic extracted from TreatyDocsCleaner class.
    Contains all the custom cleaning methods for treaty docs data.
    """

    def __init__(
        self,
        data_type_name: str = "printpackages",
        system_name: str = "govinfo",
        staging_schema: str = "bicam_staging_govinfo",
        production_schema: str = "bicam_govinfo",
    ):
        self.data_type_name = data_type_name
        self.system_name = system_name
        self.staging_schema = staging_schema
        self.production_schema = production_schema
        self.db_pool = None

        # Set print packages-specific multi-table processing configuration
        self.multi_table_data_types = {
            "printpackages_committees": [
                "printpackages_committees",
                "printpackages_granules_committees",
            ],
            "printpackages_reference_bills": [
                "printpackages_granules_references",
                "printpackages_granules_references_contents",
            ],
            "printpackages": [
                "printpackages",
                "printpackages_granules",
            ],
        }

    # =========================
    # Shared helpers
    # =========================

    def _build_print_identity(
        self, cleaned: dict[str, Any]
    ) -> tuple[str, str | None, str | None, int, str, str, str]:
        """
        Build canonical identity for print package rows (packages or granules).

        Returns tuple:
        - print_id
        - parent_print_id (nullable)
        - granule_id (nullable)
        - part_number (int)
        - print_type (str)
        - print_number (str)
        - congress (str)

        Input must contain: documenttype, documentnumber, congress. Part comes from:
        - For granules: partnumber if present; else parse via parse_part_from_fields
        - For packages: parse via parse_part_from_fields
        Parent part for granules is derived from documentpart or packageid; else 1.
        """
        # Support multiple inbound shapes for ids
        packageid = str(cleaned.get("packageid") or cleaned.get("package_id"))
        granuleid = cleaned.get("granuleid") or cleaned.get("granule_id")

        # Extract from packageid: "CPRT-118HPRT48901vI" -> chamber=h, number=48901, congress=118
        if "CPRT" not in packageid or "JCS" in packageid:
            raise ValueError(f"Invalid print package ID: {packageid}")

        # Remove leading "GPO-" if present
        norm_id = packageid
        if norm_id.startswith("GPO-"):
            norm_id = norm_id[4:]

        match = re.match(
            r"^CPRT-(\d{2,3})([JWHSP])PRT((?:\d{5,})|null)([pv])([A-Z]+|\d+)",
            norm_id,
            re.IGNORECASE,
        )
        if match:
            congress = match.group(1)
            chamber_code = match.group(2).lower()
            prt_number = match.group(3) or ""
            suffix = match.group(5)

            # Convert chamber code to full name
            print_type = f"{chamber_code}prt"
            print_number = prt_number

            # Try to convert roman numerals to int, else keep as is
            if suffix.isdigit():
                part_number = int(suffix)
            else:
                roman_val = self.roman_to_int(suffix.upper())
                part_number = roman_val if roman_val is not None else 1
        else:
            raise ValueError(f"Cannot parse print package ID: {packageid}")

        if granuleid:
            # For granules, determine parent part number
            parent_part_number = self.parse_part_from_fields(cleaned) or 1
            parent_print_id = (
                f"{print_type}{print_number}-{parent_part_number}-{congress}"
            )
        else:
            part_number = self.parse_part_from_fields(cleaned) or part_number
            parent_print_id = None

        print_id = f"{print_type}{print_number}-{part_number}-{congress}"
        granule_id = str(granuleid) if granuleid else None

        return (
            print_id,
            parent_print_id,
            granule_id,
            part_number,
            print_type,
            print_number,
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
        m = re.search(r"(?:vol|pt|v)[-_.]?([ivx]+|\d+)\b", pkg, re.IGNORECASE)
        if m:
            token = m.group(1)
            if token.isdigit():
                return int(token)
            roman_val = self.roman_to_int(token)
            if roman_val is not None:
                return roman_val

        return 1

    async def _stream_printpackages_committees_joined_chunks(
        self, chunk_size: int
    ) -> AsyncGenerator[list[dict[str, Any]], None]:
        """
        Stream joined data from printpackages_committees and printpackages_granules_committees.
        """
        if not self.db_pool:
            raise ValueError("Database pool not configured")

        async with self.db_pool.acquire() as conn:
            try:
                # First, check if both tables exist
                tables_exist_query = """
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_schema = $1
                AND table_name IN ('printpackages_committees', 'printpackages_granules_committees')
                """
                tables_count = await conn.fetchval(
                    tables_exist_query, self.staging_schema
                )

                if tables_count < 2:
                    logger.warning(
                        "One or both printpackages_committees staging tables do not exist"
                    )
                    return

                # Get total count for logging
                package_count_query = f"""
                SELECT COUNT(DISTINCT c.package_id, c.committee_code)
                FROM {self.staging_schema}.printpackages_committees c
                """
                package_count = await conn.fetchval(package_count_query)

                granule_count_query = f"""
                SELECT COUNT(DISTINCT gc.granule_id, gc.committee_code)
                FROM {self.staging_schema}.printpackages_granules_committees gc
                """
                granule_count = await conn.fetchval(granule_count_query)

                total_count = package_count + granule_count

                if total_count == 0:
                    logger.info("No printpackages_committees records found")
                    return

                logger.info(
                    f"Streaming {total_count} printpackages_committees records in chunks of {chunk_size}"
                )

                # Stream using pagination
                offset = 0
                while True:
                    query = f"""
                    SELECT
                        pc.packageid as package_id,
                        NULL as granuleid,
                        pc.committee_code,
                        pc.committee_name,
                        p.documenttype,
                        p.documentnumber,
                        p.congress,
                        p.documentpart AS partnumber
                    FROM {self.staging_schema}.printpackages_committees AS pc
                    JOIN {self.staging_schema}.printpackages AS p ON pc.packageid = p.packageid
                    GROUP BY pc.packageid, pc.committee_code, pc.committee_name, p.documenttype, p.documentnumber, p.congress, p.documentpart

                    UNION ALL

                    SELECT
                        pgc.packageid as package_id,
                        pgc.granuleid,
                        pgc.committee_code,
                        pgc.committee_name,
                        p.documenttype,
                        p.documentnumber,
                        p.congress,
                        pg.partnumber
                    FROM {self.staging_schema}.printpackages_granules_committees AS pgc
                    JOIN {self.staging_schema}.printpackages_granules AS pg ON pgc.granule_id = pg.id
                    JOIN {self.staging_schema}.printpackages AS p ON pg.packageid = p.packageid
                    GROUP BY pgc.packageid, pgc.granuleid, pgc.committee_code, pgc.committee_name, p.documenttype, p.documentnumber, p.congress, pg.partnumber

                    ORDER BY package_id, granuleid, committee_code, committee_name
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
                                f"Error converting printpackages_committees row to dict: {e}, row type: {type(row)}, row: {row}"
                            )
                            continue

                    if chunk:
                        yield chunk

                    offset += chunk_size

                    # Log progress periodically
                    if offset % (chunk_size * 10) == 0:
                        logger.debug(
                            f"Streamed {offset} printpackages_committees records"
                        )

            except Exception as e:
                logger.error(f"Error streaming printpackages_committees: {e}")
                raise

    async def _stream_printpackages_joined_chunks(
        self, chunk_size: int
    ) -> AsyncGenerator[list[dict[str, Any]], None]:
        """
        Stream joined data from printpackages and printpackages_granules.
        """
        if not self.db_pool:
            raise ValueError("Database pool not configured")

        async with self.db_pool.acquire() as conn:
            try:
                # First, check if both tables exist
                tables_exist_query = """
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_schema = $1
                AND table_name IN ('printpackages', 'printpackages_granules')
                """
                tables_count = await conn.fetchval(
                    tables_exist_query, self.staging_schema
                )

                if tables_count < 2:
                    logger.warning(
                        "One or both printpackages staging tables do not exist"
                    )
                    return

                # Get total count for logging
                package_count_query = f"""
                SELECT COUNT(DISTINCT p.packageid)
                FROM {self.staging_schema}.printpackages p
                WHERE p.packageid LIKE '%CPRT%' AND p.packageid NOT LIKE '%JCS%'
                """
                package_count = await conn.fetchval(package_count_query)

                granule_count_query = f"""
                SELECT COUNT(DISTINCT g.granuleid)
                FROM {self.staging_schema}.printpackages_granules g
                JOIN {self.staging_schema}.printpackages p ON g.packageid = p.packageid
                WHERE p.packageid LIKE '%CPRT%' AND p.packageid NOT LIKE '%JCS%'
                """
                granule_count = await conn.fetchval(granule_count_query)

                total_count = package_count + granule_count

                if total_count == 0:
                    logger.info("No valid printpackages records found")
                    return

                logger.info(
                    f"Streaming {total_count} printpackages records in chunks of {chunk_size}"
                )

                # Stream using pagination
                offset = 0
                while True:
                    query = f"""
                    SELECT * FROM (
                        -- Package-only row (always include)
                        SELECT
                            p.packageid AS packageid,
                            NULL::text AS granuleid,
                            p.title AS title,
                            p.branch AS branch,
                            p.chamber AS chamber,
                            p.session AS session,
                            p.category AS category,
                            p.congress AS congress,
                            p.docclass AS docclass,
                            p.download_ziplink AS download_ziplink,
                            p.download_modslink AS download_modslink,
                            p.download_premislink AS download_premislink,
                            p.download_pdflink AS download_pdflink,
                            p.download_txtlink AS download_txtlink,
                            p.detailslink AS detailslink,
                            p.lastmodified AS lastmodified,
                            p.documenttype AS documenttype,
                            p.documentnumber AS documentnumber,
                            p.publisher AS publisher,
                            p.dateissued AS dateissued,
                            p.granuleslink AS granuleslink,
                            p.collectioncode AS collectioncode,
                            p.collectionname AS collectionname,
                            p.pages AS pages,
                            p.otheridentifier_migrated_doc_id AS otheridentifier_migrated_doc_id,
                            p.sudocclassnumber AS sudocclassnumber,
                            p.governmentauthor1 AS governmentauthor1,
                            p.governmentauthor2 AS governmentauthor2,
                            p.processed_at AS processed_at,
                            p.source_doc_id AS source_doc_id,
                            p.documentpart AS documentpart,
                            NULL::text AS heading,
                            NULL::text AS partnumber
                        FROM {self.staging_schema}.printpackages p
                        WHERE p.packageid LIKE '%CPRT%' AND p.packageid NOT LIKE '%JCS%'

                        UNION ALL

                        -- Granule rows
                        SELECT
                            p.packageid AS packageid,
                            g.granuleid AS granuleid,
                            COALESCE(g.title, p.title) AS title,
                            p.branch AS branch,
                            p.chamber AS chamber,
                            p.session AS session,
                            COALESCE(g.category, p.category) AS category,
                            p.congress AS congress,
                            COALESCE(g.docclass, p.docclass) AS docclass,
                            COALESCE(g.download_ziplink, p.download_ziplink) AS download_ziplink,
                            COALESCE(g.download_modslink, p.download_modslink) AS download_modslink,
                            COALESCE(g.download_premislink, p.download_premislink) AS download_premislink,
                            COALESCE(g.download_pdflink, p.download_pdflink) AS download_pdflink,
                            COALESCE(g.download_txtlink, p.download_txtlink) AS download_txtlink,
                            COALESCE(g.detailslink, p.detailslink) AS detailslink,
                            COALESCE(g.lastmodified, p.lastmodified) AS lastmodified,
                            p.documenttype AS documenttype,
                            p.documentnumber AS documentnumber,
                            p.publisher AS publisher,
                            p.dateissued AS dateissued,
                            p.granuleslink AS granuleslink,
                            p.collectioncode AS collectioncode,
                            p.collectionname AS collectionname,
                            p.pages AS pages,
                            p.otheridentifier_migrated_doc_id AS otheridentifier_migrated_doc_id,
                            p.sudocclassnumber AS sudocclassnumber,
                            p.governmentauthor1 AS governmentauthor1,
                            p.governmentauthor2 AS governmentauthor2,
                            p.processed_at AS processed_at,
                            p.source_doc_id AS source_doc_id,
                            COALESCE(p.documentpart, g.partnumber) AS documentpart,
                            g.heading AS heading,
                            g.partnumber AS partnumber
                        FROM {self.staging_schema}.printpackages p
                        JOIN {self.staging_schema}.printpackages_granules g
                            ON p.packageid = g.packageid
                        WHERE p.packageid LIKE '%CPRT%' AND p.packageid NOT LIKE '%JCS%'
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
                                f"Error converting printpackages row to dict: {e}, row type: {type(row)}, row: {row}"
                            )
                            continue

                    if chunk:
                        yield chunk

                    offset += chunk_size

                    # Log progress periodically
                    if offset % (chunk_size * 10) == 0:
                        logger.debug(f"Streamed {offset} printpackages records")

            except Exception as e:
                logger.error(f"Error streaming printpackages: {e}")
                raise

    async def _stream_printpackages_reference_bills_joined_chunks(
        self, chunk_size: int
    ) -> AsyncGenerator[list[dict[str, Any]], None]:
        """
        Stream print packages reference bills joined chunks.
        """
        if not self.db_pool:
            raise ValueError("Database pool not configured")

        logger.info(
            f"Streaming printpackages_reference_bills records in chunks of {chunk_size}"
        )

        # Check if tables exist
        async with self.db_pool.acquire() as conn, conn.transaction():
            tables_exist_query = f"""
                SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '{self.staging_schema}' AND table_name IN ('printpackages_granules_references_contents', 'printpackages_granules_references', 'printpackages_granules', 'printpackages')
            """
            tables_count = await conn.fetchval(tables_exist_query)

            if tables_count < 4:
                logger.warning(
                    f"Missing required tables in {self.staging_schema} schema: "
                    "printpackages_granules_references_contents, "
                    "printpackages_granules_references, "
                    "printpackages_granules, "
                    "printpackages"
                )
                return

        try:
            offset = 0
            while True:
                async with self.db_pool.acquire() as conn, conn.transaction():
                    query = f"""
                            SELECT pgrc.*, pg.granuleid, pg.partnumber, p.packageid, p.documenttype, p.documentnumber, p.congress, p.documentpart FROM {self.staging_schema}.printpackages_granules_references_contents AS pgrc
                            JOIN {self.staging_schema}.printpackages_granules_references AS pgr ON pgr.id = pgrc.references_id
                            JOIN {self.staging_schema}.printpackages_granules AS pg ON pg.id = pgr.granule_id
                            JOIN {self.staging_schema}.printpackages AS p ON pg.packageid = p.packageid
                            WHERE p.packageid LIKE '%CPRT%' AND p.packageid NOT LIKE '%JCS%'
                            ORDER BY p.packageid, pg.granuleid NULLS FIRST
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
                                f"Error converting printpackages_reference_bills row to dict: {e}, row type: {type(row)}, row: {row}"
                            )
                            continue

                    if chunk:
                        yield chunk

                    offset += chunk_size

                    if offset % (chunk_size * 10) == 0:
                        logger.debug(
                            f"Streamed {offset} printpackages_reference_bills records"
                        )

        except Exception as e:
            logger.error(f"Error streaming printpackages_reference_bills: {e}")
            raise

    async def _clean_printpackages_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual print packages records.
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
        - packageid                          text,
        - publisher                          text,
        - dateissued                         text,
        - detailslink                        text,
        - documenttype                       text,
        - granuleslink                       text,
        - lastmodified                       text,
        - collectioncode                     text,
        - collectionname                     text,
        - otheridentifier_migrated_doc_id    text,
        - sudocclassnumber                   text,
        - governmentauthor1                  text,
        - governmentauthor2                  text,
        - package_id                         text,
        - processed_at                       text,
        - source_doc_id                      text,
        - documentnumber                     text,
        - otheridentifier_ils_system_id      text,
        - otheridentifier_isbn               text,
        - otheridentifier_sudoc_class_number text,
        - md5                                text,
        - fields                             text,
        - download_pdflink                   text,
        - committees                         text,
        - federalpublicationname             text,
        - download_xlslink                   text,
        - download_videolink                 text,
        - download_txtlink                   text,
        - download_thumbnailjpeg             text,
        - download_mp3link                   text,
        - download_jpeglink                  text,
        - _references                        text,
        - subtitle                           text,
        - otheridentifier_stock_number       text

        FINAL COLUMNS:
        - package_id TEXT PRIMARY KEY,
        - print_id TEXT,
        - title TEXT,
        - chamber TEXT, -- lower
        - congress INTEGER,
        - session INTEGER,
        - pages INTEGER,
        - document_number TEXT,
        - issued_at DATE,
        - branch TEXT,
        - government_author1 TEXT,
        - government_author2 TEXT,
        - publisher TEXT,
        - collection_code TEXT,
        - migrated_doc_id TEXT,
        - su_doc_class_number TEXT,
        - last_modified TIMESTAMP WITH TIME ZONE
        """
        cleaned = record_data.copy()

        # Skip if not a valid print package
        if "CPRT" not in cleaned.get("packageid", "") or "JCS" in cleaned.get(
            "packageid", ""
        ):
            return None

        try:
            (
                print_id,
                parent_print_id,
                granule_id,
                part_number,
                print_type,
                print_number,
                congress,
            ) = self._build_print_identity(cleaned)
        except ValueError as e:
            logger.warning(f"Failed to build print identity: {e}")
            return None

        # Apply print packages-specific cleaning logic
        filtered_cleaned = {
            "package_id": str(cleaned.get("packageid", "ID_ERROR")),
            "print_id": str(print_id),
            "granule_id": str(cleaned.get("granuleid"))
            if cleaned.get("granuleid")
            else None,
            "parent_print_id": parent_print_id,
            "title": cleaned.get("title", None),
            "chamber": self.standardize_chamber(cleaned.get("chamber", None)),
            "congress": self.safe_int(cleaned.get("congress", None)),
            "session": self.safe_int(cleaned.get("session", None)),
            "pages": self.safe_int(cleaned.get("pages", None)),
            "document_number": cleaned.get("documentnumber", None),
            "issued_at": self.standardize_date(cleaned.get("dateissued", None)),
            "branch": cleaned.get("branch", None),
            "government_author1": cleaned.get("governmentauthor1", None),
            "government_author2": cleaned.get("governmentauthor2", None),
            "publisher": cleaned.get("publisher", None),
            "collection_code": cleaned.get("collectioncode", None),
            "migrated_doc_id": cleaned.get("otheridentifier_migrated_doc_id", None),
            "su_doc_class_number": cleaned.get("sudocclassnumber", None),
            "last_modified": self.standardize_date(cleaned.get("lastmodified", None)),
        }

        return filtered_cleaned

    # TODO: printpackages granules references are different than package references

    # TODO: printpackages committees references are different than package references

    # TODO: ignore agencies

    # TODO: clean committee names

    async def _clean_printpackages_granules_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """Custom cleaning logic for bills actions records
        STAGING COLUMNS:
        - title               text,
        - category            text,
        - docclass            text,
        - download_pdflink    text,
        - download_ziplink    text,
        - download_modslink   text,
        - download_premislink text,
        - granuleid           text,
        - packageid           text,
        - dateissued          text,
        - detailslink         text,
        - packagelink         text,
        - relatedlink         text,
        - granuleclass        text,
        - granuleslink        text,
        - lastmodified        text,
        - graphicsinpdf       text,
        - collectioncode      text,
        - collectionname      text,
        - granule_id          text,
        - id                  text,
        - processed_at        text,
        - source_doc_id       text
        - download_txtlink    text,
        - heading             text,
        - download_xmllink    text,
        - agencies            text,
        - download_xlslink    text
        """
        cleaned = record_data.copy()

        # if 'CPRT' isn't in packageid or if "JCS" in packageid, return None
        if "CPRT" not in cleaned.get("packageid", "") or "JCS" in cleaned.get(
            "packageid", ""
        ):
            return None

        filtered_cleaned = {
            "package_id": str(cleaned.get("packageid", "ID_ERROR")),
            "granule_id": str(cleaned.get("granuleid", "ID_ERROR")),
            "print_id": None,  # TODO add after post-processing
            "formatted_text": str(cleaned.get("download_txtlink", None)),
            "pdf": str(cleaned.get("download_pdflink", None)),
            "has_pdf_graphics": cleaned.get("graphicsinpdf") == "true"
            if cleaned.get("graphicsinpdf")
            else None,
            "heading": str(cleaned.get("heading", None)),
            "part_number": str(cleaned.get("partnumber", None)),
            "id": str(cleaned.get("id", "ID_ERROR")),
        }
        return filtered_cleaned

    async def _clean_printpackages_committees_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual print packages committees records.
        """
        cleaned = record_data.copy()
        print_id, _, granule_id, _, _, _, _ = self._build_print_identity(cleaned)

        filtered_cleaned = {
            "package_id": cleaned.get("package_id", "ID_ERROR"),
            "granule_id": granule_id,
            "print_id": print_id,
            "committee_code": cleaned.get("committee_code", "ID_ERROR"),
            "committee_name": cleaned.get("committee_name", None),
        }
        return filtered_cleaned

    async def _clean_printpackages_reference_bills_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual print packages reference bills records.
        """
        logger.info(f"Cleaning printpackages_reference_bills: {record_data}")

        cleaned = record_data.copy()
        print_id, _, granule_id, _, _, _, _ = self._build_print_identity(cleaned)

        bill_type = cleaned.get("type", "").lower()
        bill_number = cleaned.get("number", "")
        bill_congress = cleaned.get("congress", "")

        if all([bill_type, bill_number, bill_congress]):
            bill_id = f"{bill_type}{bill_number}-{bill_congress}"
        else:
            bill_id = None

        filtered_cleaned = {
            "package_id": cleaned.get("packageid", "ID_ERROR"),
            "granule_id": granule_id,
            "print_id": print_id,
            "bill_id": bill_id,
            "bill_type": bill_type,
            "bill_number": bill_number,
            "congress": bill_congress,
        }
        return filtered_cleaned

    # Add post-processing methods
    async def _post_process_printpackages(self) -> dict[str, Any]:
        """Post-processing operations specific to print packages data."""
        if not self.db_pool:
            raise ValueError("Database pool not configured")

        results = {
            "status": "success",
            "operations": [],
            "rows_affected": 0,
        }

        # Operation: Update granules with print_id from main table
        try:
            async with self.db_pool.acquire() as conn, conn.transaction():
                sql = f"""
                UPDATE {self.production_schema}.printpackages_granules pg
                SET print_id = p.print_id
                FROM {self.production_schema}.printpackages p
                WHERE p.package_id = pg.package_id;
                """

                result = await conn.execute(sql)
                rows_affected = int(result.split()[-1]) if result.split() else 0

                results["operations"].append(
                    {
                        "name": "populate_granule_print_id_field",
                        "status": "success",
                        "rows_affected": rows_affected,
                    }
                )
                results["rows_affected"] += rows_affected

                logger.info(
                    f"Updated print_id for {rows_affected} print packages granules"
                )

        except Exception as e:
            logger.error(f"Error populating granule print_id field: {e}", exc_info=True)
            results["operations"].append(
                {
                    "name": "populate_granule_print_id_field",
                    "status": "error",
                    "error": str(e),
                }
            )
            results["status"] = "partial_failure"

        return results
