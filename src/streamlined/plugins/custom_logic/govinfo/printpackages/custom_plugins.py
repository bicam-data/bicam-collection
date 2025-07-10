"""
Print Packages Custom Plugin Logic

This module contains all the custom logic for print packages data type including:
- Fetching logic (from fetcher.py)
- Cleaning logic (from cleaner.py)

"""

import logging
import re
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

        # Set printpackages-specific multi-table processing configuration
        self.multi_table_data_types = {
        }

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

        # if "CPRT" isn't in packageid, return None
        if "CPRT" not in cleaned.get("packageid", "") or "JCS" in cleaned.get("packageid", ""):
            return None

        # Convert "CDOC-104tdoc13" to "td104-13"
        # Extract the congress/session number and the tdoc number
        # Example: "CDOC-104tdoc13" -> "td104-13"
        raw_id = cleaned.get("packageid", "")

        # Example: "CPRT-118HPRT48901vI" -> "hprt48901-1-118"
        #          "CPRT-117SPRT12345pA" -> "sprt12345-A-117"
        #          "CPRT-117WPRT12345pIV" -> "wprt12345-4-117"
        def roman_to_int(s):
            roman_numerals = {
                'I': 1, 'II': 2, 'III': 3, 'IV': 4, 'V': 5, 'VI': 6, 'VII': 7, 'VIII': 8, 'IX': 9, 'X': 10,
                'XI': 11, 'XII': 12, 'XIII': 13, 'XIV': 14, 'XV': 15, 'XVI': 16, 'XVII': 17, 'XVIII': 18, 'XIX': 19, 'XX': 20
            }
            return roman_numerals.get(s)

        # Remove leading "GPO-" if present
        norm_id = raw_id
        if norm_id.startswith("GPO-"):
            norm_id = norm_id[4:]

        match = re.match(
            r"^CPRT-(\d{2,3})([JWHSP])PRT((?:\d{5,})|null)([pv])([A-Z]+|\d+)", norm_id, re.IGNORECASE
        )
        if match:
            congress = match.group(1)
            chamber = match.group(2).lower()
            prt_number = match.group(3) or ""
            suffix = match.group(5)
            # Try to convert roman numerals to int, else keep as is
            if suffix.isdigit():
                suffix_part = suffix
            else:
                roman = roman_to_int(suffix.upper())
                suffix_part = str(roman) if roman is not None else suffix.upper()
            print_id = f"{chamber}prt{prt_number}-{suffix_part}-{congress}"
        else:
            print_id = "ID_ERROR"

        # Apply bills-specific cleaning logic
        filtered_cleaned = {
            "package_id": str(print_id),
            "print_id": str(print_id),
            "title": str(cleaned.get("title", None)),
            "congress": self.safe_int(cleaned.get("congress", None)),
            "session": self.safe_int(cleaned.get("session", None)),
            "chamber": self.standardize_chamber(cleaned.get("chamber", None)),
            "pages": self.safe_int(cleaned.get("pages", None)),
            "document_number": str(cleaned.get("documentnumber", None)),
            "issued_at": self.standardize_date(cleaned.get("dateissued", None)),
            "branch": str(cleaned.get("branch", None)),
            "government_author1": str(cleaned.get("governmentauthor1", None)),
            "government_author2": str(cleaned.get("governmentauthor2", None)),
            "publisher": str(cleaned.get("publisher", None)),
            "collection_code": str(cleaned.get("collectioncode", None)),
            "migrated_doc_id": str(cleaned.get("otheridentifier_migrated_doc_id", None)),
            "ils_system_id": str(cleaned.get("otheridentifier_ils_system_id", None)),
            "su_doc_class_number": str(cleaned.get("sudocclassnumber", None)),
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
        if "CPRT" not in cleaned.get("packageid", "") or "JCS" in cleaned.get("packageid", ""):
            return None

        filtered_cleaned = {
            "package_id": str(cleaned.get("packageid", "ID_ERROR")),
            "granule_id": str(cleaned.get("granuleid", "ID_ERROR")),
            "print_id": None, # TODO add after post-processing
            "formatted_text": str(cleaned.get("download_txtlink", None)),
            "pdf": str(cleaned.get("download_pdflink", None)),
            "has_pdf_graphics": cleaned.get("graphicsinpdf") == 'true' if cleaned.get("graphicsinpdf") else None,
            "heading": str(cleaned.get("heading", None)),
            "part_number": str(cleaned.get("partnumber", None)),
            "id": str(cleaned.get("id", "ID_ERROR")),
        }
        return filtered_cleaned

    async def _clean_printpackages_committees_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """Custom cleaning logic for print packages committees records
        STAGING COLUMNS:
        - packageid           text,
        - committeeid         text,
        - committee_name      text,
        """
        cleaned = record_data.copy()

        filtered_cleaned = {
            "package_id": str(cleaned.get("packageid", "ID_ERROR")),
            "committee_id": str(cleaned.get("committeeid", "ID_ERROR")),
            "committee_name": str(cleaned.get("committee_name", "ID_ERROR")),
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

        async with self.db_pool.acquire() as conn:
            # Operation: Populate summary field in treatydocs from staging treaty_docs_granules
            try:
                async with conn.transaction():
                    sql = f"""
                        UPDATE {self.production_schema}.treatydocs td
                        SET summary = sg.summary
                        FROM {self.staging_schema}.treaty_docs_granules sg
                        WHERE td.package_id = sg.packageid
                            AND (td.summary IS NULL OR td.summary = '');
                    """

                    result = await conn.execute(sql)
                    rows_affected = int(result.split()[-1]) if result.split() else 0

                    results["operations"].append(
                        {
                            "name": "populate_summary_field",
                            "status": "success",
                            "rows_affected": rows_affected,
                        }
                    )
                    results["rows_affected"] += rows_affected

                    logger.info(f"Updated summary for {rows_affected} treaty docs")

            except Exception as e:
                logger.error(f"Error populating is_law field: {e}")
                results["operations"].append(
                    {
                        "name": "populate_summary_field",
                        "status": "error",
                        "error": str(e),
                    }
                )
                results["status"] = "partial_failure"

            # Operation: Populate treaty_id field in treatydocs
            try:
                async with conn.transaction():
                    sql = f"""
                        UPDATE {self.production_schema}.treatydocs_granules td
                        SET treaty_id = td.treaty_id
                        FROM {self.production_schema}.treatydocs td
                        WHERE td.package_id = tdg.packageid;
                    """
                    result = await conn.execute(sql)
                    rows_affected = int(result.split()[-1]) if result.split() else 0

                    results["operations"].append(
                        {
                            "name": "populate_treaty_id_field",
                            "status": "success",
                            "rows_affected": rows_affected,
                        }
                    )
                    results["rows_affected"] += rows_affected

                    logger.info(f"Updated treaty_id for {rows_affected} treaty docs granules")

            except Exception as e:
                logger.error(f"Error populating treaty_id field: {e}")

        return results
