"""
Treaty Docs Custom Plugin Logic

This module contains all the custom logic for treaty docs data type including:
- Fetching logic (from fetcher.py)
- Cleaning logic (from cleaner.py)

"""

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

class TreatyDocsCleaner(BaseCleanerLogic):
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
            "bills_texts": ["bills_texts", "bills_texts_formats"],
            # Add other bills multi-table data types here as needed
        }

    async def _clean_treatydocs_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual bills records.
        STAGING COLUMNS:
        - pages                           text,
        - title                           text,
        - branch                          text,
        - chamber                         text,
        - session                         text,
        - category                        text,
        - congress                        text,
        - docclass                        text,
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
        - documentnumber                  text,
        - otheridentifier_migrated_doc_id text,
        - sudocclassnumber                text,
        - governmentauthor1               text,
        - governmentauthor2               text,
        - package_id                      text,
        - processed_at                    text,
        - source_doc_id                   text,
        - agency                          text,
        - volume                          text,
        - download_pdflink                text,
        - parentid                        text,
        - serialset_bagid                 text,
        - serialset_docid                 text,
        - serialset_isglp                 text,
        - serialset_serialsetnumber       text,
        - documentpart                    text,
        - otheridentifier_lccn            text,
        - otheridentifier_oclc            text,
        - otheridentifier_ils_system_id   text,
        - president_id                    text,
        - president_party                 text,
        - dateissuednotspecified          text,
        - otheridentifier_issn
        - subtitle

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

        # drop any rows where docclass != "TDOC"
        if cleaned.get("docclass") != "TDOC":
            return None

        # Convert "CDOC-104tdoc13" to "td104-13"
        # Extract the congress/session number and the tdoc number
        # Example: "CDOC-104tdoc13" -> "td104-13"
        raw_id = cleaned.get("packageid", "")

        match = re.match(r"^[A-Z]+-(\d+)tdoc(\d+)", raw_id, re.IGNORECASE)
        treaty_id = f"td{match.group(1)}-{match.group(2)}" if match else "ID_ERROR"

        # Apply bills-specific cleaning logic
        filtered_cleaned = {
            "package_id": str(cleaned.get("packageid", "ID_ERROR")),
            "treaty_id": str(treaty_id),
            "title": str(cleaned.get("title", None)),
            "congress": self.safe_int(cleaned.get("congress", None)),
            "session": self.safe_int(cleaned.get("session", None)),
            "chamber": str(
                self.standardize_chamber(cleaned.get("chamber", None))
            ),
            "summary": None, # TODO add after post-processing
            "pages": self.safe_int(cleaned.get("pages", None)),
            "issued_at": self.standardize_date(cleaned.get("dateissued", None)),
            "branch": str(cleaned.get("branch", None)),
            "government_author1": str(cleaned.get("governmentauthor1", None)),
            "government_author2": str(cleaned.get("governmentauthor2", None)),
            "publisher": str(cleaned.get("publisher", None)),
            "collection_code": str(cleaned.get("collectioncode", None)),
            "migrated_doc_id": str(cleaned.get("otheridentifier_migrated_doc_id", None)),
            "ils_system_id": str(cleaned.get("otheridentifier_ils_system_id", None)),
            "su_doc_class_number": str(cleaned.get("sudocclassnumber", None)),
            "last_modified": self.standardize_date(cleaned.get("lastmodified", None)),        }

        return filtered_cleaned

    async def _clean_treatydocs_granules_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """Custom cleaning logic for bills actions records
        STAGING COLUMNS:
        title               text,
        - summary             text,
        - category            text,
        - docclass            text, # TODO drop if not TDOC
        - download_pdflink    text,
        - download_txtlink    text,
        - download_ziplink    text,
        - download_modslink   text,
        - download_premislink text,
        - granuleid           text,
        - packageid           text,
        - president_id        text,
        - president_party     text,
        - dateissued          text,
        - detailslink         text,
        - packagelink         text,
        - relatedlink         text,
        - granuleclass        text,
        - granuleslink        text,
        - lastmodified        text,
        - collectioncode      text,
        - collectionname      text,
        - granule_id          text,
        - graphicsinpdf       text,
        - heading             text,
        - partnumber          text
        """
        cleaned = record_data.copy()


        # drop any rows where docclass != "TDOC"
        if cleaned.get("docclass") != "TDOC":
            return None


        filtered_cleaned = {
            "package_id": str(cleaned.get("packageid", "ID_ERROR")),
            "granule_id": str(cleaned.get("granuleid", "ID_ERROR")),
            "treaty_id": None, # TODO add after post-processing
            "formatted_text": str(cleaned.get("download_txtlink", None)),
            "pdf": str(cleaned.get("download_pdflink", None)),
            "has_pdf_graphics": cleaned.get("graphicsinpdf") == 'true' if cleaned.get("graphicsinpdf") else None,
            "heading": str(cleaned.get("heading", None)),
            "part_number": str(cleaned.get("partnumber", None)),
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
