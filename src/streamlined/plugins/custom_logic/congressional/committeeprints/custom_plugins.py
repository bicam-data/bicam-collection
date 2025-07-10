import logging
from typing import Any

# Import the base class that provides shared functionality
from ....base import BaseCleanerLogic, CongressionalBaseFetcherLogic

logger = logging.getLogger(__name__)


class CommitteeprintsFetcherLogic(CongressionalBaseFetcherLogic):
    """
    Committeeprints-specific fetcher logic that extends the base Congressional fetcher logic.

    This class provides custom methods for fetching committeeprints data, including:
    - Extracting standardized committeeprint IDs
    - Handling related data from URLs
    - Implementing specialized fetching logic for committeeprints-specific data

    Inherits shared functionality like get_generic_related_data from
    CongressionalBaseFetcherLogic.
    """

    def __init__(self, data_type: str = "committeeprints"):
        super().__init__(data_type)

    def extract_item_id(self, item_data: dict[str, Any], **kwargs) -> str:
        """Extract standardized committeeprint ID from committeeprint data."""
        item_data = item_data[0] if isinstance(item_data, list) else item_data
        jacket_number = item_data.get("jacketNumber")
        chamber = item_data.get("chamber")
        chamber_prefix = "j" if chamber == "NoChamber" else chamber[0].lower()

        congress = item_data.get("congress")
        if all([jacket_number, chamber, congress]):
            return f"{chamber_prefix}prt{jacket_number}-{congress}"
        else:
            null_values = {
                "jacket_number": jacket_number,
                "chamber": chamber,
                "congress": congress,
            }
            logger.info(
                f"Error extracting item ID: {item_data} with null values: {null_values}"
            )
            return "ID_ERROR"

    def _extract_preliminary_item_id(self, list_item: dict[str, Any]) -> str:
        """
        Derive a stable ID for checkpointing directly from the list-response record.
        This guarantees a non-null value so the progress-tracker can reliably
        decide whether the item was processed in a previous run.
        """
        return self.extract_item_id(list_item)

    # =============================================================================
    # COMMITTEEPRINTS-SPECIFIC RELATED DATA METHODS
    # =============================================================================

    async def get_committeeprints_texts(
        self, full_committeeprint_data: dict[str, Any], client
    ) -> list[dict[str, Any]]:
        """Get committee print texts using the generic helper."""
        return await self.get_generic_related_data(
            full_committeeprint_data, related_table_name="text", client=client
        )


class CommitteeprintsCleanerLogic(BaseCleanerLogic):
    """
    Committeeprints-specific cleaner logic extracted from CommitteeprintsCleaner class.
    Contains all the custom cleaning methods for committeeprints data.
    """

    def __init__(
        self,
        data_type_name: str = "committeeprints",
        system_name: str = "congressional",
        staging_schema: str = "bicam_staging_congressional",
        production_schema: str = "bicam_congressional",
    ):
        super().__init__()
        self.data_type_name = data_type_name
        self.system_name = system_name
        self.staging_schema = staging_schema
        self.production_schema = production_schema
        self.db_pool = None

        # Map data types that span or alias multiple staging tables.
        # In staging, leadership-role data lives in `members_leadership`,
        # but the logical/production data-type name we expose is
        # `members_leadershiproles`.  This mapping lets the base cleaner
        # stream from the correct table without emitting missing-table warnings.
        self.multi_table_data_types = {
            # Add other multi-table or alias mappings here as needed
        }

    async def _clean_committeeprints_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual committeeprints records.

        FINAL COLUMNS:
        - print_id TEXT PRIMARY KEY,
        - print_type TEXT,
        - jacket_number TEXT,
        - title TEXT,
        - chamber TEXT,
        - congress INTEGER,
        - print_number TEXT,
        - citation TEXT,
        - texts_count INTEGER,
        - updated_at TIMESTAMP WITH TIME ZONE

        STAGING COLUMNS:
        - text_url     text,
        - text_count   text,
        - title        text,
        - number       text,
        - chamber      text,
        - batch_id     text,
        - citation     text,
        - congress     text,
        - print_id     text not null
        - updatedate   text,
        - jacketnumber text

        Args:
            record_data: Raw committeeprints record from staging
        Returns:
            Cleaned committeeprints record
        """
        cleaned = record_data.copy()

        filtered_cleaned = {
            "print_id": cleaned.get("print_id", "ID_ERROR"),
            "print_type": cleaned.get("print_id")[:3].upper(),
            "jacket_number": cleaned.get("jacketnumber"),
            "congress": self.safe_int(cleaned.get("congress")),
            "chamber": self.standardize_chamber(cleaned.get("chamber")),
            "title": cleaned.get("title"),
            "print_number": cleaned.get("number"),
            "citation": cleaned.get("citation"),
            "texts_count": self.safe_int(cleaned.get("text_count", 0), 0),
            "updated_at": self.standardize_date(cleaned.get("updatedate")),
        }
        return filtered_cleaned

    async def _clean_committeeprints_texts_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual committees reports records.

        FINAL COLUMNS:
        - print_id TEXT PRIMARY KEY,
        - raw_text TEXT,
        - formatted_text TEXT,
        - pdf TEXT,
        - html TEXT,
        - xml TEXT,

        STAGING COLUMNS:
        - url            text,
        - type           text,
        - batch_id       text,
        - print_id       text,
        - id             text not null
        """

        cleaned = record_data.copy()
        pdf_url = None
        html_url = None
        xml_url = None
        formatted_text = None
        png_url = None

        if cleaned.get("type") == "PDF":
            pdf_url = cleaned.get("url")
        elif cleaned.get("type") == "Generated HTML":
            html_url = cleaned.get("url")
        elif cleaned.get("type") == "Formatted XML":
            xml_url = cleaned.get("url")
        elif cleaned.get("type") == "Formatted Text":
            formatted_text = cleaned.get("url")
        elif cleaned.get("type") == "Portable Network Graphics":
            png_url = cleaned.get("url")
        else:
            raise ValueError(f"Invalid type: {cleaned.get('type')}")

        filtered_cleaned = {
            "print_id": cleaned.get("print_id", "ID_ERROR"),
            "raw_text": None,
            "formatted_text": formatted_text,
            "pdf": pdf_url,
            "html": html_url,
            "xml": xml_url,
            "png": png_url,
        }
        return filtered_cleaned

    async def _clean_committeeprints_associatedbills_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual committees subcommittees records.

        FINAL COLUMNS:
        - print_id TEXT,
        - bill_id TEXT,
        - bill_type TEXT,
        - bill_number TEXT,
        - congress INTEGER,

        STAGING COLUMNS:
        - print_id
        - type
        - number
        - url
        - id
        """
        self._register_target_table_override("committeeprints_bills")
        cleaned = record_data.copy()

        bill_type = cleaned.get("type").lower()
        bill_number = cleaned.get("number")
        if bill_number and "½" in bill_number:
            bill_number = bill_number.replace("½", ".5")

        congress = self.safe_int(cleaned.get("congress"))

        if all([bill_type, bill_number, congress]):
            bill_id = f"{bill_type}{bill_number}-{congress}"
        else:
            bill_id = "ID_ERROR"

        filtered_cleaned = {
            "print_id": cleaned.get("print_id", "ID_ERROR"),
            "bill_id": bill_id,
            "bill_type": bill_type,
            "bill_number": self.safe_int(bill_number, set_to_float=True),
            "congress": congress,
        }
        return filtered_cleaned

    async def _clean_committeeprints_committees_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual committees subcommittees records.

        FINAL COLUMNS:
        - print_id TEXT,
        - committee_code TEXT,
        - committee_name TEXT,

        STAGING COLUMNS:
        - print_id
        - systemCode
        - name
        - url
        - batch_id
        - id
        """
        cleaned = record_data.copy()
        filtered_cleaned = {
            "print_id": cleaned.get("print_id", "ID_ERROR"),
            "committee_code": cleaned.get("systemcode"),
            "committee_name": cleaned.get("name"),
        }
        return filtered_cleaned

    # =============================================================================
    # COMMITTEES-SPECIFIC POST-PROCESSING METHODS
    # =============================================================================

    async def _post_process_committeeprints(self) -> dict[str, Any]:
        """
        Post-processing for committeeprints:
        - add name to committeeprints table
        """
        if not self.db_pool:
            raise ValueError("Database pool not configured")

        results: dict[str, Any] = {
            "status": "success",
            "operations": [],
            "rows_affected": 0,
        }

        async with self.db_pool.acquire() as conn:
            # Operation 1: Update committeeprint type (and subsequently the print_id) based on the type in the text url
            try:
                async with conn.transaction():
                    # Create temporary view with old->new print_id mapping
                    await conn.execute(f"""
                        CREATE TEMPORARY VIEW wprt_id_mapping AS
                        SELECT DISTINCT
                            ct.print_id as old_print_id,
                            'wrpt' || cp.jacket_number || '-' || cp.congress::text as new_print_id
                        FROM {self.production_schema}.committeeprints_texts ct
                        JOIN {self.production_schema}.committeeprints cp ON ct.print_id = cp.print_id
                        WHERE position('wprt' IN lower(ct.formatted_text)) > 0
                            OR position('wprt' IN lower(ct.html)) > 0
                            OR position('wprt' IN lower(ct.xml)) > 0
                            OR position('wprt' IN lower(ct.png)) > 0
                    """)

                    # Update main committeeprints table
                    result1 = await conn.execute(f"""
                        UPDATE {self.production_schema}.committeeprints
                        SET print_id = mapping.new_print_id
                        FROM wprt_id_mapping mapping
                        WHERE committeeprints.print_id = mapping.old_print_id
                    """)

                    # Update related tables with new print_id
                    result2 = await conn.execute(f"""
                        UPDATE {self.production_schema}.committeeprints_bills
                        SET print_id = mapping.new_print_id
                        FROM wprt_id_mapping mapping
                        WHERE committeeprints_bills.print_id = mapping.old_print_id
                    """)

                    # Update print_id for committees
                    result3 = await conn.execute(f"""
                        UPDATE {self.production_schema}.committeeprints_committees
                        SET print_id = mapping.new_print_id
                        FROM wprt_id_mapping mapping
                        WHERE committeeprints_committees.print_id = mapping.old_print_id
                    """)

                    # Update print_id for texts
                    result4 = await conn.execute(f"""
                        UPDATE {self.production_schema}.committeeprints_texts
                        SET print_id = mapping.new_print_id
                        FROM wprt_id_mapping mapping
                        WHERE committeeprints_texts.print_id = mapping.old_print_id
                    """)

                    # Drop the temporary view
                    await conn.execute("DROP VIEW IF EXISTS wprt_id_mapping")

                    # Calculate total rows affected
                    total_rows = 0
                    for result in [result1, result2, result3, result4]:
                        if result and result.split():
                            total_rows += int(result.split()[-1])

                    result = f"UPDATE {total_rows}"
                    rows_affected = int(result.split()[-1]) if result.split() else 0

                    results["operations"].append(
                        {
                            "name": "update_print_id",
                            "status": "success",
                            "rows_affected": rows_affected,
                        }
                    )
                    results["rows_affected"] += rows_affected

                    logger.info(f"Updated print_id for {rows_affected} records")

            except Exception as e:
                logger.error(f"Error updating print_id: {e}")
                results["operations"].append(
                    {
                        "name": "update_print_id",
                        "status": "error",
                        "error": str(e),
                    }
                )
                results["status"] = "partial_failure"

        return results
