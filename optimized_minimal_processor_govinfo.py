import hashlib
import io
import json
import logging
import os
import re
from collections import Counter
from datetime import datetime
from functools import lru_cache
from html import unescape
from typing import Any

import psycopg2
from dateutil.parser import parse
from dotenv import load_dotenv
from pytz import UTC

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class OptimizedUtils:
    """Optimized utility functions with caching and batch operations."""

    @staticmethod
    @lru_cache(maxsize=10000)
    def standardize_date_cached(date_str: str) -> datetime | None:
        """Cached version of date standardization for repeated date strings."""
        if not date_str or date_str in ["", "null", "None"]:
            return None

        try:
            parsed_date = parse(date_str)
            if parsed_date.tzinfo is None:
                return parsed_date.replace(tzinfo=UTC)
            return parsed_date
        except Exception:
            return None

    @staticmethod
    def standardize_date(date_value: Any) -> datetime | None:
        """Standardize date values to consistent timezone-aware datetime objects."""
        if not date_value or date_value in ["", "null", "None"]:
            return None

        if isinstance(date_value, datetime):
            if date_value.tzinfo is None:
                return date_value.replace(tzinfo=UTC)
            return date_value

        if isinstance(date_value, str):
            return OptimizedUtils.standardize_date_cached(date_value)

        return None

    @staticmethod
    @lru_cache(maxsize=1000)
    def standardize_chamber_cached(chamber: str) -> str | None:
        """Cached version of chamber standardization."""
        if not chamber:
            return None

        chamber_lower = chamber.lower().strip()

        if chamber_lower in ["house", "h", "house of representatives"]:
            return "house"
        elif chamber_lower in ["senate", "s"]:
            return "senate"
        elif chamber_lower in ["joint", "both"]:
            return "joint"
        elif chamber_lower in ["nochamber", "no chamber"]:
            return "nochamber"
        else:
            return chamber

    @staticmethod
    def standardize_chamber(chamber: str | None) -> str | None:
        """Standardize chamber values to consistent format."""
        if not chamber:
            return None
        return OptimizedUtils.standardize_chamber_cached(chamber)

    @staticmethod
    def clean_long_text(text: str | None) -> str | None:
        """Clean and normalize long text fields, including HTML content."""
        if not text:
            return None

        # Store original for fallback
        original_text = text

        try:
            # First, handle HTML content if present
            if "<" in text and ">" in text:
                # Remove DOCTYPE declarations and XML namespaces
                cleaned = re.sub(r"<!DOCTYPE[^>]*>", "", text, flags=re.IGNORECASE)
                cleaned = re.sub(r"<\?xml[^>]*\?>", "", cleaned, flags=re.IGNORECASE)

                # Convert HTML paragraph and line break tags to newlines for structure preservation
                cleaned = re.sub(r"</p>", "\n\n", cleaned, flags=re.IGNORECASE)
                cleaned = re.sub(r"<br\s*/?>", "\n", cleaned, flags=re.IGNORECASE)
                cleaned = re.sub(r"</div>", "\n", cleaned, flags=re.IGNORECASE)
                cleaned = re.sub(r"</section>", "\n\n", cleaned, flags=re.IGNORECASE)

                # Remove all remaining HTML tags
                cleaned = re.sub(r"<[^>]+>", "", cleaned)

                # Decode HTML entities (like &nbsp;, &lt;, &gt;, etc.)
                cleaned = unescape(cleaned)
            else:
                cleaned = text

            # Remove common artifacts
            cleaned = cleaned.replace("\x00", "")  # Null bytes
            cleaned = cleaned.replace("\ufffd", "")  # Unicode replacement character
            cleaned = cleaned.replace("\u00ad", "")  # Soft hyphens

            # Normalize line endings
            cleaned = cleaned.replace("\r\n", "\n")
            cleaned = cleaned.replace("\r", "\n")

            # Clean up excessive whitespace while preserving paragraph structure
            # First normalize multiple newlines (preserve double newlines for paragraphs)
            cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)

            # Remove spaces at the beginning/end of lines
            cleaned = re.sub(r"[ \t]+\n", "\n", cleaned)
            cleaned = re.sub(r"\n[ \t]+", "\n", cleaned)

            # Replace multiple spaces/tabs with single space
            cleaned = re.sub(r"[ \t]{2,}", " ", cleaned)

            # Clean up any remaining excessive whitespace
            cleaned = cleaned.strip()

            # Remove common legislative document artifacts
            cleaned = re.sub(r"\[\[Page\s+[^\]]+\]\]", "", cleaned)  # Page markers
            cleaned = re.sub(
                r"¿+", "", cleaned
            )  # Strange characters sometimes in congressional docs

            # Final whitespace cleanup
            cleaned = re.sub(
                r"\n\s*\n\s*\n", "\n\n", cleaned
            )  # Normalize paragraph breaks

            return cleaned if cleaned else None

        except Exception as e:
            # If cleaning fails for any reason, fall back to basic cleaning
            logger.warning(f"Advanced text cleaning failed, using basic cleaning: {e}")
            basic_cleaned = re.sub(r"\s+", " ", original_text.strip())
            basic_cleaned = basic_cleaned.replace("\x00", "")
            return basic_cleaned if basic_cleaned else None

    @staticmethod
    def safe_int(value: Any, default: int | None = None) -> int | float | None:
        """Safely convert value to integer."""
        if value is None or value == "":
            return default

        try:
            if isinstance(value, str):
                # Remove common non-numeric characters
                cleaned = re.sub(r"[^\d-]", "", value)
                if cleaned:
                    return int(cleaned)
            else:
                return int(value)
        except (ValueError, TypeError):
            pass

        return default

    @staticmethod
    def safe_float(value: Any, default: float | None = None) -> float | None:
        """Safely convert value to float."""
        if value is None or value == "":
            return default

        try:
            if isinstance(value, str):
                # Remove common non-numeric characters except decimal point
                cleaned = re.sub(r"[^\d.-]", "", value)
                if cleaned:
                    return float(cleaned)
            else:
                return float(value)
        except (ValueError, TypeError):
            pass

        return default

    @staticmethod
    def sanitize_for_copy(text: str | None) -> str | None:
        """Sanitize text specifically for PostgreSQL COPY operations."""
        if not text:
            return None

        if not isinstance(text, str):
            return str(text)

        # Remove null bytes and other control characters
        cleaned = text.replace("\x00", "")

        # Replace newlines, carriage returns, and tabs with spaces
        cleaned = cleaned.replace("\n", " ").replace("\r", " ").replace("\t", " ")

        # Remove other control characters that could break COPY
        cleaned = "".join(
            char for char in cleaned if ord(char) >= 32 or char in "\t\n\r"
        )

        # Escape backslashes (they need to be doubled in COPY format)
        cleaned = cleaned.replace("\\", "\\\\")

        # Remove any remaining problematic characters
        cleaned = cleaned.strip()

        return cleaned if cleaned else None

    @staticmethod
    def batch_copy_dicts_to_table(
        cursor,
        schema: str,
        table: str,
        columns: list[str],
        dicts: list[dict],
        batch_size: int = 1000,
    ):
        """Optimized batch copy operation with progress tracking."""
        if not dicts:
            return

        total_records = len(dicts)
        logger.info(
            f"Copying {total_records} records to {schema}.{table} in batches of {batch_size}"
        )

        for i in range(0, total_records, batch_size):
            batch = dicts[i : i + batch_size]
            success = False
            retry_count = 0
            max_retries = 3

            while not success and retry_count < max_retries:
                try:
                    output = io.StringIO()

                    for row_idx, row in enumerate(batch):
                        vals = [row.get(col) for col in columns]

                        # Convert booleans to 't'/'f' for PostgreSQL, None to \N
                        def pg_val(v):
                            if v is None:
                                return r"\N"
                            if isinstance(v, bool):
                                return "t" if v else "f"
                            if isinstance(v, int | float):
                                return str(v)
                            if isinstance(v, datetime):
                                # Format datetime in PostgreSQL-compatible format
                                if v.tzinfo:
                                    # Convert to UTC and format as PostgreSQL expects
                                    utc_dt = v.astimezone(UTC)
                                    return utc_dt.strftime("%Y-%m-%d %H:%M:%S+00")
                                else:
                                    return v.strftime("%Y-%m-%d %H:%M:%S")
                            # Clean string values to remove all problematic characters for COPY
                            if isinstance(v, str):
                                cleaned = OptimizedUtils.sanitize_for_copy(v)
                                return cleaned if cleaned else r"\N"
                            return str(v)

                        # Ensure we have exactly the right number of values
                        if len(vals) != len(columns):
                            logger.error(
                                f"Row {row_idx} has {len(vals)} values but expected {len(columns)} columns"
                            )
                            logger.error(f"Columns: {columns}")
                            logger.error(f"Values: {vals}")
                            logger.error(f"Row: {row}")
                            raise ValueError(
                                f"Column count mismatch: {len(vals)} vs {len(columns)}"
                            )

                        line = "\t".join(pg_val(v) for v in vals) + "\n"
                        output.write(line)

                    output.seek(0)
                    sql = f"COPY {schema}.{table} ({', '.join(columns)}) FROM STDIN WITH (FORMAT text, NULL '\\N')"
                    cursor.copy_expert(sql, output)
                    success = True

                except Exception as e:
                    retry_count += 1
                    logger.error(
                        f"Error copying batch {i // batch_size + 1}/{(total_records + batch_size - 1) // batch_size} ({len(batch)} records) - attempt {retry_count}: {e}"
                    )

                    if retry_count >= max_retries:
                        # Log the problematic batch for debugging
                        with open("error_batch.txt", "w") as f:
                            f.write(f"Error: {e}\n")
                            f.write(f"SQL: {sql}\n")
                            f.write(f"Batch size: {len(batch)}\n")
                            f.write("First few rows of problematic batch:\n")
                            for idx, row in enumerate(batch[:5]):
                                f.write(f"Row {idx}: {row}\n")
                            f.write(
                                f"\nOutput content (first 1000 chars):\n{output.getvalue()[:1000]}\n"
                            )

                        # Try to identify the problematic row by processing one by one
                        logger.info("Attempting to identify problematic row...")
                        problematic_rows = []
                        for row_idx, row in enumerate(batch):
                            try:
                                vals = [row.get(col) for col in columns]
                                placeholders = ", ".join(["%s"] * len(columns))
                                insert_sql = f"INSERT INTO {schema}.{table} ({', '.join(columns)}) VALUES ({placeholders})"
                                cursor.execute(insert_sql, vals)
                            except Exception as row_error:
                                logger.error(f"Problematic row {row_idx}: {row_error}")
                                logger.error(f"Row data: {row}")
                                logger.error(f"Values: {vals}")
                                problematic_rows.append((row_idx, row, row_error))

                        if problematic_rows:
                            logger.error(
                                f"Found {len(problematic_rows)} problematic rows in batch"
                            )
                            # Continue with the rest of the batch instead of failing completely
                            logger.info("Continuing with remaining data...")
                        else:
                            logger.info(
                                "All rows in batch processed successfully via INSERT fallback"
                            )
                    else:
                        # Try with smaller batch size on retry
                        if len(batch) > 100:
                            logger.info("Retrying with smaller batch size...")
                            # Split the batch in half for retry
                            mid = len(batch) // 2
                            first_half = batch[:mid]
                            second_half = batch[mid:]

                            # Process first half
                            OptimizedUtils.batch_copy_dicts_to_table(
                                cursor,
                                schema,
                                table,
                                columns,
                                first_half,
                                batch_size=min(100, len(first_half)),
                            )
                            # Process second half
                            OptimizedUtils.batch_copy_dicts_to_table(
                                cursor,
                                schema,
                                table,
                                columns,
                                second_half,
                                batch_size=min(100, len(second_half)),
                            )
                            success = True


class OptimizedDataProcessor:
    """Optimized data processor with efficient data structures and batch operations."""

    def __init__(self):
        self.utils = OptimizedUtils()
        self._id_cache = {}  # Cache for generated IDs

    def get_data_from_raw_table(
        self,
        data_type: str,
        table_name: str,
        cursor,
        related: bool = False,
        schema: str = "bicam_raw_govinfo",
    ) -> list[dict]:
        """Optimized data retrieval with better query structure."""
        if related:
            data_type = data_type.split("_")[0]

        query = f"""
            SELECT {schema}.{table_name}.payload
            FROM {schema}.{table_name}
            WHERE {schema}.{table_name}.payload->>'lastModified' > (
                select last_processed_date::text
                from __metadata._govinfo_last_processed_dates
                where data_type = '{data_type}'
            )
        """

        cursor.execute(query)
        results = cursor.fetchall()
        data = [result[0] for result in results]
        logger.info(f"Found {len(data)} rows in {schema}.{table_name}")
        return data

    def check_duplicates_optimized(
        self, data: list[dict], id_keys: list[str], data_type: str
    ) -> list[dict]:
        """Optimized duplicate removal using sets and Counter."""
        if isinstance(id_keys, str):
            id_keys = [id_keys]

        # Build a tuple key for each item
        def get_key(item):
            return tuple(item.get(k) for k in id_keys)

        # Use Counter for efficient duplicate detection
        key_counts = Counter(get_key(item) for item in data)
        duplicates = [item for item in data if key_counts[get_key(item)] > 1]

        if duplicates:
            logger.error(
                f"{len(duplicates)} duplicates found in {data_type} based on keys {id_keys}"
            )

        # Only keep items whose key appears once
        data = [item for item in data if key_counts[get_key(item)] == 1]
        return data

    # ============================================================================
    # GOVINFO PROCESSING METHODS
    # ============================================================================

    def process_govinfo_bills_optimized(self, data: list[dict], cursor) -> None:
        """Optimized GovInfo bills processing."""
        logger.info(f"Processing {len(data)} GovInfo bills")

        bills = []
        bills_reference_codes = []
        bills_reference_codes_sections = []
        bills_reference_laws = []
        bills_reference_statutes = []
        bills_reference_statutes_pages = []
        bills_short_titles = []

        # convert package id into bill id
        for item in data:
            package_id = item.get("packageId")
            bill_id = (
                f"{item.get('billType')}{item.get('number')}-{item.get('congress')}"
            )
            # Main bill record
            bill_record = {
                "package_id": package_id,
                "bill_id": bill_id,
                "bill_version": item.get("billVersion"),
                "origin_chamber": self.utils.standardize_chamber(
                    item.get("originChamber")
                ),
                "current_chamber": self.utils.standardize_chamber(
                    item.get("currentChamber")
                ),
                "is_appropriation": item.get("isAppropriation"),
                "is_private": item.get("isPrivate"),
                "pages": self.utils.safe_int(item.get("pages")),
                "issued_at": self.utils.standardize_date(item.get("dateIssued")),
                "government_author1": item.get("governmentAuthor1"),
                "government_author2": item.get("governmentAuthor2"),
                "publisher": item.get("publisher"),
                "collection_code": item.get("collectionCode"),
                "stock_number": item.get("otherIdentifier", {}).get("stockNumber"),
                "su_doc_class_number": item.get("otherIdentifier", {}).get(
                    "suDocClassNumber"
                ),
                "migrated_doc_id": item.get("otherIdentifier", {}).get(
                    "migrated-doc-id"
                ),
                "child_ils_system_id": item.get("otherIdentifier", {}).get(
                    "child-ils-system-id"
                ),
                "parent_ils_system_id": item.get("otherIdentifier", {}).get(
                    "parent-ils-system-id"
                ),
                "mods_url": item.get("download", {}).get("modsUrl"),
                "pdf_url": item.get("download", {}).get("pdfUrl"),
                "premis_url": item.get("download", {}).get("premisUrl"),
                "txt_url": item.get("download", {}).get("txtUrl"),
                "xml_url": item.get("download", {}).get("xmlUrl"),
                "zip_url": item.get("download", {}).get("zipUrl"),
                "last_modified": self.utils.standardize_date(item.get("lastModified")),
            }
            bills.append(bill_record)

            # Reference codes
            if item.get("references"):
                for ref in item.get("references"):
                    if ref.get("collectionCode") == "USCODE":
                        for ref_code in ref.get("contents", []):
                            bill_code_id = hashlib.sha256(
                                json.dumps(ref_code).encode()
                            ).hexdigest()[:16]
                            bills_reference_codes.append(
                                {
                                    "bill_code_id": bill_code_id,
                                    "package_id": package_id,
                                    "reference_code": ref_code.get("label", "").replace(
                                        ".", ""
                                    )
                                    + "-"
                                    + ref_code.get("title"),
                                }
                            )
                            for section in ref_code.get("sections", []):
                                bills_reference_codes_sections.append(
                                    {
                                        "bill_code_id": bill_code_id,
                                        "code_section": section,
                                    }
                                )
                if ref.get("collectionCode") == "STATUTE":
                    for ref_statute in ref.get("contents", []):
                        bill_statute_id = hashlib.sha256(
                            json.dumps(ref_statute).encode()
                        ).hexdigest()[:16]
                        bills_reference_statutes.append(
                            {
                                "bill_statute_id": bill_statute_id,
                                "package_id": package_id,
                                "reference_statute": ref_statute.get(
                                    "label", ""
                                ).lower()
                                + ref_statute.get("title", ""),
                            }
                        )
                        for page in ref_statute.get("pages", []):
                            bills_reference_statutes_pages.append(
                                {
                                    "bill_statute_id": bill_statute_id,
                                    "page": page,
                                }
                            )
                if ref.get("collectionCode") == "PLAW":
                    for ref_law in ref.get("contents", []):
                        bills_reference_laws.append(
                            {
                                "package_id": package_id,
                                "law_id": f"PL{ref_law.get('congress')}-{ref_law.get('number')}",
                                "law_type": ref_law.get("label"),
                            }
                        )

            # Short titles
            for short_title in item.get("shortTitles", []):
                bills_short_titles.append(
                    {
                        "package_id": package_id,
                        "short_title": short_title.get("title"),
                        "level": short_title.get("level"),
                        "type": short_title.get("type"),
                    }
                )

        # Remove duplicates
        bills = self.check_duplicates_optimized(bills, "package_id", "billcollections")
        bills_reference_codes = self.check_duplicates_optimized(
            bills_reference_codes, "bill_code_id", "bills_reference_codes"
        )
        bills_reference_codes_sections = self.check_duplicates_optimized(
            bills_reference_codes_sections,
            ["bill_code_id", "code_section"],
            "bills_reference_codes_sections",
        )
        bills_reference_laws = self.check_duplicates_optimized(
            bills_reference_laws,
            ["package_id", "law_id"],
            "bills_reference_laws",
        )
        bills_reference_statutes = self.check_duplicates_optimized(
            bills_reference_statutes,
            "bill_statute_id",
            "bills_reference_statutes",
        )
        bills_reference_statutes_pages = self.check_duplicates_optimized(
            bills_reference_statutes_pages,
            ["bill_statute_id", "page"],
            "bills_reference_statutes_pages",
        )
        bills_short_titles = self.check_duplicates_optimized(
            bills_short_titles,
            ["package_id", "short_title", "level", "type"],
            "bills_short_titles",
        )

        # Batch insert
        if bills:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_govinfo",
                "bills",
                [
                    "package_id",
                    "bill_id",
                    "bill_version",
                    "origin_chamber",
                    "current_chamber",
                    "is_appropriation",
                    "is_private",
                    "pages",
                    "issued_at",
                    "government_author1",
                    "government_author2",
                    "publisher",
                    "collection_code",
                    "stock_number",
                    "su_doc_class_number",
                    "migrated_doc_id",
                    "child_ils_system_id",
                    "parent_ils_system_id",
                    "mods_url",
                    "pdf_url",
                    "premis_url",
                    "txt_url",
                    "xml_url",
                    "zip_url",
                    "last_modified",
                ],
                bills,
            )

        if bills_reference_codes:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_govinfo",
                "bills_reference_codes",
                ["bill_code_id", "package_id", "reference_code"],
                bills_reference_codes,
            )

        if bills_reference_codes_sections:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_govinfo",
                "bills_reference_codes_sections",
                ["bill_code_id", "code_section"],
                bills_reference_codes_sections,
            )

        if bills_reference_laws:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_govinfo",
                "bills_reference_laws",
                ["package_id", "law_id", "law_type"],
                bills_reference_laws,
            )

        if bills_reference_statutes:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_govinfo",
                "bills_reference_statutes",
                ["bill_statute_id", "package_id", "reference_statute"],
                bills_reference_statutes,
            )

        if bills_reference_statutes_pages:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_govinfo",
                "bills_reference_statutes_pages",
                ["bill_statute_id", "page"],
                bills_reference_statutes_pages,
            )

        if bills_short_titles:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_govinfo",
                "bills_short_titles",
                ["package_id", "short_title", "level", "type"],
                bills_short_titles,
            )

        logger.info(f"Processed {len(bills)} GovInfo bills with all related data")

    def process_govinfo_committeeprints_optimized(
        self, data: list[dict], cursor
    ) -> None:
        """Optimized GovInfo committee prints processing."""
        logger.info(f"Processing {len(data)} GovInfo committeeprints")

        committeeprints = []

        for item in data:
            package_id = item.get("packageId")
            print_id = None
            if package_id and package_id.startswith("CPRT-"):
                # Extract the alpha part (possibly with digits or 'null')
                match_alpha = re.search(r"([A-Za-z]+(?:\d+|null))", package_id)
                # Extract the first sequence of digits
                match_digits = re.search(r"\d+", package_id)
                if match_alpha and match_digits:
                    alpha_part = match_alpha.group(1).lower().replace("null", "")
                    digits_part = match_digits.group(0)
                    print_id = f"{alpha_part}-{digits_part}"

            # Main committee print record
            print_record = {
                "package_id": package_id,
                "print_id": print_id,
                "title": self.utils.sanitize_for_copy(item.get("title")),
                "chamber": self.utils.standardize_chamber(item.get("chamber")),
                "congress": self.utils.safe_int(item.get("congress")),
                "session": self.utils.safe_int(item.get("session")),
                "pages": self.utils.safe_int(item.get("pages")),
                "document_number": item.get("documentNumber"),
                "issued_at": self.utils.standardize_date(item.get("dateIssued")),
                "branch": item.get("branch"),
                "government_author1": item.get("governmentAuthor1"),
                "government_author2": item.get("governmentAuthor2"),
                "publisher": item.get("publisher"),
                "collection_code": item.get("collectionCode"),
                "migrated_doc_id": item.get("otherIdentifier", {}).get(
                    "migrated-doc-id"
                ),
                "su_doc_class_number": item.get("suDocClassNumber"),
                "last_modified": self.utils.standardize_date(item.get("lastModified")),
            }
            committeeprints.append(print_record)

        # Remove duplicates
        committeeprints = self.check_duplicates_optimized(
            committeeprints, "package_id", "committeeprints"
        )

        # Batch insert
        if committeeprints:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_govinfo",
                "committeeprints",
                [
                    "package_id",
                    "print_id",
                    "title",
                    "chamber",
                    "congress",
                    "session",
                    "pages",
                    "document_number",
                    "issued_at",
                    "branch",
                    "government_author1",
                    "government_author2",
                    "publisher",
                    "collection_code",
                    "migrated_doc_id",
                    "su_doc_class_number",
                    "last_modified",
                ],
                committeeprints,
            )

        logger.info(f"Processed {len(committeeprints)} GovInfo committeeprints")

    def process_govinfo_committeeprints_granules_optimized(
        self, data: list[dict], cursor
    ) -> None:
        """Optimized GovInfo committee prints granules processing."""
        logger.info(f"Processing {len(data)} GovInfo committeeprints granules")

        committeeprints_granules = []
        committeeprints_committees = []
        committeeprints_reference_bills = []

        for item in data:
            package_id = item.get("packageId")
            granule_id = item.get("granuleId")

            committeeprints_granules.append(
                {
                    "granule_id": granule_id,
                    "package_id": package_id,
                }
            )

            # Committees
            if item.get("committees"):
                for committee in item.get("committees", []):
                    committeeprints_committees.append(
                        {
                            "package_id": package_id,
                            "granule_id": granule_id,
                            "committee_code": committee.get("authorityId"),
                            "committee_name": self.utils.sanitize_for_copy(
                                committee.get("committeeName")
                            ),
                            "chamber": self.utils.standardize_chamber(
                                committee.get("chamber")
                            ),
                        }
                    )

            # Reference bills
            if item.get("references"):
                for ref in item.get("references"):
                    if ref.get("collectionCode") == "BILLS":
                        for bill in ref.get("contents", []):
                            committeeprints_reference_bills.append(
                                {
                                    "package_id": package_id,
                                    "granule_id": granule_id,
                                    "bill_id": f"{bill.get('type').lower()}{bill.get('number')}-{bill.get('congress')}",
                                }
                            )

        # Remove duplicates
        committeeprints_granules = self.check_duplicates_optimized(
            committeeprints_granules,
            ["granule_id", "package_id"],
            "committeeprints_granules",
        )
        committeeprints_committees = self.check_duplicates_optimized(
            committeeprints_committees,
            ["package_id", "granule_id", "committee_code", "committee_name", "chamber"],
            "committeeprints_committees",
        )
        committeeprints_reference_bills = self.check_duplicates_optimized(
            committeeprints_reference_bills,
            ["package_id", "granule_id", "bill_id"],
            "committeeprints_reference_bills",
        )

        # Batch insert
        if committeeprints_granules:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_govinfo",
                "committeeprints_granules",
                ["granule_id", "package_id"],
                committeeprints_granules,
            )

        if committeeprints_committees:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_govinfo",
                "committeeprints_committees",
                [
                    "package_id",
                    "granule_id",
                    "committee_code",
                    "committee_name",
                    "chamber",
                ],
                committeeprints_committees,
            )

        if committeeprints_reference_bills:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_govinfo",
                "committeeprints_reference_bills",
                ["package_id", "granule_id", "bill_id"],
                committeeprints_reference_bills,
            )

        logger.info(
            f"Processed {len(committeeprints_granules)} GovInfo committeeprints granules"
        )

    def process_govinfo_committeereports_optimized(
        self, data: list[dict], cursor
    ) -> None:
        """Optimized GovInfo committee reports processing."""
        logger.info(f"Processing {len(data)} GovInfo committeereports")

        committeereports = []

        for item in data:
            package_id = item.get("packageId")
            report_id = None
            if package_id and package_id.startswith("CRPT-"):
                # Extract the alpha part (possibly with digits)
                match_alpha = re.search(r"[A-Za-z]+(?:\d+)", package_id)
                # Extract the first sequence of digits
                match_digits = re.search(r"\d+", package_id)
                if match_alpha and match_digits:
                    alpha_part = match_alpha.group(0).lower()
                    digits_part = match_digits.group(0)
                    report_id = f"{alpha_part}-{digits_part}"

            # Main committee report record
            report_record = {
                "package_id": package_id,
                "report_id": report_id,
                "title": self.utils.sanitize_for_copy(item.get("title")),
                "subtitle": self.utils.sanitize_for_copy(item.get("subtitle")),
                "chamber": self.utils.standardize_chamber(item.get("chamber")),
                "congress": self.utils.safe_int(item.get("congress")),
                "session": self.utils.safe_int(item.get("session")),
                "pages": self.utils.safe_int(item.get("pages")),
                "issued_at": self.utils.standardize_date(item.get("dateIssued")),
                "branch": item.get("branch"),
                "government_author1": item.get("governmentAuthor1"),
                "government_author2": item.get("governmentAuthor2"),
                "publisher": item.get("publisher"),
                "collection_code": item.get("collectionCode"),
                "migrated_doc_id": item.get("otherIdentifier", {}).get(
                    "migrated-doc-id"
                ),
                "su_doc_class_number": item.get("suDocClassNumber"),
                "last_modified": self.utils.standardize_date(item.get("lastModified")),
            }
            committeereports.append(report_record)

        # Remove duplicates
        committeereports = self.check_duplicates_optimized(
            committeereports, "package_id", "committeereports"
        )

        # Batch insert
        if committeereports:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_govinfo",
                "committeereports",
                [
                    "package_id",
                    "report_id",
                    "title",
                    "subtitle",
                    "chamber",
                    "congress",
                    "session",
                    "pages",
                    "issued_at",
                    "branch",
                    "government_author1",
                    "government_author2",
                    "publisher",
                    "collection_code",
                    "migrated_doc_id",
                    "su_doc_class_number",
                    "last_modified",
                ],
                committeereports,
            )

        logger.info(
            f"Processed {len(committeereports)} GovInfo committeereports with all related data"
        )

    def process_govinfo_committeereports_granules_optimized(
        self, data: list[dict], cursor
    ) -> None:
        """Optimized GovInfo committee reports granules processing."""
        logger.info(f"Processing {len(data)} GovInfo committee reports granules")

        committeereports_granules = []
        committeereports_committees = []
        committeereports_members = []
        committeereports_reference_bills = []

        for item in data:
            package_id = item.get("packageId")
            granule_id = item.get("granuleId")

            committeereports_granules.append(
                {
                    "granule_id": granule_id,
                    "package_id": package_id,
                }
            )

            # Committees
            for committee in item.get("committees", []):
                committeereports_committees.append(
                    {
                        "package_id": package_id,
                        "granule_id": granule_id,
                        "committee_code": committee.get("authorityId"),
                        "committee_name": self.utils.sanitize_for_copy(
                            committee.get("committeeName")
                        ),
                        "chamber": self.utils.standardize_chamber(
                            committee.get("chamber")
                        ),
                    }
                )

            # Members
            for member in item.get("members", []):
                committeereports_members.append(
                    {
                        "granule_id": granule_id,
                        "package_id": package_id,
                        "bioguide_id": member.get("bioGuideId"),
                    }
                )

            # Reference bills
            if item.get("references"):
                for ref in item.get("references"):
                    if ref.get("collectionCode") == "BILLS":
                        for bill in ref.get("contents", []):
                            committeereports_reference_bills.append(
                                {
                                    "package_id": package_id,
                                    "granule_id": granule_id,
                                    "bill_id": f"{bill.get('type').lower()}{bill.get('number')}-{bill.get('congress')}",
                                }
                            )

        # Remove duplicates
        committeereports_granules = self.check_duplicates_optimized(
            committeereports_granules,
            ["granule_id", "package_id"],
            "committeereports_granules",
        )
        committeereports_committees = self.check_duplicates_optimized(
            committeereports_committees,
            ["package_id", "granule_id", "committee_code", "committee_name", "chamber"],
            "committeereports_committees",
        )
        committeereports_members = self.check_duplicates_optimized(
            committeereports_members,
            ["granule_id", "package_id", "bioguide_id"],
            "committeereports_members",
        )
        committeereports_reference_bills = self.check_duplicates_optimized(
            committeereports_reference_bills,
            ["package_id", "granule_id", "bill_id"],
            "committeereports_reference_bills",
        )

        # Batch insert
        if committeereports_granules:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_govinfo",
                "committeereports_granules",
                ["granule_id", "package_id"],
                committeereports_granules,
            )

        if committeereports_committees:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_govinfo",
                "committeereports_committees",
                [
                    "package_id",
                    "granule_id",
                    "committee_code",
                    "committee_name",
                    "chamber",
                ],
                committeereports_committees,
            )

        if committeereports_members:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_govinfo",
                "committeereports_members",
                ["granule_id", "package_id", "bioguide_id"],
                committeereports_members,
            )

        if committeereports_reference_bills:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_govinfo",
                "committeereports_reference_bills",
                ["package_id", "granule_id", "bill_id"],
                committeereports_reference_bills,
            )

        logger.info(
            f"Processed {len(committeereports_granules)} GovInfo committee reports granules"
        )

    def process_govinfo_hearings_optimized(self, data: list[dict], cursor) -> None:
        """Optimized GovInfo hearings processing."""
        logger.info(f"Processing {len(data)} GovInfo hearings")

        hearings = []

        for item in data:
            package_id = item.get("packageId")
            # Derive hearing_id from package_id, following the SQL logic in datatypes/congressional_hearings.py
            hearing_id = None
            if package_id and re.search(r"\d", package_id):
                match = re.match(
                    r"^(?:GPO-)?CHRG-(\d+)([a-z]+)(\d.*)", package_id, re.IGNORECASE
                )
                if match:
                    # \2\3-\1 in SQL: group(2) + group(3) + '-' + group(1)
                    hearing_id = f"{match.group(2)}{match.group(3)}-{match.group(1)}"
                else:
                    hearing_id = package_id
            else:
                hearing_id = package_id

            # Main hearing record
            hearing_record = {
                "package_id": package_id,
                "hearing_id": hearing_id,
                "title": self.utils.sanitize_for_copy(item.get("title")),
                "chamber": self.utils.standardize_chamber(item.get("chamber")),
                "congress": self.utils.safe_int(item.get("congress")),
                "session": self.utils.safe_int(item.get("session")),
                "pages": self.utils.safe_int(item.get("pages")),
                "is_appropriation": None,  # TODO: postprocess "isappropriation" field in hearings table
                "issued_at": self.utils.standardize_date(item.get("dateIssued")),
                "branch": item.get("branch"),
                "government_author1": item.get("governmentAuthor1"),
                "government_author2": item.get("governmentAuthor2"),
                "publisher": item.get("publisher"),
                "collection_code": item.get("collectionCode"),
                "migrated_doc_id": item.get("otherIdentifier", {}).get(
                    "migrated-doc-id"
                ),
                "su_doc_class_number": item.get("suDocClassNumber"),
                "last_modified": self.utils.standardize_date(item.get("lastModified")),
            }
            hearings.append(hearing_record)

        # Remove duplicates
        hearings = self.check_duplicates_optimized(hearings, "package_id", "hearings")

        # Batch insert
        if hearings:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_govinfo",
                "hearings",
                [
                    "package_id",
                    "hearing_id",
                    "title",
                    "chamber",
                    "congress",
                    "session",
                    "pages",
                    "is_appropriation",
                    "issued_at",
                    "branch",
                    "government_author1",
                    "government_author2",
                    "publisher",
                    "collection_code",
                    "migrated_doc_id",
                    "su_doc_class_number",
                    "last_modified",
                ],
                hearings,
            )

        logger.info(f"Processed {len(hearings)} GovInfo hearings with all related data")

    def process_govinfo_hearings_granules_optimized(
        self, data: list[dict], cursor
    ) -> None:
        """Optimized GovInfo hearings granules processing."""
        logger.info(f"Processing {len(data)} GovInfo hearings granules")

        hearings_granules = []
        hearings_committees = []
        hearings_members = []
        hearings_reference_bills = []
        hearings_witnesses = []

        for item in data:
            package_id = item.get("packageId")
            granule_id = item.get("granuleId")

            hearings_granules.append(
                {
                    "granule_id": granule_id,
                    "package_id": package_id,
                }
            )

            # Committees
            for committee in item.get("committees", []):
                hearings_committees.append(
                    {
                        "granule_id": granule_id,
                        "package_id": package_id,
                        "committee_code": committee.get("authorityId"),
                        "committee_name": self.utils.sanitize_for_copy(
                            committee.get("committeeName")
                        ),
                    }
                )

            # Members
            for member in item.get("members", []):
                hearings_members.append(
                    {
                        "granule_id": granule_id,
                        "package_id": package_id,
                        "bioguide_id": member.get("bioGuideId"),
                        "name": self.utils.sanitize_for_copy(member.get("name")),
                    }
                )

            # Reference bills
            if item.get("references"):
                for ref in item.get("references"):
                    if ref.get("collectionCode") == "BILLS":
                        for bill in ref.get("contents", []):
                            hearings_reference_bills.append(
                                {
                                    "package_id": package_id,
                                    "granule_id": granule_id,
                                    "bill_id": f"{bill.get('type').lower()}{bill.get('number')}-{bill.get('congress')}",
                                }
                            )

            for witness in item.get("witnesses", []):
                hearings_witnesses.append(
                    {
                        "granule_id": granule_id,
                        "witness": self.utils.sanitize_for_copy(witness),
                    }
                )

        # Remove duplicates
        hearings_granules = self.check_duplicates_optimized(
            hearings_granules, ["granule_id", "package_id"], "hearings_granules"
        )
        hearings_committees = self.check_duplicates_optimized(
            hearings_committees,
            ["granule_id", "package_id", "committee_code", "committee_name"],
            "hearings_committees",
        )
        hearings_members = self.check_duplicates_optimized(
            hearings_members,
            ["granule_id", "package_id", "bioguide_id"],
            "hearings_members",
        )
        hearings_reference_bills = self.check_duplicates_optimized(
            hearings_reference_bills,
            ["package_id", "granule_id", "bill_id"],
            "hearings_reference_bills",
        )
        hearings_witnesses = self.check_duplicates_optimized(
            hearings_witnesses,
            ["granule_id", "witness"],
            "hearings_witnesses",
        )

        # Batch insert
        if hearings_granules:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_govinfo",
                "hearings_granules",
                ["granule_id", "package_id"],
                hearings_granules,
            )

        if hearings_committees:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_govinfo",
                "hearings_committees",
                ["granule_id", "package_id", "committee_code", "committee_name"],
                hearings_committees,
            )

        if hearings_members:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_govinfo",
                "hearings_members",
                ["granule_id", "package_id", "bioguide_id", "name"],
                hearings_members,
            )

        if hearings_reference_bills:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_govinfo",
                "hearings_reference_bills",
                ["package_id", "granule_id", "bill_id"],
                hearings_reference_bills,
            )

        if hearings_witnesses:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_govinfo",
                "hearings_witnesses",
                ["granule_id", "witness"],
                hearings_witnesses,
            )

        logger.info(
            f"Processed {len(hearings_granules)} GovInfo hearings granules with all related data"
        )

    def process_govinfo_treaties_optimized(self, data: list[dict], cursor) -> None:
        logger.info(f"Processing {len(data)} GovInfo treaties")

        treaties = []

        for item in data:
            package_id = item.get("packageId")
            # Construct treaty_id from package_id as in .dep/cleaning/datatypes/treaty_docs.py
            # treaty_id = 'td' + <congress> + '-' + <treaty_number>
            package_id_str = str(package_id) if package_id is not None else ""
            if "tdoc" in package_id_str:
                parts = package_id_str.split("tdoc")
                left = parts[0].rstrip("-")
                right = parts[-1]
                left_split = left.split("-")
                congress = left_split[-1] if left_split else ""
                treaty_id = f"td{congress}-{right}"
            else:
                treaty_id = None

            # Main treaty record
            treaty_record = {
                "package_id": package_id,
                "treaty_id": treaty_id,
                "title": self.utils.sanitize_for_copy(item.get("title")),
                "congress": self.utils.safe_int(item.get("congress")),
                "session": self.utils.safe_int(item.get("session")),
                "chamber": self.utils.standardize_chamber(item.get("chamber")),
                "summary": None,  # TODO: postprocess "summary" field in treaties table
                "pages": self.utils.safe_int(item.get("pages")),
                "issued_at": self.utils.standardize_date(item.get("dateIssued")),
                "branch": item.get("branch"),
                "government_author1": item.get("governmentAuthor1"),
                "government_author2": item.get("governmentAuthor2"),
                "publisher": item.get("publisher"),
                "collection_code": item.get("collectionCode"),
                "migrated_doc_id": item.get("otherIdentifier", {}).get(
                    "migrated-doc-id"
                ),
                "su_doc_class_number": item.get("suDocClassNumber"),
                "last_modified": self.utils.standardize_date(item.get("lastModified")),
            }
            treaties.append(treaty_record)

        # Remove duplicates
        treaties = self.check_duplicates_optimized(treaties, "package_id", "treaties")
        # Batch insert
        if treaties:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_govinfo",
                "treaties",
                [
                    "package_id",
                    "treaty_id",
                    "title",
                    "congress",
                    "session",
                    "chamber",
                    "summary",
                    "pages",
                    "issued_at",
                    "branch",
                    "government_author1",
                    "government_author2",
                    "publisher",
                    "collection_code",
                    "migrated_doc_id",
                    "su_doc_class_number",
                    "last_modified",
                ],
                treaties,
            )

        logger.info(f"Processed {len(treaties)} GovInfo treaties with all related data")

    def process_govinfo_treaties_granules_optimized(
        self, data: list[dict], cursor
    ) -> None:
        """Optimized GovInfo treaties granules processing."""
        logger.info(f"Processing {len(data)} GovInfo treaties granules")

        treaties_granules = []
        treaties_committees = []

        for item in data:
            package_id = item.get("packageId")
            granule_id = item.get("granuleId")
            # Granules
            treaties_granules.append(
                {
                    "granule_id": granule_id,
                    "package_id": package_id,
                }
            )

            # Committees
            for committee in item.get("committees", []):
                treaties_committees.append(
                    {
                        "granule_id": granule_id,
                        "package_id": package_id,
                        "committee_code": committee.get("authorityId"),
                        "committee_name": self.utils.sanitize_for_copy(
                            committee.get("committeeName")
                        ),
                        "chamber": self.utils.standardize_chamber(
                            committee.get("chamber")
                        ),
                    }
                )

        # Remove duplicates

        treaties_granules = self.check_duplicates_optimized(
            treaties_granules, ["granule_id", "package_id"], "treaties_granules"
        )
        treaties_committees = self.check_duplicates_optimized(
            treaties_committees,
            ["granule_id", "package_id", "committee_code", "committee_name", "chamber"],
            "treaties_committees",
        )

        # Batch insert

        if treaties_granules:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_govinfo",
                "treaties_granules",
                ["granule_id", "package_id"],
                treaties_granules,
            )

        if treaties_committees:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_govinfo",
                "treaties_committees",
                [
                    "granule_id",
                    "package_id",
                    "committee_code",
                    "committee_name",
                    "chamber",
                ],
                treaties_committees,
            )

        logger.info(
            f"Processed {len(treaties_granules)} GovInfo treaties granules with all related data"
        )

    def process_govinfo_congressional_directories_optimized(
        self, data: list[dict], cursor
    ) -> None:
        """Optimized GovInfo congressional directories processing."""
        logger.info(f"Processing {len(data)} GovInfo congressional directories")

        congressional_directories = []
        congressional_directories_isbn = []

        for item in data:
            package_id = item.get("packageId")

            # Main congressional directory record
            directory_record = {
                "package_id": package_id,
                "title": self.utils.sanitize_for_copy(item.get("title")),
                "congress": self.utils.safe_int(item.get("congress")),
                "issued_at": self.utils.standardize_date(item.get("dateIssued")),
                "branch": item.get("branch"),
                "government_author1": item.get("governmentAuthor1"),
                "government_author2": item.get("governmentAuthor2"),
                "publisher": item.get("publisher"),
                "collection_code": item.get("collectionCode"),
                "ils_system_id": item.get("ilsSystemId"),
                "migrated_doc_id": item.get("otherIdentifier", {}).get(
                    "migrated-doc-id"
                ),
                "su_doc_class_number": item.get("suDocClassNumber"),
                "text_url": item.get("download", {}).get("textUrl"),
                "pdf_url": item.get("download", {}).get("pdfUrl"),
                "last_modified": self.utils.standardize_date(item.get("lastModified")),
            }
            congressional_directories.append(directory_record)

            # ISBNs
            if "isbn" in item.get("otherIdentifier", {}):
                for isbn in item.get("otherIdentifier", {}).get("isbn"):
                    congressional_directories_isbn.append(
                        {
                            "package_id": package_id,
                            "isbn": isbn,
                        }
                    )

        # Remove duplicates
        congressional_directories = self.check_duplicates_optimized(
            congressional_directories, "package_id", "congressional_directories"
        )
        congressional_directories_isbn = self.check_duplicates_optimized(
            congressional_directories_isbn,
            ["package_id", "isbn"],
            "congressional_directories_isbn",
        )
        # Batch insert
        if congressional_directories:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_govinfo",
                "congressional_directories",
                [
                    "package_id",
                    "title",
                    "congress",
                    "issued_at",
                    "branch",
                    "government_author1",
                    "government_author2",
                    "publisher",
                    "collection_code",
                    "ils_system_id",
                    "migrated_doc_id",
                    "su_doc_class_number",
                    "text_url",
                    "pdf_url",
                    "last_modified",
                ],
                congressional_directories,
            )

        if congressional_directories_isbn:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_govinfo",
                "congressional_directories_isbn",
                ["package_id", "isbn"],
                congressional_directories_isbn,
            )

        logger.info(
            f"Processed {len(congressional_directories)} GovInfo congressional directories with all related data"
        )

    def process_govinfo_congressional_directories_granules_optimized(
        self, data: list[dict], cursor
    ) -> None:
        """Optimized GovInfo congressional directories granules processing."""
        logger.info(
            f"Processing {len(data)} GovInfo congressional directories granules"
        )

        members = []

        for item in (item for item in data if item.get("subGranuleClass") == "STATEDELEGATION" or item.get("granuleClass") != "CONGRESSMEMBERSTATE"):
            package_id = item.get("packageId")
            granule_id = item.get("granuleId")
            # Members
            if item.get("members") and item.get("members")[0]:
                member = item.get("members")[0]
                bioguide_id = member.get("bioGuideId")
                gpo_id = item.get("gpoId")
                authority_id = item.get("authorityId")
            else:
                bioguide_id = None
                gpo_id = None
                authority_id = None
            members.append(
                {
                    "package_id": package_id,
                    "granule_id": granule_id,
                    "bioguide_id": bioguide_id,
                    "title": self.utils.sanitize_for_copy(item.get("title")),
                    "biography": self.utils.clean_long_text(item.get("biography")),
                    "member_type": item.get("subGranuleClass", "").title(),
                    "population": item.get("population"),
                    "gpo_id": gpo_id,
                    "authority_id": authority_id,
                    "official_url": next(
                        (url for url in item.get("online", []) if "gov" in url), None
                    ),
                    "twitter_url": next(
                        (url for url in item.get("online", []) if "twitter" in url),
                        None,
                    ),
                    "instagram_url": next(
                        (url for url in item.get("online", []) if "instagram" in url),
                        None,
                    ),
                    "facebook_url": next(
                        (url for url in item.get("online", []) if "facebook" in url),
                        None,
                    ),
                    "youtube_url": next(
                        (url for url in item.get("online", []) if "youtube" in url),
                        None,
                    ),
                    "other_url": next(
                        (
                            url
                            for url in item.get("online", [])
                            if "gov" not in url
                            and "twitter" not in url
                            and "instagram" not in url
                            and "facebook" not in url
                            and "youtube" not in url
                        ),
                        None,
                    ),
                }
            )

        # Remove duplicates
        members = self.check_duplicates_optimized(
            members, ["granule_id", "package_id"], "members"
        )

        # Batch insert
        if members:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_govinfo",
                "members",
                [
                    "granule_id",
                    "package_id",
                    "bioguide_id",
                    "title",
                    "biography",
                    "member_type",
                    "population",
                    "gpo_id",
                    "authority_id",
                    "official_url",
                    "twitter_url",
                    "instagram_url",
                    "facebook_url",
                    "youtube_url",
                    "other_url",
                ],
                members,
            )

        logger.info(
            f"Processed {len(members)} GovInfo congressional directories granules with all related data"
        )

    def process_data_parallel(
        self, data_type: str, data: list[dict], cursor, max_workers: int = 4
    ) -> None:
        """Process data using optimized processors."""
        # GovInfo data types
        if data_type == "billcollections":
            self.process_govinfo_bills_optimized(data, cursor)
        elif data_type == "printpackages":
            self.process_govinfo_committeeprints_optimized(data, cursor)
        elif data_type == "congressionalreports":
            self.process_govinfo_committeereports_optimized(data, cursor)
        elif data_type == "hearingpackages":
            self.process_govinfo_hearings_optimized(data, cursor)
        elif data_type == "treatydocs":
            self.process_govinfo_treaties_optimized(data, cursor)
        elif data_type == "congressionaldirectories":
            self.process_govinfo_congressional_directories_optimized(data, cursor)
        elif data_type == "printpackages_granules":
            self.process_govinfo_committeeprints_granules_optimized(data, cursor)
        elif data_type == "congressionalreports_granules":
            self.process_govinfo_committeereports_granules_optimized(data, cursor)
        elif data_type == "hearingpackages_granules":
            self.process_govinfo_hearings_granules_optimized(data, cursor)
        elif data_type == "treatydocs_granules":
            self.process_govinfo_treaties_granules_optimized(data, cursor)
        elif data_type == "congressionaldirectories_granules":
            self.process_govinfo_congressional_directories_granules_optimized(
                data, cursor
            )
        else:
            logger.warning(f"No optimized processor for {data_type}")


def connect_to_database():
    """Connect to database with connection pooling."""
    conn = psycopg2.connect(
        host=os.getenv("POSTGRESQL_HOST"),
        database=os.getenv("POSTGRESQL_DATABASE"),
        user=os.getenv("POSTGRESQL_USERNAME"),
        password=os.getenv("POSTGRESQL_PASSWORD"),
        port=os.getenv("POSTGRESQL_PORT"),
    )
    return conn


def main():
    """Main function with optimized GovInfo processing."""
    processor = OptimizedDataProcessor()
    conn = connect_to_database()
    cursor = conn.cursor()
    conn.autocommit = True

    # Process GovInfo bills
    data = processor.get_data_from_raw_table(
        "billcollections", "billcollections_raw", cursor, schema="bicam_raw_govinfo"
    )
    processor.process_data_parallel("billcollections", data, cursor)

    # Process GovInfo committee prints
    data = processor.get_data_from_raw_table(
        "printpackages",
        "printpackages_raw",
        cursor,
        schema="bicam_raw_govinfo",
    )
    processor.process_data_parallel("printpackages", data, cursor)

    # Process GovInfo committee reports
    data = processor.get_data_from_raw_table(
        "congressionalreports",
        "congressionalreports_raw",
        cursor,
        schema="bicam_raw_govinfo",
    )
    processor.process_data_parallel("congressionalreports", data, cursor)

    # Process GovInfo hearings
    data = processor.get_data_from_raw_table(
        "hearingpackages", "hearingpackages_raw", cursor, schema="bicam_raw_govinfo"
    )
    processor.process_data_parallel("hearingpackages", data, cursor)

    # Process GovInfo treaties
    data = processor.get_data_from_raw_table(
        "treatydocs", "treatydocs_raw", cursor, schema="bicam_raw_govinfo"
    )
    processor.process_data_parallel("treatydocs", data, cursor)

    # Process GovInfo congressional directories
    data = processor.get_data_from_raw_table(
        "congressionaldirectories",
        "congressionaldirectories_raw",
        cursor,
        schema="bicam_raw_govinfo",
    )
    processor.process_data_parallel("congressionaldirectories", data, cursor)

    # Process GovInfo congressional directories granules
    data = processor.get_data_from_raw_table(
        "congressionaldirectories_granules",
        "congressionaldirectories_granules_raw",
        cursor,
        schema="bicam_raw_govinfo",
        related=True,
    )
    processor.process_data_parallel("congressionaldirectories_granules", data, cursor)

    # Process GovInfo committee reports granules
    data = processor.get_data_from_raw_table(
        "congressionalreports_granules",
        "congressionalreports_granules_raw",
        cursor,
        schema="bicam_raw_govinfo",
        related=True,
    )
    processor.process_data_parallel("congressionalreports_granules", data, cursor)

    # Process GovInfo committee prints granules
    data = processor.get_data_from_raw_table(
        "printpackages_granules",
        "printpackages_granules_raw",
        cursor,
        schema="bicam_raw_govinfo",
        related=True,
    )
    processor.process_data_parallel("printpackages_granules", data, cursor)

    # Process GovInfo hearings granules
    data = processor.get_data_from_raw_table(
        "hearingpackages_granules",
        "hearingpackages_granules_raw",
        cursor,
        schema="bicam_raw_govinfo",
        related=True,
    )
    processor.process_data_parallel("hearingpackages_granules", data, cursor)

    # Process GovInfo treaties granules
    data = processor.get_data_from_raw_table(
        "treatydocs_granules",
        "treatydocs_granules_raw",
        cursor,
        schema="bicam_raw_govinfo",
        related=True,
    )
    processor.process_data_parallel("treatydocs_granules", data, cursor)
    conn.commit()
    cursor.close()
    conn.close()
    logger.info("GovInfo optimized processing completed")


if __name__ == "__main__":
    main()


# TODO: postprocess "isappropriation" field in hearings table
# TODO: postprocess "summary" field in treaties table
