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
    def batch_copy_dicts_to_table(
        cursor,
        schema: str,
        table: str,
        columns: list[str],
        dicts: list[dict],
        batch_size: int = 10000,
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
                            logger.info(f"Retrying with smaller batch size...")
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
            #     with open("error.txt", "a") as f:
            #         f.write(
            #             f"Error copying batch {i // batch_size + 1}/{(total_records + batch_size - 1) // batch_size} ({len(batch)} records): {e}\n"
            #         )
            #         f.write(f"SQL: {sql}\n")
            #         f.write(f"Batch: {batch}\n")
            #         f.write(f"Values: {vals}\n")
            #         f.write(f"Output: {output.getvalue()}\n")
            #     raise e

            # logger.debug(
            #     f"Copied batch {i // batch_size + 1}/{(total_records + batch_size - 1) // batch_size} ({len(batch)} records)"
            # )


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
        schema: str = "bicam_raw_congressional",
        related: bool = False,
    ) -> list[dict]:
        """Optimized data retrieval with better query structure."""
        if related:
            if table_name == "committeeprints_texts_raw":
                query = f"""
                SELECT
                    jsonb_set(
                        {schema}.{table_name}.payload,
                        '{{url}}',
                        to_jsonb({schema}.{table_name}.url)
                    ) AS payload
                FROM {schema}.{table_name}
                JOIN {schema}.{table_name.split("_")[0]}_raw USING (source_doc_id)
                WHERE {schema}.{table_name.split("_")[0]}_raw.payload->>'updateDate' > (
                    select last_processed_date::text
                    from __metadata.last_processed_dates
                    where data_type = '{table_name.split("_")[0]}'
                )
                """
            else:
                query = f"""
                SELECT {schema}.{table_name}.payload
                FROM {schema}.{table_name}
                JOIN {schema}.{table_name.split("_")[0]}_raw USING (source_doc_id)
                WHERE {schema}.{table_name.split("_")[0]}_raw.payload->>'updateDate' > (
                    select last_processed_date::text
                    from __metadata.last_processed_dates
                    where data_type = '{table_name.split("_")[0]}'
                )
                """
        else:
            query = f"""
            SELECT payload
            FROM {schema}.{table_name}
            WHERE payload->>'updateDate' > (
                select last_processed_date::text
                from __metadata.last_processed_dates
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

    def process_amendments_optimized(self, data: list[dict], cursor) -> None:
        """Optimized amendments processing with vectorized operations."""
        logger.info(f"Processing {len(data)} amendments")

        # Pre-allocate lists for better memory efficiency
        amendments = []
        amendments_sponsors = []
        amendments_amended_bills = []
        amendments_amended_treaties = []
        amendments_amended_amendments = []

        # Process all amendments in a single pass
        for item in data:
            amendment_id = (
                f"{item.get('type').lower()}{item.get('number')}-{item.get('congress')}"
            )

            # Main amendment record
            amendment_record = {
                "amendment_id": amendment_id,
                "amendment_type": item.get("type").lower(),
                "amendment_number": item.get("number"),
                "congress": item.get("congress"),
                "chamber": self.utils.standardize_chamber(item.get("chamber")),
                "purpose": item.get("purpose"),
                "description": self.utils.clean_long_text(item.get("description")),
                "proposed_at": self.utils.standardize_date(item.get("proposedDate")),
                "submitted_at": self.utils.standardize_date(item.get("submittedDate")),
                "is_bill_amendment": item.get("amendedBill") is not None,
                "is_treaty_amendment": item.get("amendedTreaty") is not None,
                "is_amendment_amendment": item.get("amendedAmendment") is not None,
                "notes": item.get("notes"),
                "actions_count": item.get("actions", {}).get("count", 0),
                "cosponsors_count": item.get("cosponsors", {}).get("count", 0),
                "amendments_to_amendment_count": item.get(
                    "amendmentsToAmendment", {}
                ).get("count", 0),
                "updated_at": self.utils.standardize_date(item.get("updateDate")),
            }
            amendments.append(amendment_record)

            # Sponsors
            for sponsor in item.get("sponsors", []):
                if sponsor.get("bioguideId") is not None:
                    amendments_sponsors.append(
                        {
                            "amendment_id": amendment_id,
                            "bioguide_id": sponsor.get("bioguideId"),
                        }
                    )

            # Amended bills
            if item.get("amendedBill") is not None:
                bill = item.get("amendedBill")
                amendments_amended_bills.append(
                    {
                        "amendment_id": amendment_id,
                        "bill_id": f"{bill.get('type').lower()}{bill.get('number')}-{bill.get('congress')}",
                    }
                )

            # Amended treaties
            if item.get("amendedTreaty") is not None:
                treaty = item.get("amendedTreaty")
                amendments_amended_treaties.append(
                    {
                        "amendment_id": amendment_id,
                        "treaty_id": f"td{treaty.get('congress')}-{treaty.get('number')}",
                    }
                )

            # Amended amendments
            if item.get("amendedAmendment") is not None:
                amendment = item.get("amendedAmendment")
                amendments_amended_amendments.append(
                    {
                        "amendment_id": amendment_id,
                        "amended_amendment_id": f"{amendment.get('type').lower()}{amendment.get('number')}-{amendment.get('congress')}",
                    }
                )

        # Remove duplicates
        amendments = self.check_duplicates_optimized(
            amendments, "amendment_id", "amendments"
        )
        amendments_sponsors = self.check_duplicates_optimized(
            amendments_sponsors, ["amendment_id", "bioguide_id"], "amendments_sponsors"
        )
        amendments_amended_bills = self.check_duplicates_optimized(
            amendments_amended_bills,
            ["amendment_id", "bill_id"],
            "amendments_amended_bills",
        )
        amendments_amended_treaties = self.check_duplicates_optimized(
            amendments_amended_treaties,
            ["amendment_id", "treaty_id"],
            "amendments_amended_treaties",
        )
        amendments_amended_amendments = self.check_duplicates_optimized(
            amendments_amended_amendments,
            ["amendment_id", "amended_amendment_id"],
            "amendments_amended_amendments",
        )

        # Batch insert all data
        if amendments:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "amendments",
                [
                    "amendment_id",
                    "amendment_type",
                    "amendment_number",
                    "congress",
                    "chamber",
                    "purpose",
                    "description",
                    "proposed_at",
                    "submitted_at",
                    "is_bill_amendment",
                    "is_treaty_amendment",
                    "is_amendment_amendment",
                    "notes",
                    "actions_count",
                    "cosponsors_count",
                    "amendments_to_amendment_count",
                    "updated_at",
                ],
                amendments,
            )

        if amendments_sponsors:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "amendments_sponsors",
                ["amendment_id", "bioguide_id"],
                amendments_sponsors,
            )

        if amendments_amended_bills:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "amendments_amended_bills",
                ["amendment_id", "bill_id"],
                amendments_amended_bills,
            )

        if amendments_amended_treaties:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "amendments_amended_treaties",
                ["amendment_id", "treaty_id"],
                amendments_amended_treaties,
            )

        if amendments_amended_amendments:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "amendments_amended_amendments",
                ["amendment_id", "amended_amendment_id"],
                amendments_amended_amendments,
            )

        logger.info(f"Processed {len(amendments)} amendments with all related data")

    def process_bills_optimized(self, data: list[dict], cursor) -> None:
        """Optimized bills processing with efficient data structures."""
        logger.info(f"Processing {len(data)} bills")

        # Pre-allocate lists
        bills = []
        bills_sponsors = []
        bills_laws = []
        bills_notes = []
        bills_notes_links = []
        bills_cbocostestimates = []

        # Process all bills in a single pass
        for item in data:
            bill_id = f"{item.get('type').lower()}{item.get('number').replace('½', '.5')}-{item.get('congress')}"

            # Main bill record
            bill_record = {
                "bill_id": bill_id,
                "bill_type": item.get("type").lower(),
                "bill_number": item.get("number"),
                "congress": item.get("congress"),
                "title": item.get("title"),
                "origin_chamber": item.get("originChamber").lower(),
                "introduced_at": self.utils.standardize_date(
                    item.get("introducedDate")
                ),
                "constitutional_authority_statement": self.utils.clean_long_text(
                    item.get("constitutionalAuthorityStatement")
                ),
                "is_law": len(item.get("laws", [])) > 0,
                "notes": item.get("notes"),
                "policy_area": item.get("policyArea"),
                "actions_count": item.get("actions", {}).get("count", 0),
                "amendments_count": item.get("amendments", {}).get("count", 0),
                "committees_count": item.get("committees", {}).get("count", 0),
                "cosponsors_count": item.get("cosponsors", {}).get("count", 0),
                "summaries_count": item.get("summaries", {}).get("count", 0),
                "subjects_count": item.get("subjects", {}).get("count", 0),
                "titles_count": item.get("titles", {}).get("count", 0),
                "texts_count": item.get("texts", {}).get("count", 0),
                "updated_at": self.utils.standardize_date(item.get("updateDate")),
            }
            bills.append(bill_record)

            # Sponsors
            for sponsor in item.get("sponsors", []):
                if sponsor.get("bioguideId"):
                    bills_sponsors.append(
                        {
                            "bill_id": bill_id,
                            "bioguide_id": sponsor.get("bioguideId"),
                        }
                    )

            # Laws
            for law in item.get("laws", []):
                bills_laws.append(
                    {
                        "bill_id": bill_id,
                        "law_id": f"PL{law.get('congress')}-{law.get('number')}",
                        "law_number": law.get("number"),
                        "law_type": law.get("type"),
                    }
                )

            # Notes
            for note in item.get("notes", []):
                note_number = hashlib.sha256(json.dumps(note).encode()).hexdigest()[:16]
                bills_notes.append(
                    {
                        "bill_id": bill_id,
                        "note_number": note_number,
                        "note_text": note.get("text"),
                        "updated_at": self.utils.standardize_date(datetime.now()),
                    }
                )

                # Note links
                for link in note.get("links", []):
                    bills_notes_links.append(
                        {
                            "bill_id": bill_id,
                            "note_number": note_number,
                            "link_name": link.get("name"),
                            "link_url": link.get("url"),
                            "updated_at": self.utils.standardize_date(datetime.now()),
                        }
                    )

            # CBO cost estimates
            for cbo in item.get("cbocostestimates", []):
                bills_cbocostestimates.append(
                    {
                        "bill_id": bill_id,
                        "description": cbo.get("description"),
                        "pub_date": self.utils.standardize_date(cbo.get("pubDate")),
                        "title": cbo.get("title"),
                        "url": cbo.get("url"),
                    }
                )

        # Remove duplicates
        bills = self.check_duplicates_optimized(bills, "bill_id", "bills")
        bills_sponsors = self.check_duplicates_optimized(
            bills_sponsors, ["bill_id", "bioguide_id"], "bills_sponsors"
        )
        bills_laws = self.check_duplicates_optimized(
            bills_laws, ["bill_id", "law_id"], "bills_laws"
        )
        bills_notes = self.check_duplicates_optimized(
            bills_notes, ["bill_id", "note_number"], "bills_notes"
        )
        bills_notes_links = self.check_duplicates_optimized(
            bills_notes_links,
            ["bill_id", "note_number", "link_url"],
            "bills_notes_links",
        )
        bills_cbocostestimates = self.check_duplicates_optimized(
            bills_cbocostestimates,
            ["bill_id", "pub_date", "url"],
            "bills_cbocostestimates",
        )

        # Batch insert all data
        if bills:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "bills",
                [
                    "bill_id",
                    "bill_type",
                    "bill_number",
                    "congress",
                    "title",
                    "origin_chamber",
                    "introduced_at",
                    "constitutional_authority_statement",
                    "is_law",
                    "notes",
                    "policy_area",
                    "actions_count",
                    "amendments_count",
                    "committees_count",
                    "cosponsors_count",
                    "summaries_count",
                    "subjects_count",
                    "titles_count",
                    "texts_count",
                    "updated_at",
                ],
                bills,
            )

        if bills_sponsors:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "bills_sponsors",
                ["bill_id", "bioguide_id"],
                bills_sponsors,
            )

        if bills_laws:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "bills_laws",
                ["bill_id", "law_id", "law_number", "law_type"],
                bills_laws,
            )

        if bills_notes:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "bills_notes",
                ["bill_id", "note_number", "note_text"],
                bills_notes,
            )

        if bills_notes_links:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "bills_notes_links",
                ["bill_id", "note_number", "link_name", "link_url"],
                bills_notes_links,
            )

        if bills_cbocostestimates:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "bills_cbocostestimates",
                ["bill_id", "description", "pub_date", "title", "url"],
                bills_cbocostestimates,
            )

        logger.info(f"Processed {len(bills)} bills with all related data")

    def process_committees_optimized(self, data: list[dict], cursor) -> None:
        """Optimized committees processing."""
        logger.info(f"Processing {len(data)} committees")

        committees = []
        committees_subcommittees = []
        committees_history = []

        for item in data:
            committee_code = item.get("systemCode")

            # Main committee record
            committee_record = {
                "committee_code": committee_code,
                "name": item.get("name"),
                "chamber": self.utils.standardize_chamber(item.get("chamber")),
                "is_subcommittee": item.get("parent") is not None,
                "is_current": item.get("isCurrent"),
                "bills_count": item.get("bills", {}).get("count", 0),
                "reports_count": item.get("reports", {}).get("count", 0),
                "nominations_count": item.get("nominations", {}).get("count", 0),
                "updated_at": self.utils.standardize_date(item.get("updateDate")),
            }
            committees.append(committee_record)

            # Subcommittees
            for subcommittee in item.get("subcommittees", []):
                committees_subcommittees.append(
                    {
                        "committee_code": committee_code,
                        "subcommittee_code": subcommittee.get("systemCode"),
                    }
                )

            # History
            for hist in item.get("history", []):
                committees_history.append(
                    {
                        "committee_code": committee_code,
                        "name": hist.get("officialName"),
                        "loc_name": hist.get("libraryOfCongressName"),
                        "started_at": self.utils.standardize_date(
                            hist.get("startDate")
                        ),
                        "ended_at": self.utils.standardize_date(hist.get("endDate")),
                        "committee_type": hist.get("committeeTypeCode"),
                        "establishing_authority": hist.get("establishingAuthority"),
                        "su_doc_class_number": hist.get("superintendentDocumentNumber"),
                        "nara_id": hist.get("naraId"),
                        "loc_linked_data_id": hist.get("locLinkedDataId"),
                    }
                )

        # Remove duplicates
        committees = self.check_duplicates_optimized(
            committees, "committee_code", "committees"
        )
        committees_subcommittees = self.check_duplicates_optimized(
            committees_subcommittees,
            ["committee_code", "subcommittee_code"],
            "committees_subcommittees",
        )
        committees_history = self.check_duplicates_optimized(
            committees_history,
            ["committee_code", "started_at", "ended_at"],
            "committees_history",
        )

        # Batch insert
        if committees:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "committees",
                [
                    "committee_code",
                    "name",
                    "chamber",
                    "is_subcommittee",
                    "is_current",
                    "bills_count",
                    "reports_count",
                    "nominations_count",
                    "updated_at",
                ],
                committees,
            )

        if committees_subcommittees:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "committees_subcommittees",
                ["committee_code", "subcommittee_code"],
                committees_subcommittees,
            )

        if committees_history:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "committees_history",
                [
                    "committee_code",
                    "name",
                    "loc_name",
                    "started_at",
                    "ended_at",
                    "committee_type",
                    "establishing_authority",
                    "su_doc_class_number",
                    "nara_id",
                    "loc_linked_data_id",
                ],
                committees_history,
            )

        logger.info(f"Processed {len(committees)} committees with all related data")

    def process_members_optimized(self, data: list[dict], cursor) -> None:
        """Optimized members processing."""
        logger.info(f"Processing {len(data)} members")

        members = []
        members_terms = []
        members_leadership_roles = []
        members_party_history = []

        for item in data:
            bioguide_id = item.get("bioguideId")
            if not bioguide_id:
                logger.warning(
                    f"Skipping member without bioguide_id: {item.get('id', 'unknown')}"
                )
                continue

            # Main member record
            address_info = item.get("addressInformation", {}) or {}
            depiction = item.get("depiction", {}) or {}

            member_record = {
                "bioguide_id": bioguide_id,
                "normalized_name": item.get("directOrderName"),
                "direct_order_name": item.get("directOrderName"),
                "inverted_order_name": item.get("invertedOrderName"),
                "honorific_prefix": item.get("honorificName"),
                "first_name": item.get("firstName"),
                "middle_name": item.get("middleName"),
                "last_name": item.get("lastName"),
                "suffix": item.get("suffixName"),
                "nickname": item.get("nickName"),
                "party": item.get("party"),
                "state": item.get("state"),
                "district": self.utils.safe_int(item.get("district")),
                "birth_year": self.utils.safe_int(item.get("birthYear")),
                "death_year": self.utils.safe_int(item.get("deathYear")),
                "official_url": item.get("officialUrl"),
                "office_address": address_info.get("officeAddress"),
                "office_city": address_info.get("city"),
                "office_district": address_info.get("district"),
                "office_zip": address_info.get("zipCode"),
                "office_phone": address_info.get("phoneNumber"),
                "sponsored_legislation_count": item.get("sponsoredLegislation", {}).get(
                    "count", 0
                ),
                "cosponsored_legislation_count": item.get(
                    "cosponsoredLegislation", {}
                ).get("count", 0),
                "depiction_image_url": depiction.get("imageUrl"),
                "depiction_attribution": depiction.get("attribution"),
                "is_current_member": item.get("currentMember") == "True",
                "updated_at": self.utils.standardize_date(item.get("updateDate")),
            }
            members.append(member_record)

            for term in item.get("terms", []):
                if term.get("congress") and term.get("startYear"):
                    members_terms.append(
                        {
                            "bioguide_id": bioguide_id,
                            "member_type": term.get("memberType"),
                            "chamber": self.utils.standardize_chamber(
                                term.get("chamber")
                            ),
                            "congress": self.utils.safe_int(term.get("congress")),
                            "start_year": self.utils.safe_int(term.get("startYear")),
                            "end_year": self.utils.safe_int(term.get("endYear")),
                            "state_name": term.get("stateName"),
                            "state_code": term.get("stateCode"),
                            "district": self.utils.safe_int(term.get("district")),
                        }
                    )

            # Leadership roles
            for role in item.get("leadership", []):
                if role.get("type") and role.get("congress"):
                    members_leadership_roles.append(
                        {
                            "bioguide_id": bioguide_id,
                            "role": role.get("type"),
                            "congress": self.utils.safe_int(role.get("congress")),
                            "chamber": self.utils.standardize_chamber(
                                next(
                                    (
                                        term.get("chamber")
                                        for term in item.get("terms", [])
                                        if term.get("congress") == role.get("congress")
                                    ),
                                    role.get("chamber"),
                                )
                            ),
                            "is_current": role.get("current") == "True",
                        }
                    )

            for partyhistory in item.get("partyHistory", []):
                if partyhistory.get("partyCode") and partyhistory.get("startYear"):
                    members_party_history.append(
                        {
                            "bioguide_id": bioguide_id,
                            "party_code": partyhistory.get("partyAbbreviation"),
                            "party_name": partyhistory.get("partyName"),
                            "start_year": self.utils.safe_int(
                                partyhistory.get("startYear")
                            ),
                            "end_year": self.utils.safe_int(
                                partyhistory.get("endYear")
                            ),
                        }
                    )

        # Remove duplicates
        members = self.check_duplicates_optimized(members, "bioguide_id", "members")
        members_terms = self.check_duplicates_optimized(
            members_terms, ["bioguide_id", "start_year", "end_year"], "members_terms"
        )
        members_leadership_roles = self.check_duplicates_optimized(
            members_leadership_roles,
            ["bioguide_id", "role", "congress", "chamber"],
            "members_leadership_roles",
        )
        members_party_history = self.check_duplicates_optimized(
            members_party_history,
            ["bioguide_id", "party_code", "start_year", "end_year"],
            "members_party_history",
        )

        # Batch insert
        if members:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "members",
                [
                    "bioguide_id",
                    "normalized_name",
                    "direct_order_name",
                    "inverted_order_name",
                    "honorific_prefix",
                    "first_name",
                    "middle_name",
                    "last_name",
                    "suffix",
                    "nickname",
                    "party",
                    "state",
                    "district",
                    "birth_year",
                    "death_year",
                    "official_url",
                    "office_address",
                    "office_city",
                    "office_district",
                    "office_zip",
                    "office_phone",
                    "sponsored_legislation_count",
                    "cosponsored_legislation_count",
                    "depiction_image_url",
                    "depiction_attribution",
                    "is_current_member",
                    "updated_at",
                ],
                members,
            )

        if members_terms:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "members_terms",
                [
                    "bioguide_id",
                    "member_type",
                    "chamber",
                    "congress",
                    "start_year",
                    "end_year",
                    "state_name",
                    "state_code",
                    "district",
                ],
                members_terms,
            )

        if members_leadership_roles:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "members_leadership_roles",
                ["bioguide_id", "role", "congress", "chamber", "is_current"],
                members_leadership_roles,
            )

        if members_party_history:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "members_party_history",
                ["bioguide_id", "party_code", "party_name", "start_year", "end_year"],
                members_party_history,
            )

        logger.info(f"Processed {len(members)} members with all related data")

    def process_hearings_optimized(self, data: list[dict], cursor) -> None:
        """Optimized hearings processing."""
        logger.info(f"Processing {len(data)} hearings")

        hearings = []
        hearings_committees = []
        hearings_dates = []
        hearings_texts = []

        for item in data:
            hearing_id = f"{item.get('chamber')[0].lower() if item.get('chamber')[0].lower() != 'n' else 'j'}rg{item.get('jacketNumber')}-{item.get('congress')}"

            # Main hearing record
            hearing_record = {
                "hearing_id": hearing_id,
                "hearing_jacketnumber": item.get("jacketNumber"),
                "loc_id": item.get("libraryOfCongressIdentifier"),
                "title": item.get("title"),
                "congress": item.get("congress"),
                "chamber": self.utils.standardize_chamber(item.get("chamber")),
                "hearing_number": item.get("number"),
                "part_number": item.get("part"),
                "citation": item.get("citation"),
                "updated_at": self.utils.standardize_date(item.get("updateDate")),
            }
            hearings.append(hearing_record)

            # Committees
            if item.get("committees"):
                for committee in item.get("committees"):
                    committee_code = committee.get("systemCode")
                    if committee_code:
                        hearings_committees.append(
                            {
                                "hearing_id": hearing_id,
                                "committee_code": committee_code,
                            }
                        )

            # Dates
            if item.get("dates"):
                for date_item in item.get("dates", []):
                    date_val = date_item.get("date")
                    if date_val:
                        hearings_dates.append(
                            {
                                "hearing_id": hearing_id,
                                "hearing_date": self.utils.standardize_date(date_val),
                            }
                        )

            # Texts (formats)
            formats = item.get("formats") if item.get("formats") else []

            # Only one formatted_text and one pdf per hearing, if available
            formatted_text = None
            pdf = None
            for fmt in formats:
                fmt_type = fmt.get("type")
                url = fmt.get("url")
                if fmt_type and url:
                    if fmt_type.lower() == "formatted text" and not formatted_text:
                        formatted_text = url
                    elif fmt_type.lower() == "pdf" and not pdf:
                        pdf = url

            if formatted_text or pdf:
                hearings_texts.append(
                    {
                        "hearing_id": hearing_id,
                        "raw_text": None,
                        "pdf": pdf,
                        "formatted_text": formatted_text,
                    }
                )

        # Remove duplicates
        hearings = self.check_duplicates_optimized(hearings, "hearing_id", "hearings")
        hearings_committees = self.check_duplicates_optimized(
            hearings_committees, ["hearing_id", "committee_code"], "hearings_committees"
        )
        hearings_dates = self.check_duplicates_optimized(
            hearings_dates, ["hearing_id", "hearing_date"], "hearings_dates"
        )
        hearings_texts = self.check_duplicates_optimized(
            hearings_texts, ["hearing_id", "pdf", "formatted_text"], "hearings_texts"
        )

        # Batch insert
        if hearings:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "hearings",
                [
                    "hearing_id",
                    "hearing_jacketnumber",
                    "loc_id",
                    "title",
                    "congress",
                    "chamber",
                    "hearing_number",
                    "part_number",
                    "citation",
                    "updated_at",
                ],
                hearings,
            )

        if hearings_committees:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "hearings_committees",
                ["hearing_id", "committee_code"],
                hearings_committees,
            )

        if hearings_dates:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "hearings_dates",
                ["hearing_id", "hearing_date"],
                hearings_dates,
            )

        if hearings_texts:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "hearings_texts",
                ["hearing_id", "raw_text", "pdf", "formatted_text"],
                hearings_texts,
            )

        logger.info(f"Processed {len(hearings)} hearings with all related data")

    def process_nominations_optimized(self, data: list[dict], cursor) -> None:
        """Optimized nominations processing."""
        logger.info(f"Processing {len(data)} nominations")

        nominations = []
        nominations_positions = []

        for item in data:
            nomination_id = f"PN{item.get('number')}-{item.get('partNumber', '00')}-{item.get('congress')}"

            # Main nomination record
            nomination_record = {
                "nomination_id": nomination_id,
                "nomination_number": item.get("number"),
                "part_number": item.get("partNumber", "00"),
                "congress": item.get("congress"),
                "description": item.get("description"),
                "is_privileged": item.get("isPrivileged"),
                "is_civilian": item.get("isList"),
                "received_at": self.utils.standardize_date(item.get("receivedDate")),
                "authority_date": self.utils.standardize_date(
                    item.get("authorityDate")
                ),
                "executive_calendar_number": item.get("executiveCalendarNumber"),
                "citation": item.get("citation"),
                "committees_count": item.get("committees", {}).get("count", 0),
                "actions_count": item.get("actions", {}).get("count", 0),
                "updated_at": self.utils.standardize_date(item.get("updateDate")),
            }
            nominations.append(nomination_record)

            # Nominees/positions
            if item.get("nominees"):
                for nominee in item.get("nominees"):
                    nominations_positions.append(
                        {
                            "nomination_id": nomination_id,
                            "ordinal": nominee.get("ordinal"),
                            "position_title": nominee.get("positionTitle"),
                            "organization": nominee.get("organization"),
                            "intro_text": self.utils.clean_long_text(
                                nominee.get("introText")
                            ),
                            "nominee_count": nominee.get("nomineeCount"),
                        }
                    )

        # Remove duplicates
        nominations = self.check_duplicates_optimized(
            nominations, "nomination_id", "nominations"
        )
        nominations_positions = self.check_duplicates_optimized(
            nominations_positions, ["nomination_id", "ordinal"], "nominations_positions"
        )

        # Batch insert
        if nominations:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "nominations",
                [
                    "nomination_id",
                    "nomination_number",
                    "part_number",
                    "congress",
                    "description",
                    "is_privileged",
                    "is_civilian",
                    "received_at",
                    "authority_date",
                    "executive_calendar_number",
                    "citation",
                    "committees_count",
                    "actions_count",
                    "updated_at",
                ],
                nominations,
            )

        if nominations_positions:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "nominations_positions",
                [
                    "nomination_id",
                    "ordinal",
                    "position_title",
                    "organization",
                    "intro_text",
                    "nominee_count",
                ],
                nominations_positions,
            )

        logger.info(f"Processed {len(nominations)} nominations with all related data")

    def process_treaties_optimized(self, data: list[dict], cursor) -> None:
        """Optimized treaties processing."""
        logger.info(f"Processing {len(data)} treaties")

        treaties = []
        treaties_country_parties = []
        treaties_index_terms = []
        treaties_titles = []

        for item in data:
            treaty_id = f"td{item.get('congressReceived')}-{item.get('number')}{item.get('suffix', '')}"

            # Main treaty record
            treaty_record = {
                "treaty_id": treaty_id,
                "treaty_number": item.get("number"),
                "suffix": item.get("suffix"),
                "congress_received": item.get("congressReceived"),
                "congress_considered": item.get("congressConsidered"),
                "topic": item.get("topic"),
                "transmitted_at": self.utils.standardize_date(
                    item.get("transmittedDate")
                ),
                "in_force_at": self.utils.standardize_date(item.get("inForceDate")),
                "resolution_text": self.utils.clean_long_text(
                    item.get("resolutionText")
                ),
                "parts_count": item.get("parts", {}).get("count", 0),
                "actions_count": item.get("actions", {}).get("count", 0),
                "old_number": item.get("oldNumber"),
                "old_number_display_name": item.get("oldNumberDisplayName"),
                "updated_at": self.utils.standardize_date(item.get("updateDate")),
            }
            treaties.append(treaty_record)

            # Country parties
            for country_party in item.get("countriesParties", []):
                treaties_country_parties.append(
                    {
                        "treaty_id": treaty_id,
                        "country": country_party.get("name"),
                    }
                )

            # Index terms
            for index_term in item.get("indexTerms", []):
                treaties_index_terms.append(
                    {
                        "treaty_id": treaty_id,
                        "index_term": index_term.get("name"),
                    }
                )

            # Titles
            for title in item.get("titles", []):
                treaties_titles.append(
                    {
                        "treaty_id": treaty_id,
                        "title": title.get("title"),
                        "title_type": title.get("titleType"),
                    }
                )

        # Remove duplicates
        treaties = self.check_duplicates_optimized(treaties, "treaty_id", "treaties")
        treaties_country_parties = self.check_duplicates_optimized(
            treaties_country_parties,
            ["treaty_id", "country"],
            "treaties_country_parties",
        )
        treaties_index_terms = self.check_duplicates_optimized(
            treaties_index_terms, ["treaty_id", "index_term"], "treaties_index_terms"
        )
        treaties_titles = self.check_duplicates_optimized(
            treaties_titles, ["treaty_id", "title", "title_type"], "treaties_titles"
        )

        # Batch insert
        if treaties:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "treaties",
                [
                    "treaty_id",
                    "treaty_number",
                    "suffix",
                    "congress_received",
                    "congress_considered",
                    "topic",
                    "transmitted_at",
                    "in_force_at",
                    "resolution_text",
                    "parts_count",
                    "actions_count",
                    "old_number",
                    "old_number_display_name",
                    "updated_at",
                ],
                treaties,
            )

        if treaties_country_parties:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "treaties_country_parties",
                ["treaty_id", "country"],
                treaties_country_parties,
            )

        if treaties_index_terms:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "treaties_index_terms",
                ["treaty_id", "index_term"],
                treaties_index_terms,
            )

        if treaties_titles:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "treaties_titles",
                ["treaty_id", "title", "title_type"],
                treaties_titles,
            )

        logger.info(f"Processed {len(treaties)} treaties with all related data")

    def process_committeemeetings_optimized(self, data: list[dict], cursor) -> None:
        """Optimized committee meetings processing."""
        logger.info(f"Processing {len(data)} committeemeetings")

        committeemeetings = []
        committeemeetings_committees = []
        committeemeetings_associated_bills = []
        committeemeetings_associated_treaties = []
        committeemeetings_associated_nominations = []
        committeemeetings_meeting_documents = []
        committeemeetings_witness_documents = []
        committeemeetings_witnesses = []
        committeemeetings_associated_hearings = []

        for item in data:
            meeting_id = item.get("eventId")

            # Parse address information
            location = item.get("location", {}) or {}
            address_raw = item.get("address")
            address = None
            if address_raw:
                try:
                    address = json.loads(address_raw)
                except Exception:
                    address = None

            # Prefer address fields if present, else fallback to location/top-level
            building = (
                address.get("building_name")
                if address and address.get("building_name")
                else location.get("building")
            )
            street_address = (
                address.get("street-address")
                if address and address.get("street-address")
                else None
            )
            city = address.get("city") if address and address.get("city") else None
            state = address.get("state") if address and address.get("state") else None
            zip_code = (
                address.get("postal_code")
                if address and address.get("postal_code")
                else None
            )

            # Main meeting record
            meeting_record = {
                "meeting_id": meeting_id,
                "title": item.get("title"),
                "meeting_type": item.get("type"),
                "chamber": self.utils.standardize_chamber(item.get("chamber")),
                "congress": item.get("congress"),
                "date": self.utils.standardize_date(item.get("date")),
                "room": location.get("room"),
                "street_address": street_address,
                "building": building,
                "city": city,
                "state": state,
                "zip_code": zip_code,
                "meeting_status": item.get("meetingStatus"),
                "updated_at": self.utils.standardize_date(item.get("updateDate")),
            }
            committeemeetings.append(meeting_record)

            # Committees
            for committee in item.get("committees", []):
                committeemeetings_committees.append(
                    {
                        "meeting_id": meeting_id,
                        "committee_code": committee.get("systemCode"),
                    }
                )

            # Associated bills/treaties/nominations
            if item.get("relatedItems", {}):
                if item.get("relatedItems", {}).get("bills"):
                    for bill in item.get("relatedItems", {}).get("bills", []):
                        committeemeetings_associated_bills.append(
                            {
                                "meeting_id": meeting_id,
                                "bill_id": f"{bill.get('type').lower()}{bill.get('number')}-{bill.get('congress')}",
                            }
                        )

                if item.get("relatedItems", {}).get("treaties"):
                    for treaty in item.get("relatedItems", {}).get("treaties", []):
                        committeemeetings_associated_treaties.append(
                            {
                                "meeting_id": meeting_id,
                                "treaty_id": f"td{treaty.get('congress')}-{treaty.get('number')}",
                            }
                        )

                if item.get("relatedItems", {}).get("nominations"):
                    for nomination in item.get("relatedItems", {}).get(
                        "nominations", []
                    ):
                        committeemeetings_associated_nominations.append(
                            {
                                "meeting_id": meeting_id,
                                "nomination_id": f"PN{nomination.get('number')}-{nomination.get('part', '00')}-{nomination.get('congress')}",
                            }
                        )

            # Meeting documents
            for meeting_document in item.get("meetingDocuments", []):
                committeemeetings_meeting_documents.append(
                    {
                        "meeting_id": meeting_id,
                        "name": meeting_document.get("name"),
                        "document_type": meeting_document.get("documentType"),
                        "description": meeting_document.get("description"),
                        "url": meeting_document.get("url"),
                    }
                )

            # Witness documents
            for witness_document in item.get("witnessDocuments", []):
                committeemeetings_witness_documents.append(
                    {
                        "meeting_id": meeting_id,
                        "document_type": witness_document.get("documentType"),
                        "url": witness_document.get("url"),
                    }
                )

            # Witnesses
            for witness in item.get("witnesses", []):
                committeemeetings_witnesses.append(
                    {
                        "meeting_id": meeting_id,
                        "name": witness.get("name"),
                        "position": witness.get("position"),
                        "organization": witness.get("organization"),
                    }
                )

            # Associated hearings
            for hearing in item.get("hearingTranscript", []):
                hearing_id = None
                url = hearing.get("url", "")
                match = re.search(r"/hearing/(\d+)/(.*?)(?:\?|$)", url)
                if match:
                    chamber_code = match.group(2).lower()
                    if chamber_code == "nochamber":
                        chamber_code = "j"
                    else:
                        chamber_code = chamber_code[0]
                    hearing_id = f"{chamber_code}hrg-{match.group(1)}"

                if hearing_id:
                    committeemeetings_associated_hearings.append(
                        {
                            "meeting_id": meeting_id,
                            "hearing_id": hearing_id,
                        }
                    )

        # Remove duplicates
        committeemeetings = self.check_duplicates_optimized(
            committeemeetings, "meeting_id", "committeemeetings"
        )
        committeemeetings_committees = self.check_duplicates_optimized(
            committeemeetings_committees,
            ["meeting_id", "committee_code"],
            "committeemeetings_committees",
        )
        committeemeetings_associated_bills = self.check_duplicates_optimized(
            committeemeetings_associated_bills,
            ["meeting_id", "bill_id"],
            "committeemeetings_associated_bills",
        )
        committeemeetings_associated_treaties = self.check_duplicates_optimized(
            committeemeetings_associated_treaties,
            ["meeting_id", "treaty_id"],
            "committeemeetings_associated_treaties",
        )
        committeemeetings_associated_nominations = self.check_duplicates_optimized(
            committeemeetings_associated_nominations,
            ["meeting_id", "nomination_id"],
            "committeemeetings_associated_nominations",
        )
        committeemeetings_meeting_documents = self.check_duplicates_optimized(
            committeemeetings_meeting_documents,
            ["meeting_id", "url"],
            "committeemeetings_meeting_documents",
        )
        committeemeetings_witness_documents = self.check_duplicates_optimized(
            committeemeetings_witness_documents,
            ["meeting_id", "url"],
            "committeemeetings_witness_documents",
        )
        committeemeetings_witnesses = self.check_duplicates_optimized(
            committeemeetings_witnesses,
            ["meeting_id", "name", "position", "organization"],
            "committeemeetings_witnesses",
        )
        committeemeetings_associated_hearings = self.check_duplicates_optimized(
            committeemeetings_associated_hearings,
            ["meeting_id", "hearing_id"],
            "committeemeetings_associated_hearings",
        )

        # Batch insert
        if committeemeetings:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "committeemeetings",
                [
                    "meeting_id",
                    "title",
                    "meeting_type",
                    "chamber",
                    "congress",
                    "date",
                    "room",
                    "street_address",
                    "building",
                    "city",
                    "state",
                    "zip_code",
                    "meeting_status",
                    "updated_at",
                ],
                committeemeetings,
            )

        if committeemeetings_committees:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "committeemeetings_committees",
                ["meeting_id", "committee_code"],
                committeemeetings_committees,
            )

        if committeemeetings_associated_bills:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "committeemeetings_associated_bills",
                ["meeting_id", "bill_id"],
                committeemeetings_associated_bills,
            )

        if committeemeetings_associated_treaties:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "committeemeetings_associated_treaties",
                ["meeting_id", "treaty_id"],
                committeemeetings_associated_treaties,
            )

        if committeemeetings_associated_nominations:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "committeemeetings_associated_nominations",
                ["meeting_id", "nomination_id"],
                committeemeetings_associated_nominations,
            )

        if committeemeetings_meeting_documents:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "committeemeetings_meeting_documents",
                ["meeting_id", "name", "document_type", "description", "url"],
                committeemeetings_meeting_documents,
            )

        if committeemeetings_witness_documents:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "committeemeetings_witness_documents",
                ["meeting_id", "document_type", "url"],
                committeemeetings_witness_documents,
            )

        if committeemeetings_witnesses:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "committeemeetings_witnesses",
                ["meeting_id", "name", "position", "organization"],
                committeemeetings_witnesses,
            )

        if committeemeetings_associated_hearings:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "committeemeetings_associated_hearings",
                ["meeting_id", "hearing_id"],
                committeemeetings_associated_hearings,
            )

        logger.info(
            f"Processed {len(committeemeetings)} committeemeetings with all related data"
        )

    def process_committeeprints_optimized(self, data: list[dict], cursor) -> None:
        """Optimized committee prints processing."""
        logger.info(f"Processing {len(data)} committeeprints")

        committeeprints = []
        committeeprints_associated_bills = []
        committeeprints_committees = []

        for item in data:
            # Generate print_id
            chamber_code = item.get("chamber", "").lower()
            chamber_code = "j" if chamber_code == "nochamber" else chamber_code[0]

            print_id = f"{chamber_code}prt{str(item.get('jacketNumber', '') or '')}-{str(item.get('congress', '') or '')}"

            # Main print record
            print_record = {
                "print_id": print_id,
                "print_jacketnumber": item.get("jacketNumber"),
                "congress": item.get("congress"),
                "chamber": self.utils.standardize_chamber(item.get("chamber")),
                "title": item.get("title"),
                "print_number": item.get("number"),
                "citation": item.get("citation"),
                "updated_at": self.utils.standardize_date(item.get("updateDate")),
            }
            committeeprints.append(print_record)

            # Associated bills
            for bill in item.get("associatedBills", []):
                committeeprints_associated_bills.append(
                    {
                        "print_id": print_id,
                        "bill_id": f"{bill.get('type').lower()}{bill.get('number')}-{bill.get('congress')}",
                    }
                )

            # Committees
            for committee in item.get("committees", []):
                committeeprints_committees.append(
                    {
                        "print_id": print_id,
                        "committee_code": committee.get("systemCode"),
                    }
                )

        # Remove duplicates
        committeeprints = self.check_duplicates_optimized(
            committeeprints, "print_id", "committeeprints"
        )
        committeeprints_associated_bills = self.check_duplicates_optimized(
            committeeprints_associated_bills,
            ["print_id", "bill_id"],
            "committeeprints_associated_bills",
        )
        committeeprints_committees = self.check_duplicates_optimized(
            committeeprints_committees,
            ["print_id", "committee_code"],
            "committeeprints_committees",
        )

        # Batch insert
        if committeeprints:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "committeeprints",
                [
                    "print_id",
                    "print_jacketnumber",
                    "congress",
                    "chamber",
                    "title",
                    "print_number",
                    "citation",
                    "updated_at",
                ],
                committeeprints,
            )

        if committeeprints_associated_bills:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "committeeprints_associated_bills",
                ["print_id", "bill_id"],
                committeeprints_associated_bills,
            )

        if committeeprints_committees:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "committeeprints_committees",
                ["print_id", "committee_code"],
                committeeprints_committees,
            )

        logger.info(
            f"Processed {len(committeeprints)} committeeprints with all related data"
        )

    def process_committeereports_optimized(self, data: list[dict], cursor) -> None:
        """Optimized committee reports processing."""
        logger.info(f"Processing {len(data)} committeereports")

        committeereports = []
        committeereports_associated_bills = []
        committeereports_associated_treaties = []

        for item in data:
            # Generate report_id
            report_id = (
                f"{item.get('type').lower()}{str(item.get('number', ''))}-"
                f"{item.get('part') or ''}{'-' if item.get('part') else ''}"
                f"{str(item.get('congress', ''))}"
            )

            # Main report record
            report_record = {
                "report_id": report_id,
                "citation": item.get("citation"),
                "report_type": item.get("type"),
                "report_number": item.get("number"),
                "report_part": item.get("part"),
                "congress": item.get("congress"),
                "session": item.get("session"),
                "title": item.get("title"),
                "chamber": self.utils.standardize_chamber(item.get("chamber")),
                "is_conference_report": item.get("isConferenceReport"),
                "issued_at": self.utils.standardize_date(item.get("issueDate")),
                "texts_count": item.get("text", {}).get("count", 0),
                "updated_at": self.utils.standardize_date(item.get("updateDate")),
            }
            committeereports.append(report_record)

            # Associated bills
            for bill in item.get("associatedBill", []):
                committeereports_associated_bills.append(
                    {
                        "report_id": report_id,
                        "bill_id": f"{bill.get('type').lower()}{bill.get('number')}-{bill.get('congress')}",
                    }
                )

            # Associated treaties
            for treaty in item.get("associatedTreaties", []):
                committeereports_associated_treaties.append(
                    {
                        "report_id": report_id,
                        "treaty_id": f"td{treaty.get('congress')}-{treaty.get('number')}{treaty.get('part') or ''}",
                    }
                )

        # Remove duplicates
        committeereports = self.check_duplicates_optimized(
            committeereports, "report_id", "committeereports"
        )
        committeereports_associated_bills = self.check_duplicates_optimized(
            committeereports_associated_bills,
            ["report_id", "bill_id"],
            "committeereports_associated_bills",
        )
        committeereports_associated_treaties = self.check_duplicates_optimized(
            committeereports_associated_treaties,
            ["report_id", "treaty_id"],
            "committeereports_associated_treaties",
        )

        # Batch insert
        if committeereports:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "committeereports",
                [
                    "report_id",
                    "citation",
                    "report_type",
                    "report_number",
                    "report_part",
                    "congress",
                    "session",
                    "title",
                    "chamber",
                    "is_conference_report",
                    "issued_at",
                    "texts_count",
                    "updated_at",
                ],
                committeereports,
            )

        if committeereports_associated_bills:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "committeereports_associated_bills",
                ["report_id", "bill_id"],
                committeereports_associated_bills,
            )

        if committeereports_associated_treaties:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "committeereports_associated_treaties",
                ["report_id", "treaty_id"],
                committeereports_associated_treaties,
            )

        logger.info(
            f"Processed {len(committeereports)} committeereports with all related data"
        )

    def process_amendments_actions_optimized(self, data: list[dict], cursor) -> None:
        """Optimized amendments actions processing."""
        logger.info(f"Processing {len(data)} amendments_actions")

        actions = []
        amendments_actions_recorded_votes = []

        for item in data:
            action_id = hashlib.sha256(json.dumps(item).encode()).hexdigest()[:16]

            # Main action record
            action_record = {
                "action_id": action_id,
                "amendment_id": item.get("amendments_id"),
                "action_code": item.get("actionCode"),
                "action_date": self.utils.standardize_date(item.get("actionDate")),
                "text": self.utils.sanitize_for_copy(item.get("text")),
                "action_type": item.get("type"),
                "source_system": item.get("sourceSystem"),
                "source_system_code": item.get("sourceSystemCode"),
            }
            actions.append(action_record)

            # Recorded votes
            recorded_votes = item.get("recordedVotes")
            if recorded_votes and isinstance(recorded_votes, list):
                for recorded_vote in recorded_votes:
                    if recorded_vote is not None:
                        amendments_actions_recorded_votes.append(
                            {
                                "action_id": action_id,
                                "amendment_id": item.get("amendments_id"),
                                "chamber": recorded_vote.get("chamber"),
                                "congress": recorded_vote.get("congress"),
                                "date": self.utils.standardize_date(
                                    recorded_vote.get("date")
                                ),
                                "roll_number": recorded_vote.get("rollNumber"),
                                "session": recorded_vote.get("session"),
                                "url": recorded_vote.get("url"),
                            }
                        )

        # Remove duplicates
        actions = self.check_duplicates_optimized(
            actions, ["action_id", "amendment_id"], "amendments_actions"
        )
        amendments_actions_recorded_votes = self.check_duplicates_optimized(
            amendments_actions_recorded_votes,
            [
                "action_id",
                "amendment_id",
                "chamber",
                "congress",
                "date",
                "roll_number",
                "session",
                "url",
            ],
            "amendments_actions_recorded_votes",
        )

        # Batch insert
        if actions:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "amendments_actions",
                [
                    "action_id",
                    "amendment_id",
                    "action_code",
                    "action_date",
                    "text",
                    "action_type",
                    "source_system",
                    "source_system_code",
                ],
                actions,
            )

        if amendments_actions_recorded_votes:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "amendments_actions_recorded_votes",
                [
                    "action_id",
                    "amendment_id",
                    "chamber",
                    "congress",
                    "date",
                    "roll_number",
                    "session",
                    "url",
                ],
                amendments_actions_recorded_votes,
            )

        logger.info(
            f"Processed {len(actions)} amendments_actions with {len(amendments_actions_recorded_votes)} recorded votes"
        )

    def process_amendments_cosponsors_optimized(self, data: list[dict], cursor) -> None:
        """Optimized amendments cosponsors processing."""
        logger.info(f"Processing {len(data)} amendments_cosponsors")

        cosponsors = [
            {
                "amendment_id": item.get("amendments_id"),
                "bioguide_id": item.get("bioguideId"),
            }
            for item in data
        ]

        cosponsors = self.check_duplicates_optimized(
            cosponsors, ["amendment_id", "bioguide_id"], "amendments_cosponsors"
        )

        if cosponsors:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "amendments_cosponsors",
                ["amendment_id", "bioguide_id"],
                cosponsors,
            )

        logger.info(f"Processed {len(cosponsors)} amendments_cosponsors")

    def process_amendments_texts_optimized(self, data: list[dict], cursor) -> None:
        """Optimized amendments texts processing."""
        logger.info(f"Processing {len(data)} amendments_texts")

        texts = [
            {
                "amendment_id": item.get("amendments_id"),
                "date": self.utils.standardize_date(item.get("date")),
                "type": item.get("type"),
                "raw_text": None,
                "html": next(
                    (
                        fmt.get("url")
                        for fmt in item.get("formats", [])
                        if fmt.get("type") == "HTML"
                    ),
                    None,
                ),
                "pdf": next(
                    (
                        fmt.get("url")
                        for fmt in item.get("formats", [])
                        if fmt.get("type") == "PDF"
                    ),
                    None,
                ),
            }
            for item in data
        ]

        texts = self.check_duplicates_optimized(
            texts, ["amendment_id"], "amendments_texts"
        )

        if texts:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "amendments_texts",
                ["amendment_id", "date", "type", "raw_text", "html", "pdf"],
                texts,
            )

        logger.info(f"Processed {len(texts)} amendments_texts")

    def process_bills_actions_optimized(self, data: list[dict], cursor) -> None:
        """Optimized bills actions processing."""
        logger.info(f"Processing {len(data)} bills_actions")

        bills_actions = []
        bills_actions_committees = []
        bills_actions_recorded_votes = []

        for item in data:
            action_id = hashlib.sha256(json.dumps(item).encode()).hexdigest()[:16]
            bill_id = item.get("bills_id")

            # Main action record
            action_record = {
                "action_id": action_id,
                "bill_id": bill_id,
                "action_code": item.get("actionCode"),
                "action_date": self.utils.standardize_date(item.get("actionDate")),
                "text": self.utils.sanitize_for_copy(item.get("text")),
                "action_type": item.get("type"),
                "source_system": item.get("sourceSystem"),
                "source_system_code": item.get("sourceSystemCode"),
                "calendar": item.get("calendarNumber", {}).get("calendar")
                if item.get("calendarNumber")
                else None,
                "calendar_number": item.get("calendarNumber", {}).get("number")
                if item.get("calendarNumber")
                else None,
            }
            bills_actions.append(action_record)

            # Committees
            for committee in item.get("committees", []):
                bills_actions_committees.append(
                    {
                        "action_id": action_id,
                        "bill_id": bill_id,
                        "committee_code": committee.get("systemCode"),
                    }
                )

            # Recorded votes
            for recorded_vote in item.get("recordedVotes", []):
                bills_actions_recorded_votes.append(
                    {
                        "action_id": action_id,
                        "bill_id": bill_id,
                        "chamber": self.utils.standardize_chamber(
                            recorded_vote.get("chamber")
                        ),
                        "congress": recorded_vote.get("congress"),
                        "date": self.utils.standardize_date(recorded_vote.get("date")),
                        "roll_number": recorded_vote.get("rollNumber"),
                        "session": recorded_vote.get("session"),
                        "url": recorded_vote.get("url"),
                    }
                )

        # Remove duplicates
        bills_actions = self.check_duplicates_optimized(
            bills_actions, ["action_id", "bill_id"], "bills_actions"
        )
        bills_actions_committees = self.check_duplicates_optimized(
            bills_actions_committees,
            ["action_id", "committee_code"],
            "bills_actions_committees",
        )
        bills_actions_recorded_votes = self.check_duplicates_optimized(
            bills_actions_recorded_votes,
            ["action_id", "bill_id", "url"],
            "bills_actions_recorded_votes",
        )

        # Batch insert
        if bills_actions:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "bills_actions",
                [
                    "action_id",
                    "bill_id",
                    "action_code",
                    "action_date",
                    "text",
                    "action_type",
                    "source_system",
                    "source_system_code",
                    "calendar",
                    "calendar_number",
                ],
                bills_actions,
                batch_size=500,  # Smaller batch size for problematic data
            )

        if bills_actions_committees:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "bills_actions_committees",
                ["action_id", "bill_id", "committee_code"],
                bills_actions_committees,
            )

        if bills_actions_recorded_votes:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "bills_actions_recorded_votes",
                [
                    "action_id",
                    "bill_id",
                    "chamber",
                    "congress",
                    "date",
                    "roll_number",
                    "session",
                    "url",
                ],
                bills_actions_recorded_votes,
            )

        logger.info(
            f"Processed {len(bills_actions)} bills_actions with {len(bills_actions_committees)} committees and {len(bills_actions_recorded_votes)} recorded votes"
        )

    def process_bills_summaries_optimized(self, data: list[dict], cursor) -> None:
        """Optimized bills summaries processing."""
        logger.info(f"Processing {len(data)} bills_summaries")

        bills_summaries = [
            {
                "bill_id": item.get("bills_id"),
                "action_date": self.utils.standardize_date(item.get("actionDate")),
                "action_desc": item.get("actionDesc"),
                "text": item.get("text"),
                "version_code": item.get("versionCode"),
            }
            for item in data
        ]

        bills_summaries = self.check_duplicates_optimized(
            bills_summaries,
            ["bill_id", "action_date", "action_desc", "version_code"],
            "bills_summaries",
        )

        if bills_summaries:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "bills_summaries",
                ["bill_id", "action_date", "action_desc", "text", "version_code"],
                bills_summaries,
            )

        logger.info(f"Processed {len(bills_summaries)} bills_summaries")

    def process_bills_subjects_optimized(self, data: list[dict], cursor) -> None:
        """Optimized bills subjects processing."""
        logger.info(f"Processing {len(data)} bills_subjects")

        bills_subjects = [
            {
                "bill_id": item.get("bills_id"),
                "subject": item.get("name"),
                "updated_at": self.utils.standardize_date(item.get("updateDate")),
            }
            for item in data
        ]

        bills_subjects = self.check_duplicates_optimized(
            bills_subjects, ["bill_id", "subject"], "bills_subjects"
        )

        if bills_subjects:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "bills_subjects",
                ["bill_id", "subject", "updated_at"],
                bills_subjects,
            )

        logger.info(f"Processed {len(bills_subjects)} bills_subjects")

    def process_bills_titles_optimized(self, data: list[dict], cursor) -> None:
        """Optimized bills titles processing."""
        logger.info(f"Processing {len(data)} bills_titles")

        bills_titles = [
            {
                "bill_id": item.get("bills_id"),
                "title": item.get("title"),
                "title_type": item.get("titleType"),
                "bill_text_version_code": item.get("billTextVersionCode"),
                "bill_text_version_name": item.get("billTextVersionName"),
                "chamber": self.utils.standardize_chamber(item.get("chamber"))
                if item.get("chamber")
                else None,
                "title_type_code": item.get("titleTypeCode"),
            }
            for item in data
        ]

        bills_titles = self.check_duplicates_optimized(
            bills_titles, ["bill_id", "title", "title_type_code"], "bills_titles"
        )

        if bills_titles:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "bills_titles",
                [
                    "bill_id",
                    "title",
                    "title_type",
                    "bill_text_version_code",
                    "bill_text_version_name",
                    "chamber",
                    "title_type_code",
                ],
                bills_titles,
            )

        logger.info(f"Processed {len(bills_titles)} bills_titles")

    def process_bills_texts_optimized(self, data: list[dict], cursor) -> None:
        """Optimized bills texts processing."""
        logger.info(f"Processing {len(data)} bills_texts")

        bills_texts = [
            {
                "bill_id": item.get("bills_id"),
                "date": self.utils.standardize_date(item.get("date")),
                "type": item.get("type"),
                "raw_text": None,
                "formatted_text": next(
                    (
                        fmt.get("url")
                        for fmt in item.get("formats", [])
                        if fmt.get("type") == "Formatted Text"
                    ),
                    None,
                ),
                "pdf": next(
                    (
                        fmt.get("url")
                        for fmt in item.get("formats", [])
                        if fmt.get("type") == "PDF"
                    ),
                    None,
                ),
                "xml": next(
                    (
                        fmt.get("url")
                        for fmt in item.get("formats", [])
                        if fmt.get("type") == "FormattedXML"
                    ),
                    None,
                ),
            }
            for item in data
        ]

        bills_texts = self.check_duplicates_optimized(
            bills_texts, ["bill_id", "date", "type"], "bills_texts"
        )

        if bills_texts:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "bills_texts",
                ["bill_id", "date", "type", "raw_text", "formatted_text", "pdf", "xml"],
                bills_texts,
            )

        logger.info(f"Processed {len(bills_texts)} bills_texts")

    def process_bills_cosponsors_optimized(self, data: list[dict], cursor) -> None:
        """Optimized bills cosponsors processing."""
        logger.info(f"Processing {len(data)} bills_cosponsors")

        bills_cosponsors = [
            {
                "bill_id": item.get("bills_id"),
                "bioguide_id": item.get("bioguideId"),
            }
            for item in data
        ]

        bills_cosponsors = self.check_duplicates_optimized(
            bills_cosponsors, ["bill_id", "bioguide_id"], "bills_cosponsors"
        )

        if bills_cosponsors:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "bills_cosponsors",
                ["bill_id", "bioguide_id"],
                bills_cosponsors,
            )

        logger.info(f"Processed {len(bills_cosponsors)} bills_cosponsors")

    def process_bills_relatedbills_optimized(self, data: list[dict], cursor) -> None:
        """Optimized bills related bills processing."""
        logger.info(f"Processing {len(data)} bills_relatedbills")

        bills_relatedbills = [
            {
                "bill_id": item.get("bills_id"),
                "related_bill_id": item.get("relatedbill_id"),
                "identification_entity": item.get("relationship_identified_by"),
            }
            for item in data
        ]

        bills_relatedbills = self.check_duplicates_optimized(
            bills_relatedbills, ["bill_id", "related_bill_id"], "bills_relatedbills"
        )

        if bills_relatedbills:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "bills_related_bills",
                ["bill_id", "related_bill_id", "identification_entity"],
                bills_relatedbills,
            )

        logger.info(f"Processed {len(bills_relatedbills)} bills_relatedbills")

    def process_committeeprints_texts_optimized(self, data: list[dict], cursor) -> None:
        """Optimized committee prints texts processing."""
        logger.info(f"Processing {len(data)} committeeprints_texts")

        # Group by source doc id
        data_by_source_doc_id = {}
        for item in data:
            source_doc_id = item.get("committeeprints_id")
            if source_doc_id not in data_by_source_doc_id:
                data_by_source_doc_id[source_doc_id] = []
            data_by_source_doc_id[source_doc_id].append(item)

        # Process grouped data
        type_map = {
            "PDF": "pdf",
            "Formatted Text": "formatted_text",
            "Generated   HTML": "html",
            "Formatted XML": "xml",
            "Portable Network Graphics": "png",
        }

        committeeprints_texts = []
        for source_doc_id, items in data_by_source_doc_id.items():
            entry = {
                "print_id": source_doc_id,
                "raw_text": None,
                "formatted_text": None,
                "pdf": None,
                "html": None,
                "xml": None,
                "png": None,
            }
            for item in items:
                key = type_map.get(item.get("type"))
                if key:
                    entry[key] = item.get("url")
            committeeprints_texts.append(entry)

        committeeprints_texts = self.check_duplicates_optimized(
            committeeprints_texts,
            ["print_id", "formatted_text", "pdf", "html", "xml"],
            "committeeprints_texts",
        )

        if committeeprints_texts:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "committeeprints_texts",
                ["print_id", "raw_text", "formatted_text", "pdf", "html", "xml", "png"],
                committeeprints_texts,
            )

        logger.info(f"Processed {len(committeeprints_texts)} committeeprints_texts")

    def process_committeereports_texts_optimized(
        self, data: list[dict], cursor
    ) -> None:
        """Optimized committee reports texts processing."""
        logger.info(f"Processing {len(data)} committeereports_texts")

        # Group all formats by committeereports_id
        from collections import defaultdict

        grouped = defaultdict(list)
        for item in data:
            report_id = item.get("committeereports_id")
            formats = item.get("formats", [])
            if isinstance(formats, list):
                grouped[report_id].extend(formats)

        rows = []
        for report_id, formats in grouped.items():
            # Group formats by type
            pdfs = []
            formatted_texts = []
            for f in formats:
                url = f.get("url")
                ftype = f.get("type", "").strip().lower()
                is_errata = f.get("isErrata", "").upper() == "Y"
                if ftype == "pdf":
                    pdfs.append({"url": url, "is_errata": is_errata})
                elif ftype == "formatted text":
                    formatted_texts.append({"url": url, "is_errata": is_errata})

            # Extract part numbers for pairing
            def extract_part(url):
                import re

                m = re.search(r"-pt(\d+)", url or "")
                return m.group(1) if m else None

            # Build mappings by part number
            pdf_by_part = {extract_part(pdf["url"]): pdf for pdf in pdfs}
            formatted_by_part = {extract_part(ft["url"]): ft for ft in formatted_texts}

            # Union of all part numbers
            all_parts = set(pdf_by_part.keys()) | set(formatted_by_part.keys())
            if not all_parts:
                # If no part numbers, do all combinations
                if pdfs or formatted_texts:
                    for pdf in pdfs or [None]:
                        for ft in formatted_texts or [None]:
                            rows.append(
                                {
                                    "report_id": report_id,
                                    "raw_text": None,
                                    "formatted_text": ft["url"] if ft else None,
                                    "formatted_text_is_errata": ft["is_errata"]
                                    if ft
                                    else None,
                                    "pdf": pdf["url"] if pdf else None,
                                    "pdf_is_errata": pdf["is_errata"] if pdf else None,
                                }
                            )
                continue

            for part in all_parts:
                pdf = pdf_by_part.get(part)
                ft = formatted_by_part.get(part)
                rows.append(
                    {
                        "report_id": report_id,
                        "raw_text": None,
                        "formatted_text": ft["url"] if ft else None,
                        "formatted_text_is_errata": ft["is_errata"] if ft else None,
                        "pdf": pdf["url"] if pdf else None,
                        "pdf_is_errata": pdf["is_errata"] if pdf else None,
                    }
                )

        # Remove duplicates
        seen = set()
        deduped_rows = []
        for row in rows:
            key = (row["report_id"], row["formatted_text"], row["pdf"])
            if key not in seen:
                seen.add(key)
                deduped_rows.append(row)

        if deduped_rows:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "committeereports_texts",
                [
                    "report_id",
                    "raw_text",
                    "formatted_text",
                    "formatted_text_is_errata",
                    "pdf",
                    "pdf_is_errata",
                ],
                deduped_rows,
            )

        logger.info(f"Processed {len(deduped_rows)} committeereports_texts")

    def process_committees_committeereports_optimized(
        self, data: list[dict], cursor
    ) -> None:
        """Optimized committees committee reports processing."""
        logger.info(f"Processing {len(data)} committees_committeereports")

        committees_committeereports = [
            {
                "committee_code": item.get("committees_id"),
                "report_id": (
                    f"{item.get('type').lower()}{str(item.get('number', ''))}-"
                    f"{item.get('part') or ''}{'-' if item.get('part') else ''}"
                    f"{str(item.get('congress', ''))}"
                ),
            }
            for item in data
        ]

        committees_committeereports = self.check_duplicates_optimized(
            committees_committeereports,
            ["committee_code", "report_id"],
            "committees_committeereports",
        )

        if committees_committeereports:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "committees_committeereports",
                ["committee_code", "report_id"],
                committees_committeereports,
            )

        logger.info(
            f"Processed {len(committees_committeereports)} committees_committeereports"
        )

    def process_committees_bills_optimized(self, data: list[dict], cursor) -> None:
        """Optimized committees bills processing."""
        logger.info(f"Processing {len(data)} committees_bills")

        committees_bills = [
            {
                "committee_code": item.get("committees_id"),
                "bill_id": f"{item.get('type').lower()}{str(item.get('number', '').replace('½', '.5'))}-{str(item.get('congress', ''))}",
                "relationship_type": item.get("relationshipType"),
                "committee_action_date": self.utils.standardize_date(
                    item.get("actionDate")
                ),
                "updated_at": self.utils.standardize_date(item.get("updateDate")),
            }
            for item in data
        ]

        committees_bills = self.check_duplicates_optimized(
            committees_bills,
            ["committee_code", "bill_id", "relationship_type", "committee_action_date"],
            "committees_bills",
        )

        if committees_bills:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "committees_bills",
                [
                    "committee_code",
                    "bill_id",
                    "relationship_type",
                    "committee_action_date",
                    "updated_at",
                ],
                committees_bills,
            )

        logger.info(f"Processed {len(committees_bills)} committees_bills")

    def process_congresses_optimized(self, data: list[dict], cursor) -> None:
        """Optimized congresses processing."""
        logger.info(f"Processing {len(data)} congresses")

        congresses = []
        congresses_sessions = []

        for item in data:
            # Top-level congress fields
            congress_number = item.get("number")
            congress_name = item.get("name")
            start_year = item.get("startYear")
            end_year = item.get("endYear")
            updated_at = item.get("updateDate")

            # Insert into congresses table
            congresses.append(
                {
                    "congress_number": congress_number,
                    "name": congress_name,
                    "start_year": start_year,
                    "end_year": end_year,
                    "updated_at": self.utils.standardize_date(updated_at),
                }
            )

            # Sessions (if present)
            if item.get("sessions"):
                for session in item.get("sessions"):
                    session_number = session.get("number")
                    chamber = session.get("chamber")
                    session_type = session.get("type")
                    start_date = session.get("startDate")
                    end_date = session.get("endDate")
                    congresses_sessions.append(
                        {
                            "congress_number": congress_number,
                            "session": session_number,
                            "chamber": chamber,
                            "type": session_type,
                            "start_date": self.utils.standardize_date(start_date),
                            "end_date": self.utils.standardize_date(end_date),
                        }
                    )

        congresses = self.check_duplicates_optimized(
            congresses, ["congress_number"], "congresses"
        )

        if congresses:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "congresses",
                ["congress_number", "name", "start_year", "end_year", "updated_at"],
                congresses,
            )

        if congresses_sessions:
            congresses_sessions = self.check_duplicates_optimized(
                congresses_sessions,
                ["congress_number", "session", "chamber", "type"],
                "congresses_sessions",
            )
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "congresses_sessions",
                [
                    "congress_number",
                    "session",
                    "chamber",
                    "type",
                    "start_date",
                    "end_date",
                ],
                congresses_sessions,
            )

        logger.info(
            f"Processed {len(congresses)} congresses with {len(congresses_sessions)} sessions"
        )

    def process_nominations_actions_optimized(self, data: list[dict], cursor) -> None:
        """Optimized nominations actions processing."""
        logger.info(f"Processing {len(data)} nominations_actions")

        nominations_actions = []
        nominations_actions_committees = []

        for item in data:
            action_id = hashlib.sha256(json.dumps(item).encode()).hexdigest()[:16]
            nomination_id = item.get("nominations_id")

            # Main action record
            action_record = {
                "action_id": action_id,
                "nomination_id": nomination_id,
                "action_code": item.get("actionCode"),
                "action_type": item.get("type"),
                "action_date": self.utils.standardize_date(item.get("date")),
                "text": self.utils.sanitize_for_copy(item.get("text")),
            }
            nominations_actions.append(action_record)

            # Committees
            for committee in item.get("committees", []):
                nominations_actions_committees.append(
                    {
                        "action_id": action_id,
                        "nomination_id": nomination_id,
                        "committee_code": committee.get("systemCode"),
                    }
                )

        # Remove duplicates
        nominations_actions = self.check_duplicates_optimized(
            nominations_actions, ["action_id", "nomination_id"], "nominations_actions"
        )
        nominations_actions_committees = self.check_duplicates_optimized(
            nominations_actions_committees,
            ["action_id", "nomination_id", "committee_code"],
            "nominations_actions_committees",
        )

        # Batch insert
        if nominations_actions:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "nominations_actions",
                [
                    "action_id",
                    "nomination_id",
                    "action_code",
                    "action_type",
                    "action_date",
                    "text",
                ],
                nominations_actions,
            )

        if nominations_actions_committees:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "nominations_actions_committees",
                ["action_id", "nomination_id", "committee_code"],
                nominations_actions_committees,
            )

        logger.info(
            f"Processed {len(nominations_actions)} nominations_actions with {len(nominations_actions_committees)} committees"
        )

    def process_nominations_committeeactivities_optimized(
        self, data: list[dict], cursor
    ) -> None:
        """Optimized nominations committee activities processing."""
        logger.info(f"Processing {len(data)} nominations_committeeactivities")

        nominations_committeeactivities = [
            {
                "nomination_id": item.get("nominations_id"),
                "committee_code": item.get("committee_code"),
                "activity_name": item.get("activity_name"),
                "activity_date": self.utils.standardize_date(item.get("activity_date")),
            }
            for item in data
            if item.get("committee_code", "")[-2:] != "00"  # Skip subcommittees
        ]

        nominations_committeeactivities = self.check_duplicates_optimized(
            nominations_committeeactivities,
            ["nomination_id", "committee_code", "activity_date", "activity_name"],
            "nominations_committeeactivities",
        )

        if nominations_committeeactivities:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "nominations_committeeactivities",
                ["nomination_id", "committee_code", "activity_name", "activity_date"],
                nominations_committeeactivities,
            )

        logger.info(
            f"Processed {len(nominations_committeeactivities)} nominations_committeeactivities"
        )

    def process_nominations_hearings_optimized(self, data: list[dict], cursor) -> None:
        """Optimized nominations hearings processing."""
        logger.info(f"Processing {len(data)} nominations_hearings")

        nominations_associated_hearings = []

        for item in data:
            chamber = item.get("chamber")
            jacketnumber = item.get("jacketNumber")
            citation = item.get("citation")

            if citation:
                numbers = citation.split(".")[2]
                if numbers:
                    congress = numbers.split("-")[0]
                    if chamber and jacketnumber and congress:
                        hearing_id = f"{chamber[0].lower()}hrg{jacketnumber}-{congress}"
                    else:
                        hearing_id = citation
                else:
                    hearing_id = citation
            else:
                hearing_id = "ID_ERROR"

            nominations_associated_hearings.append(
                {
                    "nomination_id": item.get("nominations_id"),
                    "hearing_id": hearing_id,
                }
            )

        nominations_associated_hearings = self.check_duplicates_optimized(
            nominations_associated_hearings,
            ["nomination_id", "hearing_id"],
            "nominations_hearings",
        )

        if nominations_associated_hearings:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "nominations_associated_hearings",
                ["nomination_id", "hearing_id"],
                nominations_associated_hearings,
            )

        logger.info(
            f"Processed {len(nominations_associated_hearings)} nominations_associated_hearings"
        )

    def process_nominations_nominees_optimized(self, data: list[dict], cursor) -> None:
        """Optimized nominations nominees processing."""
        logger.info(f"Processing {len(data)} nominations_nominees")

        nominations_nominees = [
            {
                "nomination_id": item.get("nominations_id"),
                "ordinal": item.get("position_id"),
                "first_name": item.get("firstName"),
                "middle_name": item.get("middleName"),
                "last_name": item.get("lastName"),
                "prefix": item.get("prefix"),
                "suffix": item.get("suffix"),
                "state": item.get("state"),
                "effective_date": self.utils.standardize_date(
                    item.get("effectiveDate")
                ),
                "predecessor_name": item.get("predecessorName"),
                "corps_code": item.get("corpsCode"),
            }
            for item in data
        ]

        nominations_nominees = self.check_duplicates_optimized(
            nominations_nominees,
            ["nomination_id", "ordinal", "first_name", "middle_name", "last_name"],
            "nominations_nominees",
        )

        if nominations_nominees:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "nominations_nominees",
                [
                    "nomination_id",
                    "ordinal",
                    "first_name",
                    "middle_name",
                    "last_name",
                    "prefix",
                    "suffix",
                    "state",
                    "effective_date",
                    "predecessor_name",
                    "corps_code",
                ],
                nominations_nominees,
            )

        logger.info(f"Processed {len(nominations_nominees)} nominations_nominees")

    def process_treaties_actions_optimized(self, data: list[dict], cursor) -> None:
        """Optimized treaties actions processing."""
        logger.info(f"Processing {len(data)} treaties_actions")

        treaties_actions = []
        treaties_actions_committees = []

        for item in data:
            action_id = hashlib.sha256(json.dumps(item).encode()).hexdigest()[:16]
            treaty_id = item.get("treaty_id")

            # Main action record
            action_record = {
                "action_id": action_id,
                "treaty_id": treaty_id,
                "action_code": item.get("actionCode"),
                "action_date": self.utils.standardize_date(item.get("date")),
                "text": self.utils.sanitize_for_copy(item.get("text")),
                "action_type": item.get("type"),
            }
            treaties_actions.append(action_record)

            # Committees
            for committee in item.get("committees", []):
                treaties_actions_committees.append(
                    {
                        "action_id": action_id,
                        "treaty_id": treaty_id,
                        "committee_code": committee.get("systemCode"),
                    }
                )

        # Remove duplicates
        treaties_actions = self.check_duplicates_optimized(
            treaties_actions, ["action_id", "treaty_id"], "treaties_actions"
        )
        treaties_actions_committees = self.check_duplicates_optimized(
            treaties_actions_committees,
            ["action_id", "treaty_id", "committee_code"],
            "treaties_actions_committees",
        )

        # Batch insert
        if treaties_actions:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "treaties_actions",
                [
                    "action_id",
                    "treaty_id",
                    "action_code",
                    "action_date",
                    "text",
                    "action_type",
                ],
                treaties_actions,
            )

        if treaties_actions_committees:
            OptimizedUtils.batch_copy_dicts_to_table(
                cursor,
                "staging_congressional",
                "treaties_actions_committees",
                ["action_id", "treaty_id", "committee_code"],
                treaties_actions_committees,
            )

        logger.info(
            f"Processed {len(treaties_actions)} treaties_actions with {len(treaties_actions_committees)} committees"
        )

    def process_data_parallel(
        self, data_type: str, data: list[dict], cursor, max_workers: int = 4
    ) -> None:
        """Process data using optimized processors."""
        if data_type == "amendments":
            self.process_amendments_optimized(data, cursor)
        elif data_type == "amendments_actions":
            self.process_amendments_actions_optimized(data, cursor)
        elif data_type == "amendments_cosponsors":
            self.process_amendments_cosponsors_optimized(data, cursor)
        elif data_type == "amendments_texts":
            self.process_amendments_texts_optimized(data, cursor)
        elif data_type == "bills":
            self.process_bills_optimized(data, cursor)
        elif data_type == "bills_actions":
            self.process_bills_actions_optimized(data, cursor)
        elif data_type == "bills_summaries":
            self.process_bills_summaries_optimized(data, cursor)
        elif data_type == "bills_subjects":
            self.process_bills_subjects_optimized(data, cursor)
        elif data_type == "bills_titles":
            self.process_bills_titles_optimized(data, cursor)
        elif data_type == "bills_texts":
            self.process_bills_texts_optimized(data, cursor)
        elif data_type == "bills_cosponsors":
            self.process_bills_cosponsors_optimized(data, cursor)
        elif data_type == "bills_relatedbills":
            self.process_bills_relatedbills_optimized(data, cursor)
        elif data_type == "committees":
            self.process_committees_optimized(data, cursor)
        elif data_type == "committees_committeereports":
            self.process_committees_committeereports_optimized(data, cursor)
        elif data_type == "committees_bills":
            self.process_committees_bills_optimized(data, cursor)
        elif data_type == "congresses":
            self.process_congresses_optimized(data, cursor)
        elif data_type == "hearings":
            self.process_hearings_optimized(data, cursor)
        elif data_type == "members":
            self.process_members_optimized(data, cursor)
        elif data_type == "nominations":
            self.process_nominations_optimized(data, cursor)
        elif data_type == "nominations_actions":
            self.process_nominations_actions_optimized(data, cursor)
        elif data_type == "nominations_committeeactivities":
            self.process_nominations_committeeactivities_optimized(data, cursor)
        elif data_type == "nominations_hearings":
            self.process_nominations_hearings_optimized(data, cursor)
        elif data_type == "nominations_nominees":
            self.process_nominations_nominees_optimized(data, cursor)
        elif data_type == "treaties":
            self.process_treaties_optimized(data, cursor)
        elif data_type == "treaties_actions":
            self.process_treaties_actions_optimized(data, cursor)
        elif data_type == "committeemeetings":
            self.process_committeemeetings_optimized(data, cursor)
        elif data_type == "committeeprints":
            self.process_committeeprints_optimized(data, cursor)
        elif data_type == "committeeprints_texts":
            self.process_committeeprints_texts_optimized(data, cursor)
        elif data_type == "committeereports":
            self.process_committeereports_optimized(data, cursor)
        elif data_type == "committeereports_texts":
            self.process_committeereports_texts_optimized(data, cursor)
        else:
            # Fall back to original processing for other types
            logger.warning(
                f"No optimized processor for {data_type}, using original logic"
            )
            # You would need to implement the original processing here


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
    """Main function with optimized processing."""
    processor = OptimizedDataProcessor()
    conn = connect_to_database()
    cursor = conn.cursor()
    conn.autocommit = True

    # Process amendments
    data = processor.get_data_from_raw_table(
        "amendments", "amendments_raw", cursor, schema="bicam_raw_congressional"
    )
    processor.process_data_parallel("amendments", data, cursor)

    # Process amendments_actions
    data = processor.get_data_from_raw_table(
        "amendments_actions",
        "amendments_actions_raw",
        cursor,
        schema="bicam_raw_congressional",
        related=True,
    )
    processor.process_data_parallel("amendments_actions", data, cursor)

    # Process amendments_cosponsors
    data = processor.get_data_from_raw_table(
        "amendments_cosponsors",
        "amendments_cosponsors_raw",
        cursor,
        schema="bicam_raw_congressional",
        related=True,
    )
    processor.process_data_parallel("amendments_cosponsors", data, cursor)

    # Process amendments_texts
    data = processor.get_data_from_raw_table(
        "amendments_texts",
        "amendments_texts_raw",
        cursor,
        schema="bicam_raw_congressional",
        related=True,
    )
    processor.process_data_parallel("amendments_texts", data, cursor)

    # Process committees
    data = processor.get_data_from_raw_table(
        "committees", "committees_raw", cursor, schema="bicam_raw_congressional"
    )
    processor.process_data_parallel("committees", data, cursor)

    # Process committees_committeereports
    data = processor.get_data_from_raw_table(
        "committees_committeereports",
        "committees_committeereports_raw",
        cursor,
        schema="bicam_raw_congressional",
        related=True,
    )
    processor.process_data_parallel("committees_committeereports", data, cursor)

    # Process committees_bills
    data = processor.get_data_from_raw_table(
        "committees_bills",
        "committees_bills_raw",
        cursor,
        schema="bicam_raw_congressional",
        related=True,
    )
    processor.process_data_parallel("committees_bills", data, cursor)

    # Process congresses
    data = processor.get_data_from_raw_table(
        "congresses", "congresses_raw", cursor, schema="bicam_raw_congressional"
    )
    processor.process_data_parallel("congresses", data, cursor)

    # Process hearings
    data = processor.get_data_from_raw_table(
        "hearings", "hearings_raw", cursor, schema="bicam_raw_congressional"
    )
    processor.process_data_parallel("hearings", data, cursor)

    # Process members
    data = processor.get_data_from_raw_table(
        "members", "members_raw", cursor, schema="bicam_raw_congressional"
    )
    processor.process_data_parallel("members", data, cursor)

    # Process nominations
    data = processor.get_data_from_raw_table(
        "nominations", "nominations_raw", cursor, schema="bicam_raw_congressional"
    )
    processor.process_data_parallel("nominations", data, cursor)

    # Process nominations_actions
    data = processor.get_data_from_raw_table(
        "nominations_actions",
        "nominations_actions_raw",
        cursor,
        schema="bicam_raw_congressional",
        related=True,
    )
    processor.process_data_parallel("nominations_actions", data, cursor)

    # Process nominations_committeeactivities
    data = processor.get_data_from_raw_table(
        "nominations_committeeactivities",
        "nominations_committeeactivities_raw",
        cursor,
        schema="bicam_raw_congressional",
        related=True,
    )
    processor.process_data_parallel("nominations_committeeactivities", data, cursor)

    # Process nominations_hearings
    data = processor.get_data_from_raw_table(
        "nominations_hearings",
        "nominations_hearings_raw",
        cursor,
        schema="bicam_raw_congressional",
        related=True,
    )
    processor.process_data_parallel("nominations_hearings", data, cursor)

    # Process nominations_nominees
    data = processor.get_data_from_raw_table(
        "nominations_nominees",
        "nominations_individualnominees_raw",
        cursor,
        schema="bicam_raw_congressional",
        related=True,
    )
    processor.process_data_parallel("nominations_nominees", data, cursor)

    # Process treaties
    data = processor.get_data_from_raw_table(
        "treaties", "treaties_raw", cursor, schema="bicam_raw_congressional"
    )
    processor.process_data_parallel("treaties", data, cursor)

    # Process treaties_actions
    data = processor.get_data_from_raw_table(
        "treaties_actions",
        "treaties_actions_raw",
        cursor,
        schema="bicam_raw_congressional",
        related=True,
    )
    processor.process_data_parallel("treaties_actions", data, cursor)

    # Process committeemeetings
    data = processor.get_data_from_raw_table(
        "committeemeetings",
        "committeemeetings_raw",
        cursor,
        schema="bicam_raw_congressional",
    )
    processor.process_data_parallel("committeemeetings", data, cursor)

    # Process committeeprints
    data = processor.get_data_from_raw_table(
        "committeeprints",
        "committeeprints_raw",
        cursor,
        schema="bicam_raw_congressional",
    )
    processor.process_data_parallel("committeeprints", data, cursor)

    # Process committeeprints_texts
    data = processor.get_data_from_raw_table(
        "committeeprints_texts",
        "committeeprints_texts_raw",
        cursor,
        schema="bicam_raw_congressional",
        related=True,
    )
    processor.process_data_parallel("committeeprints_texts", data, cursor)

    # Process committeereports
    data = processor.get_data_from_raw_table(
        "committeereports",
        "committeereports_raw",
        cursor,
        schema="bicam_raw_congressional",
    )
    processor.process_data_parallel("committeereports", data, cursor)

    # Process committeereports_texts
    data = processor.get_data_from_raw_table(
        "committeereports_texts",
        "committeereports_texts_raw",
        cursor,
        schema="bicam_raw_congressional",
        related=True,
    )
    processor.process_data_parallel("committeereports_texts", data, cursor)

    # Process bills
    data = processor.get_data_from_raw_table(
        "bills", "bills_raw", cursor, schema="bicam_raw_congressional"
    )
    processor.process_data_parallel("bills", data, cursor)

    # Process bills_actions
    data = processor.get_data_from_raw_table(
        "bills_actions",
        "bills_actions_raw",
        cursor,
        schema="bicam_raw_congressional",
        related=True,
    )
    processor.process_data_parallel("bills_actions", data, cursor)

    # Process bills_summaries
    data = processor.get_data_from_raw_table(
        "bills_summaries",
        "bills_summaries_raw",
        cursor,
        schema="bicam_raw_congressional",
        related=True,
    )
    processor.process_data_parallel("bills_summaries", data, cursor)

    # Process bills_subjects
    data = processor.get_data_from_raw_table(
        "bills_subjects",
        "bills_subjects_raw",
        cursor,
        schema="bicam_raw_congressional",
        related=True,
    )
    processor.process_data_parallel("bills_subjects", data, cursor)

    # Process bills_titles
    data = processor.get_data_from_raw_table(
        "bills_titles",
        "bills_titles_raw",
        cursor,
        schema="bicam_raw_congressional",
        related=True,
    )
    processor.process_data_parallel("bills_titles", data, cursor)

    # Process bills_texts
    data = processor.get_data_from_raw_table(
        "bills_texts",
        "bills_texts_raw",
        cursor,
        schema="bicam_raw_congressional",
        related=True,
    )
    processor.process_data_parallel("bills_texts", data, cursor)

    # Process bills_cosponsors
    data = processor.get_data_from_raw_table(
        "bills_cosponsors",
        "bills_cosponsors_raw",
        cursor,
        schema="bicam_raw_congressional",
        related=True,
    )
    processor.process_data_parallel("bills_cosponsors", data, cursor)

    # Process bills_relatedbills
    data = processor.get_data_from_raw_table(
        "bills_relatedbills",
        "bills_relatedbills_raw",
        cursor,
        schema="bicam_raw_congressional",
        related=True,
    )
    processor.process_data_parallel("bills_relatedbills", data, cursor)

    conn.commit()
    cursor.close()
    conn.close()
    logger.info("Optimized processing completed")


if __name__ == "__main__":
    main()
