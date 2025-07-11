import hashlib
import io
import json
import logging
import os
import re
from datetime import datetime
from html import unescape
from typing import Any

import psycopg2
from dateutil.parser import parse
from dotenv import load_dotenv
from pytz import UTC

load_dotenv()


class Utils:
    @staticmethod
    def standardize_date(date_value: Any) -> datetime | None:
        """
        Standardize date values to consistent timezone-aware datetime objects.

        Args:
            date_value: Raw date value in various formats

        Returns:
            Standardized timezone-aware datetime object (UTC) or None if invalid
        """
        if not date_value or date_value in ["", "null", "None"]:
            return None

        if isinstance(date_value, datetime):
            # Ensure existing datetime objects are timezone-aware
            if date_value.tzinfo is None:
                # Assume timezone-naive datetime is in UTC
                return date_value.replace(tzinfo=UTC)
            return date_value

        if isinstance(date_value, str):
            try:
                parsed_date = parse(date_value)
                # Ensure parsed datetime is timezone-aware
                if parsed_date.tzinfo is None:
                    # Assume timezone-naive datetime is in UTC
                    return parsed_date.replace(tzinfo=UTC)
                return parsed_date
            except Exception:
                logger.warning(f"Could not parse date: {date_value}")
                return None

        return None

    @staticmethod
    def clean_long_text(text: str | None) -> str | None:
        """
        Clean and normalize long text fields, including HTML content.

        Args:
            text: Raw text to clean (may contain HTML)

        Returns:
            Cleaned text or None
        """
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
    def standardize_chamber(chamber: str | None) -> str | None:
        """
        Standardize chamber values to consistent format.

        Args:
            chamber: Raw chamber value

        Returns:
            Standardized chamber value
        """
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
            return chamber  # Return original if not recognized

    @staticmethod
    def safe_int(value: Any, default: int | None = None) -> int | float | None:
        """
        Safely convert value to integer.

        Args:
            value: Value to convert
            default: Default value if conversion fails

        Returns:
            Integer value or default
        """
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
        """
        Safely convert value to float.

        Args:
            value: Value to convert
            default: Default value if conversion fails

        Returns:
            Float value or default
        """
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
    def copy_dicts_to_table(cursor, schema, table, columns, dicts):
        if not dicts:
            return
        output = io.StringIO()
        for row in dicts:
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
                    return v.isoformat()
                return str(v)

            output.write("\t".join(pg_val(v) for v in vals) + "\n")
        output.seek(0)
        # Use correct signature for psycopg2's copy_expert: (sql, file)
        sql = f"COPY {schema}.{table} ({', '.join(columns)}) FROM STDIN WITH (FORMAT text, NULL '\\N')"
        cursor.copy_expert(sql, output)  # Corrected to use (sql, file)


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_data_from_raw_table(
    data_type, table_name, cursor, schema="bicam_raw_congressional", related=False
):
    if related:
        if table_name == "committeeprints_texts_raw":
            cursor.execute(
                f"""
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
            )
        else:
            cursor.execute(
                f"SELECT {schema}.{table_name}.payload FROM {schema}.{table_name} JOIN {schema}.{table_name.split('_')[0]}_raw USING (source_doc_id) WHERE {schema}.{table_name.split('_')[0]}_raw.payload->>'updateDate' > (select last_processed_date::text from __metadata.last_processed_dates where data_type = '{table_name.split('_')[0]}')"
            )
    else:
        cursor.execute(
            f"SELECT payload FROM {schema}.{table_name} WHERE payload->>'updateDate' > (select last_processed_date::text from __metadata.last_processed_dates where data_type = '{data_type}')"
        )
    results = cursor.fetchall()
    data = [result[0] for result in results]
    logger.info(f"Found {len(data)} rows in {schema}.{table_name}")
    return data


def check_duplicates(data, id_keys, data_type):
    """
    Remove duplicates from data based on one or more id_keys.
    Args:
        data: list of dicts
        id_keys: str or list/tuple of str, column(s) to use as unique key
        data_type: str, for logging
    Returns:
        list of dicts with duplicates removed
    """
    if isinstance(id_keys, str):
        id_keys = [id_keys]

    # Build a tuple key for each item
    def get_key(item):
        return tuple(item.get(k) for k in id_keys)

    from collections import Counter

    key_counts = Counter(get_key(item) for item in data)
    duplicates = [item for item in data if key_counts[get_key(item)] > 1]
    if duplicates:
        logger.error(
            f"{len(duplicates)} duplicates found in {data_type} based on keys {id_keys}"
        )

    # Only keep items whose key appears once
    data = [item for item in data if key_counts[get_key(item)] == 1]
    return data


def process_amendments(data, cursor):
    # Create amendment_id from {type.lower()}{number}-{congress}
    utils = Utils()

    # Use a list comprehension for efficient and idiomatic processing of the entire list of dicts
    amendments = [
        {
            "amendment_id": f"{item.get('type').lower()}{item.get('number')}-{item.get('congress')}",
            "amendment_type": item.get("type").lower(),
            "amendment_number": item.get("number"),
            "congress": item.get("congress"),
            "chamber": utils.standardize_chamber(item.get("chamber")),
            "purpose": item.get("purpose"),
            "description": utils.clean_long_text(item.get("description")),
            "proposed_at": utils.standardize_date(item.get("proposedDate")),
            "submitted_at": utils.standardize_date(item.get("submittedDate")),
            "is_bill_amendment": item.get("amendedBill") is not None,
            "is_treaty_amendment": item.get("amendedTreaty") is not None,
            "is_amendment_amendment": item.get("amendedAmendment") is not None,
            "notes": item.get("notes"),
            "actions_count": item.get("actions", {}).get("count", 0),
            "cosponsors_count": item.get("cosponsors", {}).get("count", 0),
            "amendments_to_amendment_count": item.get("amendmentsToAmendment", {}).get(
                "count", 0
            ),
            "updated_at": utils.standardize_date(item.get("updateDate")),
            # Extracted fields
            "sponsors": item.get("sponsors"),
            "amendedBill": item.get("amendedBill"),
            "amendedTreaty": item.get("amendedTreaty"),
            "amendedAmendment": item.get("amendedAmendment"),
        }
        for item in data
    ]
    amendments = check_duplicates(amendments, "amendment_id", "amendments")

    amendments_sponsors = [
        {
            "amendment_id": item.get("amendment_id"),
            "bioguide_id": sponsor.get("bioguideId"),
        }
        for item in amendments
        for sponsor in item.get("sponsors", [])
        if sponsor.get("bioguideId") is not None
    ]
    amendments_sponsors = check_duplicates(
        amendments_sponsors, ["amendment_id", "bioguide_id"], "amendments_sponsors"
    )

    amendments_amended_bills = [
        {
            "amendment_id": item.get("amendment_id"),
            "bill_id": f"{item.get('amendedBill').get('type').lower()}{item.get('amendedBill').get('number')}-{item.get('amendedBill').get('congress')}",
        }
        for item in amendments
        if item.get("amendedBill") is not None
    ]
    amendments_amended_bills = check_duplicates(
        amendments_amended_bills,
        ["amendment_id", "bill_id"],
        "amendments_amended_bills",
    )
    amendments_amended_treaties = [
        {
            "amendment_id": item.get("amendment_id"),
            "treaty_id": f"td{item.get('amendedTreaty').get('congress')}-{item.get('amendedTreaty').get('number')}",
        }
        for item in amendments
        if item.get("amendedTreaty") is not None
    ]
    amendments_amended_treaties = check_duplicates(
        amendments_amended_treaties,
        ["amendment_id", "treaty_id"],
        "amendments_amended_treaties",
    )
    amendments_amended_amendments = [
        {
            "amendment_id": item.get("amendment_id"),
            "amended_amendment_id": f"{item.get('amendedAmendment').get('type').lower()}{item.get('amendedAmendment').get('number')}-{item.get('amendedAmendment').get('congress')}",
        }
        for item in amendments
        if item.get("amendedAmendment") is not None
    ]
    amendments_amended_amendments = check_duplicates(
        amendments_amended_amendments,
        ["amendment_id", "amended_amendment_id"],
        "amendments_amended_amendments",
    )
    amendments = [
        {
            k: v
            for k, v in item.items()
            if k not in ["sponsors", "amendedBill", "amendedTreaty", "amendedAmendment"]
        }
        for item in amendments
    ]

    Utils.copy_dicts_to_table(
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
    Utils.copy_dicts_to_table(
        cursor,
        "staging_congressional",
        "amendments_sponsors",
        ["amendment_id", "bioguide_id"],
        amendments_sponsors,
    )
    Utils.copy_dicts_to_table(
        cursor,
        "staging_congressional",
        "amendments_amended_bills",
        ["amendment_id", "bill_id"],
        amendments_amended_bills,
    )
    Utils.copy_dicts_to_table(
        cursor,
        "staging_congressional",
        "amendments_amended_treaties",
        ["amendment_id", "treaty_id"],
        amendments_amended_treaties,
    )
    Utils.copy_dicts_to_table(
        cursor,
        "staging_congressional",
        "amendments_amended_amendments",
        ["amendment_id", "amended_amendment_id"],
        amendments_amended_amendments,
    )
    logger.info(f"Inserted {len(amendments)} amendments")
    logger.info(f"Inserted {len(amendments_sponsors)} amendments_sponsors")
    logger.info(f"Inserted {len(amendments_amended_bills)} amendments_amended_bills")
    logger.info(
        f"Inserted {len(amendments_amended_treaties)} amendments_amended_treaties"
    )
    logger.info(
        f"Inserted {len(amendments_amended_amendments)} amendments_amended_amendments"
    )


def process_amendments_actions(data, cursor):
    # Create amendment_id from {type.lower()}{number}-{congress}
    utils = Utils()
    # Use a list comprehension for efficient and idiomatic processing of the entire list of dicts
    actions = [
        {
            "action_id": hashlib.sha256(json.dumps(item).encode()).hexdigest(),
            "amendment_id": item.get("amendment_id"),
            "action_code": item.get("actionCode"),
            "action_date": utils.standardize_date(item.get("actionDate")),
            "text": item.get("text"),
            "action_type": item.get("actionType"),
            "source_system": item.get("sourceSystem"),
            "source_system_code": item.get("sourceSystemCode"),
            "recorded_votes": item.get("recordedVotes"),
        }
        for item in data
    ]
    actions = check_duplicates(
        actions, ["action_id", "amendment_id"], "amendments_actions"
    )

    amendments_actions_recorded_votes = []
    for action in actions:
        recorded_votes = action.get("recorded_votes")
        if recorded_votes is None:
            continue
        if not isinstance(recorded_votes, list):
            logger.warning(
                f"Expected list for recorded_votes in action {action.get('action_id')}, got {type(recorded_votes)}. Skipping."
            )
            continue
        for recorded_vote in recorded_votes:
            if recorded_vote is None:
                continue
            amendments_actions_recorded_votes.append(
                {
                    "action_id": action.get("action_id"),
                    "amendment_id": action.get("amendments_id"),
                    "chamber": recorded_vote.get("chamber"),
                    "congress": recorded_vote.get("congress"),
                    "date": utils.standardize_date(recorded_vote.get("date")),
                    "roll_number": recorded_vote.get("rollNumber"),
                    "session": recorded_vote.get("session"),
                    "url": recorded_vote.get("url"),
                }
            )
    # drop the recorded_votes field
    actions = [
        {k: v for k, v in action.items() if k != "recorded_votes"} for action in actions
    ]
    Utils.copy_dicts_to_table(
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
    Utils.copy_dicts_to_table(
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
    logger.info(f"Inserted {len(actions)} amendments_actions")
    logger.info(
        f"Inserted {len(amendments_actions_recorded_votes)} amendments_actions_recorded_votes"
    )


def process_amendments_cosponsors(data, cursor):
    # Use a list comprehension for efficient and idiomatic processing of the entire list of dicts
    logger.info(f"Processing {len(data)} cosponsors")
    cosponsors = [
        {
            "amendment_id": item.get("amendments_id"),
            "bioguide_id": item.get("bioguideId"),
        }
        for item in data
    ]
    cosponsors = check_duplicates(
        cosponsors, ["amendment_id", "bioguide_id"], "amendments_cosponsors"
    )
    logger.info(f"Found {len(cosponsors)} cosponsors")

    Utils.copy_dicts_to_table(
        cursor,
        "staging_congressional",
        "amendments_cosponsors",
        ["amendment_id", "bioguide_id"],
        cosponsors,
    )
    logger.info(f"Inserted {len(cosponsors)} amendments_cosponsors")


def process_amendments_texts(data, cursor):
    utils = Utils()
    logger.info(f"Processing {len(data)} texts")
    texts = [
        {
            "amendment_id": item.get("amendments_id"),
            "date": utils.standardize_date(item.get("date")),
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
    texts = check_duplicates(texts, ["amendment_id"], "amendments_texts")
    logger.info(f"Found {len(texts)} texts")

    Utils.copy_dicts_to_table(
        cursor,
        "staging_congressional",
        "amendments_texts",
        ["amendment_id", "date", "type", "raw_text", "html", "pdf"],
        texts,
    )
    logger.info(f"Inserted {len(texts)} amendments_texts")


def process_bills(data, cursor):
    logger.info(f"Processing {len(data)} bills")
    utils = Utils()
    bills = [
        {
            "bill_id": f"{item.get('type').lower()}{item.get('number').replace('½', '.5')}-{item.get('congress')}",
            "bill_type": item.get("type").lower(),
            "bill_number": item.get("number"),
            "congress": item.get("congress"),
            "title": item.get("title"),
            "origin_chamber": item.get("originChamber").lower(),
            "introduced_at": utils.standardize_date(item.get("introducedDate")),
            "constitutional_authority_statement": utils.clean_long_text(
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
            "updated_at": utils.standardize_date(item.get("updateDate")),
        }
        for item in data
    ]
    bills = check_duplicates(bills, ["bill_id"], "bills")
    logger.info(f"Found {len(bills)} bills")
    bills_sponsors = [
        {
            "bill_id": item.get("bills_id"),
            "bioguide_id": item.get("bioguideId"),
        }
        for item in data
    ]
    bills_sponsors = check_duplicates(
        bills_sponsors, ["bill_id", "bioguide_id"], "bills_sponsors"
    )
    logger.info(f"Found {len(bills_sponsors)} bills_sponsors")

    bills_laws = [
        {
            "bill_id": item.get("bills_id"),
            "law_id": f"PL{item.get('congress')}-{item.get('number')}",
            "law_number": item.get("number"),
            "law_type": item.get("type"),
        }
        for item in data
    ]
    bills_laws = check_duplicates(bills_laws, ["bill_id", "law_id"], "bills_laws")
    logger.info(f"Found {len(bills_laws)} bills_laws")
    bills_notes = [
        {
            "bill_id": item.get("bills_id"),
            "note_id": hashlib.sha256(json.dumps(item).encode()).hexdigest(),
            "note_text": item.get("text"),
            "links": item.get("links"),
        }
        for item in data
    ]
    bills_notes = check_duplicates(bills_notes, ["bill_id", "note_id"], "bills_notes")
    logger.info(f"Found {len(bills_notes)} bills_notes")
    bills_notes_links = [
        {
            "bill_id": item.get("bills_id"),
            "note_id": item.get("note_id"),
            "link_name": link.get("name"),
            "link_url": link.get("url"),
        }
        for item in bills_notes
        for link in item.get("links", [])
    ]
    bills_notes_links = check_duplicates(
        bills_notes_links,
        ["bill_id", "note_id", "link_name", "link_url"],
        "bills_notes_links",
    )
    logger.info(f"Found {len(bills_notes_links)} bills_notes_links")

    # drop the links field
    bills_notes = [
        {k: v for k, v in item.items() if k != "links"} for item in bills_notes
    ]
    bills_cbocostestimates = [
        {
            "bill_id": item.get("bills_id"),
            "description": item.get("description"),
            "pub_date": utils.standardize_date(item.get("pubDate")),
            "title": item.get("title"),
            "url": item.get("url"),
        }
        for item in data
    ]
    bills_cbocostestimates = check_duplicates(
        bills_cbocostestimates, ["bill_id", "pub_date", "url"], "bills_cbocostestimates"
    )
    logger.info(f"Found {len(bills_cbocostestimates)} bills_cbocostestimates")

    Utils.copy_dicts_to_table(
        cursor,
        "staging_congressional",
        "bills_sponsors",
        ["bill_id", "bioguide_id"],
        bills_sponsors,
    )
    Utils.copy_dicts_to_table(
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
    Utils.copy_dicts_to_table(
        cursor,
        "staging_congressional",
        "bills_laws",
        ["bill_id", "law_id", "law_number", "law_type"],
        bills_laws,
    )
    Utils.copy_dicts_to_table(
        cursor,
        "staging_congressional",
        "bills_notes",
        ["bill_id", "note_id", "note_text"],
        bills_notes,
    )
    Utils.copy_dicts_to_table(
        cursor,
        "staging_congressional",
        "bills_notes_links",
        ["bill_id", "note_id", "link_name", "link_url"],
        bills_notes_links,
    )
    Utils.copy_dicts_to_table(
        cursor,
        "staging_congressional",
        "bills_cbocostestimates",
        ["bill_id", "description", "pub_date", "title", "url"],
        bills_cbocostestimates,
    )
    logger.info(f"Inserted {len(bills_notes)} bills_notes")
    logger.info(f"Inserted {len(bills_notes_links)} bills_notes_links")
    logger.info(f"Inserted {len(bills_cbocostestimates)} bills_cbocostestimates")
    logger.info(f"Inserted {len(bills)} bills")
    logger.info(f"Inserted {len(bills_sponsors)} bills_sponsors")
    logger.info(f"Inserted {len(bills_laws)} bills_laws")
    logger.info(f"Inserted {len(bills_notes)} bills_notes")
    logger.info(f"Inserted {len(bills_notes_links)} bills_notes_links")
    logger.info(f"Inserted {len(bills_cbocostestimates)} bills_cbocostestimates")


def process_bills_actions(data, cursor):
    logger.info(f"Processing {len(data)} actions")
    utils = Utils()
    bills_actions = [
        {
            "action_id": hashlib.sha256(json.dumps(item).encode()).hexdigest(),
            "bill_id": item.get("bills_id"),
            "action_code": item.get("actionCode"),
            "action_date": utils.standardize_date(item.get("actionDate")),
            "text": item.get("text"),
            "action_type": item.get("actionType"),
            "source_system": item.get("sourceSystem"),
            "source_system_code": item.get("sourceSystemCode"),
            "calendar": item.get("calendar"),
            "calendar_number": item.get("calendarNumber"),
            "recorded_votes": item.get("recordedVotes"),
            "committees": item.get("committees"),
        }
        for item in data
    ]
    bills_actions = check_duplicates(
        bills_actions, ["action_id", "bill_id"], "bills_actions"
    )
    logger.info(f"Found {len(bills_actions)} bills_actions")

    bills_actions_committees = [
        {
            "action_id": action.get("action_id"),
            "bill_id": action.get("bills_id"),
            "committee_code": committee.get("systemCode"),
        }
        for action in bills_actions
        for committee in action.get("committees", [])
    ]
    bills_actions_committees = check_duplicates(
        bills_actions_committees,
        ["action_id", "committee_code"],
        "bills_actions_committees",
    )
    logger.info(f"Found {len(bills_actions_committees)} bills_actions_committees")

    bills_actions_recorded_votes = [
        {
            "action_id": action.get("action_id"),
            "bill_id": action.get("bills_id"),
            "chamber": utils.standardize_chamber(recorded_vote.get("chamber")),
            "congress": recorded_vote.get("congress"),
            "date": utils.standardize_date(recorded_vote.get("date")),
            "roll_number": recorded_vote.get("rollNumber"),
            "session": recorded_vote.get("session"),
            "url": recorded_vote.get("url"),
        }
        for action in bills_actions
        for recorded_vote in action.get("recordedVotes", [])
    ]
    bills_actions_recorded_votes = check_duplicates(
        bills_actions_recorded_votes,
        ["action_id", "bill_id", "url"],
        "bills_actions_recorded_votes",
    )
    logger.info(
        f"Found {len(bills_actions_recorded_votes)} bills_actions_recorded_votes"
    )

    bills_actions = [
        {k: v for k, v in action.items() if k != "recordedVotes" and k != "committees"}
        for action in bills_actions
    ]

    Utils.copy_dicts_to_table(
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
            "recorded_votes",
            "committees",
        ],
        bills_actions,
    )
    Utils.copy_dicts_to_table(
        cursor,
        "staging_congressional",
        "bills_actions_committees",
        ["action_id", "bill_id", "committee_code"],
        bills_actions_committees,
    )
    Utils.copy_dicts_to_table(
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
    logger.info(f"Inserted {len(bills_actions)} bills_actions")
    logger.info(f"Inserted {len(bills_actions_committees)} bills_actions_committees")
    logger.info(
        f"Inserted {len(bills_actions_recorded_votes)} bills_actions_recorded_votes"
    )


def process_bills_summaries(data, cursor):
    logger.info(f"Processing {len(data)} summaries")
    utils = Utils()
    bills_summaries = [
        {
            "bill_id": item.get("bills_id"),
            "action_date": utils.standardize_date(item.get("actionDate")),
            "action_desc": item.get("actionDesc"),
            "text": item.get("text"),
            "version_code": item.get("versionCode"),
        }
        for item in data
    ]
    bills_summaries = check_duplicates(
        bills_summaries,
        ["bill_id", "action_date", "action_desc", "version_code"],
        "bills_summaries",
    )
    logger.info(f"Found {len(bills_summaries)} bills_summaries")

    Utils.copy_dicts_to_table(
        cursor,
        "staging_congressional",
        "bills_summaries",
        ["bill_id", "action_date", "action_desc", "text", "version_code"],
        bills_summaries,
    )
    logger.info(f"Inserted {len(bills_summaries)} bills_summaries")


def process_bills_subjects(data, cursor):
    logger.info(f"Processing {len(data)} subjects")
    utils = Utils()
    bills_subjects = [
        {
            "bill_id": item.get("bills_id"),
            "subject": item.get("subject"),
            "updated_at": utils.standardize_date(item.get("updateDate")),
        }
        for item in data
    ]
    bills_subjects = check_duplicates(
        bills_subjects, ["bill_id", "subject"], "bills_subjects"
    )
    logger.info(f"Found {len(bills_subjects)} bills_subjects")

    Utils.copy_dicts_to_table(
        cursor,
        "staging_congressional",
        "bills_subjects",
        ["bill_id", "subject", "updated_at"],
        bills_subjects,
    )
    logger.info(f"Inserted {len(bills_subjects)} bills_subjects")


def process_bills_titles(data, cursor):
    logger.info(f"Processing {len(data)} titles")
    utils = Utils()
    bills_titles = [
        {
            "bill_id": item.get("bills_id"),
            "title": item.get("title"),
            "title_type": item.get("titleType"),
            "bill_text_version_code": item.get("billTextVersionCode"),
            "bill_text_version_name": item.get("billTextVersionName"),
            "chamber": utils.standardize_chamber(item.get("chamber"))
            if item.get("chamber")
            else None,
            "title_type_code": item.get("titleTypeCode"),
        }
        for item in data
    ]
    bills_titles = check_duplicates(
        bills_titles, ["bill_id", "title", "title_type_code"], "bills_titles"
    )
    logger.info(f"Found {len(bills_titles)} bills_titles")

    Utils.copy_dicts_to_table(
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
    logger.info(f"Inserted {len(bills_titles)} bills_titles")


def process_bills_texts(data, cursor):
    logger.info(f"Processing {len(data)} texts")
    utils = Utils()
    bills_texts = [
        {
            "bill_id": item.get("bills_id"),
            "date": utils.standardize_date(item.get("date")),
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
    bills_texts = check_duplicates(
        bills_texts, ["bill_id", "date", "type"], "bills_texts"
    )
    logger.info(f"Found {len(bills_texts)} bills_texts")

    Utils.copy_dicts_to_table(
        cursor,
        "staging_congressional",
        "bills_texts",
        ["bill_id", "date", "type", "raw_text", "formatted_text", "pdf", "xml"],
        bills_texts,
    )
    logger.info(f"Inserted {len(bills_texts)} bills_texts")


def process_bills_cosponsors(data, cursor):
    logger.info(f"Processing {len(data)} cosponsors")
    bills_cosponsors = [
        {
            "bill_id": item.get("bills_id"),
            "bioguide_id": item.get("bioguideId"),
        }
        for item in data
    ]
    bills_cosponsors = check_duplicates(
        bills_cosponsors, ["bill_id", "bioguide_id"], "bills_cosponsors"
    )
    logger.info(f"Found {len(bills_cosponsors)} bills_cosponsors")

    Utils.copy_dicts_to_table(
        cursor,
        "staging_congressional",
        "bills_cosponsors",
        ["bill_id", "bioguide_id"],
        bills_cosponsors,
    )
    logger.info(f"Inserted {len(bills_cosponsors)} bills_cosponsors")


def process_bills_relatedbills(data, cursor):
    logger.info(f"Processing {len(data)} related bills")
    bills_relatedbills = [
        {
            "bill_id": item.get("bills_id"),
            "related_bill_id": f"{item.get('type').lower()}{item.get('number')}-{item.get('congress')}",
            "relationship_type": item.get("relationshipType"),
        }
        for item in data
    ]
    bills_relatedbills = check_duplicates(
        bills_relatedbills, ["bill_id", "related_bill_id"], "bills_relatedbills"
    )
    logger.info(f"Found {len(bills_relatedbills)} bills_relatedbills")

    Utils.copy_dicts_to_table(
        cursor,
        "staging_congressional",
        "bills_related_bills",
        ["bill_id", "related_bill_id", "relationship_type"],
        bills_relatedbills,
    )
    logger.info(f"Inserted {len(bills_relatedbills)} bills_relatedbills")


def process_committeemeetings(data, cursor):
    logger.info(f"Processing {len(data)} committeemeetings")
    utils = Utils()
    committeemeetings = []
    for item in data:
        # Default values from top-level or location
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

        committeemeetings.append(
            {
                "meeting_id": item.get("eventId"),
                "title": item.get("title"),
                "meeting_type": item.get("type"),
                "chamber": utils.standardize_chamber(item.get("chamber")),
                "congress": item.get("congress"),
                "date": utils.standardize_date(item.get("date")),
                "room": location.get("room"),
                "street_address": street_address,
                "building": building,
                "city": city,
                "state": state,
                "zip_code": zip_code,
                "meeting_status": item.get("meetingStatus"),
                "updated_at": utils.standardize_date(item.get("updateDate")),
                # Extracted fields
                "committees": item.get("committees"),
                "related_items": item.get("relatedItems", []),
                "meeting_documents": item.get("meetingDocuments", []),
                "witness_documents": item.get("witnessDocuments", []),
                "witnesses": item.get("witnesses", []),
                "associated_hearings": item.get("hearingTranscript", []),
            }
        )
    committeemeetings = check_duplicates(
        committeemeetings, ["meeting_id"], "committeemeetings"
    )
    logger.info(f"Found {len(committeemeetings)} committeemeetings")

    committeemeetings_committees = [
        {
            "meeting_id": item.get("meeting_id"),
            "committee_code": committee.get("systemCode"),
        }
        for item in committeemeetings
        for committee in item.get("committees", [])
    ]
    committeemeetings_committees = check_duplicates(
        committeemeetings_committees,
        ["meeting_id", "committee_code"],
        "committeemeetings_committees",
    )
    logger.info(
        f"Found {len(committeemeetings_committees)} committeemeetings_committees"
    )

    committeemeetings_associated_bills = []
    for item in committeemeetings:
        for related_item in item.get("related_items", []):
            if related_item.get("bills"):
                for bill in related_item.get("bills"):
                    committeemeetings_associated_bills.append(
                        {
                            "meeting_id": item.get("meeting_id"),
                            "bill_id": f"{bill.get('type').lower()}{bill.get('number')}-{bill.get('congress')}",
                        }
                    )
    committeemeetings_associated_bills = check_duplicates(
        committeemeetings_associated_bills,
        ["meeting_id", "bill_id"],
        "committeemeetings_associated_bills",
    )
    logger.info(
        f"Found {len(committeemeetings_associated_bills)} committeemeetings_associated_bills"
    )

    committeemeetings_associated_treaties = []
    for item in committeemeetings:
        for related_item in item.get("related_items", []):
            if related_item.get("treaties"):
                for treaty in related_item.get("treaties"):
                    committeemeetings_associated_treaties.append(
                        {
                            "meeting_id": item.get("meeting_id"),
                            "treaty_id": f"td{treaty.get('congress')}-{treaty.get('number')}",
                        }
                    )
    committeemeetings_associated_treaties = check_duplicates(
        committeemeetings_associated_treaties,
        ["meeting_id", "treaty_id"],
        "committeemeetings_associated_treaties",
    )
    logger.info(
        f"Found {len(committeemeetings_associated_treaties)} committeemeetings_associated_treaties"
    )

    committeemeetings_associated_nominations = []
    for item in committeemeetings:
        for related_item in item.get("related_items", []):
            if related_item.get("nominations"):
                for nomination in related_item.get("nominations"):
                    committeemeetings_associated_nominations.append(
                        {
                            "meeting_id": item.get("meeting_id"),
                            "nomination_id": f"PN{nomination.get('number')}-{nomination.get('part', '00')}-{nomination.get('congress')}",
                        }
                    )
    committeemeetings_associated_nominations = check_duplicates(
        committeemeetings_associated_nominations,
        ["meeting_id", "nomination_id"],
        "committeemeetings_associated_nominations",
    )
    logger.info(
        f"Found {len(committeemeetings_associated_nominations)} committeemeetings_associated_nominations"
    )

    committeemeetings_meeting_documents = []
    for item in committeemeetings:
        for meeting_document in item.get("meeting_documents", []):
            committeemeetings_meeting_documents.append(
                {
                    "meeting_id": item.get("meeting_id"),
                    "name": meeting_document.get("name"),
                    "document_type": meeting_document.get("documentType"),
                    "description": meeting_document.get("description"),
                    "url": meeting_document.get("url"),
                }
            )
    committeemeetings_meeting_documents = check_duplicates(
        committeemeetings_meeting_documents,
        ["meeting_id", "url"],
        "committeemeetings_meeting_documents",
    )
    logger.info(
        f"Found {len(committeemeetings_meeting_documents)} committeemeetings_meeting_documents"
    )

    committeemeetings_witness_documents = []
    for item in committeemeetings:
        for witness_document in item.get("witness_documents", []):
            committeemeetings_witness_documents.append(
                {
                    "meeting_id": item.get("meeting_id"),
                    "document_type": witness_document.get("documentType"),
                    "url": witness_document.get("url"),
                }
            )
    committeemeetings_witness_documents = check_duplicates(
        committeemeetings_witness_documents,
        ["meeting_id", "url"],
        "committeemeetings_witness_documents",
    )
    logger.info(
        f"Found {len(committeemeetings_witness_documents)} committeemeetings_witness_documents"
    )

    committeemeetings_witnesses = []
    for item in committeemeetings:
        for witness in item.get("witnesses", []):
            committeemeetings_witnesses.append(
                {
                    "meeting_id": item.get("meeting_id"),
                    "name": witness.get("name"),
                    "position": witness.get("position"),
                    "organization": witness.get("organization"),
                }
            )
    committeemeetings_witnesses = check_duplicates(
        committeemeetings_witnesses,
        ["meeting_id", "name", "position", "organization"],
        "committeemeetings_witnesses",
    )
    logger.info(f"Found {len(committeemeetings_witnesses)} committeemeetings_witnesses")

    committeemeetings_associated_hearings = []
    for item in committeemeetings:
        for hearing in item.get("associated_hearings", []):
            committeemeetings_associated_hearings.append(
                {
                    "meeting_id": item.get("meeting_id"),
                    "hearing_id": (
                        lambda url: (
                            lambda match: (
                                f"{('j' if match.group(2).lower() == 'nochamber' else match.group(2).lower()[0])}hrg-{match.group(1)}"
                            )
                            if match
                            else None
                        )(re.search(r"/hearing/(\d+)/(.*?)(?:\?|$)", url))
                    )(hearing.get("url", "")),
                }
            )
    committeemeetings_associated_hearings = check_duplicates(
        committeemeetings_associated_hearings,
        ["meeting_id", "hearing_id"],
        "committeemeetings_associated_hearings",
    )
    logger.info(
        f"Found {len(committeemeetings_associated_hearings)} committeemeetings_associated_hearings"
    )

    # drop extracted fields columns from committeemeetings
    committeemeetings = [
        {
            k: v
            for k, v in item.items()
            if k
            not in [
                "committees",
                "related_items",
                "meeting_documents",
                "witness_documents",
                "witnesses",
                "associated_hearings",
            ]
        }
        for item in committeemeetings
    ]
    logger.info(f"Found {len(committeemeetings)} committeemeetings")

    Utils.copy_dicts_to_table(
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
    logger.info(f"Inserted {len(committeemeetings)} committeemeetings")

    Utils.copy_dicts_to_table(
        cursor,
        "staging_congressional",
        "committeemeetings_committees",
        ["meeting_id", "committee_code"],
        committeemeetings_committees,
    )
    logger.info(
        f"Inserted {len(committeemeetings_committees)} committeemeetings_committees"
    )

    Utils.copy_dicts_to_table(
        cursor,
        "staging_congressional",
        "committeemeetings_associated_bills",
        ["meeting_id", "bill_id"],
        committeemeetings_associated_bills,
    )
    logger.info(
        f"Inserted {len(committeemeetings_associated_bills)} committeemeetings_associated_bills"
    )

    Utils.copy_dicts_to_table(
        cursor,
        "staging_congressional",
        "committeemeetings_associated_treaties",
        ["meeting_id", "treaty_id"],
        committeemeetings_associated_treaties,
    )
    logger.info(
        f"Inserted {len(committeemeetings_associated_treaties)} committeemeetings_associated_treaties"
    )

    Utils.copy_dicts_to_table(
        cursor,
        "staging_congressional",
        "committeemeetings_associated_nominations",
        ["meeting_id", "nomination_id"],
        committeemeetings_associated_nominations,
    )
    logger.info(
        f"Inserted {len(committeemeetings_associated_nominations)} committeemeetings_associated_nominations"
    )

    Utils.copy_dicts_to_table(
        cursor,
        "staging_congressional",
        "committeemeetings_meeting_documents",
        ["meeting_id", "name", "document_type", "description", "url"],
        committeemeetings_meeting_documents,
    )
    logger.info(
        f"Inserted {len(committeemeetings_meeting_documents)} committeemeetings_meeting_documents"
    )

    Utils.copy_dicts_to_table(
        cursor,
        "staging_congressional",
        "committeemeetings_witness_documents",
        ["meeting_id", "document_type", "url"],
        committeemeetings_witness_documents,
    )
    logger.info(
        f"Inserted {len(committeemeetings_witness_documents)} committeemeetings_witness_documents"
    )

    Utils.copy_dicts_to_table(
        cursor,
        "staging_congressional",
        "committeemeetings_witnesses",
        ["meeting_id", "name", "position", "organization"],
        committeemeetings_witnesses,
    )
    logger.info(
        f"Inserted {len(committeemeetings_witnesses)} committeemeetings_witnesses"
    )

    Utils.copy_dicts_to_table(
        cursor,
        "staging_congressional",
        "committeemeetings_associated_hearings",
        ["meeting_id", "hearing_id"],
        committeemeetings_associated_hearings,
    )
    logger.info(
        f"Inserted {len(committeemeetings_associated_hearings)} committeemeetings_associated_hearings"
    )

    Utils.copy_dicts_to_table(
        cursor,
        "staging_congressional",
        "committeemeetings_associated_nominations",
        ["meeting_id", "nomination_id"],
        committeemeetings_associated_nominations,
    )
    logger.info(
        f"Inserted {len(committeemeetings_associated_nominations)} committeemeetings_associated_nominations"
    )

    Utils.copy_dicts_to_table(
        cursor,
        "staging_congressional",
        "committeemeetings_meeting_documents",
        ["meeting_id", "name", "document_type", "description", "url"],
        committeemeetings_meeting_documents,
    )
    logger.info(
        f"Inserted {len(committeemeetings_meeting_documents)} committeemeetings_meeting_documents"
    )

    Utils.copy_dicts_to_table(
        cursor,
        "staging_congressional",
        "committeemeetings_witness_documents",
        ["meeting_id", "document_type", "url"],
        committeemeetings_witness_documents,
    )
    logger.info(
        f"Inserted {len(committeemeetings_witness_documents)} committeemeetings_witness_documents"
    )

    Utils.copy_dicts_to_table(
        cursor,
        "staging_congressional",
        "committeemeetings_witnesses",
        ["meeting_id", "name", "position", "organization"],
        committeemeetings_witnesses,
    )
    logger.info(
        f"Inserted {len(committeemeetings_witnesses)} committeemeetings_witnesses"
    )

    Utils.copy_dicts_to_table(
        cursor,
        "staging_congressional",
        "committeemeetings_associated_hearings",
        ["meeting_id", "hearing_id"],
        committeemeetings_associated_hearings,
    )
    logger.info(
        f"Inserted {len(committeemeetings_associated_hearings)} committeemeetings_associated_hearings"
    )


def process_committeeprints(data, cursor):
    logger.info(f"Processing {len(data)} committeeprints")
    utils = Utils()
    committeeprints = [
        {
            "print_id": (
                (
                    (item.get("chamber", "") or "").lower()[0]
                    if (item.get("chamber", "") or "").lower() != "nochamber"
                    else "j"
                )
                + "prt"
                + str(item.get("jacketNumber", "") or "")
                + "-"
                + str(item.get("congress", "") or "")
            ),
            "print_jacketnumber": item.get("jacketNumber"),
            "congress": item.get("congress"),
            "chamber": utils.standardize_chamber(item.get("chamber")),
            "title": item.get("title"),
            "print_number": item.get("number"),
            "citation": item.get("citation"),
            "updated_at": utils.standardize_date(item.get("updateDate")),
            # extracted fields
            "associated_bills": item.get("associatedBills", []),
            "committees": item.get("committees", []),
        }
        for item in data
    ]
    committeeprints = check_duplicates(committeeprints, ["print_id"], "committeeprints")
    logger.info(f"Found {len(committeeprints)} committeeprints")

    committeeprints_associated_bills = [
        {
            "print_id": item.get("print_id"),
            "bill_id": f"{bill.get('type').lower()}{bill.get('number')}-{bill.get('congress')}",
        }
        for item in committeeprints
        for bill in item.get("associated_bills", [])
    ]
    committeeprints_associated_bills = check_duplicates(
        committeeprints_associated_bills,
        ["print_id", "bill_id"],
        "committeeprints_associated_bills",
    )
    logger.info(
        f"Found {len(committeeprints_associated_bills)} committeeprints_associated_bills"
    )

    committeeprints_committees = [
        {
            "print_id": item.get("print_id"),
            "committee_code": committee.get("systemCode"),
        }
        for item in committeeprints
        for committee in item.get("committees", [])
    ]
    committeeprints_committees = check_duplicates(
        committeeprints_committees,
        ["print_id", "committee_code"],
        "committeeprints_committees",
    )
    logger.info(f"Found {len(committeeprints_committees)} committeeprints_committees")

    # drop extracted fields columns from committeeprints
    committeeprints = [
        {k: v for k, v in item.items() if k not in ["associated_bills", "committees"]}
        for item in committeeprints
    ]
    logger.info(f"Found {len(committeeprints)} committeeprints")

    Utils.copy_dicts_to_table(
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
    logger.info(f"Inserted {len(committeeprints)} committeeprints")

    Utils.copy_dicts_to_table(
        cursor,
        "staging_congressional",
        "committeeprints_associated_bills",
        ["print_id", "bill_id"],
        committeeprints_associated_bills,
    )
    logger.info(
        f"Inserted {len(committeeprints_associated_bills)} committeeprints_associated_bills"
    )

    Utils.copy_dicts_to_table(
        cursor,
        "staging_congressional",
        "committeeprints_committees",
        ["print_id", "committee_code"],
        committeeprints_committees,
    )
    logger.info(
        f"Inserted {len(committeeprints_committees)} committeeprints_committees"
    )

    logger.info("Done processing committeeprints")


def process_committeeprints_texts(data, cursor):  # TODO: check this one
    logger.info(f"Processing {len(data)} committeeprints_texts")
    # first group by source doc id
    data_by_source_doc_id = {}
    for item in data:
        if item.get("committeeprints_id") not in data_by_source_doc_id:
            data_by_source_doc_id[item.get("committeeprints_id")] = []
        data_by_source_doc_id[item.get("committeeprints_id")].append(item)
    # then set up the dictionary of print_id, raw_text, formatted_text, pdf, html, xml, png
    committeeprints_texts = []
    type_map = {
        "PDF": "pdf",
        "Formatted Text": "formatted_text",
        "Generated   HTML": "html",
        "Formatted XML": "xml",
        "Portable Network Graphics": "png",
    }
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
    committeeprints_texts = check_duplicates(
        committeeprints_texts,
        ["print_id", "formatted_text", "pdf", "html", "xml"],
        "committeeprints_texts",
    )
    logger.info(f"Found {len(committeeprints_texts)} committeeprints_texts")

    Utils.copy_dicts_to_table(
        cursor,
        "staging_congressional",
        "committeeprints_texts",
        ["print_id", "raw_text", "formatted_text", "pdf", "html", "xml", "png"],
        committeeprints_texts,
    )
    logger.info(f"Inserted {len(committeeprints_texts)} committeeprints_texts")
    logger.info("Done processing committeeprints_texts")


def process_committeereports(data, cursor):
    logger.info(f"Processing {len(data)} committeereports")
    utils = Utils()
    committeereports = [
        {
            "report_id": (
                item.get("type").lower()
                + str(item.get("number", ""))
                + "-"
                + f"{item.get('part') or ''}"
                + f"{'-' if item.get('part') else ''}"
                + str(item.get("congress", ""))
            ),
            "citation": item.get("citation"),
            "report_type": item.get("type"),
            "report_number": item.get("number"),
            "report_part": item.get("part"),
            "congress": item.get("congress"),
            "session": item.get("session"),
            "title": item.get("title"),
            "chamber": utils.standardize_chamber(item.get("chamber")),
            "is_conference_report": item.get("isConferenceReport"),
            "issued_at": utils.standardize_date(item.get("issueDate")),
            "texts_count": item.get("text", {}).get("count", 0),
            "updated_at": utils.standardize_date(item.get("updateDate")),
            # extracted fields
            "associated_bills": item.get("associatedBill", []),
            "committees": item.get("committees", []),
            "associated_treaties": item.get("associatedTreaties", []),
        }
        for item in data
    ]
    committeereports = check_duplicates(
        committeereports, "report_id", "committeereports"
    )
    logger.info(f"Found {len(committeereports)} committeereports")

    committeereports_associated_bills = [
        {
            "report_id": item.get("report_id"),
            "bill_id": f"{bill.get('type').lower()}{bill.get('number')}-{bill.get('congress')}",
        }
        for item in committeereports
        for bill in item.get("associated_bills", [])
    ]
    committeereports_associated_bills = check_duplicates(
        committeereports_associated_bills,
        ["report_id", "bill_id"],
        "committeereports_associated_bills",
    )
    logger.info(
        f"Found {len(committeereports_associated_bills)} committeereports_associated_bills"
    )

    committeereports_committees = [
        {
            "report_id": item.get("report_id"),
            "committee_code": committee.get("systemCode"),
        }
        for item in committeereports
        for committee in item.get("committees", [])
    ]
    committeereports_committees = check_duplicates(
        committeereports_committees,
        ["report_id", "committee_code"],
        "committeereports_committees",
    )
    logger.info(f"Found {len(committeereports_committees)} committeereports_committees")

    committeereports_associated_treaties = [
        {
            "report_id": item.get("report_id"),
            "treaty_id": f"td{treaty.get('congress')}-{treaty.get('number')}{treaty.get('part') or ''}",
        }
        for item in committeereports
        for treaty in item.get("associated_treaties", [])
    ]
    committeereports_associated_treaties = check_duplicates(
        committeereports_associated_treaties,
        ["report_id", "treaty_id"],
        "committeereports_associated_treaties",
    )
    logger.info(
        f"Found {len(committeereports_associated_treaties)} committeereports_associated_treaties"
    )

    # drop extracted fields columns from committeereports
    committeereports = [
        {
            k: v
            for k, v in item.items()
            if k not in ["associated_bills", "committees", "associated_treaties"]
        }
        for item in committeereports
    ]
    logger.info(f"Found {len(committeereports)} committeereports")

    Utils.copy_dicts_to_table(
        cursor,
        "staging_congressional",
        "committeereports_associated_bills",
        ["report_id", "bill_id"],
        committeereports_associated_bills,
    )
    logger.info(
        f"Inserted {len(committeereports_associated_bills)} committeereports_associated_bills"
    )

    Utils.copy_dicts_to_table(
        cursor,
        "staging_congressional",
        "committeereports_committees",
        ["report_id", "committee_code"],
        committeereports_committees,
    )
    logger.info(
        f"Inserted {len(committeereports_committees)} committeereports_committees"
    )

    Utils.copy_dicts_to_table(
        cursor,
        "staging_congressional",
        "committeereports_associated_treaties",
        ["report_id", "treaty_id"],
        committeereports_associated_treaties,
    )
    logger.info(
        f"Inserted {len(committeereports_associated_treaties)} committeereports_associated_treaties"
    )


def process_committeereports_texts(data, cursor):
    """
    Processes committeereports_texts data and inserts into staging_congressional.committeereports_texts.

    Each input item is expected to have:
      - 'committeereports_id'
      - 'formats': a list of dicts with keys: 'url', 'type', 'isErrata'

    Output table columns:
      - report_id
      - raw_text
      - formatted_text
      - formatted_text_is_errata
      - pdf
      - pdf_is_errata
    """
    logger.info(f"Processing {len(data)} committeereports_texts")

    # Group all formats by committeereports_id
    from collections import defaultdict

    grouped = defaultdict(list)
    for item in data:
        report_id = item.get("committeereports_id")
        formats = item.get("formats", [])
        if not isinstance(formats, list):
            continue
        grouped[report_id].extend(formats)

    rows = []
    for report_id, formats in grouped.items():
        # For each unique combination of formatted_text/pdf, create a row
        # There may be multiple formatted_text/pdf per report_id (e.g. pt1, pt2, etc.)
        # We'll group by the "base" of the url (without -ptN) for both formatted_text and pdf

        # Helper: group formats by (pdf_url, formatted_url)
        # We'll use the full url for uniqueness, but allow for missing pdf or formatted_text
        # For each format, collect by type
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

        # If both pdf and formatted_text exist, try to pair by part number in url, else cross product
        def extract_part(url):
            # e.g. ...hrpt168-pt1.pdf or ...hrpt168-pt2.htm
            import re

            m = re.search(r"-pt(\d+)", url or "")
            return m.group(1) if m else None

        # Build a mapping of part number to pdf/formatted_text
        pdf_by_part = {}
        for pdf in pdfs:
            part = extract_part(pdf["url"])
            pdf_by_part[part] = pdf

        formatted_by_part = {}
        for ft in formatted_texts:
            part = extract_part(ft["url"])
            formatted_by_part[part] = ft

        # Union of all part numbers
        all_parts = set(pdf_by_part.keys()) | set(formatted_by_part.keys())
        if not all_parts:
            # If no part numbers, just do all combinations (should be rare)
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

    # Remove duplicates (by report_id, formatted_text, pdf)
    seen = set()
    deduped_rows = []
    for row in rows:
        key = (row["report_id"], row["formatted_text"], row["pdf"])
        if key not in seen:
            seen.add(key)
            deduped_rows.append(row)

    logger.info(
        f"Inserting {len(deduped_rows)} rows into staging_congressional.committeereports_texts"
    )
    Utils.copy_dicts_to_table(
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
    logger.info(f"Inserted {len(deduped_rows)} committeereports_texts")
    logger.info("Done processing committeereports_texts")


def process_committees(data, cursor):
    logger.info(f"Processing {len(data)} committees")
    utils = Utils()
    committees = [
        {
            "committee_code": item.get("systemCode"),
            "name": item.get("name"),
            "chamber": item.get("chamber"),
            "is_subcommittee": item.get("parent") is not None,
            "is_current": item.get("isCurrent"),
            "bills_count": item.get("bills", {}).get("count", 0),
            "reports_count": item.get("reports", {}).get("count", 0),
            "nominations_count": item.get("nominations", {}).get("count", 0),
            "updated_at": utils.standardize_date(item.get("updateDate")),
            # extracted fields
            "history": item.get("history", []),
            "subcommittees": item.get("subcommittees", []),
        }
        for item in data
    ]
    committees = check_duplicates(committees, ["committee_code"], "committees")
    logger.info(f"Found {len(committees)} committees")

    committees_subcommittees = [
        {
            "committee_code": item.get("committee_code"),
            "subcommittee_code": subcommittee.get("systemCode"),
        }
        for item in committees
        for subcommittee in item.get("subcommittees", [])
    ]
    committees_subcommittees = check_duplicates(
        committees_subcommittees,
        ["committee_code", "subcommittee_code"],
        "committees_subcommittees",
    )
    logger.info(f"Found {len(committees_subcommittees)} committees_subcommittees")

    committees_history = [
        {
            "committee_code": item.get("committee_code"),
            "name": hist.get("officialName"),
            "loc_name": hist.get("libraryOfCongressName"),
            "started_at": utils.standardize_date(hist.get("startDate")),
            "ended_at": utils.standardize_date(hist.get("endDate")),
            "committee_type": hist.get("committeeTypeCode"),
            "establishing_authority": hist.get("establishingAuthority"),
            "su_doc_class_number": hist.get("superintendentDocumentNumber"),
            "nara_id": hist.get("naraId"),
            "loc_linked_data_id": hist.get("locLinkedDataId"),
        }
        for item in committees
        for hist in item.get("history", [])
    ]
    committees_history = check_duplicates(
        committees_history,
        ["committee_code", "started_at", "ended_at"],
        "committees_history",
    )
    logger.info(f"Found {len(committees_history)} committees_history")

    # drop extracted fields columns from committees
    committees = [
        {k: v for k, v in item.items() if k not in ["history", "subcommittees"]}
        for item in committees
    ]
    logger.info(f"Found {len(committees)} committees")

    Utils.copy_dicts_to_table(
        cursor,
        "staging_congressional",
        "committees_subcommittees",
        ["committee_code", "subcommittee_code"],
        committees_subcommittees,
    )
    logger.info(f"Inserted {len(committees_subcommittees)} committees_subcommittees")

    Utils.copy_dicts_to_table(
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
    logger.info(f"Inserted {len(committees_history)} committees_history")

    logger.info("Done processing committees")


def process_committees_committeereports(data, cursor):
    logger.info(f"Processing {len(data)} committees_committeereports")
    committees_committeereports = [
        {
            "committee_code": item.get("committee_code"),
            "report_id": (
                item.get("type").lower()
                + str(item.get("number", ""))
                + "-"
                + f"{item.get('part') or ''}"
                + f"{'-' if item.get('part') else ''}"
                + str(item.get("congress", ""))
            ),
        }
        for item in data
    ]
    committees_committeereports = check_duplicates(
        committees_committeereports,
        ["committee_code", "report_id"],
        "committees_committeereports",
    )
    logger.info(f"Found {len(committees_committeereports)} committees_committeereports")

    Utils.copy_dicts_to_table(
        cursor,
        "staging_congressional",
        "committees_committeereports",
        ["committee_code", "report_id"],
        committees_committeereports,
    )
    logger.info(
        f"Inserted {len(committees_committeereports)} committees_committeereports"
    )
    logger.info("Done processing committees_committeereports")

def process_committees_bills(data, cursor):
    """
    Processes a list of committees bills API response objects and inserts into:
        - staging_congressional.committees_bills
    """
    logger.info(f"Processing {len(data)} committees_bills")

    raise NotImplementedError("Not implemented")


def process_congresses(data, cursor):
    """
    Processes a list of congress API response objects and inserts into:
        - staging_congressional.congresses
        - staging_congressional.congresses_sessions
    """
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
                "updated_at": updated_at,
            }
        )

        # Sessions (if present)
        sessions = item.get("sessions", {}).get("item", [])
        # If only one session, it may not be a list
        if isinstance(sessions, dict):
            sessions = [sessions]
        for session in sessions:
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
                    "start_date": start_date,
                    "end_date": end_date,
                }
            )

    congresses = check_duplicates(congresses, ["congress_number"], "congresses")
    logger.info(f"Found {len(congresses)} congresses")
    Utils.copy_dicts_to_table(
        cursor,
        "staging_congressional",
        "congresses",
        ["congress_number", "name", "start_year", "end_year", "updated_at"],
        congresses,
    )
    logger.info(f"Inserted {len(congresses)} congresses")

    if congresses_sessions:
        congresses_sessions = check_duplicates(
            congresses_sessions,
            ["congress_number", "session", "chamber", "type"],
            "congresses_sessions",
        )
        logger.info(f"Found {len(congresses_sessions)} congresses_sessions")
        Utils.copy_dicts_to_table(
            cursor,
            "staging_congressional",
            "congresses_sessions",
            ["congress_number", "session", "chamber", "type", "start_date", "end_date"],
            congresses_sessions,
        )
        logger.info(f"Inserted {len(congresses_sessions)} congresses_sessions")
    congresses = check_duplicates(congresses, ["congress_number"], "congresses")
    logger.info(f"Found {len(congresses)} congresses")

    Utils.copy_dicts_to_table(
        cursor,
        "staging_congressional",
        "congresses",
        [
            "congress_number",
            "congress_name",
            "congress_type",
            "congress_start_date",
            "congress_end_date",
            "congress_updated_at",
        ],
        congresses,
    )
    logger.info(f"Inserted {len(congresses)} congresses")


def process_members(data, cursor):
    """
    Processes member data and inserts into:
        - staging_congressional.members
        - staging_congressional.members_terms
        - staging_congressional.members_leadership_roles
        - staging_congressional.members_party_history
    """
    logger.info(f"Processing {len(data)} members")
    utils = Utils()

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

        members.append(
            {
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
                "district": utils.safe_int(item.get("district")),
                "birth_year": utils.safe_int(item.get("birthYear")),
                "death_year": utils.safe_int(item.get("deathYear")),
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
                "updated_at": utils.standardize_date(item.get("updateDate")),
            }
        )

        # Terms of service
        terms = item.get("terms", {}).get("item", [])
        if isinstance(terms, dict):
            terms = [terms]

        for term in terms:
            if term.get("congress") and term.get("startYear"):
                members_terms.append(
                    {
                        "bioguide_id": bioguide_id,
                        "member_type": term.get("memberType"),
                        "chamber": utils.standardize_chamber(term.get("chamber")),
                        "congress": utils.safe_int(term.get("congress")),
                        "start_year": utils.safe_int(term.get("startYear")),
                        "end_year": utils.safe_int(term.get("endYear")),
                        "state_name": term.get("stateName"),
                        "state_code": term.get("stateCode"),
                        "district": utils.safe_int(term.get("district")),
                    }
                )

        # Leadership roles
        leadership = item.get("leadership", {}).get("item", [])
        if isinstance(leadership, dict):
            leadership = [leadership]

        for role in leadership:
            if role.get("type") and role.get("congress"):
                members_leadership_roles.append(
                    {
                        "bioguide_id": bioguide_id,
                        "role": role.get("type"),
                        "congress": utils.safe_int(role.get("congress")),
                        "chamber": utils.standardize_chamber(role.get("chamber")),
                        "is_current": role.get("current") == "True",
                    }
                )

        # Party history (extracted from terms)
        party_history_seen = set()
        for term in terms:
            if term.get("partyCode") and term.get("startYear"):
                party_key = (
                    term.get("partyCode"),
                    term.get("startYear"),
                    term.get("endYear"),
                )
                if party_key not in party_history_seen:
                    party_history_seen.add(party_key)
                    members_party_history.append(
                        {
                            "bioguide_id": bioguide_id,
                            "party_code": term.get("partyCode"),
                            "party_name": term.get("partyName"),
                            "start_year": utils.safe_int(term.get("startYear")),
                            "end_year": utils.safe_int(term.get("endYear")),
                        }
                    )

    # Remove duplicates
    members = check_duplicates(members, ["bioguide_id"], "members")
    members_terms = check_duplicates(
        members_terms, ["bioguide_id", "start_year", "end_year"], "members_terms"
    )
    members_leadership_roles = check_duplicates(
        members_leadership_roles,
        ["bioguide_id", "role", "congress", "chamber"],
        "members_leadership_roles",
    )
    members_party_history = check_duplicates(
        members_party_history,
        ["bioguide_id", "party_code", "start_year", "end_year"],
        "members_party_history",
    )

    logger.info(f"Found {len(members)} members")
    logger.info(f"Found {len(members_terms)} members_terms")
    logger.info(f"Found {len(members_leadership_roles)} members_leadership_roles")
    logger.info(f"Found {len(members_party_history)} members_party_history")

    # Insert into tables
    if members:
        Utils.copy_dicts_to_table(
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
        logger.info(f"Inserted {len(members)} members")

    if members_terms:
        Utils.copy_dicts_to_table(
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
        logger.info(f"Inserted {len(members_terms)} members_terms")

    if members_leadership_roles:
        Utils.copy_dicts_to_table(
            cursor,
            "staging_congressional",
            "members_leadership_roles",
            ["bioguide_id", "role", "congress", "chamber", "is_current"],
            members_leadership_roles,
        )
        logger.info(
            f"Inserted {len(members_leadership_roles)} members_leadership_roles"
        )

    if members_party_history:
        Utils.copy_dicts_to_table(
            cursor,
            "staging_congressional",
            "members_party_history",
            ["bioguide_id", "party_code", "party_name", "start_year", "end_year"],
            members_party_history,
        )
        logger.info(f"Inserted {len(members_party_history)} members_party_history")

    logger.info("Done processing members")




def process_hearings(data, cursor):
    logger.info(f"Processing {len(data)} hearings")

    utils = Utils()
    hearings = []
    hearings_committees = []
    hearings_dates = []
    hearings_texts = []

    for item in data:
        hearing_id = item.get("id")
        hearings.append(
            {
                "hearing_id": hearing_id,
                "hearing_jacketnumber": item.get("jacketNumber"),
                "loc_id": item.get("libraryOfCongressIdentifier"),
                "title": item.get("title"),
                "congress": item.get("congress"),
                "chamber": utils.standardize_chamber(item.get("chamber")),
                "hearing_number": item.get("number"),
                "part_number": item.get("part"),
                "citation": item.get("citation"),
                "updated_at": utils.standardize_date(item.get("updateDate")),
            }
        )

        # Committees
        committees = item.get("committees", {}).get("item", [])
        # Congress.gov API sometimes returns a dict if only one committee, so normalize to list
        if isinstance(committees, dict):
            committees = [committees]
        for committee in committees:
            committee_code = committee.get("systemCode")
            if committee_code:
                hearings_committees.append(
                    {
                        "hearing_id": hearing_id,
                        "committee_code": committee_code,
                    }
                )

        # Dates
        dates = item.get("dates", {}).get("item", [])
        if isinstance(dates, dict):
            dates = [dates]
        for date_item in dates:
            date_val = date_item.get("date")
            if date_val:
                hearings_dates.append(
                    {
                        "hearing_id": hearing_id,
                        "hearing_date": utils.standardize_date(date_val),
                    }
                )

        # Texts (formats)
        formats = item.get("formats", {}).get("item", [])
        if isinstance(formats, dict):
            formats = [formats]
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

    # Deduplicate
    hearings = check_duplicates(hearings, ["hearing_id"], "hearings")
    hearings_committees = check_duplicates(
        hearings_committees, ["hearing_id", "committee_code"], "hearings_committees"
    )
    hearings_dates = check_duplicates(
        hearings_dates, ["hearing_id", "hearing_date"], "hearings_dates"
    )
    hearings_texts = check_duplicates(
        hearings_texts, ["hearing_id", "pdf", "formatted_text"], "hearings_texts"
    )

    # Insert into tables
    if hearings:
        Utils.copy_dicts_to_table(
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
        logger.info(f"Inserted {len(hearings)} hearings")

    if hearings_committees:
        Utils.copy_dicts_to_table(
            cursor,
            "staging_congressional",
            "hearings_committees",
            ["hearing_id", "committee_code"],
            hearings_committees,
        )
        logger.info(f"Inserted {len(hearings_committees)} hearings_committees")

    if hearings_dates:
        Utils.copy_dicts_to_table(
            cursor,
            "staging_congressional",
            "hearings_dates",
            ["hearing_id", "hearing_date"],
            hearings_dates,
        )
        logger.info(f"Inserted {len(hearings_dates)} hearings_dates")

    if hearings_texts:
        Utils.copy_dicts_to_table(
            cursor,
            "staging_congressional",
            "hearings_texts",
            ["hearing_id", "raw_text", "pdf", "formatted_text"],
            hearings_texts,
        )
        logger.info(f"Inserted {len(hearings_texts)} hearings_texts")

def process_nominations(data, cursor):
    logger.info(f"Processing {len(data)} nominations")

    utils = Utils()
    nominations_positions = []
    nominations = []

    for item in data:

        nomination_id = f"PN{item.get('number')}-{item.get('partNumber', '00')}-{item.get('congress')}"
        nominations.append(
            {
                "nomination_id": nomination_id,
                "nomination_number": item.get("number"),
                "part_number": item.get("partNumber", "00"),
                "congress": item.get("congress"),
                "description": item.get("description"),
                "is_privileged": item.get("isPrivileged"),
                "is_civilian": item.get("isList"),
                "received_at": utils.standardize_date(item.get("receivedDate")),
                "authority_date": utils.standardize_date(item.get("authorityDate")),
                "executive_calendar_number": item.get("executiveCalendarNumber"),
                "citation": item.get("citation"),
                "committees_count": item.get("committees", {}).get("count", 0),
                "actions_count": item.get("actions", {}).get("count", 0),
                "updated_at": utils.standardize_date(item.get("updateDate")),
            }
        )

        if item.get("nominees"):
            for nominee in item.get("nominees"):
                nominations_positions.append(
                    {
                        "nomination_id": nomination_id,
                        "ordinal": nominee.get("ordinal"),
                        "position_title": nominee.get("positionTitle"),
                        "organization": nominee.get("organization"),
                        "intro_text": utils.clean_long_text(nominee.get("introText")),
                        "nominee_count": nominee.get("nomineeCount"),
                    }
                )

    # Deduplicate
    nominations = check_duplicates(nominations, ["nomination_id"], "nominations")
    nominations_positions = check_duplicates(
        nominations_positions, ["nomination_id", "ordinal"], "nominations_positions"
    )

    # Insert into tables
    if nominations:
        Utils.copy_dicts_to_table(
            cursor,
            "staging_congressional",
            "nominations",
            ["nomination_id", "nomination_number", "part_number", "congress", "description", "is_privileged", "is_civilian", "received_at", "authority_date", "executive_calendar_number", "citation", "committees_count", "actions_count", "updated_at"],
            nominations,
        )
        logger.info(f"Inserted {len(nominations)} nominations")

    if nominations_positions:
        Utils.copy_dicts_to_table(
            cursor,
            "staging_congressional",
            "nominations_positions",
            ["nomination_id", "ordinal", "position_title", "organization", "intro_text", "nominee_count"],
            nominations_positions,
        )
        logger.info(f"Inserted {len(nominations_positions)} nominations_positions")

    logger.info("Done processing nominations")

def process_nominations_actions(data, cursor):
    logger.info(f"Processing {len(data)} nominations_actions")

    utils = Utils()
    nominations_actions = []
    nominations_actions_committees = []
    for item in data:
        action_id = hashlib.sha256(json.dumps(item).encode()).hexdigest()

        nominations_actions.append(
            {
                "action_id": action_id,
                "nomination_id": item.get("nominations_id"),
                "action_code": item.get("actionCode"),
                "action_type": item.get("type"),
                "action_date": utils.standardize_date(item.get("date")),
                "text": utils.clean_long_text(item.get("text")),
            }
        )

        if item.get('committees'):
            for committee in item.get('committees'):
                nominations_actions_committees.append(
                    {
                        "action_id": action_id,
                        "nomination_id": item.get("nominations_id"),
                        "committee_code": committee.get("systemCode"),
                    }
                )

    # Deduplicate
    nominations_actions = check_duplicates(nominations_actions, ["action_id", "nomination_id"], "nominations_actions")
    nominations_actions_committees = check_duplicates(nominations_actions_committees, ["action_id", "nomination_id", "committee_code"], "nominations_actions_committees")

    # Insert into tables
    if nominations_actions:
        Utils.copy_dicts_to_table(
            cursor,
            "staging_congressional",
            "nominations_actions",
            ["action_id", "nomination_id", "action_code", "action_type", "action_date", "text"],
            nominations_actions,
        )
        logger.info(f"Inserted {len(nominations_actions)} nominations_actions")

    if nominations_actions_committees:
        Utils.copy_dicts_to_table(
            cursor,
            "staging_congressional",
            "nominations_actions_committees",
            ["action_id", "nomination_id", "committee_code"],
            nominations_actions_committees,
        )
        logger.info(f"Inserted {len(nominations_actions_committees)} nominations_actions_committees")

    logger.info("Done processing nominations_actions")

def process_nominations_committeeactivities(data, cursor):
    logger.info(f"Processing {len(data)} nominations_committeeactivities")

    utils = Utils()
    nominations_committeeactivities = []
    for item in data:
        if item.get('committee_code', '')[-2:] != '00':
            continue
        nominations_committeeactivities.append(
            {
                "nomination_id": item.get("nominations_id"),
                "committee_code": item.get("committee_code"),
                "activity_name": item.get("activity_name"),
                "activity_date": utils.standardize_date(item.get("activity_date")),
            }
        )

    # Deduplicate
    nominations_committeeactivities = check_duplicates(nominations_committeeactivities, ["nomination_id", "committee_code", "activity_date", "activity_name"], "nominations_committeeactivities")

    # Insert into tables
    if nominations_committeeactivities:
        Utils.copy_dicts_to_table(
            cursor,
            "staging_congressional",
            "nominations_committeeactivities",
            ["nomination_id", "committee_code", "activity_name", "activity_date"],
            nominations_committeeactivities,
        )
        logger.info(f"Inserted {len(nominations_committeeactivities)} nominations_committeeactivities")

    logger.info("Done processing nominations_committeeactivities")

def process_nominations_hearings(data, cursor):
    logger.info(f"Processing {len(data)} nominations_hearings")

    nominations_associated_hearings = []
    for item in data:
        chamber = item.get('chamber')
        jacketnumber = item.get('jacketNumber')
        citation = item.get("citation")
        if citation:
            numbers = citation.split('.')[2]
            if numbers:
                congress = numbers.split('-')[0]
                if chamber and jacketnumber and congress:
                    hearing_id = f"{chamber[0].lower()}hrg{jacketnumber}-{congress}"
            else:
                hearing_id = citation
        else:
            hearing_id = 'ID_ERROR'

        nominations_associated_hearings.append(
            {
                "nomination_id": item.get("nominations_id"),
                "hearing_id": hearing_id,
            }
        )

    # Deduplicate
    nominations_associated_hearings = check_duplicates(nominations_associated_hearings, ["nomination_id", "hearing_id"], "nominations_hearings")

    # Insert into tables
    if nominations_associated_hearings:
        Utils.copy_dicts_to_table(
            cursor,
            "staging_congressional",
            "nominations_associated_hearings",
            ["nomination_id", "hearing_id"],
            nominations_associated_hearings,
        )
        logger.info(f"Inserted {len(nominations_associated_hearings)} nominations_associated_hearings")

    logger.info("Done processing nominations_associated_hearings")

def process_nominations_nominees(data, cursor):
    logger.info(f"Processing {len(data)} nominations_nominees")

    utils = Utils()
    nominations_nominees = []
    for item in data:
        nominations_nominees.append(
            {
                "nomination_id": item.get("nominations_id"),
                "ordinal": item.get("position_id"),
                "first_name": item.get("firstName"),
                "middle_name": item.get("middleName"),
                "last_name": item.get("lastName"),
                "prefix": item.get("prefix"),
                "suffix": item.get("suffix"),
                "state": item.get("state"),
                "effective_date": utils.standardize_date(item.get("effectiveDate")),
                "predecessor_name": item.get("predecessorName"),
                "corps_code": item.get("corpsCode"),
            }
        )

    # Deduplicate
    nominations_nominees = check_duplicates(nominations_nominees, ["nomination_id", "ordinal", "first_name", "middle_name", "last_name"], "nominations_nominees")

    # Insert into tables
    if nominations_nominees:
        Utils.copy_dicts_to_table(
            cursor,
            "staging_congressional",
            "nominations_nominees",
            ["nomination_id", "ordinal", "first_name", "middle_name", "last_name", "prefix", "suffix", "state", "effective_date", "predecessor_name", "corps_code"],
            nominations_nominees,
        )
        logger.info(f"Inserted {len(nominations_nominees)} nominations_nominees")

    logger.info("Done processing nominations_nominees")

def process_treaties(data, cursor):
    logger.info(f"Processing {len(data)} treaties")

    utils = Utils()
    treaties = []
    treaties_country_parties = []
    treaties_index_terms = []
    treaties_titles = []
    for item in data:
        treaty_id = f"td{item.get('congressReceived')}-{item.get('number')}{item.get('suffix', '')}"
        treaties.append(
            {
                "treaty_id": treaty_id,
                "treaty_number": item.get("number"),
                "suffix": item.get("suffix"),
                "congress_received": item.get("congressReceived"),
                "congress_considered": item.get("congressConsidered"),
                "topic": item.get("topic"),
                "transmitted_at": utils.standardize_date(item.get("transmittedDate")),
                "in_force_at": utils.standardize_date(item.get("inForceDate")),
                "resolution_text": utils.clean_long_text(item.get("resolutionText")),
                "parts_count": item.get("parts", {}).get("count", 0),
                "actions_count": item.get("actions", {}).get("count", 0),
                "old_number": item.get("oldNumber"),
                "old_number_display_name": item.get("oldNumberDisplayName"),
                "updated_at": utils.standardize_date(item.get("updateDate")),
            }
        )

        if item.get('countriesParties'):
            for country_party in item.get('countriesParties'):
                treaties_country_parties.append(
                    {
                        "treaty_id": treaty_id,
                        "country": country_party.get("name"),
                    }
                )
        if item.get('indexTerms'):
            for index_term in item.get('indexTerms'):
                treaties_index_terms.append(
                    {
                        "treaty_id": treaty_id,
                        "index_term": index_term.get("name"),
                    }
                )
        if item.get('titles'):
            for title in item.get('titles'):
                treaties_titles.append(
                    {
                        "treaty_id": treaty_id,
                        "title": title.get("title"),
                        "title_type": title.get("titleType"),
                    }
                )

    # Deduplicate
    treaties = check_duplicates(treaties, ["treaty_id"], "treaties")
    treaties_country_parties = check_duplicates(treaties_country_parties, ["treaty_id", "country"], "treaties_country_parties")
    treaties_index_terms = check_duplicates(treaties_index_terms, ["treaty_id", "index_term"], "treaties_index_terms")
    treaties_titles = check_duplicates(treaties_titles, ["treaty_id", "title", "title_type"], "treaties_titles")

    # Insert into tables
    if treaties:
        Utils.copy_dicts_to_table(
            cursor,
            "staging_congressional",
            "treaties",
            ["treaty_id", "treaty_number", "suffix", "congress_received", "congress_considered", "topic", "transmitted_at", "in_force_at", "resolution_text", "parts_count", "actions_count", "old_number", "old_number_display_name", "updated_at"],
            treaties,
        )
        logger.info(f"Inserted {len(treaties)} treaties")

    if treaties_country_parties:

        Utils.copy_dicts_to_table(
            cursor,
            "staging_congressional",
            "treaties_country_parties",
            ["treaty_id", "country"],
            treaties_country_parties,
        )
        logger.info(f"Inserted {len(treaties_country_parties)} treaties_country_parties")

    if treaties_index_terms:

        Utils.copy_dicts_to_table(
            cursor,
            "staging_congressional",
            "treaties_index_terms",
            ["treaty_id", "index_term"],
            treaties_index_terms,
        )
        logger.info(f"Inserted {len(treaties_index_terms)} treaties_index_terms")

    if treaties_titles:

        Utils.copy_dicts_to_table(
            cursor,
            "staging_congressional",
            "treaties_titles",
            ["treaty_id", "title", "title_type"],
            treaties_titles,
        )
        logger.info(f"Inserted {len(treaties_titles)} treaties_titles")

    logger.info("Done processing treaties")

def process_treaties_actions(data, cursor):
    logger.info(f"Processing {len(data)} treaties_actions")
    utils = Utils()
    treaties_actions = []
    treaties_actions_committees = []
    for item in data:
        action_id = hashlib.sha256(json.dumps(item).encode()).hexdigest()
        treaty_id = item.get("treaty_id")
        treaties_actions.append(
            {
                "action_id": action_id,
                "treaty_id": treaty_id,
                "action_code": item.get("actionCode"),
                "action_date": utils.standardize_date(item.get("date")),
                "text": item.get("text"),
                "action_type": item.get("type"),
            }
        )

        if item.get('committees'):
            for committee in item.get('committees'):
                treaties_actions_committees.append(
                    {
                        "action_id": action_id,
                        "treaty_id": treaty_id,
                        "committee_code": committee.get("systemCode"),
                    }
                )

    # Deduplicate
    treaties_actions = check_duplicates(treaties_actions, ["action_id", "treaty_id"], "treaties_actions")
    treaties_actions_committees = check_duplicates(treaties_actions_committees, ["action_id", "treaty_id", "committee_code"], "treaties_actions_committees")

    # Insert into tables
    if treaties_actions:
        Utils.copy_dicts_to_table(
            cursor,
            "staging_congressional",
            "treaties_actions",
            ["action_id", "treaty_id", "action_code", "action_date", "text", "action_type"],
            treaties_actions,
        )
        logger.info(f"Inserted {len(treaties_actions)} treaties_actions")

    if treaties_actions_committees:
        Utils.copy_dicts_to_table(
            cursor,
            "staging_congressional",
            "treaties_actions_committees",
            ["action_id", "treaty_id", "committee_code"],
            treaties_actions_committees,
        )
        logger.info(f"Inserted {len(treaties_actions_committees)} treaties_actions_committees")

    logger.info("Done processing treaties_actions")

def process_data(data, data_type, cursor):
    if data_type == "amendments":
        process_amendments(data, cursor)
    if data_type == "amendments_actions":
        process_amendments_actions(data, cursor)
    if data_type == "amendments_cosponsors":
        process_amendments_cosponsors(data, cursor)
    if data_type == "amendments_texts":
        process_amendments_texts(data, cursor)
    if data_type == "bills":
        process_bills(data, cursor)
    if data_type == "bills_actions":
        process_bills_actions(data, cursor)
    if data_type == "bills_summaries":
        process_bills_summaries(data, cursor)
    if data_type == "bills_subjects":
        process_bills_subjects(data, cursor)
    if data_type == "bills_titles":
        process_bills_titles(data, cursor)
    if data_type == "bills_texts":
        process_bills_texts(data, cursor)
    if data_type == "bills_cosponsors":
        process_bills_cosponsors(data, cursor)
    if data_type == "bills_relatedbills":
        process_bills_relatedbills(data, cursor)
    if data_type == "committeemeetings":
        process_committeemeetings(data, cursor)
    if data_type == "committeeprints":
        process_committeeprints(data, cursor)
    if data_type == "committeeprints_texts":
        process_committeeprints_texts(data, cursor)
    if data_type == "committeereports":
        process_committeereports(data, cursor)
    if data_type == "committeereports_texts":
        process_committeereports_texts(data, cursor)
    if data_type == "committees":
        process_committees(data, cursor)
    if data_type == "committees_committeereports":
        process_committees_committeereports(data, cursor)
    if data_type == "congresses":
        process_congresses(data, cursor)
    if data_type == "hearings":
        process_hearings(data, cursor)
    if data_type == "members":
        process_members(data, cursor)
    if data_type == "nominations":
        process_nominations(data, cursor)
    if data_type == "nominations_actions":
        process_nominations_actions(data, cursor)
    if data_type == "nominations_committeeactivities":
        process_nominations_committeeactivities(data, cursor)
    if data_type == "nominations_hearings":
        process_nominations_hearings(data, cursor)
    if data_type == 'nominations_nominees':
        process_nominations_nominees(data, cursor)
    if data_type == "treaties":
        process_treaties(data, cursor)
    if data_type == "treaties_actions":
        process_treaties_actions(data, cursor)

def connect_to_database():
    conn = psycopg2.connect(
        host=os.getenv("POSTGRESQL_HOST"),
        database=os.getenv("POSTGRESQL_DATABASE"),
        user=os.getenv("POSTGRESQL_USERNAME"),
        password=os.getenv("POSTGRESQL_PASSWORD"),
        port=os.getenv("POSTGRESQL_PORT"),
    )
    return conn

def main():
    conn = connect_to_database()
    cursor = conn.cursor()
    conn.autocommit = True
    data = get_data_from_raw_table("amendments", "amendments_raw", cursor, schema='bicam_raw_congressional')
    process_data(data, "amendments", cursor)
    data = get_data_from_raw_table("amendments", "amendments_actions_raw", cursor, schema='bicam_raw_congressional', related=True)
    process_data(data, "amendments_actions", cursor)
    data = get_data_from_raw_table("amendments", "amendments_cosponsors_raw", cursor, schema='bicam_raw_congressional', related=True)
    process_data(data, "amendments_cosponsors", cursor)
    data = get_data_from_raw_table("amendments", "amendments_texts_raw", cursor, schema='bicam_raw_congressional', related=True)
    process_data(data, "amendments_texts", cursor)
    # data = get_data_from_raw_table("bills", "bills_raw", cursor, schema='bicam_raw_congressional')
    # process_data(data, "bills", cursor)
    # data = get_data_from_raw_table(
    #     "bills",
    #     "bills_relatedbills_raw",
    #     cursor,
    #     schema="bicam_raw_congressional",
    #     related=True,
    # )
    # process_data(data, "bills_relatedbills", cursor)
    # data = get_data_from_raw_table(
    #     "bills",
    #     "bills_actions_raw",
    #     cursor,
    #     schema="bicam_raw_congressional",
    #     related=True,
    # )
    # process_data(data, "bills_actions", cursor)
    # data = get_data_from_raw_table(
    #     "bills",
    #     "bills_summaries_raw",
    #     cursor,
    #     schema="bicam_raw_congressional",
    #     related=True,
    # )
    # process_data(data, "bills_summaries", cursor)
    # data = get_data_from_raw_table(
    #     "bills",
    #     "bills_subjects_raw",
    #     cursor,
    #     schema="bicam_raw_congressional",
    #     related=True,
    # )
    # process_data(data, "bills_subjects", cursor)
    # data = get_data_from_raw_table(
    #     "bills",
    #     "bills_titles_raw",
    #     cursor,
    #     schema="bicam_raw_congressional",
    #     related=True,
    # )
    # process_data(data, "bills_titles", cursor)
    # data = get_data_from_raw_table(
    #     "bills",
    #     "bills_texts_raw",
    #     cursor,
    #     schema="bicam_raw_congressional",
    #     related=True,
    # )
    # process_data(data, "bills_texts", cursor)
    # data = get_data_from_raw_table(
    #     "bills",
    #     "bills_cosponsors_raw",
    #     cursor,
    #     schema="bicam_raw_congressional",
    #     related=True,
    # )
    # process_data(data, "bills_cosponsors", cursor)
    # data = get_data_from_raw_table(
    #     "committeemeetings",
    #     "committeemeetings_raw",
    #     cursor,
    #     schema="bicam_raw_congressional",
    # )
    process_data(data, "committeemeetings", cursor)
    data = get_data_from_raw_table(
        "committeeprints",
        "committeeprints_raw",
        cursor,
        schema="bicam_raw_congressional",
    )
    process_data(data, "committeeprints", cursor)
    data = get_data_from_raw_table(
        "committeeprints",
        "committeeprints_texts_raw",
        cursor,
        schema="bicam_raw_congressional",
        related=True,
    )
    process_data(data, "committeeprints_texts", cursor)
    data = get_data_from_raw_table(
        "committeereports",
        "committeereports_raw",
        cursor,
        schema="bicam_raw_congressional",
    )
    process_data(data, "committeereports", cursor)
    data = get_data_from_raw_table(
        "committeereports",
        "committeereports_texts_raw",
        cursor,
        schema="bicam_raw_congressional",
        related=True,
    )
    process_data(data, "committeereports_texts", cursor)
    data = get_data_from_raw_table(
        "committees", "committees_raw", cursor, schema="bicam_raw_congressional"
    )
    process_data(data, "committees", cursor)
    data = get_data_from_raw_table(
        "committees",
        "committees_committeereports_raw",
        cursor,
        schema="bicam_raw_congressional",
        related=True,
    )
    process_data(data, "committees_committeereports", cursor)
    data = get_data_from_raw_table(
        "congresses", "congresses_raw", cursor, schema="bicam_raw_congressional",
    )
    process_data(data, "congresses", cursor)
    data = get_data_from_raw_table(
        "hearings", "hearings_raw", cursor, schema="bicam_raw_congressional"
    )
    process_data(data, "hearings", cursor)
    data = get_data_from_raw_table(
        "nominations", "nominations_raw", cursor, schema="bicam_raw_congressional"
    )
    process_data(data, "nominations", cursor)
    data = get_data_from_raw_table(
        "nominations", "nominations_actions_raw", cursor, schema="bicam_raw_congressional", related=True
    )
    process_data(data, "nominations_actions", cursor)
    data = get_data_from_raw_table(
        "nominations", "nominations_committeeactivities_raw", cursor, schema="bicam_raw_congressional", related=True
    )
    process_data(data, "nominations_committeeactivities", cursor)
    data = get_data_from_raw_table(
        "nominations", "nominations_hearings_raw", cursor, schema="bicam_raw_congressional", related=True
    )
    process_data(data, "nominations_hearings", cursor)
    data = get_data_from_raw_table(
        "nominations", "nominations_invidivualnominees_raw", cursor, schema="bicam_raw_congressional", related=True
    )
    process_data(data, "nominations_nominees", cursor)
    data = get_data_from_raw_table(
        "treaties", "treaties_raw", cursor, schema="bicam_raw_congressional"
    )
    process_data(data, "treaties", cursor)
    data = get_data_from_raw_table(
        "treaties", "treaties_actions_raw", cursor, schema="bicam_raw_congressional", related=True
    )
    process_data(data, "treaties_actions", cursor)
    conn.commit()
    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
