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

from streamlined.libs import (
    CONGRESSIONALREPORT_BIOGUIDE_FIXES,
)
from streamlined.libs.manual_fixes import CONGRESSIONALREPORT_ID_FIXES
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

    def _normalize_report_type(self, report_type_raw: str | None) -> str | None:
        """Normalize document type to canonical tokens like 'hrpt' or 'srpt'.

        Accepts common variants (e.g., 'H. Rept.', 'House Report', 'S Rept', etc.).
        """
        if not report_type_raw:
            return None
        s = str(report_type_raw).lower().strip()
        # remove punctuation and extra spaces
        s = re.sub(r"[\s\._-]+", " ", s)
        s_nopunct = re.sub(r"[^a-z0-9 ]", "", s)

        mappings = {
            # House report
            "hrpt": "hrpt",
            "h rept": "hrpt",
            "h rpt": "hrpt",
            "h rep": "hrpt",
            "h report": "hrpt",
            "house report": "hrpt",
            "house rept": "hrpt",
            # Senate report
            "srpt": "srpt",
            "s rept": "srpt",
            "s rpt": "srpt",
            "s rep": "srpt",
            "s report": "srpt",
            "senate report": "srpt",
            "senate rept": "srpt",
        }

        # Direct hit
        if s in mappings:
            return mappings[s]
        if s_nopunct in mappings:
            return mappings[s_nopunct]

        # Try collapsing to letters only
        letters_only = re.sub(r"[^a-z]", "", s_nopunct)
        if letters_only in ("hrpt", "hrept", "hreport"):
            return "hrpt"
        if letters_only in ("srpt", "srept", "sreport"):
            return "srpt"

        # Fallback to raw token if already hrpt/srpt-like
        if letters_only in ("hrpt", "srpt"):
            return letters_only

        return report_type_raw.lower()

    def _normalize_report_number(self, num: Any) -> str | None:
        """Extract numeric component of documentnumber if present.

        Returns string digits or None.
        """
        if num is None:
            return None
        s = str(num).strip()
        m = re.findall(r"\d+", s)
        if not m:
            return s or None
        return "".join(m)

    def _parse_heading_for_part(self, cleaned: dict[str, Any]) -> int | None:
        """Look for part/volume indications in heading/title/subtitle fields."""
        fields = [
            cleaned.get("heading"),
            cleaned.get("title"),
            cleaned.get("subtitle"),
            cleaned.get("shorttitle"),
        ]
        for val in fields:
            if not val:
                continue
            text = str(val)
            m = re.search(r"\bpart\s+([ivxlcdm]+|\d+)\b", text, flags=re.IGNORECASE)
            if m:
                token = m.group(1)
                if token.isdigit():
                    return int(token)
                roman_val = self.roman_to_int(token)
                if roman_val:
                    return roman_val
            m = re.search(r"\bvol(?:ume)?\s+([ivxlcdm]+|\d+)\b", text, flags=re.IGNORECASE)
            if m:
                token = m.group(1)
                if token.isdigit():
                    return int(token)
                roman_val = self.roman_to_int(token)
                if roman_val:
                    return roman_val
        return None

    def _detect_is_supplement(self, cleaned: dict[str, Any]) -> bool:
        """Detect if record is a supplement/appendix/addendum variant."""
        candidates = [cleaned.get("heading"), cleaned.get("title"), cleaned.get("subtitle")]
        for val in candidates:
            if not val:
                continue
            s = str(val).lower()
            if any(token in s for token in ("supplement", "suppl", "supp.", "appendix", "app.", "addendum")):
                return True
        return False

    def _build_report_identity(
        self, cleaned: dict[str, Any]
    ) -> tuple[str, str | None, str | None, int, str, str, str, str]:
        """
        Build canonical identity for congressional report rows (packages or granules).

        Returns tuple:
        - report_id
        - report_set_id (nullable)
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
        raw_id = str(
            cleaned.get("packageid")
            or cleaned.get("package_id")
            or cleaned.get("granuleid")
            or cleaned.get("granule_id")
        )
        # A row is a package if:
        # 1. No granuleid field exists, OR
        # 2. granuleid equals the packageid (same record)
        # 3. But if we have a partnumber field, it's likely a granule even if granuleid is missing
        is_package_row = (
            not cleaned.get("granuleid") and not cleaned.get("partnumber")
        ) or cleaned.get("granuleid") == raw_id

        if raw_id in CONGRESSIONALREPORT_ID_FIXES:
            manual_val = CONGRESSIONALREPORT_ID_FIXES.get(raw_id)
            report_type = manual_val.get("report_type")
            report_number = manual_val.get("report_number")
            congress = manual_val.get("congress")
            part_number = manual_val.get("part_number")
        else:
            report_type = self._normalize_report_type(cleaned.get("documenttype"))
            part_number = cleaned.get("documentpart") or cleaned.get("volumenumber") or 0
            # Extract numeric component of documentnumber if not clean
            report_number = self._normalize_report_number(cleaned.get("documentnumber"))
            congress = str(cleaned.get("congress") or "") or None

        if not all([report_type, report_number, congress]):
            raise ValueError(
                f"Invalid report_id inputs: report_type={report_type}, report_number={report_number}, congress={congress}. Record is {cleaned}"
            )

        if not is_package_row:
            part_field = cleaned.get("partnumber") or cleaned.get("documentpart") or cleaned.get("volumenumber")
            if part_field is None:
                # Parse from package ID or other fields
                parsed_part = (
                    self._parse_heading_for_part(cleaned) or self.parse_part_from_fields(cleaned)
                )
                part_number = parsed_part if parsed_part is not None else part_number
            elif cleaned.get("packageid") == cleaned.get("granuleid"):
                part_number = 0
            else:
                part_str = str(part_field).strip()
                part_number = (
                    int(part_str)
                    if part_str.isdigit()
                    else (self.roman_to_int(part_str) or 0)
                )

        report_set_id = f"{report_type}{report_number}-{congress}"

        # Append errata suffix to the part token when applicable so errata sorts
        # immediately after the main part (e.g., "2" < "2e" < "3").
        is_errata = self._detect_is_errata(cleaned)

        # Use placeholder 'x' when package shows trailing numeric tokens but no explicit part in fields
        part_token_base: str
        if self._should_use_placeholder_part(cleaned):
            part_token_base = "x"
        else:
            part_token_base = str(part_number)

        # Detect supplements and append appropriate suffix token
        is_supplement = self._detect_is_supplement(cleaned)
        suffix = ("e" if is_errata else "") + ("s" if is_supplement else "")
        part_token = f"{part_token_base}{suffix}"
        report_id = f"{report_type}{report_number}-{part_token}-{congress}"
        granule_id = (
            str(cleaned.get("granuleid") or cleaned.get("granule_id"))
            if not is_package_row
            else None
        )
        return (
            report_id,
            report_set_id,
            granule_id,
            part_number,
            report_type,
            report_number,
            congress,
        )

    def _should_use_placeholder_part(self, cleaned: dict[str, Any]) -> bool:
        """Check for when to emit placeholder part 'x'.

        Only use placeholder when we have a hierarchical part structure where we don't know
        the total number of parts. This happens when:
        1. We have multiple numeric tokens in the package ID (indicating hierarchical structure)
        2. We don't have explicit part indicators in the fields
        3. The granule_id is different from package_id (indicating this is a granule, not a package)

        If granule_id equals package_id, we should use the documentpart value, parsed part, or 0.
        """
        try:
            pkg = str(cleaned.get("packageid") or cleaned.get("package_id") or "")
            granule_id = cleaned.get("granuleid") or cleaned.get("granule_id")
            package_id = cleaned.get("packageid") or cleaned.get("package_id")

            # If granule_id equals package_id, this is a package-level record, don't use placeholder
            if granule_id and package_id and str(granule_id) == str(package_id):
                return False

            # If we have a partnumber field but no granule_id, this is likely a granule
            # that should use its actual part number, not a placeholder
            if not granule_id and cleaned.get("partnumber"):
                return False
            heading = cleaned.get("heading")
            partnumber = cleaned.get("partnumber") or cleaned.get("documentpart")
            volumenumber = cleaned.get("volumenumber")

            # If we have explicit part indicators, don't use placeholder
            if heading or partnumber or volumenumber:
                return False

            # Check if parse_part_from_fields would find a valid part number (>1)
            parsed_part = self.parse_part_from_fields(cleaned)
            if parsed_part > 1:
                return False

            # Only use placeholder for hierarchical structures (multiple numeric tokens)
            # Pattern matches things like "...-4-3" but not "...-4" (single token)
            return bool(re.search(r"\d+-\d+(?:-\d+)*$", pkg))
        except Exception:
            return False

    def roman_to_int(self, roman: str) -> int | None:
        """Convert Roman numerals up to several hundred. Returns None if invalid."""
        if not roman:
            return None
        s = roman.upper()
        values = {"I": 1, "V": 5, "X": 10, "L": 50, "C": 100, "D": 500, "M": 1000}
        total = 0
        prev = 0
        for ch in reversed(s):
            if ch not in values:
                return None
            val = values[ch]
            if val < prev:
                total -= val
            else:
                total += val
                prev = val
        return total if total > 0 else None

    def parse_part_from_fields(self, cleaned: dict[str, Any]) -> int:
        # Priority: explicit partnumber/volumenumber -> heading/title/subtitle -> packageid suffix
        part_field = cleaned.get("partnumber") or cleaned.get("documentpart") or cleaned.get("volumenumber")
        if part_field:
            part_field_str = str(part_field).strip()
            if part_field_str.isdigit():
                return int(part_field_str)
            roman_val = self.roman_to_int(part_field_str)
            if roman_val is not None:
                return roman_val

        # Try headings/titles
        parsed = self._parse_heading_for_part(cleaned)
        if parsed:
            return parsed

        # As a last resort, inspect packageid for suffix hints like volII/ptII/-pt2
        # But avoid matching report numbers (e.g., don't match "pt128" in "CRPT-104hrpt128")
        pkg = str(cleaned.get("packageid", ""))

        # Look for standalone part indicators that are not embedded in report numbers
        # Pattern: pt/vol followed by number/roman, but not if it's part of a report number
        m = re.search(
            r"(?:^|[-_.])(?:vol|pt)[-_.]?([ivx]+|\d+)(?:$|[-_.])", pkg, re.IGNORECASE
        )
        if m:
            token = m.group(1)
            # Additional check: if the token is a large number (>20), it's likely a report number, not a part
            if token.isdigit() and int(token) <= 20:
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
                        NULL as granule_id,
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
                        crg.packageid as package_id,
                        crg.granuleid AS granule_id,
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
                    GROUP BY crg.packageid, crg.granuleid, crgc.authorityid, crgc.committeename, cr.documenttype, cr.documentnumber, cr.congress, crg.partnumber, crg.heading

                    ORDER BY granule_id, authorityid, committeename
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
                        crg.packageid AS package_id,
                        crg.granuleid AS granule_id,
                        crgm.bioguideid,
                        crgm.membername,
                        crgm.authorityid,
                        crgm.gpoid,
                        crgmn.parsed,
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
                    GROUP BY crg.packageid, crg.granuleid, crgm.bioguideid, crgm.membername, crgm.authorityid, crgm.gpoid, crgmn.parsed, crgmn.authority_fnf, crgmn.authority_other, crg.partnumber, cr.documenttype, cr.documentnumber, cr.congress, cr.documentpart, crg.heading
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

                            # Check if parsed field contains " and " and split into multiple records
                            parsed_value = row_dict.get("parsed", "")
                            if parsed_value and " and " in parsed_value:
                                # Split the parsed field and create separate records
                                parsed_parts = parsed_value.split(" and ")
                                for parsed_part in parsed_parts:
                                    # Create a copy of the row with the split parsed value
                                    split_row = row_dict.copy()
                                    split_row["parsed"] = parsed_part.strip()
                                    # Add a suffix to distinguish between split records if needed
                                    chunk.append(split_row)
                            else:
                                # No splitting needed, add the original row
                                chunk.append(row_dict)

                        except Exception as e:
                            logger.error(
                                f"Error converting congressionalreports_members row to dict: {e}, row type: {type(row)}, row: {row}",
                                exc_info=True,
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
                packages_without_granules_query = f"""
                SELECT COUNT(DISTINCT c.packageid)
                FROM {self.staging_schema}.congressionalreports c
                WHERE NOT EXISTS (
                    SELECT 1 FROM {self.staging_schema}.congressionalreports_granules g
                    WHERE g.packageid = c.packageid
                )
                """
                packages_without_granules_count = await conn.fetchval(
                    packages_without_granules_query
                )

                granule_count_query = f"""
                SELECT COUNT(DISTINCT g.granuleid)
                FROM {self.staging_schema}.congressionalreports_granules g
                """
                granule_count = await conn.fetchval(granule_count_query)

                total_count = packages_without_granules_count + granule_count

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
                        -- Package-only rows (only for packages that don't have granules)
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
                        WHERE NOT EXISTS (
                            SELECT 1 FROM {self.staging_schema}.congressionalreports_granules g
                            WHERE g.packageid = c.packageid
                        )

                        UNION ALL

                        -- Granule rows (for packages that have granules)
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
                            SELECT DISTINCT ON (type, number, congress, references_id) crgrc.*, crg.granuleid AS granule_id, crg.packageid AS package_id
                            FROM {self.staging_schema}.congressionalreports_granules_references_contents AS crgrc
                            JOIN {self.staging_schema}.congressionalreports_granules_references AS crgr ON crgr.id = crgrc.references_id
                            JOIN {self.staging_schema}.congressionalreports_granules AS crg ON crgr.granule_id = crg.id
                            ORDER BY type, number, congress, references_id, crg.packageid, crg.granuleid NULLS FIRST
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
            report_set_id,
            granule_id,
            part_number,
            report_type,
            report_number,
            congress,
        ) = self._build_report_identity(cleaned)

        # Compute errata flag using shared heuristics
        errata_detected = self._detect_is_errata(cleaned)

        # Apply bills-specific cleaning logic
        filtered_cleaned = {
            "package_id": str(cleaned.get("packageid", "ID_ERROR")),
            "report_id": str(report_id),
            "granule_id": str(cleaned.get("granuleid", granule_id))
            if cleaned.get("granuleid")
            else None,
            "report_set_id": report_set_id,
            "report_type": report_type,
            "report_number": self.safe_int(report_number),
            "part_number": self.safe_int(part_number),
            "title": cleaned.get("title", None),
            "subtitle": cleaned.get("subtitle", None),
            "chamber": self.standardize_chamber(cleaned.get("chamber", None)),
            "congress": self.safe_int(cleaned.get("congress", congress)),
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

        filtered_cleaned = {
            "package_id": cleaned.get("package_id", "ID_ERROR"),
            "granule_id": cleaned.get("granule_id", "ID_ERROR"),
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
        if cleaned.get("bioguideid") is None or cleaned.get("bioguideid") == "":
            if (
                cleaned.get("parsed", "") in CONGRESSIONALREPORT_BIOGUIDE_FIXES
                and cleaned.get("bioguideid", None) is None
                or cleaned.get("bioguideid") == ""
                ):
                    cleaned["bioguideid"] = CONGRESSIONALREPORT_BIOGUIDE_FIXES.get(
                        cleaned.get("parsed", "")
                    )
                    logger.info(
                        f"Bioguide ID fixed: {cleaned.get('parsed', '')} -> {cleaned.get('bioguideid', 'ID_ERROR')}"
                    )
            else:
                cleaned["bioguideid"] = "ID_ERROR"
                raise ValueError(
                    f"Bioguide ID not found for {cleaned.get('package_id', 'ID_ERROR')}"
                )

        filtered_cleaned = {
            "package_id": cleaned.get("package_id", "ID_ERROR"),
            "granule_id": cleaned.get("granule_id", "ID_ERROR"),
            "bioguide_id": (
                cleaned.get("bioguideid")
                if cleaned.get("bioguideid", None) != ""
                else "ID_ERROR"
            ),
            "membername": cleaned.get("membername", None),
            "authorityid": (
                cleaned.get("authorityid")
                if cleaned.get("authorityid", None) != ""
                else None
            ),
            "gpoid": (
                cleaned.get("gpoid") if cleaned.get("gpoid", None) != "" else None
            ),
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
        - granule_id    text,
        - package_id    text,
        - congress      text,

        """
        cleaned = record_data.copy()

        bill_type = cleaned.get("type", "").lower()
        bill_number = cleaned.get("number", "")
        bill_congress = cleaned.get("congress", "")

        if all([bill_type, bill_number, bill_congress]):
            bill_id = f"{bill_type}{bill_number}-{bill_congress}"
        else:
            bill_id = None

        filtered_cleaned = {
            "package_id": cleaned.get("package_id", "ID_ERROR"),
            "granule_id": cleaned.get("granule_id", "ID_ERROR"),
            "bill_id": bill_id,
            "bill_type": bill_type,
            "bill_number": bill_number,
            "congress": self.safe_int(bill_congress),
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
                SELECT packageid as package_id, serialset_bagid, serialset_docid, serialset_serialsetnumber, agency, volume, parentid, otheridentifier_oclc, otheridentifier_lccn, otheridentifier_issn, serialset_isglp, lastmodified, documenttype, documentnumber, congress, documentpart FROM {self.staging_schema}.congressionalreports
                WHERE serialset_bagid IS NOT NULL OR serialset_docid IS NOT NULL OR serialset_serialsetnumber IS NOT NULL
                """
                rows = await conn.fetch(fetch_sql)
                logger.info(f"Found {len(rows)} records with serialset data")

                serialset_records = []
                for row in rows:
                    # Coerce isglp to proper boolean
                    isglp_raw = row.get("serialset_isglp")
                    if isinstance(isglp_raw, str):
                        isglp = (
                            True
                            if isglp_raw.lower() == "true"
                            else False
                            if isglp_raw.lower() == "false"
                            else None
                        )
                    else:
                        isglp = bool(isglp_raw) if isglp_raw is not None else None

                    serialset_records.append(
                        (
                            row["package_id"],
                            row.get("serialset_bagid"),
                            row.get("serialset_docid"),
                            row.get("serialset_serialsetnumber"),
                            row.get("agency"),
                            row.get("volume"),
                            row.get("parentid"),
                            row.get("otheridentifier_oclc"),
                            row.get("otheridentifier_lccn"),
                            row.get("otheridentifier_issn"),
                            isglp,
                            self.standardize_date(row.get("lastmodified")),
                        )
                    )
                if serialset_records:
                    insert_sql = f"""
                        INSERT INTO {self.production_schema}.congressionalreports_serialset (
                            package_id,
                            bag_id,
                            doc_id,
                            serialset_number,
                            agency,
                            volume,
                            parent_serialset_id,
                            oclc_number,
                            lccn_number,
                            issn_number,
                            isglp,
                            last_modified
                        ) VALUES (
                            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
                        )
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

                chunk_count = 0
                while True:
                    chunk_count += 1
                    fetch_sql = f"""
                    SELECT package_id, collectioncode, contents
                    FROM {self.staging_schema}.congressionalreports_references
                    ORDER BY package_id
                    LIMIT {limit} OFFSET {offset}
                    """

                    logger.info(
                        f"Fetching references chunk {chunk_count} at offset {offset}"
                    )
                    rows = await conn.fetch(fetch_sql)
                    logger.info(
                        f"Retrieved {len(rows)} rows for references chunk {chunk_count}"
                    )

                    if not rows:
                        logger.info(f"No more rows found at offset {offset}, breaking")
                        break

                    law_params = []
                    statute_params = []
                    code_params = []
                    statute_page_params = []
                    code_section_params = []

                    # Log sample data to understand structure
                    if chunk_count == 1 and rows:
                        sample_row = rows[0]
                        logger.info(
                            f"Sample row structure: package_id={sample_row['package_id']}, collectioncode={sample_row['collectioncode']}"
                        )
                        logger.info(
                            f"Sample contents type: {type(sample_row['contents'])}, value: {sample_row['contents'][:200] if sample_row['contents'] else 'None'}"
                        )

                    for row_idx, row in enumerate(rows):
                        contents = row["contents"]
                        collection_code = row["collectioncode"]

                        logger.debug(
                            f"Processing row {row_idx + 1}/{len(rows)}: collection_code={collection_code}"
                        )

                        if isinstance(contents, str):
                            try:
                                contents = json.loads(contents)
                                logger.debug(
                                    f"Parsed JSON contents: {type(contents)}, length: {len(contents) if isinstance(contents, list) else 'N/A'}"
                                )
                            except json.JSONDecodeError as e:
                                logger.warning(
                                    f"JSON decode error for row {row_idx}: {e}, contents: {contents[:100]}"
                                )
                                continue

                        if not contents:
                            logger.debug(f"Skipping row {row_idx}: empty contents")
                            continue

                        # Log what we're processing
                        if isinstance(contents, list):
                            logger.debug(
                                f"Processing {len(contents)} content items for collection_code={collection_code}"
                            )
                        else:
                            logger.debug(
                                f"Processing single content item for collection_code={collection_code}"
                            )

                        if collection_code == "PLAW":
                            for content_idx, content in enumerate(
                                contents if isinstance(contents, list) else [contents]
                            ):
                                logger.debug(
                                    f"Processing PLAW content {content_idx + 1}: {content}"
                                )
                                try:
                                    # Defensive: ensure number and congress are int or None
                                    number_val = self.safe_int(content.get("number"))
                                    congress_val = self.safe_int(
                                        content.get("congress")
                                    )
                                    # If either is None, skip this record (cannot insert str into int column)
                                    if number_val is None or congress_val is None:
                                        logger.warning(
                                            f"Skipping PLAW content {content_idx} due to non-integer number/congress: number={content.get('number')}, congress={content.get('congress')}, content={content}"
                                        )
                                        continue
                                    law_params.append(
                                        (
                                            row["package_id"],
                                            f"PL{content.get('congress')}-{content.get('number')}",
                                            content.get("label", "")
                                            .split(" ")[0]
                                            .lower()
                                            if content.get("label", "")
                                            else None,
                                            f"{content.get('congress')}-{content.get('number')}",
                                            self.safe_int(content.get("number")),
                                            self.safe_int(content.get("congress")),
                                        )
                                    )
                                except Exception as e:
                                    logger.error(
                                        f"Error processing PLAW content {content_idx}: {e}, content: {content}"
                                    )

                        elif collection_code == "STATUTE":
                            for content_idx, content in enumerate(
                                contents if isinstance(contents, list) else [contents]
                            ):
                                logger.debug(
                                    f"Processing STATUTE content {content_idx + 1}: {content}"
                                )
                                try:
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
                                except Exception as e:
                                    logger.error(
                                        f"Error processing STATUTE content {content_idx}: {e}, content: {content}"
                                    )

                        elif collection_code == "USCODE":
                            for content_idx, content in enumerate(
                                contents if isinstance(contents, list) else [contents]
                            ):
                                logger.debug(
                                    f"Processing USCODE content {content_idx + 1}: {content}"
                                )
                                try:
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
                                except Exception as e:
                                    logger.error(
                                        f"Error processing USCODE content {content_idx}: {e}, content: {content}"
                                    )
                        else:
                            logger.debug(f"Unknown collection_code: {collection_code}")

                    # Log batch sizes before inserting
                    logger.info(
                        f"Chunk {chunk_count} batch sizes: laws={len(law_params)}, statutes={len(statute_params)}, codes={len(code_params)}, statute_pages={len(statute_page_params)}, code_sections={len(code_section_params)}"
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
                        logger.info(f"Inserted {len(law_params)} law records")

                    if statute_params:
                        insert_sql = f"""
                            INSERT INTO {self.production_schema}.congressionalreports_reference_statutes (report_statute_id, package_id, reference_statute)
                            VALUES ($1, $2, $3)
                            ON CONFLICT DO NOTHING
                        """
                        await conn.executemany(insert_sql, statute_params)
                        total_counts["statutes"] += len(statute_params)
                        logger.info(f"Inserted {len(statute_params)} statute records")

                    if code_params:
                        insert_sql = f"""
                            INSERT INTO {self.production_schema}.congressionalreports_reference_codes (report_code_id, package_id, reference_code)
                            VALUES ($1, $2, $3)
                            ON CONFLICT DO NOTHING
                        """
                        await conn.executemany(insert_sql, code_params)
                        total_counts["codes"] += len(code_params)
                        logger.info(f"Inserted {len(code_params)} code records")

                    if statute_page_params:
                        insert_sql = f"""
                            INSERT INTO {self.production_schema}.congressionalreports_reference_statutes_pages (report_statute_id, page)
                            VALUES ($1, $2)
                            ON CONFLICT DO NOTHING
                        """
                        await conn.executemany(insert_sql, statute_page_params)
                        total_counts["statute_pages"] += len(statute_page_params)
                        logger.info(
                            f"Inserted {len(statute_page_params)} statute page records"
                        )

                    if code_section_params:
                        insert_sql = f"""
                            INSERT INTO {self.production_schema}.congressionalreports_reference_codes_sections (report_code_id, code_section)
                            VALUES ($1, $2)
                            ON CONFLICT DO NOTHING
                        """
                        await conn.executemany(insert_sql, code_section_params)
                        total_counts["code_sections"] += len(code_section_params)
                        logger.info(
                            f"Inserted {len(code_section_params)} code section records"
                        )

                    offset += limit
                    logger.info(
                        f"Completed chunk {chunk_count}, moving to offset {offset}"
                    )

                logger.info(f"References processing completed: {total_counts}")
                logger.info(
                    f"Total chunks processed: {chunk_count - 1}"
                )  # -1 because we increment before checking
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
                # Only process records where granule_id != package_id (hierarchical structures)
                # or where granule_id is NULL but we have part numbers (indicating granules)
                prod_rows = await conn.fetch(
                    f"""
                    SELECT cr.package_id, cr.report_id, cr.is_errata, cr.granule_id, cr.part_number
                    FROM {self.production_schema}.congressionalreports cr
                    WHERE (cr.report_id LIKE '%-x-%' OR cr.report_id LIKE '%-xe-%')
                    AND (
                        cr.granule_id IS NULL
                        OR cr.granule_id != cr.package_id
                        OR (cr.granule_id IS NULL AND cr.part_number > 0)
                    )
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

                            # Check if this is a single-row group with existing part number
                            if len(rows_with_order) == 1:
                                (_ord, r) = rows_with_order[0]
                                package_id = r["package_id"]
                                old_report_id = r["report_id"]
                                existing_part_number = r.get("part_number", 0)

                                # If we have a valid existing part number (including 0), use it
                                if (
                                    existing_part_number is not None
                                    and existing_part_number >= 0
                                ):
                                    m = re.match(
                                        r"^([a-z]+)(\d+)-([^-]+)-(\d+)$",
                                        str(old_report_id),
                                        re.IGNORECASE,
                                    )
                                    if m:
                                        rtype, rnum, part_comp, cong = (
                                            m.group(1).lower(),
                                            m.group(2),
                                            m.group(3),
                                            m.group(4),
                                        )

                                        is_errata_flag = (
                                            bool(r.get("is_errata"))
                                            if r.get("is_errata") is not None
                                            else False
                                        )
                                        has_errata_suffix = (
                                            part_comp.endswith("e") or is_errata_flag
                                        )
                                        new_part_token = f"{existing_part_number}{'e' if has_errata_suffix else ''}"
                                        new_report_id = (
                                            f"{rtype}{rnum}-{new_part_token}-{cong}"
                                        )

                                        if new_report_id != old_report_id:
                                            main_table_updates.append(
                                                (
                                                    new_report_id,
                                                    existing_part_number,
                                                    package_id,
                                                )
                                            )
                                            committees_updates.append(
                                                (new_report_id, package_id)
                                            )
                                            members_updates.append(
                                                (new_report_id, package_id)
                                            )
                                            serialset_updates.append(
                                                (new_report_id, package_id)
                                            )
                                    continue

                            # Assign parts sequentially starting at 1 for multi-row groups or single rows with no valid part number
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

                                # Use the new part number (idx) for the part_number field
                                new_part_number = idx

                                # Collect updates for batch processing
                                main_table_updates.append(
                                    (new_report_id, new_part_number, package_id)
                                )
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
                                    SET report_id = $1, part_number = $2
                                    WHERE package_id = $3
                                    """,
                                    batch,
                                )
                                updated_count += len(batch)

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
        # ? Operation 5: serialset topics
        logger.info("Starting serialset topics post-processing")
        try:
            async with self.db_pool.acquire() as conn, conn.transaction():
                # Fetch all production rows with placeholder 'x' (including errata 'xe')
                prod_rows = await conn.fetch(
                    f"""
                    SELECT package_id, subjects_topics FROM {self.staging_schema}.congressionalreports
                    """
                )
                insert_rows = []
                for row in prod_rows:
                    package_id = row["package_id"]
                    topics = row["subjects_topics"]
                    if topics and topics.strip():
                        try:
                            topic_list = json.loads(topics)
                        except json.JSONDecodeError:
                            topic_list = topics.split(",")
                        if isinstance(topic_list, list):
                            for topic in topic_list:
                                insert_rows.append((package_id, topic))
                        else:
                            logger.warning(
                                f"subjects_topics is not a list for package_id={package_id}: {topics!r}"
                            )
                if insert_rows:
                    await conn.executemany(
                        f"""
                        INSERT INTO {self.production_schema}.congressionalreports_serialset_topics (package_id, topic)
                        VALUES ($1, $2)
                        ON CONFLICT (package_id, topic) DO NOTHING
                        """,
                        insert_rows,
                    )

        except Exception as e:
            logger.error(f"Error inserting serialset topics: {e}", exc_info=True)
            results["operations"].append(
                {
                    "name": "insert_serialset_topics",
                    "status": "error",
                    "error": str(e),
                }
            )
            results["status"] = "partial_failure"
        logger.info("Completed serialset topics post-processing")

        # ? Operation 6: serialset committees
        logger.info("Starting serialset committees post-processing")
        try:
            async with self.db_pool.acquire() as conn, conn.transaction():
                # Fetch all production rows with placeholder 'x' (including errata 'xe')
                prod_rows = await conn.fetch(
                    f"""
                    SELECT package_id, committees FROM {self.staging_schema}.congressionalreports
                    """
                )
                insert_rows = []
                for row in prod_rows:
                    package_id = row["package_id"]
                    committees = row["committees"]
                    if committees:
                        committee_list = json.loads(committees)
                        for committee in committee_list:
                            insert_rows.append(
                                (
                                    package_id,
                                    None,
                                    committee["authorityId"],
                                    committee["committeeName"],
                                )
                            )
                if insert_rows:
                    await conn.executemany(
                        f"""
                        INSERT INTO {self.production_schema}.congressionalreports_committees (package_id, granule_id, committee_code, committee_name)
                        VALUES ($1, $2, $3, $4)
                        ON CONFLICT (package_id, granule_id, committee_code, committee_name) DO NOTHING
                        """,
                        insert_rows,
                    )

        except Exception as e:
            logger.error(f"Error inserting serialset committees: {e}", exc_info=True)
            results["operations"].append(
                {
                    "name": "insert_serialset_committees",
                    "status": "error",
                    "error": str(e),
                }
            )
            results["status"] = "partial_failure"
        logger.info("Completed serialset committees post-processing")


        # ? Operation 7: errata backfill for congressional reports
        logger.info(
            "Starting errata flag backfill for congressional reports from title/granule tokens"
        )
        try:
            async with self.db_pool.acquire() as conn, conn.transaction():
                update_sql = f"""
                    WITH marks AS (
                        SELECT cr.report_id
                        FROM {self.production_schema}.congressionalreports cr
                        WHERE COALESCE(cr.is_errata, FALSE) = FALSE
                            AND (
                                cr.title ILIKE '%errata%'
                            AND COALESCE(cr.title, '') NOT ILIKE '%addendum%'
                            )
                    )
                    UPDATE {self.production_schema}.congressionalreports cr
                    SET is_errata = TRUE,
                        report_id = (
                            split_part(cr.report_id, '-', 1) || '-' ||
                            CASE
                                WHEN split_part(cr.report_id, '-', 2) LIKE '%e' THEN split_part(cr.report_id, '-', 2)
                                ELSE split_part(cr.report_id, '-', 2) || 'e'
                            END || '-' ||
                            split_part(cr.report_id, '-', 3)
                        )
                    FROM marks m
                    WHERE cr.report_id = m.report_id
                """
                result = await conn.execute(update_sql)
                affected = 0
                try:
                    parts = result.split()
                    if len(parts) == 2:
                        affected = int(parts[1])
                except Exception:
                    affected = 0
                logger.info(f"Congressional reports errata backfill updated {affected} rows")
                results["operations"].append(
                    {
                        "name": "mark_errata_from_tokens",
                        "status": "success",
                        "rows_affected": affected,
                    }
                )
                results["rows_affected"] += affected
        except Exception as e:
            logger.error(f"Error backfilling congressional reports errata: {e}", exc_info=True)
            results["operations"].append(
                {
                    "name": "mark_congressionalreports_errata_from_tokens",
                    "status": "error",
                    "error": str(e),
                }
            )
            results["status"] = "partial_failure"



        return results
