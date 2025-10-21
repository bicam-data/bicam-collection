"""
Hearing Packages Custom Plugin Logic

This module contains all the custom logic for hearing packages data type including:
- Fetching logic (from fetcher.py)
- Cleaning logic (from cleaner.py)

"""

import hashlib
import html
import json
import logging
import re
from collections.abc import AsyncGenerator
from datetime import datetime
from typing import Any

from streamlined.libs import (
    COMMITTEE_FIXES,
    HEARING_BIOGUIDE_FIXES,
    HEARING_PACKAGE_JACKETNUMBERS,
)
from streamlined.plugins.base import BaseCleanerLogic

logger = logging.getLogger(__name__)


class HearingpackagesFetcherLogic:
    """
    Hearing Packages fetcherogic that extends the base GovInfo fetcher logic.

    This class provides custom methods for fetching hearing packages data, including:
    - Extracting standardized hearing packages IDs
    - Handling related data from URLs
    - Implementing specialized fetching logic for hearing packages-specific data

    Inherits shared functionality like get_generic_related_data from
    GovInfoBaseFetcher.
    """

    def __init__(self, data_type: str = "hearingpackages"):
        self.data_type = data_type


class HearingpackagesCleanerLogic(BaseCleanerLogic):
    """
    Hearing Packages cleaner logic extracted from HearingPackagesCleaner class.
    Contains all the custom cleaning methods for hearing packages data.
    """

    # TODO: for now, skip GOVPUB, but then get the jacket numbers from page 1 of the PDFs OR by hand (for example, "GOVPUB-Y4_AP6_1-cdb7e3ca60f825d996dd64fa187d6c2a" is "94361")

    # TODO: fix partnumber/heading/volume/book handling, like "packageid LIKE '%p%v%' OR packageid LIKE '%p%b%' OR packageid LIKE '%v%b%' OR packageid LIKE '%v%p%' OR packageid LIKE '%b%v' OR packageid LIKE '%v%p%' OR packageid LIKE '%b%p%' OR heading is not null"
    # TODO: agencies are valid, we should clean them but this entity disambiguation would be tough (granules)
    # TODO: clean committee codes (only one really) "position ('o' in lower(authorityid)) > 4" (both granules and packages)
    # TODO: fill in 539 hearings members without bioguide ids (granules)
    ####
    # SELECT DISTINCT ON (lower(gmn.parsed)) * FROM bicam_staging_govinfo.hearingpackages_granules_members AS gm
    # JOIN bicam_staging_govinfo.hearingpackages_granules_members_name AS gmn ON gm.id = gmn.members_id
    # WHERE gm.bioguideid IS NULL;
    ####

    # TODO: create ils_system_id table from hearingpackages
    # TODO: reference_bills (granules)
    # TODO: other references (right now, only one code from package)
    # TODO: helddates (package)
    # TODO: witnesses (granules)
    def __init__(
        self,
        data_type_name: str = "hearingpackages",
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
            "hearingpackages_committees": [
                "hearingpackages_committees",
                "hearingpackages_granules_committees",
            ],
            "hearingpackages_members": [
                "hearingpackages_granules_members",
                "hearingpackages_granules_members_name",
            ],
            "hearingpackages": [
                "hearingpackages",
                "hearingpackages_granules",
            ],
            "hearingpackages_reference_bills": [
                "hearingpackages_granules_references",
                "hearingpackages_granules_references_contents",
            ],
            "hearingpackages_witnesses": [
                "hearingpackages_granules",
                "hearingpackages_granules_witnesses",
            ],
            "hearingpackages_agencies": [
                "hearingpackages_granules",
                "hearingpackages_granules_agencies",
            ],
            "hearingpackages_dates": [
                "hearingpackages_helddates",
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

    def _build_special_hearing_identity(
        self, cleaned: dict[str, Any], packageid: str
    ) -> tuple[str, str | None, str | None, int, str, str, str]:
        """
        Build hearing identity for special GOVPUB packages.

        For GOVPUB packages like "GOVPUB-Y1_2-PURL-gpo71122" or "GOVPUB-Y4_SE2-PURL-LPS4204":
        - Calculate congress from dateissued using [(year - 1789) // 2] + 1
        - Map governmentauthor2 to hearing type (hhrg/shrg/jhrg/xhrg)
        - Extract hearing number from gpo or LPS patterns
        - Use manual overrides for packages without gpo/LPS numbers

        Returns tuple:
        - hearing_id (hearing_id for hearings)
        - parent_hearing_id (nullable)
        - granule_id (nullable)
        - part_number (int) - always 0 for GOVPUB
        - hearing_type (str) - hhrg/shrg/jhrg/xhrg
        - hearing_number (str) - hearing number
        - congress (str)
        """

        # Calculate congress from dateissued
        dateissued = cleaned.get("dateissued")
        if not dateissued:
            raise ValueError(
                f"dateissued field is required for GOVPUB package {packageid}"
            )

        try:
            parsed_date = None
            year = None
            # Parse the date - handle various formats
            if isinstance(dateissued, str):
                # Try common date formats
                for fmt in ["%Y-%m-%d", "%Y", "%Y-%m", "%m/%d/%Y", "%d/%m/%Y"]:
                    try:
                        parsed_date = datetime.strptime(dateissued, fmt)
                        year = parsed_date.year
                        break
                    except Exception:
                        continue
                if parsed_date is None:
                    # If no format matches, try to extract year from string
                    year_match = re.search(r"\b(19|20)\d{2}\b", dateissued)
                    if year_match:
                        year = int(year_match.group())
                        parsed_date = datetime(year, 1, 1)
                    else:
                        raise ValueError(f"Could not parse date from {dateissued}")
            else:
                # Assume it's already a datetime or date object
                parsed_date = dateissued
                year = getattr(parsed_date, "year", None)
                if year is None:
                    raise ValueError(f"Could not extract year from {dateissued}")

            congress = str((year - 1789) // 2 + 1)
        except Exception as e:
            raise ValueError(
                f"Could not calculate congress from dateissued '{dateissued}' for package {packageid}: {e}"
            ) from e

        # Map governmentauthor2 to hearing type
        governmentauthor2 = cleaned.get("governmentauthor2", "").lower()
        if "house of representatives" in governmentauthor2:
            hearing_type = "hhrg"
        elif "senate" in governmentauthor2:
            hearing_type = "shrg"
        elif "joint" in governmentauthor2:
            hearing_type = "jhrg"
        else:
            hearing_type = "xhrg"  # placeholder

        # Extract hearing number from package_id
        hearing_number = None

        # Check for gpo pattern (e.g., "gpo71122")
        gpo_match = re.search(r"gpo(\d+)", packageid, re.IGNORECASE)
        if gpo_match:
            hearing_number = gpo_match.group(1)

        # Check for LPS pattern (e.g., "LPS4204")
        lps_match = re.search(r"lps(\d+)", packageid, re.IGNORECASE)
        if lps_match:
            hearing_number = lps_match.group(1)

        # Check for FDLP pattern (e.g., "GOVPUB-Y4_F49-PURL-FDLP1598")
        fdlp_match = re.search(r"fdlp(\d+)", packageid, re.IGNORECASE)
        if fdlp_match:
            hearing_number = fdlp_match.group(1)

        # Check for manual overrides first
        manual_override = HEARING_PACKAGE_JACKETNUMBERS.get(packageid)
        if manual_override:
            # If manual override is a dict, it contains the complete hearing ID
            if isinstance(manual_override, dict):
                base_hearing_id = manual_override.get("hearing_id")
                hearing_type = manual_override.get("hearing_type")
                hearing_number = manual_override.get("hearing_number")
                congress = manual_override.get("congress")

                hearing_set_id = f"{hearing_type}{hearing_number}-{congress}"

                granuleid = cleaned.get("granuleid") or cleaned.get("granule_id")

                # For GPO-CHRG-ROBERTS, use placeholders initially - they'll be resolved in post-processing
                if granuleid and granuleid != packageid:
                    # Use placeholder 'x' for granules - will be resolved in post-processing
                    hearing_id = f"{hearing_type}{hearing_number}-x-{congress}"
                    part_number = 0  # Placeholder value
                else:
                    # For package rows, use the base hearing ID
                    hearing_id = base_hearing_id
                    part_number = 0

                return (
                    hearing_id,
                    hearing_set_id,
                    str(granuleid) if granuleid else None,
                    part_number,
                    hearing_type,
                    hearing_number,
                    congress,
                )
            else:
                # If manual override is a string, it's just the hearing number
                hearing_number = manual_override
        else:
            # If no gpo/LPS pattern found and no manual override, raise error
            if not hearing_number:
                raise ValueError(
                    f"No hearing number found for special package {packageid}. "
                    f"Expected gpo or LPS pattern, or manual override."
                )

        # GOVPUB packages always have part number 1 and are never errata
        part_number = 0
        part_token = "0"

        hearing_id = f"{hearing_type}{hearing_number}-{part_token}-{congress}"
        hearing_set_id = f"{hearing_type}{hearing_number}-{congress}"

        granuleid = cleaned.get("granuleid") or cleaned.get("granule_id")

        return (
            hearing_id,
            hearing_set_id,
            str(granuleid) if granuleid else None,
            part_number,
            hearing_type,
            hearing_number,
            congress,
        )

    def _build_hearing_identity(
        self, cleaned: dict[str, Any]
    ) -> tuple[str, str | None, str | None, int, str, str, str]:
        """
        Build canonical identity. If the record is a GovInfo hearing (CHRG-*), build a hearing identity
        that captures part/volume/book with errata and placeholder handling. Otherwise, fall back to
        congressional report identity semantics (legacy).

        Returns tuple:
        - hearing_id (hearing_id for hearings)
        - hearing_set_id (nullable)
        - granule_id (nullable)
        - part_number (int)
        - hearing_type (str)  # hhrg/shrg/jhrg for hearings
        - hearing_number (str)  # hearing number for hearings
        - congress (str)
        """
        packageid = str(cleaned.get("packageid") or cleaned.get("package_id") or "")
        granuleid = cleaned.get("granuleid") or cleaned.get("granule_id")

        # Special handling for GOVPUB packages
        if packageid.upper().startswith("GOVPUB"):
            return self._build_special_hearing_identity(cleaned, packageid)

        # Special handling for GPO- prefixed packages
        if packageid.upper().startswith("GPO-"):
            if any(char.isdigit() for char in packageid.split("-")[2]):
                packageid = packageid[4:]
            else:
                return self._build_special_hearing_identity(cleaned, packageid)

        # Hearing path: CHRG-<congress><chamber>hrg<hearing_number>...
        m = re.match(r"^CHRG-(\d{2,3})([hsj])hrg(\d+)", packageid, re.IGNORECASE)
        if m:
            congress = m.group(1)
            chamber_char = m.group(2).lower()
            hearing_number_digits = m.group(3)  # digits-only jacket number
            hearing_type = f"{chamber_char}hrg"  # hhrg/shrg/jhrg
            hearing_number = hearing_number_digits

            # Part determination for both package and granule rows
            part_number_val = self.parse_part_from_fields(cleaned) or 0
            part_token_base: str = (
                "x"
                if self._should_use_placeholder_part(cleaned, packageid)
                else str(part_number_val)
            )
            is_errata = self._detect_is_errata(cleaned)
            part_token = f"{part_token_base}{'e' if is_errata else ''}"

            hearing_id = f"{hearing_type}{hearing_number}-{part_token}-{congress}"
            # Parent hearing ID should always be part 1 (not the same as granule)
            hearing_set_id = f"{hearing_type}{hearing_number}-{congress}"

        return (
            hearing_id,
            hearing_set_id,
            str(granuleid) if granuleid else None,
            int(part_number_val),
            hearing_type,
            hearing_number,
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

            # If explicit indicators include mixed alphanumeric tokens (e.g., '6A', '8-C'),
            # we force placeholder handling to allow ordered post-processing.
            if isinstance(partnumber, str) and self._has_mixed_alnum(partnumber):
                return True
            if isinstance(heading, str):
                # 1) Heading equals a compact mixed token like '6A'/'8-C'
                if re.fullmatch(r"\s*\d+[\-_()]*[A-Za-z]\s*", heading):
                    return True
                # 2) 'Part <token>' pattern with mixed token
                m_head = re.search(
                    r"\bpart\s+([A-Za-z0-9()\-]+)", heading, re.IGNORECASE
                )
                if m_head and self._has_mixed_alnum(m_head.group(1)):
                    return True

            # If we have explicit part indicators, don't use placeholder
            if heading or partnumber or volumenumber:
                return False

            # Check if parse_part_from_fields would find a valid part number (>1)
            parsed_part = self.parse_part_from_fields(cleaned)
            if parsed_part > 1:
                return False

            # For hearing packages, check if package ID suggests parts exist
            if "CHRG-" in pkg:
                # Look for patterns that suggest parts: -pt*, -add*, -p*, etc.
                if re.search(
                    r"[-_](?:pt|add|p|vol|book)[A-Za-z0-9]+", pkg, re.IGNORECASE
                ):
                    # If '-pt' has a mixed token, prefer placeholder; otherwise allow generic placeholder
                    m_pt = re.search(r"[-_]pt([A-Za-z0-9()\-]+)", pkg, re.IGNORECASE)
                    if m_pt and self._has_mixed_alnum(m_pt.group(1)):
                        return True
                    return True
                # Check if package ID ends with numeric tokens that aren't congress
                if re.search(r"\d+(?:-\d+)?$", pkg):
                    return True

            return bool(re.search(r"\d+(?:-\d+)?$", pkg))
        except Exception:
            return False

    def roman_to_int(self, roman: str) -> int | None:
        # Robust Roman numeral parser (supports large values like CC, M, etc.)
        if not roman:
            return None
        if roman == "D":
            return 4
        s = roman.upper().strip()
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

    def alpha_to_int(self, token: str) -> int | None:
        """Convert alphabetic token (e.g., 'a', 'B') to 1-based index. Multi-letters like 'cc' map via Roman if possible.

        Returns None if not alphabetic.
        """
        if not token:
            return None
        t = token.strip()
        if not t.isalpha():
            return None
        # Prefer Roman interpretation for multi-letter uppercases like 'CC', 'IV', etc.
        roman_val = self.roman_to_int(t)
        if roman_val is not None:
            return roman_val
        # Fallback: single-letter A=1..Z=26
        if len(t) == 1:
            return ord(t.lower()) - ord("a") + 1
        return None

    def _has_mixed_alnum(self, token: str | None) -> bool:
        """Return True if token contains both a digit and an alphabetic character.

        Examples triggering True: '6A', '8-C', '2(a)'.
        """
        if not token:
            return False
        s = re.sub(r"[^A-Za-z0-9]", "", str(token))
        if not s:
            return False
        return bool(re.search(r"[A-Za-z]", s) and re.search(r"\d", s))

    def _parse_alphanumeric_part_token(self, raw: str) -> tuple[int | None, int | None]:
        """Parse tokens like '4B', '1A', '3(a)', '3-A', '13E', 'A', 'II'.

        Returns (main_part, subpart_index) where subpart_index is the numeric mapping of letter when present.
        """
        if not raw:
            return (None, None)
        tok = str(raw).strip()
        # Normalize token: remove spaces and parentheses
        tok = re.sub(r"[\s()]+", "", tok)
        # Common separators: 3-A -> 3A, 3_a -> 3a
        tok = tok.replace("-", "").replace("_", "")

        # Range like 'I-II' -> take first component
        if "-" in tok:
            left = tok.split("-")[0]
            return self._parse_alphanumeric_part_token(left)

        # Pure digits
        if tok.isdigit():
            return (int(tok), None)

        # Pure letters -> either Roman or alphabetic
        if tok.isalpha():
            roman_val = self.roman_to_int(tok)
            if roman_val is not None:
                return (roman_val, None)
            alpha_val = self.alpha_to_int(tok)
            return (alpha_val, None)

        # Mixed number + single trailing letter, e.g., 4B, 1A
        m = re.match(r"^(\d+)([a-zA-Z])$", tok)
        if m:
            base = int(m.group(1))
            sub = self.alpha_to_int(m.group(2)) or 1
            # Map 1A->1, 1B->2, 4A->4, 4B->5 (letter increments base by (sub-1))
            return (base + (sub - 1), None)

        # Mixed Roman + anything else: try Roman only
        roman_digits = re.match(r"^([ivxlcdmIVXLCDM]+)$", tok)
        if roman_digits:
            rv = self.roman_to_int(roman_digits.group(1))
            return (rv, None)

        # Fallback: extract first number found
        m = re.search(r"(\d+)", tok)
        if m:
            return (int(m.group(1)), None)
        return (None, None)

    def parse_part_from_fields(self, cleaned: dict[str, Any]) -> int:
        """Parse hearing part number with alpha/Roman handling.

        Priority:
        1) explicit partnumber/documentpart
        2) heading like 'Part X' (X may be roman, digit, or letter; supports '3-A', '2(a)')
        3) package/granule id suffixes like '...p3', '...-ptb' (b->2), '...-ptIV'
        4) default 1
        """
        # 1) Explicit field - handle alphabetic parts like 'a', 'b', 'III'
        part_field = cleaned.get("partnumber") or cleaned.get("documentpart")
        if part_field:
            part_str = str(part_field).strip()
            # If token mixes digits and letters (e.g., '6A', '8-C'), treat as ambiguous
            # and defer to placeholder resolution logic -> return 1 for now
            if self._has_mixed_alnum(part_str):
                return 1
            # Try direct alphabetic conversion first
            alpha_val = self.alpha_to_int(part_str)
            if alpha_val is not None:
                if alpha_val > 100:
                    logger.warning(
                        f"\nPart number {part_str} (alpha={alpha_val}) > 100 for granule_id {cleaned.get('granuleid')}, replacing with 1\n"
                    )
                    return 0
                return alpha_val
            # Try Roman numeral
            roman_val = self.roman_to_int(part_str)
            if roman_val is not None:
                if roman_val > 100:
                    if part_str.lower() == "c":
                        return 3
                    else:
                        logger.warning(
                            f"\nPart number {part_str} (roman={roman_val}) > 100 for granule_id {cleaned.get('granuleid')}, replacing with 1\n"
                        )
                        return 0
                return roman_val
            # Try alphanumeric parsing
            main, _sub = self._parse_alphanumeric_part_token(part_str)
            if main is not None:
                if main > 100:
                    logger.warning(
                        f"Part number {part_str} (parsed={main}) > 100 for granule_id {cleaned.get('granuleid')}, replacing with 1"
                    )
                    return 0
                return main
            # Try direct integer conversion
            if part_str.isdigit():
                int_val = int(part_str)
                if int_val > 100:
                    logger.warning(
                        f"Part number {part_str} (int={int_val}) > 100 for granule_id {cleaned.get('granuleid')}, replacing with 1"
                    )
                    return 0
                return int_val

        # 2) From heading
        heading = cleaned.get("heading")
        if isinstance(heading, str) and heading.strip():
            # Match 'Part <token>' where token can be roman/digit/letter or combos like 3-A, 2(a)
            m = re.search(r"\bpart\s+([A-Za-z0-9()\-]+)", heading, re.IGNORECASE)
            if m:
                token = m.group(1).strip()
                # Mixed alphanumeric tokens -> defer to placeholder resolution
                if self._has_mixed_alnum(token):
                    return 0
                # Try alphabetic first
                alpha_val = self.alpha_to_int(token)
                if alpha_val is not None:
                    return alpha_val
                # Try Roman
                roman_val = self.roman_to_int(token)
                if roman_val is not None:
                    return roman_val
                # Try alphanumeric parsing
                main, _sub = self._parse_alphanumeric_part_token(token)
                if main is not None:
                    return main

        # 3) Inspect ids and common suffixes
        pkg = str(cleaned.get("packageid") or "")
        gran = str(cleaned.get("granuleid") or "")
        candidate = gran or pkg

        # -pt<tok>
        m = re.search(r"[-_]pt([A-Za-z0-9]+)$", candidate, re.IGNORECASE)
        if m:
            token = m.group(1).strip()
            # Mixed alphanumeric tokens -> defer to placeholder resolution
            if self._has_mixed_alnum(token):
                return 0
            # Try alphabetic first
            alpha_val = self.alpha_to_int(token)
            if alpha_val is not None:
                return alpha_val
            # Try Roman
            roman_val = self.roman_to_int(token)
            if roman_val is not None:
                return roman_val
            # Try alphanumeric parsing
            main, _sub = self._parse_alphanumeric_part_token(token)
            if main is not None:
                return main

        # p<digits> possibly followed by volume/book tokens (no dash)
        # But not hrg, shrg, jhrg patterns
        m = re.search(r"p(\d+)(?:v\d+)?(?:b\d+)?$", candidate, re.IGNORECASE)
        if m and not re.search(r"[hsj]hrg\d+", candidate, re.IGNORECASE):
            return int(m.group(1))

        # Roman part after '-pt' or in heading-like id
        m = re.search(r"[-_]pt([ivxlcdmIVXLCDM]+)$", candidate)
        if m:
            rv = self.roman_to_int(m.group(1))
            if rv is not None:
                return rv

        # Fallback
        return 0

    def parse_volume_from_fields(self, cleaned: dict[str, Any]) -> int | None:
        """Parse volume number from 'volumenumber', heading, or id suffixes."""
        vol_field = cleaned.get("volumenumber")
        if vol_field:
            tok = str(vol_field).strip()
            if tok.isdigit():
                return int(tok)
            rv = self.roman_to_int(tok)
            if rv is not None:
                return rv

        heading = cleaned.get("heading")
        if isinstance(heading, str) and heading.strip():
            m = re.search(r"\bvol(?:ume)?\s+([A-Za-z0-9]+)", heading, re.IGNORECASE)
            if m:
                tok = m.group(1)
                if tok.isdigit():
                    return int(tok)
                rv = self.roman_to_int(tok)
                if rv is not None:
                    return rv

        pkg = str(cleaned.get("packageid") or "")
        gran = str(cleaned.get("granuleid") or "")
        candidate = gran or pkg

        # '-vol<tok>' suffix
        m = re.search(r"[-_]vol(?:ume)?([A-Za-z0-9]+)$", candidate, re.IGNORECASE)
        if m:
            tok = m.group(1)
            if tok.isdigit():
                return int(tok)
            rv = self.roman_to_int(tok)
            if rv is not None:
                return rv

        # 'v<digits>' suffix (compact style like ...p1v2)
        m = re.search(r"v(\d+)$", candidate, re.IGNORECASE)
        if m:
            return int(m.group(1))
        return None

    def parse_book_from_fields(self, cleaned: dict[str, Any]) -> int | None:
        """Parse book number from 'booknumber', heading, or id suffixes."""
        book_field = cleaned.get("booknumber")
        if book_field:
            tok = str(book_field).strip()
            if tok.isdigit():
                return int(tok)
            rv = self.roman_to_int(tok)
            if rv is not None:
                return rv

        heading = cleaned.get("heading")
        if isinstance(heading, str) and heading.strip():
            m = re.search(r"\bbook\s+([A-Za-z0-9]+)", heading, re.IGNORECASE)
            if m:
                tok = m.group(1)
                if tok.isdigit():
                    return int(tok)
                rv = self.roman_to_int(tok)
                if rv is not None:
                    return rv

        pkg = str(cleaned.get("packageid") or "")
        gran = str(cleaned.get("granuleid") or "")
        candidate = gran or pkg
        m = re.search(r"b(\d+)$", candidate, re.IGNORECASE)
        if m:
            return int(m.group(1))
        return None

    async def _stream_hearingpackages_committees_joined_chunks(
        self, chunk_size: int
    ) -> AsyncGenerator[list[dict[str, Any]], None]:
        """
        Stream joined data from hearingpackages_committees and hearingpackages_granules_committees.
        """

        if not self.db_pool:
            raise ValueError("Database pool not configured")

        async with self.db_pool.acquire() as conn:
            try:
                logger.info(
                    f"Starting hearingpackages_committees streaming with chunk_size={chunk_size}"
                )

                # First, check if both tables exist
                tables_exist_query = """
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_schema = $1
                AND table_name IN ('hearingpackages_committees', 'hearingpackages_granules_committees')
                """
                tables_count = await conn.fetchval(
                    tables_exist_query, self.staging_schema
                )

                if tables_count < 2:
                    logger.warning(
                        "One or both hearingpackages_committees staging tables do not exist"
                    )
                    return

                # Get total count for logging
                package_count_query = f"""
                SELECT DISTINCT ON (c.package_id, c.authorityid) COUNT(*)
                FROM {self.staging_schema}.hearingpackages_committees AS c
                GROUP BY c.package_id, c.authorityid
                """
                package_count = await conn.fetchval(package_count_query)

                granule_count_query = f"""
                SELECT DISTINCT ON (gc.granule_id, gc.authorityid) COUNT(*)
                FROM {self.staging_schema}.hearingpackages_granules_committees gc
                GROUP BY gc.granule_id, gc.authorityid
                """
                granule_count = await conn.fetchval(granule_count_query)

                total_count = package_count + granule_count

                if total_count == 0:
                    logger.info("No hearingpackages_committees records found")
                    return

                logger.info(
                    f"Streaming {total_count} hearingpackages_committees records in chunks of {chunk_size}"
                )

                # Stream using pagination with aggregated formats
                offset = 0
                while True:
                    logger.debug(
                        f"Fetching hearingpackages_committees chunk at offset {offset}"
                    )

                    # Use JSON aggregation to collect all formats for each text
                    query = f"""
                    SELECT
                        hpc.package_id,
                        NULL as granule_id,
                        hpc.authorityid,
                        hpc.committeename
                    FROM {self.staging_schema}.hearingpackages_committees AS hpc
                    GROUP BY hpc.package_id, hpc.authorityid, hpc.committeename

                    UNION ALL

                    SELECT
                        hpg.packageid as package_id,
                        hpg.granuleid AS granule_id,
                        hpgc.authorityid,
                        hpgc.committeename
                    FROM {self.staging_schema}.hearingpackages_granules_committees AS hpgc
                    JOIN {self.staging_schema}.hearingpackages_granules AS hpg ON hpgc.granule_id = hpg.id
                    GROUP BY hpg.packageid, hpg.granuleid, hpgc.authorityid, hpgc.committeename
                    ORDER BY granule_id, authorityid, committeename
                    LIMIT {chunk_size} OFFSET {offset}
                    """

                    logger.debug(
                        f"Executing query for hearingpackages_committees at offset {offset}"
                    )
                    rows = await conn.fetch(query)
                    logger.debug(
                        f"Retrieved {len(rows)} rows for hearingpackages_committees at offset {offset}"
                    )

                    if not rows:
                        logger.info(
                            f"No more hearingpackages_committees records at offset {offset}"
                        )
                        break

                    chunk = []
                    for row in rows:
                        try:
                            row_dict = dict(row)
                            chunk.append(row_dict)
                        except Exception as e:
                            logger.error(
                                f"Error converting hearingpackages_committees row to dict: {e}, row type: {type(row)}, row: {row}"
                            )
                            continue

                    if chunk:  # Only yield if we have valid records
                        logger.debug(
                            f"Yielding {len(chunk)} hearingpackages_committees records"
                        )
                        yield chunk

                    offset += chunk_size

                    # Log progress periodically
                    if offset % (chunk_size * 10) == 0:
                        logger.debug(
                            f"Streamed {offset} hearingpackages_committees records"
                        )

            except Exception as e:
                logger.error(f"Error streaming hearingpackages_committees: {e}")
                raise

    async def _stream_hearingpackages_members_joined_chunks(
        self, chunk_size: int
    ) -> AsyncGenerator[list[dict[str, Any]], None]:
        """
        Stream joined data from hearingpackages_members and hearingpackages_granules_members.
        """

        if not self.db_pool:
            raise ValueError("Database pool not configured")

        async with self.db_pool.acquire() as conn:
            try:
                logger.info(
                    f"Starting hearingpackages_members streaming with chunk_size={chunk_size}"
                )

                # First, check if both tables exist
                tables_exist_query = """
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_schema = $1
                AND table_name IN ('hearingpackages_granules_members', 'hearingpackages_granules_members_name')
                """
                tables_count = await conn.fetchval(
                    tables_exist_query, self.staging_schema
                )

                if tables_count < 2:
                    logger.warning(
                        "One or both hearingpackages_members staging tables do not exist"
                    )
                    return

                # Get total count for logging
                package_count_query = f"""
                SELECT COUNT(DISTINCT c.id)
                FROM {self.staging_schema}.hearingpackages_granules_members c
                """
                package_count = await conn.fetchval(package_count_query)

                granule_count_query = f"""
                SELECT COUNT(DISTINCT gc.id)
                FROM {self.staging_schema}.hearingpackages_granules_members_name gc
                """
                granule_count = await conn.fetchval(granule_count_query)

                total_count = package_count + granule_count

                if total_count == 0:
                    logger.info("No hearingpackages_committees records found")
                    return

                logger.info(
                    f"Streaming {total_count} hearingpackages_committees records in chunks of {chunk_size}"
                )

                # Stream using pagination with aggregated formats
                offset = 0
                while True:
                    logger.debug(
                        f"Fetching hearingpackages_members chunk at offset {offset}"
                    )

                    # Use JSON aggregation to collect all formats for each text
                    query = f"""
                    SELECT DISTINCT ON (hpgm.id)
                        hpg.packageid AS package_id,
                        hpg.granuleid AS granule_id,
                        hpgm.bioguideid,
                        hpgm.membername,
                        hpgm.authorityid,
                        hpgm.gpoid,
                        hpgmn.parsed,
                        hpgmn.authority_fnf,
                        hpgmn.authority_other
                    FROM {self.staging_schema}.hearingpackages_granules_members AS hpgm
                    JOIN {self.staging_schema}.hearingpackages_granules_members_name AS hpgmn ON hpgm.id = hpgmn.members_id
                    JOIN {self.staging_schema}.hearingpackages_granules AS hpg ON hpgm.granule_id = hpg.id
                    ORDER BY hpgm.id
                    LIMIT {chunk_size} OFFSET {offset}
                    """

                    logger.debug(
                        f"Executing query for hearingpackages_members at offset {offset}"
                    )
                    rows = await conn.fetch(query)
                    logger.debug(
                        f"Retrieved {len(rows)} rows for hearingpackages_members at offset {offset}"
                    )

                    if not rows:
                        logger.info(
                            f"No more hearingpackages_members records at offset {offset}"
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
                                f"Error converting hearingpackages_members row to dict: {e}, row type: {type(row)}, row: {row}",
                                exc_info=True,
                            )
                            continue

                    if chunk:  # Only yield if we have valid records
                        logger.debug(
                            f"Yielding {len(chunk)} hearingpackages_members records"
                        )
                        yield chunk

                    offset += chunk_size

                    # Log progress periodically
                    if offset % (chunk_size * 10) == 0:
                        logger.debug(
                            f"Streamed {offset} hearingpackages_members records"
                        )

            except Exception as e:
                logger.error(f"Error streaming hearingpackages_members: {e}")
                raise

    async def _stream_hearingpackages_joined_chunks(
        self, chunk_size: int
    ) -> AsyncGenerator[list[dict[str, Any]], None]:
        """
        Stream joined data from hearingpackages and hearingpackages_granules.
        """
        # TODO: between hearingpackages and hearingpackages_granules

        if not self.db_pool:
            raise ValueError("Database pool not configured")

        async with self.db_pool.acquire() as conn:
            try:
                logger.info(
                    f"Starting hearingpackages streaming with chunk_size={chunk_size}"
                )

                # First, check if both tables exist
                tables_exist_query = """
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_schema = $1
                AND table_name IN ('hearingpackages', 'hearingpackages_granules')
                """
                tables_count = await conn.fetchval(
                    tables_exist_query, self.staging_schema
                )

                if tables_count < 2:
                    logger.warning(
                        "One or both hearingpackages staging tables do not exist"
                    )
                    return

                # Get total count for logging
                packages_without_granules_query = f"""
                SELECT COUNT(DISTINCT c.packageid)
                FROM {self.staging_schema}.hearingpackages c
                WHERE NOT EXISTS (
                    SELECT 1 FROM {self.staging_schema}.hearingpackages_granules g
                    WHERE g.packageid = c.packageid
                )
                """
                packages_without_granules_count = await conn.fetchval(
                    packages_without_granules_query
                )

                granule_count_query = f"""
                SELECT COUNT(DISTINCT g.granuleid)
                FROM {self.staging_schema}.hearingpackages_granules g
                """
                granule_count = await conn.fetchval(granule_count_query)

                total_count = packages_without_granules_count + granule_count

                if total_count == 0:
                    logger.info("No hearingpackages records found")
                    return

                logger.info(
                    f"Streaming {total_count} hearingpackages records in chunks of {chunk_size}"
                )

                # Stream using pagination with aggregated formats
                offset = 0
                while True:
                    logger.debug(f"Fetching hearingpackages chunk at offset {offset}")

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
                            NULL::boolean AS isappropriation,
                            c.otheridentifier_migrated_doc_id AS otheridentifier_migrated_doc_id,
                            c.sudocclassnumber AS sudocclassnumber,
                            c.governmentauthor1 AS governmentauthor1,
                            c.governmentauthor2 AS governmentauthor2,
                            c.processed_at AS processed_at,
                            c.source_doc_id AS source_doc_id,
                            c.otheridentifier_ils_system_id AS otheridentifier_ils_system_id,
                            c.otheridentifier_sudoc_item_number AS otheridentifier_sudoc_item_number,
                            c.otheridentifier_purl AS otheridentifier_purl,
                            c.otheridentifier_oclc AS otheridentifier_oclc,
                            c.committees AS committees,
                            c.otheridentifier_isbn AS otheridentifier_isbn,
                            c.fields AS fields,
                            c.download_thumbnailjpeg AS download_thumbnailjpeg,
                            c.federalpublicationname AS federalpublicationname,
                            c.download_txtlink AS download_txtlink,
                            c._references AS _references,
                            c.fields_name AS fields_name,
                            c.fields_value AS fields_value,
                            c.subjects_topics AS subjects_topics,
                            c.seriestitle AS seriestitle,
                            NULL::text AS heading,
                            NULL::text AS partnumber,
                            NULL::text AS volumenumber,
                            NULL AS graphicsinpdf,
                            NULL::text AS witnesses,
                            NULL::text AS booknumber
                        FROM {self.staging_schema}.hearingpackages c
                        WHERE NOT EXISTS (
                            SELECT 1 FROM {self.staging_schema}.hearingpackages_granules g
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
                            g.isappropriation::BOOLEAN AS isappropriation,
                            c.otheridentifier_migrated_doc_id AS otheridentifier_migrated_doc_id,
                            c.sudocclassnumber AS sudocclassnumber,
                            c.governmentauthor1 AS governmentauthor1,
                            c.governmentauthor2 AS governmentauthor2,
                            c.processed_at AS processed_at,
                            c.source_doc_id AS source_doc_id,
                            c.otheridentifier_ils_system_id AS otheridentifier_ils_system_id,
                            c.otheridentifier_sudoc_item_number AS otheridentifier_sudoc_item_number,
                            c.otheridentifier_purl AS otheridentifier_purl,
                            c.otheridentifier_oclc AS otheridentifier_oclc,
                            c.committees AS committees,
                            c.otheridentifier_isbn AS otheridentifier_isbn,
                            c.fields AS fields,
                            c.download_thumbnailjpeg AS download_thumbnailjpeg,
                            c.federalpublicationname AS federalpublicationname,
                            c.download_txtlink AS download_txtlink,
                            c._references AS _references,
                            c.fields_name AS fields_name,
                            c.fields_value AS fields_value,
                            c.subjects_topics AS subjects_topics,
                            c.seriestitle AS seriestitle,
                            g.heading AS heading,
                            g.partnumber AS partnumber,
                            g.volumenumber AS volumenumber,
                            g.graphicsinpdf AS graphicsinpdf,
                            g.witnesses AS witnesses,
                            g.booknumber AS booknumber
                        FROM {self.staging_schema}.hearingpackages c
                        JOIN {self.staging_schema}.hearingpackages_granules g
                            ON c.packageid = g.packageid
                        WHERE g.granuleid IS NOT NULL
                    ) q
                    ORDER BY q.packageid, q.granuleid NULLS FIRST
                    LIMIT {chunk_size} OFFSET {offset}
                    """

                    logger.debug(
                        f"Executing query for hearingpackages at offset {offset}"
                    )
                    rows = await conn.fetch(query)
                    logger.debug(
                        f"Retrieved {len(rows)} rows for hearingpackages at offset {offset}"
                    )

                    if not rows:
                        logger.info(
                            f"No more hearingpackages records at offset {offset}"
                        )
                        break

                    chunk = []
                    for row in rows:
                        try:
                            row_dict = dict(row)
                            chunk.append(row_dict)
                        except Exception as e:
                            logger.error(
                                f"Error converting hearingpackages row to dict: {e}, row type: {type(row)}, row: {row}"
                            )
                            continue

                    if chunk:  # Only yield if we have valid records
                        logger.debug(f"Yielding {len(chunk)} hearingpackages records")
                        yield chunk

                    offset += chunk_size

                    # Log progress periodically
                    if offset % (chunk_size * 10) == 0:
                        logger.debug(f"Streamed {offset} hearingpackages records")

            except Exception as e:
                logger.error(f"Error streaming hearingpackages: {e}")
                raise

    async def _stream_hearingpackages_reference_bills_joined_chunks(
        self, chunk_size: int
    ) -> AsyncGenerator[list[dict[str, Any]], None]:
        """
        Stream hearing packages bills joined chunks.
        """
        if not self.db_pool:
            raise ValueError("Database pool not configured")

        logger.info(
            f"Streaming hearingpackages_reference_bills records in chunks of {chunk_size}"
        )

        # Check if tables exist
        async with self.db_pool.acquire() as conn, conn.transaction():
            tables_exist_query = f"""
                SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '{self.staging_schema}' AND table_name IN ('hearingpackages_granules_references_contents', 'hearingpackages_granules_references', 'hearingpackages_granules', 'hearingpackages')
            """
            tables_count = await conn.fetchval(tables_exist_query)

            if tables_count < 4:
                logger.warning(
                    f"Missing required tables in {self.staging_schema} schema: "
                    "hearingpackages_granules_references_contents, "
                    "hearingpackages_granules_references, "
                    "hearingpackages_granules, "
                    "hearingpackages"
                )
                return

        try:
            offset = 0
            while True:
                logger.debug(
                    f"Fetching hearingpackages_reference_bills chunk at offset {offset}"
                )

                async with self.db_pool.acquire() as conn, conn.transaction():
                    query = f"""
                            SELECT DISTINCT ON (type, number, congress, references_id) hpgrc.*, hpg.granuleid AS granule_id, hpg.packageid AS package_id
                            FROM {self.staging_schema}.hearingpackages_granules_references_contents AS hpgrc
                            JOIN {self.staging_schema}.hearingpackages_granules_references AS hpgr ON hpgr.id = hpgrc.references_id
                            JOIN {self.staging_schema}.hearingpackages_granules AS hpg ON hpgr.granule_id = hpg.id
                            ORDER BY type, number, congress, references_id, hpg.packageid, hpg.granuleid NULLS FIRST
                            LIMIT {chunk_size} OFFSET {offset}
                            """

                    logger.debug(
                        f"Executing query for hearingpackages_reference_bills at offset {offset}"
                    )
                    rows = await conn.fetch(query)
                    logger.debug(
                        f"Retrieved {len(rows)} rows for hearingpackages_reference_bills at offset {offset}"
                    )

                    if not rows:
                        logger.info(
                            f"No more hearingpackages_reference_bills records at offset {offset}"
                        )
                        break

                    chunk = []
                    for row in rows:
                        try:
                            row_dict = dict(row)
                            chunk.append(row_dict)
                        except Exception as e:
                            logger.error(
                                f"Error converting hearingpackages_reference_bills row to dict: {e}, row type: {type(row)}, row: {row}"
                            )
                            continue

                    if chunk:
                        logger.debug(
                            f"Yielding {len(chunk)} hearingpackages_reference_bills records"
                        )
                        yield chunk

                    offset += chunk_size

                    if offset % (chunk_size * 10) == 0:
                        logger.debug(
                            f"Streamed {offset} hearingpackages_reference_bills records"
                        )

        except Exception as e:
            logger.error(f"Error streaming hearingpackages_reference_bills: {e}")
            raise

    async def _stream_hearingpackages_witnesses_joined_chunks(
        self, chunk_size: int
    ) -> AsyncGenerator[list[dict[str, Any]], None]:
        """
        Stream hearing packages witnesses joined chunks.
        """
        if not self.db_pool:
            raise ValueError("Database pool not configured")

        logger.info(
            f"Streaming hearingpackages_witnesses records in chunks of {chunk_size}"
        )

        # Check if tables exist
        async with self.db_pool.acquire() as conn, conn.transaction():
            tables_exist_query = f"""
                SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '{self.staging_schema}' AND table_name IN ('hearingpackages_granules_witnesses', 'hearingpackages_granules')
            """
            tables_count = await conn.fetchval(tables_exist_query)

            if tables_count < 2:
                logger.warning(
                    f"Missing required tables in {self.staging_schema} schema: "
                    "hearingpackages_granules_witnesses, "
                    "hearingpackages_granules"
                )
                return

        try:
            offset = 0
            while True:
                logger.debug(
                    f"Fetching hearingpackages_witnesses chunk at offset {offset}"
                )

                async with self.db_pool.acquire() as conn, conn.transaction():
                    query = f"""
                        SELECT DISTINCT ON (value) hpgrc.value AS witness, hpgr.granuleid AS granule_id, hpgr.packageid AS package_id
                        FROM {self.staging_schema}.hearingpackages_granules_witnesses AS hpgrc
                        JOIN {self.staging_schema}.hearingpackages_granules AS hpgr ON hpgr.id = hpgrc.granule_id
                        ORDER BY value, hpgr.packageid, hpgr.granuleid NULLS FIRST
                        LIMIT {chunk_size} OFFSET {offset}
                        """

                    logger.debug(
                        f"Executing query for hearingpackages_witnesses at offset {offset}"
                    )
                    rows = await conn.fetch(query)
                    logger.debug(
                        f"Retrieved {len(rows)} rows for hearingpackages_witnesses at offset {offset}"
                    )

                    if not rows:
                        logger.info(
                            f"No more hearingpackages_witnesses records at offset {offset}"
                        )
                        break

                    chunk = []
                    for row in rows:
                        try:
                            row_dict = dict(row)
                            chunk.append(row_dict)
                        except Exception as e:
                            logger.error(
                                f"Error converting hearingpackages_witnesses row to dict: {e}, row type: {type(row)}, row: {row}"
                            )
                            continue

                    if chunk:
                        logger.debug(
                            f"Yielding {len(chunk)} hearingpackages_witnesses records"
                        )
                        yield chunk

                    offset += chunk_size

                    if offset % (chunk_size * 10) == 0:
                        logger.debug(
                            f"Streamed {offset} hearingpackages_witnesses records"
                        )

        except Exception as e:
            logger.error(f"Error streaming hearingpackages_witnesses: {e}")
            raise

    async def _stream_hearingpackages_agencies_joined_chunks(
        self, chunk_size: int
    ) -> AsyncGenerator[list[dict[str, Any]], None]:
        """
        Stream hearing packages agencies joined chunks.
        """
        if not self.db_pool:
            raise ValueError("Database pool not configured")

        logger.info(
            f"Streaming hearingpackages_agencies records in chunks of {chunk_size}"
        )

        # Check if tables exist
        async with self.db_pool.acquire() as conn, conn.transaction():
            tables_exist_query = f"""
                SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '{self.staging_schema}' AND table_name IN ('hearingpackages_granules_agencies', 'hearingpackages_granules')
            """
            tables_count = await conn.fetchval(tables_exist_query)

            if tables_count < 2:
                logger.warning(
                    f"Missing required tables in {self.staging_schema} schema: "
                    "hearingpackages_granules_agencies, "
                    "hearingpackages_granules"
                )
                return

        try:
            offset = 0
            while True:
                logger.debug(
                    f"Fetching hearingpackages_agencies chunk at offset {offset}"
                )

                async with self.db_pool.acquire() as conn, conn.transaction():
                    query = f"""
                        SELECT DISTINCT ON (name) hpgrc.name AS agency, hpgr.granuleid AS granule_id, hpgr.packageid AS package_id
                        FROM {self.staging_schema}.hearingpackages_granules_agencies AS hpgrc
                        JOIN {self.staging_schema}.hearingpackages_granules AS hpgr ON hpgr.id = hpgrc.granule_id
                        ORDER BY name, hpgr.packageid, hpgr.granuleid NULLS FIRST
                        LIMIT {chunk_size} OFFSET {offset}
                        """

                    logger.debug(
                        f"Executing query for hearingpackages_agencies at offset {offset}"
                    )
                    rows = await conn.fetch(query)
                    logger.debug(
                        f"Retrieved {len(rows)} rows for hearingpackages_agencies at offset {offset}"
                    )

                    if not rows:
                        logger.info(
                            f"No more hearingpackages_agencies records at offset {offset}"
                        )
                        break

                    chunk = []
                    for row in rows:
                        try:
                            row_dict = dict(row)
                            chunk.append(row_dict)
                        except Exception as e:
                            logger.error(
                                f"Error converting hearingpackages_agencies row to dict: {e}, row type: {type(row)}, row: {row}"
                            )
                            continue

                    if chunk:
                        logger.debug(
                            f"Yielding {len(chunk)} hearingpackages_agencies records"
                        )
                        yield chunk

                    offset += chunk_size

                    if offset % (chunk_size * 10) == 0:
                        logger.debug(
                            f"Streamed {offset} hearingpackages_agencies records"
                        )

        except Exception as e:
            logger.error(f"Error streaming hearingpackages_agencies: {e}")
            raise

    async def _clean_hearingpackages_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual congressional reports records.
        STAGING COLUMNS:
        - packageid,
        - granuleid,
        - title,
        - subtitle,
        - branch,
        - chamber,
        - session,
        - category,
        - congress,
        - docclass,
        - download_ziplink,
        - download_modslink,
        - download_premislink,
        - download_pdflink,
        - download_txtlink,
        - detailslink,
        - lastmodified,
        - documenttype,
        - documentnumber,
        - publisher,
        - dateissued,
        - granuleslink,
        - collectioncode,
        - collectionname,
        - pages,
        - isappropriation,
        - otheridentifier_migrated_doc_id,
        - sudocclassnumber,
        - governmentauthor1,
        - governmentauthor2,
        - processed_at,
        - source_doc_id,
        - otheridentifier_ils_system_id,
        - otheridentifier_sudoc_item_number,
        - otheridentifier_purl,
        - otheridentifier_oclc,
        - committees,
        - otheridentifier_isbn,
        - fields,
        - download_thumbnailjpeg,
        - federalpublicationname,
        - download_txtlink,
        - _references,
        - fields_name,
        - fields_value,
        - subjects_topics,
        - seriestitle,
        - heading,
        - partnumber,
        - volumenumber,
        - graphicsinpdf,
        - witnesses,
        - booknumber

        FINAL COLUMNS:
        - package_id TEXT PRIMARY KEY,
        - hearing_id TEXT,
        - granule_id TEXT,
        - hearing_set_id TEXT,
        - title TEXT,
        - subtitle TEXT,
        - chamber TEXT, -- lower
        - congress INTEGER,
        - session INTEGER,
        - pages INTEGER,
        - is_appropriation BOOLEAN,
        - is_errata BOOLEAN,
        - issued_at DATE,
        - branch TEXT,
        - government_author1 TEXT,
        - government_author2 TEXT,
        - publisher TEXT,
        - collection_code TEXT,
        - federal_publication_name TEXT,
        - migrated_doc_id TEXT,
        - su_doc_class_number TEXT,
        - last_modified TIMESTAMP WITH TIME ZONE
        """
        cleaned = record_data.copy()

        # For historical reasons this method uses the identity builder; retain call
        (
            hearing_id,
            hearing_set_id,
            _,
            part_number,
            hearing_type,
            hearing_number,
            congress,
        ) = self._build_hearing_identity(cleaned)

        # Compute errata flag using shared heuristics
        errata_detected = self._detect_is_errata(cleaned)
        if hearing_type is None:
            raise ValueError(f"Hearing type is None for {cleaned.get('packageid')}")
        chamber = (
            "house "
            if hearing_type == "hhrg"
            else "senate"
            if hearing_type == "shrg"
            else "joint"
            if hearing_type == "jhrg"
            else None
        )

        def normalize_subtitle(value: str) -> str:
            # Robustly normalize whitespace-like or NBSP-only strings to None
            if value is None:
                return None
            try:
                text = str(value)
            except Exception:
                return None
            # Decode HTML entities (e.g., &nbsp;)
            text = html.unescape(text)
            # Replace common non-breaking/zero-width spaces
            text = (
                text.replace("\u00a0", " ")  # NBSP
                .replace("\u202f", " ")  # NARROW NBSP
                .replace("\u2007", " ")  # FIGURE SPACE
                .replace("\ufeff", "")  # BOM
                .replace("\u200b", "")  # ZERO WIDTH SPACE
            )
            # Collapse whitespace and strip
            text = re.sub(r"\s+", " ", text).strip()
            if not text or text.lower() == "nbsp":
                return None
            return text

        # Normalize subtitle: map NBSP-only values to NULL
        subtitle_value = normalize_subtitle(cleaned.get("subtitle", None))
        title_value = normalize_subtitle(cleaned.get("title", None))

        # Apply bills-specific cleaning logic
        filtered_cleaned = {
            "package_id": str(cleaned.get("packageid", "ID_ERROR")),
            "hearing_id": str(hearing_id),
            # Ensure a non-null granule_id: use granule if present, otherwise fallback to package_id
            "granule_id": str(
                cleaned.get("granuleid")
                or cleaned.get("granule_id")
                or cleaned.get("packageid")
                or cleaned.get("package_id")
            ),
            "hearing_set_id": hearing_set_id,
            "title": title_value,
            "subtitle": subtitle_value,
            "hearing_type": hearing_type,
            "hearing_number": str(hearing_number),
            "part_number": self.safe_int(part_number),
            "chamber": self.standardize_chamber(cleaned.get("chamber", chamber)),
            "congress": self.safe_int(congress),
            "session": self.safe_int(cleaned.get("session", None)),
            "pages": self.safe_int(cleaned.get("pages", None)),
            "is_appropriation": cleaned.get("isappropriation", False),
            "is_errata": errata_detected,
            "issued_at": self.standardize_date(cleaned.get("dateissued", None)),
            "branch": cleaned.get("branch", None),
            "government_author1": cleaned.get("governmentauthor1", None),
            "government_author2": cleaned.get("governmentauthor2", None),
            "publisher": cleaned.get("publisher", None),
            "collection_code": cleaned.get("collectioncode", None),
            "federal_publication_name": cleaned.get("federalpublicationname", None),
            "migrated_doc_id": cleaned.get("otheridentifier_migrated_doc_id", None),
            "su_doc_class_number": cleaned.get("sudocclassnumber", None),
            "last_modified": self.standardize_date(cleaned.get("lastmodified", None)),
        }

        return filtered_cleaned

    async def _clean_hearingpackages_committees_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual hearing packages committees records.
        STAGING COLUMNS:
        - package_id    text,
        - granuleid    text,
        - authorityid   text,
        - committeename text,

        FINAL COLUMNS:
        - package_id TEXT,
        - granule_id TEXT,
        - committee_code TEXT,
        - committee_name TEXT,
        """
        # logger.info(f"Cleaning committees: {record_data}")
        cleaned = record_data.copy()

        if cleaned.get("authorityid") in COMMITTEE_FIXES:
            cleaned["authorityid"] = COMMITTEE_FIXES.get(cleaned.get("authority_id"))

        filtered_cleaned = {
            "package_id": cleaned.get("package_id", "ID_ERROR"),
            "granule_id": cleaned.get("granule_id", "ID_ERROR"),
            "committee_code": cleaned.get("authorityid", "ID_ERROR"),
            "committee_name": cleaned.get("committeename", None),
        }

        return filtered_cleaned

    async def _clean_hearingpackages_members_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual hearing packages members records.
        STAGING COLUMNS:
        hpg.packageid,
        hpg.granuleid,
        hpgm.bioguideid,
        hpgm.membername,
        hpgm.authorityid,
        hpgm.gpoid,
        hpgmn.authority_fnf,
        hpgmn.authority_other,

        FINAL COLUMNS:
        - package_id TEXT,
        - granule_id TEXT,
        - bioguide_id TEXT,
        - membername TEXT,
        - authorityid TEXT,
        - gpoid TEXT,
        """

        cleaned = record_data.copy()

        if (
            cleaned.get("bioguideid") is None
            or cleaned.get("bioguideid") == ""
            or cleaned.get("bioguideid") == "ID_ERROR"
        ):
            if (
                cleaned.get("parsed", "") in HEARING_BIOGUIDE_FIXES
                and cleaned.get("bioguideid", None) is None
                or cleaned.get("bioguideid") == ""
                or cleaned.get("bioguideid") == "ID_ERROR"
                and cleaned.get("granuleid") is not None
                and HEARING_BIOGUIDE_FIXES.get(cleaned.get("parsed", "")) != "WITNESS"
            ):
                cleaned["bioguideid"] = HEARING_BIOGUIDE_FIXES.get(
                    cleaned.get("parsed", "")
                )
                logger.info(
                    f"Bioguide ID fixed: {cleaned.get('parsed', '')} -> {cleaned.get('bioguideid', 'ID_ERROR')}"
                )
            elif (
                cleaned.get("bioguideid", None) is None
                or cleaned.get("bioguideid") == ""
                and cleaned.get("granuleid") is not None
            ):
                cleaned["bioguideid"] = "ID_ERROR"
                with open("error.txt", "a") as f:
                    f.write(json.dumps(cleaned) + "\n")
                raise ValueError(
                    f"Bioguide ID not found for {cleaned.get('package_id', 'ID_ERROR')} and granuleid {cleaned.get('granuleid')}"
                )

        filtered_cleaned = {
            "package_id": cleaned.get("package_id", "ID_ERROR"),
            "granule_id": cleaned.get("granule_id", "ID_ERROR"),
            "bioguide_id": (
                cleaned.get("bioguideid")
                if cleaned.get("bioguideid", None) != ""
                else "ID_ERROR"
            ),
            "name": cleaned.get("membername", None),
            "authority_id": (
                cleaned.get("authorityid")
                if cleaned.get("authorityid", None) != ""
                else None
            ),
            "gpo_id": (
                cleaned.get("gpoid") if cleaned.get("gpoid", None) != "" else None
            ),
        }

        return filtered_cleaned

    async def _clean_hearingpackages_dates_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual hearing packages dates records.
        """
        cleaned = record_data.copy()
        filtered_cleaned = {
            "package_id": cleaned.get("package_id", "ID_ERROR"),  # parent only?
            "date": cleaned.get("value", "ID_ERROR"),
        }
        return filtered_cleaned

    async def _clean_hearingpackages_reference_bills_singular(
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

    async def _clean_hearingpackages_witnesses_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual hearing packages witnesses records.
        """
        cleaned = record_data.copy()
        filtered_cleaned = {
            "package_id": cleaned.get("package_id", "ID_ERROR"),
            "granule_id": cleaned.get("granule_id", "ID_ERROR"),
            "witness": cleaned.get("witness", "ID_ERROR"),
        }
        return filtered_cleaned

    async def _clean_hearingpackages_agencies_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual hearing packages agencies records.
        """
        cleaned = record_data.copy()
        filtered_cleaned = {
            "package_id": cleaned.get("package_id", "ID_ERROR"),
            "granule_id": cleaned.get("granule_id", "ID_ERROR"),
            "agency": cleaned.get("agency", "ID_ERROR"),
        }
        return filtered_cleaned

    # Add post-processing methods
    async def _post_process_hearingpackages(self) -> dict[str, Any]:
        """Post-processing operations specific to bills data."""
        # TODO: ils system id
        # TODO: reference laws/codes/statutes
        # TODO: fix gpo-chrg-roberts
        # TODO: fix placeholder parts
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
                    FROM {self.staging_schema}.hearingpackages
                """
                rows = await conn.fetch(fetch_sql)
                logger.info(f"Found {len(rows)} records with ils_system_id data")

                # Prepare batch params
                insert_sql = f"""
                    INSERT INTO {self.production_schema}.hearingpackages_ils_system_id (package_id, ils_system_id)
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

        # ? Operation 2: references
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
                    FROM {self.staging_schema}.hearingpackages_references
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
                            INSERT INTO {self.production_schema}.hearingpackages_reference_laws (package_id, law_id, law_type, law_number, order_number, congress)
                            VALUES ($1, $2, $3, $4, $5, $6)
                            ON CONFLICT DO NOTHING
                        """
                        await conn.executemany(insert_sql, law_params)
                        total_counts["laws"] += len(law_params)
                        logger.info(f"Inserted {len(law_params)} law records")

                    if statute_params:
                        insert_sql = f"""
                            INSERT INTO {self.production_schema}.hearingpackages_reference_statutes (report_statute_id, package_id, reference_statute)
                            VALUES ($1, $2, $3)
                            ON CONFLICT DO NOTHING
                        """
                        await conn.executemany(insert_sql, statute_params)
                        total_counts["statutes"] += len(statute_params)
                        logger.info(f"Inserted {len(statute_params)} statute records")

                    if code_params:
                        insert_sql = f"""
                            INSERT INTO {self.production_schema}.hearingpackages_reference_codes (report_code_id, package_id, reference_code)
                            VALUES ($1, $2, $3)
                            ON CONFLICT DO NOTHING
                        """
                        await conn.executemany(insert_sql, code_params)
                        total_counts["codes"] += len(code_params)
                        logger.info(f"Inserted {len(code_params)} code records")

                    if statute_page_params:
                        insert_sql = f"""
                            INSERT INTO {self.production_schema}.hearingpackages_reference_statutes_pages (report_statute_id, page)
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
                            INSERT INTO {self.production_schema}.hearingpackages_reference_codes_sections (report_code_id, code_section)
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

        # ? Operation 3: Resolve placeholder 'x' part numbers across all reports
        logger.info("Starting placeholder part resolution post-processing")
        try:
            async with self.db_pool.acquire() as conn, conn.transaction():
                # Fetch all production rows with placeholder 'x' (including errata 'xe')
                prod_rows = await conn.fetch(
                    f"""
                    SELECT package_id, granule_id, hearing_id, is_errata
                    FROM {self.production_schema}.hearingpackages
                    WHERE hearing_id LIKE '%-x-%' OR hearing_id LIKE '%-xe-%'
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
                                str(row["hearing_id"]),
                                re.IGNORECASE,
                            )
                            if not m:
                                continue
                            key = (m.group(1).lower(), m.group(2), m.group(4))
                            groups.setdefault(key, []).append(dict(row))

                        def parse_order(pkg_id: str) -> tuple[int, int]:
                            """Compute ordering key for placeholder parts within a hearing group.

                            Priority:
                            1) GPO-CHRG-ROBERTS special numeric triple
                            2) '-pt<token>' where token mixes digits+letters -> (number, alphaIndex)
                            3) Trailing mixed token like '...-6A' -> (6, alphaIndex)
                            4) Generic fallback using trailing numeric tokens (a, b)
                            """
                            try:
                                s = str(pkg_id) if pkg_id is not None else ""

                                # 1) Robust ordering for GPO-CHRG-ROBERTS: concatenate numeric tokens base-1000
                                if "GPO-CHRG-ROBERTS" in s:
                                    parts = s.split("-")
                                    if len(parts) >= 4:
                                        try:
                                            a = (
                                                int(parts[2])
                                                if parts[2].isdigit()
                                                else 0
                                            )
                                            b = (
                                                int(parts[3])
                                                if parts[3].isdigit()
                                                else 0
                                            )
                                            c = (
                                                int(parts[4])
                                                if len(parts) > 4 and parts[4].isdigit()
                                                else 0
                                            )
                                            return (a * 1000000 + b * 1000 + c, 0)
                                        except (ValueError, IndexError):
                                            pass

                                def letter_idx(token_letters: str | None) -> int:
                                    if not token_letters:
                                        return 0
                                    m = re.search(r"[A-Za-z]", token_letters)
                                    if not m:
                                        return 0
                                    ch = m.group(0)
                                    return ord(ch.lower()) - ord("a") + 1

                                # 2) '-pt<token>' with mixed alnum (e.g., pt1A, pt2-B, pt3(a))
                                m_pt = re.search(
                                    r"[-_]pt([A-Za-z0-9()\-]+)", s, re.IGNORECASE
                                )
                                if m_pt:
                                    raw_tok = m_pt.group(1)
                                    norm = re.sub(r"[^A-Za-z0-9]", "", raw_tok)
                                    if re.search(r"[A-Za-z]", norm) and re.search(
                                        r"\d", norm
                                    ):
                                        num_m = re.search(r"(\d+)", norm)
                                        let_m = re.search(r"([A-Za-z]+)", norm)
                                        num = int(num_m.group(1)) if num_m else 0
                                        alph = letter_idx(
                                            let_m.group(1) if let_m else None
                                        )
                                        return (num, alph)

                                # 3) Trailing mixed token like '...-6A' or '..._8-C'
                                m_tail = re.search(r"(\d+)[\-_()]*([A-Za-z])$", s)
                                if m_tail:
                                    num = int(m_tail.group(1))
                                    alph = letter_idx(m_tail.group(2))
                                    return (num, alph)

                                # 4) Generic fallback: last one or two numeric tokens
                                m = re.search(r"(\d+)(?:-(\d+))?$", s)
                                if m:
                                    a = int(m.group(1))
                                    b = int(m.group(2)) if m.group(2) else 0
                                    return a, b
                            except Exception:
                                pass
                            return (1 << 30, 1 << 30)

                        # Prepare batch updates for this chunk
                        main_table_updates = []

                        total_groups = len(groups)
                        logger.debug(
                            f"Processing {total_groups} groups in chunk {chunk_idx + 1}"
                        )

                        for group_idx, (_key, rows) in enumerate(groups.items()):
                            if group_idx % 1000 == 0 and total_groups > 1000:
                                logger.debug(
                                    f"Processing group {group_idx}/{total_groups} in chunk {chunk_idx + 1}"
                                )

                            # Order within group by trailing numeric tokens of granule_id (fallback to package_id)
                            rows_with_order = []
                            for r in rows:
                                key_for_order = (
                                    r.get("granule_id") or r.get("package_id") or ""
                                )
                                rows_with_order.append(
                                    (parse_order(str(key_for_order)), r)
                                )
                            rows_with_order.sort(key=lambda x: x[0])

                            # Assign parts sequentially starting at 1
                            for idx, (_ord, r) in enumerate(rows_with_order, start=1):
                                package_id = r["package_id"]
                                old_hearing_id = r["hearing_id"]
                                is_errata_flag = (
                                    bool(r["is_errata"])
                                    if r["is_errata"] is not None
                                    else False
                                )

                                m = re.match(
                                    r"^([a-z]+)(\d+)-([^-]+)-(\d+)$",
                                    str(old_hearing_id),
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
                                new_hearing_id = (
                                    f"{rtype}{rnum}-{new_part_token}-{cong}"
                                )

                                if new_hearing_id == old_hearing_id:
                                    continue

                                # Collect updates for batch processing
                                granule_id = r.get("granule_id")
                                # Map chamber from hearing type if chamber is currently NULL
                                # hhrg -> house, shrg -> senate, jhrg -> joint
                                if rtype.startswith("h"):
                                    chamber_val = "house"
                                elif rtype.startswith("s"):
                                    chamber_val = "senate"
                                elif rtype.startswith("j"):
                                    chamber_val = "joint"
                                else:
                                    chamber_val = None

                                # Include part_number (as integer) and chamber
                                main_table_updates.append(
                                    (
                                        new_hearing_id,
                                        idx,  # part_number should be integer, not string
                                        chamber_val,
                                        package_id,
                                        granule_id,
                                    )
                                )
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
                                    UPDATE {self.production_schema}.hearingpackages
                                    SET hearing_id = $1,
                                        part_number = $2,
                                        chamber = COALESCE(chamber, $3)
                                    WHERE package_id = $4 AND granule_id = $5
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

        # ? Operation 4: Backfill NULL chamber values from hearing type prefix in hearing_id
        logger.info("Starting chamber backfill post-processing")
        try:
            async with self.db_pool.acquire() as conn, conn.transaction():
                update_sql = f"""
                    UPDATE {self.production_schema}.hearingpackages
                    SET chamber = CASE
                        WHEN lower(substr(hearing_id, 1, 4)) = 'hhrg' THEN 'house'
                        WHEN lower(substr(hearing_id, 1, 4)) = 'shrg' THEN 'senate'
                        WHEN lower(substr(hearing_id, 1, 4)) = 'jhrg' THEN 'joint'
                        ELSE chamber
                    END
                    WHERE chamber IS NULL AND hearing_id IS NOT NULL
                """
                result = await conn.execute(update_sql)
                # asyncpg returns strings like "UPDATE <n>"
                affected = 0
                try:
                    parts = result.split()
                    if len(parts) == 2:
                        affected = int(parts[1])
                except Exception:
                    affected = 0
                logger.info(f"Chamber backfill updated {affected} rows")
                results["operations"].append(
                    {
                        "name": "backfill_chamber_from_hearing_id",
                        "status": "success",
                        "rows_affected": affected,
                    }
                )
                results["rows_affected"] += affected

                # Also update hearing_id and parent_hearing_id prefixes from 'xhrg' to the new hearing_type
                logger.info(
                    "Updating hearing_id and parent_hearing_id prefixes for xhrg rows just backfilled"
                )
                res = await conn.execute(
                    f"""
                    WITH type_map AS (
                        SELECT DISTINCT package_id,
                               CASE lower(substr(committee_code, 1, 1))
                                   WHEN 'h' THEN 'hhrg'
                                   WHEN 's' THEN 'shrg'
                                   WHEN 'j' THEN 'jhrg'
                                   ELSE NULL
                               END AS new_type
                        FROM {self.production_schema}.hearingpackages_committees
                        WHERE committee_code IS NOT NULL AND committee_code <> ''
                    ),
                    targets AS (
                        SELECT hp.package_id, tm.new_type, hp.hearing_id, hp.hearing_set_id
                        FROM {self.production_schema}.hearingpackages hp
                        JOIN type_map tm USING (package_id)
                        WHERE hp.hearing_type = tm.new_type
                          AND tm.new_type IS NOT NULL
                          AND (
                            hp.hearing_id LIKE 'xhrg%'
                            OR (hp.hearing_set_id IS NOT NULL AND hp.hearing_set_id LIKE 'xhrg%')
                          )
                    ),
                    updated AS (
                        SELECT
                            package_id,
                            CASE WHEN hearing_id LIKE 'xhrg%'
                                 THEN regexp_replace(hearing_id, '^xhrg', new_type)
                                 ELSE hearing_id END AS new_hearing_id,
                            CASE WHEN hearing_set_id LIKE 'xhrg%'
                                 THEN regexp_replace(hearing_set_id, '^xhrg', new_type)
                                 ELSE hearing_set_id END AS new_hearing_set_id
                        FROM targets
                    )
                    UPDATE {self.production_schema}.hearingpackages hp
                    SET hearing_id = u.new_hearing_id,
                        hearing_set_id = u.new_hearing_set_id
                    FROM updated u
                    WHERE hp.package_id = u.package_id
                      AND (hp.hearing_id IS DISTINCT FROM u.new_hearing_id
                           OR hp.hearing_set_id IS DISTINCT FROM u.new_hearing_set_id)
                    """
                )
                id_fix = 0
                try:
                    parts = res.split()
                    if len(parts) == 2:
                        id_fix = int(parts[1])
                except Exception:
                    id_fix = 0
                logger.info(
                    f"Updated hearing_id/hearing_set_id prefixes for {id_fix} rows"
                )
                results["operations"].append(
                    {
                        "name": "update_hearing_ids_after_hearing_type_backfill",
                        "status": "success",
                        "rows_affected": id_fix,
                    }
                )
                results["rows_affected"] += id_fix
        except Exception as e:
            logger.error(f"Error backfilling chamber: {e}", exc_info=True)
            results["operations"].append(
                {
                    "name": "backfill_hearing_type_from_hearing_id",
                    "status": "error",
                    "error": str(e),
                }
            )
            results["status"] = "partial_failure"

        # ? Operation 4B: Derive hearing_type/chamber from committees for remaining 'xhrg' with NULL chamber
        logger.info(
            "Starting hearing type/chamber backfill from committees for xhrg with NULL chamber"
        )
        try:
            async with self.db_pool.acquire() as conn, conn.transaction():
                update_sql = f"""
                    WITH first_comm AS (
                        SELECT DISTINCT ON (package_id)
                            package_id,
                            lower(substr(committee_code, 1, 1)) AS cc1
                        FROM {self.production_schema}.hearingpackages_committees
                        WHERE committee_code IS NOT NULL AND committee_code <> ''
                        ORDER BY package_id, committee_code
                    ),
                    mapped AS (
                        SELECT
                            package_id,
                            CASE cc1
                                WHEN 'h' THEN 'hhrg'
                                WHEN 's' THEN 'shrg'
                                WHEN 'j' THEN 'jhrg'
                                ELSE NULL
                            END AS new_type,
                            CASE cc1
                                WHEN 'h' THEN 'house'
                                WHEN 's' THEN 'senate'
                                WHEN 'j' THEN 'joint'
                                ELSE NULL
                            END AS new_chamber
                        FROM first_comm
                    )
                    UPDATE {self.production_schema}.hearingpackages hp
                    SET
                    hearing_type = m.new_type,
                        chamber = m.new_chamber
                    FROM mapped m
                    WHERE hp.package_id = m.package_id
                        AND hp.hearing_type = 'xhrg'
                        AND hp.chamber IS NULL
                        AND m.new_type IS NOT NULL
                """

                result = await conn.execute(update_sql)
                affected = 0
                try:
                    parts = result.split()
                    if len(parts) == 2:
                        affected = int(parts[1])
                except Exception:
                    affected = 0
                logger.info(f"xhrg committee-based backfill updated {affected} rows")
                results["operations"].append(
                    {
                        "name": "backfill_hearing_type_chamber_from_committees",
                        "status": "success",
                        "rows_affected": affected,
                    }
                )
                results["rows_affected"] += affected
        except Exception as e:
            logger.error(
                f"Error backfilling hearing type/chamber from committees: {e}",
                exc_info=True,
            )
            results["operations"].append(
                {
                    "name": "backfill_hearing_type_chamber_from_committees",
                    "status": "error",
                    "error": str(e),
                }
            )
            results["status"] = "partial_failure"

        # ? Operation 5: Group series by highly-similar titles within same congress
        logger.info("Starting title series grouping post-processing (SQL-optimized)")
        try:
            async with self.db_pool.acquire() as conn, conn.transaction():
                # Try to enable pg_trgm for fuzzy similarity if available
                pg_trgm_available = True
                try:
                    await conn.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
                except Exception:
                    pg_trgm_available = False
                    logger.warning(
                        "pg_trgm extension not available; falling back to exact-title grouping only"
                    )

                # Build a temporary working set with normalized titles and numeric fields
                await conn.execute(
                    f"""
                    CREATE TEMP TABLE hp_titles AS
                    SELECT
                        package_id,
                        hearing_id,
                        CASE
                            WHEN trim(COALESCE(hearing_number, '')) ~ '^[0-9]+$' THEN trim(hearing_number)::bigint
                            ELSE NULL
                        END AS hearing_number,
                        LOWER(COALESCE(title, '')) AS norm_title,
                        -- Normalize a series key by splitting at 'part/volume/vol/book' tokens
                        -- followed by a number, roman numeral, or single letter, and taking the prefix
                        trim(
                            regexp_replace(
                                LOWER(COALESCE(title, '')),
                                '\\b(?:part|volume|vol|book)\\s*(?:[0-9]+|[ivxlcdm]+|[a-z])\\b.*$',
                                '', 'gi'
                            )
                        ) AS series_key,
                        LOWER(COALESCE(hearing_type, '')) AS hearing_type,
                        COALESCE(congress, 0) AS congress,
                        CASE
                            WHEN part_number::text ~ '^[0-9]+' THEN (regexp_match(part_number::text, '^[0-9]+'))[1]::int
                            ELSE NULL
                        END AS part_num_int
                        FROM {self.production_schema}.hearingpackages
                    WHERE title IS NOT NULL AND hearing_id IS NOT NULL
                    """
                )
                await conn.execute(
                    "CREATE INDEX ON hp_titles (congress, hearing_number)"
                )
                await conn.execute("CREATE INDEX ON hp_titles (package_id)")
                if pg_trgm_available:
                    await conn.execute(
                        "CREATE INDEX hp_titles_norm_title_trgm_idx ON hp_titles USING GIN (norm_title gin_trgm_ops)"
                    )
                await conn.execute(
                    "CREATE INDEX ON hp_titles (congress, series_key, hearing_type)"
                )

                total_updated = 0

                # Step 5A: Exact-title clusters among part_number=1 within same congress
                logger.info("Grouping exact-title series for part_number=1")
                res = await conn.execute(
                    f"""
                    WITH p1 AS (
                        SELECT * FROM hp_titles WHERE part_num_int = 1
                    ),
                    clusters AS (
                        SELECT norm_title, congress, COUNT(*) AS cnt, MIN(hearing_number) AS min_hearing_number
                        FROM p1
                        GROUP BY norm_title, congress
                        HAVING COUNT(*) > 1
                    ),
                    earliest AS (
                        SELECT p.norm_title, p.congress, p.hearing_number, p.hearing_id,
                               p.hearing_type || p.hearing_number || '-' || p.congress AS hearing_set_id
                        FROM p1 p
                        JOIN clusters c USING (norm_title, congress)
                        WHERE p.hearing_number = c.min_hearing_number
                    ),
                    updates AS (
                        SELECT p.package_id, e.hearing_set_id AS new_hearing_set_id
                        FROM p1 p
                        JOIN earliest e USING (norm_title, congress)
                        WHERE p.hearing_id <> e.hearing_id
                    )
                    UPDATE {self.production_schema}.hearingpackages hp
                    SET hearing_set_id = u.new_hearing_set_id
                    FROM updates u
                    WHERE hp.package_id = u.package_id
                    """
                )
                affected = 0
                try:
                    parts = res.split()
                    if len(parts) == 2:
                        affected = int(parts[1])
                except Exception:
                    affected = 0
                total_updated += affected
                logger.info(f"Exact-title grouping updated {affected} rows")

                # Step 5B (revised): Series grouping using series_key within same congress and type
                logger.info("Grouping series by series_key for part_number=1 parents")
                res = await conn.execute(
                    f"""
                    WITH p1 AS (
                        SELECT package_id, series_key, congress, hearing_type, hearing_number, hearing_id
                        FROM hp_titles
                        WHERE part_num_int = 1 AND series_key <> ''
                    ),
                    groups AS (
                        SELECT series_key, congress, hearing_type, MIN(hearing_number) AS min_hearing_number
                        FROM p1
                        GROUP BY series_key, congress, hearing_type
                        HAVING COUNT(*) > 1 OR MIN(hearing_number) IS NOT NULL
                    ),
                    earliest AS (
                        SELECT p.series_key, p.congress, p.hearing_type, p.hearing_number,
                               p.hearing_id,
                               p.hearing_type || p.hearing_number || '-' || p.congress AS hearing_set_id
                        FROM p1 p
                        JOIN groups g USING (series_key, congress, hearing_type)
                        WHERE p.hearing_number = g.min_hearing_number
                    ),
                    updates AS (
                        SELECT hp.package_id, e.hearing_set_id AS new_hearing_set_id
                        FROM {self.production_schema}.hearingpackages hp
                        JOIN hp_titles t USING (package_id)
                        JOIN earliest e
                          ON e.series_key = t.series_key
                         AND e.congress = t.congress
                         AND e.hearing_type = t.hearing_type
                        WHERE COALESCE(hp.hearing_set_id, hp.hearing_id) <> e.hearing_set_id
                    )
                    UPDATE {self.production_schema}.hearingpackages hp
                    SET hearing_set_id = u.new_hearing_set_id
                    FROM updates u
                    WHERE hp.package_id = u.package_id
                    """
                )
                affected = 0
                try:
                    parts = res.split()
                    if len(parts) == 2:
                        affected = int(parts[1])
                except Exception:
                    affected = 0
                total_updated += affected
                logger.info(f"Series-key grouping updated {affected} rows")

                # Step 5C: For part_number>1 with unique hearing_number, map to earliest similar part_number=1 in same congress
                logger.info(
                    "Linking unique-hearing_number part>1 hearings to earliest similar part_number=1 within congress"
                )
                # Step 5C: For part_number>1 with unique hearing_number, map to earliest part_number=1 with same series_key
                logger.info(
                    "Linking unique-hearing_number part>1 hearings to earliest part_number=1 within congress by series_key"
                )
                res = await conn.execute(
                    f"""
                    WITH all_rows AS (
                        SELECT * FROM hp_titles
                    ),
                    p1 AS (
                        SELECT * FROM all_rows WHERE part_num_int = 1 AND series_key <> ''
                    ),
                    hearing_number_multi AS (
                        SELECT * FROM all_rows WHERE part_num_int > 1 AND series_key <> ''
                    ),
                    hearing_number_counts AS (
                        SELECT hearing_number, COUNT(DISTINCT package_id) AS cnt FROM all_rows GROUP BY hearing_number
                    ),
                    unique_hearing_number_multi AS (
                        SELECT m.* FROM hearing_number_multi m JOIN hearing_number_counts hnc USING (hearing_number) WHERE hnc.cnt = 1
                    ),
                    hearing_number_candidates AS (
                        SELECT u.package_id, u.congress, MIN(p1.hearing_number) AS min_hearing_number
                        FROM unique_hearing_number_multi u
                        JOIN p1 ON p1.congress = u.congress AND p1.series_key = u.series_key AND p1.hearing_type = u.hearing_type
                        GROUP BY u.package_id, u.congress
                    ),
                    earliest AS (
                        SELECT c.package_id,
                               e.hearing_type || e.hearing_number || '-' || e.congress AS new_hearing_set_id
                        FROM hearing_number_candidates c
                        JOIN p1 e ON e.congress = c.congress AND e.hearing_number = c.min_hearing_number
                    ),
                    updates AS (
                        SELECT hp.package_id, earliest.new_hearing_set_id
                        FROM {self.production_schema}.hearingpackages hp
                        JOIN earliest ON hp.package_id = earliest.package_id
                        WHERE COALESCE(hp.hearing_set_id, hp.hearing_id) <> earliest.new_hearing_set_id
                    )
                    UPDATE {self.production_schema}.hearingpackages hp
                    SET hearing_set_id = u.new_hearing_set_id
                    FROM updates u
                    WHERE hp.package_id = u.package_id
                    """
                )
                affected = 0
                try:
                    parts = res.split()
                    if len(parts) == 2:
                        affected = int(parts[1])
                except Exception:
                    affected = 0
                total_updated += affected
                logger.info(
                    f"Unique-hearing_number part>1 linking updated {affected} rows"
                )

                results["operations"].append(
                    {
                        "name": "group_series_by_fuzzy_title_hearing_number",
                        "status": "success",
                        "rows_affected": total_updated,
                    }
                )
                results["rows_affected"] += total_updated
        except Exception as e:
            logger.error(f"Error grouping series by titles: {e}", exc_info=True)
            results["operations"].append(
                {
                    "name": "group_series_by_fuzzy_title_hearing_number",
                    "status": "error",
                    "error": str(e),
                }
            )
            results["status"] = "partial_failure"

        # ? Operation 6: Mark errata based on title/granule_id tokens (exclude addendum)
        logger.info("Starting errata flag backfill from title/granule_id")
        try:
            async with self.db_pool.acquire() as conn, conn.transaction():
                update_sql = f"""
                    UPDATE {self.production_schema}.hearingpackages
                    SET is_errata = TRUE,
                    hearing_id = split_part(hearing_id, '-', 1) || '-' || split_part(hearing_id, '-', 2) || 'e-' || split_part(hearing_id, '-', 3)
                    WHERE COALESCE(is_errata, FALSE) = FALSE
                      AND (
                        title ILIKE '%errata%'
                        OR granule_id ILIKE '%-err%'
                      )
                      AND COALESCE(title, '') NOT ILIKE '%addendum%'
                      AND granule_id LIKE 'CHRG%'
                """
                result = await conn.execute(update_sql)
                affected = 0
                try:
                    parts = result.split()
                    if len(parts) == 2:
                        affected = int(parts[1])
                except Exception:
                    affected = 0
                logger.info(f"Errata backfill updated {affected} rows")
                results["operations"].append(
                    {
                        "name": "mark_errata_from_tokens",
                        "status": "success",
                        "rows_affected": affected,
                    }
                )
                results["rows_affected"] += affected
        except Exception as e:
            logger.error(f"Error backfilling errata: {e}", exc_info=True)
            results["operations"].append(
                {
                    "name": "mark_errata_from_tokens",
                    "status": "error",
                    "error": str(e),
                }
            )
            results["status"] = "partial_failure"

        # ? Operation 7: Create su_doc_item_number table from hearing_id
        logger.info("Starting su_doc_item_number post-processing")
        try:
            async with self.db_pool.acquire() as conn, conn.transaction():
                # Fetch all records from staging
                fetch_sql = f"""
                    SELECT package_id, otheridentifier_sudoc_item_number
                    FROM {self.staging_schema}.hearingpackages
                """
                rows = await conn.fetch(fetch_sql)
                logger.info(f"Found {len(rows)} records with su_doc_item_number data")

                # Prepare batch params
                insert_sql = f"""
                    INSERT INTO {self.production_schema}.hearingpackages_su_doc_item_number (package_id, su_doc_item_number)
                    VALUES ($1, $2)
                    ON CONFLICT DO NOTHING
                """
                batch_params = []
                for row in rows:
                    su_doc_item_number_raw = row["otheridentifier_sudoc_item_number"]
                    if not su_doc_item_number_raw:
                        continue
                    try:
                        if isinstance(su_doc_item_number_raw, str):
                            su_doc_item_numbers = json.loads(su_doc_item_number_raw)
                        elif isinstance(su_doc_item_number_raw, list):
                            su_doc_item_numbers = su_doc_item_number_raw
                        elif isinstance(su_doc_item_number_raw, int | float):
                            # Handle case where it's a single integer/float
                            su_doc_item_numbers = [str(su_doc_item_number_raw)]
                        else:
                            su_doc_item_numbers = [str(su_doc_item_number_raw)]
                    except json.JSONDecodeError:
                        su_doc_item_numbers = [str(su_doc_item_number_raw)]

                    # Ensure su_doc_item_numbers is always a list
                    if not isinstance(su_doc_item_numbers, list):
                        su_doc_item_numbers = [str(su_doc_item_numbers)]

                    for su_doc_item_number in su_doc_item_numbers:
                        if su_doc_item_number:
                            batch_params.append(
                                (row["package_id"], str(su_doc_item_number))
                            )

                total_inserted = 0
                if batch_params:
                    chunk_size = 1000
                    for i in range(0, len(batch_params), chunk_size):
                        chunk = batch_params[i : i + chunk_size]
                        await conn.executemany(insert_sql, chunk)
                        total_inserted += len(chunk)

                logger.info(f"Inserted {total_inserted} su_doc_item_number records")
                results["operations"].append(
                    {
                        "name": "populate_su_doc_item_number_field",
                        "status": "success",
                        "rows_affected": total_inserted,
                    }
                )
                results["rows_affected"] += total_inserted

        except Exception as e:
            logger.error(
                f"Error populating su_doc_item_number field: {e}", exc_info=True
            )
            results["operations"].append(
                {
                    "name": "populate_su_doc_item_number_field",
                    "status": "error",
                    "error": str(e),
                }
            )
            results["status"] = "partial_failure"
