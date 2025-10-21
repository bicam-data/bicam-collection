"""
Print Packages Custom Plugin Logic

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
    PRINT_PACKAGE_JACKETNUMBERS,
    PRINT_PACKAGE_PART_FIXES,
)
from streamlined.plugins.base import BaseCleanerLogic

logger = logging.getLogger(__name__)


class PrintpackagesFetcherLogic:
    """
    Hearing Packages fetcherogic that extends the base GovInfo fetcher logic.

    This class provides custom methods for fetching hearing packages data, including:
    - Extracting standardized hearing packages IDs
    - Handling related data from URLs
    - Implementing specialized fetching logic for hearing packages-specific data

    Inherits shared functionality like get_generic_related_data from
    GovInfoBaseFetcher.
    """

    def __init__(self, data_type: str = "printpackages"):
        self.data_type = data_type


class PrintpackagesCleanerLogic(BaseCleanerLogic):
    """
    Hearing Packages cleaner logic extracted from HearingPackagesCleaner class.
    Contains all the custom cleaning methods for hearing packages data.
    """

    # TODO: create ils_system_id table from printpackages
    # TODO: reference_bills (granules)
    # TODO: other references (all 3: uscode, statute, plaw)
    # TODO: committees (granules and packages)
    # TODO: agencies (granules)
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

        # Set bills-specific multi-table processing configuration
        self.multi_table_data_types = {
            "printpackages_committees": [
                "printpackages_committees",
                "printpackages_granules_committees",
            ],
            "printpackages": [
                "printpackages",
                "printpackages_granules",
            ],
            "printpackages_reference_bills": [
                "printpackages_granules_references",
                "printpackages_granules_references_contents",
            ],
            "printpackages_agencies": [
                "printpackages_granules",
                "printpackages_granules_agencies",
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

            # if "errata" in title, return True
            title_val = cleaned.get("title")
            if title_val is not None and "errata" in str(title_val).strip().lower():
                return True

        except Exception:
            # Non-fatal; treat as non-errata if parsing fails
            pass
        return False

    def _extract_part_from_gpo_granule(self, granuleid: str) -> int:
        """Extract part number from GPO granule ID like 'GPO-CHRG-ROBERTS-3-3-5'."""
        try:
            # Handle GPO-CHRG-ROBERTS special case
            if "GPO-CHRG-ROBERTS" in granuleid:
                # Extract numeric parts from granule ID like "GPO-CHRG-ROBERTS-3-3-5"
                parts = granuleid.split("-")
                if len(parts) >= 4:
                    # Convert to numeric for sorting: 3-3-5 -> 3003005
                    a = int(parts[2]) if parts[2].isdigit() else 0
                    b = int(parts[3]) if parts[3].isdigit() else 0
                    c = int(parts[4]) if len(parts) > 4 and parts[4].isdigit() else 0
                    # Return the combined numeric value as the part number
                    return a * 1000000 + b * 1000 + c
        except (ValueError, IndexError):
            pass
        return 1  # Default fallback

    def _build_special_print_identity(
        self, cleaned: dict[str, Any], packageid: str
    ) -> tuple[str, str | None, str | None, int, str, str, str]:
        """
        Build print identity for special GOVPUB packages.

        For GOVPUB packages like "GOVPUB-Y1_2-PURL-gpo71122" or "GOVPUB-Y4_SE2-PURL-LPS4204":
        - Calculate congress from dateissued using [(year - 1789) // 2] + 1
        - Map governmentauthor2 to hearing type (hhrg/shrg/jhrg/xhrg)
        - Extract hearing number from gpo or LPS patterns
        - Use manual overrides for packages without gpo/LPS numbers

        Returns tuple:
        - hearing_id (hearing_id for hearings)
        - parent_hearing_id (nullable)
        - granule_id (nullable)
        - part_number (int) - always 1 for GOVPUB
        - hearing_type (str) - hhrg/shrg/jhrg/xhrg
        - hearing_number (str) - hearing number
        - congress (str)
        """

        # Calculate congress from dateissued (fallback for other special prints)
        congress = cleaned.get("congress")
        if not congress:
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

        # Special PLUMBOOK handling
        pkg_upper = packageid.upper()
        granuleid = cleaned.get("granuleid") or cleaned.get("granule_id")
        if pkg_upper.startswith("GPO-PLUMBOOK") or (
            isinstance(granuleid, str) and granuleid.upper().startswith("GPO-PLUMBOOK")
        ):
            print_type = cleaned.get("docclass", "").lower()
            orig_pkg_upper = str(cleaned.get("packageid") or packageid).upper()
            manual_val = PRINT_PACKAGE_JACKETNUMBERS.get(
                orig_pkg_upper
            ) or PRINT_PACKAGE_JACKETNUMBERS.get(pkg_upper)
            if isinstance(manual_val, dict):
                print_number = manual_val.get("print_number", "0")
                if not cleaned.get("congress") and manual_val.get("congress"):
                    congress = manual_val["congress"]
            else:
                print_number = manual_val or "0"

            # Use placeholder parts for granules; packages use part 0
            is_package_row = not granuleid or str(granuleid) == packageid
            part_token = "0" if is_package_row else "x"
            part_number = 0 if is_package_row else None
            print_id = f"{print_type}{print_number}-{part_token}-{congress}"
            # Print set id has no part token
            print_set_id = f"{print_type}{print_number}-{congress}"
            return (
                print_id,
                print_set_id,
                str(granuleid) if granuleid else None,
                part_number,
                print_type,
                print_number,
                congress,
            )

        # Special J6 handling
        if "J6" in pkg_upper or (
            isinstance(granuleid, str) and "J6" in str(granuleid).upper()
        ):
            print_type = "hprt"
            manual_val = PRINT_PACKAGE_JACKETNUMBERS.get(
                packageid
            ) or PRINT_PACKAGE_JACKETNUMBERS.get(pkg_upper)
            if isinstance(manual_val, dict) and manual_val.get("congress"):
                congress = manual_val["congress"]
            else:
                congress = cleaned.get("congress", congress)
            # Extract digit runs after -, _, or . with optional letters in between
            # Only consider digits after the first occurrence of 'J6'
            idx = packageid.upper().find("J6")
            tail = packageid[idx + 2 :] if idx >= 0 else packageid
            tokens = re.findall(r"[-_.][A-Za-z]*([0-9]+)", tail)
            if tokens:
                parts: list[str] = []
                for tok in tokens:
                    stripped = tok.lstrip("0")
                    parts.append(stripped if stripped else "0")
                print_number = "".join(parts)
            else:
                if isinstance(manual_val, dict):
                    print_number = manual_val.get("print_number") or "0"
                else:
                    print_number = (
                        PRINT_PACKAGE_JACKETNUMBERS.get(packageid)
                        or PRINT_PACKAGE_JACKETNUMBERS.get(pkg_upper)
                        or "0"
                    )

            is_package_row = not granuleid or str(granuleid) == packageid
            part_token = "0" if is_package_row else "x"
            part_number = 0 if is_package_row else 1

            print_id = f"{print_type}{print_number}-{part_token}-{congress}"
            print_set_id = f"{print_type}{print_number}-{congress}"
            return (
                print_id,
                print_set_id,
                str(granuleid) if granuleid else None,
                part_number,
                print_type,
                print_number,
                congress,
            )

        # Special JCS handling
        id_for_jcs = f"{packageid}-{granuleid}" if granuleid else packageid
        if re.search(r"JCS", id_for_jcs, re.IGNORECASE):
            print_type = "jprt"
            # Extract two number tokens after JCS and concatenate
            m = re.search(
                r"JCS[^0-9]*([0-9]+)[^0-9]+([0-9]+)", id_for_jcs, re.IGNORECASE
            )
            if m:
                # Only strip leading zeros from the first token; preserve zeros on the second
                n1 = m.group(1).lstrip("0") or "0"
                n2 = m.group(2)
                print_number = f"{n1}{n2}"
            else:
                manual_val = PRINT_PACKAGE_JACKETNUMBERS.get(
                    packageid
                ) or PRINT_PACKAGE_JACKETNUMBERS.get(pkg_upper)
                if isinstance(manual_val, dict):
                    print_number = manual_val.get("print_number") or "0"
                else:
                    print_number = manual_val or "0"
            is_package_row = not granuleid or str(granuleid) == packageid
            part_token = "0" if is_package_row else "x"
            part_number = 0 if is_package_row else 1
            print_id = f"{print_type}{print_number}-{part_token}-{congress}"
            print_set_id = f"{print_type}{print_number}-{congress}"
            return (
                print_id,
                print_set_id,
                str(granuleid) if granuleid else None,
                part_number,
                print_type,
                print_number,
                congress,
            )

        # Special WMC Medicare 1997 handling
        if packageid == "GPO-WMC-MEDICARE-1997" or (
            isinstance(granuleid, str)
            and str(granuleid).startswith("GPO-WMC-MEDICARE-1997")
        ):
            print_type = "wprt"
            print_number = PRINT_PACKAGE_JACKETNUMBERS.get("GPO-WMC-MEDICARE-1997", "0")
            if not granuleid or str(granuleid) == packageid:
                part_number = 0
                part_token = "0"
            else:
                # Parse last numeric token after '-' from granuleid
                m = re.search(r"-(\d+)$", str(granuleid))
                pnum = int(m.group(1)) if m else 1
                part_number = pnum
                part_token = str(pnum)
            print_id = f"{print_type}{print_number}-{part_token}-{congress}"
            print_set_id = f"{print_type}{print_number}-{congress}"
            return (
                print_id,
                print_set_id,
                str(granuleid) if granuleid else None,
                part_number,
                print_type,
                print_number,
                congress,
            )

        # If none of the special patterns matched, fail explicitly
        raise ValueError(
            f"Unrecognized special print package pattern for package '{packageid}'"
        )

    def _build_print_identity(
        self, cleaned: dict[str, Any]
    ) -> tuple[str, str | None, str | None, int, str, str, str]:
        """
        Build canonical identity. If the record is a GovInfo print (CHRG-*), build a print identity
        that captures part/volume/book with errata and placeholder handling.

        Returns tuple:
        - print_id (print_id for prints)
        - print_set_id (nullable, no part token)
        - granule_id (nullable)
        - part_number (int)
        - print_type (str)  # hhrg/shrg/jhrg for prints
        - print_number (str)  # print number for prints
        - congress (str)
        """
        packageid = str(cleaned.get("packageid") or cleaned.get("package_id") or "")
        granuleid = cleaned.get("granuleid") or cleaned.get("granule_id")

        # Special handling for J6/JCS style packages
        if packageid.upper().startswith("GPO") and any(
            term in packageid.upper() for term in ("J6", "JCS")
        ):
            return self._build_special_print_identity(cleaned, packageid)

        # Special handling for GPO- prefixed packages: defer to special builder for PLUMBOOK and WMC
        if packageid.upper().startswith("GPO-"):
            upper_pkg = packageid.upper()
            if upper_pkg.startswith(("GPO-PLUMBOOK", "GPO-WMC-")):
                return self._build_special_print_identity(cleaned, packageid)
            if any(char.isdigit() for char in packageid.split("-")[2]):
                packageid = packageid[4:]
            else:
                return self._build_special_print_identity(cleaned, packageid)

        # Print path: CPRT-<congress><chamber>prt<print_number>...
        m = re.match(r"^CPRT-(\d{2,3})([hsjw])prt(\d+)", packageid, re.IGNORECASE)
        if m:
            congress = m.group(1)
            chamber_char = m.group(2).lower()
            print_number_digits = m.group(3)  # digits-only jacket number
            print_type = f"{chamber_char}prt"  # hprt/sprt/jprt/wprt
            print_number = print_number_digits

            # Part determination: packages use part 0; granules use parsed or placeholder
            is_package_row = not granuleid or str(granuleid) == packageid
            # Treat volume/part-like suffixes as granule rows even if equal to packageid
            if granuleid and str(granuleid) == packageid:
                try:
                    if (
                        re.search(
                            r"v(?:\d+|[ivxlcdmIVXLCDM]+)$", packageid, re.IGNORECASE
                        )
                        or re.search(
                            r"v(one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|thirteen|fourteen|fifteen|sixteen|seventeen|eighteen|nineteen|twenty)$",
                            packageid,
                            re.IGNORECASE,
                        )
                        or re.search(r"p\d+$", packageid, re.IGNORECASE)
                        or re.search(r"p([ivxlcdmIVXLCDM]+)$", packageid)
                        or re.search(
                            r"p(one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|thirteen|fourteen|fifteen|sixteen|seventeen|eighteen|nineteen|twenty)$",
                            packageid,
                            re.IGNORECASE,
                        )
                        or re.search(r"Ob(?:\d+|[ivxlcdmIVXLCDM]+)$", packageid)
                        or re.search(
                            r"Ob(one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|thirteen|fourteen|fifteen|sixteen|seventeen|eighteen|nineteen|twenty)$",
                            packageid,
                            re.IGNORECASE,
                        )
                        or re.search(r"b(?:\d+|[ivxlcdmIVXLCDM]+)$", packageid)
                        or re.search(
                            r"b(one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|thirteen|fourteen|fifteen|sixteen|seventeen|eighteen|nineteen|twenty)$",
                            packageid,
                            re.IGNORECASE,
                        )
                    ):
                        is_package_row = False
                except Exception:
                    pass
            part_number_val = (
                0 if is_package_row else (self.parse_part_from_fields(cleaned) or 1)
            )
            part_token_base: str = (
                "0"
                if is_package_row
                else (
                    "x"
                    if self._should_use_placeholder_part(cleaned, packageid)
                    else str(part_number_val)
                )
            )
            is_errata = self._detect_is_errata(cleaned)
            part_token = f"{part_token_base}{'e' if is_errata else ''}"

            print_id = f"{print_type}{print_number}-{part_token}-{congress}"
            # Print set id has no part token
            print_set_id = f"{print_type}{print_number}-{congress}"

            return (
                print_id,
                print_set_id,
                str(granuleid) if granuleid else None,
                int(part_number_val),
                print_type,
                print_number,
                congress,
            )

        # Manual fixes fallback: handle malformed CPRT packageids like 'CPRT-105HPRTnull'
        manual_print_val = PRINT_PACKAGE_JACKETNUMBERS.get(
            packageid
        ) or PRINT_PACKAGE_JACKETNUMBERS.get(packageid.upper())
        if manual_print_val:
            # Try to get congress from id or cleaned
            cong_match = re.match(r"^CPRT-(\d{2,3})", packageid, re.IGNORECASE)
            if isinstance(manual_print_val, dict) and manual_print_val.get("congress"):
                congress = manual_print_val["congress"]
            else:
                congress = (
                    cong_match.group(1)
                    if cong_match
                    else str(cleaned.get("congress") or "0")
                )
            # Determine print type from id or docclass
            type_match = re.search(r"([hsjw])prt", packageid, re.IGNORECASE)
            if type_match:
                chamber_char = type_match.group(1).lower()
                print_type = f"{chamber_char}prt"
            else:
                print_type = str(cleaned.get("docclass") or "hprt").lower()

            # Determine part using existing heuristics (packages part=0)
            is_package_row = not granuleid or str(granuleid) == packageid
            part_number_val = (
                0 if is_package_row else (self.parse_part_from_fields(cleaned) or 1)
            )
            part_token_base: str = (
                "0"
                if is_package_row
                else (
                    "x"
                    if self._should_use_placeholder_part(cleaned, packageid)
                    else str(part_number_val)
                )
            )
            is_errata = self._detect_is_errata(cleaned)
            part_token = f"{part_token_base}{'e' if is_errata else ''}"

            manual_print_number = (
                manual_print_val.get("print_number")
                if isinstance(manual_print_val, dict)
                else str(manual_print_val)
            )
            print_id = f"{print_type}{manual_print_number}-{part_token}-{congress}"
            print_set_id = f"{print_type}{manual_print_number}-{congress}"

            return (
                print_id,
                print_set_id,
                str(granuleid) if granuleid else None,
                int(part_number_val),
                print_type,
                str(manual_print_number),
                congress,
            )

        # Fallback: try special identity builder (e.g., PLUMBOOK/J6/JCS/WMC)
        return self._build_special_print_identity(cleaned, packageid)

    def _should_use_placeholder_part(
        self, cleaned: dict[str, Any], packageid: str | None
    ) -> bool:
        """Decide whether to emit a placeholder part token ('x') for this record.

        Rules:
        - If the heading suggests multiple counting systems, use placeholder
        - Force placeholder for Section/Appendix granules to resolve later by hierarchy
        - Otherwise do not force placeholder
        """
        try:
            # 1) Complex headings → placeholder
            heading = cleaned.get("heading")
            if (
                isinstance(heading, str)
                and heading.strip()
                and self._has_two_keywords_or_mixed_counts(heading)
            ):
                return True

            # 2) Granules that are Sections/Appendix (or similar) should be placeholders
            #    so they can be ordered after pure numeric parts during post-processing.
            #    Check granule id and heading text.
            text_candidates: list[str] = []
            for key in ("granuleid", "granule_id", "heading", "title"):
                val = cleaned.get(key)
                if isinstance(val, str) and val:
                    text_candidates.append(val.lower())

            text_blob = " \n ".join(text_candidates)
            if text_blob and re.search(
                r"\b(section|appendix)\b", text_blob, re.IGNORECASE
            ):
                return True

            # 3) Multi-token numeric suffix relative to package → placeholder
            pkg_id_for_suffix = cleaned.get("packageid") or cleaned.get("package_id")
            gran_id_for_suffix = cleaned.get("granuleid") or cleaned.get("granule_id")
            if (
                pkg_id_for_suffix
                and gran_id_for_suffix
                and str(pkg_id_for_suffix) != str(gran_id_for_suffix)
            ):
                suffix_tokens = self._suffix_tokens_from_ids(
                    str(pkg_id_for_suffix), str(gran_id_for_suffix)
                )
                if suffix_tokens:
                    numeric_suffix_tokens = [t for t in suffix_tokens if t.isdigit()]
                    if len(suffix_tokens) > 1 or len(numeric_suffix_tokens) > 1:
                        return True

            return False
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

    def number_word_to_int(self, token: str) -> int | None:
        """Map number words like 'one', 'two', 'twenty' to integers."""
        if not token:
            return None
        words = {
            "one": 1,
            "two": 2,
            "three": 3,
            "four": 4,
            "five": 5,
            "six": 6,
            "seven": 7,
            "eight": 8,
            "nine": 9,
            "ten": 10,
            "eleven": 11,
            "twelve": 12,
            "thirteen": 13,
            "fourteen": 14,
            "fifteen": 15,
            "sixteen": 16,
            "seventeen": 17,
            "eighteen": 18,
            "nineteen": 19,
            "twenty": 20,
        }
        return words.get(token.strip().lower())

    def _has_two_keywords_or_mixed_counts(self, heading: str) -> bool:
        """Return True if heading contains two or more key tokens or two different counting types.

        Keywords: part, volume, vol, book, section, division, chapter, appendix
        Counting types: digits, roman numerals, single letters, number words
        """
        if not heading or not isinstance(heading, str):
            return False
        text = heading.lower()
        # Count keywords
        keywords = [
            "part",
            "volume",
            "vol",
            "book",
            "section",
            "division",
            "chapter",
            "appendix",
        ]
        keyword_hits = 0
        for kw in keywords:
            if re.search(rf"\b{re.escape(kw)}\b", text, re.IGNORECASE):
                keyword_hits += 1
                if keyword_hits >= 2:
                    return True

        # Detect counting methods
        methods: set[str] = set()
        if re.search(r"\b\d+\b", text):
            methods.add("digits")
        # Roman numerals as whole tokens
        if re.search(r"\b[ivxlcdm]+\b", text, re.IGNORECASE):
            methods.add("roman")
        # Single letter tokens (A, B, C)
        if re.search(r"\b[a-zA-Z]\b", text):
            methods.add("letter")
        # Number words
        if re.search(
            r"\b(one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|thirteen|fourteen|fifteen|sixteen|seventeen|eighteen|nineteen|twenty)\b",
            text,
            re.IGNORECASE,
        ):
            methods.add("word")

        return len(methods) >= 2

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

    def _suffix_tokens_from_ids(
        self, packageid: str | None, granuleid: str | None
    ) -> list[str]:
        """Return tokens in granuleid after removing the longest common token-prefix with packageid.

        - Splits on '-'
        - Case-insensitive comparison
        - Returns remaining granule tokens (may be empty)
        """
        if not packageid or not granuleid:
            return []
        try:
            p = str(packageid)
            g = str(granuleid)
            p_tokens = [t for t in p.split("-") if t != ""]
            g_tokens = [t for t in g.split("-") if t != ""]
            i = 0
            while i < len(p_tokens) and i < len(g_tokens):
                if p_tokens[i].lower() != g_tokens[i].lower():
                    break
                i += 1
            return g_tokens[i:]
        except Exception:
            return []

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
                    return 1
                return alpha_val
            # Try Roman numeral
            roman_val = self.roman_to_int(part_str)
            if roman_val is not None:
                if roman_val > 100:
                    logger.warning(
                        f"\nPart number {part_str} (roman={roman_val}) > 100 for granule_id {cleaned.get('granuleid')}, replacing with 1\n"
                    )
                    return 1
                return roman_val
            # Try alphanumeric parsing
            main, _sub = self._parse_alphanumeric_part_token(part_str)
            if main is not None:
                if main > 100:
                    logger.warning(
                        f"Part number {part_str} (parsed={main}) > 100 for granule_id {cleaned.get('granuleid')}, replacing with 1"
                    )
                    return 1
                return main
            # Try direct integer conversion
            if part_str.isdigit():
                int_val = int(part_str)
                if int_val > 100:
                    logger.warning(
                        f"Part number {part_str} (int={int_val}) > 100 for granule_id {cleaned.get('granuleid')}, replacing with 1"
                    )
                    return 1
                return int_val

        # 1B) Derive part from granule suffix relative to package (e.g., '...-b-10-3-2' -> 10)
        pkg_id_for_suffix = cleaned.get("packageid") or cleaned.get("package_id")
        gran_id_for_suffix = cleaned.get("granuleid") or cleaned.get("granule_id")
        if (
            pkg_id_for_suffix
            and gran_id_for_suffix
            and str(pkg_id_for_suffix) != str(gran_id_for_suffix)
        ):
            suffix_tokens = self._suffix_tokens_from_ids(
                str(pkg_id_for_suffix), str(gran_id_for_suffix)
            )
            if suffix_tokens:
                numeric_suffix_tokens = [tok for tok in suffix_tokens if tok.isdigit()]
                # Only derive part directly when suffix is a single numeric token
                if len(suffix_tokens) == 1 and len(numeric_suffix_tokens) == 1:
                    val = int(numeric_suffix_tokens[0])
                    if 1 <= val <= 100:
                        return val
                    # Cap extremes to 1 to avoid garbage values
                    logger.warning(
                        f"Suffix-derived part {val} out of range for granule_id {gran_id_for_suffix}, using 1"
                    )
                    return 1

        # 2) From heading (printpackages: consider multiple keywords)
        heading = cleaned.get("heading")
        if isinstance(heading, str) and heading.strip():
            # Tokens: part, volume/vol, book, section, division, chapter, appendix
            patterns = [
                r"\bpart\s+([A-Za-z0-9()\-]+)",
                r"\bvol(?:ume)?\s+([A-Za-z0-9()\-]+)",
                r"\bbook\s+([A-Za-z0-9()\-]+)",
                r"\bsection\s+([A-Za-z0-9()\-]+)",
                r"\bdivision\s+([A-Za-z0-9()\-]+)",
                r"\bchapter\s+([A-Za-z0-9()\-]+)",
                r"\bappendix\s+([A-Za-z0-9()\-]+)",
            ]
            for pat in patterns:
                m = re.search(pat, heading, re.IGNORECASE)
                if not m:
                    continue
                token = m.group(1).strip()
                # Mixed alphanumeric tokens -> defer to placeholder resolution
                if self._has_mixed_alnum(token):
                    return 1
                # Try number words
                word_val = self.number_word_to_int(token)
                if word_val is not None:
                    return word_val
                # Try alphabetic
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
                return 1
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

        # p<digits> possibly followed by volume/book tokens (no dash) OR embedded at end (e.g., ...Op1, ...p2)
        # But not hrg, shrg, jhrg patterns
        if not re.search(r"[hsj]hrg\d+", candidate, re.IGNORECASE):
            # Strict p<digits>
            m = re.search(r"p(\d+)(?:v\d+)?(?:b\d+)?$", candidate, re.IGNORECASE)
            if m:
                return int(m.group(1))
            # Ob<roman|digits> or b<roman|digits> or Op<digits>
            m = re.search(r"Ob(\d+|[ivxlcdmIVXLCDM]+)$", candidate)
            if m:
                tok = m.group(1)
                if tok.isdigit():
                    return int(tok)
                rv = self.roman_to_int(tok)
                if rv is not None:
                    return rv
            m = re.search(r"b(\d+|[ivxlcdmIVXLCDM]+)$", candidate)
            if m:
                tok = m.group(1)
                if tok.isdigit():
                    return int(tok)
                rv = self.roman_to_int(tok)
                if rv is not None:
                    return rv
            m = re.search(r"Op(\d+)$", candidate)
            if m:
                return int(m.group(1))
            # Word forms: vONE, pTWO, ObTHREE, bFOUR ...
            m = re.search(
                r"v(one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|thirteen|fourteen|fifteen|sixteen|seventeen|eighteen|nineteen|twenty)$",
                candidate,
                re.IGNORECASE,
            )
            if m:
                return self.number_word_to_int(m.group(1)) or 1
            m = re.search(
                r"p(one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|thirteen|fourteen|fifteen|sixteen|seventeen|eighteen|nineteen|twenty)$",
                candidate,
                re.IGNORECASE,
            )
            if m:
                return self.number_word_to_int(m.group(1)) or 1
            m = re.search(
                r"Ob(one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|thirteen|fourteen|fifteen|sixteen|seventeen|eighteen|nineteen|twenty)$",
                candidate,
                re.IGNORECASE,
            )
            if m:
                return self.number_word_to_int(m.group(1)) or 1
            m = re.search(
                r"b(one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|thirteen|fourteen|fifteen|sixteen|seventeen|eighteen|nineteen|twenty)$",
                candidate,
                re.IGNORECASE,
            )
            if m:
                return self.number_word_to_int(m.group(1)) or 1
            # Roman p<roman> at end (e.g., pI, pX)
            m = re.search(r"p([ivxlcdmIVXLCDM]+)$", candidate)
            if m:
                rv = self.roman_to_int(m.group(1))
                if rv is not None:
                    return rv

        # Compact volume suffix at end of id (treat as part when present)
        m = re.search(r"v(\d+)$", candidate, re.IGNORECASE)
        if m:
            return int(m.group(1))

        m = re.search(r"v([ivxlcdmIVXLCDM]+)$", candidate)
        if m:
            rv = self.roman_to_int(m.group(1))
            if rv is not None:
                return rv

        # Roman part after '-pt' or in heading-like id
        m = re.search(r"[-_]pt([ivxlcdmIVXLCDM]+)$", candidate)
        if m:
            rv = self.roman_to_int(m.group(1))
            if rv is not None:
                return rv

        # Fallback
        return 1

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
                logger.info(
                    f"Starting printpackages_committees streaming with chunk_size={chunk_size}"
                )

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
                SELECT DISTINCT ON (c.package_id, c.authorityid) COUNT(*)
                FROM {self.staging_schema}.printpackages_committees AS c
                GROUP BY c.package_id, c.authorityid
                """
                package_count = await conn.fetchval(package_count_query)

                granule_count_query = f"""
                SELECT DISTINCT ON (gc.granule_id, gc.authorityid) COUNT(*)
                FROM {self.staging_schema}.printpackages_granules_committees gc
                GROUP BY gc.granule_id, gc.authorityid
                """
                granule_count = await conn.fetchval(granule_count_query)

                total_count = package_count + granule_count

                if total_count == 0:
                    logger.info("No printpackages_committees records found")
                    return

                logger.info(
                    f"Streaming {total_count} printpackages_committees records in chunks of {chunk_size}"
                )

                # Stream using pagination with aggregated formats
                offset = 0
                while True:
                    logger.debug(
                        f"Fetching printpackages_committees chunk at offset {offset}"
                    )

                    # Use JSON aggregation to collect all formats for each text
                    query = f"""
                    SELECT
                        ppc.package_id,
                        NULL as granule_id,
                        ppc.authorityid,
                        ppc.committeename
                    FROM {self.staging_schema}.printpackages_committees AS ppc
                    GROUP BY ppc.package_id, ppc.authorityid, ppc.committeename

                    UNION ALL

                    SELECT
                        ppg.packageid as package_id,
                        ppg.granuleid AS granule_id,
                        ppgc.authorityid,
                        ppgc.committeename
                    FROM {self.staging_schema}.printpackages_granules_committees AS ppgc
                    JOIN {self.staging_schema}.printpackages_granules AS ppg ON ppgc.granule_id = ppg.id
                    GROUP BY ppg.packageid, ppg.granuleid, ppgc.authorityid, ppgc.committeename
                    ORDER BY granule_id, authorityid, committeename
                    LIMIT {chunk_size} OFFSET {offset}
                    """

                    logger.debug(
                        f"Executing query for printpackages_committees at offset {offset}"
                    )
                    rows = await conn.fetch(query)
                    logger.debug(
                        f"Retrieved {len(rows)} rows for printpackages_committees at offset {offset}"
                    )

                    if not rows:
                        logger.info(
                            f"No more printpackages_committees records at offset {offset}"
                        )
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

                    if chunk:  # Only yield if we have valid records
                        logger.debug(
                            f"Yielding {len(chunk)} printpackages_committees records"
                        )
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
                logger.info(
                    f"Starting printpackages streaming with chunk_size={chunk_size}"
                )

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
                packages_without_granules_query = f"""
                SELECT COUNT(DISTINCT c.packageid)
                FROM {self.staging_schema}.printpackages c
                WHERE NOT EXISTS (
                    SELECT 1 FROM {self.staging_schema}.printpackages_granules g
                    WHERE g.packageid = c.packageid
                )
                """
                packages_without_granules_count = await conn.fetchval(
                    packages_without_granules_query
                )

                granule_count_query = f"""
                SELECT COUNT(DISTINCT g.granuleid)
                FROM {self.staging_schema}.printpackages_granules g
                """
                granule_count = await conn.fetchval(granule_count_query)

                total_count = packages_without_granules_count + granule_count

                if total_count == 0:
                    logger.info("No printpackages records found")
                    return

                logger.info(
                    f"Streaming {total_count} printpackages records in chunks of {chunk_size}"
                )

                # Stream using pagination with aggregated formats
                offset = 0
                while True:
                    logger.debug(f"Fetching printpackages chunk at offset {offset}")

                    # Build a unified projection, preferring granule values where overlapping
                    query = f"""
                    SELECT * FROM (
                        -- Package-only rows (no granules)
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
                            c.dateissued AS dateissued,
                            c.granuleslink AS granuleslink,
                            c.lastmodified AS lastmodified,
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
                            c.otheridentifier_isbn AS otheridentifier_isbn,
                            c.otheridentifier_sudoc_class_number AS otheridentifier_sudoc_class_number,
                            c.md5 AS md5,
                            c.fields AS fields,
                            c.committees AS committees,
                            c.federalpublicationname AS federalpublicationname,
                            c.download_xlslink AS download_xlslink,
                            c.download_videolink AS download_videolink,
                            c.download_thumbnailjpeg AS download_thumbnailjpeg,
                            c.download_mp3link AS download_mp3link,
                            c.download_jpeglink AS download_jpeglink,
                            c._references AS _references,
                            c.package_id AS package_id,
                            c.documenttype AS documenttype,
                            c.documentnumber AS documentnumber,
                            c.md5 AS package_md5,
                            c.publisher AS publisher,
                            NULL::text AS heading,
                            NULL::text AS graphicsinpdf,
                            NULL::text AS granuleclass,
                            NULL::text AS packagelink,
                            NULL::text AS relatedlink,
                            NULL::text AS granule_db_id,
                            NULL::text AS download_xmllink,
                            NULL::text AS agencies,
                            c.otheridentifier_stock_number AS otheridentifier_stock_number
                        FROM {self.staging_schema}.printpackages c
                        WHERE NOT EXISTS (
                            SELECT 1 FROM {self.staging_schema}.printpackages_granules g
                            WHERE g.packageid = c.packageid
                        )

                        UNION ALL

                        -- Joined package+granule rows
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
                            COALESCE(g.dateissued, c.dateissued) AS dateissued,
                            COALESCE(g.granuleslink, c.granuleslink) AS granuleslink,
                            COALESCE(g.lastmodified, c.lastmodified) AS lastmodified,
                            COALESCE(g.collectioncode, c.collectioncode) AS collectioncode,
                            COALESCE(g.collectionname, c.collectionname) AS collectionname,
                            c.pages AS pages,
                            NULL::boolean AS isappropriation,
                            c.otheridentifier_migrated_doc_id AS otheridentifier_migrated_doc_id,
                            c.sudocclassnumber AS sudocclassnumber,
                            c.governmentauthor1 AS governmentauthor1,
                            c.governmentauthor2 AS governmentauthor2,
                            COALESCE(g.processed_at, c.processed_at) AS processed_at,
                            COALESCE(g.source_doc_id, c.source_doc_id) AS source_doc_id,
                            c.otheridentifier_ils_system_id AS otheridentifier_ils_system_id,
                            c.otheridentifier_isbn AS otheridentifier_isbn,
                            c.otheridentifier_sudoc_class_number AS otheridentifier_sudoc_class_number,
                            c.md5 AS md5,
                            c.fields AS fields,
                            c.committees AS committees,
                            c.federalpublicationname AS federalpublicationname,
                            COALESCE(g.download_xlslink, c.download_xlslink) AS download_xlslink,
                            c.download_videolink AS download_videolink,
                            c.download_thumbnailjpeg AS download_thumbnailjpeg,
                            c.download_mp3link AS download_mp3link,
                            c.download_jpeglink AS download_jpeglink,
                            c._references AS _references,
                            c.package_id AS package_id,
                            c.documenttype AS documenttype,
                            c.documentnumber AS documentnumber,
                            c.md5 AS package_md5,
                            c.publisher AS publisher,
                            g.heading AS heading,
                            g.graphicsinpdf AS graphicsinpdf,
                            g.granuleclass AS granuleclass,
                            g.packagelink AS packagelink,
                            g.relatedlink AS relatedlink,
                            g.id AS granule_db_id,
                            g.download_xmllink AS download_xmllink,
                            g.agencies AS agencies,
                            c.otheridentifier_stock_number AS otheridentifier_stock_number
                        FROM {self.staging_schema}.printpackages c
                        JOIN {self.staging_schema}.printpackages_granules g
                            ON c.packageid = g.packageid
                        WHERE g.granuleid IS NOT NULL
                    ) q
                    ORDER BY q.packageid, q.granuleid NULLS FIRST
                    LIMIT {chunk_size} OFFSET {offset}
                    """

                    logger.debug(
                        f"Executing query for printpackages at offset {offset}"
                    )
                    rows = await conn.fetch(query)
                    logger.debug(
                        f"Retrieved {len(rows)} rows for printpackages at offset {offset}"
                    )

                    if not rows:
                        logger.info(f"No more printpackages records at offset {offset}")
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

                    if chunk:  # Only yield if we have valid records
                        logger.debug(f"Yielding {len(chunk)} printpackages records")
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
        Stream print packages bills joined chunks.
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
                logger.debug(
                    f"Fetching printpackages_reference_bills chunk at offset {offset}"
                )

                async with self.db_pool.acquire() as conn, conn.transaction():
                    query = f"""
                            SELECT DISTINCT ON (type, number, congress, references_id) ppgrc.*, ppg.granuleid AS granule_id, ppg.packageid AS package_id
                            FROM {self.staging_schema}.printpackages_granules_references_contents AS ppgrc
                            JOIN {self.staging_schema}.printpackages_granules_references AS pgr ON pgr.id = ppgrc.references_id
                            JOIN {self.staging_schema}.printpackages_granules AS ppg ON pgr.granule_id = ppg.id
                            ORDER BY type, number, congress, references_id, ppg.packageid, ppg.granuleid NULLS FIRST
                            LIMIT {chunk_size} OFFSET {offset}
                            """

                    logger.debug(
                        f"Executing query for printpackages_reference_bills at offset {offset}"
                    )
                    rows = await conn.fetch(query)
                    logger.debug(
                        f"Retrieved {len(rows)} rows for printpackages_reference_bills at offset {offset}"
                    )

                    if not rows:
                        logger.info(
                            f"No more printpackages_reference_bills records at offset {offset}"
                        )
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
                        logger.debug(
                            f"Yielding {len(chunk)} printpackages_reference_bills records"
                        )
                        yield chunk

                    offset += chunk_size

                    if offset % (chunk_size * 10) == 0:
                        logger.debug(
                            f"Streamed {offset} printpackages_reference_bills records"
                        )

        except Exception as e:
            logger.error(f"Error streaming printpackages_reference_bills: {e}")
            raise

    async def _stream_printpackages_agencies_joined_chunks(
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
                SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '{self.staging_schema}' AND table_name IN ('printpackages_granules_agencies', 'printpackages_granules')
            """
            tables_count = await conn.fetchval(tables_exist_query)

            if tables_count < 2:
                logger.warning(
                    f"Missing required tables in {self.staging_schema} schema: "
                    "printpackages_granules_agencies, "
                    "printpackages_granules"
                )
                return

        try:
            offset = 0
            while True:
                logger.debug(
                    f"Fetching printpackages_agencies chunk at offset {offset}"
                )

                async with self.db_pool.acquire() as conn, conn.transaction():
                    query = f"""
                        SELECT DISTINCT ON (name) ppgrc.name AS agency, ppgr.granuleid AS granule_id, ppgr.packageid AS package_id
                        FROM {self.staging_schema}.printpackages_granules_agencies AS ppgrc
                        JOIN {self.staging_schema}.printpackages_granules AS ppgr ON ppgr.id = ppgrc.granule_id
                        ORDER BY name, ppgr.packageid, ppgr.granuleid NULLS FIRST
                        LIMIT {chunk_size} OFFSET {offset}
                        """

                    logger.debug(
                        f"Executing query for printpackages_agencies at offset {offset}"
                    )
                    rows = await conn.fetch(query)
                    logger.debug(
                        f"Retrieved {len(rows)} rows for printpackages_agencies at offset {offset}"
                    )

                    if not rows:
                        logger.info(
                            f"No more printpackages_agencies records at offset {offset}"
                        )
                        break

                    chunk = []
                    for row in rows:
                        try:
                            row_dict = dict(row)
                            chunk.append(row_dict)
                        except Exception as e:
                            logger.error(
                                f"Error converting printpackages_agencies row to dict: {e}, row type: {type(row)}, row: {row}"
                            )
                            continue

                    if chunk:
                        logger.debug(
                            f"Yielding {len(chunk)} printpackages_agencies records"
                        )
                        yield chunk

                    offset += chunk_size

                    if offset % (chunk_size * 10) == 0:
                        logger.debug(
                            f"Streamed {offset} printpackages_agencies records"
                        )

        except Exception as e:
            logger.error(f"Error streaming printpackages_agencies: {e}")
            raise

    async def _clean_printpackages_singular(
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
        - dateissued,
        - granuleslink,
            - lastmodified,
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
        - otheridentifier_isbn,
            - otheridentifier_sudoc_class_number,
            - md5,
        - fields,
            - committees,
        - federalpublicationname,
            - download_xlslink,
            - download_videolink,
            - download_thumbnailjpeg,
            - download_mp3link,
            - download_jpeglink,
        - _references,
            - package_id,
            - documenttype,
            - documentnumber,
            - package_md5,
            - publisher,
        - heading,
        - graphicsinpdf,
            - granuleclass,
            - packagelink,
            - relatedlink,
            - granule_db_id,
            - download_xmllink,
            - agencies,
            - otheridentifier_stock_number

        FINAL COLUMNS:
        - package_id TEXT PRIMARY KEY,
            - print_id TEXT,
        - title TEXT,
        - subtitle TEXT,
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
        - federal_publication_name TEXT,
            - collection_code TEXT,
        - migrated_doc_id TEXT,
        - su_doc_class_number TEXT,
            - isbn TEXT,
            - stock_number TEXT,
        - last_modified TIMESTAMP WITH TIME ZONE
        );
        """
        cleaned = record_data.copy()

        (
            print_id,
            print_set_id,
            _,
            part_number,
            print_type,
            print_number,
            congress,
        ) = self._build_print_identity(cleaned)

        # Compute errata flag using shared heuristics
        errata_detected = self._detect_is_errata(cleaned)
        if print_type is None:
            raise ValueError(f"Hearing type is None for {cleaned.get('packageid')}")
        chamber = (
            "house "
            if print_type == "hprt"
            else "senate"
            if print_type == "sprt"
            else "joint"
            if print_type == "jprt"
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

        filtered_cleaned = {
            "package_id": str(cleaned.get("packageid", "ID_ERROR")),
            "print_id": str(print_id),
            # Ensure a non-null granule_id: use granule if present, otherwise fallback to package_id
            "granule_id": str(
                cleaned.get("granuleid")
                or cleaned.get("granule_id")
                or cleaned.get("packageid")
                or cleaned.get("package_id")
            ),
            "print_set_id": print_set_id,
            "title": title_value,
            "subtitle": subtitle_value,
            "print_type": print_type,
            "print_number": str(print_number),
            "part_number": int(part_number) if part_number is not None else None,
            "chamber": self.standardize_chamber(cleaned.get("chamber", chamber)),
            "congress": self.safe_int(congress),
            "session": self.safe_int(cleaned.get("session", None)),
            "pages": self.safe_int(cleaned.get("pages", None)),
            "is_errata": errata_detected,
            "issued_at": self.standardize_date(cleaned.get("dateissued", None)),
            "branch": cleaned.get("branch", None),
            "government_author1": cleaned.get("governmentauthor1", None),
            "government_author2": cleaned.get("governmentauthor2", None),
            "publisher": cleaned.get("publisher", None),
            "collection_code": cleaned.get("collectioncode", None),
            "isbn": cleaned.get("otheridentifier_isbn", None),
            "federal_publication_name": cleaned.get("federalpublicationname", None),
            "migrated_doc_id": cleaned.get("otheridentifier_migrated_doc_id", None),
            "su_doc_class_number": cleaned.get("sudocclassnumber", None),
            "stock_number": cleaned.get("otheridentifier_stock_number", None),
            "last_modified": self.standardize_date(cleaned.get("lastmodified", None)),
        }

        return filtered_cleaned

    async def _clean_printpackages_committees_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual print packages committees records.
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
            cleaned["authorityid"] = COMMITTEE_FIXES.get(cleaned.get("authorityid"))

        filtered_cleaned = {
            "package_id": cleaned.get("package_id", "ID_ERROR"),
            "granule_id": cleaned.get("granule_id", "ID_ERROR"),
            "committee_code": cleaned.get("authorityid", "ID_ERROR"),
            "committee_name": cleaned.get("committeename", None),
        }

        return filtered_cleaned

    async def _clean_printpackages_reference_bills_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual print packages reference bills records.
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

    async def _clean_printpackages_agencies_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual print packages agencies records.
        """
        cleaned = record_data.copy()
        filtered_cleaned = {
            "package_id": cleaned.get("package_id", "ID_ERROR"),
            "granule_id": cleaned.get("granule_id", "ID_ERROR"),
            "agency": cleaned.get("agency", "ID_ERROR"),
        }
        return filtered_cleaned

    # Add post-processing methods
    async def _post_process_printpackages(self) -> dict[str, Any]:
        """Post-processing operations specific to bills data."""
        # TODO: ils system id
        # TODO: reference laws/codes/statutes
        # TODO: fix placeholder parts
        # TODO: fix is_errata
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
                    FROM {self.staging_schema}.printpackages
                """
                rows = await conn.fetch(fetch_sql)
                logger.info(f"Found {len(rows)} records with ils_system_id data")

                # Prepare batch params
                insert_sql = f"""
                    INSERT INTO {self.production_schema}.printpackages_ils_system_id (package_id, ils_system_id)
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
                    FROM {self.staging_schema}.printpackages_references
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
                            INSERT INTO {self.production_schema}.printpackages_reference_laws (package_id, law_id, law_type, law_number, order_number, congress)
                            VALUES ($1, $2, $3, $4, $5, $6)
                            ON CONFLICT DO NOTHING
                        """
                        await conn.executemany(insert_sql, law_params)
                        total_counts["laws"] += len(law_params)
                        logger.info(f"Inserted {len(law_params)} law records")

                    if statute_params:
                        insert_sql = f"""
                            INSERT INTO {self.production_schema}.printpackages_reference_statutes (report_statute_id, package_id, reference_statute)
                            VALUES ($1, $2, $3)
                            ON CONFLICT DO NOTHING
                        """
                        await conn.executemany(insert_sql, statute_params)
                        total_counts["statutes"] += len(statute_params)
                        logger.info(f"Inserted {len(statute_params)} statute records")

                    if code_params:
                        insert_sql = f"""
                            INSERT INTO {self.production_schema}.printpackages_reference_codes (report_code_id, package_id, reference_code)
                            VALUES ($1, $2, $3)
                            ON CONFLICT DO NOTHING
                        """
                        await conn.executemany(insert_sql, code_params)
                        total_counts["codes"] += len(code_params)
                        logger.info(f"Inserted {len(code_params)} code records")

                    if statute_page_params:
                        insert_sql = f"""
                            INSERT INTO {self.production_schema}.printpackages_reference_statutes_pages (report_statute_id, page)
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
                            INSERT INTO {self.production_schema}.printpackages_reference_codes_sections (report_code_id, code_section)
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
                # First, get all print sets that have at least one placeholder 'x' row
                placeholder_print_sets = await conn.fetch(
                    f"""
                    SELECT DISTINCT
                        SUBSTRING(p.print_id FROM '^([a-z]+)') as print_type,
                        SUBSTRING(p.print_id FROM '^[a-z]+([0-9]+)') as print_number,
                        SUBSTRING(p.print_id FROM '([0-9]+)$') as congress
                    FROM {self.production_schema}.printpackages p
                    WHERE (p.print_id LIKE '%-x-%' OR p.print_id LIKE '%-xe-%')
                        AND p.package_id NOT LIKE 'GPO-PLUMBOOK%'
                        AND p.granule_id IS NOT NULL
                    """
                )

                if not placeholder_print_sets:
                    results["operations"].append(
                        {
                            "name": "resolve_placeholder_parts",
                            "status": "skipped",
                            "reason": "no placeholder reports found",
                        }
                    )
                    logger.info("No placeholder reports found")
                    return results

                # Now fetch ALL rows (including non-placeholder) for these print sets
                print_set_conditions = []
                for row in placeholder_print_sets:
                    print_type = row["print_type"]
                    print_number = row["print_number"]
                    congress = row["congress"]
                    print_set_conditions.append(
                        f"p.print_id LIKE '{print_type}{print_number}-%{congress}'"
                    )

                logger.info(
                    f"Found {len(placeholder_print_sets)} print sets with placeholders"
                )
                logger.info(
                    f"Generated {len(print_set_conditions)} conditions for fetching full sets"
                )

                prod_rows = await conn.fetch(
                    f"""
                    SELECT p.package_id,
                            p.granule_id,
                            p.print_id,
                            p.is_errata
                    FROM {self.production_schema}.printpackages p
                    WHERE ({" OR ".join(print_set_conditions)})
                        AND p.package_id NOT LIKE 'GPO-PLUMBOOK%'
                        AND p.granule_id IS NOT NULL
                    """
                )

                logger.info(f"Fetched {len(prod_rows)} total rows for processing")

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

                    # Expand to include all non-placeholder siblings in the same print_set (print_type, print_number, congress)
                    try:
                        # Build unique group keys from placeholder rows
                        group_keys: set[tuple[str, str]] = set()
                        for r in prod_rows:
                            try:
                                pid = (
                                    str(r["print_id"]) if r and "print_id" in r else ""
                                )
                                m = re.match(
                                    r"^([a-z]+)\d+-[^-]+-(\d+)$", pid, re.IGNORECASE
                                )
                                if m:
                                    group_keys.add(
                                        (
                                            m.group(1).lower()
                                            + pid[
                                                len(m.group(1)) : len(m.group(1)) + 0
                                            ],
                                            m.group(2),
                                        )
                                    )
                                else:
                                    # Fallback: split parts
                                    p1 = pid.split("-")[0]
                                    p3 = pid.split("-")[-1]
                                    if p1 and p3:
                                        group_keys.add((p1, p3))
                            except Exception:
                                continue

                        # Safer: extract parts via SQL split_part for each group
                        # Collect all rows for these groups
                        all_rows_map: dict[
                            tuple[str, str], dict[tuple[str, str], dict]
                        ] = {}
                        for p1, cong in group_keys:
                            try:
                                siblings = await conn.fetch(
                                    f"""
                                    SELECT package_id, granule_id, print_id, is_errata
                                    FROM {self.production_schema}.printpackages
                                    WHERE split_part(print_id, '-', 1) = $1
                                      AND split_part(print_id, '-', 3) = $2
                                      AND granule_id IS NOT NULL
                                    """,
                                    p1,
                                    cong,
                                )
                                key = (p1, cong)
                                for s in siblings:
                                    all_rows_map.setdefault(key, {})[
                                        (s["package_id"], s["granule_id"])
                                    ] = dict(s)
                            except Exception:
                                continue

                        # Flatten to list if we fetched anything; else keep original prod_rows
                        expanded_rows: list[dict] = []
                        for _k, rows_map in all_rows_map.items():
                            expanded_rows.extend(rows_map.values())
                        if expanded_rows:
                            prod_rows = expanded_rows
                            logger.info(
                                f"Expanded processing set to {len(prod_rows)} rows including non-placeholder siblings"
                            )
                    except Exception as e:
                        logger.warning(f"Failed to expand sibling rows: {e}")

                    # Pre-scan entire print_set groups to detect multi-tier presence and establish stable ordering
                    pre_groups: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
                    for row in prod_rows:
                        m = re.match(
                            r"^([a-z]+)(\d+)-([^-]+)-(\d+)$",
                            str(row["print_id"]),
                            re.IGNORECASE,
                        )
                        if not m:
                            continue
                        gkey = (m.group(1).lower(), m.group(2), m.group(4))
                        pre_groups.setdefault(gkey, []).append(dict(row))

                    def pre_parse_order(
                        granule_id: str | None, package_id: str | None
                    ) -> tuple[int, ...]:
                        try:
                            s = str(granule_id) if granule_id is not None else ""
                            if (
                                package_id
                                and granule_id
                                and str(package_id) != str(granule_id)
                            ):
                                try:
                                    suffix_tokens = self._suffix_tokens_from_ids(
                                        str(package_id), str(granule_id)
                                    )
                                    numeric_tokens = [
                                        int(t) for t in suffix_tokens if t.isdigit()
                                    ]
                                    if numeric_tokens:
                                        if len(numeric_tokens) == 1:
                                            return (1, numeric_tokens[0], 0)
                                        else:
                                            return (
                                                1,
                                                numeric_tokens[0],
                                                1,
                                                *numeric_tokens[1:],
                                            )
                                except Exception:
                                    pass
                            # Compact trailing tokens fallback
                            mloc = re.search(r"v(\d+)$", s, re.IGNORECASE)
                            if mloc:
                                return (1, int(mloc.group(1)), 0)
                            mloc = re.search(r"v([ivxlcdmIVXLCDM]+)$", s)
                            if mloc:
                                rv = self.roman_to_int(mloc.group(1))
                                if rv is not None:
                                    return (1, rv, 0)
                            # Use plain numeric tail as weakest hint
                            mloc = re.search(r"(\d+)$", s)
                            if mloc:
                                return (2, int(mloc.group(1)))
                        except Exception:
                            pass
                        return (99, 1 << 30)

                    group_has_multitier_map: dict[tuple[str, str, str], bool] = {}
                    # Group-level padded order index; key: (ptype, pnum, cong, package_id, granule_id)
                    group_padded_seq_idx: dict[tuple[str, str, str, str, str], int] = {}

                    def _norm_token_to_int(token: str | None) -> int:
                        try:
                            if token is None:
                                return 0
                            s = str(token)
                            if s.isdigit():
                                return int(s)
                            rv = self.roman_to_int(s)
                            if rv is not None:
                                return rv
                            av = self.alpha_to_int(s)
                            if av is not None:
                                return av
                            m_any = re.search(r"(\\d+)", s)
                            if m_any:
                                return int(m_any.group(1))
                        except Exception:
                            pass
                        return 1 << 29

                    per_pkg_seq_idx: dict[tuple[str, str, str, str, str], int] = {}
                    single_suffix_map: dict[tuple[str, str], int] = {}

                    for gkey, grows in pre_groups.items():
                        # Sort entire group
                        ordered = sorted(
                            [
                                (
                                    pre_parse_order(
                                        str(r.get("granule_id") or ""),
                                        str(r.get("package_id") or ""),
                                    ),
                                    r,
                                )
                                for r in grows
                            ],
                            key=lambda x: x[0],
                        )
                        # Detect multi-tier across full group and build single-suffix map
                        has_multi = False
                        multi_examples = []
                        for _ord, rr in ordered:
                            pkg_s = str(rr.get("package_id") or "")
                            gra_s = str(rr.get("granule_id") or "")
                            if pkg_s and gra_s and pkg_s != gra_s:
                                try:
                                    stoks = self._suffix_tokens_from_ids(pkg_s, gra_s)
                                except Exception:
                                    stoks = []
                                if len(stoks) > 1:
                                    has_multi = True
                                    multi_examples.append(f"{gra_s}={stoks}")
                                elif len(stoks) == 1 and stoks[0].isdigit():
                                    single_suffix_map[(pkg_s, gra_s)] = int(stoks[0])
                        group_has_multitier_map[gkey] = has_multi
                        if has_multi:
                            logger.debug(
                                f"Group {gkey}: detected multi-tier with examples: {multi_examples[:5]}"
                            )
                        else:
                            logger.debug(f"Group {gkey}: no multi-tier detected")

                        # Build group-wide padded hierarchical order (parents interleave with children across the whole group)
                        # 1) Collect suffix tokens per row
                        toks_by_row: dict[tuple[str, str], list[str]] = {}
                        depths: list[int] = []
                        for _ord, rr in ordered:
                            pkg_s = str(rr.get("package_id") or "")
                            gra_s = str(rr.get("granule_id") or "")
                            try:
                                toks = self._suffix_tokens_from_ids(pkg_s, gra_s)
                            except Exception:
                                toks = []
                            toks_by_row[(pkg_s, gra_s)] = toks
                            depths.append(len(toks))
                        max_depth = max(depths) if depths else 1

                        def padded_key(
                            rr: dict[str, Any],
                            toks_by_row: dict[tuple[str, str], list[str]] = toks_by_row,
                            max_depth: int = max_depth,
                        ) -> tuple[int, ...]:
                            pkg_s = str(rr.get("package_id") or "")
                            gra_s = str(rr.get("granule_id") or "")
                            toks = toks_by_row.get((pkg_s, gra_s), [])
                            ints = [_norm_token_to_int(t) for t in toks]
                            if len(ints) < max_depth:
                                ints = ints + [0] * (max_depth - len(ints))
                            if not ints:
                                ints = [1 << 30]
                            return tuple(ints)

                        ordered_group = sorted(
                            [r for _ord, r in ordered],
                            key=lambda r: (
                                padded_key(r),
                                str(r.get("granule_id") or ""),
                            ),
                        )
                        for i, rr in enumerate(ordered_group, start=1):
                            group_padded_seq_idx[
                                (
                                    gkey[0],
                                    gkey[1],
                                    gkey[2],
                                    str(rr.get("package_id") or ""),
                                    str(rr.get("granule_id") or ""),
                                )
                            ] = i
                        # Per-package stable order index within this group
                        pkg_to_rows: dict[str, list[dict[str, Any]]] = {}
                        for _ord, rr in ordered:
                            pkg_to_rows.setdefault(
                                str(rr.get("package_id") or ""), []
                            ).append(rr)
                        for pkg_id, pkg_rows in pkg_to_rows.items():
                            for idx, rr in enumerate(pkg_rows, start=1):
                                per_pkg_seq_idx[
                                    (
                                        gkey[0],
                                        gkey[1],
                                        gkey[2],
                                        pkg_id,
                                        str(rr.get("granule_id") or ""),
                                    )
                                ] = idx

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

                        # Group by (print_type, print_number, congress) for this chunk
                        groups = {}
                        for row in chunk_rows:
                            m = re.match(
                                r"^([a-z]+)(\d+)-([^-]+)-(\d+)$",
                                str(row["print_id"]),
                                re.IGNORECASE,
                            )
                            if not m:
                                continue
                            key = (m.group(1).lower(), m.group(2), m.group(4))
                            groups.setdefault(key, []).append(dict(row))

                        # Determine starting index per (group, package_id) based on existing non-placeholder parts
                        existing_max_by_pkg: dict[tuple[str, str, str, str], int] = {}
                        for (ptype, pnum, cong), rows in groups.items():
                            package_ids = {str(r.get("package_id")) for r in rows}
                            for pkg_id in package_ids:
                                try:
                                    existing_max = await conn.fetchval(
                                        f"""
                                        SELECT COALESCE(MAX(part_number), 0)
                                        FROM {self.production_schema}.printpackages
                                        WHERE split_part(print_id, '-', 1) = $1
                                            AND split_part(print_id, '-', 3) = $2
                                            AND package_id = $3
                                            AND print_id NOT LIKE '%-x%'
                                        """,
                                        f"{ptype}{pnum}",
                                        cong,
                                        pkg_id,
                                    )
                                    existing_max_by_pkg[(ptype, pnum, cong, pkg_id)] = (
                                        int(existing_max or 0)
                                    )
                                except Exception:
                                    existing_max_by_pkg[(ptype, pnum, cong, pkg_id)] = 0

                        def parse_order(
                            granule_id: str | None, package_id: str | None
                        ) -> tuple[int, ...]:
                            """Compute ordering key for placeholder parts within a print group.

                            Hierarchy (lower ranks come first):
                            1) part (plain numeric suffix like '-3', or explicit 'pt')
                            2) volume/vol
                            3) book
                            4) division
                            5) chapter
                            6) section
                            7) appendix
                            100) unknown

                            Then within the same category, order by parsed numeric/alpha index
                            and an optional secondary numeric index if present.
                            """
                            try:
                                s = str(granule_id) if granule_id is not None else ""
                                s_upper = s.upper()
                                s_lower = s.lower()

                                # 0) Prefer full suffix-based numeric ordering (granule minus package)
                                if (
                                    package_id
                                    and granule_id
                                    and str(package_id) != str(granule_id)
                                ):
                                    try:
                                        suffix_tokens = self._suffix_tokens_from_ids(
                                            str(package_id), str(granule_id)
                                        )
                                        numeric_tokens = [
                                            int(t) for t in suffix_tokens if t.isdigit()
                                        ]
                                        if numeric_tokens:
                                            # Build a variable-length key:
                                            # (rank=1, primary, 0) for single number (e.g., 14)
                                            # (rank=1, primary, 1, sec, tert, ...) for sequences (e.g., 14-1, 14-2, 14-10)
                                            if len(numeric_tokens) == 1:
                                                return (1, numeric_tokens[0], 0)
                                            else:
                                                return (
                                                    1,
                                                    numeric_tokens[0],
                                                    1,
                                                    *numeric_tokens[1:],
                                                )
                                    except Exception:
                                        pass

                                # Recognize compact trailing v/p/Ob/b tokens even without suffix-diff
                                # e.g., ...vI, ...v2, ...pX, ...p2, ...ObII, ...b3
                                def trailing_compact_value(text: str) -> int | None:
                                    m = re.search(r"v(\d+)$", text, re.IGNORECASE)
                                    if m:
                                        return int(m.group(1))
                                    m = re.search(r"v([ivxlcdmIVXLCDM]+)$", text)
                                    if m:
                                        rv = self.roman_to_int(m.group(1))
                                        if rv is not None:
                                            return rv
                                    m = re.search(
                                        r"v(one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|thirteen|fourteen|fifteen|sixteen|seventeen|eighteen|nineteen|twenty)$",
                                        text,
                                        re.IGNORECASE,
                                    )
                                    if m:
                                        return self.number_word_to_int(m.group(1))
                                    m = re.search(r"p(\d+)$", text, re.IGNORECASE)
                                    if m:
                                        return int(m.group(1))
                                    m = re.search(r"p([ivxlcdmIVXLCDM]+)$", text)
                                    if m:
                                        rv = self.roman_to_int(m.group(1))
                                        if rv is not None:
                                            return rv
                                    m = re.search(
                                        r"p(one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|thirteen|fourteen|fifteen|sixteen|seventeen|eighteen|nineteen|twenty)$",
                                        text,
                                        re.IGNORECASE,
                                    )
                                    if m:
                                        return self.number_word_to_int(m.group(1))
                                    m = re.search(r"Ob(\d+|[ivxlcdmIVXLCDM]+)$", text)
                                    if m:
                                        tok = m.group(1)
                                        return (
                                            int(tok)
                                            if tok.isdigit()
                                            else (self.roman_to_int(tok) or None)
                                        )
                                    m = re.search(
                                        r"Ob(one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|thirteen|fourteen|fifteen|sixteen|seventeen|eighteen|nineteen|twenty)$",
                                        text,
                                        re.IGNORECASE,
                                    )
                                    if m:
                                        return self.number_word_to_int(m.group(1))
                                    m = re.search(r"b(\d+|[ivxlcdmIVXLCDM]+)$", text)
                                    if m:
                                        tok = m.group(1)
                                        return (
                                            int(tok)
                                            if tok.isdigit()
                                            else (self.roman_to_int(tok) or None)
                                        )
                                    m = re.search(
                                        r"b(one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|thirteen|fourteen|fifteen|sixteen|seventeen|eighteen|nineteen|twenty)$",
                                        text,
                                        re.IGNORECASE,
                                    )
                                    if m:
                                        return self.number_word_to_int(m.group(1))
                                    return None

                                compact_val = trailing_compact_value(s)
                                if compact_val is not None:
                                    return (1, compact_val, 0)

                                # Special JCS handling (fallback only if no suffix/compact was usable)
                                if "JCS" in s_upper:
                                    m_jcs = re.search(
                                        r"JCS[^0-9]*([0-9]+)(?:[^0-9]+([0-9]+))?(?:[^0-9]+([0-9]+))?",
                                        s_upper,
                                    )
                                    if m_jcs:
                                        a = int(m_jcs.group(1)) if m_jcs.group(1) else 0
                                        b = int(m_jcs.group(2)) if m_jcs.group(2) else 0
                                        c = int(m_jcs.group(3)) if m_jcs.group(3) else 0
                                        return (1, a * 1000000 + b * 1000 + c)

                                # Category rank
                                def rank_for(text: str) -> int:
                                    if re.search(
                                        r"\bpt\b|\bpart\b", text, re.IGNORECASE
                                    ):
                                        return 1
                                    if re.search(
                                        r"\bvol(?:ume)?\b", text, re.IGNORECASE
                                    ):
                                        return 2
                                    if re.search(r"\bbook\b", text, re.IGNORECASE):
                                        return 3
                                    if re.search(r"\bdivision\b", text, re.IGNORECASE):
                                        return 4
                                    if re.search(r"\bchapter\b", text, re.IGNORECASE):
                                        return 5
                                    if re.search(r"\bsection\b", text, re.IGNORECASE):
                                        return 6
                                    if re.search(r"\bappendix\b", text, re.IGNORECASE):
                                        return 7
                                    # Plain trailing number like '-3' → treat as part
                                    if re.search(r"[-_](\d+)(?:\b|$)", text):
                                        return 1
                                    return 100

                                cat_rank = rank_for(s_lower)

                                # Extract primary/secondary indices by category
                                primary = 1 << 30
                                secondary = 0

                                # SectionN or SectionN-M
                                m = re.search(
                                    r"section\s*(\d+)(?:[\-_/](\d+))?", s_lower
                                )
                                if m:
                                    primary = int(m.group(1))
                                    secondary = int(m.group(2)) if m.group(2) else 0
                                    return (cat_rank, primary, secondary)

                                # AppendixA or AppendixAA (treat multi-letters via alpha_to_int/roman)
                                m = re.search(
                                    r"appendix\s*([A-Za-z]+)", s, re.IGNORECASE
                                )
                                if m:
                                    tok = m.group(1)
                                    alpha_val = self.alpha_to_int(tok) or 0
                                    primary = alpha_val if alpha_val > 0 else (1 << 29)
                                    return (cat_rank, primary, secondary)

                                # Division/Chapter/Volume/Book with numbers or roman
                                for label in (
                                    "division",
                                    "chapter",
                                    "vol",
                                    "volume",
                                    "book",
                                    "part",
                                    "pt",
                                ):
                                    m = re.search(
                                        rf"{label}[^0-9A-Za-z]*([0-9ivxlcdmIVXLCDM]+)",
                                        s,
                                    )
                                    if m:
                                        token = m.group(1)
                                        if token.isdigit():
                                            primary = int(token)
                                        else:
                                            primary = self.roman_to_int(token) or (
                                                1 << 29
                                            )
                                        return (cat_rank, primary, secondary)

                                # '-pt<token>' with mixed alnum (e.g., pt1A, pt2-B, pt3(a))
                                m_pt = re.search(
                                    r"[-_]pt([A-Za-z0-9()\-]+)", s, re.IGNORECASE
                                )
                                if m_pt:
                                    raw_tok = m_pt.group(1)
                                    norm = re.sub(r"[^A-Za-z0-9]", "", raw_tok)
                                    if re.search(r"\d", norm):
                                        num_m = re.search(r"(\d+)", norm)
                                        primary = (
                                            int(num_m.group(1)) if num_m else primary
                                        )
                                    let_m = re.search(r"([A-Za-z]+)", norm)
                                    if let_m:
                                        secondary = (
                                            self.alpha_to_int(let_m.group(1)) or 0
                                        )
                                    return (cat_rank, primary, secondary)

                                # Trailing mixed token like '...-6A' or '..._8-C'
                                m_tail = re.search(r"(\d+)[\-_()]*([A-Za-z])$", s)
                                if m_tail:
                                    primary = int(m_tail.group(1))
                                    secondary = self.alpha_to_int(m_tail.group(2)) or 0
                                    return (cat_rank, primary, secondary)

                                # Fallback: last one or two numeric tokens
                                m = re.search(r"(\d+)(?:-(\d+))?$", s)
                                if m:
                                    primary = int(m.group(1))
                                    secondary = int(m.group(2)) if m.group(2) else 0
                                    return (cat_rank, primary, secondary)
                            except Exception:
                                pass
                            return (
                                cat_rank if "cat_rank" in locals() else 100,
                                1 << 30,
                                1 << 30,
                            )

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
                                rows_with_order.append(
                                    (
                                        parse_order(
                                            str(r.get("granule_id") or ""),
                                            str(r.get("package_id") or ""),
                                        ),
                                        r,
                                    )
                                )
                            rows_with_order.sort(key=lambda x: x[0])

                            # Use precomputed print_set-level flags and per-row values
                            group_has_multitier = group_has_multitier_map.get(
                                _key, False
                            )
                            logger.debug(
                                f"Group {_key}: group_has_multitier={group_has_multitier}, rows={len(rows_with_order)}"
                            )
                            row_single_suffix_num: dict[tuple[str, str], int] = {}
                            for _ord, rr in rows_with_order:
                                pkg_id_s = str(rr.get("package_id") or "")
                                gran_id_s = str(rr.get("granule_id") or "")
                                val = single_suffix_map.get((pkg_id_s, gran_id_s))
                                if val is not None:
                                    row_single_suffix_num[(pkg_id_s, gran_id_s)] = val

                            # For multi-tier groups, re-sort by hierarchical padded tokens to ensure parent->children order
                            if group_has_multitier:
                                logger.info(
                                    f"Applying hierarchical sort to group {_key}"
                                )
                                logger.debug(
                                    f"Multi-tier group {_key}: processing {len(rows_with_order)} rows"
                                )

                                def simple_hierarchical_key(r: dict) -> tuple[int, ...]:
                                    """Simple hierarchical sort key: extract suffix tokens and pad with zeros."""
                                    pkg_s = str(r.get("package_id") or "")
                                    gra_s = str(r.get("granule_id") or "")
                                    try:
                                        toks = self._suffix_tokens_from_ids(
                                            pkg_s, gra_s
                                        )
                                        logger.debug(
                                            f"Granule {gra_s}: suffix_tokens={toks}"
                                        )
                                        # Convert to ints, pad to depth 3 for simplicity
                                        ints = []
                                        for t in toks[:3]:  # Take first 3 levels max
                                            try:
                                                if t.isdigit():
                                                    ints.append(int(t))
                                                else:
                                                    rv = self.roman_to_int(t)
                                                    ints.append(
                                                        rv if rv is not None else 999
                                                    )
                                            except Exception:
                                                ints.append(999)
                                        # Pad to 3 levels with zeros
                                        while len(ints) < 3:
                                            ints.append(0)
                                        logger.debug(
                                            f"Granule {gra_s}: hierarchical_key={tuple(ints)}"
                                        )
                                        return tuple(ints)
                                    except Exception:
                                        logger.debug(
                                            f"Granule {gra_s}: failed to parse, using fallback key"
                                        )
                                        return (999, 999, 999)

                                # Re-sort rows by hierarchical key
                                before_sort = [
                                    (simple_hierarchical_key(r), r.get("granule_id"))
                                    for _, r in rows_with_order
                                ]
                                logger.debug(
                                    f"Before hierarchical sort: {before_sort[:10]}"
                                )  # Show first 10

                                rows_with_order = sorted(
                                    [
                                        (simple_hierarchical_key(r), r)
                                        for _, r in rows_with_order
                                    ],
                                    key=lambda x: x[0],
                                )

                                after_sort = [
                                    (key, r.get("granule_id"))
                                    for key, r in rows_with_order
                                ]
                                logger.info(
                                    f"After hierarchical sort: {after_sort[:10]}"
                                )  # Show first 10

                            # Assign parts sequentially, accounting for all rows in hierarchical order
                            total_rows_in_chunk = len(rows_with_order)
                            logger.debug(
                                f"Processing chunk {chunk_idx + 1} with {total_rows_in_chunk} rows"
                            )

                            for row_idx, (_ord, r) in enumerate(
                                rows_with_order, start=1
                            ):
                                package_id = r["package_id"]
                                old_print_id = r["print_id"]

                                # For multi-tier groups, update ALL rows to maintain hierarchical sequence
                                # For flat groups, only update placeholder rows
                                old_print_id_s = str(old_print_id or "")
                                has_placeholder = "x" in old_print_id_s.lower()

                                if not group_has_multitier and not has_placeholder:
                                    # Flat group: skip non-placeholder rows
                                    continue
                                is_errata_flag = (
                                    bool(r["is_errata"])
                                    if r["is_errata"] is not None
                                    else False
                                )

                                m = re.match(
                                    r"^([a-z]+)(\d+)-([^-]+)-(\d+)$",
                                    str(old_print_id),
                                    re.IGNORECASE,
                                )
                                if not m:
                                    continue
                                ptype, pnum, part_comp, cong = (
                                    m.group(1).lower(),
                                    m.group(2),
                                    m.group(3),
                                    m.group(4),
                                )

                                # Preserve errata suffix for non-placeholder rows, detect for placeholders
                                if has_placeholder:
                                    has_errata_suffix = (
                                        part_comp.endswith("e") or is_errata_flag
                                    )
                                else:
                                    # For non-placeholder rows, preserve existing errata suffix
                                    has_errata_suffix = part_comp.endswith("e")
                                if group_has_multitier:
                                    # For multi-tier groups, restart numbering from 1 since we're reassigning ALL rows
                                    start_idx = 0
                                else:
                                    # For flat groups, continue from existing max
                                    start_idx = existing_max_by_pkg.get(
                                        (_key[0], _key[1], _key[2], package_id), 0
                                    )
                                if row_idx <= 3:  # Log first few assignments
                                    logger.debug(
                                        f"Placeholder row at position {row_idx}: package_id={package_id}, granule_id={r.get('granule_id')}, start_idx={start_idx}"
                                    )
                                # Group-aware assignment:
                                # 1) If group has multi-tier suffixes anywhere, assign strictly by global per-package order
                                # 2) Else (flat group), first use single numeric suffix if present
                                # 3) Else (flat), try compact trailing tokens (v/p/Ob/b) → ints
                                assign_num = None

                                if group_has_multitier:
                                    # For multi-tier groups, use sequential assignment based on position in hierarchical order
                                    # The hierarchical sort above ensures proper parent->children order
                                    assign_num = start_idx + row_idx
                                    if row_idx <= 5:  # Log first few assignments
                                        logger.debug(
                                            f"Multi-tier assignment: row_idx={row_idx}, start_idx={start_idx}, assign_num={assign_num}, granule={r.get('granule_id')}"
                                        )
                                else:
                                    # Flat group: allow direct mapping
                                    single_val = row_single_suffix_num.get(
                                        (package_id, str(r.get("granule_id") or ""))
                                    )
                                    if single_val is not None:
                                        assign_num = single_val

                                # 3) flat group: allow compact v/p/Ob/b mapping
                                if assign_num is None:
                                    s = str(r.get("granule_id") or "")
                                    m = re.search(r"v(\d+)$", s, re.IGNORECASE)
                                    if m:
                                        assign_num = int(m.group(1))
                                    if assign_num is None:
                                        m = re.search(r"p(\d+)$", s, re.IGNORECASE)
                                        if m:
                                            assign_num = int(m.group(1))
                                    if assign_num is None:
                                        m = re.search(r"Ob(\d+)$", s)
                                        if m:
                                            assign_num = int(m.group(1))
                                    if assign_num is None:
                                        m = re.search(r"b(\d+)$", s)
                                        if m:
                                            assign_num = int(m.group(1))
                                    if assign_num is None:
                                        m = re.search(r"v([ivxlcdmIVXLCDM]+)$", s)
                                        if m:
                                            rv = self.roman_to_int(m.group(1))
                                            if rv is not None:
                                                assign_num = rv
                                    if assign_num is None:
                                        m = re.search(r"p([ivxlcdmIVXLCDM]+)$", s)
                                        if m:
                                            rv = self.roman_to_int(m.group(1))
                                            if rv is not None:
                                                assign_num = rv
                                    if assign_num is None:
                                        m = re.search(r"Ob([ivxlcdmIVXLCDM]+)$", s)
                                        if m:
                                            rv = self.roman_to_int(m.group(1))
                                            if rv is not None:
                                                assign_num = rv
                                    if assign_num is None:
                                        m = re.search(r"b([ivxlcdmIVXLCDM]+)$", s)
                                        if m:
                                            rv = self.roman_to_int(m.group(1))
                                            if rv is not None:
                                                assign_num = rv
                                    if assign_num is None:
                                        word_pat = r"one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|thirteen|fourteen|fifteen|sixteen|seventeen|eighteen|nineteen|twenty"
                                        m = re.search(
                                            rf"v({word_pat})$", s, re.IGNORECASE
                                        )
                                        if m:
                                            assign_num = self.number_word_to_int(
                                                m.group(1)
                                            )
                                    if assign_num is None:
                                        m = re.search(
                                            rf"p({word_pat})$", s, re.IGNORECASE
                                        )
                                        if m:
                                            assign_num = self.number_word_to_int(
                                                m.group(1)
                                            )
                                    if assign_num is None:
                                        m = re.search(
                                            rf"Ob({word_pat})$", s, re.IGNORECASE
                                        )
                                        if m:
                                            assign_num = self.number_word_to_int(
                                                m.group(1)
                                            )
                                    if assign_num is None:
                                        m = re.search(
                                            rf"b({word_pat})$", s, re.IGNORECASE
                                        )
                                        if m:
                                            assign_num = self.number_word_to_int(
                                                m.group(1)
                                            )

                                if assign_num is None:
                                    assign_num = start_idx + row_idx
                                new_part_token = (
                                    f"{assign_num}{'e' if has_errata_suffix else ''}"
                                )
                                new_print_id = f"{ptype}{pnum}-{new_part_token}-{cong}"

                                if new_print_id == old_print_id:
                                    continue

                                # Collect updates for batch processing (granule-scoped)
                                # Map chamber from print type if chamber is currently NULL
                                # hprt/wprt -> house, sprt -> senate, jprt -> joint
                                if ptype.startswith(("h", "w")):
                                    chamber_val = "house"
                                elif ptype.startswith("s"):
                                    chamber_val = "senate"
                                elif ptype.startswith("j"):
                                    chamber_val = "joint"
                                else:
                                    chamber_val = None

                                granule_id = r.get("granule_id")
                                main_table_updates.append(
                                    (
                                        new_print_id,
                                        assign_num,
                                        chamber_val,
                                        package_id,
                                        granule_id,
                                    )
                                )

                        placeholder_count = len(
                            main_table_updates
                        )  # Count actual updates made
                        logger.info(
                            f"Chunk {chunk_idx + 1}: processed {total_rows_in_chunk} rows, found {placeholder_count} placeholders, generated {len(main_table_updates)} updates"
                        )
                        if main_table_updates:
                            logger.info(
                                f"Sample updates from chunk {chunk_idx + 1}: {main_table_updates[:3]}"
                            )

                        # Execute batch updates for this chunk
                        if main_table_updates:
                            logger.info(
                                f"Executing batch updates for chunk {chunk_idx + 1}: {len(main_table_updates)} records"
                            )
                            logger.debug(
                                f"Sample updates: {main_table_updates[:3]}"
                            )  # Show first 3 updates

                            # Update main table in batches
                            update_batch_size = 1000
                            for i in range(
                                0, len(main_table_updates), update_batch_size
                            ):
                                batch = main_table_updates[i : i + update_batch_size]
                                await conn.executemany(
                                    f"""
                                    UPDATE {self.production_schema}.printpackages
                                    SET print_id = $1,
                                        part_number = $2::int,
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

        # ? Operation 3B: Resolve PLUMBOOK placeholder 'x' parts using two-tier ordering (primary, then primary-children)
        logger.info("Starting PLUMBOOK placeholder part resolution post-processing")
        try:
            async with self.db_pool.acquire() as conn, conn.transaction():
                plum_rows = await conn.fetch(
                    f"""
                    SELECT p.package_id,
                            p.granule_id,
                            p.print_id,
                            p.is_errata
                    FROM {self.production_schema}.printpackages p
                    WHERE (p.print_id LIKE '%-x-%' OR p.print_id LIKE '%-xe-%')
                        AND p.package_id LIKE 'GPO-PLUMBOOK%'
                        AND p.granule_id IS NOT NULL
                    """
                )

                if not plum_rows:
                    results["operations"].append(
                        {
                            "name": "resolve_plumbook_placeholder_parts",
                            "status": "skipped",
                            "reason": "no PLUMBOOK placeholder reports found",
                        }
                    )
                else:
                    logger.info(f"Found {len(plum_rows)} PLUMBOOK records to process")

                    # Group by (print_type, print_number, congress)
                    groups = {}
                    for row in plum_rows:
                        m = re.match(
                            r"^([a-z]+)(\d+)-([^-]+)-(\d+)$",
                            str(row["print_id"]),
                            re.IGNORECASE,
                        )
                        if not m:
                            continue
                        key = (m.group(1).lower(), m.group(2), m.group(4))
                        groups.setdefault(key, []).append(dict(row))

                    def parse_plum_order(
                        identifier: str,
                    ) -> tuple[int, int, tuple[int, ...]]:
                        """Order PLUMBOOK granules: primary segment first, then primary-children.

                        Returns (primary, is_child, rest_tuple) so that 'primary' rows sort before children.
                        """
                        try:
                            s = str(identifier) if identifier is not None else ""
                            # Extract numeric tokens after 'GPO-PLUMBOOK-'
                            suffix = s.split("GPO-PLUMBOOK-")[-1]
                            tokens = [t for t in suffix.split("-") if t]
                            nums: list[int] = []
                            for t in tokens:
                                if t.isdigit():
                                    nums.append(int(t))
                            # Skip the year (first token) if present
                            if nums and (1000 <= nums[0] <= 3000):
                                nums = nums[1:]
                            if not nums:
                                return (1 << 30, 1, ())
                            primary = nums[0]
                            rest = tuple(nums[1:])
                            is_child = 1 if rest else 0
                            return (primary, is_child, rest)
                        except Exception:
                            return (1 << 30, 1, ())

                    updated_count = 0
                    main_table_updates = []

                    for _group_key, rows in groups.items():
                        # Sort rows by (primary, is_child, rest)
                        rows_with_order = []
                        for r in rows:
                            key_for_order = (
                                r.get("granule_id") or r.get("package_id") or ""
                            )
                            rows_with_order.append(
                                (parse_plum_order(str(key_for_order)), r)
                            )
                        rows_with_order.sort(key=lambda x: x[0])

                        # Assign sequential parts starting at 1 following the ordering
                        for idx, (_ord, r) in enumerate(rows_with_order, start=1):
                            package_id = r["package_id"]
                            old_print_id = r["print_id"]
                            is_errata_flag = (
                                bool(r["is_errata"])
                                if r["is_errata"] is not None
                                else False
                            )

                            m = re.match(
                                r"^([a-z]+)(\d+)-([^-]+)-(\d+)$",
                                str(old_print_id),
                                re.IGNORECASE,
                            )
                            if not m:
                                continue
                            ptype, pnum, part_comp, cong = (
                                m.group(1).lower(),
                                m.group(2),
                                m.group(3),
                                m.group(4),
                            )
                            has_errata_suffix = (
                                part_comp.endswith("e") or is_errata_flag
                            )
                            new_part_token = f"{idx}{'e' if has_errata_suffix else ''}"
                            new_print_id = f"{ptype}{pnum}-{new_part_token}-{cong}"
                            if new_print_id == old_print_id:
                                continue

                            # Determine chamber if missing
                            if ptype.startswith(("h", "w")):
                                chamber_val = "house"
                            elif ptype.startswith("s"):
                                chamber_val = "senate"
                            elif ptype.startswith("j"):
                                chamber_val = "joint"
                            else:
                                chamber_val = None

                            granule_id = r.get("granule_id")
                            if not granule_id:
                                continue
                            main_table_updates.append(
                                (new_print_id, idx, chamber_val, package_id, granule_id)
                            )

                    # Execute batch updates to main table
                    if main_table_updates:
                        logger.info(
                            f"Executing PLUMBOOK batch updates: {len(main_table_updates)} records"
                        )
                        update_batch_size = 1000
                        for i in range(0, len(main_table_updates), update_batch_size):
                            batch = main_table_updates[i : i + update_batch_size]
                            await conn.executemany(
                                f"""
                                UPDATE {self.production_schema}.printpackages
                                SET print_id = $1,
                                    part_number = $2::int,
                                    chamber = COALESCE(chamber, $3)
                                WHERE package_id = $4 AND granule_id = $5
                                """,
                                batch,
                            )
                            updated_count += len(batch)

                    logger.info(
                        f"Completed PLUMBOOK placeholder part resolution: {updated_count} records updated"
                    )
                    results["operations"].append(
                        {
                            "name": "resolve_plumbook_placeholder_parts",
                            "status": "success",
                            "rows_affected": updated_count,
                        }
                    )
                    results["rows_affected"] += updated_count

        except Exception as e:
            logger.error(
                f"Error resolving PLUMBOOK placeholder parts: {e}", exc_info=True
            )
            results["operations"].append(
                {
                    "name": "resolve_plumbook_placeholder_parts",
                    "status": "error",
                    "error": str(e),
                }
            )
            results["status"] = "partial_failure"

        # ? Operation 5: update parts for manual fixes
        logger.info("Starting manual part updates")
        try:
            manual_updated_total = 0
            async with self.db_pool.acquire() as conn, conn.transaction():
                for package_id, fixes in PRINT_PACKAGE_PART_FIXES.items():
                    for dct in fixes:
                        result = await conn.execute(
                            f"""
                            UPDATE {self.production_schema}.printpackages
                            SET print_id = split_part(print_id, '-', 1) || '-' ||
                                            CASE WHEN split_part(print_id, '-', 2) LIKE '%e'
                                                THEN ($1::text || 'e')
                                                ELSE $1::text
                                            END || '-' || split_part(print_id, '-', 3),
                                part_number = $1::int
                            WHERE package_id = $2 AND granule_id = $3
                            """,
                            dct["part_number"],
                            package_id,
                            dct["granule_id"],
                        )
                        affected = 0
                        try:
                            parts = result.split()
                            if len(parts) == 2:
                                affected = int(parts[1])
                        except Exception:
                            affected = 0
                        manual_updated_total += affected
            logger.info(f"Manual part updates affected {manual_updated_total} rows")
            results["operations"].append(
                {
                    "name": "update_manual_parts",
                    "status": "success",
                    "rows_affected": manual_updated_total,
                }
            )
            results["rows_affected"] += manual_updated_total
        except Exception as e:
            logger.error(f"Error while updating manual parts: {e}", exc_info=True)

        # ? Operation 6: Mark errata based on title or granule tokens (prints)
        logger.info(
            "Starting errata flag backfill for printpackages from title/granule tokens"
        )
        try:
            async with self.db_pool.acquire() as conn, conn.transaction():
                update_sql = f"""
                    WITH marks AS (
                        SELECT p.package_id
                        FROM {self.production_schema}.printpackages p
                        LEFT JOIN {self.production_schema}.printpackages_granules pg
                            ON pg.package_id = p.package_id
                        WHERE COALESCE(p.is_errata, FALSE) = FALSE
                            AND (
                                p.title ILIKE '%errata%'
                            OR (pg.granule_id IS NOT NULL AND pg.granule_id ILIKE '%-err%')
                            )
                            AND COALESCE(p.title, '') NOT ILIKE '%addendum%'
                    )
                    UPDATE {self.production_schema}.printpackages p
                    SET is_errata = TRUE,
                        print_id = (
                            split_part(p.print_id, '-', 1) || '-' ||
                            CASE
                                WHEN split_part(p.print_id, '-', 2) LIKE '%e' THEN split_part(p.print_id, '-', 2)
                                ELSE split_part(p.print_id, '-', 2) || 'e'
                            END || '-' ||
                            split_part(p.print_id, '-', 3)
                        )
                    FROM marks m
                    WHERE p.package_id = m.package_id
                """
                result = await conn.execute(update_sql)
                affected = 0
                try:
                    parts = result.split()
                    if len(parts) == 2:
                        affected = int(parts[1])
                except Exception:
                    affected = 0
                logger.info(f"Print errata backfill updated {affected} rows")
                results["operations"].append(
                    {
                        "name": "mark_errata_from_tokens",
                        "status": "success",
                        "rows_affected": affected,
                    }
                )
                results["rows_affected"] += affected
        except Exception as e:
            logger.error(f"Error backfilling print errata: {e}", exc_info=True)
            results["operations"].append(
                {
                    "name": "mark_errata_from_tokens",
                    "status": "error",
                    "error": str(e),
                }
            )
            results["status"] = "partial_failure"
