"""
Congressional Directories GovInfo cleaner.

This module cleans Congressional Directories staging data to production tables,
handling both package-level data and member granules.
"""

import logging
from typing import Any

from bicam_collection.libs.data_type_registry import get_global_registry

from ..base import GovInfoBaseCleaner

logger = logging.getLogger(__name__)


class CongressionalDirectoriesCleaner(GovInfoBaseCleaner):
    """
    Congressional Directories cleaner.

    Cleans staging Congressional Directory data to production tables including:
    - Package-level directory metadata
    - Member granule data
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # Load config to get granule settings
        try:
            self.config = get_global_registry().get_data_type_config(
                "congressional_directories"
            )
            self.has_granules = getattr(self.config.processing, "has_granules", False)
            self.granule_name = getattr(
                self.config.processing, "granule_name", "members"
            )
        except Exception as e:
            logger.warning(f"Could not load config for congressional_directories: {e}")
            self.has_granules = True
            self.granule_name = "members"

    def get_data_types_to_process(self) -> list[str]:
        """Get list of data types to clean for Congressional Directories."""
        data_types = ["congressional_directories"]

        if self.has_granules:
            data_types.append(self.granule_name)

        return data_types

    # =============================================================================
    # CONGRESSIONAL DIRECTORIES SPECIFIC CLEANING METHODS
    # =============================================================================

    async def _clean_congressional_directories_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Clean a single Congressional Directory package record.

        Args:
            record_data: Raw Congressional Directory record

        Returns:
            Cleaned record
        """
        cleaned = record_data.copy()

        # Standardize package ID
        if "package_id" in cleaned:
            cleaned["package_id"] = str(cleaned["package_id"]).strip()

        # Clean text fields
        text_fields = ["title", "doc_title", "description", "summary"]
        for field in text_fields:
            if field in cleaned and cleaned[field]:
                cleaned[field] = self.clean_long_text(cleaned[field])

        # Standardize date fields
        date_fields = ["date_issued", "last_modified", "publish_date"]
        for field in date_fields:
            if field in cleaned:
                cleaned[field] = self.standardize_date(cleaned[field])

        # Clean numeric fields
        numeric_fields = ["congress", "session"]
        for field in numeric_fields:
            if field in cleaned:
                cleaned[field] = self.safe_int(cleaned[field])

        # Standardize boolean fields
        boolean_fields = ["is_current", "is_active"]
        for field in boolean_fields:
            if field in cleaned:
                cleaned[field] = self._standardize_boolean(cleaned[field])

        return cleaned

    async def _clean_members_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Clean a single Congressional Directory member record.

        Args:
            record_data: Raw member record

        Returns:
            Cleaned record
        """
        cleaned = record_data.copy()

        # Standardize granule ID
        if "granule_id" in cleaned:
            cleaned["granule_id"] = str(cleaned["granule_id"]).strip()

        # Clean name fields
        name_fields = [
            "first_name",
            "last_name",
            "middle_name",
            "full_name",
            "official_name",
        ]
        for field in name_fields:
            if field in cleaned and cleaned[field]:
                cleaned[field] = str(cleaned[field]).strip().title()

        # Clean title and role fields
        title_fields = ["title", "role", "position", "party"]
        for field in title_fields:
            if field in cleaned and cleaned[field]:
                cleaned[field] = str(cleaned[field]).strip()

        # Standardize party affiliation
        if "party" in cleaned:
            cleaned["party"] = self._standardize_party(cleaned["party"])

        # Standardize state codes
        if "state" in cleaned:
            cleaned["state"] = self._standardize_state_code(cleaned["state"])

        # Clean district numbers
        if "district" in cleaned:
            cleaned["district"] = self.safe_int(cleaned["district"])

        # Standardize chamber
        if "chamber" in cleaned:
            cleaned["chamber"] = self.standardize_chamber(cleaned["chamber"])

        # Clean contact information
        contact_fields = ["phone", "email", "website"]
        for field in contact_fields:
            if field in cleaned and cleaned[field]:
                cleaned[field] = str(cleaned[field]).strip()

        # Standardize date fields
        date_fields = ["start_date", "end_date", "term_start", "term_end"]
        for field in date_fields:
            if field in cleaned:
                cleaned[field] = self.standardize_date(cleaned[field])

        return cleaned

    # =============================================================================
    # UTILITY METHODS
    # =============================================================================

    def _standardize_boolean(self, value: Any) -> bool | None:
        """Standardize boolean values."""
        if value is None or value == "":
            return None

        if isinstance(value, bool):
            return value

        if isinstance(value, str):
            value_lower = value.lower().strip()
            if value_lower in ["true", "yes", "1", "y", "active", "current"]:
                return True
            elif value_lower in ["false", "no", "0", "n", "inactive", "former"]:
                return False

        if isinstance(value, int):
            return bool(value)

        return None

    def _standardize_party(self, party: str | None) -> str | None:
        """Standardize political party abbreviations."""
        if not party:
            return None

        party_lower = party.lower().strip()

        # Common party mappings
        party_map = {
            "democratic": "D",
            "democrat": "D",
            "d": "D",
            "republican": "R",
            "r": "R",
            "independent": "I",
            "ind": "I",
            "i": "I",
            "libertarian": "L",
            "lib": "L",
            "l": "L",
            "green": "G",
            "constitution": "C",
            "const": "C",
        }

        return party_map.get(party_lower, party.upper())

    def _standardize_state_code(self, state: str | None) -> str | None:
        """Standardize state codes to 2-letter format."""
        if not state:
            return None

        state_clean = state.strip().upper()

        # If already 2 characters, assume it's correct
        if len(state_clean) == 2:
            return state_clean

        # Common state name to code mappings (subset)
        state_map = {
            "ALABAMA": "AL",
            "ALASKA": "AK",
            "ARIZONA": "AZ",
            "ARKANSAS": "AR",
            "CALIFORNIA": "CA",
            "COLORADO": "CO",
            "CONNECTICUT": "CT",
            "DELAWARE": "DE",
            "FLORIDA": "FL",
            "GEORGIA": "GA",
            "HAWAII": "HI",
            "IDAHO": "ID",
            "ILLINOIS": "IL",
            "INDIANA": "IN",
            "IOWA": "IA",
            "KANSAS": "KS",
            "KENTUCKY": "KY",
            "LOUISIANA": "LA",
            "MAINE": "ME",
            "MARYLAND": "MD",
            "MASSACHUSETTS": "MA",
            "MICHIGAN": "MI",
            "MINNESOTA": "MN",
            "MISSISSIPPI": "MS",
            "MISSOURI": "MO",
            "MONTANA": "MT",
            "NEBRASKA": "NE",
            "NEVADA": "NV",
            "NEW HAMPSHIRE": "NH",
            "NEW JERSEY": "NJ",
            "NEW MEXICO": "NM",
            "NEW YORK": "NY",
            "NORTH CAROLINA": "NC",
            "NORTH DAKOTA": "ND",
            "OHIO": "OH",
            "OKLAHOMA": "OK",
            "OREGON": "OR",
            "PENNSYLVANIA": "PA",
            "RHODE ISLAND": "RI",
            "SOUTH CAROLINA": "SC",
            "SOUTH DAKOTA": "SD",
            "TENNESSEE": "TN",
            "TEXAS": "TX",
            "UTAH": "UT",
            "VERMONT": "VT",
            "VIRGINIA": "VA",
            "WASHINGTON": "WA",
            "WEST VIRGINIA": "WV",
            "WISCONSIN": "WI",
            "WYOMING": "WY",
            # Territories
            "PUERTO RICO": "PR",
            "VIRGIN ISLANDS": "VI",
            "GUAM": "GU",
            "AMERICAN SAMOA": "AS",
            "NORTHERN MARIANA ISLANDS": "MP",
            "DISTRICT OF COLUMBIA": "DC",
            "WASHINGTON D.C.": "DC",
            "WASHINGTON DC": "DC",
        }

        return state_map.get(
            state_clean, state_clean[:2] if len(state_clean) >= 2 else state_clean
        )
