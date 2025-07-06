"""
Bills Custom Plugin Logic

This module contains all the custom logic for bills data type including:
- Fetching logic (from fetcher.py)
- Database normalization logic (from database_normalizer.py)
- Cleaning logic (from cleaner.py)

All logic is organized into classes that can be used by the plugin system.
"""

import logging
import re
from typing import Any

# Import the base class that provides shared functionality
from ....base import CongressionalBaseFetcherLogic

logger = logging.getLogger(__name__)


class CongressesFetcherLogic(CongressionalBaseFetcherLogic):
    """
    Congresses-specific fetcher logic that extends the base Congressional fetcher logic.

    This class provides custom methods for fetching congresses data, including:
    - Extracting standardized bill IDs
    - Handling related data from URLs
    - Implementing specialized fetching logic for congresses-specific data

    Inherits shared functionality like get_generic_related_data from
    CongressionalBaseFetcherLogic.
    """

    def __init__(self, data_type: str = "congresses"):
        super().__init__(data_type)

    def extract_item_id(self, item_data: dict[str, Any]) -> str:
        """Extract standardized congress ID from congress data."""
        return str(item_data.get("number", "ID_ERROR"))

    def _extract_preliminary_item_id(self, list_item: dict[str, Any]) -> str:
        """
        Derive a stable ID for checkpointing directly from the list-response record.
        This guarantees a non-null value so the progress-tracker can reliably
        decide whether the item was processed in a previous run.
        """

        # 1. direct field
        prelim_id_field = list_item.get("name")
        if prelim_id_field:
            # only take the digits before the first space
            prelim_id_prefix = prelim_id_field.split(" ")[0]
            prelim_id = re.sub(r"\D", "", prelim_id_prefix)
            return str(prelim_id)
        else:
            return "ID_ERROR"
