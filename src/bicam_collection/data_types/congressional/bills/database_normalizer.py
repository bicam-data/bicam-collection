"""
Bills Database Normalizer - Streamlined Implementation

This module provides bills-specific database normalization logic that extends
the optimized BaseDatabaseNormalizer.

BILLS-SPECIFIC FEATURES:
- Bills data structure validation and parsing
- Bills-specific related table discovery
- Custom ID field handling for congressional bills
"""

import logging
from datetime import UTC, datetime
from typing import Any

from ..base import CongressionalBaseDatabaseNormalizer

logger = logging.getLogger(__name__)


class BillsDatabaseNormalizer(CongressionalBaseDatabaseNormalizer):
    """
    Database normalizer for Congressional bills data.

    Extends BaseDatabaseNormalizer with bills-specific logic for data validation
    and structure handling.
    """

    def __init__(
        self,
        config_path: str | None = None,
        db_pool=None,
        checkpoint_manager=None,
        run_manager=None,
        target_schema: str = "bicam_staging_congressional",
        source_schema: str = "bicam_raw_congressional",
    ):
        """Initialize the bills normalizer."""
        super().__init__(
            config_path=config_path,
            db_pool=db_pool,
            checkpoint_manager=checkpoint_manager,
            run_manager=run_manager,
            data_type_name="bills",
            target_schema=target_schema,
            source_schema=source_schema,
        )

    # =============================================================================
    # BILLS-SPECIFIC IMPLEMENTATIONS
    # =============================================================================

    def _process_item_record(
        self, raw_row: dict[str, Any]
    ) -> tuple[dict[str, Any], dict[str, list[dict[str, Any]]]]:
        """
        Process a single bill record with bills-specific validation logic.

        Overrides the base method to add bills-specific data structure handling.
        """
        # Check for data in either "payload" or "data" fields
        data = raw_row.get("payload", raw_row.get("data", {}))
        existing_id = raw_row.get("source_doc_id")

        if not existing_id:
            logger.warning("No source_doc_id found in raw data")
            return {}, {}

        # Handle different data structures with bills-specific logic
        bill_data = {}

        if isinstance(data, dict):
            # Case 1: Data wrapped in "bill" key -> {"bill": {...}}
            if "bill" in data and isinstance(data["bill"], dict):
                bill_data = data["bill"]
                logger.debug(f"Found bill data wrapped in 'bill' key for {existing_id}")
            # Case 2: Direct bill data (no wrapper) with bills-specific validation
            elif data and any(
                key in data for key in ["type", "number", "congress", "title", "url"]
            ):
                bill_data = data
                logger.debug(f"Found direct bill data (no wrapper) for {existing_id}")
            # Case 3: Empty or invalid data structure
            else:
                logger.error(
                    f"No valid bill data found for {existing_id}. Data keys: {list(data.keys())}"
                )
                return {}, {}
        else:
            logger.error(
                f"Data is not a dictionary for {existing_id}. Type: {type(data)}"
            )
            return {}, {}

        # Validate that we have substantial bill data
        if not bill_data or len(bill_data) == 0:
            logger.error(f"Bill data is empty for {existing_id}")
            return {}, {}

        # Log debug info about the bill data structure
        logger.debug(
            f"Processing bill {existing_id} with {len(bill_data)} fields: {list(bill_data.keys())[:10]}"
        )

        # Use base class methods for processing
        flattened = self._flatten_dict(bill_data)

        # Validate flattened data has content
        if not flattened or len(flattened) == 0:
            logger.error(
                f"Flattened data is empty for {existing_id}. Original bill_data: {bill_data}"
            )
            return {}, {}

        # Use existing ID and add metadata
        flattened[self.main_id_field] = existing_id
        flattened["processed_at"] = self._get_current_timestamp()

        # Extract all list fields using base class method
        extracted_lists = self._extract_lists(
            bill_data, existing_id, parent_table="bills"
        )

        logger.debug(
            f"Successfully processed bill {existing_id}: {len(flattened)} main fields, {len(extracted_lists)} list tables"
        )
        return flattened, extracted_lists

    def _get_current_timestamp(self) -> str:
        """Get current timestamp in ISO format."""
        return datetime.now(UTC).isoformat()
