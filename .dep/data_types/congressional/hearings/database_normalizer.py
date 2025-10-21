"""
Hearings Database Normalizer - Streamlined Implementation

This module provides hearings-specific database normalization logic that extends
the optimized CongressionalBaseDatabaseNormalizer.

HEARINGS-SPECIFIC FEATURES:
- Hearings data structure validation and parsing
- Hearings-specific related table discovery
- Custom ID field handling for congressional hearings
"""

import logging
from datetime import UTC, datetime
from typing import Any

from ..base import CongressionalBaseDatabaseNormalizer

logger = logging.getLogger(__name__)


class HearingsDatabaseNormalizer(CongressionalBaseDatabaseNormalizer):
    """
    Database normalizer for Congressional hearings data.

    Extends CongressionalBaseDatabaseNormalizer with hearings-specific logic for data validation
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
        """Initialize the committees normalizer."""
        super().__init__(
            config_path=config_path,
            db_pool=db_pool,
            checkpoint_manager=checkpoint_manager,
            run_manager=run_manager,
            data_type_name="hearings",
            target_schema=target_schema,
            source_schema=source_schema,
        )

    # =============================================================================
    # HEARINGS-SPECIFIC IMPLEMENTATIONS
    # =============================================================================

    def _process_item_record(
        self, raw_row: dict[str, Any]
    ) -> tuple[dict[str, Any], dict[str, list[dict[str, Any]]]]:
        """
        Process a single hearing
        """
        # Check for data in either "payload" or "data" fields
        data = raw_row.get("payload", raw_row.get("data", {}))
        existing_id = raw_row.get("source_doc_id")

        if not existing_id:
            logger.warning("No source_doc_id found in raw data")
            return {}, {}

        # Handle different data structures with hearings-specific logic
        hearing_data = {}

        if isinstance(data, dict):
            # Case 1: Data wrapped in "hearing" key -> {"hearing": {...}}
            if "hearing" in data and isinstance(data["hearing"], dict):
                hearing_data = data["hearing"]
                logger.debug(
                    f"Found hearing data wrapped in 'hearing' key for {existing_id}"
                )
            # Case 2: Direct hearing data (no wrapper) with hearings-specific validation
            elif data and any(
                key in data
                for key in ["jacketNumber", "citation", "libraryOfCongressIdentifier"]
            ):
                hearing_data = data
                logger.debug(
                    f"Found direct hearing data (no wrapper) for {existing_id}"
                )
            # Case 3: Empty or invalid data structure
            else:
                logger.error(
                    f"No valid hearing data found for {existing_id}. Data keys: {list(data.keys())}"
                )
                return {}, {}
        else:
            logger.error(
                f"Data is not a dictionary for {existing_id}. Type: {type(data)}"
            )
            return {}, {}

        # Validate that we have substantial hearing data
        if not hearing_data or len(hearing_data) == 0:
            logger.error(f"Hearing data is empty for {existing_id}")
            return {}, {}

        # Log debug info about the committee data structure
        logger.debug(
            f"Processing hearing {existing_id} with {len(hearing_data)} fields: {list(hearing_data.keys())[:10]}"
        )

        # Use base class methods for processing
        flattened = self._flatten_dict(hearing_data)

        # Validate flattened data has content
        if not flattened or len(flattened) == 0:
            logger.error(
                f"Flattened data is empty for {existing_id}. Original hearing_data: {hearing_data}"
            )
            return {}, {}

        # Use existing ID and add metadata
        flattened[self.main_id_field] = existing_id
        flattened["processed_at"] = self._get_current_timestamp()

        # Extract all list fields using base class method
        extracted_lists = self._extract_lists(
            hearing_data, existing_id, parent_table="hearings"
        )

        logger.debug(
            f"Successfully processed hearing {existing_id}: {len(flattened)} main fields, {len(extracted_lists)} list tables"
        )
        return flattened, extracted_lists

    def _get_current_timestamp(self) -> str:
        """Get current timestamp in ISO format."""
        return datetime.now(UTC).isoformat()
