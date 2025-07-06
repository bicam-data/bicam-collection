"""
Committeeprints Database Normalizer - Streamlined Implementation

This module provides committeeprints-specific database normalization logic that extends
the optimized CongressionalBaseDatabaseNormalizer.

COMMITTEEPRINTS-SPECIFIC FEATURES:
- Committeeprints data structure validation and parsing
- Committeeprints-specific related table discovery
- Custom ID field handling for congressional committeeprints
"""

import logging
from datetime import UTC, datetime
from typing import Any

from ..base import CongressionalBaseDatabaseNormalizer

logger = logging.getLogger(__name__)


class CommitteeprintsDatabaseNormalizer(CongressionalBaseDatabaseNormalizer):
    """
    Database normalizer for Congressional committeeprints data.

    Extends CongressionalBaseDatabaseNormalizer with committeeprints-specific logic for data validation
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
        """Initialize the committeeprints normalizer."""
        super().__init__(
            config_path=config_path,
            db_pool=db_pool,
            checkpoint_manager=checkpoint_manager,
            run_manager=run_manager,
            data_type_name="committeeprints",
            target_schema=target_schema,
            source_schema=source_schema,
        )

    # =============================================================================
    # COMMITTEEPRINTS-SPECIFIC IMPLEMENTATIONS
    # =============================================================================

    def _process_item_record(
        self, raw_row: dict[str, Any]
    ) -> tuple[dict[str, Any], dict[str, list[dict[str, Any]]]]:
        """
        Process a single committeeprint
        """
        # Check for data in either "payload" or "data" fields
        data = raw_row.get("payload", raw_row.get("data", {}))
        existing_id = raw_row.get("source_doc_id")

        if not existing_id:
            logger.warning("No source_doc_id found in raw data")
            return {}, {}

        # Handle different data structures with committeeprints-specific logic
        committeeprint_data = {}

        if isinstance(data, dict):
            # Case 1: Data wrapped in "committeeprint" key -> {"committeeprint": {...}}
            if "committeeprint" in data and isinstance(data["committeeprint"], dict):
                committeeprint_data = data["committeeprint"]
                logger.debug(
                    f"Found committeeprint data wrapped in 'committeeprint' key for {existing_id}"
                )
            # Case 2: Direct committeeprint data (no wrapper) with committeeprints-specific validation
            elif data and any(
                key in data for key in ["jacketnumber", "text", "associatedBills"]
            ):
                committeeprint_data = data
                logger.debug(
                    f"Found direct committeeprint data (no wrapper) for {existing_id}"
                )
            # Case 3: Empty or invalid data structure
            else:
                logger.error(
                    f"No valid committeeprint data found for {existing_id}. Data keys: {list(data.keys())}"
                )
                return {}, {}
        else:
            logger.error(
                f"Data is not a dictionary for {existing_id}. Type: {type(data)}"
            )
            return {}, {}

        # Validate that we have substantial committeeprint data
        if not committeeprint_data or len(committeeprint_data) == 0:
            logger.error(f"Committeeprint data is empty for {existing_id}")
            return {}, {}

        # Log debug info about the committeeprint data structure
        logger.debug(
            f"Processing committeeprint {existing_id} with {len(committeeprint_data)} fields: {list(committeeprint_data.keys())[:10]}"
        )

        # Use base class methods for processing
        flattened = self._flatten_dict(committeeprint_data)

        # Validate flattened data has content
        if not flattened or len(flattened) == 0:
            logger.error(
                f"Flattened data is empty for {existing_id}. Original committeeprint_data: {committeeprint_data}"
            )
            return {}, {}

        # Use existing ID and add metadata
        flattened[self.main_id_field] = existing_id
        flattened["processed_at"] = self._get_current_timestamp()

        # Extract all list fields using base class method
        extracted_lists = self._extract_lists(
            committeeprint_data, existing_id, parent_table="committeeprints"
        )

        logger.debug(
            f"Successfully processed committeeprint {existing_id}: {len(flattened)} main fields, {len(extracted_lists)} list tables"
        )
        return flattened, extracted_lists

    def _get_current_timestamp(self) -> str:
        """Get current timestamp in ISO format."""
        return datetime.now(UTC).isoformat()
