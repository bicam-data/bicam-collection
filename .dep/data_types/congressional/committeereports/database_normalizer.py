"""
Committeereports Database Normalizer - Streamlined Implementation

This module provides committeereports-specific database normalization logic that extends
the optimized CongressionalBaseDatabaseNormalizer.

COMMITTEEREPORTS-SPECIFIC FEATURES:
- Committeereports data structure validation and parsing
- Committeereports-specific related table discovery
- Custom ID field handling for congressional committeereports
"""

import logging
from datetime import UTC, datetime
from typing import Any

from ..base import CongressionalBaseDatabaseNormalizer

logger = logging.getLogger(__name__)


class CommitteereportsDatabaseNormalizer(CongressionalBaseDatabaseNormalizer):
    """
    Database normalizer for Congressional committeereports data.

    Extends CongressionalBaseDatabaseNormalizer with committeereports-specific logic for data validation
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
        """Initialize the committeereports normalizer."""
        super().__init__(
            config_path=config_path,
            db_pool=db_pool,
            checkpoint_manager=checkpoint_manager,
            run_manager=run_manager,
            data_type_name="committeereports",
            target_schema=target_schema,
            source_schema=source_schema,
        )

    # =============================================================================
    # COMMITTEEREPORTS-SPECIFIC IMPLEMENTATIONS
    # =============================================================================

    def _process_item_record(
        self, raw_row: dict[str, Any]
    ) -> tuple[dict[str, Any], dict[str, list[dict[str, Any]]]]:
        """
        Process a single committeereport
        """
        # Check for data in either "payload" or "data" fields
        data = raw_row.get("payload", raw_row.get("data", {}))
        existing_id = raw_row.get("source_doc_id")

        if not existing_id:
            logger.warning("No source_doc_id found in raw data")
            return {}, {}

        # Handle different data structures with committeereports-specific logic
        committeereport_data = {}

        if isinstance(data, dict):
            # Case 1: Data wrapped in "committeereports" key -> {"committeereports": {...}}
            if "committeeReports" in data:
                if isinstance(data.get("committeeReports"), dict):
                    committeereport_data = data["committeeReports"]
                elif isinstance(data.get("committeeReports"), list):
                    committeereport_data = data["committeeReports"][0]
                else:
                    logger.error(
                        f"Invalid committeereport data structure for {existing_id}"
                    )
                    return {}, {}
                logger.debug(
                    f"Found committeereport data wrapped in 'committeeReports' key for {existing_id}"
                )
            # Case 2: Direct committeereport data (no wrapper) with committeereports-specific validation
            elif data and any(
                key in data for key in ["isConferenceReport", "reportType", "part"]
            ):
                committeereport_data = data
                logger.debug(
                    f"Found direct committeereport data (no wrapper) for {existing_id}"
                )
            # Case 3: Empty or invalid data structure
            else:
                logger.error(
                    f"No valid committeereport data found for {existing_id}. Data keys: {list(data.keys())}"
                )
                return {}, {}
        else:
            logger.error(
                f"Data is not a dictionary for {existing_id}. Type: {type(data)}"
            )
            return {}, {}

        # Validate that we have substantial committeereport data
        if not committeereport_data or len(committeereport_data) == 0:
            logger.error(f"Committeereport data is empty for {existing_id}")
            return {}, {}

        # Log debug info about the committeereport data structure
        logger.debug(
            f"Processing committeereport {existing_id} with {len(committeereport_data)} fields: {list(committeereport_data.keys())[:10]}"
        )

        # Use base class methods for processing
        flattened = self._flatten_dict(committeereport_data)

        # Validate flattened data has content
        if not flattened or len(flattened) == 0:
            logger.error(
                f"Flattened data is empty for {existing_id}. Original committeereport_data: {committeereport_data}"
            )
            return {}, {}

        # Use existing ID and add metadata
        flattened[self.main_id_field] = existing_id
        flattened["processed_at"] = self._get_current_timestamp()

        # Extract all list fields using base class method
        extracted_lists = self._extract_lists(
            committeereport_data, existing_id, parent_table="committeereports"
        )

        logger.debug(
            f"Successfully processed committeereport {existing_id}: {len(flattened)} main fields, {len(extracted_lists)} list tables"
        )
        return flattened, extracted_lists

    def _get_current_timestamp(self) -> str:
        """Get current timestamp in ISO format."""
        return datetime.now(UTC).isoformat()
