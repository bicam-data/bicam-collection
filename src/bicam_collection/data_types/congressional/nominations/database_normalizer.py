"""
Nominations Database Normalizer - Streamlined Implementation

This module provides nominations-specific database normalization logic that extends
the optimized CongressionalBaseDatabaseNormalizer.

NOMINATIONS-SPECIFIC FEATURES:
- Nominations data structure validation and parsing
- Nominations-specific related table discovery
- Custom ID field handling for congressional nominations
"""

import logging
from datetime import UTC, datetime
from typing import Any

from ...schema_loader import get_all_data_type_configs
from ..base import CongressionalBaseDatabaseNormalizer

logger = logging.getLogger(__name__)


class NominationsDatabaseNormalizer(CongressionalBaseDatabaseNormalizer):
    """
    Database normalizer for Congressional nominations data.

    Extends CongressionalBaseDatabaseNormalizer with nominations-specific logic for data validation
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
        """Initialize the nominations normalizer."""
        super().__init__(
            config_path=config_path,
            db_pool=db_pool,
            checkpoint_manager=checkpoint_manager,
            run_manager=run_manager,
            data_type_name="nominations",
            target_schema=target_schema,
            source_schema=source_schema,
        )

    # =============================================================================
    # NOMINATIONS-SPECIFIC IMPLEMENTATIONS
    # =============================================================================

    def _get_related_tables_with_raw_data(self) -> list[str]:
        """Get list of related table suffixes that have raw data tables for nominations."""
        try:
            all_configs = get_all_data_type_configs(self.config_path)
            related_tables = []

            for config in all_configs:
                if config.is_main or not config.create_raw:
                    continue

                if config.table_name.startswith("nominations_"):
                    table_suffix = config.table_name.replace("nominations_", "")
                    related_tables.append(table_suffix)
                else:
                    table_suffix = config.name.replace("nominations_", "")
                    related_tables.append(table_suffix)

            logger.info(f"Related tables: {related_tables}")
            return related_tables

        except Exception as e:
            logger.error(f"Error loading related tables from config: {e}")
            fallback_list = []
            logger.info(f"Using fallback list: {fallback_list}")
            return fallback_list

    def _process_item_record(
        self, raw_row: dict[str, Any]
    ) -> tuple[dict[str, Any], dict[str, list[dict[str, Any]]]]:
        """
        Process a single nomination
        """
        # Check for data in either "payload" or "data" fields
        data = raw_row.get("payload", raw_row.get("data", {}))
        existing_id = raw_row.get("source_doc_id")

        if not existing_id:
            logger.warning("No source_doc_id found in raw data")
            return {}, {}

        # Handle different data structures with nominations-specific logic
        nomination_data = {}

        if isinstance(data, dict):
            # Case 1: Data wrapped in "nomination" key -> {"nomination": {...}}
            if "nomination" in data and isinstance(data["nomination"], dict):
                nomination_data = data["nomination"]
                logger.debug(
                    f"Found nomination data wrapped in 'nomination' key for {existing_id}"
                )
            # Case 2: Direct nomination data (no wrapper)
                logger.debug(f"Found direct nomination data (no wrapper) for {existing_id}")
            # Case 3: Empty or invalid data structure
            else:
                logger.error(
                    f"No valid nomination data found for {existing_id}. Data keys: {list(data.keys())}"
                )
                return {}, {}
        else:
            logger.error(
                f"Data is not a dictionary for {existing_id}. Type: {type(data)}"
                )
            return {}, {}

        # Validate that we have substantial nomination data
        if not nomination_data or len(nomination_data) == 0:
            logger.error(f"Nomination data is empty for {existing_id}")
            return {}, {}

        # Log debug info about the nomination data structure
        logger.debug(
            f"Processing nomination {existing_id} with {len(nomination_data)} fields: {list(nomination_data.keys())[:10]}"
        )

        # Use base class methods for processing
        flattened = self._flatten_dict(nomination_data)

        # Validate flattened data has content
        if not flattened or len(flattened) == 0:
            logger.error(
                f"Flattened data is empty for {existing_id}. Original nomination_data: {nomination_data}"
            )
            return {}, {}

        # Use existing ID and add metadata
        flattened[self.main_id_field] = existing_id
        flattened["processed_at"] = self._get_current_timestamp()

        # Extract all list fields using base class method
        extracted_lists = self._extract_lists(
            nomination_data, existing_id, parent_table="nominations"
        )

        logger.debug(
        f"Successfully processed nomination {existing_id}: {len(flattened)} main fields, {len(extracted_lists)} list tables"
        )
        return flattened, extracted_lists

    def _get_current_timestamp(self) -> str:
        """Get current timestamp in ISO format."""
        return datetime.now(UTC).isoformat()
