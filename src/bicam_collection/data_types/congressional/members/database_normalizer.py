"""
Members Database Normalizer - Streamlined Implementation

This module provides members-specific database normalization logic that extends
the optimized CongressionalBaseDatabaseNormalizer.

MEMBERS-SPECIFIC FEATURES:
- Members data structure validation and parsing
- Members-specific related table discovery
- Custom ID field handling for congressional members
"""

import logging
from datetime import UTC, datetime
from typing import Any

from ...schema_loader import get_all_data_type_configs
from ..base import CongressionalBaseDatabaseNormalizer

logger = logging.getLogger(__name__)


class MembersDatabaseNormalizer(CongressionalBaseDatabaseNormalizer):
    """
    Database normalizer for Congressional members data.

    Extends CongressionalBaseDatabaseNormalizer with members-specific logic for data validation
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
        """Initialize the members normalizer."""
        super().__init__(
            config_path=config_path,
            db_pool=db_pool,
            checkpoint_manager=checkpoint_manager,
            run_manager=run_manager,
            data_type_name="members",
            target_schema=target_schema,
            source_schema=source_schema,
        )

    # =============================================================================
    # MEMBERS-SPECIFIC IMPLEMENTATIONS
    # =============================================================================

    def _get_related_tables_with_raw_data(self) -> list[str]:
        """Get list of related table suffixes that have raw data tables for members."""
        try:
            all_configs = get_all_data_type_configs(self.config_path)
            related_tables = []

            for config in all_configs:
                if config.is_main or not config.create_raw:
                    continue

                if config.table_name.startswith("members_"):
                    table_suffix = config.table_name.replace("members_", "")
                    related_tables.append(table_suffix)
                else:
                    table_suffix = config.name.replace("members_", "")
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
        Process a single member
        """
        # Check for data in either "payload" or "data" fields
        data = raw_row.get("payload", raw_row.get("data", {}))
        existing_id = raw_row.get("source_doc_id")

        if not existing_id:
            logger.warning("No source_doc_id found in raw data")
            return {}, {}

        # Handle different data structures with members-specific logic
        member_data = {}

        if isinstance(data, dict):
            # Case 1: Data wrapped in "member" key -> {"member": {...}}
            if "member" in data and isinstance(data["member"], dict):
                member_data = data["member"]
                logger.debug(
                    f"Found member data wrapped in 'member' key for {existing_id}"
                )
            # Case 2: Direct member data (no wrapper) with members-specific validation
            elif data and any(
                key in data for key in ["bioguideId", "terms", "cosponsoredLegislation"]
            ):
                member_data = data
                logger.debug(f"Found direct member data (no wrapper) for {existing_id}")
            # Case 3: Empty or invalid data structure
            else:
                logger.error(
                    f"No valid member data found for {existing_id}. Data keys: {list(data.keys())}"
                )
                return {}, {}
        else:
            logger.error(
                f"Data is not a dictionary for {existing_id}. Type: {type(data)}"
            )
            return {}, {}

        # Validate that we have substantial member data
        if not member_data or len(member_data) == 0:
            logger.error(f"Member data is empty for {existing_id}")
            return {}, {}

        # Log debug info about the member data structure
        logger.debug(
            f"Processing member {existing_id} with {len(member_data)} fields: {list(member_data.keys())[:10]}"
        )

        # Use base class methods for processing
        flattened = self._flatten_dict(member_data)

        # Validate flattened data has content
        if not flattened or len(flattened) == 0:
            logger.error(
                f"Flattened data is empty for {existing_id}. Original member_data: {member_data}"
            )
            return {}, {}

        # Use existing ID and add metadata
        flattened[self.main_id_field] = existing_id
        flattened["processed_at"] = self._get_current_timestamp()

        # Extract all list fields using base class method
        extracted_lists = self._extract_lists(
            member_data, existing_id, parent_table="members"
        )

        logger.debug(
            f"Successfully processed member {existing_id}: {len(flattened)} main fields, {len(extracted_lists)} list tables"
        )
        return flattened, extracted_lists

    def _get_current_timestamp(self) -> str:
        """Get current timestamp in ISO format."""
        return datetime.now(UTC).isoformat()
