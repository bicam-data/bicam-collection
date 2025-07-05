"""
Congressional Directories GovInfo database normalizer.

This module normalizes Congressional Directories raw data to staging tables,
handling both package-level data and member granules.
"""

import logging
from typing import Any

from bicam_collection.libs.data_type_registry import get_global_registry

from ..base import GovInfoBaseDatabaseNormalizer

logger = logging.getLogger(__name__)


class CongressionalDirectoriesDatabaseNormalizer(GovInfoBaseDatabaseNormalizer):
    """
    Congressional Directories database normalizer.

    Normalizes raw Congressional Directory data to staging tables including:
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

    def extract_item_id(self, item_data: dict[str, Any]) -> str:
        """Extract package ID from Congressional Directories raw data."""
        # For raw data records, the payload contains the actual data
        if "payload" in item_data:
            import json

            try:
                payload = json.loads(item_data["payload"])
                # Try extracting from the parsed payload
                package_id = payload.get("packageId") or payload.get("package_id")
                if package_id:
                    return str(package_id)
            except (json.JSONDecodeError, AttributeError):
                pass

        # Try direct access for package/granule IDs
        package_id = item_data.get("packageId") or item_data.get("package_id")
        if package_id:
            return str(package_id)

        # Try granule ID for granule records
        granule_id = item_data.get("granuleId") or item_data.get("granule_id")
        if granule_id:
            return str(granule_id)

        # Try extracting from source_doc_id (set during storage)
        source_doc_id = item_data.get("source_doc_id")
        if source_doc_id:
            return str(source_doc_id)

        logger.warning(f"Could not extract ID from: {item_data}")
        return "UNKNOWN_ID"

    def _get_main_id_field_name(self) -> str:
        """Get the main ID field name for Congressional Directories."""
        return "package_id"

    def _get_related_tables_with_raw_data(self) -> list[str]:
        """Get list of related table suffixes that have raw data tables."""
        related_tables = ["list", "raw"]  # Collection list and package data

        if self.has_granules:
            # Add granule-related tables
            related_tables.extend([f"{self.granule_name}_list", f"{self.granule_name}"])

        return related_tables

    def get_main_table_name(self) -> str:
        """Get the main staging table name."""
        return "congressional_directories"

    def get_foreign_key_column(self) -> str:
        """Get the foreign key column name for related tables."""
        return "package_id"

    # =============================================================================
    # RAW DATA RETRIEVAL METHODS
    # =============================================================================

    async def _get_raw_package_data_by_ids(
        self, package_ids: list[str]
    ) -> list[dict[str, Any]]:
        """Get raw package data for the given package IDs."""
        if not package_ids:
            return []

        async with self.db_pool.acquire() as conn:
            # Get package data from congressional_directories_raw table
            result = await conn.fetch(
                f"""
                SELECT *
                FROM {self.source_schema}.congressional_directories_raw
                WHERE source_doc_id = ANY($1)
                ORDER BY scraped_at DESC
                """,
                package_ids,
            )
            return [dict(row) for row in result]

    async def _get_raw_granule_data_by_package_ids(
        self, package_ids: list[str]
    ) -> list[dict[str, Any]]:
        """Get raw granule data for the given package IDs."""
        if not package_ids or not self.has_granules:
            return []

        async with self.db_pool.acquire() as conn:
            # Get granule data from members_raw table
            granule_table = f"{self.granule_name}_raw"
            result = await conn.fetch(
                f"""
                SELECT *
                FROM {self.source_schema}.{granule_table}
                WHERE JSON_EXTRACT_PATH_TEXT(payload::json, 'parent_package_id') = ANY($1)
                   OR source_doc_id = ANY($1)
                ORDER BY scraped_at DESC
                """,
                package_ids,
            )
            return [dict(row) for row in result]

    async def _get_raw_collection_data_by_package_ids(
        self, package_ids: list[str]
    ) -> list[dict[str, Any]]:
        """Get raw collection data for the given package IDs."""
        if not package_ids:
            return []

        async with self.db_pool.acquire() as conn:
            # Get collection data from congressional_directories_list_raw table
            result = await conn.fetch(
                f"""
                SELECT *
                FROM {self.source_schema}.congressional_directories_list_raw
                WHERE source_doc_id = ANY($1)
                ORDER BY scraped_at DESC
                """,
                package_ids,
            )
            return [dict(row) for row in result]

    # =============================================================================
    # PROCESSING METHODS
    # =============================================================================

    async def _process_granule_records(
        self, package_ids: list[str], batch_size: int, max_concurrent: int, rerun: bool
    ) -> dict[str, Any]:
        """Process granule records for all packages."""
        total_stats = {"granule_records_processed": 0, "errors": 0}

        if not self.has_granules:
            logger.debug("Granules disabled for congressional_directories")
            return total_stats

        try:
            logger.info("Processing granule records")
            raw_data = await self._get_raw_granule_data_by_package_ids(package_ids)

            if raw_data:
                stats = await self._process_data_batch(
                    raw_data, "granules", batch_size, max_concurrent, rerun
                )
                total_stats["granule_records_processed"] += stats.get(
                    "records_processed", 0
                )
                total_stats["errors"] += stats.get("errors", 0)

        except Exception as e:
            logger.error(f"Error processing granules: {e}")
            total_stats["errors"] += 1

        return total_stats

    async def _process_related_record(
        self, payload: dict[str, Any], record_id: str, table_suffix: str
    ) -> dict[str, Any]:
        """Process a related record (collection, granules)."""
        # Handle special cases for granule data
        if table_suffix == "granules" or table_suffix.startswith(self.granule_name):
            # This is granule data - extract granule ID
            granule_id = payload.get("granuleId") or payload.get("granule_id")
            if granule_id:
                record_id = granule_id

            # Flatten the payload
            flat_item = self._flatten_dict(payload)

            # Set the appropriate ID field
            if (
                table_suffix == "granules"
                or table_suffix == f"{self.granule_name}_list"
            ):
                flat_item["granule_id"] = record_id
                flat_item["id"] = record_id
            else:
                flat_item["granule_id"] = record_id
                flat_item["id"] = record_id

            # Add parent package reference
            parent_package_id = payload.get("parent_package_id")
            if parent_package_id:
                flat_item[self.get_foreign_key_column()] = parent_package_id

            # Store the item
            if table_suffix.startswith(self.granule_name):
                actual_table_name = table_suffix
            else:
                actual_table_name = f"{self.granule_name}_{table_suffix}"

            all_table_records = {actual_table_name: [flat_item]}

            return await self._store_all_table_records(all_table_records)

        else:
            # Handle regular related records (collection data)
            return await super()._process_related_record(
                payload, record_id, table_suffix
            )

    # =============================================================================
    # HELPER METHODS
    # =============================================================================

    def get_table_names(self) -> list[str]:
        """Get all table names for Congressional Directories."""
        tables = [self.get_main_table_name()]

        # Add collection table
        tables.append("congressional_directories_list")

        # Add granule tables if enabled
        if self.has_granules:
            tables.extend([f"{self.granule_name}_list", self.granule_name])

        return tables

    async def _get_raw_data_by_ids(self, item_ids: list[str]) -> list[dict[str, Any]]:
        """Get raw data by item IDs - overridden for Congressional Directories."""
        return await self._get_raw_package_data_by_ids(item_ids)
