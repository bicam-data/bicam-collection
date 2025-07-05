"""
Congressional Directories GovInfo fetcher.

This module implements the GovInfo fetcher for Congressional Directories data,
including package-level data and member granules.
"""

import logging
from collections.abc import AsyncIterator
from typing import Any

from bicam_collection.libs.data_type_registry import get_global_registry

from ..base import GovInfoBaseFetcher

logger = logging.getLogger(__name__)


class CongressionalDirectoriesFetcher(GovInfoBaseFetcher):
    """
    Congressional Directories GovInfo fetcher.

    Implements the 4-phase GovInfo pattern:
    1. Collection data (CDIR packages)
    2. Package data (directory metadata)
    3. Granules list (members list)
    4. Granule data (individual member data)
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.collection_code = "CDIR"

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
        """Extract package ID from Congressional Directories data."""
        # Try packageId first, then package_id as fallback
        package_id = item_data.get("packageId") or item_data.get("package_id")
        if package_id:
            return str(package_id)

        # Last resort: extract from packageLink URL if available
        package_link = item_data.get("packageLink", "")
        if package_link:
            # Extract package ID from URL like: .../packages/CDIR-2023-01-01/mods
            parts = package_link.split("/")
            for part in parts:
                if part.startswith("CDIR-"):
                    return part

        logger.warning(f"Could not extract package ID from: {item_data}")
        return "UNKNOWN_PACKAGE"

    def _extract_preliminary_item_id(self, list_item: dict[str, Any]) -> str:
        """Extract preliminary ID from list item for checkpoint tracking."""
        return self.extract_item_id(list_item)

    # =============================================================================
    # PHASE 1: COLLECTION DATA
    # =============================================================================

    async def fetch_collection_data(
        self,
        from_date: str | None = None,
        to_date: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
        **kwargs,
    ) -> AsyncIterator[list[dict[str, Any]]]:
        """
        Phase 1: Fetch Congressional Directories collection data.

        Uses GovInfo API client to retrieve packages from CDIR collection.
        """
        logger.info(
            f"Fetching Congressional Directories collection data from {from_date} to {to_date}"
        )

        # Use the GovInfo API client to fetch collection data
        async for batch in self.client.retrieve_collection_data(
            collection_code=self.collection_code,
            start_date=from_date or "2000-01-01",  # Default start date if none provided
            end_date=to_date or "2030-12-31",  # Default end date if none provided
            limit=limit,
            **kwargs,
        ):
            yield batch

    # =============================================================================
    # PHASE 2: PACKAGE DATA
    # =============================================================================

    async def fetch_package_data(self, package_url: str) -> dict[str, Any] | None:
        """
        Phase 2: Fetch package metadata from Congressional Directories URL.

        Args:
            package_url: packageLink from collection data

        Returns:
            Complete package data or None on error
        """
        logger.debug(f"Fetching package data from: {package_url}")

        return await self.client.retrieve_package_data_from_url(package_url)

    # =============================================================================
    # PHASE 3: GRANULES LIST
    # =============================================================================

    async def fetch_granules_list(
        self, package_data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """
        Phase 3: Fetch granules list (members) for Congressional Directory package.

        Args:
            package_data: Complete package data from Phase 2

        Returns:
            List of granule metadata (members)
        """
        if not self.has_granules:
            logger.debug("Granules disabled for congressional_directories")
            return []

        granules_link = package_data.get("granulesLink")
        if not granules_link:
            logger.debug("No granulesLink found in package data")
            return []

        logger.debug(f"Fetching granules list from: {granules_link}")

        # Fetch granules with page size of 1000 for efficiency
        granules = await self.client.retrieve_granules_from_url(
            granules_link,
            # No granule_class filter needed for congressional directories
        )

        logger.info(f"Retrieved {len(granules)} member granules")
        return granules

    # =============================================================================
    # PHASE 4: GRANULE DATA
    # =============================================================================

    async def fetch_granule_data(self, granule_url: str) -> dict[str, Any] | None:
        """
        Phase 4: Fetch full granule data (individual member data).

        Args:
            granule_url: granuleLink from granules list

        Returns:
            Complete granule data or None on error
        """
        logger.debug(f"Fetching granule data from: {granule_url}")

        return await self.client.retrieve_package_data_from_url(granule_url)

    # =============================================================================
    # PROCESSING METHODS
    # =============================================================================

    async def _process_single_package_with_client(
        self, package_item: dict[str, Any], client
    ) -> dict[str, Any]:
        """
        Process a single Congressional Directory package through all phases.

        Args:
            package_item: Package item from collection data
            client: API client to use

        Returns:
            Complete processed package data
        """
        # Extract package ID for tracking
        package_id = self.extract_item_id(package_item)

        # Phase 1 data (collection/package list item)
        collection_data = package_item

        # Phase 2: Fetch full package data
        package_url = package_item.get("packageLink")
        if not package_url:
            raise ValueError(f"Package item missing packageLink: {package_item}")

        package_data = await client.retrieve_package_data_from_url(package_url)
        if not package_data:
            raise ValueError(f"Failed to fetch package data from: {package_url}")

        # Phase 3: Fetch granules list if enabled
        granules_list = []
        if self.has_granules and package_data:
            granules_link = package_data.get("granulesLink")
            if granules_link:
                granules_list = await client.retrieve_granules_from_url(granules_link)
                logger.debug(
                    f"Retrieved {len(granules_list)} granules for package {package_id}"
                )

        # Phase 4: Fetch full granule data
        granule_data = []
        for granule in granules_list:
            granule_link = granule.get("granuleLink")
            if granule_link:
                full_granule_data = await client.retrieve_package_data_from_url(
                    granule_link
                )
                if full_granule_data:
                    # Extract granule ID and add to data
                    granule_id = (
                        full_granule_data.get("granuleId")
                        or full_granule_data.get("granule_id")
                        or granule.get("granuleId")
                    )
                    if granule_id:
                        full_granule_data["granule_id"] = granule_id
                    granule_data.append(full_granule_data)

        logger.info(f"Processed package {package_id}: {len(granule_data)} granules")

        return {
            "package_id": package_id,
            "collection_data": collection_data,
            "package_data": package_data,
            "granules_list": granules_list,
            "granule_data": granule_data,
        }

    # =============================================================================
    # STORAGE METHODS
    # =============================================================================

    async def store_complete_package_data(
        self,
        collection_item: dict[str, Any],
        package_data: dict[str, Any],
        granules_list: list[dict[str, Any]],
        granule_data: list[dict[str, Any]],
        batch_id: str | None = None,
    ) -> None:
        """
        Store complete Congressional Directory package data across all phases.

        Args:
            collection_item: Collection/package list data
            package_data: Full package metadata
            granules_list: List of granule metadata
            granule_data: List of full granule data
            batch_id: Batch identifier for tracking
        """
        package_id = self.extract_item_id(collection_item)

        # Store collection data (Phase 1) - list of packages
        await self.store_collection_data(collection_item, batch_id)

        # Store package data (Phase 2) - full package metadata
        if package_data:
            await self.store_package_data(package_data, batch_id)

        # Store granules list (Phase 3) - member granule metadata
        if granules_list:
            await self.store_granules_data(granules_list, package_id, batch_id)

        # Store granule data (Phase 4) - full member data
        if granule_data:
            await self.store_granule_data(granule_data, package_id, batch_id)

    async def store_collection_data(
        self,
        collection_data: dict[str, Any],
        batch_id: str | None = None,
    ) -> None:
        """Store Congressional Directory collection data."""
        await super().store_collection_data(
            collection_data,
            batch_id,
            table_suffix="list_raw",  # Use list_raw for consistency
        )

    async def store_package_data(
        self,
        package_data: dict[str, Any],
        batch_id: str | None = None,
    ) -> None:
        """Store Congressional Directory package data."""
        await super().store_package_data(
            package_data,
            batch_id,
            table_suffix="raw",  # Main table uses raw suffix
        )

    async def store_granules_data(
        self,
        granules_list: list[dict[str, Any]],
        package_id: str,
        batch_id: str | None = None,
    ) -> None:
        """Store Congressional Directory granules (members) list data."""
        if not granules_list:
            return

        table_name = f"{self.granule_name}_list_raw"

        # Add parent package ID to each granule
        granules_with_parent = []
        for granule in granules_list:
            granule_with_parent = {
                **granule,
                "parent_package_id": package_id,
                "batch_id": batch_id,
            }
            granules_with_parent.append(granule_with_parent)

        await self.store_raw_data(
            schema="bicam_raw_govinfo",
            table=table_name,
            items=granules_with_parent,
            url_field="granuleLink",
            batch_id=batch_id,
        )

    async def store_granule_data(
        self,
        granule_data_list: list[dict[str, Any]],
        package_id: str,
        batch_id: str | None = None,
    ) -> None:
        """Store Congressional Directory granule (member) full data."""
        if not granule_data_list:
            return

        table_name = f"{self.granule_name}_raw"

        # Add parent package ID to each granule data item
        granule_data_with_parent = []
        for granule_data in granule_data_list:
            granule_data_with_parent_item = {
                **granule_data,
                "parent_package_id": package_id,
                "batch_id": batch_id,
            }
            granule_data_with_parent.append(granule_data_with_parent_item)

        await self.store_raw_data(
            schema="bicam_raw_govinfo",
            table=table_name,
            items=granule_data_with_parent,
            url_field="granuleLink",
            batch_id=batch_id,
        )
