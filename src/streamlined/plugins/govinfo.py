"""
Govinfo Plugins

This module provides plugins that work with the streamlined architecture using
the custom logic system. Instead of trying to import specific fetcher classes,
it creates a unified fetcher that uses the custom logic from the plugin registry.
"""

import logging
from typing import Any

logger = logging.getLogger(__name__)


class GovInfoFetcherPlugin:
    """
    Unified GovInfo fetcher plugin that works with the custom logic system.

    This plugin loads custom logic from the plugin registry and provides a unified
    interface for all GovInfo data types.
    """

    def __init__(self, data_type: str):
        self.data_type = data_type
        self._custom_logic = None
        self._config = None

    async def _get_config(self):
        """Get configuration for this data type."""
        if self._config is None:
            self._config = await self._load_config()
        return self._config

    async def _load_config(self):
        """Load configuration for this data type."""
        try:
            # Load configuration from the consolidated registry which handles both list and dict YAML
            from .consolidated_registry import get_consolidated_registry

            registry = get_consolidated_registry()
            data_type_config = registry.get_data_type_config(self.data_type)

            # Convert to SimpleNamespace for easier access
            from types import SimpleNamespace

            return SimpleNamespace(
                api=SimpleNamespace(
                    api_endpoint=data_type_config.api.api_endpoint,
                    granule_class=getattr(data_type_config.api, "granule_class", None),
                    doc_class=getattr(data_type_config.api, "doc_class", None),
                ),
                schema=SimpleNamespace(),
                processing=SimpleNamespace(),
                id_field="package_id",
            )
        except Exception as e:
            logger.warning(f"Could not load config for {self.data_type}: {e}")

        # Return fallback config
        from types import SimpleNamespace

        return SimpleNamespace(
            api=SimpleNamespace(
                api_endpoint=self.data_type,
                granule_class=None,
                doc_class=None,
            ),
            processing=SimpleNamespace(),
            id_field="package_id",
        )

    def _get_custom_logic_plugin(self, data_type: str):
        from .consolidated_registry import get_consolidated_registry

        if self._custom_logic is None:
            registry = get_consolidated_registry()
            self._custom_logic = registry.get_custom_logic_plugin(data_type, "fetching")

        return self._custom_logic

    async def fetch_list_data(
        self,
        api_client,
        from_date: str | None = None,
        to_date: str | None = None,
        limit: int = 250,
        offset: int = 0,
        single_page_only: bool = False,
        **kwargs,
    ) -> list[dict[str, Any]]:
        """
        Fetch list data using the Congressional API client.

        Args:
            api_client: API client instance
            from_date: Start date filter (YYYY-MM-DD)
            to_date: End date filter (YYYY-MM-DD)
            limit: Items per page
            offset: Offset for pagination
            single_page_only: If True, only fetch one page (used in parallel processing)
            **kwargs: Additional parameters

        Returns:
            List of items from the API
        """
        config = await self._get_config()

        # Validate that we have a valid data_type
        if not self.data_type:
            logger.error(f"data_type is None or empty: {self.data_type}")
            return []

        # Validate that we have a valid api_endpoint
        if not config.api.api_endpoint:
            logger.error(
                f"api_endpoint is None or empty for data_type {self.data_type}: {config.api.api_endpoint}"
            )
            return []

        try:
            # Handle single page requests (used in parallel processing)
            if single_page_only:
                # For single page requests, fetch a specific page using offset/limit
                logger.info(f"Fetching single page: offset={offset}, limit={limit}")

                # Use the API client's retrieve_data_list with single page parameters
                async for batch in api_client.retrieve_collection_data(
                    collection_code=config.api.api_endpoint,
                    start_date=api_client._format_date_for_api(from_date)
                    if from_date
                    else None,
                    end_date=api_client._format_date_for_api(to_date)
                    if to_date
                    else None,
                    doc_class=config.api.doc_class,
                    limit=limit,
                    offset=offset,
                    **kwargs,
                ):
                    # For single page, return only the first batch's items
                    items = self._extract_list_items(batch, config)
                    logger.debug(f"Single page returned {len(items)} items")
                    return items

                # If no data returned
                return []
            else:
                # For multi-page requests, collect all batches
                logger.debug("Fetching multi-page data")
                data_batches = []
                async for batch in api_client.retrieve_collection_data(
                    collection_code=config.api.api_endpoint,
                    start_date=api_client._format_date_for_api(from_date)
                    if from_date
                    else None,
                    end_date=api_client._format_date_for_api(to_date)
                    if to_date
                    else None,
                    doc_class=config.api.doc_class,
                    limit=limit,
                    offset=offset,
                    **kwargs,
                ):
                    items = self._extract_list_items(batch, config)
                    data_batches.extend(items)

                logger.debug(f"Multi-page returned {len(data_batches)} total items")
                return data_batches

        except Exception as e:
            logger.error(
                f"Error fetching list data for {self.data_type}: {e}", exc_info=True
            )
            return []

    def _extract_list_items(
        self, batch: dict[str, Any], config
    ) -> list[dict[str, Any]]:
        """Extract list items from API response."""
        if not batch:
            return []

        logger.debug(
            f"Extracting list items from batch type: {type(batch)}, length: {len(batch) if hasattr(batch, '__len__') else 'N/A'}"
        )

        # For GovInfo API, the batch IS the packages list
        # The API client already extracts packages from the response
        if isinstance(batch, list):
            logger.debug(f"Batch is a list with {len(batch)} items")
            # Add URL field for Phase 2 compatibility
            for item in batch:
                logger.debug(f"Processing item with keys: {list(item.keys())}")
                if "packageLink" in item and "url" not in item:
                    item["url"] = item["packageLink"]
                    logger.debug(f"Added url field: {item['url']}")
                elif "url" not in item:
                    logger.warning(f"Item missing both packageLink and url: {item}")
            return batch

        # Fallback: if it's a dict, look for packages key
        if isinstance(batch, dict):
            logger.debug(f"Batch is a dict with keys: {list(batch.keys())}")
            packages = batch.get("packages", [])
            if packages:
                logger.debug(f"Found packages key with {len(packages)} items")
                # Add URL field for Phase 2 compatibility
                for item in packages:
                    logger.debug(f"Processing item with keys: {list(item.keys())}")
                    if "packageLink" in item and "url" not in item:
                        item["url"] = item["packageLink"]
                        logger.debug(f"Added url field: {item['url']}")
                    elif "url" not in item:
                        logger.warning(f"Item missing both packageLink and url: {item}")
                return packages if isinstance(packages, list) else []

        logger.warning(f"Unexpected batch format: {type(batch)}")
        return []

    async def fetch_detailed_data(self, api_client, url: str) -> dict[str, Any] | None:
        """
        Fetch detailed data using the Congressional API client.

        Args:
            api_client: API client instance
            url: URL to fetch detailed data from

        Returns:
            Detailed item data or None if error
        """
        try:
            # Use the API client to fetch full data from URL
            full_data = await api_client.retrieve_package_data_from_url(url)
            if not full_data:
                return None

            return full_data

        except Exception as e:
            logger.error(f"Error fetching detailed data from {url}: {e}")
            return None

    async def fetch_related_data(
        self, api_client, detailed_data: dict[str, Any]
    ) -> list[dict[str, Any]] | None:
        """
        Fetch related data using the custom logic system.

        Args:
            api_client: API client instance
            detailed_data: Full item data from Phase 2

        Returns:
            List of related data items or None if error
        """
        if not detailed_data:
            return []

        config = await self._get_config()
        related_data = []

        try:
            granules_url = await self.get_granules_url(detailed_data)
            if not granules_url:
                return []

            granules_data = await api_client.retrieve_granules_from_url(
                granules_url, granule_class=config.api.granule_class or None
            )
            if not granules_data:
                return []
            else:
                # Use consistent naming scheme
                granule_type = f"{self.data_type}_granules"

                related_data.append(
                    {
                        "type": granule_type,
                        "data": granules_data,
                        "method": "granules",
                    }
                )
                logger.debug(f"granules returned {len(granules_data)} items")

            return related_data

        except Exception as e:
            logger.error(f"Error fetching related data for {self.data_type}: {e}")

        return related_data

    async def fetch_full_related_data(
        self, api_client, related_data: dict[str, Any]
    ) -> dict[str, Any] | None:
        """
        Get full granule data from granule URL.
        """

        granules_url = related_data.get("granuleLink")
        try:
            # Use the API client to fetch full data from URL
            full_data = await api_client.retrieve_granule_data_from_url(granules_url)
            if not full_data:
                return None

            return full_data

        except Exception as e:
            logger.error(f"Error fetching detailed data from {granules_url}: {e}")
            return None

    async def get_granules_url(self, package_data: dict[str, Any]) -> list[str]:
        """
        Get granules URL from package data.
        """
        return package_data.get("granulesLink", [])

    async def extract_item_id(self, item_data: dict[str, Any], **kwargs) -> str:
        """Extract item ID by delegating to custom logic."""
        if kwargs.get("granule") is True:
            return item_data.get("granuleId", "ID_ERROR")
        return item_data.get("packageId", "ID_ERROR")

    async def _extract_preliminary_item_id(self, list_item: dict[str, Any]) -> str:
        """Extract preliminary item ID for checkpoint tracking."""
        return list_item.get("packageId", "ID_ERROR")
