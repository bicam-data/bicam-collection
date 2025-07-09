"""
Congressional Plugins

This module provides plugins that work with the streamlined architecture using
the custom logic system. Instead of trying to import specific fetcher classes,
it creates a unified fetcher that uses the custom logic from the plugin registry.
"""

import logging
from typing import Any

logger = logging.getLogger(__name__)


class CongressionalFetcherPlugin:
    """
    Unified Congressional fetcher plugin that works with the custom logic system.

    This plugin loads custom logic from the plugin registry and provides a unified
    interface for all Congressional data types.
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
                    list_key=data_type_config.api.list_key,
                    full_key=data_type_config.api.full_key,
                ),
                id_field=f"{self.data_type}_id",
            )
        except Exception as e:
            logger.warning(f"Could not load config for {self.data_type}: {e}")

        # Return fallback config
        from types import SimpleNamespace

        return SimpleNamespace(
            api=SimpleNamespace(
                api_endpoint=self.data_type,
                list_key=self.data_type,
                full_key=self.data_type.rstrip("s")
                if self.data_type.endswith("s")
                else self.data_type,
            ),
            id_field=f"{self.data_type}_id",
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
                logger.debug(f"Fetching single page: offset={offset}, limit={limit}")

                # Use the API client's retrieve_data_list with single page parameters
                async for batch in api_client.retrieve_data_list(
                    data_type=config.api.api_endpoint,
                    from_date=api_client._format_date_for_api(from_date)
                    if from_date
                    else None,
                    to_date=api_client._format_date_for_api(to_date)
                    if to_date
                    else None,
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
                async for batch in api_client.retrieve_data_list(
                    data_type=config.api.api_endpoint,
                    from_date=api_client._format_date_for_api(from_date)
                    if from_date
                    else None,
                    to_date=api_client._format_date_for_api(to_date)
                    if to_date
                    else None,
                    limit=limit,
                    offset=offset,
                    **kwargs,
                ):
                    items = self._extract_list_items(batch, config)
                    data_batches.extend(items)

                logger.debug(f"Multi-page returned {len(data_batches)} total items")
                return data_batches

        except Exception as e:
            logger.error(f"Error fetching list data for {self.data_type}: {e}")
            return []

    def _extract_list_items(
        self, batch: dict[str, Any], config
    ) -> list[dict[str, Any]]:
        """Extract list items from API response."""
        if not batch:
            return []

        list_key = config.api.list_key
        if not list_key:
            return batch if isinstance(batch, list) else []

        # Handle single or multiple keys
        keys = list_key if isinstance(list_key, list) else [list_key]
        for key in keys:
            if key in batch:
                items = batch[key]
                return items if isinstance(items, list) else []

        logger.warning(f"No list key {list_key} found in response")
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
        config = await self._get_config()

        try:
            # Use the API client to fetch full data from URL
            full_data = await api_client.retrieve_full_data_from_url(url)
            if not full_data:
                return None

            # Extract using full_key if configured
            full_key = config.api.full_key
            if full_key and full_key in full_data:
                if isinstance(full_data[full_key], list) and len(full_data[full_key]) == 1:
                    return full_data[full_key][0]
                else:
                    return full_data[full_key]
            else:
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

        custom_logic = self._get_custom_logic_plugin(self.data_type)
        related_data = []

        try:
            # Get all related methods from custom logic
            related_methods = await self._get_related_methods(api_client)

            for method_name in related_methods:
                logger.debug(f"Calling {method_name} for {self.data_type}")
                try:
                    if hasattr(custom_logic, method_name):
                        method = getattr(custom_logic, method_name)
                        if callable(method):
                            # Call the method with detailed_data and client
                            result = await method(detailed_data, api_client)

                            if result:
                                # Extract item ID for the parent record
                                item_id = await self.extract_item_id(detailed_data)
                                related_data.append(
                                    {
                                        "type": method_name,
                                        f"{self.data_type}_id": item_id,
                                        "data": result,
                                        "method": method_name,
                                    }
                                )
                                logger.debug(
                                    f"{method_name} returned {len(result)} items"
                                )
                            else:
                                logger.debug(f"{method_name} returned no data")

                except Exception as e:
                    logger.error(f"Error calling {method_name}: {e}")

        except Exception as e:
            logger.error(f"Error fetching related data for {self.data_type}: {e}")

        return related_data

    async def _get_related_methods(self, api_client) -> list[str]:
        """
        Get list of available related data methods for this data type.

        Args:
            api_client: API client instance

        Returns:
            List of method names that can be used for Phase 3 data fetching
        """
        custom_logic = self._get_custom_logic_plugin(self.data_type)

        try:
            # Get all methods starting with get_ from the custom logic
            exclude = {
                "get_source_system_name",
                "get_default_schema",
                "get_run_type",
                "get_generic_related_data",
                "_get_generic_related_data",
            }

            methods = [
                m
                for m in dir(custom_logic)
                if m.startswith("get_")
                and callable(getattr(custom_logic, m))
                and m not in exclude
            ]

            logger.debug(
                f"Found {len(methods)} related methods for {self.data_type}: {methods}"
            )
            return methods

        except Exception as e:
            logger.error(f"Error getting related methods for {self.data_type}: {e}")
            return []

    def __getattr__(self, name: str):
        """
        Dynamically create wrapper methods for get_* methods from the custom logic.

        This creates async wrapper methods that match the signature expected by
        the streamlined fetcher: method(item_data, client)
        """
        if name.startswith("get_"):

            async def wrapper(
                item_data: dict[str, Any], client
            ) -> list[dict[str, Any]]:
                custom_logic = self._get_custom_logic_plugin(self.data_type)
                if custom_logic is None:
                    return []

                try:
                    # Check if the method exists on the custom logic
                    if hasattr(custom_logic, name):
                        method = getattr(custom_logic, name)
                        if callable(method):
                            # Call the method with item_data and client
                            result = await method(item_data, client)
                            return result if isinstance(result, list) else []
                except Exception as e:
                    logger.error(
                        f"Error calling {name} on {self.data_type} custom logic: {e}"
                    )
                    return []

                return []

            return wrapper

        raise AttributeError(
            f"'{self.__class__.__name__}' object has no attribute '{name}'"
        )

    async def extract_item_id(self, item_data: dict[str, Any]) -> str:
        """Extract item ID by delegating to custom logic."""
        custom_logic = self._get_custom_logic_plugin(self.data_type)
        if custom_logic and hasattr(custom_logic, "extract_item_id"):
            return custom_logic.extract_item_id(item_data)

        return "ID_ERROR"

    async def _extract_preliminary_item_id(self, list_item: dict[str, Any]) -> str:
        """Extract preliminary item ID for checkpoint tracking."""
        custom_logic = self._get_custom_logic_plugin(self.data_type)
        if custom_logic and hasattr(custom_logic, "_extract_preliminary_item_id"):
            result = custom_logic._extract_preliminary_item_id(list_item)
            if hasattr(result, "__await__"):
                return await result
            return result

        return await self.extract_item_id(list_item)
