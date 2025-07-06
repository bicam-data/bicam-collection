"""
Base Plugin Protocols and Classes

This module defines the interfaces that all plugins must implement and provides
base classes with shared functionality. These protocols ensure consistency across
different data sources while allowing for custom implementations.
"""

import logging
from typing import Any, Protocol, runtime_checkable

logger = logging.getLogger(__name__)


@runtime_checkable
class FetcherPlugin(Protocol):
    """Protocol for fetcher plugins that handle data fetching logic."""

    async def fetch_list_data(
        self,
        api_client,
        from_date: str | None = None,
        to_date: str | None = None,
        limit: int = 250,
        offset: int = 0,
        **kwargs,
    ) -> list[dict[str, Any]]:
        """Fetch list of items from the API."""
        ...

    async def fetch_detailed_data(self, api_client, url: str) -> dict[str, Any] | None:
        """Fetch detailed data for a specific item."""
        ...

    async def fetch_related_data(
        self, api_client, detailed_data: dict[str, Any]
    ) -> list[dict[str, Any]] | None:
        """Fetch related data for an item (e.g., actions, cosponsors)."""
        ...

    def extract_item_id(self, data: dict[str, Any]) -> str:
        """Extract the unique identifier from item data."""
        ...


@runtime_checkable
class CleanerPlugin(Protocol):
    """Protocol for cleaner plugins that handle data cleaning logic."""

    async def clean_record(
        self, record_data: dict[str, Any], data_type: str
    ) -> dict[str, Any]:
        """Clean a single record for production use."""
        ...

    async def clean_table_data(
        self, table_data: list[dict[str, Any]], table_name: str
    ) -> list[dict[str, Any]]:
        """Clean data for a specific table."""
        ...


@runtime_checkable
class NormalizerPlugin(Protocol):
    """Protocol for normalizer plugins that handle data normalization logic."""

    async def normalize_jsonb_data(
        self, db_pool, schema: str, table: str
    ) -> dict[str, Any]:
        """Normalize JSONB data into structured tables."""
        ...

    async def extract_list_data(
        self, db_pool, schema: str, source_table: str
    ) -> dict[str, Any]:
        """Extract list data from normalized tables."""
        ...


# =============================================================================
# BASE IMPLEMENTATION CLASSES
# =============================================================================


class CongressionalBaseFetcherLogic:
    """
    Base class for Congressional fetcher logic that provides shared functionality.

    This class provides common methods like get_generic_related_data that can be
    inherited by all Congressional data type custom logic classes.
    """

    def __init__(self, data_type: str):
        self.data_type = data_type

    async def get_generic_related_data(
        self,
        full_data: dict[str, Any],
        related_table_name: str,
        client,
        list_key: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Fetch related data for a given table name using the client helper.

        This is a shared method that can be used by all Congressional data types
        to fetch related data in a standardized way.

        Args:
            full_data: The complete item payload returned by Phase-2.
            related_table_name: The field in *full_data* that contains the
                `{"url": "…"}` dict pointing to the related endpoint.
            client: API client instance for making requests.
            list_key: Optional explicit list key path to use when
                deserialising the API response. If None, a sensible default
                is derived from config or falls back to [related_table_name].

        Returns:
            A list of dictionaries representing the related entities, or an
            empty list on error / missing data.
        """
        try:
            # Basic validation of expected structure
            if related_table_name not in full_data:
                return []

            table_info = full_data[related_table_name]
            if not isinstance(table_info, dict) or "url" not in table_info:
                return []

            url = table_info["url"]

            # Derive list_key from config unless explicitly provided
            effective_list_key = None

            if list_key is not None:
                effective_list_key = list_key
            else:
                # Try to load related table config to determine correct list_key
                try:
                    from .consolidated_registry import get_consolidated_registry

                    registry = get_consolidated_registry()

                    # Related table configs are in the main data type config file
                    # as separate entries, not separate data types
                    related_config_name = f"{self.data_type}_{related_table_name}"
                    config_file = registry.get_config_file(self.data_type)

                    if config_file:
                        import yaml

                        with open(config_file) as f:
                            config_data = yaml.safe_load(f)

                        # config_data is a list of config entries, find the one for related_table
                        if isinstance(config_data, list):
                            for entry in config_data:
                                if entry.get("name") == related_config_name:
                                    api_config = entry.get("api", {})
                                    cfg_list_key = api_config.get("list_key")

                                    if cfg_list_key:
                                        # Ensure list type for downstream helper
                                        effective_list_key = (
                                            [cfg_list_key]
                                            if isinstance(cfg_list_key, str)
                                            else cfg_list_key
                                        )
                                        break
                except Exception as e:
                    logger.debug(
                        f"Could not derive list_key from config for {related_table_name}: {e}"
                    )

            # Fallback to simple default if still None
            if not effective_list_key:
                effective_list_key = [related_table_name]

            data = await client.retrieve_related_data_from_url(
                url, list_key=effective_list_key
            )

            # Ensure we always return a list
            return data or []

        except Exception as e:
            logger.error(
                f"Failed to fetch related data '{related_table_name}' for {self.data_type}: {e}"
            )
            return []
