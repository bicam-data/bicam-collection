"""
Base Plugin Protocols

This module defines the interfaces that all plugins must implement.
These protocols ensure consistency across different data sources while
allowing for custom implementations.
"""

from typing import Any, Protocol, runtime_checkable


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
