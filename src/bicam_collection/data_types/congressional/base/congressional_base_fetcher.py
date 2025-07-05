"""
Streamlined Congressional-specific base fetcher.

This version removes all redundant code and focuses only on Congressional-specific
implementations required by the abstract base class.
"""

import json
import logging
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any

import asyncpg

from ....libs.run_tracking import RunType
from ...abstract.base_fetcher import BaseFetcher

logger = logging.getLogger(__name__)


class CongressionalBaseFetcher(BaseFetcher):
    """
    Streamlined Congressional-specific fetcher for 3-phase processing.

    Only implements what's absolutely necessary:
    - Congressional API response parsing
    - ID extraction logic
    - Storage with Congressional schema
    - Phase-specific data fetching
    """

    def __init__(
        self,
        client,
        db_pool=None,
        data_type_name=None,
        checkpoint_manager=None,
        run_manager=None,
    ):
        super().__init__(
            client, db_pool, data_type_name, checkpoint_manager, run_manager
        )
        self._load_config()

    def _load_config(self):
        """Load data type configuration."""
        self.config = None
        if self.data_type_name:
            try:
                from ....libs.data_type_registry import get_global_registry

                registry = get_global_registry()
                self.config = registry.get_data_type_config(self.data_type_name)
                self.id_field = (
                    self.config.id_field if self.config else f"{self.data_type_name}_id"
                )
            except Exception as e:
                logger.warning(f"Could not load config for {self.data_type_name}: {e}")
                self.id_field = f"{self.data_type_name}_id"

    # =============================================================================
    # REQUIRED IMPLEMENTATIONS
    # =============================================================================

    def get_source_system_name(self) -> str:
        return "congressional"

    def get_default_schema(self) -> str:
        return "bicam_raw_congressional"

    def get_run_type(self) -> RunType:
        return RunType.SCRAPER_CONGRESSIONAL

    # =============================================================================
    # PHASE 1: LIST DATA
    # =============================================================================

    async def fetch_phase_1_data(
        self, from_date=None, to_date=None, limit=None, **kwargs
    ) -> AsyncIterator[list[dict]]:
        """Fetch Congressional list data."""
        if not self.config:
            logger.error("No configuration loaded")
            return

        endpoint = self.config.api.api_endpoint or self.data_type_name
        async for batch in self.client.retrieve_data_list(
            data_type=endpoint,
            from_date=self.client._format_date_for_api(from_date)
            if from_date
            else None,
            to_date=self.client._format_date_for_api(to_date) if to_date else None,
            limit=limit if not kwargs.get("pagination_request") else 0,
            **kwargs,
        ):
            yield (
                batch
                if kwargs.get("pagination_request")
                else self._extract_list_items(batch)
            )

    async def fetch_phase_1_data_with_client(
        self, client, **kwargs
    ) -> AsyncIterator[list[dict]]:
        """Fetch Phase 1 with specific client."""
        # Swap client temporarily
        original = self.client
        self.client = client
        try:
            async for batch in self.fetch_phase_1_data(**kwargs):
                yield batch
        finally:
            self.client = original

    def _extract_list_items(self, batch: dict[str, Any]) -> list[dict[str, Any]]:
        """Extract list items from API response."""
        list_key = self.config.api.list_key
        if not list_key:
            return batch if isinstance(batch, list) else []

        # Handle single or multiple keys
        keys = list_key if isinstance(list_key, list) else [list_key]
        for key in keys:
            if key in batch:
                return batch[key]

        logger.warning(f"No list key {list_key} found in response")
        return []

    # =============================================================================
    # PHASE 2: FULL DATA
    # =============================================================================

    async def fetch_phase_2_data(self, item_url: str) -> dict[str, Any] | None:
        """Fetch complete item data."""
        try:
            full_data = await self.client.retrieve_full_data_from_url(item_url)
            if not full_data or not self.config:
                return full_data

            # Extract using full_key if configured
            full_key = self.config.api.full_key
            return full_data.get(full_key, full_data) if full_key else full_data

        except Exception as e:
            logger.error(f"Phase 2 fetch failed for {item_url}: {e}")
            return None

    async def fetch_phase_2_data_with_client(
        self, item_url: str, client
    ) -> dict[str, Any] | None:
        """Fetch Phase 2 with specific client."""
        original = self.client
        self.client = client
        try:
            return await self.fetch_phase_2_data(item_url)
        finally:
            self.client = original

    # =============================================================================
    # PHASE 3: RELATED DATA
    # =============================================================================

    async def fetch_phase_3_data(
        self, item_data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Fetch related data using get_* methods."""
        related_data = []
        item_id = self.extract_item_id(item_data)

        # Find all get_* methods (e.g., get_actions, get_amendments)
        for method_name in self._get_related_methods():
            try:
                method = getattr(self, method_name)
                result = await method(item_data)

                if result:
                    related_data.append(
                        {
                            "type": method_name,
                            "parent_id": item_id,
                            "data": result,
                            "method": method_name,
                        }
                    )

            except Exception as e:
                logger.error(f"Failed to fetch {method_name}: {e}")

        return related_data

    async def fetch_phase_3_data_with_client(
        self, item_data: dict[str, Any], client
    ) -> list[dict[str, Any]]:
        """Fetch Phase 3 with specific client."""
        original = self.client
        self.client = client
        try:
            return await self.fetch_phase_3_data(item_data)
        finally:
            self.client = original

    def _get_related_methods(self) -> list[str]:
        """Get list of related data methods."""
        exclude = {
            "get_source_system_name",
            "get_default_schema",
            "get_run_type",
            "get_generic_related_data",  # helper, not a fetcher
            "_get_related_methods",
        }
        return [
            m
            for m in dir(self)
            if m.startswith("get_") and callable(getattr(self, m)) and m not in exclude
        ]

    # =============================================================================
    # ID EXTRACTION
    # =============================================================================

    def extract_item_id(self, item_data: dict[str, Any]) -> str:
        """Extract ID from Congressional item."""
        return (
            item_data.get("number")
            or item_data.get("id")
            or item_data.get("congress", {}).get("number", "unknown")
        )

    def _extract_preliminary_item_id(self, list_item: dict[str, Any]) -> str:
        """Extract ID from list item."""
        return (
            list_item.get("number")
            or list_item.get("id")
            or f"item_{hash(str(list_item))}"
        )

    # =============================================================================
    # STORAGE
    # =============================================================================

    async def store_phase_1_data(self, data: dict, batch_id: str | None = None) -> None:
        """Store Phase 1 list data."""
        if not self.db_pool:
            return

        await self.store_raw_data(
            schema=self.get_default_schema(),
            table=f"{self.data_type_name}_list_raw",
            items=[data],
            batch_id=batch_id,
        )

    async def store_phase_2_data(self, data: dict, batch_id: str | None = None) -> None:
        """Store Phase 2 full data."""
        if not self.db_pool:
            return

        await self.store_raw_data(
            schema=self.get_default_schema(),
            table=f"{self.data_type_name}_raw",
            items=[data],
            batch_id=batch_id,
        )

    async def store_phase_3_data(
        self, data: list[dict], parent_id: str, batch_id: str | None = None
    ) -> None:
        """Store Phase 3 related data."""
        if not self.db_pool or not data:
            return

        # Group by type and store in appropriate tables
        for item in data:
            method_name = item.get("type", "unknown")
            related_data = item.get("data", [])

            if not related_data:
                continue

            # Extract table name from method (e.g., get_actions -> actions)
            table_suffix = (
                method_name[4:] if method_name.startswith("get_") else method_name
            )
            if table_suffix.startswith(f"{self.data_type_name}_"):
                table_suffix = table_suffix[len(f"{self.data_type_name}_") :]

            # Add parent reference to each item
            for record in related_data:
                if isinstance(record, dict):
                    record["parent_id"] = parent_id

            await self.store_raw_data(
                schema=self.get_default_schema(),
                table=f"{self.data_type_name}_{table_suffix}_raw",
                items=related_data,
                batch_id=batch_id,
                parent_source_doc_id=parent_id,
            )

    async def store_raw_data(
        self,
        schema: str,
        table: str,
        items: list[dict],
        batch_id: str | None = None,
        parent_source_doc_id: str | None = None,
        **kwargs,
    ) -> None:
        """Store raw data with minimal complexity."""
        if not items:
            return

        async with self.db_pool.acquire() as conn:
            # Ensure table exists
            await self._ensure_table(conn, schema, table)

            # Prepare and insert data
            for item in items:
                source_doc_id = self._get_source_doc_id(
                    item, table, parent_source_doc_id
                )

                prepared = {
                    "id_uuid": str(uuid.uuid4()),
                    "url": item.get("url"),
                    "batch_id": batch_id or str(uuid.uuid4()),
                    "scraped_at": datetime.now(UTC),
                    "payload": json.dumps(item, default=str),
                    "source_doc_id": source_doc_id,
                    "etl_batch_id": batch_id or str(uuid.uuid4()),
                }

                await conn.execute(
                    f"""
                    INSERT INTO {schema}.{table}
                    (id_uuid, url, batch_id, scraped_at, payload, source_doc_id, etl_batch_id)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    ON CONFLICT (id_uuid) DO UPDATE SET
                        scraped_at = EXCLUDED.scraped_at,
                        payload = EXCLUDED.payload
                """,
                    *prepared.values(),
                )

    def _get_source_doc_id(self, item: dict, table: str, parent_id: str | None) -> str:
        """Get appropriate source doc ID based on phase."""
        if table.endswith("_list_raw"):
            return self._extract_preliminary_item_id(item)
        elif parent_id:
            return parent_id
        else:
            return self.extract_item_id(item)

    async def _ensure_table(
        self, conn: asyncpg.Connection, schema: str, table: str
    ) -> None:
        """Ensure table exists."""
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                id_uuid TEXT PRIMARY KEY,
                url TEXT,
                batch_id TEXT,
                scraped_at TIMESTAMPTZ,
                payload JSONB,
                source_doc_id TEXT,
                etl_batch_id TEXT
            )
        """)

    # =============================================================================
    # OPTIONAL OVERRIDES
    # =============================================================================

    def _extract_pagination_info(self, page_data: dict[str, Any]) -> dict[str, Any]:
        """Extract pagination from Congressional API response."""
        # Congressional API includes count in pagination object
        logger.error(f"Page data: {page_data}")

        if isinstance(page_data, dict) and page_data.get("pagination"):
            pagination = page_data["pagination"]
            return {
                "total_count": pagination.get("count", 0),
                "count_per_page": pagination.get("per_page", 250),
            }

        # # If page_data is a list, count the items
        # if isinstance(page_data, list):
        #     return {
        #         "total_count": len(page_data),
        #         "count_per_page": len(page_data),
        #     }

        # Fallback for other structures
        return {"total_count": 0, "count_per_page": 250}

    def _extract_latest_date_from_batch(self, batch: list[dict]) -> str | None:
        """Extract latest date from Congressional items."""
        dates = []
        for item in batch:
            date_str = (
                item.get("updateDate")
                or item.get("lastModifiedDate")
                or item.get("date")
            )
            if date_str:
                dates.append(date_str)
        return max(dates) if dates else None

    # =============================================================================
    # GENERIC RELATED DATA FETCHER
    # =============================================================================

    async def get_generic_related_data(
        self,
        full_data: dict[str, Any],
        related_table_name: str,
        list_key: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch related data for a given table name using the client helper.

        This is a **minimal** replacement for the more elaborate method that
        existed in the legacy fetcher.  The goal is to support the common
        `{data_type}` helper methods (e.g. `get_bills_actions`) that simply
        pass the table name.

        Args:
            full_data: The complete item payload returned by Phase-2.
            related_table_name: The field in *full_data* that contains the
                `{"url": "…"}` dict pointing to the related endpoint.
            list_key: Optional explicit list key path to use when
                deserialising the API response.  If ``None`` a sensible default
                of ``[related_table_name]`` is used.

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
                    from bicam_collection.libs.data_type_registry import (
                        get_global_registry,
                    )

                    registry = get_global_registry()
                    related_config_name = f"{self.data_type_name}_{related_table_name}"
                    if registry.is_registered(related_config_name):
                        rel_cfg = registry.get_data_type_config(related_config_name)
                        api_cfg = getattr(rel_cfg, "api", None)
                        cfg_list_key = (
                            getattr(api_cfg, "list_key", None) if api_cfg else None
                        )

                        if cfg_list_key:
                            # Ensure list type for downstream helper
                            effective_list_key = (
                                [cfg_list_key]
                                if isinstance(cfg_list_key, str)
                                else cfg_list_key
                            )
                except Exception as e:
                    logger.debug(
                        f"Could not derive list_key from config for {related_table_name}: {e}"
                    )

            # Fallback to simple default if still None
            if not effective_list_key:
                effective_list_key = [related_table_name]

            data = await self.client.retrieve_related_data_from_url(
                url, list_key=effective_list_key
            )

            # Ensure we always return a list
            return data or []

        except Exception as e:
            logger.error(
                f"Failed to fetch related data '{related_table_name}' for {self.data_type_name}: {e}"
            )
            return []

    # =============================================================================
    # PAGINATION METADATA ANALYSIS
    # =============================================================================

    async def _get_pagination_metadata(
        self,
        from_date: str | None,
        to_date: str | None,
        limit: int | None,
        **kwargs,
    ) -> dict[str, Any]:
        """
        Fetch the first page and return raw pagination metadata only.

        This method should only fetch the raw pagination info from the API.
        All analysis, calculation, and work distribution logic should be handled
        by the calling processor.
        """
        logger.info("Fetching raw pagination metadata from API")

        try:
            # Get first page to extract raw pagination info
            first_page = None
            async for page in self.fetch_phase_1_data(
                from_date=from_date,
                to_date=to_date,
                limit=limit or 1,  # Use 1 to get minimal data for metadata
                **kwargs,
            ):
                logger.error(f"Page: {page}")
                first_page = page
                break

            if not first_page:
                logger.warning("No data found for pagination analysis")
                return {
                    "total_count": 0,
                    "count_per_page": limit or 250,
                    "has_data": False,
                }

            # Extract and return only the raw pagination info
            raw_metadata = self._extract_pagination_info(first_page)
            raw_metadata["has_data"] = True

            logger.info(f"Raw pagination metadata: {raw_metadata}")
            return raw_metadata

        except Exception as e:
            logger.error(f"Pagination metadata fetch failed: {e}")
            return {
                "total_count": 0,
                "count_per_page": limit or 250,
                "has_data": False,
                "error": str(e),
            }
