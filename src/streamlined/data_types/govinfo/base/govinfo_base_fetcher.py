"""
GovInfo-specific base fetcher implementing the 4-phase pattern.

This module provides GovInfo-specific implementations for:
- 4-phase processing pattern (collection → package → granules → granule data)
- GovInfo API patterns and response structures
- GovInfo schema defaults
- Source-specific configuration
"""

import asyncio
import logging
from collections.abc import AsyncIterator
from typing import Any

from ....libs.run_tracking import RunType
from ...abstract.base_fetcher import AbstractFetcher

logger = logging.getLogger(__name__)


class GovInfoBaseFetcher(AbstractFetcher):
    """
    GovInfo-specific base fetcher implementing the sophisticated 4-phase pattern.

    This class inherits all sophisticated processing logic from AbstractFetcher
    and implements GovInfo-specific:
    - API response parsing
    - Schema configuration
    - Data extraction patterns
    - 4-phase processing workflow
    """

    # =============================================================================
    # REQUIRED ABSTRACT METHOD IMPLEMENTATIONS
    # =============================================================================

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

        # Load configuration to get proper id_field
        self.config = None
        if data_type_name:
            try:
                from ....libs.data_type_registry import get_global_registry

                registry = get_global_registry()
                self.config = registry.get_data_type_config(data_type_name)
                # Use the id_field from config, fallback to default pattern
                self.id_field = (
                    self.config.id_field if self.config else f"{data_type_name}_id"
                )
                logger.debug(
                    f"Loaded config for {data_type_name}, id_field: {self.id_field}"
                )
            except Exception as e:
                logger.warning(f"Could not load config for {data_type_name}: {e}")
                self.id_field = f"{data_type_name}_id" if data_type_name else "id"
        else:
            self.id_field = "id"

    def get_source_system_name(self) -> str:
        """Get the source system name for progress tracking."""
        return "govinfo"

    def get_default_schema(self) -> str:
        """Get the default schema for raw data storage."""
        return "raw_govinfo"

    def get_run_type(self) -> RunType:
        """Get the run type for tracking."""
        return RunType.GOVINFO_FETCH

    async def fetch_phase_1_data(
        self,
        from_date: str | None = None,
        to_date: str | None = None,
        limit: int | None = None,
        **kwargs,
    ) -> AsyncIterator[list[dict[str, Any]]]:
        """
        Phase 1: Fetch GovInfo collection data from API.

        Yields batches of collection items containing package URLs for Phase 2.
        """
        logger.info("Starting GovInfo Phase 1 collection data fetch")

        try:
            # Get the proper API configuration from the registry
            if not self.config:
                logger.error("No configuration loaded for data type")
                return

            # Use the API configuration to get the correct collection and parameters
            api_config = self.config.api
            collection = api_config.endpoint or self.data_type_name

            # Format dates for API
            from_date_fmt = (
                self.client._format_date_for_api(from_date) if from_date else None
            )
            to_date_fmt = self.client._format_date_for_api(to_date) if to_date else None

            logger.debug(
                f"Using collection: {collection} for data type: {self.data_type_name}"
            )

            async for batch in self.client.fetch_collection_data(
                collection=collection,
                from_date=from_date_fmt,
                to_date=to_date_fmt,
                limit=limit,
                **kwargs,
            ):
                if batch:
                    yield batch

        except Exception as e:
            logger.error(f"Phase 1 fetch failed: {e}")
            raise

    async def fetch_phase_1_data_with_client(
        self,
        client,
        from_date: str | None = None,
        to_date: str | None = None,
        limit: int | None = None,
        **kwargs,
    ) -> AsyncIterator[list[dict[str, Any]]]:
        """
        Phase 1: Fetch GovInfo collection data using a specific client.
        """
        logger.debug(f"Phase 1 fetch with client: {client}")

        try:
            # Get the proper API configuration from the registry
            if not self.config:
                logger.error("No configuration loaded for data type")
                return

            # Use the API configuration to get the correct collection and parameters
            api_config = self.config.api
            collection = api_config.endpoint or self.data_type_name

            # Format dates for API
            from_date_fmt = (
                self.client._format_date_for_api(from_date) if from_date else None
            )
            to_date_fmt = self.client._format_date_for_api(to_date) if to_date else None

            logger.debug(
                f"Using collection: {collection} for data type: {self.data_type_name}"
            )

            async for batch in client.fetch_collection_data(
                collection=collection,
                from_date=from_date_fmt,
                to_date=to_date_fmt,
                limit=limit,
                **kwargs,
            ):
                if batch:
                    yield batch

        except Exception as e:
            logger.error(f"Phase 1 fetch with client failed: {e}")
            raise

    async def fetch_phase_2_data(self, item_url: str) -> dict[str, Any] | None:
        """
        Phase 2: Fetch complete GovInfo package data from individual URL.
        """
        try:
            return await self.client.retrieve_package_data_from_url(item_url)
        except Exception as e:
            logger.error(f"Phase 2 fetch failed for {item_url}: {e}")
            return None

    async def fetch_phase_2_data_with_client(
        self, item_url: str, client
    ) -> dict[str, Any] | None:
        """
        Phase 2: Fetch complete GovInfo package data using a specific client.
        """
        try:
            return await client.retrieve_package_data_from_url(item_url)
        except Exception as e:
            logger.error(f"Phase 2 fetch with client failed for {item_url}: {e}")
            return None

    async def fetch_phase_3_data(
        self, item_data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """
        Phase 3: Fetch GovInfo granules data from package data.
        """
        try:
            granule_urls = item_data.get("granules", [])
            if not granule_urls:
                return []

            return await self.client.fetch_granules_data(granule_urls)
        except Exception as e:
            logger.error(f"Phase 3 fetch failed: {e}")
            return []

    async def fetch_phase_3_data_with_client(
        self, url: str, expected_keys: list[str], client
    ) -> list[dict[str, Any]]:
        """
        Phase 3: Fetch GovInfo granules data using a specific client.
        """
        try:
            return await client.fetch_granules_data([url])
        except Exception as e:
            logger.error(f"Phase 3 fetch with client failed for {url}: {e}")
            return []

    async def fetch_phase_4_data(self, item_url: str) -> dict[str, Any] | None:
        """
        Phase 4: Fetch individual GovInfo granule data.
        """
        try:
            return await self.client.fetch_granule_data(item_url)
        except Exception as e:
            logger.error(f"Phase 4 fetch failed for {item_url}: {e}")
            return None

    async def fetch_phase_4_data_with_client(
        self, item_url: str, client
    ) -> dict[str, Any] | None:
        """
        Phase 4: Fetch individual GovInfo granule data using a specific client.
        """
        try:
            return await client.fetch_granule_data(item_url)
        except Exception as e:
            logger.error(f"Phase 4 fetch with client failed for {item_url}: {e}")
            return None

    def extract_item_id(self, item_data: dict[str, Any]) -> str:
        """Extract standardized ID from GovInfo item data."""
        return (
            item_data.get("packageId")
            or item_data.get("id")
            or item_data.get("title", "unknown")
        )

    def _extract_preliminary_item_id(self, list_item: dict[str, Any]) -> str:
        """Extract preliminary ID from GovInfo list item data."""
        return (
            list_item.get("packageId")
            or list_item.get("id")
            or f"item_{hash(str(list_item))}"
        )

    async def store_phase_1_data(
        self, data: dict[str, Any], batch_id: str | None = None
    ) -> None:
        """Store Phase 1 GovInfo collection data."""
        if not self.db_pool:
            return

        try:
            await self.store_raw_data(
                schema=self.get_default_schema(),
                table=f"{self.data_type_name}_list_raw",
                items=[data],
                batch_id=batch_id,
            )
        except Exception as e:
            logger.error(f"Failed to store Phase 1 data: {e}")

    async def store_phase_2_data(
        self, data: dict[str, Any], batch_id: str | None = None
    ) -> None:
        """Store Phase 2 GovInfo package data."""
        if not self.db_pool:
            return

        try:
            await self.store_raw_data(
                schema=self.get_default_schema(),
                table=f"{self.data_type_name}_raw",
                items=[data],
                batch_id=batch_id,
            )
        except Exception as e:
            logger.error(f"Failed to store Phase 2 data: {e}")

    async def store_phase_3_data(
        self, data: list[dict[str, Any]], parent_id: str, batch_id: str | None = None
    ) -> None:
        """Store Phase 3 GovInfo granules data in separate tables by data type."""
        if not self.db_pool or not data:
            return

        try:
            # Create table name: {main_data_type}_granules_raw
            table_name = f"{self.data_type_name}_granules_list_raw"

            # Add parent reference to each granule
            for granule in data:
                granule["parent_package_id"] = parent_id

            await self.store_raw_data(
                schema=self.get_default_schema(),
                table=table_name,
                items=data,
                batch_id=batch_id,
            )

            logger.debug(f"Stored {len(data)} granules in {table_name}")

        except Exception as e:
            logger.error(f"Failed to store Phase 3 data: {e}")

    async def store_phase_4_data(
        self, data: list[dict[str, Any]], parent_id: str, batch_id: str | None = None
    ) -> None:
        """Store Phase 4 GovInfo granule data in separate tables by data type."""
        if not self.db_pool or not data:
            return

        try:
            # Create table name: {main_data_type}_granule_data_raw
            table_name = f"{self.data_type_name}_granules_raw"

            # Add parent reference to each granule data
            for granule_data in data:
                granule_data["parent_package_id"] = parent_id

            await self.store_raw_data(
                schema=self.get_default_schema(),
                table=table_name,
                items=data,
                batch_id=batch_id,
            )

            logger.debug(f"Stored {len(data)} granule data items in {table_name}")

        except Exception as e:
            logger.error(f"Failed to store Phase 4 data: {e}")

    # =============================================================================
    # GOVINFO-SPECIFIC OVERRIDES
    # =============================================================================

    def _extract_pagination_info(
        self, page_data: list[dict[str, Any]]
    ) -> dict[str, Any]:
        """
        Extract pagination info from GovInfo API response.

        GovInfo API typically includes "count" and "nextPage" fields.
        """
        if not page_data or not isinstance(page_data, list):
            return {"total_count": 0, "count_per_page": 0}

        # Check if first item contains pagination metadata
        first_item = page_data[0] if page_data else {}

        # GovInfo API pattern: check for count and nextPage
        if "count" in first_item:
            return {
                "total_count": first_item.get("count", len(page_data)),
                "count_per_page": len(page_data),
            }

        # Fallback to batch size
        return {
            "total_count": len(page_data),
            "count_per_page": len(page_data),
        }

    def _extract_latest_date_from_batch(
        self, batch: list[dict[str, Any]]
    ) -> str | None:
        """
        Extract the latest date from a batch of GovInfo items.

        GovInfo data typically uses 'lastModified' or 'dateIssued' fields.
        """
        dates = []
        for item in batch:
            date_str = (
                item.get("lastModified") or item.get("dateIssued") or item.get("date")
            )
            if date_str:
                dates.append(date_str)

        if dates:
            return max(dates)
        return None

    # =============================================================================
    # GOVINFO-SPECIFIC PARALLEL PROCESSING
    # =============================================================================

    async def _execute_redistribution_strategy(
        self,
        api_clients: list,
        distribution_strategy: dict,
        from_date: str | None,
        to_date: str | None,
        limit: int | None,
        batch_size: int,
        **kwargs,
    ) -> dict[str, Any]:
        """
        Execute redistribution strategy for GovInfo 4-phase processing.

        GovInfo redistribution focuses on:
        - Phase 1: Collect collection data with minimal clients
        - Phase 2: Use some clients for package data
        - Phase 3-4: Use idle clients for granule processing
        """
        logger.info("Executing GovInfo redistribution strategy")

        stats = {
            "total_processed": 0,
            "successful": 0,
            "errors": 0,
            "redistribution_used": True,
            "phase1_clients": distribution_strategy["clients_for_phase1"],
            "total_clients": len(api_clients),
            "packages_processed": 0,
            "granules_processed": 0,
        }

        try:
            # Phase 1: Collect collection data with minimal clients
            phase1_clients = api_clients[: distribution_strategy["clients_for_phase1"]]
            all_collection_items = []

            # Collect all Phase 1 data
            for client in phase1_clients:
                async for batch in self.fetch_phase_1_data_with_client(
                    client, from_date, to_date, limit, **kwargs
                ):
                    for item in batch:
                        await self.store_phase_1_data(item)
                        all_collection_items.append(item)

            # Phase 2: Use available clients for package data
            semaphore = asyncio.Semaphore(len(api_clients))
            all_packages = []

            async def process_package(item, client):
                async with semaphore:
                    package_url = item.get("packageURL") or item.get("url")
                    if package_url:
                        package_data = await self.fetch_phase_2_data_with_client(
                            package_url, client
                        )
                        if package_data:
                            await self.store_phase_2_data(package_data)
                            all_packages.append(package_data)
                            return 1
                    return 0

            # Process all packages
            package_tasks = []
            for i, item in enumerate(all_collection_items):
                client = api_clients[i % len(api_clients)]
                package_tasks.append(process_package(item, client))

            package_results = await asyncio.gather(
                *package_tasks, return_exceptions=True
            )

            # Phase 3-4: Process granules with all available clients
            async def process_granules(package_data, client):
                async with semaphore:
                    try:
                        # Phase 3: Get granules list
                        granules = await self.fetch_phase_3_data(package_data)
                        if granules:
                            package_id = self.extract_item_id(package_data)
                            await self.store_phase_3_data(granules, package_id)

                            # Phase 4: Get granule data
                            granule_data_list = []
                            for granule in granules:
                                granule_url = granule.get("url")
                                if granule_url:
                                    granule_data = (
                                        await self.fetch_phase_4_data_with_client(
                                            granule_url, client
                                        )
                                    )
                                    if granule_data:
                                        granule_data_list.append(granule_data)

                            if granule_data_list:
                                await self.store_phase_4_data(
                                    granule_data_list, package_id
                                )

                            return len(granule_data_list)
                    except Exception as e:
                        logger.error(f"Granule processing failed: {e}")
                        return 0
                    return 0

            # Process granules for all packages
            granule_tasks = []
            for i, package_data in enumerate(all_packages):
                client = api_clients[i % len(api_clients)]
                granule_tasks.append(process_granules(package_data, client))

            granule_results = await asyncio.gather(
                *granule_tasks, return_exceptions=True
            )

            # Count results
            for result in package_results:
                if isinstance(result, int):
                    stats["packages_processed"] += result
                else:
                    stats["errors"] += 1

            for result in granule_results:
                if isinstance(result, int):
                    stats["granules_processed"] += result
                else:
                    stats["errors"] += 1

            stats["total_processed"] = len(all_collection_items)
            stats["successful"] = (
                stats["packages_processed"] + stats["granules_processed"]
            )

            logger.info(f"GovInfo redistribution completed: {stats}")
            return stats

        except Exception as e:
            logger.error(f"GovInfo redistribution failed: {e}")
            stats["error"] = str(e)
            return stats

    async def _execute_standard_parallel_processing(
        self,
        api_clients: list,
        from_date: str | None,
        to_date: str | None,
        limit: int | None,
        batch_size: int,
        max_concurrent: int,
        **kwargs,
    ) -> dict[str, Any]:
        """
        Execute standard parallel processing for GovInfo 4-phase data.

        Distributes collection processing across all clients, then processes
        packages, granules, and granule data with controlled concurrency.
        """
        logger.info("Executing GovInfo standard parallel processing")

        stats = {
            "total_processed": 0,
            "successful": 0,
            "errors": 0,
            "clients_used": len(api_clients),
            "packages_processed": 0,
            "granules_processed": 0,
        }

        try:
            semaphore = asyncio.Semaphore(max_concurrent)
            all_collection_items = []

            # Phase 1: Collect from all clients concurrently
            async def collect_from_client(client):
                items = []
                async for batch in self.fetch_phase_1_data_with_client(
                    client, from_date, to_date, limit, **kwargs
                ):
                    for item in batch:
                        await self.store_phase_1_data(item)
                        items.append(item)
                return items

            # Collect from all clients
            client_tasks = [collect_from_client(client) for client in api_clients]
            client_results = await asyncio.gather(*client_tasks, return_exceptions=True)

            # Aggregate all items
            for result in client_results:
                if isinstance(result, list):
                    all_collection_items.extend(result)
                else:
                    logger.error(f"Client collection failed: {result}")

            # Phase 2-4: Process all phases with distributed clients
            async def process_full_item(item, client):
                async with semaphore:
                    try:
                        # Phase 2: Get package data
                        package_url = item.get("packageURL") or item.get("url")
                        if not package_url:
                            return 0

                        package_data = await self.fetch_phase_2_data_with_client(
                            package_url, client
                        )
                        if not package_data:
                            return 0

                        await self.store_phase_2_data(package_data)
                        package_id = self.extract_item_id(package_data)

                        # Phase 3: Get granules
                        granules = await self.fetch_phase_3_data(package_data)
                        if granules:
                            await self.store_phase_3_data(granules, package_id)

                            # Phase 4: Get granule data
                            granule_data_list = []
                            for granule in granules:
                                granule_url = granule.get("url")
                                if granule_url:
                                    granule_data = (
                                        await self.fetch_phase_4_data_with_client(
                                            granule_url, client
                                        )
                                    )
                                    if granule_data:
                                        granule_data_list.append(granule_data)

                            if granule_data_list:
                                await self.store_phase_4_data(
                                    granule_data_list, package_id
                                )

                            return 1 + len(granule_data_list)  # package + granules

                        return 1  # just package

                    except Exception as e:
                        logger.error(f"Full item processing failed: {e}")
                        return 0

            # Process all items with distributed clients
            full_tasks = []
            for i, item in enumerate(all_collection_items):
                client = api_clients[i % len(api_clients)]
                full_tasks.append(process_full_item(item, client))

            full_results = await asyncio.gather(*full_tasks, return_exceptions=True)

            # Count results
            for result in full_results:
                if isinstance(result, int):
                    stats["successful"] += result
                else:
                    stats["errors"] += 1

            stats["total_processed"] = len(all_collection_items)
            logger.info(f"GovInfo standard parallel processing completed: {stats}")
            return stats

        except Exception as e:
            logger.error(f"GovInfo standard parallel processing failed: {e}")
            stats["error"] = str(e)
            return stats
