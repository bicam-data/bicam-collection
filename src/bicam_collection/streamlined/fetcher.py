"""
Streamlined Fetcher

This module provides integrated fetching with built-in parallel processing.
Replaces: BaseFetcher + CongressionalBaseFetcher + SpecificFetcher + OptimizedParallelProcessor
Benefits: Single responsibility, built-in parallelism, integrated resource management
"""

import asyncio
import logging
import time
from typing import Any

from .plugins.registry import get_plugin_registry
from .processing.key_pool import DynamicKeyPool
from .processing.work_queue import AdaptiveWorkQueue, WorkChunk
from .resources.coordinator import ResourceCoordinator

logger = logging.getLogger(__name__)


class StreamlinedFetcher:
    """
    Integrated fetcher with built-in parallel processing and plugin support.

    This replaces the complex fetcher hierarchy with a single, focused component:
    - Single responsibility (data fetching)
    - Built-in parallel processing
    - Integrated resource management
    - Plugin-based custom logic preservation
    """

    def __init__(self, resource_coordinator: ResourceCoordinator):
        """
        Initialize the streamlined fetcher.

        Args:
            resource_coordinator: Manages all resources (DB, API keys, storage, etc.)
        """
        self.coordinator = resource_coordinator
        self.plugin_registry = get_plugin_registry()

        # Initialize integrated processing components
        self.work_queue = AdaptiveWorkQueue(
            max_queue_size=1000, chunk_size=50, max_workers=10
        )

        logger.info(
            "StreamlinedFetcher initialized with integrated parallel processing"
        )

    async def fetch_data_type(
        self,
        data_type: str,
        from_date: str | None = None,
        to_date: str | None = None,
        limit: int | None = None,
        **kwargs,
    ) -> dict[str, Any]:
        """
        Fetch data for a specific data type using plugin system.

        Args:
            data_type: The data type to fetch (e.g., "bills", "nominations")
            from_date: Start date for data fetching
            to_date: End date for data fetching
            limit: Maximum number of items to fetch
            **kwargs: Additional parameters for specific data types

        Returns:
            Dictionary containing fetch results and metrics
        """
        logger.info(f"Starting streamlined fetch for {data_type}")
        start_time = time.time()

        # Get plugin for this data type
        fetcher_plugin = self.plugin_registry.get_fetcher_plugin(data_type)
        if not fetcher_plugin:
            raise ValueError(f"No fetcher plugin found for data type: {data_type}")

        # Initialize resources
        api_client = await self.coordinator.client_manager.get_api_client()
        storage_manager = self.coordinator.storage_manager

        # Create key pool for parallel processing
        api_keys = await self.coordinator.api_key_manager.get_available_keys()
        key_pool = DynamicKeyPool(api_keys, rate_limit_threshold=100)

        results = {
            "data_type": data_type,
            "items_fetched": 0,
            "items_stored": 0,
            "duration": 0,
            "status": "started",
        }

        try:
            # Phase 1: Fetch list data
            logger.info(f"Fetching list data for {data_type}")
            list_data = await self._fetch_list_data(
                fetcher_plugin, api_client, from_date, to_date, limit, **kwargs
            )

            if not list_data:
                logger.warning(f"No list data found for {data_type}")
                results["status"] = "completed"
                results["duration"] = time.time() - start_time
                return results

            # Phase 2: Fetch detailed data in parallel
            logger.info(f"Fetching detailed data for {len(list_data)} items")
            detailed_data = await self._fetch_detailed_data_parallel(
                fetcher_plugin, api_client, list_data, key_pool, **kwargs
            )

            # Phase 3: Store data
            logger.info(f"Storing {len(detailed_data)} items")
            stored_count = await self._store_data(
                storage_manager, data_type, detailed_data, **kwargs
            )

            results.update(
                {
                    "items_fetched": len(detailed_data),
                    "items_stored": stored_count,
                    "duration": time.time() - start_time,
                    "status": "completed",
                }
            )

            logger.info(f"Streamlined fetch completed for {data_type}: {results}")

        except Exception as e:
            logger.error(f"Streamlined fetch failed for {data_type}: {str(e)}")
            results.update(
                {
                    "status": "failed",
                    "error": str(e),
                    "duration": time.time() - start_time,
                }
            )

        return results

    async def _fetch_list_data(
        self,
        fetcher_plugin: Any,
        api_client: Any,
        from_date: str | None = None,
        to_date: str | None = None,
        limit: int | None = None,
        **kwargs,
    ) -> list[dict[str, Any]]:
        """Fetch list data using the plugin."""
        try:
            # Use plugin to fetch list data (preserves custom logic)
            list_data = await fetcher_plugin.fetch_list_data(
                api_client=api_client,
                from_date=from_date,
                to_date=to_date,
                limit=limit,
                **kwargs,
            )

            logger.info(f"Fetched {len(list_data)} items from list endpoint")
            return list_data

        except Exception as e:
            logger.error(f"Failed to fetch list data: {str(e)}")
            raise

    async def _fetch_detailed_data_parallel(
        self,
        fetcher_plugin: Any,
        api_client: Any,
        list_data: list[dict[str, Any]],
        key_pool: DynamicKeyPool,
        **kwargs,
    ) -> list[dict[str, Any]]:
        """Fetch detailed data in parallel using work queue and key pool."""
        detailed_data = []

        # Create work chunks
        for item in list_data:
            item_id = fetcher_plugin.extract_item_id(item)
            work_chunk = WorkChunk(
                chunk_id=item_id, data=item, chunk_type="detail_fetch", priority=1
            )
            await self.work_queue.add_work(work_chunk)

        # Process work chunks in parallel
        tasks = []
        for _ in range(min(len(list_data), self.work_queue.max_workers)):
            task = asyncio.create_task(
                self._process_work_chunk(fetcher_plugin, api_client, key_pool, **kwargs)
            )
            tasks.append(task)

        # Collect results
        chunk_results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in chunk_results:
            if isinstance(result, Exception):
                logger.error(f"Worker task failed: {result}")
            elif isinstance(result, list):
                detailed_data.extend(result)

        return detailed_data

    async def _process_work_chunk(
        self, fetcher_plugin: Any, api_client: Any, key_pool: DynamicKeyPool, **kwargs
    ) -> list[dict[str, Any]]:
        """Process work chunks from the queue."""
        processed_items = []

        while True:
            # Get work chunk
            work_chunk = await self.work_queue.get_work()
            if not work_chunk:
                break

            try:
                # Get API key
                api_key = await key_pool.get_key()
                if not api_key:
                    logger.warning("No API key available, skipping item")
                    continue

                # Fetch detailed data using plugin (preserves custom logic)
                detailed_item = await fetcher_plugin.fetch_detailed_data(
                    api_client=api_client,
                    item_data=work_chunk.data,
                    api_key=api_key.key,
                    **kwargs,
                )

                processed_items.append(detailed_item)

                # Return key to pool
                await key_pool.return_key(api_key)

            except Exception as e:
                logger.error(
                    f"Failed to process work chunk {work_chunk.chunk_id}: {str(e)}"
                )
                # Return key to pool even on failure
                if "api_key" in locals():
                    await key_pool.return_key(api_key)

        return processed_items

    async def _store_data(
        self, storage_manager: Any, data_type: str, data: list[dict[str, Any]], **kwargs
    ) -> int:
        """Store fetched data using the storage manager."""
        try:
            # Use storage manager to store data
            stored_count = await storage_manager.store_raw_data(
                data_type=data_type, data=data, **kwargs
            )

            logger.info(f"Stored {stored_count} items for {data_type}")
            return stored_count

        except Exception as e:
            logger.error(f"Failed to store data: {str(e)}")
            raise

    async def get_fetch_status(self, data_type: str) -> dict[str, Any]:
        """Get current fetch status for a data type."""
        return {
            "data_type": data_type,
            "work_queue_size": self.work_queue.get_queue_size(),
            "workers_active": self.work_queue.get_active_workers(),
            "status": "idle",
        }

    def get_supported_data_types(self) -> list[str]:
        """Get list of supported data types from plugin registry."""
        return self.plugin_registry.get_supported_data_types()

    async def test_plugin_integration(self, data_type: str) -> dict[str, Any]:
        """Test plugin integration for a specific data type."""
        fetcher_plugin = self.plugin_registry.get_fetcher_plugin(data_type)

        if not fetcher_plugin:
            return {
                "data_type": data_type,
                "plugin_available": False,
                "error": f"No fetcher plugin found for {data_type}",
            }

        try:
            # Test basic plugin functionality
            test_item = {"id": "test", "url": "https://example.com/test"}
            item_id = fetcher_plugin.extract_item_id(test_item)

            return {
                "data_type": data_type,
                "plugin_available": True,
                "plugin_type": type(fetcher_plugin).__name__,
                "test_extraction": item_id,
                "methods_available": [
                    method
                    for method in dir(fetcher_plugin)
                    if not method.startswith("_")
                ],
            }

        except Exception as e:
            return {
                "data_type": data_type,
                "plugin_available": True,
                "plugin_type": type(fetcher_plugin).__name__,
                "error": str(e),
            }
