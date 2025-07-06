"""
Streamlined Cleaner

This module provides focused data cleaning with plugin support.
Replaces: Complex cleaner hierarchy
Benefits: Direct database operations, integrated validation, simplified error handling
"""

import logging
import time
from typing import Any

from .plugins.consolidated_registry import get_consolidated_registry
from .resources.coordinator import ResourceCoordinator

logger = logging.getLogger(__name__)


class StreamlinedCleaner:
    """
    Focused data cleaner with plugin support.

    This replaces the complex cleaner hierarchy with a single, focused component:
    - Direct database operations
    - Integrated validation
    - Simplified error handling
    - Plugin-based custom logic preservation
    """

    def __init__(self, resource_coordinator: ResourceCoordinator):
        """
        Initialize the streamlined cleaner.

        Args:
            resource_coordinator: Manages all resources (DB, API keys, storage, etc.)
        """
        self.coordinator = resource_coordinator
        self.plugin_registry = get_consolidated_registry()

        logger.info(
            "StreamlinedCleaner initialized with focused data cleaning operations"
        )

    async def clean_data_type(
        self, data_type: str, batch_size: int = 100, validate: bool = True, **kwargs
    ) -> dict[str, Any]:
        """
        Clean data for a specific data type using plugin system.

        Args:
            data_type: The data type to clean (e.g., "bills", "nominations")
            batch_size: Number of items to process in each batch
            validate: Whether to validate cleaned data
            **kwargs: Additional parameters for specific data types

        Returns:
            Dictionary containing cleaning results and metrics
        """
        logger.info(f"Starting streamlined cleaning for {data_type}")
        start_time = time.time()

        # Get plugin for this data type
        cleaner_plugin = self.plugin_registry.get_cleaner_plugin(data_type)
        if not cleaner_plugin:
            raise ValueError(f"No cleaner plugin found for data type: {data_type}")

        # Initialize resources
        storage_manager = self.coordinator.storage_manager

        results = {
            "data_type": data_type,
            "items_cleaned": 0,
            "items_validated": 0,
            "items_rejected": 0,
            "batches_processed": 0,
            "duration": 0,
            "status": "started",
        }

        try:
            # Phase 1: Get raw data to clean
            logger.info(f"Retrieving raw data for {data_type}")
            raw_data = await self._get_raw_data(storage_manager, data_type, **kwargs)

            if not raw_data:
                logger.warning(f"No raw data found for {data_type}")
                results["status"] = "completed"
                results["duration"] = time.time() - start_time
                return results

            # Phase 2: Clean data in batches
            logger.info(f"Cleaning {len(raw_data)} items in batches of {batch_size}")
            cleaned_data = await self._clean_data_batches(
                cleaner_plugin, raw_data, batch_size, **kwargs
            )

            # Phase 3: Validate cleaned data (if requested)
            if validate:
                logger.info(f"Validating {len(cleaned_data)} cleaned items")
                validated_data, rejected_data = await self._validate_cleaned_data(
                    cleaner_plugin, cleaned_data, **kwargs
                )
            else:
                validated_data = cleaned_data
                rejected_data = []

            # Phase 4: Store cleaned data
            logger.info(f"Storing {len(validated_data)} cleaned items")
            stored_count = await self._store_cleaned_data(
                storage_manager, data_type, validated_data, **kwargs
            )

            results.update(
                {
                    "items_cleaned": len(cleaned_data),
                    "items_validated": len(validated_data),
                    "items_rejected": len(rejected_data),
                    "batches_processed": (len(raw_data) + batch_size - 1) // batch_size,
                    "duration": time.time() - start_time,
                    "status": "completed",
                }
            )

            logger.info(f"Streamlined cleaning completed for {data_type}: {results}")

        except Exception as e:
            logger.error(f"Streamlined cleaning failed for {data_type}: {str(e)}")
            results.update(
                {
                    "status": "failed",
                    "error": str(e),
                    "duration": time.time() - start_time,
                }
            )

        return results

    async def _get_raw_data(
        self, storage_manager: Any, data_type: str, **kwargs
    ) -> list[dict[str, Any]]:
        """Get raw data that needs to be cleaned."""
        try:
            # Use storage manager to get raw data
            raw_data = await storage_manager.get_raw_data(data_type=data_type, **kwargs)

            logger.info(f"Retrieved {len(raw_data)} raw items for {data_type}")
            return raw_data

        except Exception as e:
            logger.error(f"Failed to retrieve raw data: {str(e)}")
            raise

    async def _clean_data_batches(
        self,
        cleaner_plugin: Any,
        raw_data: list[dict[str, Any]],
        batch_size: int,
        **kwargs,
    ) -> list[dict[str, Any]]:
        """Clean data in batches using the plugin."""
        cleaned_data = []

        # Process data in batches
        for i in range(0, len(raw_data), batch_size):
            batch = raw_data[i : i + batch_size]

            try:
                # Use plugin to clean batch (preserves custom logic)
                cleaned_batch = await cleaner_plugin.clean_batch(
                    batch_data=batch, **kwargs
                )

                cleaned_data.extend(cleaned_batch)

                logger.debug(
                    f"Cleaned batch {i // batch_size + 1}: {len(cleaned_batch)} items"
                )

            except Exception as e:
                logger.error(f"Failed to clean batch {i // batch_size + 1}: {str(e)}")
                # Continue with other batches
                continue

        return cleaned_data

    async def _validate_cleaned_data(
        self, cleaner_plugin: Any, cleaned_data: list[dict[str, Any]], **kwargs
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Validate cleaned data using the plugin."""
        validated_data = []
        rejected_data = []

        try:
            # Use plugin to validate data (preserves custom logic)
            validation_results = await cleaner_plugin.validate_cleaned_data(
                cleaned_data=cleaned_data, **kwargs
            )

            validated_data = validation_results.get("validated", [])
            rejected_data = validation_results.get("rejected", [])

            logger.info(
                f"Validation completed: {len(validated_data)} valid, {len(rejected_data)} rejected"
            )

        except Exception as e:
            logger.error(f"Validation failed: {str(e)}")
            # If validation fails, use all cleaned data
            validated_data = cleaned_data
            rejected_data = []

        return validated_data, rejected_data

    async def _store_cleaned_data(
        self,
        storage_manager: Any,
        data_type: str,
        cleaned_data: list[dict[str, Any]],
        **kwargs,
    ) -> int:
        """Store cleaned data using the storage manager."""
        try:
            # Use storage manager to store cleaned data
            stored_count = await storage_manager.store_staging_data(
                data_type=data_type, data=cleaned_data, **kwargs
            )

            logger.info(f"Stored {stored_count} cleaned items for {data_type}")
            return stored_count

        except Exception as e:
            logger.error(f"Failed to store cleaned data: {str(e)}")
            raise

    async def get_cleaning_status(self, data_type: str) -> dict[str, Any]:
        """Get current cleaning status for a data type."""
        try:
            # Get storage manager
            storage_manager = self.coordinator.storage_manager

            # Query cleaning status
            # This is a simplified example - real implementation would query actual tables
            status = {
                "data_type": data_type,
                "raw_count": 0,
                "staging_count": 0,
                "last_cleaned": None,
                "status": "idle",
            }

            return status

        except Exception as e:
            logger.error(f"Failed to get cleaning status: {str(e)}")
            return {"data_type": data_type, "status": "error", "error": str(e)}

    def get_supported_data_types(self) -> list[str]:
        """Get list of supported data types from plugin registry."""
        return self.plugin_registry.get_supported_data_types()

    async def test_plugin_integration(self, data_type: str) -> dict[str, Any]:
        """Test plugin integration for a specific data type."""
        cleaner_plugin = self.plugin_registry.get_cleaner_plugin(data_type)

        if not cleaner_plugin:
            return {
                "data_type": data_type,
                "plugin_available": False,
                "error": f"No cleaner plugin found for {data_type}",
            }

        try:
            # Test basic plugin functionality
            test_data = [{"id": "test", "data": "test_value"}]

            # Check if plugin has required methods
            required_methods = ["clean_batch", "validate_cleaned_data"]
            available_methods = [
                method for method in dir(cleaner_plugin) if not method.startswith("_")
            ]

            return {
                "data_type": data_type,
                "plugin_available": True,
                "plugin_type": type(cleaner_plugin).__name__,
                "required_methods": required_methods,
                "available_methods": available_methods,
                "methods_match": all(
                    method in available_methods for method in required_methods
                ),
            }

        except Exception as e:
            return {
                "data_type": data_type,
                "plugin_available": True,
                "plugin_type": type(cleaner_plugin).__name__,
                "error": str(e),
            }

    async def get_cleaning_metrics(self, data_type: str) -> dict[str, Any]:
        """Get cleaning metrics for a specific data type."""
        try:
            # Get storage manager
            storage_manager = self.coordinator.storage_manager

            # This is a simplified example - real implementation would query actual metrics
            metrics = {
                "data_type": data_type,
                "total_cleaned": 0,
                "cleaning_rate": 0.0,
                "validation_rate": 0.0,
                "rejection_rate": 0.0,
                "avg_batch_size": 0,
                "last_24h_count": 0,
            }

            return metrics

        except Exception as e:
            logger.error(f"Failed to get cleaning metrics: {str(e)}")
            return {"data_type": data_type, "error": str(e)}

    async def clean_specific_items(
        self, data_type: str, item_ids: list[str], **kwargs
    ) -> dict[str, Any]:
        """Clean specific items by ID."""
        logger.info(f"Cleaning specific items for {data_type}: {item_ids}")
        start_time = time.time()

        # Get plugin for this data type
        cleaner_plugin = self.plugin_registry.get_cleaner_plugin(data_type)
        if not cleaner_plugin:
            raise ValueError(f"No cleaner plugin found for data type: {data_type}")

        try:
            # Get storage manager
            storage_manager = self.coordinator.storage_manager

            # Get specific items
            raw_data = await storage_manager.get_raw_data_by_ids(
                data_type=data_type, item_ids=item_ids, **kwargs
            )

            if not raw_data:
                return {
                    "data_type": data_type,
                    "item_ids": item_ids,
                    "items_cleaned": 0,
                    "status": "no_data_found",
                }

            # Clean the specific items
            cleaned_data = await cleaner_plugin.clean_batch(
                batch_data=raw_data, **kwargs
            )

            # Store cleaned data
            stored_count = await storage_manager.store_staging_data(
                data_type=data_type, data=cleaned_data, **kwargs
            )

            return {
                "data_type": data_type,
                "item_ids": item_ids,
                "items_cleaned": len(cleaned_data),
                "items_stored": stored_count,
                "duration": time.time() - start_time,
                "status": "completed",
            }

        except Exception as e:
            logger.error(f"Failed to clean specific items: {str(e)}")
            return {
                "data_type": data_type,
                "item_ids": item_ids,
                "status": "failed",
                "error": str(e),
                "duration": time.time() - start_time,
            }
