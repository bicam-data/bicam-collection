"""
Streamlined Normalizer

This module provides focused database normalization with plugin support.
Replaces: Complex normalizer hierarchy
Benefits: Direct database operations, batch processing, clear separation of concerns
"""

import logging
import time
from typing import Any

from .plugins.registry import get_plugin_registry
from .resources.coordinator import ResourceCoordinator

logger = logging.getLogger(__name__)


class StreamlinedNormalizer:
    """
    Focused database normalizer with plugin support.

    This replaces the complex normalizer hierarchy with a single, focused component:
    - Direct database operations
    - Batch processing for efficiency
    - Clear separation of concerns
    - Plugin-based custom logic preservation
    """

    def __init__(self, resource_coordinator: ResourceCoordinator):
        """
        Initialize the streamlined normalizer.

        Args:
            resource_coordinator: Manages all resources (DB, API keys, storage, etc.)
        """
        self.coordinator = resource_coordinator
        self.plugin_registry = get_plugin_registry()

        logger.info(
            "StreamlinedNormalizer initialized with focused database operations"
        )

    async def normalize_data_type(
        self, data_type: str, batch_size: int = 100, **kwargs
    ) -> dict[str, Any]:
        """
        Normalize data for a specific data type using plugin system.

        Args:
            data_type: The data type to normalize (e.g., "bills", "nominations")
            batch_size: Number of items to process in each batch
            **kwargs: Additional parameters for specific data types

        Returns:
            Dictionary containing normalization results and metrics
        """
        logger.info(f"Starting streamlined normalization for {data_type}")
        start_time = time.time()

        # Get plugin for this data type
        normalizer_plugin = self.plugin_registry.get_normalizer_plugin(data_type)
        if not normalizer_plugin:
            raise ValueError(f"No normalizer plugin found for data type: {data_type}")

        # Initialize resources
        db_manager = self.coordinator.database_manager
        storage_manager = self.coordinator.storage_manager

        results = {
            "data_type": data_type,
            "items_normalized": 0,
            "items_inserted": 0,
            "batches_processed": 0,
            "duration": 0,
            "status": "started",
        }

        try:
            # Phase 1: Get staging data to normalize
            logger.info(f"Retrieving staging data for {data_type}")
            staging_data = await self._get_staging_data(
                storage_manager, data_type, **kwargs
            )

            if not staging_data:
                logger.warning(f"No staging data found for {data_type}")
                results["status"] = "completed"
                results["duration"] = time.time() - start_time
                return results

            # Phase 2: Normalize data in batches
            logger.info(
                f"Normalizing {len(staging_data)} items in batches of {batch_size}"
            )
            normalized_data = await self._normalize_data_batches(
                normalizer_plugin, staging_data, batch_size, **kwargs
            )

            # Phase 3: Insert normalized data into production database
            logger.info(f"Inserting {len(normalized_data)} normalized items")
            inserted_count = await self._insert_normalized_data(
                db_manager, normalizer_plugin, data_type, normalized_data, **kwargs
            )

            results.update(
                {
                    "items_normalized": len(normalized_data),
                    "items_inserted": inserted_count,
                    "batches_processed": (len(staging_data) + batch_size - 1)
                    // batch_size,
                    "duration": time.time() - start_time,
                    "status": "completed",
                }
            )

            logger.info(
                f"Streamlined normalization completed for {data_type}: {results}"
            )

        except Exception as e:
            logger.error(f"Streamlined normalization failed for {data_type}: {str(e)}")
            results.update(
                {
                    "status": "failed",
                    "error": str(e),
                    "duration": time.time() - start_time,
                }
            )

        return results

    async def _get_staging_data(
        self, storage_manager: Any, data_type: str, **kwargs
    ) -> list[dict[str, Any]]:
        """Get staging data that needs to be normalized."""
        try:
            # Use storage manager to get staging data
            staging_data = await storage_manager.get_staging_data(
                data_type=data_type, **kwargs
            )

            logger.info(f"Retrieved {len(staging_data)} staging items for {data_type}")
            return staging_data

        except Exception as e:
            logger.error(f"Failed to retrieve staging data: {str(e)}")
            raise

    async def _normalize_data_batches(
        self,
        normalizer_plugin: Any,
        staging_data: list[dict[str, Any]],
        batch_size: int,
        **kwargs,
    ) -> list[dict[str, Any]]:
        """Normalize data in batches using the plugin."""
        normalized_data = []

        # Process data in batches
        for i in range(0, len(staging_data), batch_size):
            batch = staging_data[i : i + batch_size]

            try:
                # Use plugin to normalize batch (preserves custom logic)
                normalized_batch = await normalizer_plugin.normalize_batch(
                    batch_data=batch, **kwargs
                )

                normalized_data.extend(normalized_batch)

                logger.debug(
                    f"Normalized batch {i // batch_size + 1}: {len(normalized_batch)} items"
                )

            except Exception as e:
                logger.error(
                    f"Failed to normalize batch {i // batch_size + 1}: {str(e)}"
                )
                # Continue with other batches
                continue

        return normalized_data

    async def _insert_normalized_data(
        self,
        db_manager: Any,
        normalizer_plugin: Any,
        data_type: str,
        normalized_data: list[dict[str, Any]],
        **kwargs,
    ) -> int:
        """Insert normalized data into production database."""
        try:
            # Get database connection
            db_connection = await db_manager.get_connection()

            # Use plugin to insert data (preserves custom logic)
            inserted_count = await normalizer_plugin.insert_normalized_data(
                db_connection=db_connection,
                data_type=data_type,
                normalized_data=normalized_data,
                **kwargs,
            )

            logger.info(f"Inserted {inserted_count} normalized items for {data_type}")
            return inserted_count

        except Exception as e:
            logger.error(f"Failed to insert normalized data: {str(e)}")
            raise

    async def validate_normalized_data(
        self, data_type: str, sample_size: int = 100, **kwargs
    ) -> dict[str, Any]:
        """Validate normalized data for a specific data type."""
        logger.info(f"Validating normalized data for {data_type}")

        # Get plugin for this data type
        normalizer_plugin = self.plugin_registry.get_normalizer_plugin(data_type)
        if not normalizer_plugin:
            raise ValueError(f"No normalizer plugin found for data type: {data_type}")

        try:
            # Get sample of normalized data
            db_manager = self.coordinator.database_manager
            db_connection = await db_manager.get_connection()

            # Use plugin to validate data (preserves custom logic)
            validation_results = await normalizer_plugin.validate_normalized_data(
                db_connection=db_connection,
                data_type=data_type,
                sample_size=sample_size,
                **kwargs,
            )

            logger.info(f"Validation completed for {data_type}: {validation_results}")
            return validation_results

        except Exception as e:
            logger.error(f"Validation failed for {data_type}: {str(e)}")
            return {
                "data_type": data_type,
                "validation_status": "failed",
                "error": str(e),
            }

    async def get_normalization_status(self, data_type: str) -> dict[str, Any]:
        """Get current normalization status for a data type."""
        try:
            # Get database connection
            db_manager = self.coordinator.database_manager
            db_connection = await db_manager.get_connection()

            # Query normalization status
            # This is a simplified example - real implementation would query actual tables
            status = {
                "data_type": data_type,
                "staging_count": 0,
                "production_count": 0,
                "last_normalized": None,
                "status": "idle",
            }

            return status

        except Exception as e:
            logger.error(f"Failed to get normalization status: {str(e)}")
            return {"data_type": data_type, "status": "error", "error": str(e)}

    def get_supported_data_types(self) -> list[str]:
        """Get list of supported data types from plugin registry."""
        return self.plugin_registry.get_supported_data_types()

    async def test_plugin_integration(self, data_type: str) -> dict[str, Any]:
        """Test plugin integration for a specific data type."""
        normalizer_plugin = self.plugin_registry.get_normalizer_plugin(data_type)

        if not normalizer_plugin:
            return {
                "data_type": data_type,
                "plugin_available": False,
                "error": f"No normalizer plugin found for {data_type}",
            }

        try:
            # Test basic plugin functionality
            test_data = [{"id": "test", "data": "test_value"}]

            # Check if plugin has required methods
            required_methods = [
                "normalize_batch",
                "insert_normalized_data",
                "validate_normalized_data",
            ]
            available_methods = [
                method
                for method in dir(normalizer_plugin)
                if not method.startswith("_")
            ]

            return {
                "data_type": data_type,
                "plugin_available": True,
                "plugin_type": type(normalizer_plugin).__name__,
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
                "plugin_type": type(normalizer_plugin).__name__,
                "error": str(e),
            }

    async def get_normalization_metrics(self, data_type: str) -> dict[str, Any]:
        """Get normalization metrics for a specific data type."""
        try:
            # Get database connection
            db_manager = self.coordinator.database_manager
            db_connection = await db_manager.get_connection()

            # This is a simplified example - real implementation would query actual metrics
            metrics = {
                "data_type": data_type,
                "total_normalized": 0,
                "normalization_rate": 0.0,
                "error_rate": 0.0,
                "avg_batch_size": 0,
                "last_24h_count": 0,
            }

            return metrics

        except Exception as e:
            logger.error(f"Failed to get normalization metrics: {str(e)}")
            return {"data_type": data_type, "error": str(e)}
