"""
Streamlined Executor

This module provides direct execution without unnecessary indirection layers.
Replaces: Pipeline Runner + Generalized Assets
Benefits: Direct execution path, centralized error handling, simplified resource management
"""

import logging
from typing import Any

from .cleaner import StreamlinedCleaner
from .fetcher import StreamlinedFetcher
from .normalizer import StreamlinedNormalizer
from .plugins.registry import get_plugin_registry
from .resources.coordinator import ResourceCoordinator

logger = logging.getLogger(__name__)


class StreamlinedExecutor:
    """
    Direct execution coordinator that orchestrates the streamlined pipeline.

    This replaces the complex Pipeline Runner + Generalized Assets layers
    with a single, focused executor that provides:
    - Direct execution without indirection
    - Centralized error handling
    - Simplified resource management
    - Plugin-based custom logic preservation
    """

    def __init__(self, resource_coordinator: ResourceCoordinator):
        """
        Initialize the streamlined executor.

        Args:
            resource_coordinator: Manages all resources (DB, API keys, storage, etc.)
        """
        self.coordinator = resource_coordinator
        self.plugin_registry = get_plugin_registry()

        # Initialize streamlined components
        self.fetcher = StreamlinedFetcher(resource_coordinator)
        self.cleaner = StreamlinedCleaner(resource_coordinator)
        self.normalizer = StreamlinedNormalizer(resource_coordinator)

        logger.info("StreamlinedExecutor initialized with direct execution path")

    async def execute_data_type(
        self,
        data_type: str,
        phases: list[str],
        from_date: str | None = None,
        to_date: str | None = None,
        limit: int | None = None,
        **kwargs,
    ) -> dict[str, Any]:
        """
        Execute pipeline for a specific data type.

        Args:
            data_type: The data type to process (e.g., "bills", "nominations")
            phases: List of phases to execute (e.g., ["raw", "staging", "production"])
            from_date: Start date for data fetching
            to_date: End date for data fetching
            limit: Maximum number of items to process
            **kwargs: Additional parameters for specific data types

        Returns:
            Dictionary containing execution results and metrics
        """
        logger.info(
            f"Starting streamlined execution for {data_type} with phases: {phases}"
        )

        results = {
            "data_type": data_type,
            "phases": phases,
            "status": "started",
            "metrics": {},
            "errors": [],
        }

        try:
            # Initialize resources
            await self.coordinator.initialize()

            # Execute phases in sequence
            for phase in phases:
                logger.info(f"Executing phase: {phase}")

                try:
                    if phase == "raw":
                        # Raw data fetching
                        phase_results = await self._execute_raw_phase(
                            data_type, from_date, to_date, limit, **kwargs
                        )
                    elif phase == "staging":
                        # Data cleaning and staging
                        phase_results = await self._execute_staging_phase(
                            data_type, **kwargs
                        )
                    elif phase == "production":
                        # Data normalization to production
                        phase_results = await self._execute_production_phase(
                            data_type, **kwargs
                        )
                    else:
                        raise ValueError(f"Unknown phase: {phase}")

                    results["metrics"][phase] = phase_results
                    logger.info(f"Phase {phase} completed successfully")

                except Exception as e:
                    error_msg = f"Phase {phase} failed: {str(e)}"
                    logger.error(error_msg)
                    results["errors"].append(error_msg)
                    results["status"] = "failed"
                    break

            if not results["errors"]:
                results["status"] = "completed"
                logger.info(f"Streamlined execution completed for {data_type}")

        except Exception as e:
            error_msg = f"Streamlined execution failed: {str(e)}"
            logger.error(error_msg)
            results["errors"].append(error_msg)
            results["status"] = "failed"

        finally:
            # Cleanup resources
            await self.coordinator.cleanup()

        return results

    async def _execute_raw_phase(
        self,
        data_type: str,
        from_date: str | None = None,
        to_date: str | None = None,
        limit: int | None = None,
        **kwargs,
    ) -> dict[str, Any]:
        """Execute raw data fetching phase."""
        logger.info(f"Executing raw phase for {data_type}")

        # Use streamlined fetcher with plugin system
        results = await self.fetcher.fetch_data_type(
            data_type=data_type,
            from_date=from_date,
            to_date=to_date,
            limit=limit,
            **kwargs,
        )

        return {
            "phase": "raw",
            "items_fetched": results.get("items_fetched", 0),
            "items_stored": results.get("items_stored", 0),
            "duration": results.get("duration", 0),
            "status": "completed",
        }

    async def _execute_staging_phase(self, data_type: str, **kwargs) -> dict[str, Any]:
        """Execute data cleaning and staging phase."""
        logger.info(f"Executing staging phase for {data_type}")

        # Use streamlined cleaner with plugin system
        results = await self.cleaner.clean_data_type(data_type=data_type, **kwargs)

        return {
            "phase": "staging",
            "items_cleaned": results.get("items_cleaned", 0),
            "items_validated": results.get("items_validated", 0),
            "duration": results.get("duration", 0),
            "status": "completed",
        }

    async def _execute_production_phase(
        self, data_type: str, **kwargs
    ) -> dict[str, Any]:
        """Execute data normalization to production phase."""
        logger.info(f"Executing production phase for {data_type}")

        # Use streamlined normalizer with plugin system
        results = await self.normalizer.normalize_data_type(
            data_type=data_type, **kwargs
        )

        return {
            "phase": "production",
            "items_normalized": results.get("items_normalized", 0),
            "items_inserted": results.get("items_inserted", 0),
            "duration": results.get("duration", 0),
            "status": "completed",
        }

    async def get_execution_status(self, data_type: str) -> dict[str, Any]:
        """Get current execution status for a data type."""
        # This could be enhanced with persistent status tracking
        return {
            "data_type": data_type,
            "status": "not_running",
            "last_execution": None,
            "metrics": {},
        }

    def get_supported_data_types(self) -> list[str]:
        """Get list of supported data types from plugin registry."""
        return self.plugin_registry.get_supported_data_types()

    def get_plugin_info(self, data_type: str) -> dict[str, Any]:
        """Get information about plugins for a specific data type."""
        return {
            "data_type": data_type,
            "fetcher_plugin": self.plugin_registry.has_fetcher_plugin(data_type),
            "cleaner_plugin": self.plugin_registry.has_cleaner_plugin(data_type),
            "normalizer_plugin": self.plugin_registry.has_normalizer_plugin(data_type),
        }


# Convenience function for direct execution
async def execute_streamlined_pipeline(
    coordinator: ResourceCoordinator,
    data_type: str,
    phases: list[str],
    from_date: str | None = None,
    to_date: str | None = None,
    limit: int | None = None,
    **kwargs,
) -> dict[str, Any]:
    """
    Execute streamlined pipeline directly without creating executor instance.

    Args:
        coordinator: Resource coordinator instance
        data_type: The data type to process
        phases: List of phases to execute
        from_date: Start date for data fetching
        to_date: End date for data fetching
        limit: Maximum number of items to process
        **kwargs: Additional parameters

    Returns:
        Execution results dictionary
    """
    executor = StreamlinedExecutor(coordinator)
    return await executor.execute_data_type(
        data_type=data_type,
        phases=phases,
        from_date=from_date,
        to_date=to_date,
        limit=limit,
        **kwargs,
    )
