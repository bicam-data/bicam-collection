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
from .plugins.consolidated_registry import get_consolidated_registry
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
        self.plugin_registry = get_consolidated_registry()

        # Enable parallelization if we have multiple API keys
        # OptimizedParallelProcessor works best with ALL keys in a dynamic pool
        if (
            len(self.coordinator.config.api.keys) > 2
            and not self.coordinator.config.parallelization.enabled
        ):
            logger.info(
                f"Auto-enabling parallelization with {len(self.coordinator.config.api.keys)} API keys for OptimizedParallelProcessor"
            )
            # Enable parallelization with minimal config (OptimizedParallelProcessor ignores sessions)
            self.coordinator.config.parallelization.enabled = True
            self.coordinator.config.parallelization.fetcher = {
                "congressional": {
                    "num_sessions": 1,  # Ignored by OptimizedParallelProcessor
                    "keys_per_session": len(
                        self.coordinator.config.api.keys
                    ),  # All keys
                },
                "govinfo": {
                    "num_sessions": 1,  # Ignored by OptimizedParallelProcessor
                    "keys_per_session": len(
                        self.coordinator.config.api.keys
                    ),  # All keys
                },
            }

        # Initialize streamlined components using proper constructors
        # Note: fetcher will be initialized lazily since from_coordinator is async
        self.fetcher = None
        self.cleaner = StreamlinedCleaner(resource_coordinator)
        self.normalizer = StreamlinedNormalizer(resource_coordinator)

        logger.info("StreamlinedExecutor initialized with direct execution path")

    def _get_data_source(self, data_type: str) -> str:
        """Get the data source for a given data type."""
        try:
            return self.plugin_registry.get_data_source(data_type)
        except Exception as e:
            logger.warning(f"Could not determine data source for {data_type}: {e}")
            return "unknown"

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

        # Get the data source for this data type
        data_source = self._get_data_source(data_type)
        logger.info(f"Data source for {data_type}: {data_source}")

        results = {
            "data_type": data_type,
            "data_source": data_source,
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
                            data_type, data_source, from_date, to_date, limit, **kwargs
                        )
                    elif phase == "staging":
                        # Data normalization to staging
                        phase_results = await self._execute_staging_phase(
                            data_type, **kwargs
                        )
                    elif phase == "production":
                        # Data cleaning to production
                        phase_results = await self._execute_production_phase(
                            data_type, **kwargs
                        )
                    else:
                        raise ValueError(f"Unknown phase: {phase}")

                    results["metrics"][phase] = phase_results
                    logger.info(f"Phase {phase} completed successfully")

                except Exception as e:
                    error_msg = f"Phase {phase} failed: {str(e)}"
                    logger.error(error_msg, exc_info=True)
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
            # Cleanup components
            try:
                # Cleanup normalizer (will flush any buffered data)
                await self.normalizer.cleanup()

                # Cleanup fetcher if initialized
                if self.fetcher:
                    await self.fetcher.cleanup()

                # Cleanup cleaner
                # await self.cleaner.cleanup()

            except Exception as e:
                logger.warning(f"Error during component cleanup: {e}")

            # Cleanup resources
            await self.coordinator.cleanup()

        return results

    async def _execute_raw_phase(
        self,
        data_type: str,
        data_source: str,
        from_date: str | None = None,
        to_date: str | None = None,
        limit: int | None = None,
        **kwargs,
    ) -> dict[str, Any]:
        """Execute raw data fetching phase."""
        logger.info(f"Executing raw phase for {data_type} (source: {data_source})")

        # Initialize fetcher lazily if not already done
        if self.fetcher is None:
            self.fetcher = await StreamlinedFetcher.from_coordinator(
                self.coordinator, data_type_name=data_type, data_source=data_source
            )

        # Use streamlined fetcher with plugin system
        results = await self.fetcher.process_items(
            from_date=from_date,
            to_date=to_date,
            limit=limit,
            **kwargs,
        )

        return {
            "phase": "raw",
            "items_fetched": results.get("total_processed", 0),
            "items_stored": results.get("total_processed", 0),
            "duration": results.get("duration", 0),
            "status": "completed",
        }

    async def _execute_staging_phase(self, data_type: str, **kwargs) -> dict[str, Any]:
        """Execute data normalization to staging phase."""
        logger.info(f"Executing staging phase for {data_type}")

        # Use streamlined normalizer with plugin system
        results = await self.normalizer.normalize_data_type(
            data_type=data_type, **kwargs
        )

        # Map the actual returned metrics to expected format
        return {
            "phase": "staging",
            "items_normalized": (
                results.get("main_records_processed", 0)
                + results.get("related_records_processed", 0)
            ),
            "items_inserted": (
                results.get("main_records_processed", 0)
                + results.get("related_records_processed", 0)
                + results.get("lists_extracted", 0)
            ),
            "duration": results.get("duration", 0),
            "status": results.get("status", "completed"),
            "errors": results.get("errors", 0),
        }

    async def _execute_production_phase(
        self, data_type: str, **kwargs
    ) -> dict[str, Any]:
        """Execute data cleaning to production phase."""
        logger.info(f"Executing production phase for {data_type}")

        # Use streamlined cleaner with plugin system
        results = await self.cleaner.clean_data_type(data_type=data_type, **kwargs)

        return {
            "phase": "production",
            "items_cleaned": results.get("total_records_processed", 0),
            "items_validated": results.get(
                "total_records_processed", 0
            ),  # Same as cleaned for now
            "duration": results.get("duration", 0),
            "status": results.get("status", "completed"),
            "errors": results.get("total_errors", 0),
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
        return self.plugin_registry.list_data_types()

    def get_plugin_info(self, data_type: str) -> dict[str, Any]:
        """Get information about plugins for a specific data type."""
        return {
            "data_type": data_type,
            "fetcher_plugin": self.plugin_registry.get_fetcher_plugin(data_type)
            is not None,
            "cleaner_plugin": self.plugin_registry.get_cleaner_plugin(data_type)
            is not None,
            "normalizer_plugin": self.plugin_registry.get_normalizer_plugin(data_type)
            is not None,
        }

    async def fetch_related_tables_from_existing_data(
        self,
        data_type: str,
        related_tables: list[str],
        batch_size: int = 100,
        limit: int = None,
        from_date: str = None,
        to_date: str = None,
    ) -> dict[str, Any]:
        """
        Fetch specific related tables from existing Phase 2 data.

        This method:
        1. Initializes a fetcher for the specified data type
        2. Uses the fetcher's fetch_specific_related_tables_from_existing_data method
        3. Provides full resource coordination and error handling

        Args:
            data_type: The data type to process (e.g., "bills", "amendments")
            related_tables: List of related table names to fetch (e.g., ['texts', 'actions'])
            batch_size: Number of items to process in each batch
            limit: Maximum number of items to process (optional)
            from_date: Start date filter (optional)
            to_date: End date filter (optional)

        Returns:
            Dictionary with processing results and metrics
        """
        logger.info(
            f"Fetching related tables {related_tables} for {data_type} from existing Phase 2 data"
        )

        # Get the data source for this data type
        data_source = self._get_data_source(data_type)
        logger.info(f"Data source for {data_type}: {data_source}")

        results = {
            "data_type": data_type,
            "data_source": data_source,
            "related_tables": related_tables,
            "status": "started",
            "metrics": {},
            "errors": [],
        }

        try:
            # Initialize fetcher for this data type
            if self.fetcher is None or self.fetcher.data_type_name != data_type:
                self.fetcher = await StreamlinedFetcher.from_coordinator(
                    self.coordinator, data_type_name=data_type, data_source=data_source
                )

            # Use the fetcher's method to fetch related tables
            fetcher_results = (
                await self.fetcher.fetch_specific_related_tables_from_existing_data(
                    related_tables=related_tables,
                    batch_size=batch_size,
                    limit=limit,
                    from_date=from_date,
                    to_date=to_date,
                )
            )

            # Check for errors in fetcher results
            if "error" in fetcher_results:
                error_msg = f"Fetcher error: {fetcher_results['error']}"
                logger.error(error_msg)
                results["errors"].append(error_msg)
                results["status"] = "failed"
                return results

            # Update results with fetcher metrics
            results["metrics"] = fetcher_results
            results["status"] = fetcher_results.get("status", "completed")

            logger.info(f"Successfully fetched related tables for {data_type}")
            logger.info(f"Processed {fetcher_results.get('items_processed', 0)} items")
            logger.info(
                f"Fetched {fetcher_results.get('related_items_fetched', 0)} related items"
            )

        except Exception as e:
            error_msg = f"Failed to fetch related tables: {str(e)}"
            logger.error(error_msg, exc_info=True)
            results["errors"].append(error_msg)
            results["status"] = "failed"

        finally:
            # Cleanup fetcher if it was created for this operation
            if self.fetcher and self.fetcher.data_type_name == data_type:
                try:
                    await self.fetcher.cleanup()
                except Exception as e:
                    logger.warning(f"Error during fetcher cleanup: {e}")

        return results


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
