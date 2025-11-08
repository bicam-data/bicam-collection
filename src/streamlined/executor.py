"""
Streamlined Executor

This module provides direct execution without unnecessary indirection layers.
Replaces: Pipeline Runner + Generalized Assets
Benefits: Direct execution path, centralized error handling, simplified resource management
"""

import logging
from typing import Any

from .cleaner import Cleaner
from .fetcher import Fetcher
from .libs.hierarchical_checkpoint_system import ProcessingStage
from .normalizer import Normalizer
from .plugins.registry import get_registry
from .processing.processor import ParallelProcessor
from .resources.coordinator import ResourceCoordinator

logger = logging.getLogger(__name__)

# Phase name mapping: user-facing strings -> ProcessingStage enum values
# This provides a consistent mapping between executor phase names and checkpoint stages
PHASE_TO_STAGE = {
    "raw": ProcessingStage.FETCHING,
    "staging": ProcessingStage.STAGING,
    "production": ProcessingStage.CLEANING,
}

STAGE_TO_PHASE = {v: k for k, v in PHASE_TO_STAGE.items()}


class Executor:
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
        self.plugin_registry = get_registry()
        _ensure_parallelization(self.coordinator)
        logger.info("Executor initialized with direct execution path")

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
                    # Map phase string to ProcessingStage for consistency
                    stage = PHASE_TO_STAGE.get(phase)
                    if stage is None:
                        raise ValueError(
                            f"Unknown phase: {phase}. Valid phases: {list(PHASE_TO_STAGE.keys())}"
                        )

                    if stage == ProcessingStage.FETCHING:
                        # Raw data fetching
                        phase_results = await self._execute_raw_phase(
                            data_type, data_source, from_date, to_date, limit, **kwargs
                        )
                    elif stage == ProcessingStage.STAGING:
                        # Data normalization to staging
                        phase_results = await self._execute_staging_phase(
                            data_type, **kwargs
                        )
                    elif stage == ProcessingStage.CLEANING:
                        # Data cleaning to production
                        phase_results = await self._execute_production_phase(
                            data_type, **kwargs
                        )
                    else:
                        raise ValueError(f"Unhandled stage: {stage}")

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
        return await execute_raw_phase(
            self.coordinator,
            data_type,
            data_source,
            from_date,
            to_date,
            limit,
            **kwargs,
        )

    async def _execute_staging_phase(self, data_type: str, **kwargs) -> dict[str, Any]:
        """Execute data normalization to staging phase."""
        return await execute_staging_phase(self.coordinator, data_type, **kwargs)

    async def _execute_production_phase(
        self, data_type: str, **kwargs
    ) -> dict[str, Any]:
        """Execute data cleaning to production phase."""
        return await execute_production_phase(self.coordinator, data_type, **kwargs)

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
        2. Uses the ParallelProcessor to fetch related tables with full storage infrastructure
        3. Provides full resource coordination and error handling

        Args:
            data_type: The data type to process (e.g., "bills", "amendments")
            related_tables: List of related table names to fetch (e.g., ['texts', 'actions'])
            batch_size: Number of items to process in each batch
            limit: Maximum number of items to process (optional)
            from_date: Start date filter (optional)
            to_date: End date filter (optional)

        Returns:
            Dictionary containing execution results and metrics
        """
        logger.info(
            f"Starting PARALLEL related tables fetch for {data_type} with tables: {related_tables}"
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
            # Initialize fetcher
            fetcher = await Fetcher.from_coordinator(
                self.coordinator, data_type_name=data_type, data_source=data_source
            )

            # Create ParallelProcessor for parallel execution
            logger.info(
                f"Creating ParallelProcessor with {len(fetcher.api_keys)} API keys"
            )
            processor = ParallelProcessor(
                api_keys=fetcher.api_keys,
                client_class=fetcher.client.__class__ if fetcher.client else None,
                db_pool=fetcher.db_pool,
            )

            # Use the processor's method to fetch related tables with optimized storage
            results = await processor.fetch_related_tables_from_existing_data(
                fetcher=fetcher,
                data_type=data_type,
                related_tables=related_tables,
                batch_size=batch_size,
                limit=limit,
                from_date=from_date,
                to_date=to_date,
            )

            logger.info(f"Related tables fetch completed for {data_type}")
            return results

        except Exception as e:
            logger.error(f"Fetcher error: {e}")
            results["status"] = "failed"
            results["errors"].append(f"Fetcher error: {e}")
            return results
        finally:
            # Cleanup
            if "fetcher" in locals():
                await fetcher.cleanup()


# =============================================================================
# Functional phase execution functions
# =============================================================================


def _ensure_parallelization(coordinator: ResourceCoordinator):
    """Ensure parallelization is enabled if we have multiple API keys."""
    if (
        len(coordinator.config.api.keys) > 2
        and not coordinator.config.parallelization.enabled
    ):
        logger.info(
            f"Auto-enabling parallelization with {len(coordinator.config.api.keys)} API keys"
        )
        coordinator.config.parallelization.enabled = True
        coordinator.config.parallelization.fetcher = {
            "congressional": {
                "num_sessions": 1,
                "keys_per_session": len(coordinator.config.api.keys),
            },
            "govinfo": {
                "num_sessions": 1,
                "keys_per_session": len(coordinator.config.api.keys),
            },
        }


async def execute_raw_phase(
    coordinator: ResourceCoordinator,
    data_type: str,
    data_source: str,
    from_date: str | None = None,
    to_date: str | None = None,
    limit: int | None = None,
    **kwargs,
) -> dict[str, Any]:
    """Execute raw data fetching phase."""
    logger.info(f"Executing raw phase for {data_type} (source: {data_source})")

    parallel_related_data_kwargs = {
        k: v
        for k, v in kwargs.items()
        if k in ("enable_parallel_related_data", "parallel_related_data_threshold")
    }

    fetcher = await Fetcher.from_coordinator(
        coordinator,
        data_type_name=data_type,
        data_source=data_source,
        **parallel_related_data_kwargs,
    )

    try:
        results = await fetcher.process_items(
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
    finally:
        await fetcher.cleanup()


async def execute_staging_phase(
    coordinator: ResourceCoordinator, data_type: str, **kwargs
) -> dict[str, Any]:
    """Execute data normalization to staging phase."""
    logger.info(f"Executing staging phase for {data_type}")

    normalizer = Normalizer(coordinator)
    try:
        results = await normalizer.normalize_data_type(data_type=data_type, **kwargs)

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
    finally:
        await normalizer.cleanup()


async def execute_production_phase(
    coordinator: ResourceCoordinator, data_type: str, **kwargs
) -> dict[str, Any]:
    """Execute data cleaning to production phase."""
    logger.info(f"Executing production phase for {data_type}")

    cleaner = Cleaner(coordinator)
    results = await cleaner.clean_data_type(data_type=data_type, **kwargs)

    return {
        "phase": "production",
        "items_cleaned": results.get("total_records_processed", 0),
        "items_validated": results.get("total_records_processed", 0),
        "duration": results.get("duration", 0),
        "status": results.get("status", "completed"),
        "errors": results.get("total_errors", 0),
    }


# =============================================================================
# Convenience function for direct execution
# =============================================================================


async def execute_pipeline(
    coordinator: ResourceCoordinator,
    data_type: str,
    phases: list[str],
    from_date: str | None = None,
    to_date: str | None = None,
    limit: int | None = None,
    **kwargs,
) -> dict[str, Any]:
    """
    Execute pipeline directly without creating executor instance.

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
    executor = Executor(coordinator)
    return await executor.execute_data_type(
        data_type=data_type,
        phases=phases,
        from_date=from_date,
        to_date=to_date,
        limit=limit,
        **kwargs,
    )
