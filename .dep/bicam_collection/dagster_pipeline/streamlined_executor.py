"""
Streamlined Processing Executor

This module replaces the complex pipeline with a direct, efficient executor
that combines multiple abstraction layers into focused functionality.

Key improvements:
- Direct execution path (no unnecessary indirection)
- Centralized error handling
- Simplified resource management
- Clear separation of concerns
"""

import asyncio
import logging
from collections.abc import Sequence
from typing import Any

from dagster import AssetExecutionContext, asset, materialize

from ..libs.data_type_router import get_global_registry
from .resource_managers import ResourceCoordinator

logger = logging.getLogger(__name__)


class StreamlinedExecutor:
    """
    Direct executor that replaces the complex pipeline layers.

    Combines Pipeline Runner + Generalized Assets functionality
    with simplified, direct execution.
    """

    def __init__(self, coordinator: ResourceCoordinator):
        self.coordinator = coordinator

    async def execute_data_type(
        self, data_type: str, phases: Sequence[str] | None = None, **processing_params
    ) -> dict[str, Any]:
        """
        Execute processing for a data type with direct, efficient flow.

        Args:
            data_type: Data type to process
            phases: Phases to execute (raw, staging, production)
            **processing_params: Additional processing parameters

        Returns:
            Execution results
        """
        if phases is None:
            phases = ["raw", "staging", "production"]

        logger.info(
            f"Executing {len(phases)} phases for {data_type}: {', '.join(phases)}"
        )

        results = {}

        try:
            # Execute each phase directly
            for phase in phases:
                logger.info(f"Starting {phase} phase for {data_type}")

                if phase == "raw":
                    results[phase] = await self._execute_raw_phase(
                        data_type, **processing_params
                    )
                elif phase == "staging":
                    results[phase] = await self._execute_staging_phase(
                        data_type, **processing_params
                    )
                elif phase == "production":
                    results[phase] = await self._execute_production_phase(
                        data_type, **processing_params
                    )
                else:
                    raise ValueError(f"Unknown phase: {phase}")

                logger.info(f"Completed {phase} phase for {data_type}")

            return {
                "status": "success",
                "data_type": data_type,
                "phases_executed": phases,
                "results": results,
            }

        except Exception as e:
            logger.error(f"Execution failed for {data_type}: {e}")
            return {
                "status": "error",
                "data_type": data_type,
                "phases_executed": list(results.keys()),
                "error": str(e),
                "results": results,
            }

    async def _execute_raw_phase(self, data_type: str, **params) -> dict[str, Any]:
        """Execute raw data fetching phase."""
        # Get data type configuration
        registry = get_global_registry()
        if not registry.is_registered(data_type):
            raise ValueError(f"Data type '{data_type}' not registered")

        # Create streamlined fetcher
        fetcher = await self._create_streamlined_fetcher(data_type)

        # Execute fetching with integrated processing
        return await fetcher.execute_integrated_processing(**params)

    async def _execute_staging_phase(self, data_type: str, **params) -> dict[str, Any]:
        """Execute staging data normalization phase."""
        # Get data type configuration
        registry = get_global_registry()
        config = registry.get_data_type_config(data_type)

        # Create streamlined normalizer
        normalizer = await self._create_streamlined_normalizer(data_type, config)

        # Execute normalization
        return await normalizer.execute_normalization(**params)

    async def _execute_production_phase(
        self, data_type: str, **params
    ) -> dict[str, Any]:
        """Execute production data cleaning phase."""
        # Get data type configuration
        registry = get_global_registry()
        config = registry.get_data_type_config(data_type)

        # Create streamlined cleaner
        cleaner = await self._create_streamlined_cleaner(data_type, config)

        # Execute cleaning
        return await cleaner.execute_cleaning(**params)

    async def _create_streamlined_fetcher(self, data_type: str):
        """Create streamlined fetcher with integrated processing."""
        from .streamlined_fetcher import StreamlinedFetcher

        return StreamlinedFetcher(data_type=data_type, coordinator=self.coordinator)

    async def _create_streamlined_normalizer(self, data_type: str, config):
        """Create streamlined normalizer."""
        from .streamlined_normalizer import StreamlinedNormalizer

        return StreamlinedNormalizer(
            data_type=data_type, config=config, coordinator=self.coordinator
        )

    async def _create_streamlined_cleaner(self, data_type: str, config):
        """Create streamlined cleaner."""
        from .streamlined_cleaner import StreamlinedCleaner

        return StreamlinedCleaner(
            data_type=data_type, config=config, coordinator=self.coordinator
        )


def create_streamlined_asset(data_type: str, phase: str):
    """
    Create a streamlined Dagster asset for a specific data type and phase.

    This replaces the complex generalized assets with simple, direct assets.
    """

    @asset(
        name=f"{data_type}_{phase}_data",
        group_name=f"{data_type}_pipeline",
        compute_kind=phase,
        description=f"Streamlined {phase} processing for {data_type}",
    )
    async def streamlined_asset(
        context: AssetExecutionContext,
        coordinator: ResourceCoordinator,
    ) -> dict[str, Any]:
        """Execute streamlined processing for this phase."""

        # Create executor
        executor = StreamlinedExecutor(coordinator)

        # Execute this specific phase
        result = await executor.execute_data_type(data_type=data_type, phases=[phase])

        # Log results
        context.log.info(f"Streamlined {phase} execution completed for {data_type}")

        return result

    return streamlined_asset


def execute_streamlined_pipeline(
    coordinator: ResourceCoordinator,
    data_type: str,
    phases: Sequence[str] | None = None,
    **params,
) -> dict[str, Any]:
    """
    Execute streamlined pipeline without Dagster for simple use cases.

    This provides a direct execution path that bypasses Dagster entirely
    for maximum performance and simplicity.
    """

    async def _async_execute():
        executor = StreamlinedExecutor(coordinator)
        return await executor.execute_data_type(data_type, phases, **params)

    return asyncio.run(_async_execute())


def execute_streamlined_dagster_pipeline(
    coordinator: ResourceCoordinator,
    data_type: str,
    phases: Sequence[str] | None = None,
) -> dict[str, Any]:
    """
    Execute streamlined pipeline through Dagster assets.

    This provides Dagster integration for users who want the Dagster
    features like lineage tracking and monitoring.
    """
    if phases is None:
        phases = ["raw", "staging", "production"]

    # Create assets for each phase
    assets = []
    for phase in phases:
        assets.append(create_streamlined_asset(data_type, phase))

    # Execute through Dagster
    result = materialize(assets, resources={"coordinator": coordinator})

    return {
        "status": "success" if result.success else "failed",
        "data_type": data_type,
        "phases_executed": phases,
        "dagster_result": result,
    }
