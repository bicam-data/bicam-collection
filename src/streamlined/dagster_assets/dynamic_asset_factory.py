"""
Dynamic Asset Factory for Streamlined Architecture

This module creates Dagster assets dynamically for any data type based on:
- Plugin system registration (custom_plugins.py)
- Data type configuration (config.yaml)
- Streamlined architecture components

Benefits:
- Single source of truth for asset creation
- Leverages Dagster's lineage and orchestration
- No duplicate boilerplate across data types
- Dynamic asset generation based on registered data types
"""

import logging
from typing import Any

from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetOut,
    asset,
    multi_asset,
)

from ..executor import StreamlinedExecutor
from ..plugins.consolidated_registry import get_consolidated_registry
from ..resources.dagster_integration import (
    StreamlinedProcessingResource,
)

logger = logging.getLogger(__name__)


def create_data_type_assets(data_type: str):
    """
    Create a complete set of Dagster assets for a specific data type.

    This factory creates:
    1. Raw data fetching asset
    2. Staging normalization asset
    3. Production cleaning asset
    4. Summary/validation asset

    All with proper Dagster lineage and dependencies.
    """

    # Raw Data Asset
    @asset(
        name=f"{data_type}_raw",
        group_name=data_type,
        compute_kind="data_fetching",
        description=f"Fetch raw {data_type} data from Congressional API",
    )
    async def raw_data_asset(
        context: AssetExecutionContext,
        processing_resource: StreamlinedProcessingResource,
    ) -> dict[str, Any]:
        """Fetch raw data for this data type."""
        coordinator = await processing_resource.get_coordinator()
        executor = StreamlinedExecutor(coordinator)

        # Execute only the raw phase
        results = await executor.execute_data_type(
            data_type=data_type,
            phases=["raw"],
            **context.run_config.get("ops", {}).get(f"{data_type}_raw", {}),
        )

        context.add_output_metadata(
            {
                "records_fetched": results.get("raw_records", 0),
                "data_type": data_type,
                "phase": "raw",
                "run_id": results.get("run_id"),
            }
        )

        return results

    # Staging Data Asset (depends on raw)
    @asset(
        name=f"{data_type}_staging",
        group_name=data_type,
        compute_kind="data_normalization",
        description=f"Normalize {data_type} data from raw to staging schema",
        ins={f"{data_type}_raw": AssetIn()},
    )
    async def staging_data_asset(
        context: AssetExecutionContext,
        processing_resource: StreamlinedProcessingResource,
        **kwargs,  # This captures the dependency on raw_data_asset
    ) -> dict[str, Any]:
        """Normalize raw data to staging schema."""
        coordinator = await processing_resource.get_coordinator()
        executor = StreamlinedExecutor(coordinator)

        # Execute only the staging phase
        results = await executor.execute_data_type(
            data_type=data_type,
            phases=["staging"],
            **context.run_config.get("ops", {}).get(f"{data_type}_staging", {}),
        )

        context.add_output_metadata(
            {
                "records_normalized": results.get("staging_records", 0),
                "data_type": data_type,
                "phase": "staging",
                "run_id": results.get("run_id"),
            }
        )

        return results

    # Production Data Asset (depends on staging)
    @asset(
        name=f"{data_type}_production",
        group_name=data_type,
        compute_kind="data_cleaning",
        description=f"Clean and finalize {data_type} data for production use",
        ins={f"{data_type}_staging": AssetIn()},
    )
    async def production_data_asset(
        context: AssetExecutionContext,
        processing_resource: StreamlinedProcessingResource,
        **kwargs,  # This captures the dependency on staging_data_asset
    ) -> dict[str, Any]:
        """Clean staging data for production use."""
        coordinator = await processing_resource.get_coordinator()
        executor = StreamlinedExecutor(coordinator)

        # Execute only the production phase
        results = await executor.execute_data_type(
            data_type=data_type,
            phases=["production"],
            **context.run_config.get("ops", {}).get(f"{data_type}_production", {}),
        )

        context.add_output_metadata(
            {
                "records_cleaned": results.get("production_records", 0),
                "data_type": data_type,
                "phase": "production",
                "run_id": results.get("run_id"),
            }
        )

        return results

    # Complete Pipeline Summary Asset
    @asset(
        name=f"{data_type}_complete",
        group_name=data_type,
        compute_kind="summary",
        description=f"Complete pipeline summary for {data_type}",
        ins={f"{data_type}_production": AssetIn()},
    )
    async def complete_pipeline_asset(
        context: AssetExecutionContext,
        processing_resource: StreamlinedProcessingResource,
        **kwargs,  # This captures the dependency on production_data_asset
    ) -> dict[str, Any]:
        """Generate complete pipeline summary."""
        coordinator = await processing_resource.get_coordinator()

        # Get counts from all phases
        db_pool = await coordinator.db_manager.get_pool()

        async with db_pool.acquire() as conn:
            # Count records in each phase
            raw_count = (
                await conn.fetchval(
                    f"SELECT COUNT(*) FROM bicam_raw_congressional.{data_type}"
                )
                or 0
            )

            staging_count = (
                await conn.fetchval(
                    f"SELECT COUNT(*) FROM bicam_staging_congressional.{data_type}"
                )
                or 0
            )

            production_count = (
                await conn.fetchval(
                    f"SELECT COUNT(*) FROM bicam_congressional.{data_type}"
                )
                or 0
            )

        summary = {
            "data_type": data_type,
            "pipeline_status": "completed",
            "raw_records": raw_count,
            "staging_records": staging_count,
            "production_records": production_count,
            "architecture": "streamlined",
            "framework_integration": "dagster_dynamic_factory",
        }

        context.add_output_metadata(summary)

        logger.info(
            f"{data_type} pipeline complete: Raw={raw_count}, "
            f"Staging={staging_count}, Production={production_count}"
        )

        return summary

    return [
        raw_data_asset,
        staging_data_asset,
        production_data_asset,
        complete_pipeline_asset,
    ]


def create_all_registered_assets():
    """
    Create Dagster assets for all registered data types.

    This dynamically discovers all registered data types and creates
    a complete set of assets for each one.
    """
    registry = get_consolidated_registry()

    all_assets = []

    # Get all registered data types
    registered_data_types = registry.list_data_types()

    logger.info(f"Creating assets for {len(registered_data_types)} data types")

    for data_type in registered_data_types:
        try:
            # Verify the data type has plugins
            fetcher_plugin = registry.get_fetcher_plugin(data_type)
            cleaner_plugin = registry.get_cleaner_plugin(data_type)
            normalizer_plugin = registry.get_normalizer_plugin(data_type)

            if not any([fetcher_plugin, cleaner_plugin, normalizer_plugin]):
                logger.warning(
                    f"No plugins found for {data_type}, skipping asset creation"
                )
                continue

            # Create assets for this data type
            data_type_assets = create_data_type_assets(data_type)
            all_assets.extend(data_type_assets)

            logger.info(f"Created {len(data_type_assets)} assets for {data_type}")

        except Exception as e:
            logger.error(f"Error creating assets for {data_type}: {e}")
            continue

    logger.info(f"Total assets created: {len(all_assets)}")
    return all_assets


# Multi-asset for cross-data-type operations
@multi_asset(
    outs={
        "data_lineage_report": AssetOut(
            description="Complete data lineage report across all data types"
        ),
        "quality_metrics": AssetOut(
            description="Data quality metrics across all pipelines"
        ),
    },
    group_name="system_monitoring",
    compute_kind="analytics",
)
async def system_monitoring_assets(
    context: AssetExecutionContext,
    processing_resource: StreamlinedProcessingResource,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Generate system-wide monitoring and quality metrics."""
    coordinator = await processing_resource.get_coordinator()
    db_pool = await coordinator.db_manager.get_pool()

    # Get all registered data types
    registry = get_consolidated_registry()
    data_types = registry.list_data_types()

    lineage_report = {
        "generated_at": context.run_config.get("run_timestamp", "unknown"),
        "data_types": {},
        "total_data_types": len(data_types),
    }

    quality_metrics = {
        "generated_at": context.run_config.get("run_timestamp", "unknown"),
        "data_types": {},
        "overall_health": "unknown",
    }

    async with db_pool.acquire() as conn:
        for data_type in data_types:
            try:
                # Get record counts for lineage
                raw_count = (
                    await conn.fetchval(
                        f"SELECT COUNT(*) FROM bicam_raw_congressional.{data_type}"
                    )
                    or 0
                )
                staging_count = (
                    await conn.fetchval(
                        f"SELECT COUNT(*) FROM bicam_staging_congressional.{data_type}"
                    )
                    or 0
                )
                production_count = (
                    await conn.fetchval(
                        f"SELECT COUNT(*) FROM bicam_congressional.{data_type}"
                    )
                    or 0
                )

                lineage_report["data_types"][data_type] = {
                    "raw": raw_count,
                    "staging": staging_count,
                    "production": production_count,
                    "conversion_rate": production_count / raw_count
                    if raw_count > 0
                    else 0,
                }

                # Calculate quality metrics
                quality_metrics["data_types"][data_type] = {
                    "completeness": production_count / raw_count
                    if raw_count > 0
                    else 0,
                    "freshness": "green",  # Would calculate based on last update
                    "validity": "green",  # Would calculate based on validation rules
                }

            except Exception as e:
                logger.error(f"Error getting metrics for {data_type}: {e}")
                continue

    # Calculate overall health
    if quality_metrics["data_types"]:
        avg_completeness = sum(
            dt["completeness"] for dt in quality_metrics["data_types"].values()
        ) / len(quality_metrics["data_types"])
        quality_metrics["overall_health"] = (
            "green"
            if avg_completeness > 0.9
            else "yellow"
            if avg_completeness > 0.7
            else "red"
        )

    context.add_output_metadata(
        {
            "data_types_processed": len(data_types),
            "overall_health": quality_metrics["overall_health"],
        }
    )

    return lineage_report, quality_metrics


# Dynamic asset creation based on configuration
def create_configurable_asset(data_type: str, config: dict[str, Any]):
    """
    Create assets based on specific configuration.

    This allows for custom asset creation based on data type configuration
    from config.yaml files.
    """

    @asset(
        name=f"{data_type}_custom",
        group_name=f"{data_type}_custom",
        compute_kind="custom_processing",
        description=f"Custom processing for {data_type} based on configuration",
    )
    async def custom_configured_asset(
        context: AssetExecutionContext,
        processing_resource: StreamlinedProcessingResource,
    ) -> dict[str, Any]:
        """Execute custom processing based on configuration."""
        coordinator = await processing_resource.get_coordinator()

        # Use configuration to determine processing
        batch_size = config.get("processing", {}).get("batch_size", 100)
        phases = config.get("phases", ["raw", "staging", "production"])

        executor = StreamlinedExecutor(coordinator)
        results = await executor.execute_data_type(
            data_type=data_type,
            phases=phases,
            batch_size=batch_size,
        )

        context.add_output_metadata(
            {
                "batch_size": batch_size,
                "phases_executed": phases,
                "config_driven": True,
            }
        )

        return results

    return custom_configured_asset
