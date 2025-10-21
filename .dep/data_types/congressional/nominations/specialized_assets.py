"""
Nominations Specialized Dagster Assets

This module provides nominations-specific Dagster assets that inherit from CongressionalBaseSpecializedAssets.
It provides the minimal nominations-specific overrides while leveraging the base class for
standardized pipeline patterns and utilities.
"""

import logging
from datetime import datetime
from typing import Any

from dagster import (
    AssetExecutionContext,
    Definitions,
    asset,
    get_dagster_logger,
)

from ....libs.data_type_router import get_global_registry
from ...abstract.base_specialized_assets import BaseSpecializedAssets

logger = logging.getLogger(__name__)


# =============================================================================
# NOMINATIONS SPECIALIZED ASSETS CLASS
# =============================================================================


class NominationsSpecializedAssets(BaseSpecializedAssets):
    """
    Nominations-specific specialized assets that inherit from CongressionalBaseSpecializedAssets.

    Provides nominations-specific implementations while leveraging the base class
    for standardized pipeline patterns and utilities.
    """

    def __init__(self):
        super().__init__(data_type_name="nominations")

    # =============================================================================
    # REQUIRED ABSTRACT METHOD IMPLEMENTATIONS
    # =============================================================================

    def get_fetcher_class(self):
        """Get the nominations fetcher class."""
        from .fetcher import NominationsFetcher

        return NominationsFetcher

    def get_database_normalizer_class(self):
        """Get the nominations database normalizer class."""
        from .database_normalizer import NominationsDatabaseNormalizer

        return NominationsDatabaseNormalizer

    def get_cleaner_class(self):
        """Get the nominations cleaner class."""
        from .cleaner import NominationsCleaner

        return NominationsCleaner

    # =============================================================================
    # CUSTOM NOMINATIONS ASSET OVERRIDES
    # =============================================================================

    def create_complete_pipeline_asset(
        self,
        asset_name: str | None = None,
        group_name: str | None = None,
        **asset_kwargs,
    ):
        """
        Create nominations-specific complete pipeline asset with enhanced monitoring.

        Adds nominations-specific table information and asset generation details
        to the base pipeline asset functionality.
        """
        asset_name = asset_name or f"{self.data_type_name}_complete_pipeline"
        group_name = group_name or f"{self.data_type_name}_pipeline"

        @asset(
            name=asset_name,
            group_name=group_name,
            compute_kind="summary",
            **asset_kwargs,
        )
        async def nominations_complete_pipeline_asset(
            context: AssetExecutionContext,
            processing_resource,
        ) -> dict[str, Any]:
            """Nominations pipeline summary with enhanced table and asset information."""
            dagster_logger = get_dagster_logger()
            dagster_logger.info("Checking nominations pipeline status")

            db_pool = await processing_resource.get_db_pool()

            try:
                # Use base class utilities for core functionality
                raw_nomination_ids = await self._get_item_ids_from_raw_data(db_pool)
                raw_count = len(raw_nomination_ids)
                staging_count = await self._get_staging_record_count(db_pool)
                production_count = await self._get_production_record_count(db_pool)

                # Create base pipeline summary
                pipeline_summary = {
                    "pipeline": "nominations_specialized_integrated",
                    "framework": "base_specialized_assets",
                    "completed_at": datetime.now().isoformat(),
                    "data_availability": {
                        "raw_data": {
                            "total_nominations": raw_count,
                            "available": raw_count > 0,
                            "sample_ids": raw_nomination_ids[:5]
                            if raw_nomination_ids
                            else [],
                        },
                        "staging_data": {
                            "total_nominations": staging_count,
                            "available": staging_count > 0,
                        },
                        "production_data": {
                            "total_nominations": production_count,
                            "available": production_count > 0,
                        },
                    },
                    "pipeline_status": _determine_pipeline_status(
                        raw_count, staging_count, production_count
                    ),
                    "recommendations": _generate_recommendations(
                        raw_count, staging_count, production_count
                    ),
                    "key_manager_status": processing_resource.get_key_manager_status(),
                }

                # Add nominations-specific enhancements
                self._add_nominations_specific_info(pipeline_summary)

                context.add_output_metadata(
                    {
                        "raw_nominations_available": raw_count,
                        "staging_nominations_available": staging_count,
                        "production_nominations_available": production_count,
                        "pipeline_status": pipeline_summary["pipeline_status"],
                        "framework_integration": "base_specialized_assets",
                    }
                )

                dagster_logger.info(
                    f"Nominations pipeline status: {pipeline_summary['pipeline_status']}. "
                    f"Raw: {raw_count}, Staging: {staging_count}, Production: {production_count}"
                )

                return pipeline_summary

            except Exception as e:
                error_msg = f"Nominations pipeline summary failed: {str(e)}"
                dagster_logger.error(error_msg)
                raise
            finally:
                await db_pool.close()

        return nominations_complete_pipeline_asset

    def _add_nominations_specific_info(self, pipeline_summary: dict[str, Any]) -> None:
        """Add nominations-specific information to pipeline summary."""
        # Get table information from registry
        registry = get_global_registry()
        if registry.is_registered("nominations"):
            table_names = registry.get_table_names("nominations")
            table_configs = registry.get_table_configs("nominations")

            pipeline_summary["table_information"] = {
                "total_tables": len(table_names),
                "table_names": table_names,
                "staging_assets": [f"{table}_staging" for table in table_names],
                "production_assets": table_names,
                "config_source": registry.get_config_file("nominations"),
            }

            # Add sample config info
            if table_configs:
                sample_configs = {
                    name: {
                        "field_count": len(config.get("fields", {})),
                        "is_main_table": config.get("is_main", False),
                        "related_fields": list(config.get("related_fields", {}).keys()),
                    }
                    for name, config in list(table_configs.items())[:3]
                }
                pipeline_summary["table_information"]["sample_table_config"] = (
                    sample_configs
                )

        # Add asset generation information
        _, _, _, nominations_table_assets = _create_core_nominations_assets()
        total_nominations_assets = len(nominations_table_assets) + 4  # Core assets + specialized

        pipeline_summary["asset_generation"] = {
            "total_nominations_assets": total_nominations_assets,
            "core_pipeline_assets": 3,  # raw, staging, production
            "table_level_assets": len(nominations_table_assets),
            "specialized_assets": 2,  # pipeline summary + data quality report
            "generation_method": "base_specialized_assets_inheritance",
        }


# =============================================================================
# LAZY IMPORT AND ASSET CREATION
# =============================================================================


def _create_core_nominations_assets():
    """Lazily create core nominations assets to avoid circular imports."""
    from ....dagster_pipeline.generalized_assets import (
        create_production_data_asset,
        create_raw_data_asset,
        create_staging_data_asset,
        create_table_level_assets,
    )

    nominations_raw_data = create_raw_data_asset("nominations")
    nominations_staging_data = create_staging_data_asset("nominations")
    nominations_production_data = create_production_data_asset("nominations")
    nominations_table_assets = create_table_level_assets("nominations")

    return (
        nominations_raw_data,
        nominations_staging_data,
        nominations_production_data,
        nominations_table_assets,
    )


# =============================================================================
# ASSET INSTANCE CREATION
# =============================================================================


# Create the nominations specialized assets instance
nominations_specialized = NominationsSpecializedAssets()

# Create the assets - using base class for data quality report, custom for pipeline
nominations_complete_pipeline = nominations_specialized.create_complete_pipeline_asset()
nominations_data_quality_report = (
    nominations_specialized.create_data_quality_report_asset()
)

# Export for framework integration
nominations_specialized_assets = [
    nominations_complete_pipeline,
    nominations_data_quality_report,
]


# =============================================================================
# NOMINATIONS-SPECIFIC HELPER FUNCTIONS (PRESERVED CUSTOM LOGIC)
# =============================================================================


# Most helper functions are now provided by BaseSpecializedAssets
# Only keeping nominations-specific ones that need custom logic


def _determine_pipeline_status(
    raw_count: int, staging_count: int, production_count: int
) -> str:
    """Determine overall pipeline status based on data counts (nominations-specific logic)."""
    if production_count > 0:
        if production_count == staging_count == raw_count:
            return "complete"
        elif production_count < staging_count or staging_count < raw_count:
            return "partially_complete"
        else:
            return "complete"
    elif staging_count > 0:
        return "staging_only"
    elif raw_count > 0:
        return "raw_only"
    else:
        return "no_data"


def _generate_recommendations(
    raw_count: int, staging_count: int, production_count: int
) -> list[str]:
    """Generate processing recommendations based on current data state (nominations-specific)."""
    recommendations = []

    if raw_count == 0:
        recommendations.append(
            "Run nominations_raw_data to fetch raw data from Congressional API"
        )
    elif production_count == 0:
        recommendations.append("Run nominations_production_data to clean staging data")
    elif production_count < staging_count:
        recommendations.append(
            "Some staging data hasn't been cleaned - consider re-running nominations_production_data"
        )
    elif staging_count < raw_count:
        recommendations.append(
            "Some raw data hasn't been normalized - consider re-running nominations_staging_data"
        )
    else:
        recommendations.append("Nominations pipeline is fully up-to-date")

    # Add table-level recommendations
    if production_count > 0:
        recommendations.append(
            "Use table-level assets for granular processing (e.g., nominations_actions_staging)"
        )
        recommendations.append(
            "Monitor data quality with nominations_data_quality_report"
        )

    return recommendations


# =============================================================================
# ASSET COLLECTION AND LAZY CREATION
# =============================================================================


def _get_all_nominations_assets():
    """Get all nominations assets (created lazily to avoid circular imports)."""
    # Create core assets lazily
    (
        nominations_raw_data,
        nominations_staging_data,
        nominations_production_data,
        nominations_table_assets,
    ) = _create_core_nominations_assets()

    # Collect all nominations assets
    all_nominations_assets = [
        nominations_raw_data,
        nominations_staging_data,
        nominations_production_data,
        nominations_complete_pipeline,
        nominations_data_quality_report,
    ]

    # Add table-level assets generated by the framework
    all_nominations_assets.extend(nominations_table_assets)

    return all_nominations_assets


def _create_definitions():
    """Create Dagster definitions lazily."""
    from ....dagster_pipeline.shared_resources import ProcessingResource

    return Definitions(
        assets=_get_all_nominations_assets(),
        resources={
            "processing_resource": ProcessingResource(),
        },
    )


# =============================================================================
# USAGE DOCUMENTATION
# =============================================================================

"""
NOMINATIONS SPECIALIZED ASSETS - CLEAN AND SIMPLIFIED

Inherits from CongressionalBaseSpecializedAssets for standardized patterns
Minimal custom code - only where nominations-specific logic is needed

CORE ASSETS:
- dagster asset materialize -a nominations_raw_data
- dagster asset materialize -a nominations_staging_data
- dagster asset materialize -a nominations_production_data

SPECIALIZED ASSETS:
- dagster asset materialize -a nominations_complete_pipeline (enhanced monitoring)
- dagster asset materialize -a nominations_data_quality_report (base class implementation)
"""
