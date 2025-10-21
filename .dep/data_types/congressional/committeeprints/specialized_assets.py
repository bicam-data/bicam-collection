"""
Committees Specialized Dagster Assets

This module provides committees-specific Dagster assets that inherit from CongressionalBaseSpecializedAssets.
It provides the minimal committees-specific overrides while leveraging the base class for
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
from ..base import CongressionalBaseSpecializedAssets

logger = logging.getLogger(__name__)


# =============================================================================
# COMMITTEES SPECIALIZED ASSETS CLASS
# =============================================================================


class CommitteeprintsSpecializedAssets(CongressionalBaseSpecializedAssets):
    """
    Committeeprints-specific specialized assets that inherit from CongressionalBaseSpecializedAssets.

    Provides committeeprints-specific implementations while leveraging the base class
    for standardized pipeline patterns and utilities.
    """

    def __init__(self):
        super().__init__(data_type_name="committeeprints")

    # =============================================================================
    # REQUIRED ABSTRACT METHOD IMPLEMENTATIONS
    # =============================================================================

    def get_fetcher_class(self):
        """Get the committeeprints fetcher class."""
        from .fetcher import CommitteeprintsFetcher

        return CommitteeprintsFetcher

    def get_database_normalizer_class(self):
        """Get the committeeprints database normalizer class."""
        from .database_normalizer import CommitteeprintsDatabaseNormalizer

        return CommitteeprintsDatabaseNormalizer

    def get_cleaner_class(self):
        """Get the committeeprints cleaner class."""
        from .cleaner import CommitteeprintsCleaner

        return CommitteeprintsCleaner

    # =============================================================================
    # CUSTOM COMMITTEEPRINTS ASSET OVERRIDES
    # =============================================================================

    def create_complete_pipeline_asset(
        self,
        asset_name: str | None = None,
        group_name: str | None = None,
        **asset_kwargs,
    ):
        """
        Create committeeprints-specific complete pipeline asset with enhanced monitoring.

        Adds committeeprints-specific table information and asset generation details
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
        async def committeeprints_complete_pipeline_asset(
            context: AssetExecutionContext,
            processing_resource,
        ) -> dict[str, Any]:
            """Committeeprints pipeline summary with enhanced table and asset information."""
            dagster_logger = get_dagster_logger()
            dagster_logger.info("Checking committeeprints pipeline status")

            db_pool = await processing_resource.get_db_pool()

            try:
                # Use base class utilities for core functionality
                raw_committeeprint_ids = await self._get_item_ids_from_raw_data(db_pool)
                raw_count = len(raw_committeeprint_ids)
                staging_count = await self._get_staging_record_count(db_pool)
                production_count = await self._get_production_record_count(db_pool)

                # Create base pipeline summary
                pipeline_summary = {
                    "pipeline": "committeeprints_specialized_integrated",
                    "framework": "base_specialized_assets",
                    "completed_at": datetime.now().isoformat(),
                    "data_availability": {
                        "raw_data": {
                            "total_committeeprints": raw_count,
                            "available": raw_count > 0,
                            "sample_ids": raw_committeeprint_ids[:5]
                            if raw_committeeprint_ids
                            else [],
                        },
                        "staging_data": {
                            "total_committeeprints": staging_count,
                            "available": staging_count > 0,
                        },
                        "production_data": {
                            "total_committeeprints": production_count,
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

                # Add committeeprints-specific enhancements
                self._add_committeeprints_specific_info(pipeline_summary)

                context.add_output_metadata(
                    {
                        "raw_committeeprints_available": raw_count,
                        "staging_committeeprints_available": staging_count,
                        "production_committeeprints_available": production_count,
                        "pipeline_status": pipeline_summary["pipeline_status"],
                        "framework_integration": "base_specialized_assets",
                    }
                )

                dagster_logger.info(
                    f"Committeeprints pipeline status: {pipeline_summary['pipeline_status']}. "
                    f"Raw: {raw_count}, Staging: {staging_count}, Production: {production_count}"
                )

                return pipeline_summary

            except Exception as e:
                error_msg = f"Committeeprints pipeline summary failed: {str(e)}"
                dagster_logger.error(error_msg)
                raise
            finally:
                await db_pool.close()

        return committeeprints_complete_pipeline_asset

    def _add_committeeprints_specific_info(self, pipeline_summary: dict[str, Any]) -> None:
        """Add committeeprints-specific information to pipeline summary."""
        # Get table information from registry
        registry = get_global_registry()
        if registry.is_registered("committeeprints"):
            table_names = registry.get_table_names("committeeprints")
            table_configs = registry.get_table_configs("committeeprints")

            pipeline_summary["table_information"] = {
                "total_tables": len(table_names),
                "table_names": table_names,
                "staging_assets": [f"{table}_staging" for table in table_names],
                "production_assets": table_names,
                "config_source": registry.get_config_file("committeeprints"),
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
        _, _, _, committeeprints_table_assets = _create_core_committeeprints_assets()
        total_committeeprints_assets = len(committeeprints_table_assets) + 4  # Core assets + specialized

        pipeline_summary["asset_generation"] = {
            "total_committeeprints_assets": total_committeeprints_assets,
            "core_pipeline_assets": 3,  # raw, staging, production
            "table_level_assets": len(committeeprints_table_assets),
            "specialized_assets": 2,  # pipeline summary + data quality report
            "generation_method": "base_specialized_assets_inheritance",
        }


# =============================================================================
# LAZY IMPORT AND ASSET CREATION
# =============================================================================


def _create_core_committeeprints_assets():
    """Lazily create core committeeprints assets to avoid circular imports."""
    from ....dagster_pipeline.generalized_assets import (
        create_production_data_asset,
        create_raw_data_asset,
        create_staging_data_asset,
        create_table_level_assets,
    )

    committeeprints_raw_data = create_raw_data_asset("committeeprints")
    committeeprints_staging_data = create_staging_data_asset("committeeprints")
    committeeprints_production_data = create_production_data_asset("committeeprints")
    committeeprints_table_assets = create_table_level_assets("committeeprints")

    return (
        committeeprints_raw_data,
        committeeprints_staging_data,
        committeeprints_production_data,
        committeeprints_table_assets,
    )


# =============================================================================
# ASSET INSTANCE CREATION
# =============================================================================


# Create the committees specialized assets instance
committeeprints_specialized = CommitteeprintsSpecializedAssets()

# Create the assets - using base class for data quality report, custom for pipeline
committeeprints_complete_pipeline = committeeprints_specialized.create_complete_pipeline_asset()
committeeprints_data_quality_report = (
committeeprints_specialized.create_data_quality_report_asset()
)

# Export for framework integration
committeeprints_specialized_assets = [
    committeeprints_complete_pipeline,
    committeeprints_data_quality_report,
]


# =============================================================================
# COMMITTEEPRINTS-SPECIFIC HELPER FUNCTIONS (PRESERVED CUSTOM LOGIC)
# =============================================================================


# Most helper functions are now provided by BaseSpecializedAssets
# Only keeping committeeprints-specific ones that need custom logic


def _determine_pipeline_status(
    raw_count: int, staging_count: int, production_count: int
) -> str:
    """Determine overall pipeline status based on data counts (committeeprints-specific logic)."""
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
    """Generate processing recommendations based on current data state (committeeprints-specific)."""
    recommendations = []

    if raw_count == 0:
        recommendations.append(
            "Run committeeprints_raw_data to fetch raw data from Congressional API"
        )
    elif production_count == 0:
        recommendations.append("Run committeeprints_production_data to clean staging data")
    elif production_count < staging_count:
        recommendations.append(
            "Some staging data hasn't been cleaned - consider re-running committeeprints_production_data"
        )
    elif staging_count < raw_count:
        recommendations.append(
            "Some raw data hasn't been normalized - consider re-running committeeprints_staging_data"
        )
    else:
        recommendations.append("Committeeprints pipeline is fully up-to-date")

    # Add table-level recommendations
    if production_count > 0:
        recommendations.append(
            "Use table-level assets for granular processing (e.g., committeeprints_actions_staging)"
        )
        recommendations.append(
            "Monitor data quality with committeeprints_data_quality_report"
        )

    return recommendations


# =============================================================================
# ASSET COLLECTION AND LAZY CREATION
# =============================================================================


def _get_all_committeeprints_assets():
    """Get all committeeprints assets (created lazily to avoid circular imports)."""
    # Create core assets lazily
    (
        committeeprints_raw_data,
        committeeprints_staging_data,
        committeeprints_production_data,
        committeeprints_table_assets,
    ) = _create_core_committeeprints_assets()

    # Collect all committees assets
    all_committeeprints_assets = [
        committeeprints_raw_data,
        committeeprints_staging_data,
        committeeprints_production_data,
        committeeprints_complete_pipeline,
        committeeprints_data_quality_report,
    ]

    # Add table-level assets generated by the framework
    all_committeeprints_assets.extend(committeeprints_table_assets)

    return all_committeeprints_assets


def _create_definitions():
    """Create Dagster definitions lazily."""
    from ....dagster_pipeline.shared_resources import ProcessingResource

    return Definitions(
        assets=_get_all_committeeprints_assets(),
        resources={
            "processing_resource": ProcessingResource(),
        },
    )


# =============================================================================
# USAGE DOCUMENTATION
# =============================================================================

"""
COMMITTEEPRINTS SPECIALIZED ASSETS - CLEAN AND SIMPLIFIED

Inherits from CongressionalBaseSpecializedAssets for standardized patterns
Minimal custom code - only where committeeprints-specific logic is needed

CORE ASSETS:
- dagster asset materialize -a committeeprints_raw_data
- dagster asset materialize -a committeeprints_staging_data
- dagster asset materialize -a committeeprints_production_data

SPECIALIZED ASSETS:
- dagster asset materialize -a committeeprints_complete_pipeline (enhanced monitoring)
- dagster asset materialize -a committeeprints_data_quality_report (base class implementation)
"""
