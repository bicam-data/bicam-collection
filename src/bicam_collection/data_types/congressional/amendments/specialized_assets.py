"""
Amendments Specialized Dagster Assets

This module provides amendments-specific Dagster assets that inherit from CongressionalBaseSpecializedAssets.
It provides the minimal amendments-specific overrides while leveraging the base class for
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
# AMENDMENTS SPECIALIZED ASSETS CLASS
# =============================================================================


class AmendmentsSpecializedAssets(CongressionalBaseSpecializedAssets):
    """
    Amendments-specific specialized assets that inherit from CongressionalBaseSpecializedAssets.

    Provides amendments-specific implementations while leveraging the base class
    for standardized pipeline patterns and utilities.
    """

    def __init__(self):
        super().__init__(data_type_name="amendments")

    # =============================================================================
    # REQUIRED ABSTRACT METHOD IMPLEMENTATIONS
    # =============================================================================

    def get_fetcher_class(self):
        """Get the amendments fetcher class."""
        from .fetcher import AmendmentsFetcher

        return AmendmentsFetcher

    def get_database_normalizer_class(self):
        """Get the amendments database normalizer class."""
        from .database_normalizer import AmendmentsDatabaseNormalizer

        return AmendmentsDatabaseNormalizer

    def get_cleaner_class(self):
        """Get the amendments cleaner class."""
        from .cleaner import AmendmentsCleaner

        return AmendmentsCleaner

    # =============================================================================
    # CUSTOM AMENDMENTS ASSET OVERRIDES
    # =============================================================================

    def create_complete_pipeline_asset(
        self,
        asset_name: str | None = None,
        group_name: str | None = None,
        **asset_kwargs,
    ):
        """
        Create amendments-specific complete pipeline asset with enhanced monitoring.

        Adds amendments-specific table information and asset generation details
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
        async def amendments_complete_pipeline_asset(
            context: AssetExecutionContext,
            processing_resource,
        ) -> dict[str, Any]:
            """Amendments pipeline summary with enhanced table and asset information."""
            dagster_logger = get_dagster_logger()
            dagster_logger.info("Checking amendments pipeline status")

            db_pool = await processing_resource.get_db_pool()

            try:
                # Use base class utilities for core functionality
                raw_amendment_ids = await self._get_item_ids_from_raw_data(db_pool)
                raw_count = len(raw_amendment_ids)
                staging_count = await self._get_staging_record_count(db_pool)
                production_count = await self._get_production_record_count(db_pool)

                # Create base pipeline summary
                pipeline_summary = {
                    "pipeline": "amendments_specialized_integrated",
                    "framework": "base_specialized_assets",
                    "completed_at": datetime.now().isoformat(),
                    "data_availability": {
                        "raw_data": {
                            "total_amendments": raw_count,
                            "available": raw_count > 0,
                            "sample_ids": raw_amendment_ids[:5]
                            if raw_amendment_ids
                            else [],
                        },
                        "staging_data": {
                            "total_amendments": staging_count,
                            "available": staging_count > 0,
                        },
                        "production_data": {
                            "total_amendments": production_count,
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

                # Add amendments-specific enhancements
                self._add_amendments_specific_info(pipeline_summary)

                context.add_output_metadata(
                    {
                        "raw_amendments_available": raw_count,
                        "staging_amendments_available": staging_count,
                        "production_amendments_available": production_count,
                        "pipeline_status": pipeline_summary["pipeline_status"],
                        "framework_integration": "base_specialized_assets",
                    }
                )

                dagster_logger.info(
                    f"Amendments pipeline status: {pipeline_summary['pipeline_status']}. "
                    f"Raw: {raw_count}, Staging: {staging_count}, Production: {production_count}"
                )

                return pipeline_summary

            except Exception as e:
                error_msg = f"Amendments pipeline summary failed: {str(e)}"
                dagster_logger.error(error_msg)
                raise
            finally:
                await db_pool.close()

        return amendments_complete_pipeline_asset

    def _add_amendments_specific_info(self, pipeline_summary: dict[str, Any]) -> None:
        """Add amendments-specific information to pipeline summary."""
        # Get table information from registry
        registry = get_global_registry()
        if registry.is_registered("amendments"):
            table_names = registry.get_table_names("amendments")
            table_configs = registry.get_table_configs("amendments")

            pipeline_summary["table_information"] = {
                "total_tables": len(table_names),
                "table_names": table_names,
                "staging_assets": [f"{table}_staging" for table in table_names],
                "production_assets": table_names,
                "config_source": registry.get_config_file("amendments"),
            }

            # Add sample config info
            if table_configs:
                sample_configs = {
                    name: {
                        "field_count": len(config.get("fields", {})),
                        "is_main_table": config.get("is_main_table", False),
                        "related_fields": list(config.get("related_fields", {}).keys()),
                    }
                    for name, config in list(table_configs.items())[:3]
                }
                pipeline_summary["table_information"]["sample_table_config"] = (
                    sample_configs
                )

        # Add asset generation information
        _, _, _, amendments_table_assets = _create_core_amendments_assets()
        total_assets = len(amendments_table_assets) + 4  # Core assets + specialized

        pipeline_summary["asset_generation"] = {
            "total_amendments_assets": total_assets,
            "core_pipeline_assets": 3,  # raw, staging, production
            "table_level_assets": len(amendments_table_assets),
            "specialized_assets": 2,  # pipeline summary + quality report
            "generation_method": "base_specialized_assets_inheritance",
        }


# =============================================================================
# LAZY IMPORT AND ASSET CREATION
# =============================================================================


def _create_core_amendments_assets():
    """Lazily create core amendments assets to avoid circular imports."""
    from ....dagster_pipeline.generalized_assets import (
        create_production_data_asset,
        create_raw_data_asset,
        create_staging_data_asset,
        create_table_level_assets,
    )

    amendments_raw_data = create_raw_data_asset("amendments")
    amendments_staging_data = create_staging_data_asset("amendments")
    amendments_production_data = create_production_data_asset("amendments")
    amendments_table_assets = create_table_level_assets("amendments")

    return (
        amendments_raw_data,
        amendments_staging_data,
        amendments_production_data,
        amendments_table_assets,
    )


# =============================================================================
# ASSET INSTANCE CREATION
# =============================================================================


# Create the amendments specialized assets instance
amendments_specialized = AmendmentsSpecializedAssets()

# Create the assets - using base class for data quality report, custom for pipeline
amendments_complete_pipeline = amendments_specialized.create_complete_pipeline_asset()
amendments_data_quality_report = (
    amendments_specialized.create_data_quality_report_asset()
)

# Export for framework integration
amendments_specialized_assets = [
    amendments_complete_pipeline,
    amendments_data_quality_report,
]


# =============================================================================
# AMENDMENTS-SPECIFIC HELPER FUNCTIONS (PRESERVED CUSTOM LOGIC)
# =============================================================================


# Most helper functions are now provided by BaseSpecializedAssets
# Only keeping amendments-specific ones that need custom logic


def _determine_pipeline_status(
    raw_count: int, staging_count: int, production_count: int
) -> str:
    """Determine overall pipeline status based on data counts (amendments-specific logic)."""
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
    """Generate processing recommendations based on current data state (amendments-specific)."""
    recommendations = []

    if raw_count == 0:
        recommendations.append(
            "Run amendments_raw_data to fetch raw data from Congressional API"
        )
    elif staging_count == 0:
        recommendations.append("Run amendments_staging_data to normalize raw data")
    elif production_count == 0:
        recommendations.append("Run amendments_production_data to clean staging data")
    elif production_count < staging_count:
        recommendations.append(
            "Some staging data hasn't been cleaned - consider re-running amendments_production_data"
        )
    elif staging_count < raw_count:
        recommendations.append(
            "Some raw data hasn't been normalized - consider re-running amendments_staging_data"
        )
    else:
        recommendations.append("Amendments pipeline is fully up-to-date")

    # Add table-level recommendations
    if production_count > 0:
        recommendations.append(
            "Use table-level assets for granular processing (e.g., amendments_actions_staging)"
        )
        recommendations.append(
            "Monitor data quality with amendments_data_quality_report"
        )

    return recommendations


# =============================================================================
# ASSET COLLECTION AND LAZY CREATION
# =============================================================================


def _get_all_amendments_assets():
    """Get all amendments assets (created lazily to avoid circular imports)."""
    # Create core assets lazily
    (
        amendments_raw_data,
        amendments_staging_data,
        amendments_production_data,
        amendments_table_assets,
    ) = _create_core_amendments_assets()

    # Collect all amendments assets
    all_amendments_assets = [
        amendments_raw_data,
        amendments_staging_data,
        amendments_production_data,
        amendments_complete_pipeline,
        amendments_data_quality_report,
    ]

    # Add table-level assets generated by the framework
    all_amendments_assets.extend(amendments_table_assets)

    return all_amendments_assets


def _create_definitions():
    """Create Dagster definitions lazily."""
    from ....dagster_pipeline.shared_resources import ProcessingResource

    return Definitions(
        assets=_get_all_amendments_assets(),
        resources={
            "processing_resource": ProcessingResource(),
        },
    )


# =============================================================================
# USAGE DOCUMENTATION
# =============================================================================

"""
AMENDMENTS SPECIALIZED ASSETS - CLEAN AND SIMPLIFIED

Inherits from CongressionalBaseSpecializedAssets for standardized patterns
Minimal custom code - only where amendments-specific logic is needed
Uses base class utilities for common operations
Follows the same pattern as bills implementation

CORE ASSETS:
- dagster asset materialize -a amendments_raw_data
- dagster asset materialize -a amendments_staging_data
- dagster asset materialize -a amendments_production_data

SPECIALIZED ASSETS:
- dagster asset materialize -a amendments_complete_pipeline (enhanced monitoring)
- dagster asset materialize -a amendments_data_quality_report (base class implementation)
"""
