"""
Bills Specialized Dagster Assets

This module provides bills-specific Dagster assets that inherit from BaseSpecializedAssets.
It provides the minimal bills-specific overrides while leveraging the base class for
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
from ..base import BaseSpecializedAssets

logger = logging.getLogger(__name__)


# =============================================================================
# BILLS SPECIALIZED ASSETS CLASS
# =============================================================================


class BillsSpecializedAssets(BaseSpecializedAssets):
    """
    Bills-specific specialized assets that inherit from BaseSpecializedAssets.

    Provides bills-specific implementations while leveraging the base class
    for standardized pipeline patterns and utilities.
    """

    def __init__(self):
        super().__init__(data_type_name="bills")

    # =============================================================================
    # REQUIRED ABSTRACT METHOD IMPLEMENTATIONS
    # =============================================================================

    def get_fetcher_class(self):
        """Get the bills fetcher class."""
        from .fetcher import BillsFetcher

        return BillsFetcher

    def get_database_normalizer_class(self):
        """Get the bills database normalizer class."""
        from .database_normalizer import BillsDatabaseNormalizer

        return BillsDatabaseNormalizer

    def get_cleaner_class(self):
        """Get the bills cleaner class."""
        from .cleaner import BillsCleaner

        return BillsCleaner

    # =============================================================================
    # CUSTOM BILLS ASSET OVERRIDES
    # =============================================================================

    def create_complete_pipeline_asset(
        self,
        asset_name: str | None = None,
        group_name: str | None = None,
        **asset_kwargs,
    ):
        """
        Create bills-specific complete pipeline asset with enhanced monitoring.

        Adds bills-specific table information and asset generation details
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
        async def bills_complete_pipeline_asset(
            context: AssetExecutionContext,
            processing_resource,
        ) -> dict[str, Any]:
            """Bills pipeline summary with enhanced table and asset information."""
            dagster_logger = get_dagster_logger()
            dagster_logger.info("Checking bills pipeline status")

            db_pool = await processing_resource.get_db_pool()

            try:
                # Use base class utilities for core functionality
                raw_bill_ids = await self._get_item_ids_from_raw_data(db_pool)
                raw_count = len(raw_bill_ids)
                staging_count = await self._get_staging_record_count(db_pool)
                production_count = await self._get_production_record_count(db_pool)

                # Create base pipeline summary
                pipeline_summary = {
                    "pipeline": "bills_specialized_integrated",
                    "framework": "base_specialized_assets",
                    "completed_at": datetime.now().isoformat(),
                    "data_availability": {
                        "raw_data": {
                            "total_bills": raw_count,
                            "available": raw_count > 0,
                            "sample_ids": raw_bill_ids[:5] if raw_bill_ids else [],
                        },
                        "staging_data": {
                            "total_bills": staging_count,
                            "available": staging_count > 0,
                        },
                        "production_data": {
                            "total_bills": production_count,
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

                # Add bills-specific enhancements
                self._add_bills_specific_info(pipeline_summary)

                context.add_output_metadata(
                    {
                        "raw_bills_available": raw_count,
                        "staging_bills_available": staging_count,
                        "production_bills_available": production_count,
                        "pipeline_status": pipeline_summary["pipeline_status"],
                        "framework_integration": "base_specialized_assets",
                    }
                )

                dagster_logger.info(
                    f"Bills pipeline status: {pipeline_summary['pipeline_status']}. "
                    f"Raw: {raw_count}, Staging: {staging_count}, Production: {production_count}"
                )

                return pipeline_summary

            except Exception as e:
                error_msg = f"Bills pipeline summary failed: {str(e)}"
                dagster_logger.error(error_msg)
                raise
            finally:
                await db_pool.close()

        return bills_complete_pipeline_asset

    def _add_bills_specific_info(self, pipeline_summary: dict[str, Any]) -> None:
        """Add bills-specific information to pipeline summary."""
        # Get table information from registry
        registry = get_global_registry()
        if registry.is_registered("bills"):
            table_names = registry.get_table_names("bills")
            table_configs = registry.get_table_configs("bills")

            pipeline_summary["table_information"] = {
                "total_tables": len(table_names),
                "table_names": table_names,
                "staging_assets": [f"{table}_staging" for table in table_names],
                "production_assets": table_names,
                "config_source": registry.get_config_file("bills"),
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
        _, _, _, bills_table_assets = _create_core_bills_assets()
        total_assets = len(bills_table_assets) + 4  # Core assets + specialized

        pipeline_summary["asset_generation"] = {
            "total_bills_assets": total_assets,
            "core_pipeline_assets": 3,  # raw, staging, production
            "table_level_assets": len(bills_table_assets),
            "specialized_assets": 2,  # pipeline summary + quality report
            "generation_method": "base_specialized_assets_inheritance",
        }


# =============================================================================
# LAZY IMPORT AND ASSET CREATION
# =============================================================================


def _create_core_bills_assets():
    """Lazily create core bills assets to avoid circular imports."""
    from ....dagster_pipeline.generalized_assets import (
        create_production_data_asset,
        create_raw_data_asset,
        create_staging_data_asset,
        create_table_level_assets,
    )

    bills_raw_data = create_raw_data_asset("bills")
    bills_staging_data = create_staging_data_asset("bills")
    bills_production_data = create_production_data_asset("bills")
    bills_table_assets = create_table_level_assets("bills")

    return bills_raw_data, bills_staging_data, bills_production_data, bills_table_assets


# =============================================================================
# ASSET INSTANCE CREATION
# =============================================================================


# Create the bills specialized assets instance
bills_specialized = BillsSpecializedAssets()

# Create the assets - using base class for data quality report, custom for pipeline
bills_complete_pipeline = bills_specialized.create_complete_pipeline_asset()
bills_data_quality_report = bills_specialized.create_data_quality_report_asset()

# Export for framework integration
bills_specialized_assets = [
    bills_complete_pipeline,
    bills_data_quality_report,
]


# =============================================================================
# BILLS-SPECIFIC HELPER FUNCTIONS (PRESERVED CUSTOM LOGIC)
# =============================================================================


# Most helper functions are now provided by BaseSpecializedAssets
# Only keeping bills-specific ones that need custom logic


def _determine_pipeline_status(
    raw_count: int, staging_count: int, production_count: int
) -> str:
    """Determine overall pipeline status based on data counts (bills-specific logic)."""
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
    """Generate processing recommendations based on current data state (bills-specific)."""
    recommendations = []

    if raw_count == 0:
        recommendations.append(
            "Run bills_raw_data to fetch raw data from Congressional API"
        )
    elif staging_count == 0:
        recommendations.append("Run bills_staging_data to normalize raw data")
    elif production_count == 0:
        recommendations.append("Run bills_production_data to clean staging data")
    elif production_count < staging_count:
        recommendations.append(
            "Some staging data hasn't been cleaned - consider re-running bills_production_data"
        )
    elif staging_count < raw_count:
        recommendations.append(
            "Some raw data hasn't been normalized - consider re-running bills_staging_data"
        )
    else:
        recommendations.append("Bills pipeline is fully up-to-date")

    # Add table-level recommendations
    if production_count > 0:
        recommendations.append(
            "Use table-level assets for granular processing (e.g., bills_actions_staging)"
        )
        recommendations.append("Monitor data quality with bills_data_quality_report")

    return recommendations


# =============================================================================
# ASSET COLLECTION AND LAZY CREATION
# =============================================================================


def _get_all_bills_assets():
    """Get all bills assets (created lazily to avoid circular imports)."""
    # Create core assets lazily
    bills_raw_data, bills_staging_data, bills_production_data, bills_table_assets = (
        _create_core_bills_assets()
    )

    # Collect all bills assets
    all_bills_assets = [
        bills_raw_data,
        bills_staging_data,
        bills_production_data,
        bills_complete_pipeline,
        bills_data_quality_report,
    ]

    # Add table-level assets generated by the framework
    all_bills_assets.extend(bills_table_assets)

    return all_bills_assets


def _create_definitions():
    """Create Dagster definitions lazily."""
    from ....dagster_pipeline.shared_resources import ProcessingResource

    return Definitions(
        assets=_get_all_bills_assets(),
        resources={
            "processing_resource": ProcessingResource(),
        },
    )


# =============================================================================
# USAGE DOCUMENTATION
# =============================================================================

"""
BILLS SPECIALIZED ASSETS - CLEAN AND SIMPLIFIED

Inherits from BaseSpecializedAssets for standardized patterns
Minimal custom code - only where bills-specific logic is needed
Uses base class utilities for common operations
Eliminated ~200 lines of redundant code

CORE ASSETS:
- dagster asset materialize -a bills_raw_data
- dagster asset materialize -a bills_staging_data
- dagster asset materialize -a bills_production_data

SPECIALIZED ASSETS:
- dagster asset materialize -a bills_complete_pipeline (enhanced monitoring)
- dagster asset materialize -a bills_data_quality_report (base class implementation)
"""
