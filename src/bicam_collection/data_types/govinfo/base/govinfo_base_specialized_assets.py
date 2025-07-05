"""
GovInfo-specific base specialized assets.

This module provides GovInfo-specific asset configuration and schema references.
"""

import logging
from typing import Any

from dagster import AssetIn, AssetOut, asset, multi_asset

from ....dagster_pipeline.shared_resources import ProcessingResource
from ...abstract import AbstractSpecializedAssets

logger = logging.getLogger(__name__)


class GovInfoBaseSpecializedAssets(AbstractSpecializedAssets):
    """
    GovInfo-specific base assets.

    Provides GovInfo-specific asset configuration and schema references
    with comprehensive pipeline implementations for the 4-phase GovInfo pattern.
    """

    def __init__(self, data_type_name: str):
        super().__init__(data_type_name, source_system="govinfo")

    def get_fetcher_class(self):
        """Get the fetcher class for this data type."""
        # This should be implemented by concrete data type classes
        raise NotImplementedError("Subclasses must implement get_fetcher_class")

    def get_database_normalizer_class(self):
        """Get the database normalizer class for this data type."""
        # This should be implemented by concrete data type classes
        raise NotImplementedError(
            "Subclasses must implement get_database_normalizer_class"
        )

    def get_cleaner_class(self):
        """Get the cleaner class for this data type."""
        # This should be implemented by concrete data type classes
        raise NotImplementedError("Subclasses must implement get_cleaner_class")

    def create_fetch_asset(self, **kwargs):
        """Create GovInfo fetch asset for 4-phase processing."""

        @asset(
            name=f"{self.data_type_name}_govinfo_fetched",
            description=f"GovInfo {self.data_type_name} raw data (4-phase: collection → package → granules → granule data)",
            metadata={
                "source_system": "govinfo",
                "data_type": self.data_type_name,
                "processing_phases": 4,
                "schema": "bicam_raw_govinfo",
            },
        )
        async def govinfo_fetch_data(
            context, processing_resource: ProcessingResource
        ) -> dict[str, Any]:
            """Fetch GovInfo data using 4-phase pattern."""
            try:
                fetcher_class = self.get_fetcher_class()

                # Create API client
                api_client = processing_resource.get_govinfo_api_client()

                # Create fetcher
                fetcher = fetcher_class(
                    client=api_client,
                    db_pool=processing_resource.get_database_pool(),
                    data_type_name=self.data_type_name,
                    checkpoint_manager=processing_resource.get_checkpoint_manager(),
                    run_manager=processing_resource.get_run_manager(),
                )

                # Set processing resource for parallel capabilities
                fetcher.set_processing_resource(processing_resource)

                # Process with default parameters
                stats = fetcher.process_items(
                    enable_parallelization=True,
                    max_concurrent=10,
                    batch_size=50,
                )

                context.log.info(
                    f"GovInfo {self.data_type_name} fetch completed: {stats}"
                )

                return {
                    "status": "success",
                    "stats": stats,
                    "data_type": self.data_type_name,
                    "source_system": "govinfo",
                    "phases_completed": [
                        "collection",
                        "package",
                        "granules",
                        "granule_data",
                    ],
                }

            except Exception as e:
                context.log.error(f"GovInfo {self.data_type_name} fetch failed: {e}")
                raise

        return govinfo_fetch_data

    def create_normalize_asset(self, **kwargs):
        """Create GovInfo normalize asset."""

        @asset(
            name=f"{self.data_type_name}_govinfo_normalized",
            description=f"GovInfo {self.data_type_name} normalized data",
            ins={f"{self.data_type_name}_govinfo_fetched": AssetIn()},
            metadata={
                "source_system": "govinfo",
                "data_type": self.data_type_name,
                "source_schema": "bicam_raw_govinfo",
                "target_schema": "bicam_staging_govinfo",
            },
        )
        async def govinfo_normalize_data(
            context, processing_resource: ProcessingResource, **upstream_assets
        ) -> dict[str, Any]:
            """Normalize GovInfo raw data to staging schema."""
            try:
                normalizer_class = self.get_database_normalizer_class()

                # Create normalizer
                normalizer = normalizer_class(
                    db_pool=processing_resource.get_database_pool(),
                    data_type_name=self.data_type_name,
                    checkpoint_manager=processing_resource.get_checkpoint_manager(),
                    run_manager=processing_resource.get_run_manager(),
                )

                # Get package IDs from the upstream fetch result
                fetch_result = upstream_assets.get(
                    f"{self.data_type_name}_govinfo_fetched", {}
                )

                # Get package IDs to process (in a real implementation, this would
                # query the raw tables to get recently fetched package IDs)
                package_ids = fetch_result.get("package_ids", [])

                if not package_ids:
                    # Fallback: get recent package IDs from database
                    package_ids = normalizer._get_recent_package_ids(limit=1000)

                if package_ids:
                    stats = normalizer.process_items(
                        item_ids=package_ids,
                        batch_size=50,
                        max_concurrent=10,
                    )
                else:
                    stats = {"message": "No package IDs to process"}

                context.log.info(
                    f"GovInfo {self.data_type_name} normalization completed: {stats}"
                )

                return {
                    "status": "success",
                    "stats": stats,
                    "data_type": self.data_type_name,
                    "source_system": "govinfo",
                    "processed_package_ids": package_ids,
                }

            except Exception as e:
                context.log.error(
                    f"GovInfo {self.data_type_name} normalization failed: {e}"
                )
                raise

        return govinfo_normalize_data

    def create_clean_asset(self, **kwargs):
        """Create GovInfo clean asset."""

        @asset(
            name=f"{self.data_type_name}_govinfo_cleaned",
            description=f"GovInfo {self.data_type_name} cleaned data",
            ins={f"{self.data_type_name}_govinfo_normalized": AssetIn()},
            metadata={
                "source_system": "govinfo",
                "data_type": self.data_type_name,
                "source_schema": "bicam_staging_govinfo",
                "target_schema": "bicam_govinfo",
            },
        )
        async def govinfo_clean_data(
            context, processing_resource: ProcessingResource, **upstream_assets
        ) -> dict[str, Any]:
            """Clean GovInfo staging data to production schema."""
            try:
                cleaner_class = self.get_cleaner_class()

                # Create cleaner
                cleaner = cleaner_class(
                    db_pool=processing_resource.get_database_pool(),
                    data_type_name=self.data_type_name,
                    checkpoint_manager=processing_resource.get_checkpoint_manager(),
                    run_manager=processing_resource.get_run_manager(),
                )

                # Get processed package IDs from normalization step
                normalize_result = upstream_assets.get(
                    f"{self.data_type_name}_govinfo_normalized", {}
                )
                package_ids = normalize_result.get("processed_package_ids", [])

                if package_ids:
                    stats = cleaner.process_items(
                        item_ids=package_ids,
                        batch_size=100,
                        max_concurrent=10,
                    )
                else:
                    # Process all available data
                    stats = cleaner.process_items(
                        item_ids=None,
                        batch_size=100,
                        max_concurrent=10,
                    )

                context.log.info(
                    f"GovInfo {self.data_type_name} cleaning completed: {stats}"
                )

                return {
                    "status": "success",
                    "stats": stats,
                    "data_type": self.data_type_name,
                    "source_system": "govinfo",
                    "processed_package_ids": package_ids,
                }

            except Exception as e:
                context.log.error(f"GovInfo {self.data_type_name} cleaning failed: {e}")
                raise

        return govinfo_clean_data

    def create_complete_pipeline_asset(self, **kwargs):
        """Create complete pipeline asset for all 4 GovInfo phases plus normalization and cleaning."""

        @multi_asset(
            outs={
                f"{self.data_type_name}_govinfo_fetched": AssetOut(
                    description=f"GovInfo {self.data_type_name} raw data (4-phase processing)"
                ),
                f"{self.data_type_name}_govinfo_normalized": AssetOut(
                    description=f"GovInfo {self.data_type_name} normalized data"
                ),
                f"{self.data_type_name}_govinfo_cleaned": AssetOut(
                    description=f"GovInfo {self.data_type_name} cleaned data"
                ),
            },
            description=f"Complete GovInfo {self.data_type_name} pipeline (fetch → normalize → clean)",
            metadata={
                "source_system": "govinfo",
                "data_type": self.data_type_name,
                "pipeline_phases": ["fetch", "normalize", "clean"],
                "processing_pattern": "4-phase-govinfo",
            },
        )
        async def complete_govinfo_pipeline(
            context, processing_resource: ProcessingResource
        ):
            """Complete GovInfo pipeline for this data type."""
            pipeline_stats = {
                "fetch": None,
                "normalize": None,
                "clean": None,
                "overall_status": "running",
            }

            try:
                # Phase 1: Fetch (4-phase GovInfo processing)
                context.log.info(f"Starting GovInfo {self.data_type_name} fetch phase")
                fetcher_class = self.get_fetcher_class()
                api_client = processing_resource.get_govinfo_api_client()

                fetcher = fetcher_class(
                    client=api_client,
                    db_pool=processing_resource.get_database_pool(),
                    data_type_name=self.data_type_name,
                    checkpoint_manager=processing_resource.get_checkpoint_manager(),
                    run_manager=processing_resource.get_run_manager(),
                )
                fetcher.set_processing_resource(processing_resource)

                fetch_stats = fetcher.process_items(
                    enable_parallelization=True,
                    max_concurrent=10,
                    batch_size=50,
                )
                pipeline_stats["fetch"] = fetch_stats

                # Phase 2: Normalize
                context.log.info(
                    f"Starting GovInfo {self.data_type_name} normalization phase"
                )
                normalizer_class = self.get_database_normalizer_class()
                normalizer = normalizer_class(
                    db_pool=processing_resource.get_database_pool(),
                    data_type_name=self.data_type_name,
                    checkpoint_manager=processing_resource.get_checkpoint_manager(),
                    run_manager=processing_resource.get_run_manager(),
                )

                # Get package IDs from fetch results
                package_ids = fetch_stats.get("package_ids", [])
                if not package_ids:
                    package_ids = normalizer._get_recent_package_ids(limit=1000)

                normalize_stats = normalizer.process_items(
                    item_ids=package_ids,
                    batch_size=50,
                    max_concurrent=10,
                )
                pipeline_stats["normalize"] = normalize_stats

                # Phase 3: Clean
                context.log.info(
                    f"Starting GovInfo {self.data_type_name} cleaning phase"
                )
                cleaner_class = self.get_cleaner_class()
                cleaner = cleaner_class(
                    db_pool=processing_resource.get_database_pool(),
                    data_type_name=self.data_type_name,
                    checkpoint_manager=processing_resource.get_checkpoint_manager(),
                    run_manager=processing_resource.get_run_manager(),
                )

                clean_stats = cleaner.process_items(
                    item_ids=package_ids,
                    batch_size=100,
                    max_concurrent=10,
                )
                pipeline_stats["clean"] = clean_stats
                pipeline_stats["overall_status"] = "success"

                context.log.info(
                    f"Complete GovInfo {self.data_type_name} pipeline completed: {pipeline_stats}"
                )

                return {
                    f"{self.data_type_name}_govinfo_fetched": {
                        "status": "completed",
                        "stats": fetch_stats,
                    },
                    f"{self.data_type_name}_govinfo_normalized": {
                        "status": "completed",
                        "stats": normalize_stats,
                    },
                    f"{self.data_type_name}_govinfo_cleaned": {
                        "status": "completed",
                        "stats": clean_stats,
                    },
                }

            except Exception as e:
                pipeline_stats["overall_status"] = "failed"
                context.log.error(
                    f"Complete GovInfo {self.data_type_name} pipeline failed: {e}"
                )
                raise

        return complete_govinfo_pipeline

    def create_data_quality_report_asset(self, **kwargs):
        """Create comprehensive data quality report asset for GovInfo data."""

        @asset(
            name=f"{self.data_type_name}_govinfo_quality_report",
            description=f"Data quality report for GovInfo {self.data_type_name}",
            ins={f"{self.data_type_name}_govinfo_cleaned": AssetIn()},
            metadata={
                "source_system": "govinfo",
                "data_type": self.data_type_name,
                "report_type": "quality_assessment",
            },
        )
        async def govinfo_quality_report(
            context, processing_resource: ProcessingResource, **upstream_assets
        ):
            """Generate comprehensive quality report for GovInfo data."""
            try:
                db_pool = processing_resource.get_database_pool()

                # Initialize quality metrics
                quality_metrics = {
                    "overall_score": 0.0,
                    "phase_scores": {
                        "collection_data": 0.0,
                        "package_data": 0.0,
                        "granules_data": 0.0,
                        "granule_data": 0.0,
                    },
                    "completeness": {
                        "collection_records": 0,
                        "package_records": 0,
                        "granule_records": 0,
                        "missing_links": 0,
                    },
                    "data_integrity": {
                        "valid_package_ids": 0,
                        "valid_granule_ids": 0,
                        "broken_relationships": 0,
                    },
                    "issues": [],
                    "recommendations": [],
                }

                # Perform quality analysis
                quality_metrics = await _analyze_govinfo_data_quality(
                    db_pool, self.data_type_name, quality_metrics
                )

                # Calculate overall score
                phase_scores = list(quality_metrics["phase_scores"].values())
                quality_metrics["overall_score"] = (
                    sum(phase_scores) / len(phase_scores) if phase_scores else 0.0
                )

                # Add recommendations based on issues
                if quality_metrics["completeness"]["missing_links"] > 0:
                    quality_metrics["recommendations"].append(
                        "Re-run fetcher to retrieve missing package/granule links"
                    )

                if quality_metrics["data_integrity"]["broken_relationships"] > 0:
                    quality_metrics["recommendations"].append(
                        "Review and fix broken parent-child relationships in GovInfo data"
                    )

                context.log.info(
                    f"GovInfo {self.data_type_name} quality report completed: score {quality_metrics['overall_score']:.1f}"
                )

                return quality_metrics

            except Exception as e:
                context.log.error(
                    f"GovInfo {self.data_type_name} quality report failed: {e}"
                )
                return {
                    "overall_score": 0.0,
                    "error": str(e),
                    "issues": ["Quality analysis failed"],
                }

        return govinfo_quality_report

    def create_incremental_fetch_asset(self, **kwargs):
        """Create incremental fetch asset that only processes new/updated data."""

        @asset(
            name=f"{self.data_type_name}_govinfo_incremental_fetch",
            description=f"Incremental GovInfo {self.data_type_name} fetch (new data only)",
            metadata={
                "source_system": "govinfo",
                "data_type": self.data_type_name,
                "fetch_type": "incremental",
                "schema": "bicam_raw_govinfo",
            },
        )
        def govinfo_incremental_fetch(
            context, processing_resource: ProcessingResource
        ) -> dict[str, Any]:
            """Fetch only new/updated GovInfo data since last run."""
            try:
                fetcher_class = self.get_fetcher_class()
                api_client = processing_resource.get_govinfo_api_client()

                fetcher = fetcher_class(
                    client=api_client,
                    db_pool=processing_resource.get_database_pool(),
                    data_type_name=self.data_type_name,
                    checkpoint_manager=processing_resource.get_checkpoint_manager(),
                    run_manager=processing_resource.get_run_manager(),
                )
                fetcher.set_processing_resource(processing_resource)

                # Use incremental fetching - no date range specified so it will
                # automatically use last processed date
                stats = fetcher.process_items(
                    enable_parallelization=True,
                    max_concurrent=5,  # Lower concurrency for incremental
                    batch_size=25,  # Smaller batches for incremental
                )

                context.log.info(
                    f"GovInfo {self.data_type_name} incremental fetch completed: {stats}"
                )

                return {
                    "status": "success",
                    "stats": stats,
                    "data_type": self.data_type_name,
                    "source_system": "govinfo",
                    "fetch_type": "incremental",
                    "new_records": stats.get("successful", 0),
                }

            except Exception as e:
                context.log.error(
                    f"GovInfo {self.data_type_name} incremental fetch failed: {e}"
                )
                raise

        return govinfo_incremental_fetch


async def _analyze_govinfo_data_quality(
    db_pool, data_type_name: str, quality_metrics: dict[str, Any]
) -> dict[str, Any]:
    """Analyze GovInfo data quality across all 4 phases."""

    async with db_pool.acquire() as conn:
        # Analyze collection data
        try:
            collection_count = await conn.fetchval(
                f"SELECT COUNT(*) FROM bicam_govinfo.{data_type_name}_collection"
            )
            quality_metrics["completeness"]["collection_records"] = collection_count
            quality_metrics["phase_scores"]["collection_data"] = min(
                100.0, collection_count / 10
            )  # Scale appropriately
        except Exception:
            quality_metrics["issues"].append(
                "Collection data table not found or inaccessible"
            )

        # Analyze package data
        try:
            package_count = await conn.fetchval(
                f"SELECT COUNT(*) FROM bicam_govinfo.{data_type_name}_package"
            )
            quality_metrics["completeness"]["package_records"] = package_count
            quality_metrics["phase_scores"]["package_data"] = min(
                100.0, package_count / 10
            )  # Scale appropriately
        except Exception:
            quality_metrics["issues"].append(
                "Package data table not found or inaccessible"
            )

        # Analyze granules data (if applicable)
        try:
            granule_count = await conn.fetchval(
                f"SELECT COUNT(*) FROM bicam_govinfo.{data_type_name}_granules"
            )
            quality_metrics["completeness"]["granule_records"] = granule_count
            quality_metrics["phase_scores"]["granules_data"] = min(
                100.0, granule_count / 5
            )  # Scale appropriately
        except Exception:
            # Granules might not exist for all data types
            quality_metrics["phase_scores"]["granules_data"] = (
                100.0  # No penalty if not applicable
            )

        # Analyze granule data
        try:
            granule_data_count = await conn.fetchval(
                f"SELECT COUNT(*) FROM bicam_govinfo.{data_type_name}_granule_data"
            )
            quality_metrics["phase_scores"]["granule_data"] = min(
                100.0, granule_data_count / 5
            )  # Scale appropriately
        except Exception:
            quality_metrics["phase_scores"]["granule_data"] = (
                100.0  # No penalty if not applicable
            )

    return quality_metrics
