"""
Congressional-specific base specialized assets.

This module provides Congressional-specific implementations including:
- Congressional-specific asset configuration
- Congressional schema references
- Congressional progress tracking
"""

import logging
from typing import Any

import asyncpg
from dagster import AssetExecutionContext, asset

from ...abstract import AbstractSpecializedAssets

logger = logging.getLogger(__name__)


class CongressionalBaseSpecializedAssets(AbstractSpecializedAssets):
    """
    Congressional-specific base assets.

    Provides Congressional-specific asset configuration and schema references.
    """

    def __init__(self, data_type_name: str):
        super().__init__(data_type_name, source_system="congressional")

    # =============================================================================
    # ASSET FACTORY METHODS
    # =============================================================================

    def create_complete_pipeline_asset(
        self,
        asset_name: str | None = None,
        group_name: str | None = None,
        **asset_kwargs,
    ):
        """
        Create a complete pipeline asset that orchestrates all processing phases.

        Args:
            asset_name: Name for the asset (defaults to "{data_type}_complete_pipeline")
            group_name: Asset group name (defaults to "{data_type}_pipeline")
            **asset_kwargs: Additional asset configuration

        Returns:
            Dagster asset function
        """
        asset_name = asset_name or f"{self.data_type_name}_complete_pipeline"
        group_name = group_name or f"{self.data_type_name}_pipeline"

        @asset(
            name=asset_name,
            group_name=group_name,
            compute_kind="summary",
            **asset_kwargs,
        )
        async def complete_pipeline_asset(
            context: AssetExecutionContext,
            processing_resource,  # Injected by framework
        ) -> dict[str, Any]:
            """
            Complete Congressional data processing pipeline orchestrating all phases.

            Phases:
            1. Fetching: Retrieve data from Congressional APIs
            2. Normalization: Flatten and structure raw data
            3. Cleaning: Transform and clean data for production

            Returns:
                Comprehensive processing statistics and status
            """
            context.log.info(
                f"Starting complete {self.data_type_name} Congressional pipeline"
            )

            # Get processing dependencies
            db_pool = processing_resource.db_pool
            checkpoint_manager = processing_resource.checkpoint_manager
            run_manager = processing_resource.run_manager
            api_client = getattr(processing_resource, "api_client", None)

            pipeline_stats = {
                "phase": "initializing",
                "fetching": {"status": "pending"},
                "normalization": {"status": "pending"},
                "cleaning": {"status": "pending"},
                "overall_status": "running",
                "total_processed": 0,
                "total_failed": 0,
                "source_system": "congressional",
            }

            try:
                # Phase 1: Fetching
                context.log.info("Phase 1: Starting Congressional data fetching")
                pipeline_stats["phase"] = "fetching"

                fetcher_class = self.get_fetcher_class()
                fetcher = fetcher_class(
                    client=api_client,
                    db_pool=db_pool,
                    data_type_name=self.data_type_name,
                    checkpoint_manager=checkpoint_manager,
                    run_manager=run_manager,
                )

                fetching_result = await fetcher.process_items(
                    enable_parallelization=True,
                    max_concurrent=10,
                    redistribute_idle_sessions=True,
                )

                pipeline_stats["fetching"] = {
                    "status": "completed"
                    if fetching_result.get("error") is None
                    else "failed",
                    "processed": fetching_result.get("successful", 0),
                    "failed": fetching_result.get("errors", 0),
                    "duration": fetching_result.get("duration", 0),
                }

                if fetching_result.get("error"):
                    raise ValueError(f"Fetching failed: {fetching_result['error']}")

                # Get item IDs for next phases
                item_ids = await self._get_item_ids_from_raw_data(db_pool)
                context.log.info(f"Found {len(item_ids)} items to process")

                # Phase 2: Normalization
                context.log.info("Phase 2: Starting Congressional data normalization")
                pipeline_stats["phase"] = "normalization"

                normalizer_class = self.get_database_normalizer_class()
                normalizer = normalizer_class(
                    db_pool=db_pool,
                    checkpoint_manager=checkpoint_manager,
                    run_manager=run_manager,
                    data_type_name=self.data_type_name,
                    # Congressional-specific schemas
                    target_schema="bicam_staging_congressional",
                    source_schema="bicam_raw_congressional",
                )

                normalization_result = await normalizer.process_items(
                    item_ids=item_ids,
                    batch_size=50,
                    max_concurrent=10,
                )

                pipeline_stats["normalization"] = {
                    "status": "completed"
                    if normalization_result.get("error") is None
                    else "failed",
                    "main_records": normalization_result.get(
                        "main_records_processed", 0
                    ),
                    "related_records": normalization_result.get(
                        "related_records_processed", 0
                    ),
                    "failed": normalization_result.get("errors", 0),
                    "duration": normalization_result.get("duration", 0),
                }

                if normalization_result.get("error"):
                    raise ValueError(
                        f"Normalization failed: {normalization_result['error']}"
                    )

                # Phase 3: Cleaning
                context.log.info("Phase 3: Starting Congressional data cleaning")
                pipeline_stats["phase"] = "cleaning"

                cleaner_class = self.get_cleaner_class()
                cleaner = cleaner_class(
                    db_pool=db_pool,
                    checkpoint_manager=checkpoint_manager,
                    run_manager=run_manager,
                    data_type_name=self.data_type_name,
                    # Congressional-specific schemas
                    staging_schema="bicam_staging_congressional",
                    production_schema="bicam_congressional",
                )

                cleaning_result = await cleaner.process_items(
                    chunk_size=1000,
                    max_workers=4,
                )

                pipeline_stats["cleaning"] = {
                    "status": "completed"
                    if cleaning_result.get("error") is None
                    else "failed",
                    "data_types_processed": cleaning_result.get(
                        "data_types_processed", 0
                    ),
                    "total_records": cleaning_result.get("total_records_processed", 0),
                    "failed": cleaning_result.get("total_errors", 0),
                    "duration": cleaning_result.get("duration", 0),
                }

                if cleaning_result.get("error"):
                    raise ValueError(f"Cleaning failed: {cleaning_result['error']}")

                # Calculate overall statistics
                pipeline_stats["total_processed"] = (
                    pipeline_stats["fetching"]["processed"]
                    + pipeline_stats["normalization"]["main_records"]
                    + pipeline_stats["cleaning"]["total_records"]
                )
                pipeline_stats["total_failed"] = (
                    pipeline_stats["fetching"]["failed"]
                    + pipeline_stats["normalization"]["failed"]
                    + pipeline_stats["cleaning"]["failed"]
                )
                pipeline_stats["overall_status"] = "completed"
                pipeline_stats["phase"] = "completed"

                context.log.info(
                    f"Congressional pipeline completed successfully: {pipeline_stats}"
                )

            except Exception as e:
                context.log.error(
                    f"Congressional pipeline failed at {pipeline_stats['phase']}: {e}"
                )
                pipeline_stats["overall_status"] = "failed"
                pipeline_stats["error"] = str(e)
                raise

            return pipeline_stats

        return complete_pipeline_asset

    def create_data_quality_report_asset(
        self,
        asset_name: str | None = None,
        group_name: str | None = None,
        **asset_kwargs,
    ):
        """
        Create a data quality monitoring asset for Congressional data.

        Args:
            asset_name: Name for the asset (defaults to "{data_type}_quality_report")
            group_name: Asset group name (defaults to "{data_type}_monitoring")
            **asset_kwargs: Additional asset configuration

        Returns:
            Dagster asset function
        """
        asset_name = asset_name or f"{self.data_type_name}_quality_report"
        group_name = group_name or f"{self.data_type_name}_monitoring"

        @asset(
            name=asset_name,
            group_name=group_name,
            compute_kind="analysis",
            **asset_kwargs,
        )
        async def data_quality_report_asset(
            context: AssetExecutionContext,
            processing_resource,  # Injected by framework
        ) -> dict[str, Any]:
            """
            Comprehensive data quality analysis for Congressional data.

            Analyzes data completeness, consistency, and quality across
            all processing phases (raw, staging, production).

            Returns:
                Data quality metrics and recommendations
            """
            context.log.info(
                f"Generating Congressional data quality report for {self.data_type_name}"
            )

            db_pool = processing_resource.db_pool

            quality_report = {
                "data_type": self.data_type_name,
                "source_system": "congressional",
                "generated_at": context.run.run_id,
                "schemas": {
                    "raw": "bicam_raw_congressional",
                    "staging": "bicam_staging_congressional",
                    "production": "bicam_congressional",
                },
                "metrics": {},
                "analysis": {},
                "recommendations": [],
                "status": "analyzing",
            }

            try:
                # Get record counts across schemas
                raw_count = await self._get_raw_record_count(db_pool)
                staging_count = await self._get_staging_record_count(db_pool)
                production_count = await self._get_production_record_count(db_pool)

                quality_report["metrics"] = {
                    "raw_records": raw_count,
                    "staging_records": staging_count,
                    "production_records": production_count,
                    "raw_to_staging_ratio": staging_count / raw_count
                    if raw_count > 0
                    else 0,
                    "staging_to_production_ratio": production_count / staging_count
                    if staging_count > 0
                    else 0,
                    "overall_pipeline_efficiency": production_count / raw_count
                    if raw_count > 0
                    else 0,
                }

                # Determine pipeline status
                pipeline_status = self._determine_pipeline_status(
                    raw_count, staging_count, production_count
                )
                quality_report["analysis"]["pipeline_status"] = pipeline_status

                # Generate recommendations
                recommendations = self._generate_recommendations(
                    raw_count, staging_count, production_count
                )
                quality_report["recommendations"] = recommendations

                # Detailed data quality analysis
                data_quality_analysis = await self._analyze_congressional_data_quality(
                    db_pool
                )
                quality_report["analysis"]["data_quality"] = data_quality_analysis

                quality_report["status"] = "completed"
                context.log.info(
                    f"Quality report completed: {quality_report['metrics']}"
                )

            except Exception as e:
                context.log.error(f"Quality report generation failed: {e}")
                quality_report["status"] = "failed"
                quality_report["error"] = str(e)
                raise

            return quality_report

        return data_quality_report_asset

    def get_all_assets(self) -> list:
        """
        Get all assets for this Congressional data type.

        Returns:
            List of all asset functions
        """
        assets = []

        # Complete pipeline asset
        pipeline_asset = self.create_complete_pipeline_asset()
        assets.append(pipeline_asset)

        # Data quality report asset
        quality_asset = self.create_data_quality_report_asset()
        assets.append(quality_asset)

        return assets

    # =============================================================================
    # CONGRESSIONAL-SPECIFIC HELPER METHODS
    # =============================================================================

    async def _get_item_ids_from_raw_data(self, db_pool: asyncpg.Pool) -> list[str]:
        """Get item IDs from Congressional raw data."""
        query = f"""
            SELECT DISTINCT extracted_item_id
            FROM bicam_raw_congressional.{self.data_type_name}_raw
            WHERE extracted_item_id IS NOT NULL
            ORDER BY extracted_item_id
        """

        async with db_pool.acquire() as conn:
            rows = await conn.fetch(query)
            return [row["extracted_item_id"] for row in rows]

    async def _get_raw_record_count(self, db_pool: asyncpg.Pool) -> int:
        """Get count of Congressional raw records."""
        query = (
            f"SELECT COUNT(*) FROM bicam_raw_congressional.{self.data_type_name}_raw"
        )

        try:
            async with db_pool.acquire() as conn:
                return await conn.fetchval(query)
        except Exception:
            return 0

    async def _get_staging_record_count(self, db_pool: asyncpg.Pool) -> int:
        """Get count of Congressional staging records."""
        query = (
            f"SELECT COUNT(*) FROM bicam_staging_congressional.{self.data_type_name}"
        )

        try:
            async with db_pool.acquire() as conn:
                return await conn.fetchval(query)
        except Exception:
            return 0

    async def _get_production_record_count(self, db_pool: asyncpg.Pool) -> int:
        """Get count of Congressional production records."""
        query = f"SELECT COUNT(*) FROM bicam_congressional.{self.data_type_name}"

        try:
            async with db_pool.acquire() as conn:
                return await conn.fetchval(query)
        except Exception:
            return 0

    def _determine_pipeline_status(
        self, raw_count: int, staging_count: int, production_count: int
    ) -> str:
        """Determine overall Congressional pipeline status."""
        if raw_count == 0:
            return "no_data"
        elif staging_count == 0:
            return "fetching_only"
        elif production_count == 0:
            return "normalization_only"
        elif production_count < staging_count * 0.9:
            return "cleaning_issues"
        else:
            return "healthy"

    def _generate_recommendations(
        self, raw_count: int, staging_count: int, production_count: int
    ) -> list[str]:
        """Generate Congressional-specific recommendations."""
        recommendations = []

        if raw_count == 0:
            recommendations.append(
                "No raw Congressional data found. Run fetching process."
            )
        elif staging_count < raw_count * 0.9:
            recommendations.append("Low staging count indicates normalization issues.")
        elif production_count < staging_count * 0.9:
            recommendations.append("Low production count indicates cleaning issues.")

        if raw_count > 0 and staging_count > 0 and production_count > 0:
            efficiency = production_count / raw_count
            if efficiency < 0.8:
                recommendations.append(
                    f"Overall pipeline efficiency is low ({efficiency:.1%}). Review data processing."
                )

        if not recommendations:
            recommendations.append("Congressional data pipeline is operating normally.")

        return recommendations

    async def _analyze_congressional_data_quality(
        self, db_pool: asyncpg.Pool
    ) -> dict[str, Any]:
        """Analyze Congressional-specific data quality metrics."""
        analysis = {
            "schema_analysis": {},
            "data_completeness": {},
            "data_consistency": {},
        }

        try:
            # Analyze each Congressional schema
            for schema in [
                "bicam_raw_congressional",
                "bicam_staging_congressional",
                "bicam_congressional",
            ]:
                schema_analysis = await self._analyze_congressional_schema_quality(
                    db_pool, schema
                )
                analysis["schema_analysis"][schema] = schema_analysis

            # Congressional-specific completeness checks
            analysis[
                "data_completeness"
            ] = await self._analyze_congressional_completeness(db_pool)

            # Congressional-specific consistency checks
            analysis[
                "data_consistency"
            ] = await self._analyze_congressional_consistency(db_pool)

        except Exception as e:
            logger.error(f"Error in Congressional data quality analysis: {e}")
            analysis["error"] = str(e)

        return analysis

    async def _analyze_congressional_schema_quality(
        self, db_pool: asyncpg.Pool, schema: str
    ) -> dict[str, Any]:
        """Analyze quality metrics for a Congressional schema."""
        analysis = {
            "tables_found": 0,
            "total_records": 0,
            "table_details": {},
        }

        try:
            async with db_pool.acquire() as conn:
                # Get tables for this data type in schema
                tables_query = """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = $1
                    AND table_name LIKE $2
                """

                tables = await conn.fetch(
                    tables_query, schema, f"{self.data_type_name}%"
                )
                analysis["tables_found"] = len(tables)

                for table in tables:
                    table_name = table["table_name"]
                    table_analysis = await self._analyze_congressional_table_quality(
                        db_pool, schema, table_name
                    )
                    analysis["table_details"][table_name] = table_analysis
                    analysis["total_records"] += table_analysis.get("record_count", 0)

        except Exception as e:
            logger.warning(f"Error analyzing Congressional schema {schema}: {e}")
            analysis["error"] = str(e)

        return analysis

    async def _analyze_congressional_table_quality(
        self, db_pool: asyncpg.Pool, schema: str, table_name: str
    ) -> dict[str, Any]:
        """Analyze quality metrics for a Congressional table."""
        analysis = {
            "record_count": 0,
            "null_percentages": {},
            "data_types": {},
        }

        try:
            async with db_pool.acquire() as conn:
                # Get record count
                count_query = f"SELECT COUNT(*) FROM {schema}.{table_name}"
                record_count = await conn.fetchval(count_query)
                analysis["record_count"] = record_count

                if record_count > 0:
                    # Get column information
                    columns_query = """
                        SELECT column_name, data_type, is_nullable
                        FROM information_schema.columns
                        WHERE table_schema = $1 AND table_name = $2
                    """
                    columns = await conn.fetch(columns_query, schema, table_name)

                    # Analyze null percentages for each column
                    for column in columns:
                        col_name = column["column_name"]
                        null_query = f"""
                            SELECT
                                COUNT(*) as total_count,
                                COUNT({col_name}) as non_null_count
                            FROM {schema}.{table_name}
                        """

                        result = await conn.fetchrow(null_query)
                        total = result["total_count"]
                        non_null = result["non_null_count"]
                        null_percentage = (
                            ((total - non_null) / total * 100) if total > 0 else 0
                        )

                        analysis["null_percentages"][col_name] = null_percentage
                        analysis["data_types"][col_name] = column["data_type"]

        except Exception as e:
            logger.warning(
                f"Error analyzing Congressional table {schema}.{table_name}: {e}"
            )
            analysis["error"] = str(e)

        return analysis

    async def _analyze_congressional_completeness(
        self, db_pool: asyncpg.Pool
    ) -> dict[str, Any]:
        """Analyze Congressional data completeness."""
        # This can be customized for Congressional-specific completeness checks
        return {
            "status": "Congressional completeness analysis not yet implemented",
            "checks": [],
        }

    async def _analyze_congressional_consistency(
        self, db_pool: asyncpg.Pool
    ) -> dict[str, Any]:
        """Analyze Congressional data consistency."""
        # This can be customized for Congressional-specific consistency checks
        return {
            "status": "Congressional consistency analysis not yet implemented",
            "checks": [],
        }
