"""
Dagster Definitions for Congressional Data Processing

This module demonstrates best practices for organizing Dagster pipelines
with many data types and many tables using a generalized, factory-based approach.

Architecture Overview:
- Generalized asset factories create assets for any data type
- Asset groups organize assets by data type and processing stage
- Jobs target specific asset groups for focused processing
- Shared resources coordinate API keys and infrastructure
- Configuration-driven approach minimizes code duplication

Organization:
- Raw data assets: {data_type}_raw_data
- Staging assets: {data_type}_staging_data
- Production assets: {data_type}_production_data
- Table-level assets: {table_name}_staging, {table_name}
- Asset groups: {data_type}_pipeline, {data_type}_tables_staging, {data_type}_tables_production

Jobs:
- Full pipeline jobs: {data_type}_full_pipeline
- Stage-specific jobs: {data_type}_raw_only, {data_type}_staging_only, etc.
- Table-specific jobs: {data_type}_staging_tables, {data_type}_production_tables
- Multi-data-type jobs: all_data_types_raw, all_data_types_staging, etc.
"""

import logging

from dagster import (
    AssetSelection,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
)

from ..libs.data_type_router import get_global_registry
from .generalized_assets import (
    create_all_data_type_assets,
)
from .shared_resources import DataTypeSpecificResource, ProcessingResource

logger = logging.getLogger(__name__)

# Get available data types from the registry
registry = get_global_registry()
AVAILABLE_DATA_TYPES = registry.list_data_types()

# Configuration: Define data type priorities (subset of available types)
# Note: Only data types that are actually registered will be included

# High-priority data types (get more API keys, run more frequently)
# Only include types that are actually registered
HIGH_PRIORITY_DATA_TYPES = [
    dt for dt in ["bills", "amendments", "members"] if dt in AVAILABLE_DATA_TYPES
]

# Data types that can run concurrently without conflicts
# Filter to only include registered data types
CONCURRENT_SAFE_GROUPS = [
    [
        dt for dt in ["bills", "amendments"] if dt in AVAILABLE_DATA_TYPES
    ],  # Legislative content
    [
        dt for dt in ["members", "committees"] if dt in AVAILABLE_DATA_TYPES
    ],  # People and organizations
    [
        dt for dt in ["nominations", "treaties"] if dt in AVAILABLE_DATA_TYPES
    ],  # Executive branch items
    [
        dt
        for dt in ["hearings", "committee_reports", "committee_prints"]
        if dt in AVAILABLE_DATA_TYPES
    ],  # Committee materials
]
# Remove empty groups
CONCURRENT_SAFE_GROUPS = [group for group in CONCURRENT_SAFE_GROUPS if group]


def create_all_assets():
    """Create all assets for all data types, combining generalized and specialized assets."""
    all_assets = create_all_data_type_assets(AVAILABLE_DATA_TYPES)

    logger.info(f"Created {len(all_assets)} total assets:")

    # Count assets by type for reporting
    specialized_count = 0
    core_asset_count = 0
    table_asset_count = 0

    for data_type in AVAILABLE_DATA_TYPES:
        # Count specialized assets for this data type
        specialized_assets = get_specialized_assets(data_type)
        specialized_count += len(specialized_assets)

        # Count core assets (3 per data type: raw, staging, production)
        core_asset_count += 3

        # Count table assets (2 per table: staging + production)
        registry = get_global_registry()
        table_names = registry.get_table_names(data_type)
        table_asset_count += len(table_names) * 2

    logger.info(
        f"  - {core_asset_count} core pipeline assets (raw, staging, production)"
    )
    logger.info(f"  - {table_asset_count} table-level assets")
    logger.info(
        f"  - {specialized_count} specialized assets (monitoring, analysis, etc.)"
    )

    return all_assets


def get_specialized_assets(data_type: str) -> list:
    """Get specialized assets for a data type if they exist."""
    from ..libs.data_type_router import (
        get_specialized_assets as router_get_specialized_assets,
    )

    return router_get_specialized_assets(data_type)


def create_pipeline_jobs():
    """Create comprehensive job definitions for all data types."""
    jobs = []

    # 1. Individual data type jobs (full pipeline)
    for data_type in AVAILABLE_DATA_TYPES:
        # Full pipeline job
        full_job = define_asset_job(
            name=f"{data_type}_full_pipeline",
            selection=AssetSelection.groups(f"{data_type}_pipeline"),
            description=f"Complete pipeline for {data_type}: raw → staging → production",
        )
        jobs.append(full_job)

        # Stage-specific jobs
        raw_job = define_asset_job(
            name=f"{data_type}_raw_only",
            selection=AssetSelection.assets(f"{data_type}_raw_data"),
            description=f"Raw data fetching only for {data_type}",
        )
        jobs.append(raw_job)

        staging_job = define_asset_job(
            name=f"{data_type}_staging_only",
            selection=AssetSelection.assets(f"{data_type}_staging_data"),
            description=f"Staging normalization only for {data_type}",
        )
        jobs.append(staging_job)

        production_job = define_asset_job(
            name=f"{data_type}_production_only",
            selection=AssetSelection.assets(f"{data_type}_production_data"),
            description=f"Production cleaning only for {data_type}",
        )
        jobs.append(production_job)

        # Table-level jobs
        staging_tables_job = define_asset_job(
            name=f"{data_type}_staging_tables",
            selection=AssetSelection.groups(f"{data_type}_tables_staging"),
            description=f"All staging tables for {data_type}",
        )
        jobs.append(staging_tables_job)

        production_tables_job = define_asset_job(
            name=f"{data_type}_production_tables",
            selection=AssetSelection.groups(f"{data_type}_tables_production"),
            description=f"All production tables for {data_type}",
        )
        jobs.append(production_tables_job)

    # 2. Multi-data-type jobs (by stage)
    all_raw_job = define_asset_job(
        name="all_data_types_raw",
        selection=AssetSelection.assets(
            *[f"{dt}_raw_data" for dt in AVAILABLE_DATA_TYPES]
        ),
        description="Raw data fetching for all data types",
    )
    jobs.append(all_raw_job)

    all_staging_job = define_asset_job(
        name="all_data_types_staging",
        selection=AssetSelection.assets(
            *[f"{dt}_staging_data" for dt in AVAILABLE_DATA_TYPES]
        ),
        description="Staging normalization for all data types",
    )
    jobs.append(all_staging_job)

    all_production_job = define_asset_job(
        name="all_data_types_production",
        selection=AssetSelection.assets(
            *[f"{dt}_production_data" for dt in AVAILABLE_DATA_TYPES]
        ),
        description="Production cleaning for all data types",
    )
    jobs.append(all_production_job)

    # 3. Priority-based jobs
    high_priority_job = define_asset_job(
        name="high_priority_data_types",
        selection=AssetSelection.groups(
            *[f"{dt}_pipeline" for dt in HIGH_PRIORITY_DATA_TYPES]
        ),
        description="High-priority data types full pipeline",
    )
    jobs.append(high_priority_job)

    # 4. Concurrent group jobs (safe to run together)
    for i, group in enumerate(CONCURRENT_SAFE_GROUPS):
        group_job = define_asset_job(
            name=f"concurrent_group_{i + 1}",
            selection=AssetSelection.groups(*[f"{dt}_pipeline" for dt in group]),
            description=f"Concurrent processing group {i + 1}: {', '.join(group)}",
        )
        jobs.append(group_job)

    return jobs


def create_schedules():
    """Create schedule definitions for regular processing."""
    schedules = []

    # Daily schedules for high-priority data types
    for data_type in HIGH_PRIORITY_DATA_TYPES:
        daily_schedule = ScheduleDefinition(
            name=f"{data_type}_daily",
            job_name=f"{data_type}_full_pipeline",
            cron_schedule="0 2 * * *",  # 2 AM daily
            description=f"Daily processing for {data_type}",
        )
        schedules.append(daily_schedule)

    # Weekly schedules for other data types
    other_data_types = [
        dt for dt in AVAILABLE_DATA_TYPES if dt not in HIGH_PRIORITY_DATA_TYPES
    ]
    for i, data_type in enumerate(other_data_types):
        # Spread across different days of the week
        day_of_week = i % 7
        weekly_schedule = ScheduleDefinition(
            name=f"{data_type}_weekly",
            job_name=f"{data_type}_full_pipeline",
            cron_schedule=f"0 3 * * {day_of_week}",  # 3 AM on assigned day
            description=f"Weekly processing for {data_type}",
        )
        schedules.append(weekly_schedule)

    # Raw data refresh schedule (more frequent)
    raw_refresh_schedule = ScheduleDefinition(
        name="raw_data_refresh",
        job_name="high_priority_data_types",
        cron_schedule="0 */6 * * *",  # Every 6 hours
        description="Frequent raw data refresh for high-priority types",
    )
    schedules.append(raw_refresh_schedule)

    return schedules


def create_resources():
    """Create resource definitions."""

    # Main shared resource
    processing_resource = ProcessingResource()

    # Data-type-specific resources (if needed)
    bills_resource = DataTypeSpecificResource(
        data_type="bills",
        batch_size_override=200,  # Bills can handle larger batches
        num_api_keys=3,  # Bills get more API keys
    )

    members_resource = DataTypeSpecificResource(
        data_type="members",
        api_rate_limit_override=1.0,  # Members API is slower
        num_api_keys=1,  # Members need fewer keys
    )

    return {
        "processing_resource": processing_resource,
        "bills_specific": bills_resource,
        "members_specific": members_resource,
    }


def create_asset_checks():
    """Create asset checks for data quality and consistency."""
    # This would include checks like:
    # - Row count validations
    # - Data freshness checks
    # - Schema validation
    # - Cross-table consistency checks
    # - API key usage monitoring


# Create comprehensive definitions
all_assets = create_all_assets()
all_jobs = create_pipeline_jobs()
all_schedules = create_schedules()
all_resources = create_resources()

# Main definitions object
defs = Definitions(
    assets=all_assets,
    jobs=all_jobs,
    schedules=all_schedules,
    resources=all_resources,
)

# Export for easy access
__all__ = [
    "defs",
    "AVAILABLE_DATA_TYPES",
    "HIGH_PRIORITY_DATA_TYPES",
    "CONCURRENT_SAFE_GROUPS",
    "create_all_assets",
    "create_pipeline_jobs",
    "create_schedules",
    "create_resources",
]


# Example usage documentation
USAGE_EXAMPLES = """
# Example Usage:

## 1. Running specific data types
dagster job execute -j bills_full_pipeline
dagster job execute -j amendments_raw_only
dagster job execute -j members_staging_tables

## 2. Running multiple data types
dagster job execute -j all_data_types_raw
dagster job execute -j high_priority_data_types
dagster job execute -j concurrent_group_1

## 3. Materializing specific assets
dagster asset materialize -a bills_raw_data
dagster asset materialize -a bills_actions_staging
dagster asset materialize -g bills_pipeline

## 4. Environment variables needed
export CONGRESS_API_KEYS="key1,key2,key3,key4"
export POSTGRESQL_HOST="localhost"
export POSTGRESQL_DATABASE="bicam_collection"
export POSTGRESQL_USERNAME="bicam_user"
export POSTGRESQL_PASSWORD="password"
export BATCH_SIZE="100"

## 5. Monitoring API key usage
# The system automatically logs key assignments and releases
# Check logs for messages like:
# "Assigned 2 keys to bills: [abc12345..., def67890...]"
# "Released 2 keys from bills"
# "Rebalancing keys across 3 active data types"
"""
