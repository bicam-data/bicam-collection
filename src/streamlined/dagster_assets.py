"""
Dagster Assets for Bicam Collection Pipeline

This module defines Dagster assets for the complete data pipeline:
1. Raw data fetching (from APIs)
2. Normalization to staging (Python)
3. dbt transformations (int -> stg -> mart)

Best practices:
- Clear asset dependencies
- Proper resource management
- Extensible and maintainable structure
"""

import logging
from typing import Any

from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetsDefinition,
    Config,
    asset,
)
from dagster_dbt import DbtCliResource, dbt_assets
from dagster_dbt.core.resources_v2 import DbtCliInvocation

from .cleaner import Cleaner
from .executor import Executor
from .normalizer import Normalizer
from .plugins.registry import get_registry
from .resources.dagster_integration import get_coordinator_from_context

logger = logging.getLogger(__name__)


class FetchRawDataConfig(Config):
    """Configuration for raw data fetching."""

    data_types: list[str] | None = None
    from_date: str | None = None
    to_date: str | None = None
    limit: int | None = None


class NormalizeToStagingConfig(Config):
    """Configuration for normalization to staging."""

    data_types: list[str] | None = None
    chunk_size: int = 1000


class CleanToProductionConfig(Config):
    """Configuration for cleaning to production."""

    data_types: list[str] | None = None
    chunk_size: int = 1000


def get_all_data_types() -> list[str]:
    """Get all registered data types from the registry."""
    registry = get_registry()
    return registry.get_all_data_types()


@asset(
    group_name="raw",
    description="Fetch raw data from Congressional and GovInfo APIs",
    compute_kind="python",
)
async def fetch_raw_data(
    context: AssetExecutionContext, config: FetchRawDataConfig
) -> dict[str, Any]:
    """
    Fetch raw data from APIs for specified data types.

    If data_types is None, fetches all registered data types.
    """
    coordinator = await get_coordinator_from_context(context)
    executor = Executor(coordinator)

    data_types = config.data_types or get_all_data_types()
    results = {}

    for data_type in data_types:
        try:
            context.log.info(f"Fetching raw data for {data_type}")
            result = await executor.execute_data_type(
                data_type=data_type,
                phases=["raw"],
                from_date=config.from_date,
                to_date=config.to_date,
                limit=config.limit,
            )
            results[data_type] = result
            context.log.info(
                f"Completed fetching {data_type}: {result.get('metrics', {})}"
            )
        except Exception as e:
            context.log.error(f"Error fetching {data_type}: {e}", exc_info=True)
            results[data_type] = {"error": str(e), "status": "failed"}

    return results


@asset(
    group_name="staging",
    deps=[AssetKey("fetch_raw_data")],
    description="Normalize raw data to staging tables",
    compute_kind="python",
)
async def normalize_to_staging(
    context: AssetExecutionContext, config: NormalizeToStagingConfig
) -> dict[str, Any]:
    """
    Normalize raw data to staging tables.

    Reads from raw tables and writes to staging tables.
    """
    coordinator = await get_coordinator_from_context(context)
    normalizer = Normalizer(coordinator)

    data_types = config.data_types or get_all_data_types()
    results = {}

    for data_type in data_types:
        try:
            context.log.info(f"Normalizing {data_type} to staging")
            result = await normalizer.normalize_data_type(
                data_type=data_type, chunk_size=config.chunk_size
            )
            results[data_type] = result
            context.log.info(
                f"Completed normalizing {data_type}: {result.get('metrics', {})}"
            )
        except Exception as e:
            context.log.error(f"Error normalizing {data_type}: {e}", exc_info=True)
            results[data_type] = {"error": str(e), "status": "failed"}

    return results


@asset(
    group_name="production",
    deps=[AssetKey("normalize_to_staging")],
    description="Clean staging data to production tables",
    compute_kind="python",
)
async def clean_to_production(
    context: AssetExecutionContext, config: CleanToProductionConfig
) -> dict[str, Any]:
    """
    Clean staging data to production tables.

    Reads from staging tables and writes to production tables.
    """
    coordinator = await get_coordinator_from_context(context)
    cleaner = Cleaner(coordinator)

    data_types = config.data_types or get_all_data_types()
    results = {}

    for data_type in data_types:
        try:
            context.log.info(f"Cleaning {data_type} to production")
            result = await cleaner.clean_data_type(
                data_type=data_type, chunk_size=config.chunk_size
            )
            results[data_type] = result
            context.log.info(
                f"Completed cleaning {data_type}: {result.get('metrics', {})}"
            )
        except Exception as e:
            context.log.error(f"Error cleaning {data_type}: {e}", exc_info=True)
            results[data_type] = {"error": str(e), "status": "failed"}

    return results


def create_dbt_assets(dbt: DbtCliResource, project_dir: str) -> list[AssetsDefinition]:
    """
    Create dbt assets for int, stg, and mart layers.

    Args:
        dbt: DbtCliResource instance
        project_dir: Path to dbt project directory

    Returns:
        List of dbt asset definitions
    """

    @dbt_assets(
        manifest=dbt.get_manifest(project_dir=project_dir),
        select="tag:int",
        name="dbt_int",
        group_name="int",
        deps=[AssetKey("normalize_to_staging")],
    )
    def dbt_int_assets(context: AssetExecutionContext, dbt: DbtCliInvocation):
        """
        dbt models in the int layer - cleaned and standardized data.

        Reads from staging tables (bicam_staging_congressional, bicam_staging_govinfo)
        created by the normalizer and applies cleaning transformations matching Python plugin logic.
        """
        yield from dbt.cli(["build", "--select", "tag:int"], context=context).stream()

    @dbt_assets(
        manifest=dbt.get_manifest(project_dir=project_dir),
        select="tag:stg",
        name="dbt_stg",
        group_name="stg",
        deps=[AssetKey("dbt_int")],
    )
    def dbt_stg_assets(context: AssetExecutionContext, dbt: DbtCliInvocation):
        """
        dbt models in the stg layer - staging transformations.

        Reads from int layer and applies staging transformations.
        """
        yield from dbt.cli(["build", "--select", "tag:stg"], context=context).stream()

    @dbt_assets(
        manifest=dbt.get_manifest(project_dir=project_dir),
        select="tag:mart",
        name="dbt_mart",
        group_name="mart",
        deps=[AssetKey("dbt_stg")],
    )
    def dbt_mart_assets(context: AssetExecutionContext, dbt: DbtCliInvocation):
        """
        dbt models in the mart layer - final business logic.

        Combines congressional and govinfo sources into final marts.
        """
        yield from dbt.cli(["build", "--select", "tag:mart"], context=context).stream()

    return [dbt_int_assets, dbt_stg_assets, dbt_mart_assets]
