"""
Dagster Definitions for Bicam Collection Pipeline

This module defines the complete Dagster definitions including:
- Assets (raw fetching, normalization, cleaning, dbt transformations)
- Resources (processing resource, dbt resource)
- Definitions (combines everything)

Usage:
    from streamlined.dagster_definitions import defs
    # Use defs in your Dagster code location
"""

import logging
from pathlib import Path

from dagster import Definitions
from dagster_dbt import DbtCliResource

from .dagster_assets import (
    clean_to_production,
    create_dbt_assets,
    fetch_raw_data,
    normalize_to_staging,
)
from .resources.dagster_integration import processing_resource

logger = logging.getLogger(__name__)


def create_definitions(
    dbt_project_dir: str | Path | None = None,
    dbt_profiles_dir: str | Path | None = None,
) -> Definitions:
    """
    Create Dagster definitions for the Bicam Collection pipeline.

    Args:
        dbt_project_dir: Path to dbt project directory (defaults to repo_root/dbt)
        dbt_profiles_dir: Path to dbt profiles directory (defaults to ~/.dbt)

    Returns:
        Dagster Definitions object
    """
    # Determine dbt project directory
    if dbt_project_dir is None:
        repo_root = Path(__file__).resolve().parents[2]
        dbt_project_dir = repo_root / "dbt"
    else:
        dbt_project_dir = Path(dbt_project_dir)

    # Create dbt resource
    dbt = DbtCliResource(
        project_dir=str(dbt_project_dir),
        profiles_dir=str(dbt_profiles_dir) if dbt_profiles_dir else None,
    )

    # Load Python assets
    python_assets = [
        fetch_raw_data,
        normalize_to_staging,
        clean_to_production,
    ]

    # Create dbt assets
    dbt_assets_list = create_dbt_assets(dbt, str(dbt_project_dir))

    # Combine all assets
    all_assets = python_assets + dbt_assets_list

    # Define resources
    resources = {
        "processing_resource": processing_resource,
        "dbt": dbt,
    }

    # Create definitions
    defs = Definitions(
        assets=all_assets,
        resources=resources,
    )

    logger.info(
        f"Created Dagster definitions with {len(all_assets)} assets "
        f"({len(python_assets)} Python, {len(dbt_assets_list)} dbt)"
    )

    return defs


# Default definitions instance
defs = create_definitions()
