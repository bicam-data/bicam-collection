"""High-level orchestration helpers for the new, cleaner CLI.

Only a *tiny* subset of the capabilities from the original 1400-line
`dagster_pipeline/main.py` are re-implemented here – just enough to run
`process <data_type>` through the Dagster asset pipeline.  Features like
*check-dates*, *reset-dates*, database setup etc. can be ported.

Design goals:
1. Accept a fully-constructed `ProcessingResource` (see `shared_resources`).
2. No global state – everything stays inside that resource object.
3. Minimal coupling to Dagster – we build assets on the fly via
   `generalized_assets` exactly like before.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Sequence

from dagster import materialize

from ..libs.data_type_router import get_global_registry
from .generalized_assets import (
    create_production_data_asset,
    create_raw_data_asset,
    create_staging_data_asset,
)
from .shared_resources import ProcessingResource

logger = logging.getLogger(__name__)


def execute_pipeline_sync(
    processing_resource: ProcessingResource,
    data_type: str,
    phases: Sequence[str] | None = None,
) -> None:
    """Synchronous Dagster pipeline execution (Phase 2 of three-phase approach).

    This function executes purely synchronous Dagster assets without any async
    operations, avoiding event loop conflicts.

    Args:
        processing_resource: Fully configured ProcessingResource
        data_type: Name of the data type to process
        phases: Optional list of phases to run (defaults to all)
    """
    logger.info(f"Executing pipeline for data type: {data_type}")

    # Default phases if none specified
    if phases is None:
        phases = ["raw", "staging", "production"]

    # Get data type registry
    registry = get_global_registry()

    # Verify data type is registered
    if not registry.is_registered(data_type):
        raise ValueError(f"Data type '{data_type}' is not registered")

    # Create assets based on requested phases
    assets = []

    if "raw" in phases:
        assets.append(create_raw_data_asset(data_type))

    if "staging" in phases:
        assets.append(create_staging_data_asset(data_type))

    if "production" in phases:
        assets.append(create_production_data_asset(data_type))

    if not assets:
        raise ValueError(f"No valid phases specified: {phases}")

    # Execute the pipeline
    logger.info(f"Executing {len(assets)} assets for phases: {', '.join(phases)}")

    try:
        # Run the materialization
        result = materialize(
            assets,
            resources={
                "processing_resource": processing_resource,
            },
            partition_key=None,
        )

        # Log results
        if result.success:
            logger.info(f"Successfully completed pipeline for {data_type}")
        else:
            logger.error(f"Pipeline failed for {data_type}")
            raise RuntimeError(f"Pipeline execution failed for {data_type}")

    except Exception as e:
        logger.error(f"Error executing pipeline for {data_type}: {e}")
        raise

    logger.info(f"Pipeline completed for {data_type}")


# Keep the old async function for backward compatibility but mark it as deprecated
async def run_single_data_type(
    processing_resource: ProcessingResource,
    data_type: str,
    phases: Sequence[str] | None = None,
    *,
    parallel: bool = False,
    from_date: str | None = None,
    to_date: str | None = None,
    congress: int | None = None,
    resume: bool = False,
    no_incremental: bool = False,
    rerun: bool = False,
) -> None:
    """DEPRECATED: Use the three-phase approach with execute_pipeline_sync instead.

    This function is kept for backward compatibility but the new approach is cleaner.
    """
    logger.warning(
        "run_single_data_type is deprecated. Use the three-phase approach instead."
    )

    # Update processing resource parameters using object.__setattr__ to bypass frozen model
    object.__setattr__(processing_resource, "from_date", from_date)
    object.__setattr__(processing_resource, "to_date", to_date)
    object.__setattr__(processing_resource, "congress", congress)
    object.__setattr__(processing_resource, "use_checkpoint_resume", resume)
    object.__setattr__(
        processing_resource, "use_incremental_dates", not resume and not no_incremental
    )
    object.__setattr__(processing_resource, "incremental", not no_incremental)
    object.__setattr__(processing_resource, "rerun_mode", rerun)

    # Execute synchronously
    execute_pipeline_sync(processing_resource, data_type, phases)


async def _build_processing_resource(
    processing_resource: ProcessingResource,
    data_type: str,
    *,
    parallel_enabled: bool = False,
    from_date: str | None = None,
    to_date: str | None = None,
    congress: int | None = None,
    resume: bool = False,
    no_incremental: bool = False,
    rerun: bool = False,
) -> ProcessingResource:
    """Update a ProcessingResource with CLI flags and data type configuration.

    This function is kept for backward compatibility but is now mostly
    handled by the CLI's _build_processing_resource function.
    """
    # Update the processing resource parameters using object.__setattr__ to bypass frozen model
    object.__setattr__(processing_resource, "from_date", from_date)
    object.__setattr__(processing_resource, "to_date", to_date)
    object.__setattr__(processing_resource, "congress", congress)

    # Incremental / checkpoint flags
    explicit_dates_provided = from_date is not None or to_date is not None
    object.__setattr__(
        processing_resource,
        "use_checkpoint_resume",
        resume and not explicit_dates_provided,
    )
    object.__setattr__(
        processing_resource,
        "use_incremental_dates",
        (not explicit_dates_provided and not resume and not no_incremental),
    )
    object.__setattr__(processing_resource, "incremental", not no_incremental)
    object.__setattr__(processing_resource, "rerun_mode", rerun)

    # Parallelization configuration
    if parallel_enabled:
        # Get data source from registry
        registry = get_global_registry()
        try:
            data_source = registry.get_data_source(data_type)

            # Update parallelization config for this data source
            current_config = processing_resource.parallelization_config.copy()
            if not current_config:
                current_config = {}

            current_config[data_source] = {
                "num_sessions": min(32, os.cpu_count() - 4),
                "keys_per_session": 2,
            }

            object.__setattr__(
                processing_resource, "parallelization_config", current_config
            )

        except Exception as e:
            logger.warning(f"Could not configure parallelization for {data_type}: {e}")

    return processing_resource
