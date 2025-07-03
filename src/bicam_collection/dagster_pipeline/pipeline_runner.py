"""High-level orchestration helpers for the new, cleaner CLI.

Only a *tiny* subset of the capabilities from the original 1400-line
`dagster_pipeline/main.py` are re-implemented here – just enough to run
`process <data_type>` through the Dagster asset pipeline.  Features like
*check-dates*, *reset-dates*, database setup etc. can be ported later.

Design goals:
1.  Accept a fully-constructed `ApplicationContext` (see ``libs.app_context``).
2.  No global state – everything stays inside that context object.
3.  Minimal coupling to Dagster – we build assets on the fly via
    ``generalized_assets`` exactly like before.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Sequence

from dagster import Definitions, materialize

from ..libs.app_context import ApplicationContext
from ..libs.data_type_router import get_global_registry
from .generalized_assets import (
    create_production_data_asset,
    create_raw_data_asset,
    create_staging_data_asset,
)
from .shared_resources import ProcessingResource

logger = logging.getLogger(__name__)


async def _build_processing_resource(
    ctx: ApplicationContext, data_type: str
) -> ProcessingResource:
    """Factory converting our *ApplicationContext* into the Dagster resource."""
    # Get API keys *via* the central manager
    key_mgr = ctx.get_api_key_manager()

    registry = get_global_registry()
    data_source = registry.get_data_source(data_type)

    # For now we just ask the key-manager for *all* keys – more clever
    # per-data-source allocation can be added later.
    api_keys = key_mgr.api_keys

    # We don't expose parallelisation config yet – TODO add field to config
    return ProcessingResource(
        api_keys=api_keys,
        batch_size=ctx.config.processing.chunk_size,
        api_rate_limit=ctx.config.scraping.rate_limit_delay,
        # DB
        db_host=ctx.config.database.host,
        db_port=ctx.config.database.port,
        db_name=ctx.config.database.database,
        db_user=ctx.config.database.username,
        db_password=ctx.config.database.password,
    )


def run_single_data_type(
    ctx: ApplicationContext,
    data_type: str,
    phases: Sequence[str] | None = None,
):
    """Synchronously execute Dagster assets for *data_type* inside current interpreter."""
    registry = get_global_registry()
    if not registry.is_registered(data_type):
        raise ValueError(
            f"Data-type '{data_type}' is not registered.  Available: {registry.list_data_types()}"
        )

    # Dagster assets ---------------------------------------------------
    assets = [
        create_raw_data_asset(data_type),
        create_staging_data_asset(data_type),
        create_production_data_asset(data_type),
    ]

    # Build processing resource (sync wrapper around async factory)
    proc_res = asyncio.run(_build_processing_resource(ctx, data_type))

    defs = Definitions(assets=assets, resources={"processing_resource": proc_res})

    # Select phases
    if phases:
        wanted = set(phases)
        assets = [a for a in assets if a.key.path[-1].split("_")[1] in wanted]

    result = materialize(assets, resources={"processing_resource": proc_res})
    if not result.success:
        raise RuntimeError("Dagster materialization failed – see logs for details")

    logger.info(
        "✓ Completed %s (%s phases)",
        data_type,
        ",".join(p.key.path[-1] for p in assets),
    )
