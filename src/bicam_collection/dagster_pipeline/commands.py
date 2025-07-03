"""Utility command implementations used by the lightweight CLI.

Each helper is *self-contained* and depends only on public APIs from
``libs`` and ``api_clients``.  They all accept an ``ApplicationContext``
instance so that database pools, key-managers etc. are reused.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Sequence

from ..api_clients.congressional_api import CongressionalAPIClient
from ..libs.app_context import ApplicationContext
from ..libs.data_type_router import get_global_registry
from ..libs.database import setup_database

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Database schema management -------------------------------------------------
# ---------------------------------------------------------------------------


def setup_db(ctx: ApplicationContext, recreate: bool = False):
    """Create or recreate all required schemas/tables."""

    async def _inner():
        await setup_database(ctx.config, recreate=recreate)

    asyncio.run(_inner())


# ---------------------------------------------------------------------------
# Last-processed date utilities ---------------------------------------------
# ---------------------------------------------------------------------------


async def _get_congressional_client(ctx: ApplicationContext) -> CongressionalAPIClient:
    # Reuse db pool
    pool = await ctx.get_db_pool()
    keys = ctx.get_api_key_manager().api_keys
    return CongressionalAPIClient(api_keys=keys, db_pool=pool)


def check_dates(ctx: ApplicationContext):
    """Print last_processed_date per data-type (congressional only for now)."""

    async def _inner():
        registry = get_global_registry()
        client = await _get_congressional_client(ctx)
        for dt in registry.list_data_types_by_source("congressional"):
            date = await client.access_last_processed_date(dt)
            logger.info("%s → %s", dt, date or "<none>")

    asyncio.run(_inner())


def reset_dates(ctx: ApplicationContext, data_types: Sequence[str] | None):
    """Delete entries from bicam_metadata.congressional_last_processed_dates."""

    async def _inner():
        # validate list
        registry = get_global_registry()
        if data_types:
            invalid = [d for d in data_types if not registry.is_registered(d)]
            if invalid:
                raise ValueError(f"Unknown data types: {invalid}")
            targets = data_types
        else:
            targets = registry.list_data_types_by_source("congressional")

        pool = await ctx.get_db_pool()
        async with pool.acquire() as conn:  # asyncpg.Connection
            for dt in targets:
                await conn.execute(
                    """
                    DELETE FROM bicam_metadata.congressional_last_processed_dates
                    WHERE data_type = $1
                    """,
                    dt,
                )
                logger.info("reset %s", dt)

    asyncio.run(_inner())


# ---------------------------------------------------------------------------
# Checkpoint helpers ---------------------------------------------------------
# ---------------------------------------------------------------------------


def clear_checkpoints(ctx: ApplicationContext, data_type: str):
    """Remove SQLite checkpoints for *data_type* across all stages."""
    cp = ctx.get_checkpoint_manager()
    cp.delete_checkpoint("congressional", data_type)
    cp.delete_checkpoint("congressional", f"{data_type}_normalization")
    cp.delete_checkpoint("congressional", f"{data_type}_cleaning")
    cp.delete_checkpoint("congressional", f"{data_type}_analysis")
    logger.info("Deleted checkpoints for %s (all stages)", data_type)


# ---------------------------------------------------------------------------
# Listing helper -------------------------------------------------------------
# ---------------------------------------------------------------------------


def list_types():
    registry = get_global_registry()
    for src in registry.list_data_sources():
        print(f"{src}:")  # noqa: T201
        for dt in sorted(registry.list_data_types_by_source(src)):
            print(f"  - {dt}") # noqa: T201
