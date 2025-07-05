"""Utility command implementations used by the lightweight CLI.

Each helper is *self-contained* and depends only on public APIs from
``libs`` and ``api_clients``.  They all accept a ``ProcessingResource``
instance so that database pools, key-managers etc. are reused.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence

from ..api_clients.congressional_api import CongressionalAPIClient
from ..libs.config import BicamConfig
from ..libs.data_type_router import get_global_registry
from ..libs.database import setup_database
from .shared_resources import ProcessingResource

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Database schema management -------------------------------------------------
# ---------------------------------------------------------------------------


async def setup_db(processing_resource: ProcessingResource, recreate: bool = False):
    """Create or recreate all required schemas/tables."""

    # Create a basic config object from the processing resource
    config = BicamConfig(
        database={
            "host": processing_resource.db_host,
            "port": processing_resource.db_port,
            "database": processing_resource.db_name,
            "username": processing_resource.db_user,
            "password": processing_resource.db_password,
        }
    )

    success = await setup_database(config, recreate=recreate)
    if success:
        logger.info("Database setup completed successfully")
    else:
        logger.error("Database setup failed")
        raise RuntimeError("Database setup failed")


# ---------------------------------------------------------------------------
# Date management -----------------------------------------------------------
# ---------------------------------------------------------------------------


async def check_dates(processing_resource: ProcessingResource):
    """Print last_processed dates for all data types."""

    db_pool = await processing_resource.get_db_pool()
    registry = get_global_registry()

    logger.info("Checking last processed dates...")

    # Get all registered data types
    data_types = registry.list_data_types()

    if not data_types:
        logger.info("No data types registered")
        return

    async with db_pool.acquire() as conn:
        for data_type in data_types:
            try:
                # Check if metadata table exists
                table_exists = await conn.fetchval("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_schema = 'bicam_metadata'
                        AND table_name = 'last_processed'
                    )
                """)

                if not table_exists:
                    logger.info(f"{data_type}: No metadata table found")
                    continue

                # Get last processed date
                last_date = await conn.fetchval(
                    """
                    SELECT last_processed_date
                    FROM bicam_metadata.last_processed
                    WHERE data_type = $1
                """,
                    data_type,
                )

                if last_date:
                    logger.info(f"{data_type}: {last_date}")
                else:
                    logger.info(f"{data_type}: No last processed date")

            except Exception as e:
                logger.error(f"Error checking {data_type}: {e}")


async def reset_dates(
    processing_resource: ProcessingResource, data_types: Sequence[str] | None = None
):
    """Delete last_processed entries for specified data types."""

    db_pool = await processing_resource.get_db_pool()
    registry = get_global_registry()

    # Default to all data types if none specified
    if data_types is None:
        data_types = registry.list_data_types()

    if not data_types:
        logger.info("No data types to reset")
        return

    logger.info(f"Resetting dates for: {', '.join(data_types)}")

    async with db_pool.acquire() as conn:
        # Check if metadata table exists
        table_exists = await conn.fetchval("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'bicam_metadata'
                AND table_name = 'last_processed'
            )
        """)

        if not table_exists:
            logger.info("No metadata table found - nothing to reset")
            return

        # Delete entries for specified data types
        for data_type in data_types:
            try:
                result = await conn.execute(
                    """
                    DELETE FROM bicam_metadata.last_processed
                    WHERE data_type = $1
                """,
                    data_type,
                )

                # Extract number of rows affected
                rows_deleted = int(result.split()[-1]) if result.split() else 0

                if rows_deleted > 0:
                    logger.info(f"Reset {data_type} (deleted {rows_deleted} entries)")
                else:
                    logger.info(f"No entries found for {data_type}")

            except Exception as e:
                logger.error(f"Error resetting {data_type}: {e}")


# ---------------------------------------------------------------------------
# Checkpoint management -----------------------------------------------------
# ---------------------------------------------------------------------------


async def clear_checkpoints(
    processing_resource: ProcessingResource, data_type: str, stage: str | None = None
):
    """Delete checkpoints for a data type and optional stage."""

    checkpoint_manager = processing_resource.get_checkpoint_manager()

    if stage is None or stage == "all":
        stages = ["scraping", "normalization", "cleaning", "analysis"]
    else:
        stages = [stage]

    logger.info(f"Clearing checkpoints for {data_type}, stages: {', '.join(stages)}")

    total_cleared = 0

    for stage_name in stages:
        try:
            # Get checkpoints for this data type and stage
            checkpoints = checkpoint_manager.get_checkpoints_for_data_type(
                data_type, stage_name
            )

            if not checkpoints:
                logger.info(f"No checkpoints found for {data_type} - {stage_name}")
                continue

            # Clear checkpoints
            for checkpoint in checkpoints:
                checkpoint_manager.clear_checkpoint(checkpoint.id)
                total_cleared += 1

            logger.info(
                f"Cleared {len(checkpoints)} checkpoints for {data_type} - {stage_name}"
            )

        except Exception as e:
            logger.error(
                f"Error clearing checkpoints for {data_type} - {stage_name}: {e}"
            )

    logger.info(f"Total checkpoints cleared: {total_cleared}")


# ---------------------------------------------------------------------------
# Data type management ------------------------------------------------------
# ---------------------------------------------------------------------------


async def list_types():
    """List all registered data types."""

    registry = get_global_registry()
    data_types = registry.list_data_types()

    if not data_types:
        logger.info("No data types registered")
        return

    logger.info(f"Registered data types ({len(data_types)}):")
    for data_type in sorted(data_types):
        try:
            data_source = registry.get_data_source(data_type)
            config_file = registry.get_config_file(data_type)
            logger.info(f"  {data_type} ({data_source}) - {config_file}")
        except Exception as e:
            logger.info(f"  {data_type} - Error: {e}")


# ---------------------------------------------------------------------------
# API client testing -------------------------------------------------------
# ---------------------------------------------------------------------------


async def test_api_connection(processing_resource: ProcessingResource):
    """Test API connection with the configured keys."""

    if not processing_resource.api_keys:
        logger.error("No API keys configured")
        return

    logger.info(
        f"Testing API connection with {len(processing_resource.api_keys)} keys..."
    )

    db_pool = await processing_resource.get_db_pool()

    for i, api_key in enumerate(processing_resource.api_keys):
        try:
            # Test with a simple bills request
            client = CongressionalAPIClient(
                api_keys=[api_key],
                rate_limit_per_second=processing_resource.api_rate_limit,
                db_pool=db_pool,
            )

            async with client:
                # Make a simple test request
                response = await client.get_items_list("bills", limit=1, offset=0)

                if response:
                    logger.info(f"API key {i + 1}: ✓ Working")
                else:
                    logger.warning(f"API key {i + 1}: ✗ No response")

        except Exception as e:
            logger.error(f"API key {i + 1}: ✗ Error: {e}")

    logger.info("API connection test completed")
