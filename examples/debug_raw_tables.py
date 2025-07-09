"""
Debug script to check what's in the raw tables

This will help us understand why the related tables aren't being processed.
"""

import asyncio
import logging
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from src.streamlined import ResourceCoordinator

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def debug_raw_tables():
    """Check what's in the raw tables for amendments."""

    coordinator = ResourceCoordinator()
    await coordinator.initialize()

    try:
        db_pool = await coordinator.get_db_pool()

        # Check raw tables
        raw_tables = [
            "amendments_raw",
            "amendments_actions_raw",
            "amendments_texts_raw",
            "amendments_cosponsors_raw",
            "amendments_sponsors_raw",
            "amendments_on_behalf_of_raw",
            "amendments_links_raw",
            "amendments_amended_bills_raw",
            "amendments_amended_treaties_raw",
            "amendments_amended_amendments_raw",
        ]

        source_schema = "bicam_raw_congressional"

        logger.info(f"Checking tables in {source_schema} schema:")

        for table_name in raw_tables:
            try:
                async with db_pool.acquire() as conn:
                    # Check if table exists
                    exists = await conn.fetchval(
                        """
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables
                            WHERE table_schema = $1 AND table_name = $2
                        )
                        """,
                        source_schema,
                        table_name.lower(),
                    )

                    if exists:
                        # Get count
                        count = await conn.fetchval(
                            f"SELECT COUNT(*) FROM {source_schema}.{table_name}"
                        )

                        # Get sample record
                        sample = await conn.fetchrow(
                            f"SELECT * FROM {source_schema}.{table_name} LIMIT 1"
                        )

                        logger.info(f"✅ {table_name}: {count} records")
                        if sample:
                            logger.info(f"   Sample keys: {list(sample.keys())}")
                            if "payload" in sample:
                                logger.info(
                                    f"   Has payload field: {type(sample['payload'])}"
                                )
                    else:
                        logger.info(f"❌ {table_name}: Table does not exist")

            except Exception as e:
                logger.error(f"Error checking {table_name}: {e}")

    finally:
        await coordinator.cleanup()


if __name__ == "__main__":
    asyncio.run(debug_raw_tables())
