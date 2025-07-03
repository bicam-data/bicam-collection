"""
Simple example demonstrating amendments data processing.

This example shows how to use the amendments components:
1. AmendmentsFetcher - fetch amendments data from Congressional API
2. AmendmentsDatabaseNormalizer - normalize raw data to staging tables
3. AmendmentsCleaner - clean staging data to production tables

Usage:
    python examples/amendments_example.py
"""

import asyncio
import logging
from datetime import datetime

import asyncpg

from src.bicam_collection.api_clients import CongressionalAPIClient
from src.bicam_collection.data_types.congressional.amendments import (
    AmendmentsCleaner,
    AmendmentsDatabaseNormalizer,
    AmendmentsFetcher,
)
from src.bicam_collection.libs.checkpoint import CheckpointManager
from src.bicam_collection.libs.run_tracking import RunManager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main():
    """Main example function demonstrating amendments processing."""

    # Database configuration
    db_config = {
        "host": "localhost",
        "port": 5432,
        "database": "bicam_collection",
        "user": "your_user",
        "password": "your_password",
    }

    # Create database pool
    db_pool = await asyncpg.create_pool(**db_config)

    try:
        # Initialize client and checkpoint manager
        client = CongressionalAPIClient()
        checkpoint_manager = CheckpointManager(
            checkpoint_dir="data/checkpoints",
            run_id=f"amendments_example_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        )
        run_manager = RunManager(db_pool=db_pool)

        # Initialize amendments components
        fetcher = AmendmentsFetcher(
            client=client,
            db_pool=db_pool,
            checkpoint_manager=checkpoint_manager,
            run_manager=run_manager,
        )

        normalizer = AmendmentsDatabaseNormalizer(
            config_path="configs/congressional_config.yaml",
            db_pool=db_pool,
            checkpoint_manager=checkpoint_manager,
            run_manager=run_manager,
        )

        cleaner = AmendmentsCleaner(
            config_path="configs/congressional_config.yaml",
            db_pool=db_pool,
            checkpoint_manager=checkpoint_manager,
            run_manager=run_manager,
        )

        logger.info("=== AMENDMENTS EXAMPLE ===")

        # Step 1: Fetch amendments data
        logger.info("Step 1: Fetching amendments data...")
        fetch_stats = await fetcher.process_items(
            from_date="2024-01-01",
            run_limit=10,  # Limit for example
        )
        logger.info(f"Fetch results: {fetch_stats}")

        # Step 2: Normalize raw data to staging
        logger.info("Step 2: Normalizing raw data to staging...")
        normalize_stats = await normalizer.process_data()
        logger.info(f"Normalize results: {normalize_stats}")

        # Step 3: Clean staging data to production
        logger.info("Step 3: Cleaning staging data to production...")
        clean_stats = await cleaner.process_data()
        logger.info(f"Clean results: {clean_stats}")

        logger.info("=== AMENDMENTS EXAMPLE COMPLETED ===")

    except Exception as e:
        logger.error(f"Error in amendments example: {e}")
        raise
    finally:
        await db_pool.close()


if __name__ == "__main__":
    asyncio.run(main())
