#!/usr/bin/env python3
"""
Example: Bills Fetcher with 3-Phase Processing and Checkpointing

This example demonstrates how to use the BillsFetcher with the proper 3-phase
Congressional API approach and granular checkpointing at the item level.

The workflow follows:
1. Phase 1: Get list data from bulk endpoints
2. For each list item:
   - Phase 2: Get full data using URL from list item
   - Phase 3: Get all related data using full data
   - Store everything for that one item in database at once
3. Continue with next item with checkpointing support
"""

import asyncio
import logging
from pathlib import Path

import asyncpg

from src.bicam_collection.api_clients import CongressionalAPIClient
from src.bicam_collection.data_types.congressional.bills.fetcher import BillsFetcher
from src.bicam_collection.libs.checkpoint import CheckpointManager

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


async def example_bills_processing():
    """
    Example of processing bills with 3-phase approach and checkpointing.
    """
    # Setup database connection
    DATABASE_URL = "postgresql://user:password@localhost:5432/bicam_db"

    try:
        db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
        logger.info("Database connection established")
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        # For demonstration, continue without database
        db_pool = None

    # Setup API client
    api_key = "YOUR_API_KEY_HERE"  # Replace with actual API key
    client = CongressionalAPIClient(api_key=api_key)

    # Setup checkpoint manager
    checkpoint_manager = CheckpointManager(db_path=Path("checkpoints_bills.db"))

    # Setup bills fetcher
    fetcher = BillsFetcher(client, db_pool)
    progress_tracker = fetcher.setup_progress_tracker(checkpoint_manager)

    try:
        logger.info("Starting bills processing with 3-phase approach...")

        # Process bills with checkpointing (processes recent bills)
        stats = await fetcher.process_list_with_checkpointing(
            from_date="2024-01-01",
            to_date="2024-12-31",
            limit=50,  # Process 50 bills per page
            batch_id="example_batch_2024",
        )

        logger.info(f"Processing completed! Stats: {stats}")
        print(f"✅ Processed: {stats['processed']}")
        print(f"❌ Failed: {stats['failed']}")
        print(f"⏭️ Skipped: {stats['skipped']}")

        # Show progress summary
        progress = progress_tracker.detailed_progress
        print(f"\n📊 Progress Summary:")
        print(f"   Overall: {progress['overall_progress']:.1f}%")
        print(f"   Current Phase: {progress['current_phase']}")
        print(f"   Status: {progress['status']}")

    except Exception as e:
        logger.error(f"Error during processing: {e}")
        raise
    finally:
        if db_pool:
            await db_pool.close()
            logger.info("Database connection closed")


async def example_single_item_processing():
    """
    Example of processing a single bill item.
    """
    # Setup (simplified for demonstration)
    api_key = "YOUR_API_KEY_HERE"
    client = CongressionalAPIClient(api_key=api_key)
    fetcher = BillsFetcher(client, None)  # No database for this example

    checkpoint_manager = CheckpointManager(db_path=Path("checkpoints_single.db"))
    progress_tracker = fetcher.setup_progress_tracker(checkpoint_manager)

    # Example list item (this would normally come from Phase 1)
    example_list_item = {
        "url": "https://api.congress.gov/v3/bill/118/house-bill/1",
        "type": "hr",
        "number": "1",
        "congress": "118",
    }

    logger.info("Processing single bill item...")

    try:
        success = await fetcher.process_single_item_with_checkpointing(
            example_list_item, batch_id="single_item_example"
        )

        if success:
            print("✅ Single item processed successfully!")
        else:
            print("❌ Single item processing failed!")

    except Exception as e:
        logger.error(f"Error processing single item: {e}")


if __name__ == "__main__":
    print("Bills Fetcher Example - 3-Phase Processing with Checkpointing")
    print("=" * 60)

    # Choose which example to run
    choice = input(
        "Run (1) Full list processing or (2) Single item processing? [1/2]: "
    )

    if choice == "2":
        asyncio.run(example_single_item_processing())
    else:
        asyncio.run(example_bills_processing())
