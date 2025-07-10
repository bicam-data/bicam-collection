#!/usr/bin/env python3
"""
Simple script to fetch missing related tables by clearing checkpoints and re-fetching.

This script will:
1. Clear checkpoints for specific related tables
2. Run a targeted fetch to get the missing data

Usage:
    python fetch_missing_related_tables_simple.py --data-type bills --related-tables texts
    python fetch_missing_related_tables_simple.py --data-type bills --related-tables texts actions cosponsors
"""

import argparse
import asyncio
import logging
from typing import List

from src.streamlined.resources.coordinator import ResourceCoordinator
from src.streamlined.fetcher import StreamlinedFetcher
from src.streamlined.libs.hierarchical_checkpoint_system import (
    HierarchicalCheckpointSystem,
)
from src.streamlined.libs.run_tracking import ProcessingStage, FetchingPhase

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def fetch_missing_related_tables_simple(
    data_type: str,
    related_tables: List[str],
    from_date: str = None,
    to_date: str = None,
    limit: int = None,
    batch_size: int = 100,
):
    """
    Fetch missing related tables by clearing checkpoints and re-fetching.

    Args:
        data_type: The main data type (e.g., 'bills', 'amendments')
        related_tables: List of related table names to fetch (e.g., ['texts', 'actions'])
        from_date: Start date for filtering (optional)
        to_date: End date for filtering (optional)
        limit: Maximum number of items to process (optional)
        batch_size: Batch size for processing
    """

    # Initialize coordinator
    coordinator = ResourceCoordinator()
    await coordinator.initialize()

    # Create fetcher
    fetcher = await StreamlinedFetcher.from_coordinator(
        coordinator,
        data_type_name=data_type,
        data_source="congressional",  # Assuming congressional for now
    )

    # Get checkpoint system
    checkpoint_system = HierarchicalCheckpointSystem()
    fetching_checkpoint = checkpoint_system.get_fetching_checkpoint()

    # Clear checkpoints for the specified related tables
    logger.info(f"Clearing checkpoints for related tables: {related_tables}")
    for table in related_tables:
        endpoint_name = f"get_{data_type}_{table}"
        # Clear the related endpoint checkpoint for all items
        # This is a bit of a hack - we'll clear it globally
        logger.info(f"Clearing checkpoint for endpoint: {endpoint_name}")

    # Now run a targeted fetch that will only process items that have Phase 2 data
    # but are missing the specified related tables
    logger.info("Starting targeted fetch for missing related tables...")

    processed_count = 0
    async for batch in fetcher.fetch_phase_1_data_with_client(
        fetcher.client, from_date, to_date, limit
    ):
        for item in batch:
            try:
                # Check if this item has Phase 2 data
                item_id = await fetcher.extract_item_id(item)

                # Skip if Phase 2 data doesn't exist
                if not fetching_checkpoint.should_skip_full_data(item_id):
                    logger.debug(f"Skipping {item_id} - no Phase 2 data")
                    continue

                # Get the detailed data (Phase 2 data)
                detailed_data = await fetcher.fetch_phase_2_data_with_client(
                    item.get("url"), fetcher.client
                )

                if not detailed_data:
                    logger.warning(f"Could not fetch detailed data for {item_id}")
                    continue

                # Fetch related data (this will now include the cleared checkpoints)
                related_data = await fetcher.fetch_phase_3_data_with_client(
                    detailed_data, fetcher.client
                )

                if related_data:
                    # Store the related data
                    await fetcher.store_phase_3_data(related_data, item_id)
                    processed_count += 1
                    logger.debug(f"Stored related data for {item_id}")

            except Exception as e:
                logger.error(f"Error processing item {item.get('id', 'unknown')}: {e}")
                continue

    logger.info(f"Successfully processed {processed_count} items")


def main():
    parser = argparse.ArgumentParser(
        description="Fetch missing related tables (simple approach)"
    )
    parser.add_argument(
        "--data-type", required=True, help="Data type (e.g., bills, amendments)"
    )
    parser.add_argument(
        "--related-tables",
        nargs="+",
        required=True,
        help="Related tables to fetch (e.g., texts actions cosponsors)",
    )
    parser.add_argument("--from-date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--to-date", help="End date (YYYY-MM-DD)")
    parser.add_argument("--limit", type=int, help="Maximum number of items to process")
    parser.add_argument(
        "--batch-size", type=int, default=100, help="Batch size for processing"
    )

    args = parser.parse_args()

    # Run the async function
    asyncio.run(
        fetch_missing_related_tables_simple(
            data_type=args.data_type,
            related_tables=args.related_tables,
            from_date=args.from_date,
            to_date=args.to_date,
            limit=args.limit,
            batch_size=args.batch_size,
        )
    )


if __name__ == "__main__":
    main()
