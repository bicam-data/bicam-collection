#!/usr/bin/env python3
"""
Script to fetch missing related tables without restarting the entire process.

This script will:
1. Find all items that have Phase 2 data but are missing specific related tables
2. Fetch only those missing related tables
3. Store them in the appropriate raw tables

Usage:
    python fetch_missing_related_tables.py --data-type bills --related-tables texts
    python fetch_missing_related_tables.py --data-type bills --related-tables texts actions cosponsors
"""

import argparse
import asyncio
import logging
from typing import List, Set

from src.streamlined.resources.coordinator import ResourceCoordinator
from src.streamlined.fetcher import StreamlinedFetcher
from src.streamlined.libs.hierarchical_checkpoint_system import (
    HierarchicalCheckpointSystem,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def fetch_missing_related_tables(
    data_type: str,
    related_tables: List[str],
    from_date: str = None,
    to_date: str = None,
    limit: int = None,
    batch_size: int = 100,
):
    """
    Fetch missing related tables for a specific data type.

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

    # Get all items that have Phase 2 data but are missing the specified related tables
    missing_items = await get_items_missing_related_tables(
        fetcher, data_type, related_tables, from_date, to_date, limit
    )

    if not missing_items:
        logger.info("No items found missing the specified related tables")
        return

    logger.info(
        f"Found {len(missing_items)} items missing related tables: {related_tables}"
    )

    # Process items in batches
    processed_count = 0
    for i in range(0, len(missing_items), batch_size):
        batch = missing_items[i : i + batch_size]
        logger.info(
            f"Processing batch {i // batch_size + 1}/{(len(missing_items) + batch_size - 1) // batch_size}"
        )

        for item in batch:
            try:
                # Get the detailed data (Phase 2 data)
                detailed_data = await fetcher.fetch_phase_2_data_with_client(
                    item["url"], fetcher.client
                )

                if not detailed_data:
                    logger.warning(
                        f"Could not fetch detailed data for {item.get('id', 'unknown')}"
                    )
                    continue

                # Fetch only the specified related tables
                related_data = await fetch_specific_related_tables(
                    fetcher, detailed_data, related_tables
                )

                if related_data:
                    # Store the related data
                    item_id = await fetcher.extract_item_id(detailed_data)
                    await fetcher.store_phase_3_data(related_data, item_id)
                    processed_count += 1
                    logger.debug(f"Stored related data for {item_id}")

            except Exception as e:
                logger.error(f"Error processing item {item.get('id', 'unknown')}: {e}")
                continue

    logger.info(f"Successfully processed {processed_count} items")


async def get_items_missing_related_tables(
    fetcher,
    data_type: str,
    related_tables: List[str],
    from_date: str = None,
    to_date: str = None,
    limit: int = None,
) -> List[dict]:
    """
    Get items that have Phase 2 data but are missing the specified related tables.
    """
    missing_items = []

    # Get all Phase 1 items
    async for batch in fetcher.fetch_phase_1_data_with_client(
        fetcher.client, from_date, to_date, limit
    ):
        for item in batch:
            # Check if this item has Phase 2 data
            item_id = await fetcher.extract_item_id(item)

            # Check if Phase 2 data exists
            if not fetcher.get_fetching_checkpoint().should_skip_full_data(item_id):
                continue  # Skip if Phase 2 data doesn't exist

            # Check which related tables are missing
            missing_tables = []
            for table in related_tables:
                if not fetcher.get_fetching_checkpoint().should_skip_related_endpoint(
                    item_id, f"get_{data_type}_{table}", fetcher.data_source
                ):
                    missing_tables.append(table)

            if missing_tables:
                missing_items.append(
                    {
                        "id": item_id,
                        "url": item.get("url"),
                        "missing_tables": missing_tables,
                    }
                )

    return missing_items


async def fetch_specific_related_tables(
    fetcher, detailed_data: dict, related_tables: List[str]
) -> List[dict]:
    """
    Fetch only the specified related tables for an item.
    """
    related_data = []

    # Get the custom logic plugin
    if not fetcher._plugin:
        logger.error("No plugin available for fetching related data")
        return []

    # Get all available related methods
    all_related_methods = await fetcher._plugin.fetch_related_data(
        fetcher.client, detailed_data
    )

    if not isinstance(all_related_methods, list):
        return []

    # Filter to only the requested related tables
    for related_item in all_related_methods:
        method_name = related_item.get("type", "unknown")

        # Check if this method corresponds to one of the requested tables
        for table in related_tables:
            if method_name == f"get_{fetcher.data_type_name}_{table}":
                related_data.append(related_item)
                break

    return related_data


def main():
    parser = argparse.ArgumentParser(description="Fetch missing related tables")
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
        fetch_missing_related_tables(
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
