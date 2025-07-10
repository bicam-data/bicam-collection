#!/usr/bin/env python3
"""
Script to fetch related tables from existing Phase 2 data without checkpoint checking.

This script will:
1. Query the database for all existing Phase 2 data (detailed data)
2. For each item, fetch the specified related tables
3. Store the related data directly

This is much faster than checking checkpoints for every item.

Usage:
    python fetch_related_tables_from_existing_data.py --data-type bills --related-tables texts
    python fetch_related_tables_from_existing_data.py --data-type bills --related-tables texts actions cosponsors
"""

import argparse
import asyncio
import logging

from src.streamlined.fetcher import StreamlinedFetcher
from src.streamlined.resources.coordinator import ResourceCoordinator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def fetch_related_tables_from_existing_data(
    data_type: str,
    related_tables: list[str],
    batch_size: int = 100,
    limit: int = None,
    from_date: str = None,
    to_date: str = None,
):
    """
    Fetch related tables from existing Phase 2 data.

    Args:
        data_type: The main data type (e.g., 'bills', 'amendments')
        related_tables: List of related table names to fetch (e.g., ['texts', 'actions'])
        batch_size: Number of items to process in each batch
        limit: Maximum number of items to process (optional)
        from_date: Start date filter (optional)
        to_date: End date filter (optional)
    """

    # Initialize coordinator
    coordinator = ResourceCoordinator()
    await coordinator.initialize()

    # Create fetcher
    fetcher = await StreamlinedFetcher.from_coordinator(
        coordinator, data_type_name=data_type, data_source="congressional"
    )

    # Get database pool
    db_pool = coordinator.get_db_pool()
    if not db_pool:
        logger.error("No database pool available")
        return

    # Get schema name
    schema_name = fetcher.get_default_schema()
    table_name = f"{data_type}_raw"

    # Build query to get existing Phase 2 data
    query = f"""
    SELECT
        payload,
        source_doc_id,
    FROM {schema_name}.{table_name}
    WHERE payload IS NOT NULL
    """

    params = []
    param_count = 0

    # Add date filters if provided
    if from_date:
        param_count += 1
        query += f" AND payload->>'updatedate' >= ${param_count}"
        params.append(from_date)

    if to_date:
        param_count += 1
        query += f" AND payload->>'updatedate' <= ${param_count}"
        params.append(to_date)

    query += " ORDER BY payload->>'updatedate' DESC"

    if limit:
        query += f" LIMIT {limit}"

    logger.info(f"Querying existing Phase 2 data from {schema_name}.{table_name}")
    logger.info(f"Query: {query}")
    logger.info(f"Parameters: {params}")

    # Get all existing Phase 2 data
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(query, *params)

    if not rows:
        logger.info("No existing Phase 2 data found")
        return

    logger.info(f"Found {len(rows)} existing Phase 2 items")

    # Process items in batches
    processed_count = 0
    error_count = 0

    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        logger.info(
            f"Processing batch {i // batch_size + 1}/{(len(rows) + batch_size - 1) // batch_size}"
        )

        for row in batch:
            try:
                payload = row["payload"]
                source_doc_id = row["source_doc_id"]

                logger.debug(f"Processing item {source_doc_id}")

                # Fetch related data for this item
                related_data = await fetch_specific_related_tables(
                    fetcher, payload, related_tables
                )

                if related_data:
                    # Store the related data
                    await fetcher.store_phase_3_data(related_data, source_doc_id)
                    processed_count += 1
                    logger.debug(f"Stored related data for {source_doc_id}")
                else:
                    logger.debug(f"No related data found for {source_doc_id}")

            except Exception as e:
                logger.error(
                    f"Error processing item {row.get('source_doc_id', 'unknown')}: {e}"
                )
                error_count += 1
                continue

    logger.info(f"Successfully processed {processed_count} items")
    if error_count > 0:
        logger.warning(f"Encountered {error_count} errors")


async def fetch_specific_related_tables(
    fetcher, detailed_data: dict, related_tables: list[str]
) -> list[dict]:
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
                logger.debug(
                    f"Found related data for {table}: {len(related_item.get('data', []))} items"
                )
                break

    return related_data


def main():
    parser = argparse.ArgumentParser(
        description="Fetch related tables from existing Phase 2 data"
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
    parser.add_argument(
        "--batch-size", type=int, default=100, help="Batch size for processing"
    )
    parser.add_argument("--limit", type=int, help="Maximum number of items to process")
    parser.add_argument("--from-date", help="Start date filter (YYYY-MM-DD)")
    parser.add_argument("--to-date", help="End date filter (YYYY-MM-DD)")

    args = parser.parse_args()

    # Run the async function
    asyncio.run(
        fetch_related_tables_from_existing_data(
            data_type=args.data_type,
            related_tables=args.related_tables,
            batch_size=args.batch_size,
            limit=args.limit,
            from_date=args.from_date,
            to_date=args.to_date,
        )
    )


if __name__ == "__main__":
    main()
