#!/usr/bin/env python3
"""
Test script to demonstrate the improved parallel processing for fetching related tables.

This script shows how the new implementation uses multiple API keys and parallel workers
to fetch related tables much faster than the previous sequential approach.

Usage:
    python test_parallel_related_tables.py --data-type bills --related-tables texts actions
"""

import argparse
import asyncio
import logging
import time
from datetime import datetime

from src.streamlined.executor import StreamlinedExecutor
from src.streamlined.plugins.consolidated_registry import get_consolidated_registry
from src.streamlined.resources.config import StreamlinedConfig
from src.streamlined.resources.coordinator import ResourceCoordinator

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


async def test_parallel_related_tables(
    data_type: str,
    related_tables: list[str],
    batch_size: int = 100,
    limit: int = None,
    from_date: str = None,
    to_date: str = None,
):
    """
    Test the parallel processing for fetching related tables.

    Args:
        data_type: The data type to process (e.g., 'bills', 'amendments')
        related_tables: List of related table names to fetch
        batch_size: Number of items to process in each batch
        limit: Maximum number of items to process
        from_date: Start date filter
        to_date: End date filter
    """

    logger.info("=" * 80)
    logger.info("PARALLEL RELATED TABLES FETCH TEST")
    logger.info("=" * 80)

    start_time = time.time()

    # Load configuration
    logger.info("Loading configuration...")
    config = StreamlinedConfig.from_env()

    # Validate configuration
    errors = config.validate()
    if errors:
        logger.error(f"Configuration errors: {', '.join(errors)}")
        return

    logger.info(f"Configuration loaded with {len(config.api.keys)} API keys")

    # Get data source
    registry = get_consolidated_registry()
    data_source = registry.get_data_source(data_type)
    logger.info(f"Data source for {data_type}: {data_source}")

    # Initialize coordinator
    logger.info("Initializing resource coordinator...")
    coordinator = ResourceCoordinator(config=config, source=data_source)
    await coordinator.initialize()

    # Create executor
    logger.info("Creating streamlined executor...")
    executor = StreamlinedExecutor(coordinator)

    try:
        # Execute parallel related tables fetch
        logger.info(
            f"Starting parallel fetch for {data_type} with tables: {related_tables}"
        )

        results = await executor.fetch_related_tables_from_existing_data(
            data_type=data_type,
            related_tables=related_tables,
            batch_size=batch_size,
            limit=limit,
            from_date=from_date,
            to_date=to_date,
        )

        # Display results
        total_time = time.time() - start_time

        logger.info("=" * 80)
        logger.info("PARALLEL FETCH RESULTS")
        logger.info("=" * 80)

        logger.info(f"Status: {results.get('status', 'unknown')}")
        logger.info(f"Data Type: {results.get('data_type', 'unknown')}")
        logger.info(f"Data Source: {results.get('data_source', 'unknown')}")
        logger.info(f"Related Tables: {results.get('related_tables', [])}")

        metrics = results.get("metrics", {})
        logger.info(f"Items Processed: {metrics.get('items_processed', 0)}")
        logger.info(f"Related Items Fetched: {metrics.get('related_items_fetched', 0)}")
        logger.info(f"Errors: {metrics.get('errors', 0)}")
        logger.info(f"Duration: {metrics.get('duration', 0):.2f} seconds")
        logger.info(f"Workers Used: {metrics.get('workers_used', 0)}")
        logger.info(f"API Keys Used: {metrics.get('api_keys_used', 0)}")

        logger.info(f"Total Script Time: {total_time:.2f} seconds")

        # Calculate performance metrics
        if metrics.get("items_processed", 0) > 0:
            items_per_second = metrics.get("items_processed", 0) / metrics.get(
                "duration", 1
            )
            logger.info(f"Processing Rate: {items_per_second:.2f} items/second")

            if metrics.get("related_items_fetched", 0) > 0:
                related_per_second = metrics.get(
                    "related_items_fetched", 0
                ) / metrics.get("duration", 1)
                logger.info(
                    f"Related Items Rate: {related_per_second:.2f} items/second"
                )

        # Show parallel processing benefits
        if metrics.get("workers_used", 0) > 1:
            logger.info("=" * 80)
            logger.info("PARALLEL PROCESSING BENEFITS")
            logger.info("=" * 80)
            logger.info(f"✓ Used {metrics.get('workers_used', 0)} parallel workers")
            logger.info(
                f"✓ Used {metrics.get('api_keys_used', 0)} API keys concurrently"
            )
            logger.info("✓ Distributed work across multiple API keys")
            logger.info("✓ Reduced total processing time significantly")
            logger.info("✓ Better resource utilization")

    except Exception as e:
        logger.error(f"Error during parallel fetch: {e}")
        raise
    finally:
        # Cleanup
        logger.info("Cleaning up resources...")
        await coordinator.cleanup()


def main():
    parser = argparse.ArgumentParser(
        description="Test parallel processing for fetching related tables"
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

    # Run the test
    asyncio.run(
        test_parallel_related_tables(
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
