#!/usr/bin/env python3
"""
Script to fetch missing related tables using the existing CLI infrastructure.

This script will:
1. Clear checkpoints for specific related tables using the CLI
2. Run a targeted fetch to get the missing data

Usage:
    python fetch_missing_related_tables_cli.py --data-type bills --related-tables texts
    python fetch_missing_related_tables_cli.py --data-type bills --related-tables texts actions cosponsors
"""

import argparse
import asyncio
import logging
import subprocess
import sys

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def fetch_missing_related_tables_cli(
    data_type: str,
    related_tables: list[str],
    from_date: str = None,
    to_date: str = None,
    limit: int = None,
):
    """
    Fetch missing related tables using CLI commands.

    Args:
        data_type: The main data type (e.g., 'bills', 'amendments')
        related_tables: List of related table names to fetch (e.g., ['texts', 'actions'])
        from_date: Start date for filtering (optional)
        to_date: End date for filtering (optional)
        limit: Maximum number of items to process (optional)
    """

    # Step 1: Clear checkpoints for the specific related tables
    logger.info(f"Clearing checkpoints for related tables: {related_tables}")

    # Build the clear checkpoints command
    clear_cmd = [
        sys.executable,
        "-m",
        "src.streamlined.cli",
        "clear-checkpoints",
        data_type,
        "--phases",
        "related_data",
        "--confirm",  # Skip confirmation prompt
    ]

    try:
        result = subprocess.run(clear_cmd, capture_output=True, text=True, check=True)
        logger.info("Successfully cleared checkpoints")
        logger.debug(f"Clear checkpoints output: {result.stdout}")
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to clear checkpoints: {e}")
        logger.error(f"Error output: {e.stderr}")
        return

    # Step 2: Run a targeted fetch for only the raw phase (which includes Phase 3)
    logger.info("Starting targeted fetch for missing related tables...")

    # Build the process command
    process_cmd = [
        sys.executable,
        "-m",
        "src.streamlined.cli",
        "process",
        data_type,
        "--phases",
        "raw",  # Only fetch raw data (includes Phase 3)
        "--no-resume",  # Don't resume from checkpoints to force re-fetch
    ]

    if from_date:
        process_cmd.extend(["--from-date", from_date])
    if to_date:
        process_cmd.extend(["--to-date", to_date])
    if limit:
        process_cmd.extend(["--limit", str(limit)])

    try:
        logger.info(f"Running command: {' '.join(process_cmd)}")
        result = subprocess.run(process_cmd, capture_output=True, text=True, check=True)
        logger.info("Successfully completed fetch")
        logger.info(f"Process output: {result.stdout}")
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to fetch data: {e}")
        logger.error(f"Error output: {e.stderr}")
        return

    logger.info("Successfully fetched missing related tables!")


def main():
    parser = argparse.ArgumentParser(
        description="Fetch missing related tables using CLI"
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

    args = parser.parse_args()

    # Run the async function
    asyncio.run(
        fetch_missing_related_tables_cli(
            data_type=args.data_type,
            related_tables=args.related_tables,
            from_date=args.from_date,
            to_date=args.to_date,
            limit=args.limit,
        )
    )


if __name__ == "__main__":
    main()
