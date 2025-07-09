#!/usr/bin/env python3
"""
Debug script to identify why billcollections is storing duplicate bills.

This script will:
1. Check the actual data structure from GovInfo API
2. Verify ID extraction logic
3. Test checkpoint functionality
4. Identify the root cause of duplicates
"""

import asyncio
import json
import logging
from collections import Counter
from typing import Any

from src.streamlined.api_clients.govinfo_api import GovInfoAPIClient
from src.streamlined.plugins.govinfo import GovInfoFetcherPlugin

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


async def debug_billcollections_data_structure():
    """Debug the actual data structure from GovInfo API."""
    print("=" * 80)
    print("DEBUGGING BILLCOLLECTIONS DATA STRUCTURE")
    print("=" * 80)

    # Create API client
    api_key = "YOUR_API_KEY_HERE"  # Replace with actual key
    client = GovInfoAPIClient(api_key=api_key)

    # Create plugin
    plugin = GovInfoFetcherPlugin("billcollections")

    try:
        # Fetch a small sample of data
        print("\n1. FETCHING SAMPLE DATA FROM GOVINFO API")
        print("-" * 50)

        data_batches = await plugin.fetch_list_data(
            api_client=client,
            from_date="2024-01-01",
            to_date="2024-01-31",  # Small date range
            limit=10,  # Small limit
            single_page_only=True,
        )

        if not data_batches:
            print("❌ No data returned from API")
            return

        print(f"✅ Retrieved {len(data_batches)} items")

        # Analyze the first few items
        print("\n2. ANALYZING DATA STRUCTURE")
        print("-" * 50)

        for i, item in enumerate(data_batches[:3]):
            print(f"\nItem {i + 1}:")
            print(f"  Keys: {list(item.keys())}")

            # Check for ID fields
            id_fields = ["packageId", "package_id", "id", "bill_id", "number"]
            found_ids = {}
            for field in id_fields:
                if field in item:
                    found_ids[field] = item[field]

            print(f"  ID fields found: {found_ids}")

            # Test ID extraction
            extracted_id = await plugin.extract_item_id(item)
            print(f"  Extracted ID: {extracted_id}")

            # Show URL field
            print(f"  URL: {item.get('url', 'MISSING')}")
            print(f"  PackageLink: {item.get('packageLink', 'MISSING')}")

        # Check for duplicates in the sample
        print("\n3. CHECKING FOR DUPLICATES IN SAMPLE")
        print("-" * 50)

        extracted_ids = []
        for item in data_batches:
            item_id = await plugin.extract_item_id(item)
            extracted_ids.append(item_id)

        id_counts = Counter(extracted_ids)
        duplicates = {id_val: count for id_val, count in id_counts.items() if count > 1}

        if duplicates:
            print(f"❌ Found {len(duplicates)} duplicate IDs in sample:")
            for id_val, count in duplicates.items():
                print(f"  {id_val}: {count} times")
        else:
            print("✅ No duplicates found in sample")

        # Test with different date ranges
        print("\n4. TESTING DIFFERENT DATE RANGES")
        print("-" * 50)

        date_ranges = [
            ("2024-01-01", "2024-01-31"),
            ("2024-02-01", "2024-02-29"),
            ("2024-03-01", "2024-03-31"),
        ]

        for start_date, end_date in date_ranges:
            print(f"\nTesting {start_date} to {end_date}:")

            range_data = await plugin.fetch_list_data(
                api_client=client,
                from_date=start_date,
                to_date=end_date,
                limit=5,
                single_page_only=True,
            )

            if range_data:
                range_ids = [await plugin.extract_item_id(item) for item in range_data]
                range_counts = Counter(range_ids)
                range_duplicates = {
                    id_val: count for id_val, count in range_counts.items() if count > 1
                }

                print(f"  Items: {len(range_data)}")
                print(f"  Unique IDs: {len(set(range_ids))}")
                print(f"  Duplicates: {len(range_duplicates)}")

                if range_duplicates:
                    print(f"  Duplicate IDs: {list(range_duplicates.keys())}")
            else:
                print("  No data returned")

    except Exception as e:
        print(f"❌ Error during debugging: {e}")
        import traceback

        traceback.print_exc()


async def debug_checkpoint_functionality():
    """Debug checkpoint functionality."""
    print("\n" + "=" * 80)
    print("DEBUGGING CHECKPOINT FUNCTIONALITY")
    print("=" * 80)

    # This would require database setup
    print("Checkpoint debugging requires database setup")
    print("Run this after setting up the database connection")


if __name__ == "__main__":
    asyncio.run(debug_billcollections_data_structure())
    # asyncio.run(debug_checkpoint_functionality())
