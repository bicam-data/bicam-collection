#!/usr/bin/env python3
"""
Simple test script to debug the redistribution optimization.

This is a minimal version to isolate the 'NoneType' error issue.
"""

import asyncio
import logging
import os

from bicam_collection.api_clients.congressional_api import CongressionalAPIClient
from bicam_collection.data_types.congressional.bills.fetcher import BillsFetcher

# Setup detailed logging
logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


async def test_basic_functionality():
    """Test basic functionality to isolate the error."""
    print("=" * 60)
    print("SIMPLE REDISTRIBUTION TEST")
    print("=" * 60)

    # Setup
    api_key = os.getenv("CONGRESSIONAL_API_KEY")
    if not api_key:
        print("ERROR: CONGRESSIONAL_API_KEY environment variable not set")
        return

    print(f"Using API key: {api_key[:10]}...")

    try:
        # Create client with proper session management
        client = CongressionalAPIClient(api_keys=[api_key])

        # Test 1: Basic API client functionality
        print("\n--- Test 1: Basic API Client ---")

        async with client:
            # Test a simple API call
            async for batch in client.retrieve_data_list(
                data_type="bill", from_date="2024-12-01", to_date="2024-12-02", limit=5
            ):
                print(f"Retrieved batch with {len(batch)} items")
                if batch:
                    first_item = batch[0]
                    print(f"First item keys: {list(first_item.keys())}")
                    print(f"First item URL: {first_item.get('url', 'NO URL FOUND')}")
                break

        # Test 2: Basic fetcher without parallelization
        print("\n--- Test 2: Basic Fetcher (Sequential) ---")

        fetcher = BillsFetcher(
            client=CongressionalAPIClient(api_keys=[api_key]),
            db_pool=None,
        )

        print("Testing sequential processing...")
        result = await fetcher.process_items(
            from_date="2024-12-01",
            to_date="2024-12-02",
            limit=5,
            max_items=3,
            enable_parallelization=False,  # Start with sequential
        )

        print(f"Sequential result: {result}")

        # Test 3: Try parallelization without redistribution
        print("\n--- Test 3: Parallelization Without Redistribution ---")

        class MockProcessingResource:
            async def get_api_clients_for_parallel_sessions(self, data_type: str):
                return [CongressionalAPIClient(api_keys=[api_key]) for _ in range(3)]

        fetcher.set_processing_resource(MockProcessingResource())

        print("Testing parallel processing without redistribution...")
        result = await fetcher.process_items(
            from_date="2024-12-01",
            to_date="2024-12-02",
            limit=5,
            max_items=3,
            enable_parallelization=True,
            redistribute_idle_sessions=False,  # Disable redistribution first
        )

        print(f"Parallel without redistribution result: {result}")

        # Test 4: Try parallelization with redistribution
        print("\n--- Test 4: Parallelization With Redistribution ---")

        print("Testing parallel processing with redistribution...")
        result = await fetcher.process_items(
            from_date="2024-12-01",
            to_date="2024-12-02",
            limit=5,
            max_items=3,
            enable_parallelization=True,
            redistribute_idle_sessions=True,  # Enable redistribution
        )

        print(f"Parallel with redistribution result: {result}")

    except Exception as e:
        print(f"ERROR: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(test_basic_functionality())
