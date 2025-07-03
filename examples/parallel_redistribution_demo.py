#!/usr/bin/env python3
"""
Demonstration of parallel redistribution optimization for Congressional data fetching.

This script shows how the Congressional base fetcher automatically redistributes
idle API sessions to parallelize related data fetching when there are fewer pages
than available sessions.

Example scenario:
- 11 API sessions available
- Only 2 pages of data to fetch
- Without redistribution: 9 sessions idle
- With redistribution: All 11 sessions help with related data fetching

Usage:
    # First, set your Congressional API key
    export CONGRESSIONAL_API_KEY="your_api_key_here"

    # Then run the demo
    python examples/parallel_redistribution_demo.py
"""

import asyncio
import logging
import os

from bicam_collection.api_clients.congressional_api import CongressionalAPIClient
from bicam_collection.data_types.congressional.bills.fetcher import BillsFetcher

# Setup logging to see the redistribution in action
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


class MockProcessingResource:
    """Mock processing resource to simulate multiple API clients."""

    def __init__(self, num_clients: int = 11):
        self.num_clients = num_clients

    async def get_api_clients_for_parallel_sessions(self, data_type: str):
        """Create multiple API client instances."""
        clients = []
        api_key = os.getenv("CONGRESSIONAL_API_KEY")

        if not api_key:
            raise ValueError("CONGRESSIONAL_API_KEY environment variable not set")

        for i in range(self.num_clients):
            client = CongressionalAPIClient(api_keys=[api_key])
            clients.append(client)

        return clients


async def demonstrate_redistribution():
    """
    Demonstrate the redistribution optimization.
    """
    print("=" * 80)
    print("CONGRESSIONAL PARALLEL REDISTRIBUTION DEMONSTRATION")
    print("=" * 80)

    # Setup
    api_key = os.getenv("CONGRESSIONAL_API_KEY")
    if not api_key:
        raise ValueError(
            "CONGRESSIONAL_API_KEY environment variable not set. Please set it with your Congressional API key."
        )

    main_client = CongressionalAPIClient(api_keys=[api_key])

    # Create fetcher
    fetcher = BillsFetcher(
        client=main_client,
        db_pool=None,  # Not storing for demo
    )

    # Setup mock processing resource with 11 API sessions
    processing_resource = MockProcessingResource(num_clients=11)
    fetcher.set_processing_resource(processing_resource)

    print("\nScenario Setup:")
    print(f"- Available API sessions: {processing_resource.num_clients}")
    print("- Fetching recent bills (likely only 1-2 pages)")
    print("- Each bill has multiple related data endpoints (actions, cosponsors, etc.)")

    print("\n" + "=" * 40)
    print("RUNNING WITH REDISTRIBUTION ENABLED")
    print("=" * 40)

    # Run with redistribution (default)
    print("\nStarting parallel fetch with redistribution...")

    try:
        result_with_redistribution = await fetcher.process_items(
            from_date="2024-12-01",  # More recent date for better results
            to_date="2024-12-03",  # Small date range to ensure few pages
            limit=20,  # Small page size
            max_items=10,  # Limit items for demo
            enable_parallelization=True,
            redistribute_idle_sessions=True,  # This is the key optimization
            max_concurrent=5,
        )

        print("\nResults with redistribution:")
        print(
            f"- Total processed: {result_with_redistribution.get('total_processed', 0)}"
        )
        print(f"- Successful: {result_with_redistribution.get('successful', 0)}")
        print(f"- Errors: {result_with_redistribution.get('errors', 0)}")
        print(
            f"- Duration: {result_with_redistribution.get('duration', 0):.2f} seconds"
        )
        print(
            f"- Redistribution used: {result_with_redistribution.get('redistribution_used', False)}"
        )
        if result_with_redistribution.get("redistribution_used"):
            print(
                f"- Phase 1&2 sessions: {result_with_redistribution.get('phase_1_2_sessions', 'N/A')}"
            )
            print(
                f"- Phase 3 sessions: {result_with_redistribution.get('phase_3_sessions', 'N/A')}"
            )

    except Exception as e:
        print(f"Error during redistribution demo: {e}")

    print("\n" + "=" * 40)
    print("COMPARISON: RUNNING WITHOUT REDISTRIBUTION")
    print("=" * 40)

    # Run without redistribution for comparison
    print("\nStarting parallel fetch without redistribution...")

    try:
        result_without_redistribution = await fetcher.process_items(
            from_date="2024-12-01",
            to_date="2024-12-03",  # Same parameters
            limit=20,
            max_items=10,
            enable_parallelization=True,
            redistribute_idle_sessions=False,  # Disable redistribution
            max_concurrent=5,
        )

        print("\nResults without redistribution:")
        print(
            f"- Total processed: {result_without_redistribution.get('total_processed', 0)}"
        )
        print(f"- Successful: {result_without_redistribution.get('successful', 0)}")
        print(f"- Errors: {result_without_redistribution.get('errors', 0)}")
        print(
            f"- Duration: {result_without_redistribution.get('duration', 0):.2f} seconds"
        )
        print(
            f"- Redistribution used: {result_without_redistribution.get('redistribution_used', False)}"
        )

    except Exception as e:
        print(f"Error during non-redistribution demo: {e}")

    print("\n" + "=" * 40)
    print("PERFORMANCE COMPARISON")
    print("=" * 40)

    try:
        with_duration = result_with_redistribution.get("duration", 0)
        without_duration = result_without_redistribution.get("duration", 0)

        if with_duration > 0 and without_duration > 0:
            speedup = without_duration / with_duration
            improvement = ((without_duration - with_duration) / without_duration) * 100

            print("\nPerformance Analysis:")
            print(f"- With redistribution: {with_duration:.2f} seconds")
            print(f"- Without redistribution: {without_duration:.2f} seconds")
            print(f"- Speedup: {speedup:.2f}x")
            print(f"- Improvement: {improvement:.1f}% faster")

            if speedup > 1.1:
                print("✅ Redistribution optimization provided significant benefit!")
            elif speedup > 1.0:
                print("✅ Redistribution optimization provided some benefit.")
            else:
                print(
                    "ℹ️  No significant performance difference (may need more data or different conditions)"
                )

    except Exception as e:
        print(f"Could not calculate performance comparison: {e}")

    print("\n" + "=" * 40)
    print("OPTIMIZATION EXPLANATION")
    print("=" * 40)

    print("""
When few pages but many API sessions:

WITHOUT REDISTRIBUTION:
┌─ Session 0 ─┐  ┌─ Session 1 ─┐  ┌─ Sessions 2-10 ─┐
│ Page 0      │  │ Page 1      │  │                 │
│ ├─List      │  │ ├─List      │  │    IDLE         │
│ ├─Full      │  │ ├─Full      │  │                 │
│ └─Related   │  │ └─Related   │  │                 │
└─────────────┘  └─────────────┘  └─────────────────┘

WITH REDISTRIBUTION:
┌─ Session 0 ─┐  ┌─ Session 1 ─┐  ┌─ All Sessions ──┐
│ Page 0      │  │ Page 1      │  │                 │
│ ├─List      │  │ ├─List      │  │   Related Data  │
│ └─Full      │  │ └─Full      │  │   Distributed   │
└─────────────┘  └─────────────┘  │   Across All    │
                                  │   11 Sessions   │
                                  └─────────────────┘

Key Benefits:
• Unused API sessions get utilized for related data fetching
• Related data often involves multiple API calls per item
• Better parallelization of the most API-intensive phase
• Overall faster processing when pages < sessions
""")


if __name__ == "__main__":
    asyncio.run(demonstrate_redistribution())
