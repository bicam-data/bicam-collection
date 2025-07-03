#!/usr/bin/env python3
"""
Demo of phase-specific checkpoint system in Congressional Base Fetcher.

This script demonstrates how the new phase-specific checkpoint system allows for:
1. Granular resumption from incomplete phases
2. Storage of list data before processing full data
3. Independent tracking of list data, full data, and related data phases
4. Resume operations that can pick up exactly where processing left off
"""

import asyncio
import logging
import os
import sys
from datetime import UTC, datetime, timedelta

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.bicam_collection.api_clients.congressional_api import CongressionalAPIClient
from src.bicam_collection.data_types.congressional.bills.fetcher import BillsFetcher
from src.bicam_collection.libs.checkpoint import CheckpointManager
from src.bicam_collection.libs.database import DatabaseManager

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


async def demonstrate_phase_specific_checkpoints():
    """Demonstrate the phase-specific checkpoint system."""

    # Setup database and API client
    db_manager = DatabaseManager()
    db_pool = await db_manager.get_pool()

    client = CongressionalAPIClient(db_pool=db_pool)

    # Setup checkpoint manager
    checkpoint_manager = CheckpointManager("data/checkpoints/demo_checkpoint.db")

    # Create fetcher
    fetcher = BillsFetcher(client=client, db_pool=db_pool)
    fetcher.setup_progress_tracker(checkpoint_manager)

    print("\n" + "=" * 80)
    print("PHASE-SPECIFIC CHECKPOINT SYSTEM DEMO")
    print("=" * 80)

    # Demo 1: Show how processing works with phase-specific checkpointing
    print("\n1. PROCESSING WITH PHASE-SPECIFIC CHECKPOINTS")
    print("-" * 50)

    # Process a small batch of bills
    from_date = (datetime.now(UTC) - timedelta(days=7)).strftime("%Y-%m-%d")

    print(f"Processing bills from {from_date} with phase-specific checkpointing...")

    stats = await fetcher.process_items(
        from_date=from_date,
        max_items=5,  # Small batch for demo
        batch_size=2,
        enable_parallelization=False,
    )

    print(f"Processing stats: {stats}")

    # Demo 2: Show resume capability
    print("\n2. RESUME CAPABILITY ANALYSIS")
    print("-" * 50)

    # Get some sample item IDs to analyze
    sample_items = ["117_hr_1", "117_hr_2", "117_hr_3", "117_hr_4", "117_hr_5"]

    # Check what phases are incomplete
    incomplete_phases = fetcher.get_incomplete_items_for_resume(sample_items)
    fetcher.log_resume_status(incomplete_phases)

    # Demo 3: Simulate interrupted processing and resume
    print("\n3. SIMULATED INTERRUPT AND RESUME")
    print("-" * 50)

    print("Simulating interrupted processing...")
    print("In a real scenario, you could interrupt processing after list data")
    print("is stored but before related data is fetched, then resume later.")

    # Show checkpoint status
    if fetcher.progress_tracker:
        checkpoint = fetcher.progress_tracker.checkpoint
        print(f"\nCheckpoint status:")
        print(f"  Data type: {checkpoint.data_type}")
        print(f"  Status: {checkpoint.status}")
        print(f"  Total items: {checkpoint.total_items}")
        print(f"  Processed items: {checkpoint.processed_items}")
        print(f"  Failed items: {checkpoint.failed_items}")
        print(f"  Last processed: {checkpoint.last_processed_id}")

    # Demo 4: Show phase-specific item checking
    print("\n4. PHASE-SPECIFIC ITEM CHECKING")
    print("-" * 50)

    if fetcher.progress_tracker and sample_items:
        test_item = sample_items[0]

        from src.bicam_collection.libs.checkpoint import ProcessingPhase

        list_done = fetcher.progress_tracker.should_skip_item(
            test_item, ProcessingPhase.MAIN_ITEMS, field_name="list_data"
        )
        full_done = fetcher.progress_tracker.should_skip_item(
            test_item, ProcessingPhase.MAIN_ITEMS, field_name="full_data"
        )
        related_done = fetcher.progress_tracker.should_skip_item(
            test_item, ProcessingPhase.RELATED_ENTITIES
        )

        print(f"Item {test_item} phase completion status:")
        print(f"  List data: {'✓ Done' if list_done else '✗ Needs processing'}")
        print(f"  Full data: {'✓ Done' if full_done else '✗ Needs processing'}")
        print(f"  Related data: {'✓ Done' if related_done else '✗ Needs processing'}")

    print("\n" + "=" * 80)
    print("DEMO COMPLETED")
    print("=" * 80)

    print("\nKEY BENEFITS OF PHASE-SPECIFIC CHECKPOINTS:")
    print("• List data is stored immediately after Phase 1")
    print("• Each phase is tracked independently in the checkpoint database")
    print("• Resume operations can pick up from any incomplete phase")
    print("• No duplicate processing - only missing phases are executed")
    print("• Granular progress tracking allows for precise resumption")

    # Cleanup
    await db_pool.close()


async def main():
    """Main demo function."""
    try:
        await demonstrate_phase_specific_checkpoints()
    except KeyboardInterrupt:
        print("\nDemo interrupted by user")
    except Exception as e:
        logger.error(f"Demo failed: {e}", exc_info=True)


if __name__ == "__main__":
    asyncio.run(main())
