"""
Example: Bills Cleaner with Database Storage

This example demonstrates how to use the BillsCleaner with database storage
in a Dagster pipeline context. The cleaner handles:
1. Data cleaning and validation
2. Direct storage in bicam_congressional schema
"""

import asyncio
import asyncpg
from datetime import datetime

from src.bicam_collection.api_clients import CongressionalAPIClient
from src.bicam_collection.data_types.congressional.bills.cleaner import BillsCleaner
from src.bicam_collection.data_types.congressional.bills.fetcher import BillsFetcher


async def example_clean_and_store_workflow():
    """
    Example workflow showing how to use the cleaner with database storage.

    This simulates what would happen in a Dagster op/asset:
    1. Fetch raw data (from API or previous Dagster asset)
    2. Clean and store the data using BillsCleaner
    """

    # Database configuration
    db_config = {
        "host": "localhost",
        "port": 5432,
        "database": "bicam",
        "user": "postgres",
        "password": "password",
    }

    # Create database pool
    db_pool = await asyncpg.create_pool(**db_config)

    try:
        # Initialize client and fetcher for data retrieval
        client = CongressionalAPIClient()
        fetcher = BillsFetcher(
            client, None
        )  # No DB pool for fetcher, just data retrieval

        # Initialize checkpoint manager for progress tracking
        from src.bicam_collection.libs.checkpoint import CheckpointManager

        checkpoint_manager = CheckpointManager("data/checkpoints/bills_example.db")

        # Initialize cleaner with database pool and checkpoint manager for storage
        cleaner = BillsCleaner(
            config_path="configs/congressional_config.yaml",
            db_pool=db_pool,
            checkpoint_manager=checkpoint_manager,
        )

        print("🚀 Starting bills cleaning and storage workflow...")

        # Example 1: Process a single bill
        await process_single_bill_example(fetcher, cleaner)

        # Example 2: Process multiple bills
        await process_multiple_bills_example(fetcher, cleaner)

        print("✅ Workflow completed successfully!")

    finally:
        await db_pool.close()


async def process_single_bill_example(fetcher: BillsFetcher, cleaner: BillsCleaner):
    """Example: Process a single bill through the cleaning pipeline."""

    print("\n📋 Example 1: Processing single bill")

    # Simulate fetching a single bill (this would come from a previous Dagster asset)
    async for batch in fetcher.retrieve_list_data(limit=1):
        if batch:
            list_item = batch[0]
            bill_url = list_item.get("url")

            if bill_url:
                print(f"   📦 Fetching full data from: {bill_url}")

                # Get full bill data
                full_data = await fetcher.retrieve_full_data(bill_url)
                if not full_data:
                    print("   ❌ Failed to fetch full data")
                    return

                # Get related data
                print("   🔗 Fetching related data...")
                related_data = await fetcher.retrieve_all_related_data(full_data)

                # Clean and store using the cleaner
                print("   🧹 Cleaning and storing data...")
                success = await cleaner.clean_and_store_data(
                    raw_data=full_data,
                    related_data=related_data,
                    batch_id=f"example_single_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                )

                bill_id = full_data.get("bill_id", "unknown")
                if success:
                    print(f"   ✅ Successfully processed bill: {bill_id}")
                else:
                    print(f"   ❌ Failed to process bill: {bill_id}")
            break


async def process_multiple_bills_example(fetcher: BillsFetcher, cleaner: BillsCleaner):
    """Example: Process multiple bills through the cleaning pipeline."""

    print("\n📋 Example 2: Processing multiple bills")

    batch_id = f"example_batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    processed_count = 0
    max_bills = 5  # Limit for example

    # Process multiple bills
    async for batch in fetcher.retrieve_list_data(limit=max_bills):
        for list_item in batch:
            if processed_count >= max_bills:
                break

            bill_url = list_item.get("url")
            if bill_url:
                try:
                    print(f"   📦 Processing bill {processed_count + 1}/{max_bills}")

                    # Get full bill data
                    full_data = await fetcher.retrieve_full_data(bill_url)
                    if not full_data:
                        continue

                    # Get related data
                    related_data = await fetcher.retrieve_all_related_data(full_data)

                    # Clean and store
                    success = await cleaner.clean_and_store_data(
                        raw_data=full_data, related_data=related_data, batch_id=batch_id
                    )

                    if success:
                        processed_count += 1
                        bill_id = full_data.get("bill_id", "unknown")
                        print(f"   ✅ Processed: {bill_id}")

                except Exception as e:
                    print(f"   ❌ Error processing bill: {str(e)}")
                    continue

        if processed_count >= max_bills:
            break

    print(
        f"   📊 Batch complete: {processed_count} bills processed with batch_id: {batch_id}"
    )


async def dagster_style_example():
    """
    Example showing how this would look in a Dagster pipeline.

    This demonstrates the separation of concerns:
    - Fetcher: Just retrieves raw data
    - Cleaner: Cleans and stores data in bicam_congressional
    """

    print("\n🔧 Dagster-style example (pseudo-code structure)")

    # This would be a Dagster @asset
    def fetch_raw_bills_data():
        """Dagster asset: Fetch raw bills data"""
        # Returns raw data that gets stored in Dagster's asset store
        pass

    # This would be another Dagster @asset that depends on the above
    def clean_and_store_bills_data(raw_bills_data):
        """Dagster asset: Clean and store bills data in bicam_congressional"""
        # Uses BillsCleaner.clean_and_store_data()
        pass

    print("   📝 In Dagster, you would have:")
    print("      - fetch_raw_bills_data() -> Asset with raw data")
    print(
        "      - clean_and_store_bills_data() -> Asset that processes and stores clean data"
    )
    print("      - Each asset can be run independently and has clear dependencies")


if __name__ == "__main__":
    # Run the example
    asyncio.run(example_clean_and_store_workflow())

    # Show Dagster structure
    asyncio.run(dagster_style_example())
