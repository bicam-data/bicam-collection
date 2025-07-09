"""
Example of using the optimized split_columns_to_new_table method.

This demonstrates how to use the new method that works with:
- OptimizedStorageManager for efficient data transfer
- OptimizedCleanerStorage for production table operations
- New configuration structure
- Batch processing for large datasets
"""

import asyncio
import logging

from streamlined.plugins.base import CongressionalBaseCleanerLogic

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def example_split_columns():
    """Example of using the optimized column splitting method."""

    # Initialize storage managers (you would get these from your resource coordinator)
    # This is just an example - in practice you'd get these from your coordinator
    storage_manager = None  # OptimizedStorageManager(pg_pool, ...)
    cleaner_storage = None  # OptimizedCleanerStorage(storage_manager, ...)

    # Initialize the utilities class
    utilities = CongressionalBaseCleanerLogic()

    # Example: Split sponsor information from bills table to a separate sponsors table
    try:
        inserted_count = await utilities.split_columns_to_new_table(
            source_table="bills",
            dest_table="bills_sponsors",
            columns={
                "sponsor_id": "sponsor_id",
                "sponsor_name": "sponsor_name",
                "sponsor_state": "sponsor_state",
                "sponsor_district": "sponsor_district",
                "sponsor_party": "sponsor_party",
            },
            data_type="bills",  # For checkpoint tracking
            src_schema="bicam_staging_congressional",  # Staging schema
            dst_schema="bicam_congressional",  # Production schema
            create_if_missing=True,
            drop_from_source=False,  # Keep original columns
            batch_size=1000,
            storage_manager=storage_manager,
            cleaner_storage=cleaner_storage,
        )

        logger.info(
            f"Successfully split {inserted_count} records to bills_sponsors table"
        )

    except Exception as e:
        logger.error(f"Error splitting columns: {e}")


async def example_list_columns():
    """Example of splitting columns using a list (same names)."""

    utilities = CongressionalBaseCleanerLogic()

    # Example: Split action information using list format
    try:
        inserted_count = await utilities.split_columns_to_new_table(
            source_table="bills",
            dest_table="bills_actions",
            columns=[
                "action_date",
                "action_text",
                "action_type",
                "action_by",
            ],
            data_type="bills",
            batch_size=500,
            storage_manager=None,  # Would be provided in real usage
            cleaner_storage=None,  # Would be provided in real usage
        )

        logger.info(
            f"Successfully split {inserted_count} records to bills_actions table"
        )

    except Exception as e:
        logger.error(f"Error splitting columns: {e}")


if __name__ == "__main__":
    # Run examples
    asyncio.run(example_split_columns())
    asyncio.run(example_list_columns())
