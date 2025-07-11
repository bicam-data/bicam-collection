#!/usr/bin/env python3
"""
Test script to verify the date filtering functionality for amendments
"""

import os
import sys
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# Add the current directory to the path so we can import the processor
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from process_amendments import GenericRawTableProcessor


def test_date_filtering():
    """Test the date filtering functionality"""

    # Database configuration
    db_config = {
        "host": os.getenv("POSTGRESQL_HOST"),
        "port": os.getenv("POSTGRESQL_PORT"),
        "database": os.getenv("POSTGRESQL_DATABASE"),
        "user": os.getenv("POSTGRESQL_USERNAME"),
        "password": os.getenv("POSTGRESQL_PASSWORD"),
    }

    processor = GenericRawTableProcessor(db_config)

    try:
        # Connect to database
        processor.connect()

        # Test the statistics function
        print("Testing amendment statistics with date filtering...")
        stats = processor.get_processing_stats("amendments")

        print(f"Total raw amendments: {stats.get('total_raw_records', 'N/A')}")
        print(f"Already processed: {stats.get('already_processed', 'N/A')}")
        print(
            f"Recent updates (after 2024-11-15T12:08:16Z): {stats.get('recent_updates_records', 'N/A')}"
        )
        print(f"Processing status: {stats.get('processing_status', 'N/A')}")

        # Test processing a small batch to verify the filtering works
        print("\nTesting small batch processing with date filtering...")
        result = processor.process_from_raw_table(
            table_type="amendments", batch_size=10, start_offset=0, max_records=50
        )

        print(f"Processing result: {result}")

        # Test amendment_actions statistics
        print("\nTesting amendment_actions statistics...")
        actions_stats = processor.get_processing_stats("amendment_actions")
        
        print(f"Total raw actions: {actions_stats.get('total_raw_actions', 'N/A')}")
        print(f"Amendment-related actions: {actions_stats.get('amendment_related_actions', 'N/A')}")
        print(f"Already processed: {actions_stats.get('already_processed', 'N/A')}")
        print(f"Unprocessed actions: {actions_stats.get('unprocessed_records', 'N/A')}")
        print(f"Processing status: {actions_stats.get('processing_status', 'N/A')}")

        # Test processing a small batch of amendment_actions
        print("\nTesting small batch processing of amendment_actions...")
        actions_result = processor.process_from_raw_table(
            table_type="amendment_actions",
            batch_size=10,
            start_offset=0,
            max_records=50
        )
        
        print(f"Amendment actions processing result: {actions_result}")

    except Exception as e:
        print(f"Error during testing: {e}")
        sys.exit(1)
    finally:
        processor.disconnect()


if __name__ == "__main__":
    test_date_filtering()
