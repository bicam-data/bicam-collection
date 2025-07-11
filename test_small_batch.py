#!/usr/bin/env python3
"""
Test Small Batch Processing

This script tests processing a small batch of unprocessed amendments
to identify specific errors.
"""

import os
import sys
import logging
from process_amendments import AmendmentProcessor
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    # Database configuration
    db_config = {
        "host": os.getenv("POSTGRESQL_HOST"),
        "port": int(os.getenv("POSTGRESQL_PORT", "5432")),
        "database": os.getenv("POSTGRESQL_DATABASE"),
        "user": os.getenv("POSTGRESQL_USERNAME"),
        "password": os.getenv("POSTGRESQL_PASSWORD"),
    }

    processor = AmendmentProcessor(db_config)

    try:
        # Connect to database
        processor.connect()

        # Load SQL functions
        processor.load_sql_functions()

        # Get unprocessed stats first
        logger.info("=== Current Processing Status ===")
        unprocessed_stats = processor.get_unprocessed_stats()
        for key, value in unprocessed_stats.items():
            logger.info(f"  {key}: {value}")

        # Test processing a small batch
        logger.info("\n=== Testing Small Batch Processing ===")
        result = processor.process_from_raw_table(
            batch_size=10, start_offset=0, max_records=10
        )

        logger.info("Small batch processing results:")
        for key, value in result.items():
            logger.info(f"  {key}: {value}")

        # Get updated stats
        logger.info("\n=== Updated Processing Status ===")
        updated_stats = processor.get_unprocessed_stats()
        for key, value in updated_stats.items():
            logger.info(f"  {key}: {value}")

    except Exception as e:
        logger.error(f"Testing failed: {e}")
        sys.exit(1)
    finally:
        processor.disconnect()


if __name__ == "__main__":
    main()
