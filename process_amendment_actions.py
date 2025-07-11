#!/usr/bin/env python3
"""
Process Amendment Actions from Raw Table

This script processes amendment actions from bicam_raw_congressional.amendments_actions_raw
and inserts them into the staging_congressional schema tables.
"""

import os
import sys
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import argparse

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class AmendmentActionsProcessor:
    def __init__(self, db_config):
        self.db_config = db_config
        self.connection = None

    def connect(self):
        """Establish database connection"""
        try:
            self.connection = psycopg2.connect(**self.db_config)
            logger.info("Database connection established")
        except psycopg2.Error as e:
            logger.error(f"Failed to connect to database: {e}")
            raise

    def disconnect(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed")

    def load_sql_functions(self):
        """Load SQL functions from file"""
        try:
            with open("convert_amendments_json_to_sql.sql", "r") as f:
                sql_content = f.read()

            with self.connection.cursor() as cursor:
                cursor.execute(sql_content)
                self.connection.commit()
                logger.info("SQL functions loaded successfully")
        except Exception as e:
            logger.error(f"Error loading SQL functions: {e}")
            raise

    def get_stats(self):
        """Get statistics about amendment actions"""
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("SELECT * FROM get_amendment_actions_stats()")
                stats = cursor.fetchone()
                return stats
        except Exception as e:
            logger.error(f"Error getting stats: {e}")
            raise

    def process_actions(self, batch_size=1000, start_offset=0, max_records=None):
        """Process amendment actions from raw table"""
        try:
            with self.connection.cursor() as cursor:
                # Call the stored procedure
                cursor.callproc(
                    "process_amendment_actions_from_raw_table",
                    [batch_size, start_offset, max_records],
                )
                self.connection.commit()
                logger.info("Amendment actions processing completed")
        except Exception as e:
            logger.error(f"Error processing amendment actions: {e}")
            self.connection.rollback()
            raise

    def test_single_action(self):
        """Test processing a single amendment action"""
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                # Get one action record
                cursor.execute("""
                    SELECT payload FROM bicam_raw_congressional.amendments_actions_raw 
                    LIMIT 1
                """)
                result = cursor.fetchone()

                if not result:
                    logger.error("No amendment actions found")
                    return

                payload = result[0]
                logger.info(f"Testing action: {payload}")

                # Test the extraction function
                cursor.execute(
                    "SELECT * FROM extract_amendment_action_data(%s)", (payload,)
                )
                action_data = cursor.fetchone()
                logger.info(f"Action data extracted: {action_data}")

                # Test recorded votes extraction
                cursor.execute(
                    "SELECT * FROM extract_amendment_action_recorded_votes(%s)",
                    (payload,),
                )
                votes = cursor.fetchall()
                logger.info(f"Recorded votes extracted: {votes}")

        except Exception as e:
            logger.error(f"Error in test_single_action: {e}")
            raise


def main():
    parser = argparse.ArgumentParser(
        description="Process amendment actions from raw table"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Batch size for processing (default: 1000)",
    )
    parser.add_argument(
        "--start-offset",
        type=int,
        default=0,
        help="Starting offset for processing (default: 0)",
    )
    parser.add_argument(
        "--max-records",
        type=int,
        default=None,
        help="Maximum number of records to process (default: all)",
    )
    parser.add_argument("--stats", action="store_true", help="Show statistics only")
    parser.add_argument(
        "--test", action="store_true", help="Test with a single action record"
    )
    parser.add_argument(
        "--reload-functions",
        action="store_true",
        help="Reload SQL functions before processing",
    )

    args = parser.parse_args()

    # Database configuration
    db_config = {
        "host": os.getenv("POSTGRESQL_HOST"),
        "port": int(os.getenv("POSTGRESQL_PORT", "5432")),
        "database": os.getenv("POSTGRESQL_DATABASE"),
        "user": os.getenv("POSTGRESQL_USERNAME"),
        "password": os.getenv("POSTGRESQL_PASSWORD"),
    }

    processor = AmendmentActionsProcessor(db_config)

    try:
        processor.connect()

        if args.reload_functions:
            logger.info("Reloading SQL functions...")
            processor.load_sql_functions()

        if args.stats:
            stats = processor.get_stats()
            logger.info("Amendment Actions Statistics:")
            logger.info(f"  Total raw actions: {stats['total_raw_actions']}")
            logger.info(f"  Processed actions: {stats['processed_actions']}")
            logger.info(f"  Unprocessed actions: {stats['unprocessed_actions']}")
            logger.info(f"  Status: {stats['processing_status']}")
            return

        if args.test:
            logger.info("Testing single action processing...")
            processor.test_single_action()
            return

        # Show stats before processing
        stats = processor.get_stats()
        logger.info("Pre-processing statistics:")
        logger.info(f"  Total raw actions: {stats['total_raw_actions']}")
        logger.info(f"  Processed actions: {stats['processed_actions']}")

        # Process actions
        logger.info(
            f"Processing amendment actions with batch_size={args.batch_size}, "
            f"start_offset={args.start_offset}, max_records={args.max_records}"
        )

        processor.process_actions(
            batch_size=args.batch_size,
            start_offset=args.start_offset,
            max_records=args.max_records,
        )

        # Show stats after processing
        stats = processor.get_stats()
        logger.info("Post-processing statistics:")
        logger.info(f"  Total raw actions: {stats['total_raw_actions']}")
        logger.info(f"  Processed actions: {stats['processed_actions']}")

    except Exception as e:
        logger.error(f"Processing failed: {e}")
        sys.exit(1)
    finally:
        processor.disconnect()


if __name__ == "__main__":
    main()
