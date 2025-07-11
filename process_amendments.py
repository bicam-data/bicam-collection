#!/usr/bin/env python3
"""
Generic Raw Table Processor

This script processes data from various raw tables and converts it to SQL tables
using the functions defined in convert_amendments_json_to_sql.sql

For amendments, the script now filters by updateDate to process only amendments
with updates after 2024-11-15T12:08:16Z, rather than checking for unprocessed records.
"""

import argparse
import logging
import os
import sys
from typing import Any

import psycopg2
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class GenericRawTableProcessor:
    def __init__(self, db_config: dict[str, str]):
        """
        Initialize the processor with database configuration

        Args:
            db_config: Dictionary with database connection parameters
        """
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

    def load_sql_functions(self, sql_file: str = "convert_amendments_json_to_sql.sql"):
        """Load the SQL functions from file"""
        try:
            with open(sql_file) as f:
                sql_content = f.read()

            with self.connection.cursor() as cursor:
                cursor.execute(sql_content)
                self.connection.commit()
                logger.info("SQL functions loaded successfully")
        except Exception as e:
            logger.error(f"Failed to load SQL functions: {e}")
            raise

    def get_table_config(self, table_type: str) -> dict[str, Any]:
        """
        Get configuration for a specific table type

        Args:
            table_type: Type of table to process ('amendments', 'amendment_actions', etc.)

        Returns:
            Dictionary with table configuration
        """
        configs = {
            "amendments": {
                "raw_table": "bicam_raw_congressional.amendments_raw",
                "processed_table": "bicam.amendments",
                "id_column": "amendment_id",
                "source_id_column": "source_doc_id",
                "process_function": "process_amendments_from_raw_table",
                "stats_function": "get_unprocessed_amendment_stats",
                "test_function": "extract_amendment_data",
            },
            "amendment_actions": {
                "raw_table": "bicam_raw_congressional.amendments_actions_raw",
                "processed_table": "staging_congressional.amendments_actions",
                "id_column": "amendment_id",
                "source_id_column": "source_doc_id",
                "process_function": "process_amendment_actions_from_raw_table",
                "stats_function": "get_amendment_actions_stats",
                "test_function": "extract_amendment_action_data",
            },
        }

        if table_type not in configs:
            raise ValueError(
                f"Unsupported table type: {table_type}. Supported types: {list(configs.keys())}"
            )

        return configs[table_type]

    def process_from_raw_table(
        self,
        table_type: str,
        batch_size: int = 1000,
        start_offset: int = 0,
        max_records: int = None,
    ):
        """
        Process data directly from the raw table

        Args:
            table_type: Type of table to process
            batch_size: Number of records to process in each batch
            start_offset: Starting offset for processing
            max_records: Maximum number of records to process (None for all)
        """
        try:
            config = self.get_table_config(table_type)

            with self.connection.cursor() as cursor:
                # Call the appropriate SQL function to process from raw table
                if table_type == "amendments":
                    cursor.execute(
                        """
                        SELECT * FROM process_amendments_from_raw_table(%s, %s, %s)
                    """,
                        (batch_size, start_offset, max_records),
                    )
                    result = cursor.fetchone()
                    self.connection.commit()

                    if result:
                        processed, errors, total = result
                        logger.info(
                            f"Processing complete: {processed} processed, {errors} errors, {total} total records"
                        )
                        return {
                            "processed": processed,
                            "errors": errors,
                            "total": total,
                        }
                    else:
                        logger.warning("No results returned from processing")
                        return {}

                elif table_type == "amendment_actions":
                    # Call the procedure using CALL statement
                    cursor.execute(
                        f"CALL {config['process_function']}(%s, %s, %s)",
                        (batch_size, start_offset, max_records),
                    )
                    # For procedures, we don't get a return value, so we'll just log completion
                    logger.info(f"Processing of {table_type} completed")
                    return {"processed": "completed", "errors": 0, "total": "unknown"}

        except Exception as e:
            logger.error(f"Failed to process from raw table: {e}")
            self.connection.rollback()
            raise

    def process_all_records(self, table_type: str):
        """
        Process all records from the raw table
        """
        try:
            config = self.get_table_config(table_type)

            if table_type == "amendments":
                with self.connection.cursor() as cursor:
                    cursor.execute("SELECT * FROM process_all_amendments()")
                    result = cursor.fetchone()
                    self.connection.commit()

                    if result:
                        processed, errors, total = result
                        logger.info(
                            f"All {table_type} processed: {processed} processed, {errors} errors, {total} total records"
                        )
                        return {
                            "processed": processed,
                            "errors": errors,
                            "total": total,
                        }
                    else:
                        logger.warning("No results returned from processing")
                        return {}
            elif table_type == "amendment_actions":
                # For amendment_actions, call the procedure directly
                with self.connection.cursor() as cursor:
                    cursor.execute(f"CALL {config['process_function']}(1000, 0, NULL)")
                    self.connection.commit()
                    logger.info(f"All {table_type} processed")
                    return {"processed": "completed", "errors": 0, "total": "unknown"}
            else:
                # For other table types, just process all records
                return self.process_from_raw_table(
                    table_type, batch_size=1000, start_offset=0, max_records=None
                )

        except Exception as e:
            logger.error(f"Failed to process all {table_type}: {e}")
            self.connection.rollback()
            raise

    def get_processing_stats(self, table_type: str) -> dict[str, Any]:
        """
        Get statistics about records with recent updates

        Args:
            table_type: Type of table to get stats for

        Returns:
            Dictionary with counts of raw, processed, and records with recent updates
        """
        try:
            config = self.get_table_config(table_type)

            with self.connection.cursor() as cursor:
                cursor.execute(f"SELECT * FROM {config['stats_function']}()")
                result = cursor.fetchone()

                if result:
                    if table_type == "amendments":
                        total_raw, already_processed, unprocessed, status = result
                        return {
                            "total_raw_records": total_raw,
                            "already_processed": already_processed,
                            "recent_updates_records": unprocessed,
                            "processing_status": status,
                        }
                    elif table_type == "amendment_actions":
                        total_raw, amendment_related, processed, unprocessed, status = (
                            result
                        )
                        return {
                            "total_raw_records": total_raw,
                            "amendment_related_actions": amendment_related,
                            "already_processed": processed,
                            "unprocessed_records": unprocessed,
                            "processing_status": status,
                        }
                else:
                    logger.warning("No results returned from unprocessed stats")
                    return {}

        except Exception as e:
            logger.error(f"Failed to get unprocessed stats: {e}")
            return {}

    def test_single_record(self, table_type: str):
        """
        Test processing a single record from the raw table

        Args:
            table_type: Type of table to test
        """
        try:
            config = self.get_table_config(table_type)

            with self.connection.cursor() as cursor:
                # Get one record (filter for amendment-related actions if applicable)
                if table_type == "amendment_actions":
                    cursor.execute(
                        f"""
                        SELECT payload FROM {config["raw_table"]} 
                        WHERE source_doc_id IN (SELECT amendment_id FROM staging_congressional.amendments)
                        LIMIT 1
                        """
                    )
                else:
                    cursor.execute(
                        f"""
                        SELECT payload FROM {config["raw_table"]} 
                        LIMIT 1
                        """
                    )
                result = cursor.fetchone()

                if not result:
                    logger.error(f"No {table_type} records found")
                    return

                payload = result[0]
                logger.info(f"Testing {table_type}: {payload}")

                # Convert payload to JSON string if it's a dict
                if isinstance(payload, dict):
                    import json

                    payload_json = json.dumps(payload)
                else:
                    payload_json = payload

                # Test the extraction function
                cursor.execute(
                    f"SELECT * FROM {config['test_function']}(%s)", (payload_json,)
                )
                data = cursor.fetchone()
                logger.info(f"{table_type} data extracted: {data}")

        except Exception as e:
            logger.error(f"Error in test_single_record: {e}")
            raise


def main():
    parser = argparse.ArgumentParser(
        description="Process data from various raw tables into SQL tables"
    )
    parser.add_argument(
        "--table-type",
        required=True,
        choices=["amendments", "amendment_actions"],
        help="Type of table to process",
    )
    parser.add_argument(
        "--db-host", default=os.getenv("POSTGRESQL_HOST"), help="Database host"
    )
    parser.add_argument(
        "--db-port",
        type=int,
        default=os.getenv("POSTGRESQL_PORT"),
        help="Database port",
    )
    parser.add_argument(
        "--db-name", default=os.getenv("POSTGRESQL_DATABASE"), help="Database name"
    )
    parser.add_argument(
        "--db-user", default=os.getenv("POSTGRESQL_USERNAME"), help="Database user"
    )
    parser.add_argument(
        "--db-password",
        default=os.getenv("POSTGRESQL_PASSWORD"),
        help="Database password",
    )
    parser.add_argument(
        "--from-raw-table",
        action="store_true",
        help="Process from raw table",
    )
    parser.add_argument(
        "--process-all",
        action="store_true",
        help="Process all records from raw table",
    )
    parser.add_argument(
        "--batch-size", type=int, default=1000, help="Batch size for processing"
    )
    parser.add_argument(
        "--start-offset",
        type=int,
        default=0,
        help="Starting offset for raw table processing",
    )
    parser.add_argument(
        "--max-records",
        type=int,
        help="Maximum number of records to process from raw table",
    )
    parser.add_argument(
        "--sql-file",
        default="convert_amendments_json_to_sql.sql",
        help="SQL functions file",
    )
    parser.add_argument(
        "--stats",
        action="store_true",
        help="Show statistics for records with recent updates (after 2024-11-15T12:08:16Z)",
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Test with a single record",
    )
    parser.add_argument(
        "--reload-functions",
        action="store_true",
        help="Reload SQL functions before processing",
    )

    args = parser.parse_args()

    # Database configuration
    db_config = {
        "host": args.db_host,
        "port": args.db_port,
        "database": args.db_name,
        "user": args.db_user,
        "password": args.db_password,
    }

    processor = GenericRawTableProcessor(db_config)

    try:
        # Connect to database
        processor.connect()

        # Load SQL functions if requested
        if args.reload_functions:
            logger.info("Loading SQL functions...")
            processor.load_sql_functions(args.sql_file)

        # Process data
        if args.process_all:
            processor.process_all_records(args.table_type)
        elif args.from_raw_table:
            processor.process_from_raw_table(
                table_type=args.table_type,
                batch_size=args.batch_size,
                start_offset=args.start_offset,
                max_records=args.max_records,
            )
        elif args.test:
            processor.test_single_record(args.table_type)
        elif args.stats:
            # Just show stats, no processing needed
            pass
        else:
            logger.error(
                "Must provide one of: --process-all, --from-raw-table, --test, or --stats"
            )
            sys.exit(1)

        # Show statistics if requested
        if args.stats:
            processing_stats = processor.get_processing_stats(args.table_type)
            logger.info(f"{args.table_type} Statistics (filtered by recent updates):")
            for key, value in processing_stats.items():
                logger.info(f"  {key}: {value}")

    except Exception as e:
        logger.error(f"Processing failed: {e}")
        sys.exit(1)
    finally:
        processor.disconnect()


if __name__ == "__main__":
    main()
