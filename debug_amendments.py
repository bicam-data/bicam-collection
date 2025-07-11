#!/usr/bin/env python3
"""
Debug Amendment Processing Errors

This script runs diagnostic queries to identify why amendment processing is failing.
"""

import argparse
import logging
import os
import sys

import psycopg2
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class AmendmentDebugger:
    def __init__(self, db_config: dict[str, str]):
        """
        Initialize the debugger with database configuration

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

    def run_diagnostic_query(self, query: str, description: str) -> list:
        """Run a diagnostic query and return results"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                results = cursor.fetchall()
                logger.info(f"{description}: {len(results)} results")
                return results
        except Exception as e:
            logger.error(f"Error running query '{description}': {e}")
            # Reset transaction on error
            self.connection.rollback()
            return []

    def debug_raw_data_structure(self):
        """Check the basic structure of raw data"""
        logger.info("=== Checking Raw Data Structure ===")

        query = """
        SELECT 
            COUNT(*) as total_records,
            COUNT(CASE WHEN payload IS NULL THEN 1 END) as null_payloads,
            COUNT(CASE WHEN source_doc_id IS NULL THEN 1 END) as null_source_doc_id
        FROM bicam_raw_congressional.amendments_raw
        """

        results = self.run_diagnostic_query(query, "Raw data structure")
        if results:
            total, null_payloads, null_ids = results[0]
            logger.info(f"Total records: {total}")
            logger.info(f"Null payloads: {null_payloads}")
            logger.info(f"Null source_doc_id: {null_ids}")

    def debug_json_parsing(self):
        """Check for JSON parsing issues"""
        logger.info("=== Checking JSON Parsing ===")

        query = """
        SELECT 
            source_doc_id,
            CASE 
                WHEN payload::text IS NULL THEN 'NULL payload'
                WHEN jsonb_typeof(payload) != 'object' THEN 'Not an object: ' || jsonb_typeof(payload)
                ELSE 'Valid JSON'
            END as json_status
        FROM bicam_raw_congressional.amendments_raw 
        WHERE payload::text IS NULL OR jsonb_typeof(payload) != 'object'
        LIMIT 10
        """

        results = self.run_diagnostic_query(query, "JSON parsing issues")
        for result in results:
            logger.info(f"  {result[0]}: {result[1]}")

    def debug_missing_fields(self):
        """Check for missing required fields"""
        logger.info("=== Checking Missing Required Fields ===")

        query = """
        SELECT 
            source_doc_id,
            CASE 
                WHEN NOT (payload ? 'type') THEN 'Missing type'
                WHEN NOT (payload ? 'number') THEN 'Missing number'
                WHEN NOT (payload ? 'congress') THEN 'Missing congress'
                WHEN NOT (payload ? 'chamber') THEN 'Missing chamber'
                ELSE 'All required fields present'
            END as missing_fields,
            payload->>'type' as amendment_type,
            payload->>'number' as amendment_number,
            payload->>'congress' as congress
        FROM bicam_raw_congressional.amendments_raw 
        WHERE NOT (payload ? 'type' AND payload ? 'number' AND payload ? 'congress')
        LIMIT 10
        """

        results = self.run_diagnostic_query(query, "Missing required fields")
        for result in results:
            logger.info(
                f"  {result[0]}: {result[1]} (type: {result[2]}, number: {result[3]}, congress: {result[4]})"
            )

    def debug_id_generation(self):
        """Test ID generation for sample records"""
        logger.info("=== Testing ID Generation ===")

        query = """
        WITH sample_data AS (
            SELECT 
                source_doc_id,
                payload,
                payload->>'type' as amendment_type,
                payload->>'number' as amendment_number,
                payload->>'congress' as congress
            FROM bicam_raw_congressional.amendments_raw 
            WHERE payload ? 'type' AND payload ? 'number' AND payload ? 'congress'
            LIMIT 5
        )
        SELECT 
            source_doc_id,
            amendment_type,
            amendment_number,
            congress,
            lower(amendment_type) || amendment_number || '-' || congress as generated_id,
            CASE 
                WHEN EXISTS (SELECT 1 FROM bicam.amendments WHERE amendment_id = lower(amendment_type) || amendment_number || '-' || congress)
                THEN 'Already exists'
                ELSE 'New'
            END as status
        FROM sample_data
        """

        results = self.run_diagnostic_query(query, "ID generation test")
        for result in results:
            logger.info(f"  Source ID: {result[0]}")
            logger.info(
                f"    Type: {result[1]}, Number: {result[2]}, Congress: {result[3]}"
            )
            logger.info(f"    Generated ID: {result[4]}")
            logger.info(f"    Status: {result[5]}")
            logger.info("")

    def debug_data_types(self):
        """Check for data type issues"""
        logger.info("=== Checking Data Type Issues ===")

        query = """
        SELECT 
            source_doc_id,
            payload->>'congress' as congress_raw,
            CASE 
                WHEN payload->>'congress' ~ '^[0-9]+$' THEN 'Valid integer'
                ELSE 'Invalid integer: ' || COALESCE(payload->>'congress', 'NULL')
            END as congress_validation,
            payload->>'number' as number_raw,
            CASE 
                WHEN payload->>'number' ~ '^[0-9]+$' THEN 'Valid integer'
                ELSE 'Invalid integer: ' || COALESCE(payload->>'number', 'NULL')
            END as number_validation
        FROM bicam_raw_congressional.amendments_raw 
        WHERE NOT (payload->>'congress' ~ '^[0-9]+$' AND payload->>'number' ~ '^[0-9]+$')
        LIMIT 10
        """

        results = self.run_diagnostic_query(query, "Data type issues")
        for result in results:
            logger.info(
                f"  {result[0]}: congress={result[1]} ({result[2]}), number={result[3]} ({result[4]})"
            )

    def debug_id_mismatch(self):
        """Check for mismatches between source_doc_id and generated amendment_id"""
        logger.info("=== Checking ID Mismatches ===")

        query = """
        SELECT 
            source_doc_id,
            payload->>'type' as amendment_type,
            payload->>'number' as amendment_number,
            payload->>'congress' as congress,
            lower(payload->>'type') || (payload->>'number')::TEXT || '-' || (payload->>'congress')::TEXT as generated_id,
            CASE 
                WHEN source_doc_id = lower(payload->>'type') || (payload->>'number')::TEXT || '-' || (payload->>'congress')::TEXT
                THEN 'Match'
                ELSE 'Mismatch'
            END as id_match
        FROM bicam_raw_congressional.amendments_raw 
        WHERE payload ? 'type' AND payload ? 'number' AND payload ? 'congress'
        LIMIT 10
        """

        results = self.run_diagnostic_query(query, "ID mismatches")
        for result in results:
            logger.info(f"  Source ID: {result[0]}")
            logger.info(f"    Generated ID: {result[4]}")
            logger.info(f"    Match: {result[5]}")
            logger.info("")

    def debug_processing_stats(self):
        """Check processing statistics"""
        logger.info("=== Processing Statistics ===")

        query = """
        SELECT 
            COUNT(*) as total_raw,
            COUNT(CASE WHEN source_doc_id NOT IN (SELECT amendment_id FROM bicam.amendments) THEN 1 END) as unprocessed,
            COUNT(CASE WHEN source_doc_id IN (SELECT amendment_id FROM bicam.amendments) THEN 1 END) as already_processed
        FROM bicam_raw_congressional.amendments_raw
        """

        results = self.run_diagnostic_query(query, "Processing statistics")
        if results:
            total, unprocessed, processed = results[0]
            logger.info(f"Total raw records: {total}")
            logger.info(f"Unprocessed: {unprocessed}")
            logger.info(f"Already processed: {processed}")

    def debug_amendment_types(self):
        """Check distribution of amendment types"""
        logger.info("=== Amendment Type Distribution ===")

        query = """
        SELECT 
            payload->>'type' as amendment_type,
            COUNT(*) as count
        FROM bicam_raw_congressional.amendments_raw 
        WHERE payload ? 'type'
        GROUP BY payload->>'type'
        ORDER BY count DESC
        LIMIT 10
        """

        results = self.run_diagnostic_query(query, "Amendment type distribution")
        for result in results:
            logger.info(f"  {result[0]}: {result[1]} records")

    def run_all_diagnostics(self):
        """Run all diagnostic checks"""
        logger.info("Starting comprehensive amendment processing diagnostics...")

        self.debug_raw_data_structure()
        self.debug_json_parsing()
        self.debug_missing_fields()
        self.debug_id_generation()
        self.debug_data_types()
        self.debug_id_mismatch()
        self.debug_processing_stats()
        self.debug_amendment_types()

        logger.info("Diagnostics complete!")


def main():
    parser = argparse.ArgumentParser(description="Debug amendment processing errors")
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

    args = parser.parse_args()

    # Database configuration
    db_config = {
        "host": args.db_host,
        "port": args.db_port,
        "database": args.db_name,
        "user": args.db_user,
        "password": args.db_password,
    }

    debugger = AmendmentDebugger(db_config)

    try:
        # Connect to database
        debugger.connect()

        # Run all diagnostics
        debugger.run_all_diagnostics()

    except Exception as e:
        logger.error(f"Debugging failed: {e}")
        sys.exit(1)
    finally:
        debugger.disconnect()


if __name__ == "__main__":
    main()
