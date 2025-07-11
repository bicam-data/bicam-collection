#!/usr/bin/env python3
"""
Debug Specific Amendment Processing Errors

This script runs detailed diagnostics on the unprocessed amendments
to identify the exact cause of processing errors.
"""

import os
import sys
import logging
import psycopg2
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class SpecificErrorDebugger:
    def __init__(self, db_config):
        self.db_config = db_config
        self.connection = None

    def connect(self):
        try:
            self.connection = psycopg2.connect(**self.db_config)
            logger.info("Database connection established")
        except psycopg2.Error as e:
            logger.error(f"Failed to connect to database: {e}")
            raise

    def disconnect(self):
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed")

    def run_query(self, query, description):
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                results = cursor.fetchall()
                logger.info(f"\n=== {description} ===")
                if results:
                    for result in results:
                        logger.info(f"  {result}")
                else:
                    logger.info("  No results")
                return results
        except Exception as e:
            logger.error(f"Error in {description}: {e}")
            self.connection.rollback()
            return []

    def debug_unprocessed_records(self):
        """Debug the actual unprocessed records"""
        logger.info("Starting detailed debugging of unprocessed amendments...")

        # 1. Look at unprocessed amendments in detail
        query1 = """
        WITH unprocessed_sample AS (
            SELECT 
                source_doc_id,
                payload,
                payload->>'type' as amendment_type,
                payload->>'number' as amendment_number,
                payload->>'congress' as congress,
                payload->>'chamber' as chamber,
                payload->>'purpose' as purpose,
                payload->>'updateDate' as update_date
            FROM bicam_raw_congressional.amendments_raw 
            WHERE source_doc_id NOT IN (SELECT amendment_id FROM bicam.amendments)
            LIMIT 5
        )
        SELECT 
            source_doc_id,
            amendment_type,
            amendment_number,
            congress,
            chamber,
            purpose,
            update_date,
            lower(amendment_type) || amendment_number || '-' || congress as generated_id,
            CASE 
                WHEN source_doc_id = lower(amendment_type) || amendment_number || '-' || congress
                THEN 'Match'
                ELSE 'Mismatch'
            END as id_comparison
        FROM unprocessed_sample
        """
        self.run_query(query1, "Unprocessed Amendment Details")

        # 2. Check for data type issues
        query2 = """
        SELECT 
            source_doc_id,
            payload->>'congress' as congress_raw,
            payload->>'number' as number_raw,
            CASE 
                WHEN payload->>'congress' ~ '^[0-9]+$' THEN 'Valid'
                ELSE 'Invalid: ' || COALESCE(payload->>'congress', 'NULL')
            END as congress_valid,
            CASE 
                WHEN payload->>'number' ~ '^[0-9]+$' THEN 'Valid'
                ELSE 'Invalid: ' || COALESCE(payload->>'number', 'NULL')
            END as number_valid
        FROM bicam_raw_congressional.amendments_raw 
        WHERE source_doc_id NOT IN (SELECT amendment_id FROM bicam.amendments)
          AND (NOT (payload->>'congress' ~ '^[0-9]+$') OR NOT (payload->>'number' ~ '^[0-9]+$'))
        LIMIT 10
        """
        self.run_query(query2, "Data Type Issues")

        # 3. Check for missing required fields
        query3 = """
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
            payload->>'congress' as congress,
            payload->>'chamber' as chamber
        FROM bicam_raw_congressional.amendments_raw 
        WHERE source_doc_id NOT IN (SELECT amendment_id FROM bicam.amendments)
          AND NOT (payload ? 'type' AND payload ? 'number' AND payload ? 'congress')
        LIMIT 10
        """
        self.run_query(query3, "Missing Required Fields")

        # 4. Test manual data extraction
        query4 = """
        WITH test_record AS (
            SELECT payload
            FROM bicam_raw_congressional.amendments_raw 
            WHERE source_doc_id NOT IN (SELECT amendment_id FROM bicam.amendments)
            LIMIT 1
        )
        SELECT 
            lower(payload->>'type') || (payload->>'number')::TEXT || '-' || (payload->>'congress')::TEXT as test_amendment_id,
            payload->>'type' as test_type,
            payload->>'number' as test_number,
            payload->>'congress' as test_congress,
            payload->>'chamber' as test_chamber,
            payload->>'purpose' as test_purpose,
            CASE 
                WHEN payload->>'proposedDate' IS NOT NULL 
                THEN (payload->>'proposedDate')::TIMESTAMP WITH TIME ZONE
                ELSE NULL
            END as test_proposed_date,
            CASE WHEN payload ? 'amendedBill' THEN TRUE ELSE FALSE END as test_is_bill_amendment
        FROM test_record
        """
        self.run_query(query4, "Manual Data Extraction Test")

        # 5. Try to manually insert one record to see the exact error
        logger.info("\n=== Testing Manual Insert ===")
        try:
            with self.connection.cursor() as cursor:
                # Get one unprocessed record
                cursor.execute("""
                    SELECT payload FROM bicam_raw_congressional.amendments_raw 
                    WHERE source_doc_id NOT IN (SELECT amendment_id FROM bicam.amendments)
                    LIMIT 1
                """)
                result = cursor.fetchone()

                if result:
                    payload = result[0]
                    logger.info(f"Testing manual insert with payload: {payload}")

                    # Try to extract data manually
                    amendment_type = payload.get("type")
                    amendment_number = payload.get("number")
                    congress = payload.get("congress")

                    if amendment_type and amendment_number and congress:
                        amendment_id = (
                            f"{amendment_type.lower()}{amendment_number}-{congress}"
                        )
                        logger.info(f"Generated amendment_id: {amendment_id}")

                        # Try to insert manually
                        try:
                            cursor.execute(
                                """
                                INSERT INTO staging_congressional.amendments (
                                    amendment_id, amendment_type, amendment_number, congress
                                ) VALUES (%s, %s, %s, %s)
                            """,
                                (
                                    amendment_id,
                                    amendment_type,
                                    amendment_number,
                                    congress,
                                ),
                            )
                            self.connection.commit()
                            logger.info("Manual insert successful!")

                            # Clean up
                            cursor.execute(
                                "DELETE FROM staging_congressional.amendments WHERE amendment_id = %s",
                                (amendment_id,),
                            )
                            self.connection.commit()
                            logger.info("Test record cleaned up")

                        except Exception as e:
                            logger.error(f"Manual insert failed: {e}")
                            self.connection.rollback()
                    else:
                        logger.error(
                            f"Missing required fields: type={amendment_type}, number={amendment_number}, congress={congress}"
                        )
                else:
                    logger.error("No unprocessed records found")

        except Exception as e:
            logger.error(f"Error in manual insert test: {e}")
            self.connection.rollback()


def main():
    # Database configuration
    db_config = {
        "host": os.getenv("POSTGRESQL_HOST"),
        "port": int(os.getenv("POSTGRESQL_PORT", "5432")),
        "database": os.getenv("POSTGRESQL_DATABASE"),
        "user": os.getenv("POSTGRESQL_USERNAME"),
        "password": os.getenv("POSTGRESQL_PASSWORD"),
    }

    debugger = SpecificErrorDebugger(db_config)

    try:
        debugger.connect()
        debugger.debug_unprocessed_records()
    except Exception as e:
        logger.error(f"Debugging failed: {e}")
        sys.exit(1)
    finally:
        debugger.disconnect()


if __name__ == "__main__":
    main()
