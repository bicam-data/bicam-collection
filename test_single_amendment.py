#!/usr/bin/env python3
"""
Test Single Amendment Processing

This script tests processing a single amendment through the full pipeline
to identify exactly where the error occurs.
"""

import os
import sys
import logging
import json
import psycopg2
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class SingleAmendmentTester:
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

    def test_single_amendment(self):
        """Test processing a single amendment through the full pipeline"""
        try:
            with self.connection.cursor() as cursor:
                # Get one unprocessed amendment
                cursor.execute("""
                    SELECT payload FROM bicam_raw_congressional.amendments_raw 
                    WHERE source_doc_id NOT IN (SELECT amendment_id FROM bicam.amendments)
                    LIMIT 1
                """)
                result = cursor.fetchone()

                if not result:
                    logger.error("No unprocessed amendments found")
                    return

                payload = result[0]
                logger.info(f"Testing amendment: {payload}")

                # Test step 1: Extract amendment data
                logger.info("\n=== Step 1: Testing extract_amendment_data ===")
                try:
                    cursor.execute(
                        """
                        SELECT * FROM extract_amendment_data(%s)
                    """,
                        (json.dumps(payload),),
                    )
                    amendment_data = cursor.fetchone()
                    logger.info(
                        f"Amendment data extracted successfully: {amendment_data}"
                    )
                except Exception as e:
                    logger.error(f"Error in extract_amendment_data: {e}")
                    return

                # Test step 2: Extract sponsors
                logger.info("\n=== Step 2: Testing extract_amendment_sponsors ===")
                try:
                    cursor.execute(
                        """
                        SELECT * FROM extract_amendment_sponsors(%s)
                    """,
                        (json.dumps(payload),),
                    )
                    sponsors = cursor.fetchall()
                    logger.info(f"Sponsors extracted: {sponsors}")
                except Exception as e:
                    logger.error(f"Error in extract_amendment_sponsors: {e}")

                # Test step 3: Extract amended bills
                logger.info("\n=== Step 3: Testing extract_amended_bills ===")
                try:
                    cursor.execute(
                        """
                        SELECT * FROM extract_amended_bills(%s)
                    """,
                        (json.dumps(payload),),
                    )
                    bills = cursor.fetchall()
                    logger.info(f"Amended bills extracted: {bills}")
                except Exception as e:
                    logger.error(f"Error in extract_amended_bills: {e}")

                # Test step 4: Extract amended amendments
                logger.info("\n=== Step 4: Testing extract_amended_amendments ===")
                try:
                    cursor.execute(
                        """
                        SELECT * FROM extract_amended_amendments(%s)
                    """,
                        (json.dumps(payload),),
                    )
                    amendments = cursor.fetchall()
                    logger.info(f"Amended amendments extracted: {amendments}")
                except Exception as e:
                    logger.error(f"Error in extract_amended_amendments: {e}")

                # Test step 5: Extract amended treaties
                logger.info("\n=== Step 5: Testing extract_amended_treaties ===")
                try:
                    cursor.execute(
                        """
                        SELECT * FROM extract_amended_treaties(%s)
                    """,
                        (json.dumps(payload),),
                    )
                    treaties = cursor.fetchall()
                    logger.info(f"Amended treaties extracted: {treaties}")
                except Exception as e:
                    logger.error(f"Error in extract_amended_treaties: {e}")

                # Test step 6: Manual insertion step by step
                logger.info("\n=== Step 6: Testing manual insertion ===")
                try:
                    # Insert main amendment record
                    amendment_id = amendment_data[0]  # First field is amendment_id
                    cursor.execute(
                        """
                        INSERT INTO staging_congressional.amendments (
                            amendment_id, amendment_type, amendment_number, congress,
                            chamber, purpose, description, proposed_at, submitted_at,
                            is_bill_amendment, is_treaty_amendment, is_amendment_amendment,
                            notes, actions_count, cosponsors_count, amendments_to_amendment_count,
                            updated_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                        amendment_data,
                    )
                    logger.info("Main amendment record inserted successfully")

                    # Insert sponsors
                    for sponsor in sponsors:
                        cursor.execute(
                            """
                            INSERT INTO staging_congressional.amendments_sponsors (
                                amendment_id, bioguide_id
                            ) VALUES (%s, %s)
                        """,
                            sponsor,
                        )
                    logger.info(f"Sponsors inserted: {len(sponsors)}")

                    # Insert amended bills
                    for bill in bills:
                        cursor.execute(
                            """
                            INSERT INTO staging_congressional.amendments_amended_bills (
                                amendment_id, bill_id
                            ) VALUES (%s, %s)
                        """,
                            bill,
                        )
                    logger.info(f"Amended bills inserted: {len(bills)}")

                    # Insert amended amendments
                    for amendment in amendments:
                        cursor.execute(
                            """
                            INSERT INTO staging_congressional.amendments_amended_amendments (
                                amendment_id, amended_amendment_id
                            ) VALUES (%s, %s)
                        """,
                            amendment,
                        )
                    logger.info(f"Amended amendments inserted: {len(amendments)}")

                    # Insert amended treaties
                    for treaty in treaties:
                        cursor.execute(
                            """
                            INSERT INTO staging_congressional.amendments_amended_treaties (
                                amendment_id, treaty_id
                            ) VALUES (%s, %s)
                        """,
                            treaty,
                        )
                    logger.info(f"Amended treaties inserted: {len(treaties)}")

                    self.connection.commit()
                    logger.info("All records inserted successfully!")

                    # Clean up
                    cursor.execute(
                        "DELETE FROM staging_congressional.amendments_amended_treaties WHERE amendment_id = %s",
                        (amendment_id,),
                    )
                    cursor.execute(
                        "DELETE FROM staging_congressional.amendments_amended_amendments WHERE amendment_id = %s",
                        (amendment_id,),
                    )
                    cursor.execute(
                        "DELETE FROM staging_congressional.amendments_amended_bills WHERE amendment_id = %s",
                        (amendment_id,),
                    )
                    cursor.execute(
                        "DELETE FROM staging_congressional.amendments_sponsors WHERE amendment_id = %s",
                        (amendment_id,),
                    )
                    cursor.execute(
                        "DELETE FROM staging_congressional.amendments WHERE amendment_id = %s",
                        (amendment_id,),
                    )
                    self.connection.commit()
                    logger.info("Test records cleaned up")

                except Exception as e:
                    logger.error(f"Error in manual insertion: {e}")
                    self.connection.rollback()

        except Exception as e:
            logger.error(f"Error in test_single_amendment: {e}")
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

    tester = SingleAmendmentTester(db_config)

    try:
        tester.connect()
        tester.test_single_amendment()
    except Exception as e:
        logger.error(f"Testing failed: {e}")
        sys.exit(1)
    finally:
        tester.disconnect()


if __name__ == "__main__":
    main()
