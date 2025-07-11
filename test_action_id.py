#!/usr/bin/env python3
"""
Test Action ID Generation

This script tests the deterministic action_id generation for amendment actions.
"""

import os
import sys
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class ActionIdTester:
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

    def test_action_id_generation(self):
        """Test the deterministic action_id generation"""
        try:
            # Test payload from the examples
            test_payload = {
                "text": "Amendment Passed in Committee of the Whole by Voice Vote. ",
                "type": "Floor",
                "actionCode": "H32111",
                "actionDate": "1987-05-05",
                "sourceSystem": {"code": 2, "name": "House floor actions"},
                "amendments_id": "hamdt71-100",
            }

            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                # Test the extraction function
                cursor.execute(
                    "SELECT * FROM extract_amendment_action_data(%s)",
                    (psycopg2.extras.Json(test_payload),),
                )
                result = cursor.fetchone()

                if result:
                    logger.info(f"Action data extracted: {result}")
                    logger.info(f"Generated action_id: {result['action_id']}")

                    # Test that the same payload generates the same action_id
                    cursor.execute(
                        "SELECT * FROM extract_amendment_action_data(%s)",
                        (psycopg2.extras.Json(test_payload),),
                    )
                    result2 = cursor.fetchone()

                    if result2 and result["action_id"] == result2["action_id"]:
                        logger.info(
                            "✓ Deterministic action_id generation works correctly"
                        )
                    else:
                        logger.error("✗ Action_id is not deterministic!")

                    # Test recorded votes function
                    cursor.execute(
                        "SELECT * FROM extract_amendment_action_recorded_votes(%s)",
                        (psycopg2.extras.Json(test_payload),),
                    )
                    votes = cursor.fetchall()
                    logger.info(f"Recorded votes extracted: {votes}")

                    # Test with a payload that has recorded votes
                    test_payload_with_votes = {
                        "text": "Amendment Passed in Committee of the Whole by Voice Vote. ",
                        "type": "Floor",
                        "actionCode": "H32111",
                        "actionDate": "1987-05-05",
                        "sourceSystem": {"code": 2, "name": "House floor actions"},
                        "amendments_id": "hamdt71-100",
                        "recordedVotes": [
                            {
                                "url": "https://example.com/vote/123",
                                "date": "1987-05-05T10:00:00Z",
                                "chamber": "House",
                                "congress": 100,
                                "rollNumber": 123,
                                "sessionNumber": 1,
                            }
                        ],
                    }

                    cursor.execute(
                        "SELECT * FROM extract_amendment_action_recorded_votes(%s)",
                        (psycopg2.extras.Json(test_payload_with_votes),),
                    )
                    votes_with_data = cursor.fetchall()
                    logger.info(
                        f"Recorded votes with data extracted: {votes_with_data}"
                    )

                else:
                    logger.error("No result from extract_amendment_action_data")

        except Exception as e:
            logger.error(f"Error in test_action_id_generation: {e}")
            raise


def main():
    # Database configuration
    db_config = {
        "host": os.getenv("POSTGRESQL_HOST"),
        "port": int(os.getenv("POSTGRESQL_PORT", "5432")),
        "database": os.getenv("POSTGRESQL_DATABASE"),
        "user": os.getenv("POSTGRESQL_USERNAME"),
        "password": os.getenv("POSTGRESQL_PASSWORD"),
    }

    tester = ActionIdTester(db_config)

    try:
        tester.connect()
        tester.test_action_id_generation()
    except Exception as e:
        logger.error(f"Testing failed: {e}")
        sys.exit(1)
    finally:
        tester.disconnect()


if __name__ == "__main__":
    main()
