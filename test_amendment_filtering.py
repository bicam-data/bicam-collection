#!/usr/bin/env python3
"""
Test script to verify amendment actions filtering works correctly.
"""

import os
import sys
import logging
from typing import Dict, Any
import psycopg2
from psycopg2.extras import RealDictCursor

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def connect_to_db() -> psycopg2.extensions.connection:
    """Connect to the database using environment variables."""
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        database=os.getenv("DB_NAME", "bicam"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", ""),
    )


def test_amendment_filtering():
    """Test the amendment actions filtering logic."""
    try:
        connection = connect_to_db()

        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            # Get total counts
            cursor.execute(
                "SELECT COUNT(*) as total FROM bicam_raw_congressional.amendments_actions_raw"
            )
            total_actions = cursor.fetchone()["total"]

            cursor.execute(
                "SELECT COUNT(*) as total FROM staging_congressional.amendments"
            )
            total_amendments = cursor.fetchone()["total"]

            # Get amendment-related actions count
            cursor.execute("""
                SELECT COUNT(*) as amendment_actions 
                FROM bicam_raw_congressional.amendments_actions_raw 
                WHERE source_doc_id IN (SELECT amendment_id FROM staging_congressional.amendments)
            """)
            amendment_related_actions = cursor.fetchone()["amendment_actions"]

            # Get processed actions count
            cursor.execute(
                "SELECT COUNT(*) as processed FROM staging_congressional.amendments_actions"
            )
            processed_actions = cursor.fetchone()["processed"]

            # Test the statistics function
            cursor.execute("SELECT * FROM get_amendment_actions_stats()")
            stats = cursor.fetchone()

            logger.info("=== Amendment Actions Filtering Test ===")
            logger.info(f"Total raw actions: {total_actions}")
            logger.info(f"Total amendments: {total_amendments}")
            logger.info(f"Amendment-related actions: {amendment_related_actions}")
            logger.info(f"Processed actions: {processed_actions}")
            logger.info(
                f"Filtering ratio: {amendment_related_actions / total_actions * 100:.1f}%"
            )

            logger.info("\n=== Statistics Function Results ===")
            logger.info(f"Total raw actions: {stats['total_raw_actions']}")
            logger.info(
                f"Amendment-related actions: {stats['amendment_related_actions']}"
            )
            logger.info(f"Processed actions: {stats['processed_actions']}")
            logger.info(
                f"Unprocessed amendment actions: {stats['unprocessed_amendment_actions']}"
            )
            logger.info(f"Status: {stats['processing_status']}")

            # Test a few sample records to see what gets filtered
            logger.info("\n=== Sample Records Analysis ===")

            # Get a few non-amendment actions
            cursor.execute("""
                SELECT source_doc_id, payload->>'text' as action_text
                FROM bicam_raw_congressional.amendments_actions_raw
                WHERE source_doc_id NOT IN (SELECT amendment_id FROM staging_congressional.amendments)
                LIMIT 3
            """)
            non_amendment_actions = cursor.fetchall()

            logger.info(f"Sample non-amendment actions ({len(non_amendment_actions)}):")
            for action in non_amendment_actions:
                logger.info(
                    f"  source_doc_id: {action['source_doc_id']}, text: {action['action_text'][:50]}..."
                )

            # Get a few amendment actions
            cursor.execute("""
                SELECT source_doc_id, payload->>'text' as action_text
                FROM bicam_raw_congressional.amendments_actions_raw 
                WHERE source_doc_id IN (SELECT amendment_id FROM staging_congressional.amendments)
                LIMIT 3
            """)
            amendment_actions = cursor.fetchall()

            logger.info(f"\nSample amendment actions ({len(amendment_actions)}):")
            for action in amendment_actions:
                logger.info(
                    f"  source_doc_id: {action['source_doc_id']}, text: {action['action_text'][:50]}..."
                )

            connection.close()

    except Exception as e:
        logger.error(f"Error testing amendment filtering: {e}")
        raise


if __name__ == "__main__":
    test_amendment_filtering()
