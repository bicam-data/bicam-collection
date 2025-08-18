#!/usr/bin/env python3
"""
Direct test of the timeout mechanism to verify it's working correctly.
"""

import asyncio
import os
import sys
import time
import signal
from dotenv import load_dotenv

# Add the src directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

import asyncpg
from streamlined.lobbyist_matching.timeout_handler import (
    TimeoutTracker,
    RegexTimeout,
    timeout_handler,
)
from streamlined.lobbyist_matching.db_utils import FilingSection
from streamlined.lobbyist_matching.schema_setup import initialize_run


def timeout_processing_function():
    """A function that will definitely timeout."""
    # Set up signal-based timeout
    signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(1)  # 1 second timeout

    try:
        # Do something that will take longer than 1 second
        time.sleep(5)  # Sleep for 5 seconds
        return "This should not be reached"
    except RegexTimeout:
        print("✅ Timeout exception caught!")
        raise
    finally:
        signal.alarm(0)


async def test_timeout_mechanism():
    """Test the timeout mechanism directly."""

    # Load environment variables
    load_dotenv()

    # Database configuration
    db_config = {
        "host": os.getenv("POSTGRESQL_HOST"),
        "port": int(os.getenv("POSTGRESQL_PORT", 5432)),
        "user": os.getenv("POSTGRESQL_USERNAME"),
        "password": os.getenv("POSTGRESQL_PASSWORD"),
        "database": os.getenv("POSTGRESQL_DATABASE"),
    }

    try:
        # Create connection pool
        pool = await asyncpg.create_pool(**db_config)

        # Create a test run
        run_id = await initialize_run(
            pool,
            total_filings=1,
            total_sections=1,
            parameters={"description": "Timeout mechanism test", "test": True},
        )

        print(f"Created test run with ID: {run_id}")

        # Initialize timeout tracker
        timeout_tracker = TimeoutTracker(pool)
        await timeout_tracker.initialize_tracking(run_id)

        # Create a test section
        test_section = FilingSection(
            filing_uuid="12345678-1234-1234-1234-123456789abc",
            section_id="test-section-timeout-mechanism",
            text="Test text",
            filing_year=2020,
        )

        print("Testing timeout mechanism...")

        try:
            # This should timeout
            timeout_processing_function()
            print("❌ Function completed without timeout!")
        except RegexTimeout as e:
            print("✅ Timeout exception caught successfully!")

            # Store the timeout
            from streamlined.lobbyist_matching.timeout_handler import TimeoutSection

            timeout_info = TimeoutSection(
                filing_uuid=test_section.filing_uuid,
                section_id=test_section.section_id,
                chunk_id=0,
                start_offset=0,
                pattern_type="test_timeout",
                processing_time=1.0,
                error_message=str(e),
                text_length=len(test_section.text),
            )

            timeout_tracker.add_timeout(timeout_info)
            await timeout_tracker.store_all_timeouts(run_id)

            print("✅ Timeout stored to database!")

        # Check if timeout was stored
        async with pool.acquire() as conn:
            timeout_count = await conn.fetchval(
                """
                SELECT COUNT(*)
                FROM lobbied_bill_matching.timeout_sections
                WHERE run_id = $1
                """,
                run_id,
            )

            print(f"Timeout sections in database: {timeout_count}")

            if timeout_count > 0:
                print("✅ Timeout storage mechanism is working correctly!")
            else:
                print("❌ Timeout was not stored to database")

        # Clean up
        async with pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM lobbied_bill_matching.timeout_sections WHERE run_id = $1",
                run_id,
            )
            await conn.execute(
                "DELETE FROM lobbied_bill_matching.processing_runs WHERE run_id = $1",
                run_id,
            )

        print("Test run cleaned up.")

    except Exception as e:
        print(f"❌ Error during testing: {str(e)}")
        raise
    finally:
        if "pool" in locals():
            await pool.close()


if __name__ == "__main__":
    asyncio.run(test_timeout_mechanism())
