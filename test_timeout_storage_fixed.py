#!/usr/bin/env python3
"""
Test script to verify that the timeout storage fix is working correctly.

This script tests the timeout storage functionality by:
1. Creating a test run
2. Processing a section that will timeout
3. Checking if the timeout is stored in the database
"""

import asyncio
import os
import sys
import time
from dotenv import load_dotenv

# Add the src directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

import asyncpg
from streamlined.lobbyist_matching.section_processor import (
    process_single_section_with_timeout,
)
from streamlined.lobbyist_matching.timeout_handler import TimeoutTracker
from streamlined.lobbyist_matching.db_utils import FilingSection
from streamlined.lobbyist_matching.schema_setup import initialize_run


def create_timeout_text():
    """Create text that will definitely cause a regex timeout."""
    # Create a string that will cause catastrophic backtracking
    # This pattern will cause exponential time complexity
    # Use a pattern that will definitely cause regex engine to struggle
    problematic_pattern = "a" * 1000 + "b" * 1000 + "c" * 1000
    # Create a string that will cause the regex engine to backtrack extensively
    text = ""
    for i in range(100):
        text += problematic_pattern + "x" * i + "y" * i
    return text


async def test_timeout_storage_fixed():
    """Test that timeouts are being stored correctly with the fixed implementation."""

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
            parameters={"description": "Timeout storage fix test", "test": True},
        )

        print(f"Created test run with ID: {run_id}")

        # Initialize timeout tracker
        timeout_tracker = TimeoutTracker(pool)
        await timeout_tracker.initialize_tracking(run_id)

        # Create a test section with text that will cause regex timeout
        test_section = FilingSection(
            filing_uuid="12345678-1234-1234-1234-123456789abc",  # Valid UUID format
            section_id="test-section-timeout-fix",
            text=create_timeout_text(),
            filing_year=2020,
        )

        print("Processing test section with timeout tracking...")
        print(f"Text length: {len(test_section.text)} characters")

        # Process the section with a very short timeout to force a timeout
        start_time = time.time()
        results, unmatched = process_single_section_with_timeout(
            test_section,
            timeout=2,  # 2 second timeout
            timeout_tracker=timeout_tracker,
            run_id=run_id,
        )
        processing_time = time.time() - start_time

        print(f"Processing complete in {processing_time:.2f}s")
        print(f"Results: {len(results)}, Unmatched: {len(unmatched)}")

        # Store any collected timeouts
        await timeout_tracker.store_all_timeouts(run_id)

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

            print(f"Timeout sections stored: {timeout_count}")

            if timeout_count > 0:
                print("✅ Timeout storage fix is working correctly!")

                # Show timeout details
                timeouts = await conn.fetch(
                    """
                    SELECT filing_uuid, section_id, pattern_type, text_length, processing_time
                    FROM lobbied_bill_matching.timeout_sections
                    WHERE run_id = $1
                    """,
                    run_id,
                )

                for timeout in timeouts:
                    print(
                        f"  - {timeout['section_id']}: {timeout['pattern_type']} ({timeout['text_length']} chars, {timeout['processing_time']}s)"
                    )
            else:
                print("❌ No timeouts were stored. The fix might not be working.")
                print(f"   - Processing took {processing_time:.2f}s (timeout was 2s)")

        # Clean up test run
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
    asyncio.run(test_timeout_storage_fixed())
