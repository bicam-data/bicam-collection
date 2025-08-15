#!/usr/bin/env python3
"""
Test script to verify that timeout storage is working correctly.

This script tests the timeout storage functionality by:
1. Creating a test run
2. Processing a section that will timeout
3. Checking if the timeout is stored in the database
"""

import asyncio
import os
import sys
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


async def test_timeout_storage():
    """Test that timeouts are being stored correctly."""

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
            parameters={"description": "Timeout storage test", "test": True},
        )

        print(f"Created test run with ID: {run_id}")

        # Initialize timeout tracker
        timeout_tracker = TimeoutTracker(pool)
        await timeout_tracker.initialize_tracking(run_id)

        # Create a test section with very long text that might timeout
        test_section = FilingSection(
            filing_uuid="test-uuid-123",
            section_id="test-section-456",
            text="A" * 1000000,  # Very long text that might cause timeout
            filing_year=2020,
        )

        print("Processing test section with timeout tracking...")

        # Process the section with a very short timeout to force a timeout
        results, unmatched = process_single_section_with_timeout(
            test_section,
            timeout=1,  # Very short timeout to force timeout
            timeout_tracker=timeout_tracker,
            run_id=run_id,
        )

        print(
            f"Processing complete. Results: {len(results)}, Unmatched: {len(unmatched)}"
        )

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
                print("✅ Timeout storage is working correctly!")

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
                print("❌ No timeouts were stored. This might mean:")
                print("   - The section processed successfully without timing out")
                print("   - The timeout storage mechanism is not working")
                print("   - The timeout duration was too long")

        # Clean up test run
        await conn.execute(
            """
            DELETE FROM lobbied_bill_matching.timeout_sections WHERE run_id = $1;
            DELETE FROM lobbied_bill_matching.processing_runs WHERE run_id = $1;
            """,
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
    asyncio.run(test_timeout_storage())
