#!/usr/bin/env python3
"""
Script to check timeout status for a specific run_id.

This helps diagnose whether timeout reprocessing worked correctly.
"""

import asyncio
import os
import sys
from dotenv import load_dotenv

# Add the src directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

import asyncpg


async def check_timeout_status(run_id: int):
    """Check timeout status for a specific run_id."""

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

        async with pool.acquire() as conn:
            print(f"=== Timeout Status for Run ID: {run_id} ===\n")

            # Check run status
            run_info = await conn.fetchrow(
                """
                SELECT run_id, status, start_time, end_time, total_filings, total_sections
                FROM lobbied_bill_matching.processing_runs
                WHERE run_id = $1
                """,
                run_id,
            )

            if not run_info:
                print(f"❌ Run ID {run_id} not found!")
                return

            print(f"Run Status: {run_info['status']}")
            print(f"Start Time: {run_info['start_time']}")
            print(f"End Time: {run_info['end_time']}")
            print(f"Total Filings: {run_info['total_filings']}")
            print(f"Total Sections: {run_info['total_sections']}")
            print()

            # Check timeout sections
            timeout_count = await conn.fetchval(
                """
                SELECT COUNT(DISTINCT filing_uuid, section_id)
                FROM lobbied_bill_matching.timeout_sections
                WHERE run_id = $1
                """,
                run_id,
            )

            print(f"Timeout Sections: {timeout_count}")

            if timeout_count > 0:
                print("\n=== Timeout Details ===")
                timeouts = await conn.fetch(
                    """
                    SELECT filing_uuid, section_id, pattern_type, text_length, processing_time
                    FROM lobbied_bill_matching.timeout_sections
                    WHERE run_id = $1
                    ORDER BY section_id
                    LIMIT 10
                    """,
                    run_id,
                )

                for timeout in timeouts:
                    print(
                        f"  - {timeout['section_id']}: {timeout['pattern_type']} ({timeout['text_length']} chars, {timeout['processing_time']:.2f}s)"
                    )

                if timeout_count > 10:
                    print(f"  ... and {timeout_count - 10} more")

            # Check if any references were extracted from timeout sections
            timeout_refs = await conn.fetchval(
                """
                SELECT COUNT(*)
                FROM lobbied_bill_matching.extracted_references er
                JOIN lobbied_bill_matching.timeout_sections ts 
                    ON er.filing_uuid = ts.filing_uuid 
                    AND er.section_id = ts.section_id
                WHERE er.run_id = $1 AND ts.run_id = $1
                """,
                run_id,
            )

            print(f"\nReferences from timeout sections: {timeout_refs}")

            # Check total references
            total_refs = await conn.fetchval(
                """
                SELECT COUNT(*)
                FROM lobbied_bill_matching.extracted_references
                WHERE run_id = $1
                """,
                run_id,
            )

            print(f"Total references extracted: {total_refs}")

            # Check matches
            total_matches = await conn.fetchval(
                """
                SELECT COUNT(*)
                FROM lobbied_bill_matching.reference_matches
                WHERE run_id = $1
                """,
                run_id,
            )

            print(f"Total matches: {total_matches}")

            # Check match types
            match_types = await conn.fetch(
                """
                SELECT match_type, COUNT(*) as count
                FROM lobbied_bill_matching.reference_matches
                WHERE run_id = $1
                GROUP BY match_type
                ORDER BY count DESC
                """,
                run_id,
            )

            print("\n=== Match Types ===")
            for match_type in match_types:
                print(f"  {match_type['match_type']}: {match_type['count']}")

            # Check if any chunked sections were processed
            chunked_refs = await conn.fetchval(
                """
                SELECT COUNT(*)
                FROM lobbied_bill_matching.extracted_references
                WHERE run_id = $1 AND section_id LIKE '%-chunk-%'
                """,
                run_id,
            )

            print(f"\nChunked section references: {chunked_refs}")

    except Exception as e:
        print(f"❌ Error: {str(e)}")
        raise
    finally:
        if "pool" in locals():
            await pool.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Check timeout status for a run_id")
    parser.add_argument("run_id", type=int, help="The run_id to check")

    args = parser.parse_args()

    asyncio.run(check_timeout_status(args.run_id))
