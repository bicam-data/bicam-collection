#!/usr/bin/env python3
"""
Test script to validate the pre-2008 LDA table setup before running full processing.

This script tests the JOIN and shows sample data to ensure everything is configured correctly.
"""

import asyncio
import os

import asyncpg
from dotenv import load_dotenv


async def test_table_setup():
    """Test the pre-2008 table configuration."""

    # Load environment variables
    load_dotenv()
    print(os.getenv("POSTGRESQL_USERNAME"))

    # Database configuration
    db_config = {
        "host": os.getenv("POSTGRESQL_HOST"),
        "port": int(os.getenv("POSTGRESQL_PORT", 5432)),
        "user": os.getenv("POSTGRESQL_USERNAME"),
        "password": os.getenv("POSTGRESQL_PASSWORD"),
        "database": os.getenv("POSTGRESQL_DATABASE"),
    }

    try:
        # Connect to database
        conn = await asyncpg.connect(**db_config)

        print("🔍 Testing pre-2008 LDA table setup...")
        print("=" * 60)

        # Test 1: Check if tables exist and JOIN works
        print("\n1️⃣ Testing table access and JOINs...")

        test_query = """
        SELECT 
            f.filing_uuid,
            cts.section_id,
            LEFT(cts.issue_text, 100) as issue_text_preview,
            f.filing_year,
            LENGTH(cts.issue_text) as text_length
        FROM raw___lda_pre2008.cleaned_text_sections cts 
        JOIN relational___lda.filings f ON cts.filing_uuid = f.filing_uuid
        WHERE cts.issue_text IS NOT NULL 
        AND length(cts.issue_text) > 3
        ORDER BY f.filing_year, cts.section_id
        LIMIT 5
        """

        rows = await conn.fetch(test_query)

        if rows:
            print(f"✅ SUCCESS! Found {len(rows)} sample records:")
            for i, row in enumerate(rows, 1):
                print(f"   {i}. Filing: {row['filing_uuid']}")
                print(f"      Section: {row['section_id']}")
                print(f"      Year: {row['filing_year']}")
                print(f"      Text length: {row['text_length']} chars")
                print(f"      Preview: {row['issue_text_preview']}...")
                print()
        else:
            print("❌ No records found! Check your table setup.")
            return False

        # Test 2: Get counts and year distribution
        print("\n2️⃣ Getting data overview...")

        count_query = """
        SELECT 
            COUNT(*) as total_sections,
            COUNT(DISTINCT f.filing_uuid) as total_filings,
            MIN(f.filing_year) as min_year,
            MAX(f.filing_year) as max_year,
            AVG(LENGTH(cts.issue_text)) as avg_text_length
        FROM raw___lda_pre2008.cleaned_text_sections cts 
        JOIN relational___lda.filings f ON cts.filing_uuid = f.filing_uuid
        WHERE cts.issue_text IS NOT NULL 
        AND length(cts.issue_text) > 3
        """

        stats = await conn.fetchrow(count_query)
        print("📊 Data Overview:")
        print(f"   • Total sections: {stats['total_sections']:,}")
        print(f"   • Total filings: {stats['total_filings']:,}")
        print(f"   • Year range: {stats['min_year']} - {stats['max_year']}")
        print(f"   • Average text length: {stats['avg_text_length']:.0f} chars")

        # Test 3: Year distribution
        print("\n3️⃣ Year distribution...")

        year_query = """
        SELECT 
            f.filing_year,
            COUNT(*) as section_count,
            COUNT(DISTINCT f.filing_uuid) as filing_count
        FROM raw___lda_pre2008.cleaned_text_sections cts 
        JOIN relational___lda.filings f ON cts.filing_uuid = f.filing_uuid
        WHERE cts.issue_text IS NOT NULL 
        AND length(cts.issue_text) > 3
        GROUP BY f.filing_year
        ORDER BY f.filing_year
        """

        year_stats = await conn.fetch(year_query)
        print("📅 By Year:")
        for year_stat in year_stats:
            print(
                f"   • {year_stat['filing_year']}: {year_stat['section_count']:,} sections, {year_stat['filing_count']:,} filings"
            )

        await conn.close()

        print("\n" + "=" * 60)
        print("✅ Table test completed successfully!")
        print("\n🚀 Ready to run processing with:")
        print("   python run_pre2008_lda.py")

        return True

    except Exception as e:
        raise e


if __name__ == "__main__":
    asyncio.run(test_table_setup())
