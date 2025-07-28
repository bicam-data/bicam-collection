#!/usr/bin/env python3
"""
Example script showing how to run lobbyist matching on a custom table.

This script demonstrates how to configure and run the bill reference extraction
and matching system on your own table instead of the default relational___lda tables.
"""

import asyncio
import os
from dotenv import load_dotenv

# Import the main processing function
from src.bicam_collection.lobbyist_matching.main import process_filings


async def main():
    """Example of running lobbyist matching on a custom table."""

    # Load environment variables
    load_dotenv()

    # Database configuration - same as usual
    db_config = {
        "host": os.getenv("POSTGRESQL_HOST"),
        "port": int(os.getenv("POSTGRESQL_PORT", 5432)),
        "user": os.getenv("POSTGRESQL_USER"),
        "password": os.getenv("POSTGRESQL_PASSWORD"),
        "database": os.getenv("POSTGRESQL_DB"),
    }

    # Configure your custom table
    table_config = {
        "table_name": "your_schema.your_table",  # Replace with your table name
        "filing_uuid_col": "your_filing_id_column",  # Column containing filing UUIDs
        "section_id_col": "your_section_id_column",  # Column containing section IDs
        "text_col": "your_text_column",  # Column containing the text to analyze
        "year_col": "your_year_column",  # Column containing filing year
        # Optional: add additional WHERE conditions
        "additional_where": "your_status_column = 'active' AND your_category = 'lobbying'",
    }

    try:
        # Run the processing with custom table
        run_id = await process_filings(
            db_config=db_config,
            sample_size=1000,  # Process 1000 random sections
            batch_size=50,
            max_concurrent_batches=2,
            description="Custom table test run",
            year_range=(2020, 2023),  # Optional: filter by year range
            table_config=table_config,  # Use your custom table
        )

        print(f"Processing completed successfully with run ID: {run_id}")

        # Results will be in the lobbied_bill_matching schema:
        # - lobbied_bill_matching.extracted_references
        # - lobbied_bill_matching.reference_matches
        # - lobbied_bill_matching.processing_runs

    except Exception as e:
        print(f"Error during processing: {str(e)}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
