#!/usr/bin/env python3
"""
Script to run lobbyist matching on pre-2008 LDA cleaned text sections.

This script processes the raw___lda_pre2008.cleaned_text_sections table,
joining with relational___lda tables to get filing information.
"""

import argparse
import asyncio
import logging
import multiprocessing as mp
import os

from dotenv import load_dotenv

from .main import process_filings

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)


async def main():
    """Run lobbyist matching on pre-2008 LDA data."""

    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Process pre-2008 LDA cleaned text sections for bill reference extraction"
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=None,
        help="Number of random sections to process",
    )
    parser.add_argument(
        "--batch-size", type=int, default=50, help="Number of sections per batch"
    )
    parser.add_argument(
        "--max-batches",
        type=int,
        default=2,
        help="Maximum number of concurrent batches",
    )
    parser.add_argument(
        "--workers-per-batch",
        type=int,
        default=max(2, mp.cpu_count() // 4),
        help="Number of worker processes per batch",
    )
    parser.add_argument(
        "--min-sections",
        type=int,
        default=1,
        help="Minimum number of sections per filing",
    )
    parser.add_argument(
        "--description",
        type=str,
        default="Pre-2008 LDA cleaned text processing",
        help="Description for this processing run",
    )

    args = parser.parse_args()

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

    # Configure the pre-2008 table with necessary joins
    table_config = {
        "table_name": """
            raw___lda_pre2008.cleaned_text_sections cts
            JOIN relational___lda.filings f ON cts.filing_uuid = f.filing_uuid
        """,
        "filing_uuid_col": "cts.filing_uuid",  # Use filing_uuid directly from cleaned_text_sections
        "section_id_col": "cts.section_id",
        "text_col": "cts.issue_text",
        "year_col": "f.filing_year",  # Get year from the JOIN to filings
        # Optional: filter to ensure we have good text
        "additional_where": "cts.issue_text IS NOT NULL AND length(cts.issue_text) > 3",
    }

    try:
        logger.info("Starting pre-2008 LDA data processing...")
        logger.info("Source table: raw___lda_pre2008.cleaned_text_sections")
        logger.info(f"Sample size: {args.sample_size or 'All sections'}")
        logger.info(f"Batch size: {args.batch_size}")
        logger.info(f"Max concurrent batches: {args.max_batches}")
        logger.info(f"Workers per batch: {args.workers_per_batch}")

        # Run the processing
        run_id = await process_filings(
            db_config=db_config,
            table_config=table_config,
            sample_size=args.sample_size,
            batch_size=args.batch_size,
            max_concurrent_batches=args.max_batches,
            max_workers_per_batch=args.workers_per_batch,
            min_sections_per_filing=args.min_sections,
            description=args.description,
            # No year_range needed since pre-2008 data is already filtered
        )

        logger.info("\n✅ Processing completed successfully!")
        logger.info(f"📊 Run ID: {run_id}")
        logger.info("📋 Check results in lobbied_bill_matching schema")

        # Show some basic stats query
        logger.info("\n📈 To view results, try this query:")
        logger.info(f"""
        SELECT 
            COUNT(*) as total_references,
            COUNT(CASE WHEN rm.match_type = 'high_confidence_match' THEN 1 END) as high_confidence,
            COUNT(CASE WHEN rm.match_type = 'unmatched' THEN 1 END) as unmatched,
            MIN(f.filing_year) as min_year,
            MAX(f.filing_year) as max_year
        FROM lobbied_bill_matching.extracted_references er
        JOIN lobbied_bill_matching.reference_matches rm ON er.reference_id = rm.reference_id
        JOIN raw___lda_pre2008.cleaned_text_sections cts ON er.section_id = cts.section_id
        JOIN relational___lda.filings f ON cts.filing_uuid = f.filing_uuid
        WHERE er.run_id = {run_id};
        """)

    except Exception as e:
        logger.error(f"❌ Error during processing: {str(e)}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
