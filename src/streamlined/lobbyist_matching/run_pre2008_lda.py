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
import sys

from dotenv import load_dotenv

# Handle imports for both direct execution and module execution
try:
    from streamlined.lobbyist_matching.batch_processor import BatchProcessor
    from streamlined.lobbyist_matching.db_utils import DatabaseInterface
    from streamlined.lobbyist_matching.main import ensure_schema_exists
    from streamlined.lobbyist_matching.matcher import MatchingManager
    from streamlined.lobbyist_matching.post_processor import post_process_all
    from streamlined.lobbyist_matching.schema_setup import initialize_run
    from streamlined.lobbyist_matching.timeout_handler import TimeoutTracker
except ImportError:
    # When run directly, add the parent directory to path
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    from streamlined.lobbyist_matching.batch_processor import BatchProcessor
    from streamlined.lobbyist_matching.db_utils import DatabaseInterface
    from streamlined.lobbyist_matching.main import ensure_schema_exists
    from streamlined.lobbyist_matching.matcher import MatchingManager
    from streamlined.lobbyist_matching.post_processor import post_process_all
    from streamlined.lobbyist_matching.schema_setup import initialize_run
    from streamlined.lobbyist_matching.timeout_handler import TimeoutTracker

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
    parser.add_argument(
        "--rerun-timeouts",
        type=int,
        help="Run ID whose timed-out sections should be reprocessed in chunks",
    )
    parser.add_argument(
        "--timeout-chunk-size",
        type=int,
        default=5,
        help="Number of text chunks per section when rerunning timeouts",
    )
    parser.add_argument(
        "--reprocess-timeouts-only",
        type=int,
        help="Run ID whose timed-out sections should be reprocessed (skips initial processing)",
    )
    parser.add_argument(
        "--run-matching-after-timeouts",
        action="store_true",
        help="Run matching and post-processing after timeout reprocessing (only used with --reprocess-timeouts-only)",
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
        if args.reprocess_timeouts_only is not None:
            # Reprocess timed-out sections only (skips initial processing)
            run_id = args.reprocess_timeouts_only
            chunk_size = args.timeout_chunk_size

            logger.info(f"Reprocessing timed-out sections only for run ID: {run_id}")
            db = await DatabaseInterface.create_pool(**db_config)

            # Fetch timed-out sections for the given run
            async with db.pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT DISTINCT filing_uuid, section_id
                    FROM lobbied_bill_matching.timeout_sections
                    WHERE run_id = $1
                    ORDER BY section_id
                    """,
                    run_id,
                )

            if not rows:
                logger.info("No timed-out sections found to reprocess.")
                return

            logger.info(f"Found {len(rows)} timed-out sections to reprocess")

            sections: list[dict] = []
            for row in rows:
                # Fetch the text for each section from the correct table
                async with db.pool.acquire() as conn:
                    rec = await conn.fetchrow(
                        """
                        SELECT cts.filing_uuid, cts.section_id, cts.issue_text AS text, f.filing_year
                        FROM raw___lda_pre2008.cleaned_text_sections cts
                        JOIN relational___lda.filings f ON cts.filing_uuid = f.filing_uuid
                        WHERE cts.filing_uuid = $1 AND cts.section_id = $2
                        AND cts.issue_text IS NOT NULL AND length(cts.issue_text) > 3
                        """,
                        row["filing_uuid"],
                        row["section_id"],
                    )
                if rec and rec["text"]:
                    text = rec["text"]
                    length = max(1, len(text))
                    approx_chunk_len = max(1000, length // max(1, chunk_size))
                    start = 0
                    while start < length:
                        end = min(length, start + approx_chunk_len)
                        chunk_text = text[start:end]
                        sections.append(
                            {
                                "filing_uuid": str(rec["filing_uuid"]),
                                "section_id": f"{rec['section_id']}-chunk-{start}-{end}",
                                "issue_text": chunk_text,
                                "filing_year": rec["filing_year"],
                            }
                        )
                        start = end

            if not sections:
                logger.info("No valid text found in timed-out sections to reprocess.")
                return

            logger.info(f"Created {len(sections)} chunked sections for reprocessing")

            timeout_tracker = TimeoutTracker(db.pool)
            await timeout_tracker.initialize_tracking(run_id)
            processor = BatchProcessor(
                db=db,
                batch_size=50,
                max_concurrent_batches=2,
                max_workers_per_batch=max(2, mp.cpu_count() // 4),
                timeout_tracker=timeout_tracker,
            )
            await processor.process_all_sections(run_id, sections)
            logger.info(
                f"Reprocessed {len(sections)} chunks from timed-out sections for run ID: {run_id}"
            )

            # Optionally run matching and post-processing
            if args.run_matching_after_timeouts:
                logger.info(
                    "Running matching and post-processing after timeout reprocessing..."
                )

                # Matching and post-processing
                matching_manager = MatchingManager()
                await matching_manager.initialize(db.pool)
                await matching_manager.match_references(db.pool, run_id)
                await post_process_all(db.pool, run_id)

                logger.info("Matching and post-processing completed!")

            await db.close()
            return

        if args.rerun_timeouts is not None:
            # Rerun timed-out sections in chunks
            run_id = args.rerun_timeouts
            chunk_size = args.timeout_chunk_size

            logger.info("Reprocessing timed-out sections in chunks...")
            db = await DatabaseInterface.create_pool(**db_config)

            # Fetch timed-out sections for the given run
            async with db.pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT DISTINCT filing_uuid, section_id
                    FROM lobbied_bill_matching.timeout_sections
                    WHERE run_id = $1
                    ORDER BY section_id
                    """,
                    run_id,
                )

            sections: list[dict] = []
            for row in rows:
                # Fetch the text for each section
                async with db.pool.acquire() as conn:
                    rec = await conn.fetchrow(
                        """
                        SELECT fs.filing_uuid, fs.section_id, fst.issue_text AS text, f.filing_year
                        FROM relational___lda.filing_sections fs
                        JOIN relational___lda.filing_sections_text fst ON fs.section_id = fst.section_id
                        JOIN relational___lda.filings f ON fs.filing_uuid = f.filing_uuid
                        WHERE fs.filing_uuid = $1 AND fs.section_id = $2
                        """,
                        row["filing_uuid"],
                        row["section_id"],
                    )
                if rec and rec["text"]:
                    text = rec["text"]
                    length = max(1, len(text))
                    approx_chunk_len = max(1000, length // max(1, chunk_size))
                    start = 0
                    while start < length:
                        end = min(length, start + approx_chunk_len)
                        chunk_text = text[start:end]
                        sections.append(
                            {
                                "filing_uuid": str(rec["filing_uuid"]),
                                "section_id": f"{rec['section_id']}-chunk-{start}-{end}",
                                "issue_text": chunk_text,
                                "filing_year": rec["filing_year"],
                            }
                        )
                        start = end

            if not sections:
                logger.info("No timed-out sections found to rerun.")
                return

            timeout_tracker = TimeoutTracker(db.pool)
            await timeout_tracker.initialize_tracking(run_id)
            processor = BatchProcessor(
                db=db,
                batch_size=50,
                max_concurrent_batches=2,
                max_workers_per_batch=max(2, mp.cpu_count() // 4),
                timeout_tracker=timeout_tracker,
            )
            await processor.process_all_sections(run_id, sections)
            logger.info(
                f"Reprocessed {len(sections)} chunks from timed-out sections for run ID: {run_id}"
            )
            await db.close()
            return

        logger.info("Starting pre-2008 LDA data processing...")
        logger.info("Source table: raw___lda_pre2008.cleaned_text_sections")
        logger.info(f"Sample size: {args.sample_size or 'All sections'}")
        logger.info(f"Batch size: {args.batch_size}")
        logger.info(f"Max concurrent batches: {args.max_batches}")
        logger.info(f"Workers per batch: {args.workers_per_batch}")

        # Custom pre-2008 pipeline (process_filings doesn't accept table_config)
        db = await DatabaseInterface.create_pool(
            min_size=2,
            max_size=10,
            **db_config,
        )

        await ensure_schema_exists(db.pool)

        # Fetch sections from pre-2008 table
        async with db.pool.acquire() as conn:
            if args.sample_size:
                rows = await conn.fetch(
                    f"""
                    SELECT cts.filing_uuid,
                           cts.section_id,
                           cts.issue_text AS text,
                           f.filing_year
                    FROM raw___lda_pre2008.cleaned_text_sections cts
                    JOIN relational___lda.filings f
                      ON cts.filing_uuid = f.filing_uuid
                    WHERE {table_config["additional_where"]}
                    ORDER BY random()
                    LIMIT $1
                    """,
                    args.sample_size,
                )
            else:
                rows = await conn.fetch(
                    f"""
                    SELECT cts.filing_uuid,
                           cts.section_id,
                           cts.issue_text AS text,
                           f.filing_year
                    FROM raw___lda_pre2008.cleaned_text_sections cts
                    JOIN relational___lda.filings f
                      ON cts.filing_uuid = f.filing_uuid
                    WHERE {table_config["additional_where"]}
                    """
                )

        sections: list[dict] = [
            {
                "filing_uuid": str(r["filing_uuid"]),
                "section_id": str(r["section_id"]),
                "issue_text": (r["text"] or ""),
                "filing_year": r["filing_year"],
            }
            for r in rows
            if r["text"] and len(r["text"]) > 3
        ]

        if not sections:
            logger.error("No sections found matching criteria")
            return

        total_filings = len({s["filing_uuid"] for s in sections})

        run_id = await initialize_run(
            db.pool,
            total_filings=total_filings,
            total_sections=len(sections),
            parameters={
                "description": args.description,
                "batch_size": args.batch_size,
                "max_concurrent_batches": args.max_batches,
                "max_workers_per_batch": args.workers_per_batch
                or (mp.cpu_count() // 4),
                "min_sections_per_filing": args.min_sections,
                "sample_size": args.sample_size,
                "source": "pre2008",
                "version": "2.0",
            },
        )

        timeout_tracker = TimeoutTracker(db.pool)
        await timeout_tracker.initialize_tracking(run_id)

        processor = BatchProcessor(
            db=db,
            batch_size=args.batch_size,
            max_concurrent_batches=args.max_batches,
            max_workers_per_batch=args.workers_per_batch or (mp.cpu_count() // 4),
            timeout_tracker=timeout_tracker,
        )
        await processor.process_all_sections(run_id, sections)

        # Automatically rerun timed-out sections with chunking before matching
        logger.info("Checking for timed-out sections to reprocess...")
        async with db.pool.acquire() as conn:
            # First check how many timeouts we have
            timeout_count = await conn.fetchval(
                """
                SELECT COUNT(*)
                FROM (
                    SELECT DISTINCT filing_uuid, section_id
                    FROM lobbied_bill_matching.timeout_sections
                    WHERE run_id = $1
                ) AS distinct_timeouts
                """,
                run_id,
            )

            if timeout_count == 0:
                logger.info("No timed-out sections found. Skipping reprocessing.")
            else:
                logger.info(
                    f"Found {timeout_count} timed-out sections. Starting reprocessing..."
                )

                timeout_recs = await conn.fetch(
                    """
                    WITH timeouts AS (
                        SELECT DISTINCT filing_uuid, section_id
                        FROM lobbied_bill_matching.timeout_sections
                        WHERE run_id = $1
                    )
                    SELECT cts.filing_uuid, cts.section_id, cts.issue_text AS text, f.filing_year
                    FROM timeouts t
                    JOIN raw___lda_pre2008.cleaned_text_sections cts
                      ON cts.filing_uuid = t.filing_uuid AND cts.section_id = t.section_id
                    JOIN relational___lda.filings f
                      ON cts.filing_uuid = f.filing_uuid
                    WHERE cts.issue_text IS NOT NULL AND length(cts.issue_text) > 3
                    ORDER BY cts.section_id
                    """,
                    run_id,
                )

        chunked_sections: list[dict] = []
        if timeout_count > 0:
            for rec in timeout_recs:
                if not rec["text"]:
                    continue
                text = rec["text"]
                length = max(1, len(text))
                approx_chunk_len = max(1000, length // max(1, args.timeout_chunk_size))
                start = 0
                while start < length:
                    end = min(length, start + approx_chunk_len)
                    chunk_text = text[start:end]
                    chunked_sections.append(
                        {
                            "filing_uuid": str(rec["filing_uuid"]),
                            "section_id": f"{rec['section_id']}-chunk-{start}-{end}",
                            "issue_text": chunk_text,
                            "filing_year": rec["filing_year"],
                        }
                    )
                    start = end

            if chunked_sections:
                logger.info(
                    f"Reprocessing {len(chunked_sections)} chunked sections from timed-out sections for run ID: {run_id}"
                )
                await timeout_tracker.initialize_tracking(run_id)
                await processor.process_all_sections(run_id, chunked_sections)
            else:
                logger.info(
                    "No valid text found in timed-out sections for reprocessing."
                )
        else:
            logger.info("No timed-out sections to reprocess.")

        # Matching and post-processing
        matching_manager = MatchingManager()
        await matching_manager.initialize(db.pool)
        await matching_manager.match_references(db.pool, run_id)
        await post_process_all(db.pool, run_id)

        # Mark run complete
        async with db.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE lobbied_bill_matching.processing_runs
                SET status = 'completed',
                    end_time = CURRENT_TIMESTAMP
                WHERE run_id = $1
                """,
                run_id,
            )

        await db.close()

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
