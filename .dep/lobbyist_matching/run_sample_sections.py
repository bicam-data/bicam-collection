#!/usr/bin/env python3
"""
Run lobbyist matching on sections from _sandbox_ryan.sample_bill_sections only.

This script fetches sections from the sandbox table with schema:
  - filing_uuid UUID
  - general_issue_code TEXT
  - issue_text TEXT
  - section_order INTEGER

It then wires them into the existing extraction → matching → post-processing
pipeline used by the lobbyist matching modules.
"""

import argparse
import asyncio
import logging
import multiprocessing as mp
import os
import sys
from typing import Any

from dotenv import load_dotenv

from streamlined.lobbyist_matching.utils.config import resolve_db_config

# Support running both as module and as script
try:
    from streamlined.lobbyist_matching.batch_processor import BatchProcessor
    from streamlined.lobbyist_matching.db_utils import DatabaseInterface
    from streamlined.lobbyist_matching.main import ensure_schema_exists
    from streamlined.lobbyist_matching.matcher import MatchingManager
    from streamlined.lobbyist_matching.post_processor import post_process_all
    from streamlined.lobbyist_matching.schema_setup import initialize_run
    from streamlined.lobbyist_matching.timeout_handler import TimeoutTracker
except ImportError:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    src_dir = os.path.join(current_dir, "..", "..")
    sys.path.insert(0, src_dir)

    from streamlined.lobbyist_matching.batch_processor import BatchProcessor
    from streamlined.lobbyist_matching.db_utils import DatabaseInterface
    from streamlined.lobbyist_matching.main import ensure_schema_exists
    from streamlined.lobbyist_matching.matcher import MatchingManager
    from streamlined.lobbyist_matching.post_processor import post_process_all
    from streamlined.lobbyist_matching.schema_setup import initialize_run
    from streamlined.lobbyist_matching.timeout_handler import TimeoutTracker


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_db_config_from_env() -> dict[str, Any]:
    """Resolve DB config from environment with common fallbacks (shared)."""
    return resolve_db_config()


async def fetch_sample_sections(
    db: DatabaseInterface,
    sample_size: int | None,
    general_issue_code: str | None,
) -> list[dict[str, Any]]:
    """Fetch sections from _sandbox_ryan.sample_bill_sections and shape them for processing.

    Returns list of dicts with keys: filing_uuid, section_id, issue_text, filing_year.
    """
    base_sql = """
        SELECT 
            s.filing_uuid,
            s.section_order,
            s.issue_text AS text,
            f.filing_year,
            s.general_issue_code
        FROM _sandbox_ryan.sample_bill_sections s
        JOIN relational___lda.filings f
          ON s.filing_uuid = f.filing_uuid
        WHERE s.issue_text IS NOT NULL AND length(s.issue_text) > 3
        {gi_filter}
        {order_limit}
        """

    gi_filter = ""
    params: list[Any] = []

    if general_issue_code:
        gi_filter = " AND s.general_issue_code = $1"
        params.append(general_issue_code)

    if sample_size is not None:
        order_limit = f" ORDER BY random() LIMIT ${len(params) + 1}"
        params.append(sample_size)
    else:
        order_limit = " ORDER BY s.filing_uuid, s.section_order"

    sql = base_sql.format(gi_filter=gi_filter, order_limit=order_limit)

    async with db.pool.acquire() as conn:
        # Disable server-side timeouts for long-running reads
        await conn.execute("SET statement_timeout TO 0")
        await conn.execute("SET idle_in_transaction_session_timeout TO 0")
        await conn.execute("SET lock_timeout TO 0")
        rows = await conn.fetch(sql, *params)

    sections: list[dict[str, Any]] = []
    for r in rows:
        # Create a stable synthetic section_id tied to the sample table
        section_id = f"{r['filing_uuid']}-sample-{r['section_order']}"
        sections.append(
            {
                "filing_uuid": str(r["filing_uuid"]),
                "section_id": section_id,
                "issue_text": r["text"] or "",
                "filing_year": r["filing_year"],
            }
        )

    return sections


async def create_results_view(db: DatabaseInterface, run_id: int) -> None:
    """Create or replace a view with matched bills for sample sections."""
    view_sql = f"""
    CREATE OR REPLACE VIEW _sandbox_ryan.sample_bill_sections_with_matches AS
    SELECT
        s.filing_uuid,
        s.general_issue_code,
        s.issue_text,
        ARRAY_AGG(DISTINCT COALESCE(rm.updated_bill_id, rm.bill_id))
            FILTER (WHERE COALESCE(rm.updated_bill_id, rm.bill_id) IS NOT NULL) AS matched_bills
    FROM _sandbox_ryan.sample_bill_sections s
    LEFT JOIN lobbied_bill_matching.extracted_references er
      ON er.section_id = (s.filing_uuid::text || '-sample-' || s.section_order::text)
     AND er.run_id = {run_id}
    LEFT JOIN lobbied_bill_matching.reference_matches rm
      ON rm.reference_id = er.reference_id
     AND rm.run_id = {run_id}
    GROUP BY s.filing_uuid, s.general_issue_code, s.issue_text;
    """

    async with db.pool.acquire() as conn:
        await conn.execute("SET statement_timeout TO 0")
        await conn.execute(view_sql)


async def main():
    parser = argparse.ArgumentParser(
        description=(
            "Run lobbyist matching on sections from _sandbox_ryan.sample_bill_sections"
        )
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=None,
        help="If provided, randomly sample this many sections from the table",
    )
    parser.add_argument(
        "--gi",
        "--general-issue",
        dest="general_issue_code",
        type=str,
        default=None,
        help="Filter by a specific general_issue_code",
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
        "--description",
        type=str,
        default="Sample sections run",
        help="Description for this processing run",
    )
    parser.add_argument(
        "--skip-post",
        action="store_true",
        help="Skip post-processing steps (only extract + match)",
    )

    args = parser.parse_args()

    load_dotenv()

    db_config = get_db_config_from_env()

    db = await DatabaseInterface.create_pool(min_size=2, max_size=10, **db_config)

    try:
        logger.info("Ensuring schema exists (non-destructive check)...")
        schema_ok = await ensure_schema_exists(db.pool)
        if not schema_ok:
            logger.warning(
                "Required schema not fully present. Please initialize schema before running."
            )

        logger.info("Fetching sections from _sandbox_ryan.sample_bill_sections...")
        sections = await fetch_sample_sections(
            db=db,
            sample_size=args.sample_size,
            general_issue_code=args.general_issue_code,
        )

        if not sections:
            logger.error("No sections found in the sample table matching criteria")
            return

        total_filings = len({s["filing_uuid"] for s in sections})
        logger.info(f"Processing {len(sections)} sections from {total_filings} filings")

        run_id = await initialize_run(
            db.pool,
            total_filings=total_filings,
            total_sections=len(sections),
            parameters={
                "description": args.description,
                "batch_size": args.batch_size,
                "max_concurrent_batches": args.max_batches,
                "workers_per_batch": args.workers_per_batch,
                "sample_size": args.sample_size,
                "general_issue_code": args.general_issue_code,
                "source": "_sandbox_ryan.sample_bill_sections",
                "version": "2.0",
            },
        )

        timeout_tracker = TimeoutTracker(db.pool)
        await timeout_tracker.initialize_tracking(run_id)

        processor = BatchProcessor(
            db=db,
            batch_size=args.batch_size,
            max_concurrent_batches=args.max_batches,
            max_workers_per_batch=args.workers_per_batch,
            timeout_tracker=timeout_tracker,
        )

        await processor.process_all_sections(run_id, sections)

        logger.info("Initializing matching...")
        matching_manager = MatchingManager()
        await matching_manager.initialize(db.pool)
        await matching_manager.match_references(db.pool, run_id)

        if not args.skip_post:
            logger.info("Running post-processing...")
            await post_process_all(db.pool, run_id)
        else:
            logger.info("Post-processing skipped by flag")

        # Create/replace a view with the results joined to the sample sections
        logger.info("Creating view _sandbox_ryan.sample_bill_sections_with_matches ...")
        await create_results_view(db, run_id)

        # Mark run as complete
        async with db.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE lobbied_bill_matching.processing_runs
                SET status = 'completed', end_time = CURRENT_TIMESTAMP
                WHERE run_id = $1
                """,
                run_id,
            )

        logger.info(f"Run completed successfully. Run ID: {run_id}")

    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
