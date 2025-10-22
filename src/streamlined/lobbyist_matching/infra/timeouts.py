import logging
import multiprocessing as mp

from streamlined.lobbyist_matching.batch_processor import BatchProcessor
from streamlined.lobbyist_matching.db_utils import DatabaseInterface, FilingSection
from streamlined.lobbyist_matching.timeout_handler import TimeoutTracker


async def rerun_timeouts_fixed(
    db: DatabaseInterface,
    run_id: int,
    chunk_count: int = 5,
    batch_size: int = 50,
    max_concurrent_batches: int = 2,
) -> int:
    """Rerun timed-out sections by splitting each section into a fixed number of chunks.

    Args:
        db: Database interface
        run_id: Run whose timeouts to reprocess
        chunk_count: Number of chunks per section
        batch_size: Sections per batch
        max_concurrent_batches: Concurrent batches

    Returns:
        int: Number of chunked sections reprocessed
    """
    try:
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

        sections: list[FilingSection] = []
        for row in rows:
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
                text: str = rec["text"]
                length = max(1, len(text))
                approx_chunk_len = max(1000, length // max(1, chunk_count))
                start = 0
                while start < length:
                    end = min(length, start + approx_chunk_len)
                    chunk_text = text[start:end]
                    sections.append(
                        FilingSection(
                            filing_uuid=str(rec["filing_uuid"]),
                            section_id=f"{rec['section_id']}-chunk-{start}-{end}",
                            text=chunk_text,
                            filing_year=rec["filing_year"],
                        )
                    )
                    start = end

        if not sections:
            logging.info("No timed-out sections found to rerun.")
            return 0

        timeout_tracker = TimeoutTracker(db.pool)
        await timeout_tracker.initialize_tracking(run_id)

        processor = BatchProcessor(
            db=db,
            batch_size=batch_size,
            max_concurrent_batches=max_concurrent_batches,
            max_workers_per_batch=max(2, mp.cpu_count() // 4),
            timeout_tracker=timeout_tracker,
        )

        await processor.process_all_sections(run_id, sections)
        logging.info(
            f"Reprocessed {len(sections)} chunks from timed-out sections for run ID: {run_id}"
        )
        return len(sections)

    except Exception as e:
        logging.error(f"Error during rerun of timeouts: {str(e)}")
        raise
