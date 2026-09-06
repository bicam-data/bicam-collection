-- Support bounded recovery of legacy skipped-existing fetch lineage.
--
-- Crawls created before migration 001/003 can retain a raw_response_id but
-- no exact raw_response_version_id.  The application now records the exact
-- version for new skips; this partial index lets the resumable repair script
-- recover the historical singleton-version rows without repeatedly scanning
-- the full fetch ledger.
CREATE INDEX IF NOT EXISTS idx_crawl_run_fetches_missing_raw_response_id
    ON crawl_run_fetches(raw_response_id)
    WHERE raw_response_id IS NOT NULL
      AND raw_response_version_id IS NULL
      AND outcome = 'skipped_existing_backfill';
