-- Durable, source-independent ledger for bounded GovInfo date windows.
-- A window advances only after each configured collection has completed its
-- full pagination and child-resource crawl with no failed fetch events.
CREATE TABLE IF NOT EXISTS govinfo_collection_windows (
    id UUID PRIMARY KEY,
    category TEXT NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('running', 'completed', 'failed')),
    crawl_run_id UUID REFERENCES crawl_runs(id),
    list_pages INTEGER,
    fetch_events INTEGER,
    failed_events INTEGER,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    error_message TEXT,
    is_bootstrap BOOLEAN NOT NULL DEFAULT FALSE,
    CHECK (window_end > window_start),
    CHECK (list_pages IS NULL OR list_pages >= 0),
    CHECK (fetch_events IS NULL OR fetch_events >= 0),
    CHECK (failed_events IS NULL OR failed_events >= 0),
    UNIQUE (category, window_start, window_end)
);

CREATE INDEX IF NOT EXISTS idx_govinfo_collection_windows_frontier
    ON govinfo_collection_windows(category, status, window_end DESC);
CREATE INDEX IF NOT EXISTS idx_govinfo_collection_windows_run_id
    ON govinfo_collection_windows(crawl_run_id)
    WHERE crawl_run_id IS NOT NULL;
