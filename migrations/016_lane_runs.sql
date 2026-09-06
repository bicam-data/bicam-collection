-- One row per systemd lane-service invocation, inserted by
-- scripts/record_lane_run.py from the ExecStopPost= hook in
-- deploy/systemd/bicam-.service.d/10-lane-status.conf.
--
-- exit_code / exit_status are kept as text because systemd reports a
-- signal name ("TERM") in $EXIT_STATUS when the main process was killed.
CREATE TABLE IF NOT EXISTS lane_runs (
    invocation_id UUID PRIMARY KEY,
    unit TEXT NOT NULL,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ NOT NULL,
    service_result TEXT NOT NULL,
    exit_code TEXT NOT NULL,
    exit_status TEXT NOT NULL,
    details JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_lane_runs_unit_finished
    ON lane_runs(unit, finished_at DESC);
