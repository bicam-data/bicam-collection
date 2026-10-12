#!/usr/bin/env bash
set -uo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/lib/bicam_lane.sh"

report_dir="$reports_root"
lock_path="/run/user/$(id -u)/bicam-ingestion.lock"
stamp=$(date -u +%Y%m%dT%H%M%SZ)
checkpoint="$report_dir/fetch-lineage-checkpoint-$stamp.md"
latest="$report_dir/fetch-lineage-checkpoint-latest.md"
attempt_latest="$report_dir/fetch-lineage-checkpoint-attempt-latest.md"
log_path="$report_dir/fetch-lineage-backfill.log"

mkdir -p "$report_dir"
lock_wait_seconds=${BICAM_LINEAGE_LOCK_WAIT_SECONDS:-2700}
if [[ "${BICAM_INGESTION_LOCK_HELD:-0}" != "1" ]]; then
  bicam_reexec_under_ingestion_lock "$lock_path" -w "$lock_wait_seconds" -- "$@"
  printf '# Fetch-lineage checkpoint\n\n- generated: %s\n- status: lock_busy\n- note: another ingestion lane owns the shared lock; next timer invocation will retry\n' \
    "$(date -u --iso-8601=seconds)" >"$checkpoint"
  # A lock-busy skip is recorded only under the attempt pointer.
  ln -sfn "$(basename "$checkpoint")" "$attempt_latest"
  exit 0
fi

exec >>"$log_path" 2>&1
started_at=$(date -u --iso-8601=seconds)
echo "$started_at fetch_lineage_backfill_started"
cd "$project_root" || exit 1
bicam_load_project_env

# Database-only provenance repair; 5,000 raw IDs per transaction stays under the statement timeout.
batch_rows=${BICAM_LINEAGE_BATCH_ROWS:-2000000}
batch_raw_ids=${BICAM_LINEAGE_BATCH_RAW_IDS:-5000}
timeout_seconds=${BICAM_LINEAGE_TIMEOUT_SECONDS:-900}
status=0
timeout --signal=TERM --kill-after=30s "${timeout_seconds}s" \
  .venv/bin/python scripts/backfill_fetch_version_links.py \
    --max-fetch-rows "$batch_rows" \
    --batch-raw-ids "$batch_raw_ids" \
    --resolve-endpoint \
    --resolve-temporal \
    --skip-count || status=$?

freshness_status=1
freshness_output="lineage batch did not complete; freshness check skipped"
if (( status == 0 )); then
  freshness_output=$(timeout --signal=TERM --kill-after=30s 300s \
    .venv/bin/python scripts/check_release_freshness.py 2>&1) \
    && freshness_status=0 || freshness_status=$?
  echo "$freshness_output"
fi

{
  echo '# Fetch-lineage checkpoint'
  echo
  echo "- generated: $(date -u '+%Y-%m-%d %H:%M UTC')"
  echo "- started: $started_at"
  echo "- repair_status: $status"
  echo "- freshness_status: $freshness_status"
  echo "- batch_rows: $batch_rows"
  echo
  echo '```text'
  tail -80 "$log_path" 2>/dev/null || true
  echo '```'
} >"$checkpoint"
ln -sfn "$(basename "$checkpoint")" "$attempt_latest"
if (( status == 0 && freshness_status == 0 )); then
  ln -sfn "$(basename "$checkpoint")" "$latest"
fi
echo "$(date -u --iso-8601=seconds) fetch_lineage_backfill_finished repair_status=$status freshness_status=$freshness_status"

if (( status != 0 )); then
  exit "$status"
fi
exit "$freshness_status"
