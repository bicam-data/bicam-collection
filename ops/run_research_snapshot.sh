#!/usr/bin/env bash
set -u

source "$(dirname "${BASH_SOURCE[0]}")/lib/bicam_lane.sh"

# Serialize the export with crawler/dbt lanes.  A dbt view/table rebuild can
# briefly drop a relation while the exporter is enumerating its contract;
# using a separate snapshot-only lock allowed the 05:00 export to race that
# rebuild and fail with a missing staging relation.  The bounded wait keeps a
# collision from losing the entire daily snapshot window.
lock_path="/run/user/$(id -u)/bicam-ingestion.lock"
log_path="$reports_root/research-snapshot.log"

mkdir -p "$reports_root"
lock_wait_seconds=${BICAM_SNAPSHOT_LOCK_WAIT_SECONDS:-1800}
if [[ "${BICAM_INGESTION_LOCK_HELD:-0}" != "1" ]]; then
  bicam_reexec_under_ingestion_lock "$lock_path" -w "$lock_wait_seconds" -- "$@"
  echo "research snapshot lock wait exceeded ${lock_wait_seconds}s"
  exit 0
fi
exec >>"$log_path" 2>&1

started_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
echo "$started_at research_snapshot_started"
cd "$project_root" || exit 1
bicam_load_project_env

export_status=0
export_output=$(timeout --signal=TERM --kill-after=30s "${BICAM_EXPORT_TIMEOUT_SECONDS:-1800}s" \
  .venv/bin/python scripts/export_research_snapshot.py \
  --output-dir "$reports_root/exports" \
  --include-supplemental 2>&1) || export_status=$?
echo "$export_output"

check_status=1
check_output="snapshot export did not complete; freshness check skipped"
candidate_manifest=""
if [ "$export_status" -eq 0 ]; then
  candidate_manifest=$(printf '%s\n' "$export_output" | sed -n 's/^candidate_manifest=//p' | tail -1)
  if [ -z "$candidate_manifest" ] || [ ! -f "$candidate_manifest" ]; then
    export_status=1
    check_output="export did not report an exact candidate manifest"
  fi
fi
if [ "$export_status" -eq 0 ]; then
  check_output=$(timeout --signal=TERM --kill-after=30s "${BICAM_EXPORT_CHECK_TIMEOUT_SECONDS:-300}s" \
    .venv/bin/python scripts/check_release_freshness.py \
    --manifest "$candidate_manifest" 2>&1) \
    && check_status=0 || check_status=$?
  echo "$check_output"
fi

promote_status=1
promote_output="candidate was not promoted"
if [ "$export_status" -eq 0 ] && [ "$check_status" -eq 0 ]; then
  promote_output=$(timeout --signal=TERM --kill-after=30s "${BICAM_EXPORT_PROMOTE_TIMEOUT_SECONDS:-300}s" \
    .venv/bin/python scripts/promote_research_snapshot.py \
    --candidate "$candidate_manifest" \
    --output-dir "$reports_root/exports" \
    --include-supplemental 2>&1) \
    && promote_status=0 || promote_status=$?
  echo "$promote_output"
fi

queue_status=0
timeout --signal=TERM --kill-after=30s "${BICAM_SOURCE_GAP_REPORT_TIMEOUT_SECONDS:-180}s" \
  .venv/bin/python scripts/report_source_gap_queue.py \
  --output-dir "$reports_root/source_gap_queue" || queue_status=$?

checkpoint_timestamp=$(date -u +%Y%m%dT%H%M%SZ)
checkpoint_path="$reports_root/research-snapshot-checkpoint-${checkpoint_timestamp}.md"
{
  echo "# Research snapshot checkpoint"
  echo
  echo "Generated: $(date -u '+%Y-%m-%d %H:%M UTC')"
  echo
  echo "Started: ${started_at}"
  echo "Export status: ${export_status}"
  echo "Freshness check status: ${check_status}"
  echo "Promotion status: ${promote_status}"
  echo "Source-gap report status: ${queue_status}"
  echo
  if [ -f "$reports_root/exports/latest/manifest.json" ]; then
    echo "Latest manifest: reports/exports/latest/manifest.json"
    echo "Snapshot directory: $(readlink "$reports_root/exports/latest")"
  fi
  if [ -n "$candidate_manifest" ]; then
    echo "Candidate manifest: ${candidate_manifest}"
  fi
  echo
  echo '```text'
  echo "$check_output"
  echo '```'
} >"$checkpoint_path"
ln -sfn "$(basename "$checkpoint_path")" "$reports_root/research-snapshot-checkpoint-latest.md"
echo "research_snapshot_checkpoint=${checkpoint_path}"
echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) research_snapshot_finished export_status=${export_status} check_status=${check_status} queue_status=${queue_status}"

if [ "$export_status" -ne 0 ]; then
  exit "$export_status"
fi
if [ "$check_status" -ne 0 ]; then
  exit "$check_status"
fi
exit "$promote_status"
