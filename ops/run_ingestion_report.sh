#!/usr/bin/env bash
set -uo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/lib/bicam_lane.sh"

lock_path="/run/user/$(id -u)/bicam-ingestion.lock"
report_dir="$reports_root"
mkdir -p "$report_dir"

# Hold the lock so all three reports see one consistent post-dbt state.
lock_wait_seconds=${BICAM_REPORT_LOCK_WAIT_SECONDS:-1800}
if [[ "${BICAM_INGESTION_LOCK_HELD:-0}" != "1" ]]; then
  bicam_reexec_under_ingestion_lock "$lock_path" -w "$lock_wait_seconds" -- "$@"
  echo "ingestion report lock wait exceeded ${lock_wait_seconds}s" >&2
  exit 0
fi

cd "$project_root" || exit 1
status=0
if ! .venv/bin/python scripts/report_ingestion_status.py \
    --output "$report_dir/ingestion-status-latest.md" \
    --history-dir "$report_dir/ingestion-history"; then
  status=1
fi
if ! .venv/bin/python scripts/write_release_manifest.py \
    --output "$report_dir/main-11-release-manifest-latest.json" \
    --history-dir "$report_dir/release-manifests"; then
  status=1
fi
if ! .venv/bin/python scripts/report_source_gap_queue.py \
    --output-dir "$report_dir/source_gap_queue"; then
  status=1
fi
if ! .venv/bin/python scripts/report_html_text_progress.py \
    --output-dir "$report_dir/html-text"; then
  status=1
fi
if ! .venv/bin/python scripts/check_release_freshness.py \
    --no-file-verification \
    --manifest "$report_dir/exports/latest/manifest.json"; then
  status=1
fi
exit "$status"
