#!/usr/bin/env bash
set -u

source "$(dirname "${BASH_SOURCE[0]}")/lib/bicam_lane.sh"
bicam_load_project_env

log_file="$reports_root/reconciliation-probe.log"

mkdir -p "$reports_root"
exec >>"$log_file" 2>&1

date -u
echo "reconciliation_probe_followup_started"

# OnSuccess is ordered after the supervisor process exits, but keep this
# guard because a crawler worker may still be finishing its database cleanup.
while :; do
  active_runs=$(bicam_psql -At -c \
    "select count(*) from crawl_runs where status = 'running';" 2>/dev/null || echo 1)
  supervisor_state=$(systemctl --user is-active bicam-ingestion-supervisor.service 2>/dev/null || true)
  if [ "$active_runs" = "0" ] \
    && [ "$supervisor_state" != "active" ] \
    && ! pgrep -f '[b]ackfill_congress_root_details.py|[r]un_congress_history.py|[r]un_multi_crawl.py' >/dev/null 2>&1; then
    break
  fi
  echo "waiting_for_active_crawl_runs=$active_runs supervisor_state=$supervisor_state"
  sleep 30
done

cd "$project_root" || exit 1

probe_attempt=0
while [ "$probe_attempt" -lt 3 ]; do
  probe_attempt=$((probe_attempt + 1))
  echo "govinfo_only_bill_probe_attempt=$probe_attempt"
  uv run python scripts/backfill_congress_bill_details.py \
    --limit "${BICAM_GOVINFO_ONLY_BILL_PROBE_LIMIT:-100}" \
    --concurrency 1 \
    --request-interval 2.0 \
    --key-wait-timeout 3600 || true
  open_count=$(bicam_psql -At -c \
    "select count(*) from fct_reconciliation_exceptions where entity='bill' and exception_status='open';" \
    2>/dev/null || echo 1)
  echo "open_bill_exceptions=$open_count"
  [ "$open_count" = "0" ] && break
  sleep 300
done

echo "probe_modern_congress_only_govinfo_bills"
# The source-gap seed intentionally keeps the quality gate usable while the
# modern overlap is being checked, but it must not turn unprobed Congress-only
# records into permanent truth. Probe each remaining modern identity's
# conventional GovInfo BILLS package and retain 404 evidence for review.
uv run python scripts/backfill_govinfo_bills.py \
  --modern-missing \
  --limit "${BICAM_MODERN_GOVINFO_BILL_PROBE_LIMIT:-500}" \
  --concurrency 1 \
  --request-interval 2.0 \
  --key-wait-timeout 3600 || true

echo "govinfo_only_hearing_probe"
uv run python scripts/backfill_congress_govinfo_hearings.py \
  --congress-start 104 --congress-end 119 \
  --limit "${BICAM_GOVINFO_HEARING_PROBE_LIMIT:-2000}" \
  --concurrency 2 \
  --batch-size 100 \
  --request-interval 1.0 \
  --key-wait-timeout 3600 || true

echo "rebuild_reconciliation_after_probe"
uv run dbt seed --project-dir dbt_project --select reconciliation_source_gap_rules reconciliation_quality_thresholds \
  > /tmp/bicam-reconciliation-probe-seed.log 2>&1 || true
cat /tmp/bicam-reconciliation-probe-seed.log
set +e
timeout --signal=TERM --kill-after=15s "${BICAM_DBT_TIMEOUT_SECONDS:-900}s" \
  uv run dbt build --project-dir dbt_project --select +fct_reconciliation_metrics \
  > /tmp/bicam-reconciliation-probe-dbt.log 2>&1
dbt_rc=$?
set -e
echo "reconciliation_probe_dbt_rc=$dbt_rc"
cat /tmp/bicam-reconciliation-probe-dbt.log

uv run python scripts/report_ingestion_status.py \
  --output "$reports_root/ingestion-status-latest.md" \
  --history-dir "$reports_root/history" || true
echo "reconciliation_probe_followup_finished"
