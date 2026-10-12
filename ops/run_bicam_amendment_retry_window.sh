#!/usr/bin/env bash
set -u

source "$(dirname "${BASH_SOURCE[0]}")/lib/bicam_lane.sh"

lock_file=/run/user/$(id -u)/bicam-ingestion.lock
cd "$project_root" || exit 1
bicam_load_project_env

# Skip (don't queue) when the lock is busy; queuing through a long cooldown gets
# the job killed by systemd.
if [[ "${BICAM_INGESTION_LOCK_HELD:-0}" != "1" ]]; then
  bicam_reexec_under_ingestion_lock "$lock_file" -n -- "$@"
  echo "[retry-window] skipped: ingestion lock is held"
  exit 0
fi

active_runs=$(bicam_psql -At -c \
  "select count(*) from crawl_runs where status = 'running';" 2>/dev/null || echo 1)
if [ "$active_runs" != "0" ]; then
  echo "[retry-window] skipped: active crawl_runs=$active_runs"
  exit 0
fi
if pgrep -f '[r]un_congress_history.py|[r]un_congress_descendant_refresh.py|[b]ackfill_congress_root_details.py|[r]ecover_congress_list_pages.py' >/dev/null 2>&1; then
  echo "[retry-window] skipped: crawler process is active"
  exit 0
fi

# Small batches of aged transient child failures, without descendant fan-out.
exec "$project_root/.venv/bin/python" scripts/retry_failed_fetches.py \
  --categories amendments \
  --limit "${BICAM_AMENDMENT_RETRY_LIMIT:-25}" \
  --request-interval "${BICAM_AMENDMENT_RETRY_REQUEST_INTERVAL:-2.0}" \
  --key-wait-timeout "${BICAM_AMENDMENT_RETRY_KEY_WAIT_TIMEOUT:-900}" \
  --min-age-minutes "${BICAM_AMENDMENT_RETRY_MIN_AGE_MINUTES:-30}" \
  --no-descendants
