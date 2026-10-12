#!/usr/bin/env bash
set -u

source "$(dirname "${BASH_SOURCE[0]}")/lib/bicam_lane.sh"

lock_file=/run/user/$(id -u)/bicam-ingestion.lock
cd "$project_root" || exit 1
bicam_load_project_env

# This is a bounded maintenance lane, not a second concurrent crawler.  The
# historical bill/amendment sweep and the dedicated amendment reserve already
# consume the shared Congress.gov account window.  Never queue behind their
# lock (which can make systemd kill the job during a long cooldown); skip this
# window and let the next timer invocation try again instead.
if [[ "${BICAM_INGESTION_LOCK_HELD:-0}" != "1" ]]; then
  bicam_reexec_under_ingestion_lock "$lock_file" -n -- "$@"
  echo "[retry-window] skipped: ingestion lock is held"
  exit 0
fi

active_runs=$(docker exec bicam-collection-postgres psql -U admin -d bicam_collection -At -c \
  "select count(*) from crawl_runs where status = 'running';" 2>/dev/null || echo 1)
if [ "$active_runs" != "0" ]; then
  echo "[retry-window] skipped: active crawl_runs=$active_runs"
  exit 0
fi
if pgrep -f '[r]un_congress_history.py|[r]un_congress_descendant_refresh.py|[b]ackfill_congress_root_details.py|[r]ecover_congress_list_pages.py' >/dev/null 2>&1; then
  echo "[retry-window] skipped: crawler process is active"
  exit 0
fi

# Keep this repair lane deliberately small.  It handles aged transient child
# failures after a source cooldown without competing with the high-throughput
# historical workers or replaying descendant fan-out.
exec "$project_root/.venv/bin/python" scripts/retry_failed_fetches.py \
  --categories amendments \
  --limit "${BICAM_AMENDMENT_RETRY_LIMIT:-25}" \
  --request-interval "${BICAM_AMENDMENT_RETRY_REQUEST_INTERVAL:-2.0}" \
  --key-wait-timeout "${BICAM_AMENDMENT_RETRY_KEY_WAIT_TIMEOUT:-900}" \
  --min-age-minutes "${BICAM_AMENDMENT_RETRY_MIN_AGE_MINUTES:-30}" \
  --no-descendants
