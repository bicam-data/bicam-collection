#!/usr/bin/env bash
set -u

source "$(dirname "${BASH_SOURCE[0]}")/lib/bicam_lane.sh"
bicam_load_project_env

lock_path="/run/user/$(id -u)/bicam-ingestion.lock"
log_path="$reports_root/post-crawl-supervisor.log"

mkdir -p "$reports_root"

# Refresh availability state so a restart during a cooldown doesn't start retries.
"$project_root/ops/guard_congress_resume.sh" >/dev/null 2>&1 || true

# Reclaim crawl_runs left 'running' by a killed worker, only when no crawler runs.
if ! pgrep -f '[b]ackfill_congress_root_details.py|[r]un_congress_history.py|[r]un_congress_descendant_refresh.py|[r]un_multi_crawl.py' >/dev/null 2>&1; then
  bicam_psql -At -c \
    "update crawl_runs set status='abandoned', finished_at=now() where status='running' and started_at < now() - interval '15 minutes';" \
    >/dev/null 2>&1 || true
fi
# Reclaim zero-event runs whose retry process died before its first fetch.
bicam_psql -At -c \
  "update crawl_runs as runs set status='abandoned', finished_at=now()
   where runs.status='running'
     and runs.started_at < now() - interval '15 minutes'
     and not exists (
       select 1 from crawl_run_fetches as fetches
       where fetches.crawl_run_id = runs.id
     );" >/dev/null 2>&1 || true

# A uv-spawned crawler can outlive a restarted service and keep holding the lock;
# wait for it instead of letting systemd flap.
while :; do
  if flock -n "$lock_path" bash "$project_root/ops/post_crawl_reconcile_v5.sh" \
    99999999 "$log_path" >>"$log_path" 2>&1; then
    exit 0
  fi
  # Another lane holds the lock between child launches; wait and retry.
  sleep 60
done
