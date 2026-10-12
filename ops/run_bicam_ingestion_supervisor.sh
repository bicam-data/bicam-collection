#!/usr/bin/env bash
set -u

source "$(dirname "${BASH_SOURCE[0]}")/lib/bicam_lane.sh"

lock_path="/run/user/$(id -u)/bicam-ingestion.lock"
log_path="$reports_root/post-crawl-supervisor.log"

mkdir -p "$reports_root"

# Refresh the persisted availability state before deciding whether to enter
# bounded retry/root-repair stages.  This read-only check prevents a service
# restart during a live quota cooldown from launching another long retry pass.
"$project_root/ops/guard_congress_resume.sh" >/dev/null 2>&1 || true

# A killed worker can leave its crawl_run row marked running.  Only reclaim
# rows when no crawler process exists; active workers are never touched.  The
# age bound protects a process that is between child launches.
if ! pgrep -f '[b]ackfill_congress_root_details.py|[r]un_congress_history.py|[r]un_congress_descendant_refresh.py|[r]un_multi_crawl.py' >/dev/null 2>&1; then
  docker exec bicam-collection-postgres psql -U admin -d bicam_collection -At -c \
    "update crawl_runs set status='abandoned', finished_at=now() where status='running' and started_at < now() - interval '15 minutes';" \
    >/dev/null 2>&1 || true
fi
# A retry process can disappear before it records its first fetch while a
# separate historical worker remains healthy.  Reclaim only zero-event rows in
# that case; active runs with recorded telemetry are never touched.
docker exec bicam-collection-postgres psql -U admin -d bicam_collection -At -c \
  "update crawl_runs as runs set status='abandoned', finished_at=now()
   where runs.status='running'
     and runs.started_at < now() - interval '15 minutes'
     and not exists (
       select 1 from crawl_run_fetches as fetches
       where fetches.crawl_run_id = runs.id
     );" >/dev/null 2>&1 || true

# The non-blocking lock prevents a manual recovery command or a second systemd
# start from creating overlapping API-key lanes.  ``uv run`` may place its
# Python child in a separate user scope, so a service restart can leave the
# child holding this lock after the original shell has exited.  Wait for that
# known crawler to finish instead of making systemd flap and starting retries
# that can never acquire the lock.  Once the child exits, the next loop owns
# the lock and resumes the normal post-crawl supervisor.
while :; do
  if flock -n "$lock_path" bash "$project_root/ops/post_crawl_reconcile_v5.sh" \
    99999999 "$log_path" >>"$log_path" 2>&1; then
    exit 0
  fi
  # Another bounded ingestion lane may own the lock while its shell is
  # between child launches (so no crawler executable is visible in pgrep).
  # A busy lock is normal coordination, not a supervisor failure; wait for
  # the owner to release it and then retry without systemd flapping.
  sleep 60
done
