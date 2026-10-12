#!/usr/bin/env bash
set -u

source "$(dirname "${BASH_SOURCE[0]}")/lib/bicam_lane.sh"
bicam_load_project_env

# Pauses the Congress history workers during a coordinated Congress.gov
# outage; isolated failures must not pause a healthy crawl.
threshold_total=${BICAM_RESUME_GUARD_FAILURE_THRESHOLD:-20}
threshold_503=${BICAM_RESUME_GUARD_503_THRESHOLD:-10}
threshold_failure_ratio=${BICAM_RESUME_GUARD_FAILURE_RATIO:-0.50}
resume_after=${BICAM_RESUME_GUARD_RETRY_SECONDS:-600}
quota_low_activity_limit=${BICAM_RESUME_GUARD_QUOTA_LOW_ACTIVITY_LIMIT:-1000}
state_file=${BICAM_RESUME_GUARD_STATE_FILE:-$reports_root/availability-guard-latest.txt}

write_state() {
  mkdir -p "$(dirname "$state_file")"
  {
    printf 'generated_at=%s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
    printf 'state=%s\n' "$1"
    printf 'failures_2m=%s\n' "$total"
    printf 'http_503_2m=%s\n' "$failures_503"
    printf 'http_429_2m=%s\n' "$failures_429"
    printf 'fetched_2m=%s\n' "$fetched"
    printf 'failure_ratio=%s\n' "$failure_ratio"
    printf 'last_http_429_at=%s\n' "$last_429_at"
  } >"$state_file"
}

# Plain 500s are deterministic source defects on historical URLs, not an outage.
read -r total failures_503 failures_429 fetched last_429_epoch <<EOF
$(bicam_psql -At -F ' ' -c \
  "select count(*) filter (where outcome = 'failed' and http_status is distinct from 500),
          count(*) filter (where outcome = 'failed' and http_status = 503),
          count(*) filter (where outcome = 'failed' and http_status = 429),
          count(*) filter (where outcome = 'fetched'),
          (select coalesce(extract(epoch from max(recent.fetched_at)), 0)::bigint
             from crawl_run_fetches as recent
            where recent.outcome = 'failed'
              and recent.http_status = 429
              and recent.fetched_at >= now() - interval '2 hours')
     from crawl_run_fetches
    where fetched_at >= now() - interval '2 minutes';")
EOF

total=${total:-0}
failures_503=${failures_503:-0}
failures_429=${failures_429:-0}
fetched=${fetched:-0}
last_429_epoch=${last_429_epoch:-0}
last_429_at="never"
if [[ "$last_429_epoch" =~ ^[0-9]+$ ]] && (( last_429_epoch > 0 )); then
  last_429_at=$(date -u -d "@$last_429_epoch" '+%Y-%m-%dT%H:%M:%SZ')
fi
failure_ratio=0
if (( total + fetched > 0 )); then
  failure_ratio=$(awk -v failed="$total" -v fetched="$fetched" \
    'BEGIN { printf "%.4f", failed / (failed + fetched) }')
fi
failure_ratio_triggered=$(awk -v ratio="$failure_ratio" -v threshold="$threshold_failure_ratio" \
  'BEGIN { print (ratio >= threshold) ? 1 : 0 }')

# Find stopped workers by command line: the transient resume unit holding
# their PIDs can be lost across a restart.
resume_stopped_workers() {
  mapfile -t stopped_pids < <(
    ps -eo pid=,stat=,args= \
      | awk '$2 ~ /^T/ && $0 ~ /\/bicam-collection\/\.venv\/bin\/python3 scripts\/run_congress_history\.py/ {print $1}'
  )
  if ((${#stopped_pids[@]} > 0)); then
    kill -CONT "${stopped_pids[@]}" 2>/dev/null || true
    echo "resume_guard_continued pids=${stopped_pids[*]}"
  fi
}

# Quota cooldown is reported separately from "no workers" so a recent 429
# window is never mistaken for a clean idle state.
now_epoch=$(date +%s)
quota_cooldown=0
quota_low_activity=0
# Cooldown means zero successful fetches in the window plus a 429 in the last
# hour; a slow lane's trickle of successes is healthy progress.
if (( fetched == 0 && failures_503 == 0 && (total == 0 || failures_429 == total) )); then
  quota_low_activity=1
fi
if (( quota_low_activity == 1 )) && [[ "$last_429_epoch" =~ ^[0-9]+$ ]] \
  && (( last_429_epoch > 0 && now_epoch - last_429_epoch < 3600 )); then
  quota_cooldown=1
fi
if (( quota_cooldown == 1 )); then
  write_state quota_cooldown
  echo "resume_guard_quota_cooldown failures=$total http_503=$failures_503 http_429=$failures_429 fetched=$fetched last_429_at=$last_429_at"
  exit 0
fi
if systemctl --user is-active --quiet bicam-crawl-resume-guard.timer; then
  write_state paused_waiting_retry
  echo "resume_guard_retry_pending failures=$total http_503=$failures_503 http_429=$failures_429 fetched=$fetched failure_ratio=$failure_ratio"
  exit 0
fi
if (( total < threshold_total || (failures_503 < threshold_503 && failure_ratio_triggered == 0) )); then
  resume_stopped_workers
  write_state healthy
  echo "resume_guard_ok failures=$total http_503=$failures_503 http_429=$failures_429 fetched=$fetched failure_ratio=$failure_ratio"
  exit 0
fi

mapfile -t pids < <(
  pgrep -f '/bicam-collection/.venv/bin/python3 scripts/run_congress_history.py' || true
)
if ((${#pids[@]} == 0)); then
  write_state no_workers
  echo "resume_guard_no_workers failures=$total http_503=$failures_503 http_429=$failures_429 fetched=$fetched failure_ratio=$failure_ratio"
  exit 0
fi

kill -STOP "${pids[@]}" || true
write_state paused
echo "resume_guard_paused failures=$total http_503=$failures_503 fetched=$fetched failure_ratio=$failure_ratio pids=${pids[*]}"
if systemctl --user is-active --quiet bicam-crawl-resume-guard.timer; then
  echo "resume_guard_retry_already_scheduled"
  exit 0
fi
systemd-run --user --unit=bicam-crawl-resume-guard.service \
  --on-active="${resume_after}s" /usr/bin/kill -CONT "${pids[@]}" >/dev/null \
  || echo "resume_guard_retry_schedule_already_present"
