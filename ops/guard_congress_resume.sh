#!/usr/bin/env bash
set -u

source "$(dirname "${BASH_SOURCE[0]}")/lib/bicam_lane.sh"

# Recurring guard for a coordinated Congress.gov outage. It is intentionally
# conservative: ordinary isolated failures must not pause a healthy crawl.
threshold_total=${BICAM_RESUME_GUARD_FAILURE_THRESHOLD:-20}
threshold_503=${BICAM_RESUME_GUARD_503_THRESHOLD:-10}
threshold_failure_ratio=${BICAM_RESUME_GUARD_FAILURE_RATIO:-0.50}
resume_after=${BICAM_RESUME_GUARD_RETRY_SECONDS:-600}
quota_low_activity_limit=${BICAM_RESUME_GUARD_QUOTA_LOW_ACTIVITY_LIMIT:-1000}
state_file=${BICAM_RESUME_GUARD_STATE_FILE:-$reports_root/availability-guard-latest.txt}
cooldown_marker=${BICAM_CONGRESS_COOLDOWN_MARKER:-$reports_root/congress-api-cooldown-until}

write_state() {
  local state=$1
  local failures=$2
  local failures_503=$3
  local failures_429=$4
  local fetched=$5
  local failure_ratio=$6
  local last_429_at=$7
  mkdir -p "$(dirname "$state_file")"
  {
    printf 'generated_at=%s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
    printf 'state=%s\n' "$state"
    printf 'failures_2m=%s\n' "$failures"
    printf 'http_503_2m=%s\n' "$failures_503"
    printf 'http_429_2m=%s\n' "$failures_429"
    printf 'fetched_2m=%s\n' "$fetched"
    printf 'failure_ratio=%s\n' "$failure_ratio"
    printf 'last_http_429_at=%s\n' "$last_429_at"
  } >"$state_file"
}

# Congress.gov emits a bounded set of deterministic 500 responses for some
# historical URLs.  They remain in the durable failure ledger and are handled
# by the explicit source-500 retry stage, but must not be mistaken for a
# coordinated outage that stops every healthy descendant lane.
read -r total failures_503 failures_429 fetched last_429_epoch <<EOF
$(docker exec bicam-collection-postgres psql -U admin -d bicam_collection -At -F ' ' -c \
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

resume_stopped_workers() {
  # A prior guard invocation may have stopped a worker and then lost its
  # transient systemd resume unit across a worker restart.  Once the live
  # window is healthy, discover stopped current workers by command line rather
  # than trusting the old PID embedded in that unit.
  mapfile -t stopped_pids < <(
    ps -eo pid=,stat=,args= \
      | awk '$2 ~ /^T/ && $0 ~ /\/bicam-collection\/\.venv\/bin\/python3 scripts\/run_congress_history\.py/ {print $1}'
  )
  if ((${#stopped_pids[@]} > 0)); then
    kill -CONT "${stopped_pids[@]}" 2>/dev/null || true
    echo "resume_guard_continued pids=${stopped_pids[*]}"
  fi
}

# A worker can be completely idle—or may have been stopped by the circuit
# breaker—while every credential is cooling after a coordinated 429 burst.
# Preserve that distinction in the durable artifact even when no crawler
# process remains, so the supervisor does not mistake a recent source window
# for a clean no-worker state.
now_epoch=$(date +%s)
quota_cooldown=0
quota_low_activity=0
# A coordinated 429 circuit writes a durable marker with the server-provided
# Retry-After window. Treat that marker as authoritative even when a small
# number of already-in-flight requests completed in the same reporting
# window; otherwise the supervisor can launch retries while the source is
# still cooling down.
marker_until=$(cat "$cooldown_marker" 2>/dev/null || true)
if [[ "$marker_until" =~ ^[0-9]+$ ]] && (( marker_until > now_epoch )); then
  quota_cooldown=1
fi
# A small but nonzero successful sample is normal for the dedicated amendment
# reserve lane (about 4,000 requests/hour).  Do not call that healthy progress
# a coordinated quota cooldown: cooldown requires that the two-minute window
# contain no successful network fetches at all.
if (( fetched == 0 && failures_503 == 0 && (total == 0 || failures_429 == total) )); then
  quota_low_activity=1
fi
if (( quota_cooldown == 0 && quota_low_activity == 1 )) && [[ "$last_429_epoch" =~ ^[0-9]+$ ]] \
  && (( last_429_epoch > 0 && now_epoch - last_429_epoch < 3600 )); then
  quota_cooldown=1
fi
if (( quota_cooldown == 1 )); then
  write_state quota_cooldown "$total" "$failures_503" "$failures_429" "$fetched" "$failure_ratio" "$last_429_at"
  echo "resume_guard_quota_cooldown failures=$total http_503=$failures_503 http_429=$failures_429 fetched=$fetched last_429_at=$last_429_at"
  exit 0
fi
if systemctl --user is-active --quiet bicam-crawl-resume-guard.timer; then
  write_state paused_waiting_retry "$total" "$failures_503" "$failures_429" "$fetched" "$failure_ratio" "$last_429_at"
  echo "resume_guard_retry_pending failures=$total http_503=$failures_503 http_429=$failures_429 fetched=$fetched failure_ratio=$failure_ratio"
  exit 0
fi
if (( total < threshold_total || (failures_503 < threshold_503 && failure_ratio_triggered == 0) )); then
  resume_stopped_workers
  write_state healthy "$total" "$failures_503" "$failures_429" "$fetched" "$failure_ratio" "$last_429_at"
  echo "resume_guard_ok failures=$total http_503=$failures_503 http_429=$failures_429 fetched=$fetched failure_ratio=$failure_ratio"
  exit 0
fi

mapfile -t pids < <(
  pgrep -f '/bicam-collection/.venv/bin/python3 scripts/run_congress_history.py' || true
)
if ((${#pids[@]} == 0)); then
  write_state no_workers "$total" "$failures_503" "$failures_429" "$fetched" "$failure_ratio" "$last_429_at"
  echo "resume_guard_no_workers failures=$total http_503=$failures_503 http_429=$failures_429 fetched=$fetched failure_ratio=$failure_ratio"
  exit 0
fi

kill -STOP "${pids[@]}" || true
write_state paused "$total" "$failures_503" "$failures_429" "$fetched" "$failure_ratio" "$last_429_at"
echo "resume_guard_paused failures=$total http_503=$failures_503 fetched=$fetched failure_ratio=$failure_ratio pids=${pids[*]}"
if systemctl --user is-active --quiet bicam-crawl-resume-guard.timer; then
  echo "resume_guard_retry_already_scheduled"
  exit 0
fi
systemd-run --user --unit=bicam-crawl-resume-guard.service \
  --on-active="${resume_after}s" /usr/bin/kill -CONT "${pids[@]}" >/dev/null \
  || echo "resume_guard_retry_schedule_already_present"
