#!/usr/bin/env bash
set -u

source "$(dirname "${BASH_SOURCE[0]}")/lib/bicam_lane.sh"

cd "$project_root" || exit 1
bicam_load_project_env
bicam_prefer_container_db_password
# This lane has an independent client process. Keep its budget explicit and
# bounded when it runs outside the main supervisor; the live API applies its
# rolling window per credential.
export BICAM_CONGRESS_GLOBAL_RATE_LIMIT="${BICAM_AMENDMENT_REFRESH_GLOBAL_RATE_LIMIT:-4000}"
export BICAM_CONGRESS_PER_KEY_RATE_LIMIT="${BICAM_AMENDMENT_REFRESH_PER_KEY_RATE_LIMIT:-20000}"

# Pace the bounded maintenance lane instead of spending its allowance in a
# burst that could trigger a credential cooldown.
exec "$project_root/.venv/bin/python" scripts/run_congress_descendant_refresh.py \
  --categories amendments \
  --batch-size "${BICAM_AMENDMENT_REFRESH_BATCH_SIZE:-500}" \
  --concurrency "${BICAM_AMENDMENT_REFRESH_CONCURRENCY:-8}" \
  --page-size 250 \
  --max-pages 1000 \
  --request-interval "${BICAM_AMENDMENT_REFRESH_REQUEST_INTERVAL:-0.95}" \
  --key-wait-timeout 3600 \
  --active-key-budget "${BICAM_AMENDMENT_REFRESH_ACTIVE_KEY_BUDGET:-4}"
