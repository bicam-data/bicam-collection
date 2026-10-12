#!/usr/bin/env bash
set -u

source "$(dirname "${BASH_SOURCE[0]}")/lib/bicam_lane.sh"
cd "$project_root" || exit 1
bicam_load_project_env

exec uv run python scripts/run_congress_history.py \
  --categories amendments --congress-start 1 --congress-end 119 \
  --mode backfill_descendants --max-pages 1000 --page-size 250 \
  --concurrency 4 --category-workers 1 --key-wait-timeout 3600 \
  --request-interval 2.0 --active-key-budget 2
