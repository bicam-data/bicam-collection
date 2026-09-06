#!/usr/bin/env bash
set -u

project_root=/home/rdelano/bicam/bicam-collection
cd "$project_root" || exit 1
set -a
. ./.env
set +a
if [ -z "${BICAM_DB_PASSWORD_OVERRIDE:-}" ]; then
  BICAM_DB_PASSWORD_OVERRIDE="$(docker inspect bicam-collection-postgres -f '{{range .Config.Env}}{{println .}}{{end}}' 2>/dev/null | awk -F= '$1=="POSTGRES_PASSWORD"{print substr($0,index($0,"=")+1)}')"
  export BICAM_DB_PASSWORD_OVERRIDE
fi
if [ -n "${BICAM_DB_PASSWORD_OVERRIDE:-}" ]; then
  export DB_PASSWORD="$BICAM_DB_PASSWORD_OVERRIDE"
fi

exec uv run python scripts/run_congress_history.py \
  --categories amendments --congress-start 1 --congress-end 119 \
  --mode backfill_descendants --max-pages 1000 --page-size 250 \
  --concurrency 4 --category-workers 1 --key-wait-timeout 3600 \
  --request-interval 2.0 --active-key-budget 2
