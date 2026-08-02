COMPOSE := docker compose
TOOLS_PROFILE := --profile tools
OPS_PROFILE := --profile ops

.PHONY: help init-env build up up-ops down logs psql migrate recover-stale reconcile reconcile-committee-reports reconcile-bills crawl crawl-bounded crawl-one dbt-run dbt-test crawler-shell dbt-shell

help:
	@printf "Available targets:\n"
	@printf "  init-env      Copy .env.example to .env if .env does not exist\n"
	@printf "  build         Build the app image used by crawler and dbt services\n"
	@printf "  up            Start Postgres\n"
	@printf "  up-ops        Start Postgres plus the backup sidecar\n"
	@printf "  down          Stop the compose stack\n"
	@printf "  logs          Tail Postgres logs\n"
	@printf "  psql          Open psql against the compose Postgres instance\n"
	@printf "  migrate       Apply ordered SQL migrations inside the crawler image\n"
	@printf "  recover-stale Inspect stale runs; pass ARGS=\"--apply\" to mark them abandoned\n"
	@printf "  reconcile    Build committee-report reconciliation models and tests\n"
	@printf "  reconcile-committee-reports Refresh committee-report source evidence, matches, and coverage\n"
	@printf "  reconcile-bills Refresh bill package evidence, matches, and coverage\n"
	@printf "  crawl         Run scripts/run_multi_crawl.py inside the crawler container; pass ARGS=\"...\"\n"
	@printf "  crawl-bounded Run the declared Congress working scope with conservative limits\n"
	@printf "  crawl-one     Run scripts/run_crawl.py inside the crawler container; pass ARGS=\"...\"\n"
	@printf "  dbt-run       Run dbt run inside the dbt container; pass ARGS=\"...\"\n"
	@printf "  dbt-test      Run dbt test inside the dbt container; pass ARGS=\"...\"\n"
	@printf "  crawler-shell Open a shell in the crawler image\n"
	@printf "  dbt-shell     Open a shell in the dbt image\n"

init-env:
	@test -f .env || cp .env.example .env

build:
	$(COMPOSE) build crawler

up:
	$(COMPOSE) up -d postgres

up-ops:
	$(COMPOSE) $(OPS_PROFILE) up -d postgres postgres-backup

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f postgres

psql:
	$(COMPOSE) exec postgres psql -U $${DB_USER:-admin} -d $${DB_NAME:-bicam_collection}

migrate:
	$(COMPOSE) $(TOOLS_PROFILE) run --rm crawler uv run python scripts/run_migrations.py

recover-stale:
	$(COMPOSE) $(TOOLS_PROFILE) run --rm crawler uv run python scripts/recover_stale_runs.py $(ARGS)

reconcile:
	$(COMPOSE) $(TOOLS_PROFILE) run --rm dbt dbt build --project-dir dbt_project --select tag:committee_report_reconciliation

reconcile-committee-reports:
	$(COMPOSE) $(TOOLS_PROFILE) run --rm dbt dbt build --project-dir dbt_project --select +int_committee_reports__congress
	$(COMPOSE) $(TOOLS_PROFILE) run --rm crawler uv run python scripts/backfill_govinfo_committee_reports.py --limit $${REPORT_BACKFILL_LIMIT:-5000} --concurrency $${GOVINFO_BACKFILL_CONCURRENCY:-3}
	$(COMPOSE) $(TOOLS_PROFILE) run --rm dbt dbt build --project-dir dbt_project --select +fct_committee_report_matches
	$(COMPOSE) $(TOOLS_PROFILE) run --rm crawler uv run python scripts/backfill_govinfo_committee_reports.py --granules-for-unmatched --limit $${REPORT_BACKFILL_LIMIT:-5000} --concurrency $${GOVINFO_BACKFILL_CONCURRENCY:-3}
	$(COMPOSE) $(TOOLS_PROFILE) run --rm dbt dbt build --project-dir dbt_project --select +fct_committee_report_matches fct_collection_coverage

reconcile-bills:
	$(COMPOSE) $(TOOLS_PROFILE) run --rm dbt dbt build --project-dir dbt_project --select +stg_congress__bill_detail
	$(COMPOSE) $(TOOLS_PROFILE) run --rm crawler uv run python scripts/backfill_congress_bill_texts.py --limit $${BILL_TEXT_BACKFILL_LIMIT:-5000} --concurrency $${CONGRESS_BACKFILL_CONCURRENCY:-3}
	$(COMPOSE) $(TOOLS_PROFILE) run --rm dbt dbt build --project-dir dbt_project --select +stg_congress__bill_texts
	$(COMPOSE) $(TOOLS_PROFILE) run --rm crawler uv run python scripts/backfill_govinfo_bills.py --limit $${BILL_BACKFILL_LIMIT:-5000} --concurrency $${GOVINFO_BACKFILL_CONCURRENCY:-3}
	$(COMPOSE) $(TOOLS_PROFILE) run --rm dbt dbt build --project-dir dbt_project --select +fct_bill_matches fct_collection_coverage

crawl:
	$(COMPOSE) $(TOOLS_PROFILE) run --rm crawler uv run python scripts/run_multi_crawl.py $(ARGS)

crawl-bounded:
	$(COMPOSE) $(TOOLS_PROFILE) run --rm crawler uv run python scripts/run_multi_crawl.py \
		--categories amendments bills committee-meetings committee-prints committee-reports committees hearings nominations treaties \
		--mode $${CRAWL_MODE:-resume} --max-pages $${MAX_PAGES:-3} --page-size $${PAGE_SIZE:-250} \
		--concurrency $${CONCURRENCY:-3} --category-workers $${CATEGORY_WORKERS:-1} $(ARGS)

crawl-one:
	$(COMPOSE) $(TOOLS_PROFILE) run --rm crawler uv run python scripts/run_crawl.py $(ARGS)

dbt-run:
	$(COMPOSE) $(TOOLS_PROFILE) run --rm dbt dbt run --project-dir dbt_project $(ARGS)

dbt-test:
	$(COMPOSE) $(TOOLS_PROFILE) run --rm dbt dbt test --project-dir dbt_project $(ARGS)

crawler-shell:
	$(COMPOSE) $(TOOLS_PROFILE) run --rm crawler bash

dbt-shell:
	$(COMPOSE) $(TOOLS_PROFILE) run --rm dbt bash
