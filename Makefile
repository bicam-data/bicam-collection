COMPOSE := docker compose
TOOLS_PROFILE := --profile tools
OPS_PROFILE := --profile ops
TOOLS_RUN := $(COMPOSE) $(TOOLS_PROFILE) run --build --rm

.PHONY: help init-env build up up-ops down logs psql migrate recover-stale quality-gate schedule reconcile reconcile-committee-reports reconcile-bills reconcile-bill-details release-manifest export-snapshot release-check archive-parity html-text html-text-progress source-gap-report crawl crawl-bounded crawl-one benchmark-crawl dbt-run dbt-test crawler-shell dbt-shell

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
	@printf "  quality-gate Check stale runs, reconciliation quality, open exceptions, and coverage\n"
	@printf "  schedule      Run a lock-protected bounded job; pass ARGS=\"--job ...\"\n"
	@printf "  reconcile    Build committee-report reconciliation models and tests\n"
	@printf "  reconcile-committee-reports Refresh committee-report source evidence, matches, and coverage\n"
	@printf "  reconcile-bills Refresh bill package evidence, matches, and coverage\n"
	@printf "  reconcile-bill-details Backfill Congress details for GovInfo-only bills; pass ARGS=\"...\"\n"
	@printf "  release-manifest Write a reproducible main-11 coverage/reconciliation manifest\n"
	@printf "  export-snapshot Write an immutable CSV/GZIP main-11 research snapshot\n"
	@printf "  release-check  Verify snapshot integrity, freshness, and quality gates\n"
  @printf "  html-text      Fetch a bounded, resumable batch of Congress.gov HTML text\n"
  @printf "  html-text-progress Write per-table HTML extraction progress\n"
	@printf "  source-gap-report Write the reconciliation source-gap queue report\n"
	@printf "  archive-parity Compare current bicam counts with published contract archives\n"
	@printf "  crawl         Run scripts/run_multi_crawl.py inside the crawler container; pass ARGS=\"...\"\n"
	@printf "  crawl-bounded Run the declared Congress working scope with conservative limits\n"
	@printf "  crawl-one     Run scripts/run_crawl.py inside the crawler container; pass ARGS=\"...\"\n"
	@printf "  benchmark-crawl Measure bounded child-resource concurrency without network I/O\n"
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
	$(TOOLS_RUN) crawler uv run python scripts/run_migrations.py

recover-stale:
	$(TOOLS_RUN) crawler uv run python scripts/recover_stale_runs.py $(ARGS)

quality-gate:
	$(TOOLS_RUN) crawler uv run python scripts/check_quality_gates.py $(ARGS)

schedule:
	$(TOOLS_RUN) crawler uv run python scripts/run_scheduled_batch.py $(ARGS)

reconcile:
	$(TOOLS_RUN) dbt dbt build --project-dir dbt_project --select tag:committee_report_reconciliation

reconcile-committee-reports:
	$(TOOLS_RUN) dbt dbt build --project-dir dbt_project --select +int_committee_reports__congress
	$(TOOLS_RUN) crawler uv run python scripts/backfill_govinfo_committee_reports.py --limit $${REPORT_BACKFILL_LIMIT:-5000} --concurrency $${GOVINFO_BACKFILL_CONCURRENCY:-3}
	$(TOOLS_RUN) dbt dbt build --project-dir dbt_project --select +fct_committee_report_matches
	$(TOOLS_RUN) crawler uv run python scripts/backfill_govinfo_committee_reports.py --granules-for-unmatched --limit $${REPORT_BACKFILL_LIMIT:-5000} --concurrency $${GOVINFO_BACKFILL_CONCURRENCY:-3}
	$(TOOLS_RUN) dbt dbt build --project-dir dbt_project --select +fct_committee_report_matches +fct_collection_coverage

reconcile-bills:
	$(TOOLS_RUN) dbt dbt build --project-dir dbt_project --select +stg_congress__bill_detail
	$(TOOLS_RUN) crawler uv run python scripts/backfill_congress_bill_texts.py --limit $${BILL_TEXT_BACKFILL_LIMIT:-5000} --concurrency $${CONGRESS_BACKFILL_CONCURRENCY:-3}
	$(TOOLS_RUN) dbt dbt build --project-dir dbt_project --select +stg_congress__bill_texts
	$(TOOLS_RUN) crawler uv run python scripts/backfill_govinfo_bills.py --limit $${BILL_BACKFILL_LIMIT:-5000} --concurrency $${GOVINFO_BACKFILL_CONCURRENCY:-3}
	$(TOOLS_RUN) dbt dbt build --project-dir dbt_project --select +fct_bill_matches +fct_collection_coverage

reconcile-treaty-documents:
	$(TOOLS_RUN) dbt dbt build --project-dir dbt_project --select +stg_congress__treaty_detail
	$(TOOLS_RUN) crawler uv run python scripts/backfill_govinfo_treaty_documents.py --limit $${TREATY_DOCUMENT_BACKFILL_LIMIT:-100} --concurrency $${GOVINFO_BACKFILL_CONCURRENCY:-3}
	$(TOOLS_RUN) dbt dbt build --project-dir dbt_project --select +treaties +fct_collection_coverage

reconcile-bill-details:
	$(TOOLS_RUN) crawler uv run python scripts/backfill_congress_bill_details.py $(ARGS)
	$(TOOLS_RUN) dbt dbt build --project-dir dbt_project --select +fct_bill_matches +fct_reconciliation_exceptions +fct_reconciliation_metrics +fct_collection_coverage

release-manifest:
	$(TOOLS_RUN) crawler uv run python scripts/write_release_manifest.py $(ARGS)

export-snapshot:
	$(TOOLS_RUN) crawler uv run python scripts/export_research_snapshot.py $(ARGS)

release-check:
	$(TOOLS_RUN) crawler uv run python scripts/check_release_freshness.py $(ARGS)

html-text:
	./scripts/run_html_text_batches.sh

html-text-progress:
	uv run python scripts/report_html_text_progress.py $(ARGS)

source-gap-report:
	uv run python scripts/report_source_gap_queue.py $(ARGS)

archive-parity:
	$(TOOLS_RUN) crawler uv run python scripts/compare_public_archive.py $(ARGS)

crawl:
	$(TOOLS_RUN) crawler uv run python scripts/run_multi_crawl.py $(ARGS)

crawl-bounded:
	$(TOOLS_RUN) crawler uv run python scripts/run_multi_crawl.py \
		--categories amendments bills committee-meetings committee-prints committee-reports committees hearings nominations treaties \
		--mode $${CRAWL_MODE:-resume} --max-pages $${MAX_PAGES:-3} --page-size $${PAGE_SIZE:-250} \
		--concurrency $${CONCURRENCY:-3} --category-workers $${CATEGORY_WORKERS:-1} $(ARGS)

crawl-one:
	$(TOOLS_RUN) crawler uv run python scripts/run_crawl.py $(ARGS)

benchmark-crawl:
	$(TOOLS_RUN) crawler uv run python scripts/benchmark_async_crawl.py $(ARGS)

dbt-run:
	$(TOOLS_RUN) dbt dbt run --project-dir dbt_project $(ARGS)

dbt-test:
	$(TOOLS_RUN) dbt dbt test --project-dir dbt_project $(ARGS)

crawler-shell:
	$(TOOLS_RUN) crawler bash

dbt-shell:
	$(TOOLS_RUN) dbt bash
