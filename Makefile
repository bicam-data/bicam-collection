COMPOSE := docker compose
TOOLS_PROFILE := --profile tools
OPS_PROFILE := --profile ops

.PHONY: help init-env build up up-ops down logs psql crawl crawl-one dbt-run dbt-test crawler-shell dbt-shell

help:
	@printf "Available targets:\n"
	@printf "  init-env      Copy .env.example to .env if .env does not exist\n"
	@printf "  build         Build the app image used by crawler and dbt services\n"
	@printf "  up            Start Postgres\n"
	@printf "  up-ops        Start Postgres plus the backup sidecar\n"
	@printf "  down          Stop the compose stack\n"
	@printf "  logs          Tail Postgres logs\n"
	@printf "  psql          Open psql against the compose Postgres instance\n"
	@printf "  crawl         Run scripts/run_multi_crawl.py inside the crawler container; pass ARGS=\"...\"\n"
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

crawl:
	$(COMPOSE) $(TOOLS_PROFILE) run --rm crawler uv run python scripts/run_multi_crawl.py $(ARGS)

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
