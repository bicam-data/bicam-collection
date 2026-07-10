# bicam-collection

Spec-driven raw crawler and dbt staging foundation for Congress.gov data.

## Current Truth

- The crawler is generic over YAML specs in `specs/`; current implemented categories are `bills`, `members`, `congresses`, `committees`, `amendments`, `committee-reports`, `committee-prints`, `hearings`, `committee-meetings`, `nominations`, and `treaties`.
- The crawler now also has its first GovInfo category, `govinfo-committee-reports`, which traverses collection updates -> package summary -> granules -> granule summary for `CRPT`.
- Raw crawl storage is centered on `raw_responses`, `crawl_runs`, and `crawl_run_fetches`.
- dbt is initialized for staging models only; marts and GovInfo are still out of scope.
- Member sponsored and cosponsored legislation can include amendments; dbt now exposes separate sponsored/cosponsored amendment staging views and derives amendment `type` from the amendment URL when the source `type` is null.
- Committee staging currently includes committee detail, committee history, committee bills, committee reports, and committee nominations. Communications are intentionally excluded for now.
- Amendment staging currently includes amendment detail, sponsors, committee sponsors, on-behalf-of sponsors, cosponsors, actions, amendments-to-amendment, texts, and three detail-derived amended-* mapping tables.
- Committee-report staging currently includes committee report detail, detail-derived committees, detail-derived bills, detail-derived treaties, and the text child endpoint.
- Committee-print staging currently includes committee print detail, detail-derived committees, detail-derived bills, and the text child endpoint.
- Hearing staging currently includes hearing detail, detail-derived committees, detail-derived dates, and detail-derived formats, with `associatedMeeting` flattened into `event_id` and `event_id_url`.
- Committee-meeting staging currently includes detail, committees, continuations, location, videos, witnesses, witness documents, meeting documents, related bills, related treaties, and related nominations, with `hearingTranscript` flattened into hearing jacket and URL columns.
- Nomination staging currently includes detail, actions, committees, hearings, detail-derived nominees, and nominee child details, with `nominationType` flattened into `is_civilian` and `is_military`.
- Treaty staging currently includes detail, actions, committees, detail-derived countries/parties, index terms, related docs, and titles, with only `parts_count` retained from the `parts` field.

## Docker Stack

The repo now includes a production-oriented local stack in [docker-compose.yaml](/home/rdelano/bicam/bicam-collection/docker-compose.yaml):

- `postgres`: persistent Postgres 16 with safer defaults, `pg_stat_statements`, and healthchecks
- `postgres-backup`: optional logical backup sidecar
- `crawler`: app image for long-running crawl commands
- `dbt`: app image for dbt runs and tests

First-time setup:

```bash
cp .env.example .env
```

Start the database:

```bash
make up
```

Start the database plus the backup sidecar:

```bash
make up-ops
```

Run the crawler inside the stack:

```bash
make crawl ARGS="--mode full --category-workers 2 --concurrency 5"
```

Run dbt inside the stack:

```bash
make dbt-run
make dbt-test
```

The app containers build against the local [`rotisserie`](/home/rdelano/bicam/rotisserie) checkout, so the Docker image uses the same dependency source as local `uv`.

Common shortcuts are defined in [Makefile](/home/rdelano/bicam/bicam-collection/Makefile). Run `make help` to see the available targets.

## Run The Crawler

```bash
uv run python scripts/run_crawl.py --category bills --mode full
uv run python scripts/run_crawl.py --category members --mode full --max-pages 1
uv run python scripts/run_crawl.py --category congresses --mode full --max-pages 1
uv run python scripts/run_crawl.py --category committees --mode full --max-pages 1
uv run python scripts/run_crawl.py --category amendments --mode full --max-pages 1
uv run python scripts/run_crawl.py --category committee-reports --mode full --max-pages 1
uv run python scripts/run_crawl.py --category committee-prints --mode full --max-pages 1
uv run python scripts/run_crawl.py --category hearings --mode full --max-pages 1
uv run python scripts/run_crawl.py --category committee-meetings --mode full --max-pages 1
uv run python scripts/run_crawl.py --category nominations --mode full --max-pages 1
uv run python scripts/run_crawl.py --category treaties --mode full --max-pages 1
```

Modes:

- `full`: full seed traversal without incremental filtering
- `incremental`: uses the stored high-water mark when `--from-datetime` is omitted and refetches returned subtrees
- `refresh`: explicit refetch mode without automatic high-water-mark lookup
- `resume`: uses the stored high-water mark when `--from-datetime` is omitted and skips URLs already present in `raw_responses`
- `refresh_descendants`: uses a category's configured refresh root node and refetches its stored child collection URLs without revisiting the seed

The crawler reads source-specific API keys plus `DATABASE_URL` from the environment. If they are not set, it will also read them from a local `.env` file.

- Congress.gov: `CONGRESS_API_KEYS`, `CONGRESSIONAL_API_KEY`, or `CONGRESS_API_KEY_*`
- GovInfo: `GOVINFO_API_KEYS`, `GOVINFO_API_KEY`, or `GOVINFO_API_KEY_*`

API key rotation is handled by the local [`rotisserie`](/home/rdelano/bicam/rotisserie) checkout through `uv`.

## Run dbt

```bash
export DBT_PROFILES_DIR=/home/rdelano/bicam/bicam-collection/dbt_project
export DB_PASSWORD='<your-postgres-password>'
uv run dbt run --project-dir dbt_project
uv run dbt test --project-dir dbt_project
```

The bundled `profiles.yml` expects Postgres connection settings via `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `DB_NAME`, and `DB_SCHEMA`.

Right now staging models in [dbt_project.yml](/home/rdelano/bicam/bicam-collection/dbt_project/dbt_project.yml) are materialized as `view`. The expected next evolution is to keep lightweight staging where that still works, then move heavier marts to `table` or `incremental` materializations as the warehouse grows.

## Schema migrations

SQL migrations in `migrations/` are applied once and tracked in
`schema_migrations`. Apply them before a crawl or dbt run that depends on a new
schema feature:

```bash
make migrate
```

`001_raw_response_versions.sql` creates an append-only record of each distinct
payload observed at an endpoint. `raw_responses` remains the current-state
table, while `raw_response_versions` preserves source evidence for repeatable
transformations and reconciliation.

## Committee-report reconciliation

The initial reconciliation slice compares Congress.gov and GovInfo committee
reports by a shared canonical identity: report type, number, Congress, part,
and errata status. It produces deterministic candidates in
`int_committee_report_candidates` and reviewed outcomes in
`fct_committee_report_matches`.

Add an explicit human decision to
`dbt_project/seeds/reconciliation/committee_report_match_overrides.csv` when a
candidate must be accepted or rejected outside the deterministic rule. The dbt
tests enforce unique accepted pairings and non-null canonical identities.

After collecting either committee-report source, run the complete incremental
evidence and reconciliation workflow:

```bash
make reconcile-committee-reports
```

It first refreshes Congress.gov report identities, then retrieves only missing
GovInfo package summaries and granules needed for unmatched report parts, and
finally rebuilds the match and coverage models. It uses a default ceiling of
three concurrent GovInfo requests. Set `REPORT_BACKFILL_LIMIT` (default `5000`)
or `GOVINFO_BACKFILL_CONCURRENCY` only when a deliberately different operating
limit is appropriate.
