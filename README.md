# bicam-collection

Builds **BICAM**, a research database of U.S. congressional activity. It
crawls the Congress.gov and GovInfo APIs, keeps every response it receives,
reconciles the two sources, and publishes 91 validated tables in eleven
families: bills, amendments, members, congresses, committees, committee
meetings, committee prints, committee reports, hearings, nominations, and
treaties.

New to the project? Read [HANDOFF.md](HANDOFF.md) for production, state, and
open work.

## How it works

1. **Crawl.** One generic engine walks the endpoint graphs declared in
   `specs/*.yaml`. Every distinct response is stored as an immutable version in
   Postgres, with a log of what each crawl expected, fetched, and failed.
   API keys are rotated by [rotisserie](../rotisserie).
2. **Transform.** dbt (`dbt_project/`) turns raw JSON into staging views,
   reconciles Congress.gov against GovInfo, and builds the public `bicam`
   schema.
3. **Reconcile.** Cases the rules cannot settle are queued for a human
   decision; decisions are dbt seeds (see [Reconciliation](#reconciliation)).
4. **Release.** A snapshot is exported, validated against
   `contracts/bicam_public_schema.yaml`, and only then promoted to `latest`.
5. **Operate.** systemd timers run bounded, restartable lanes (`ops/`) that
   share one lock so they never overlap.

## Setup

Requires [uv](https://docs.astral.sh/uv/), `psql` (`postgresql-client-16`),
and a `rotisserie` checkout next to this repo. Docker is needed only for the
bundled database.

```bash
cp .env.example .env    # set CONGRESS_API_KEYS, GOVINFO_API_KEY, DB_PASSWORD
uv sync
```

Keys are free from <https://api.congress.gov/sign-up/> and also work for
GovInfo. Comma-separate several keys to rotate between them.

**Database.** Either start the bundled Postgres 16:

```bash
make up                 # localhost:5433
```

or point `.env` at an existing server. The crawler, dbt, and the ops lanes
all read the same settings:

```bash
DB_HOST=db.example.org
DB_PORT=5432
DB_USER=bicam
DB_PASSWORD=...
DB_NAME=bicam_collection
```

The user needs `CREATE` on the database (dbt creates its own schemas); no
extensions or superuser are required. Then create the tables:

```bash
uv run python scripts/run_migrations.py
```

## Running

```bash
# Crawl one category (see specs/ for the list)
uv run python scripts/run_crawl.py --category bills --mode incremental
uv run python scripts/run_crawl.py --category members --mode full --max-pages 1

# Build and test the models
export DBT_PROFILES_DIR="$PWD/dbt_project"
uv run dbt run --project-dir dbt_project
uv run dbt test --project-dir dbt_project

# Export, validate, and promote a release
make export-snapshot
uv run python scripts/promote_research_snapshot.py \
  --candidate ../reports/exports/<timestamp>/manifest.json --output-dir ../reports/exports
make release-check
```

Crawl modes: `full`, `incremental` (from the last high-water mark),
`refresh`, `resume` (skip stored URLs), `refresh_descendants` and
`backfill_descendants` (revisit children of stored records).

In production these run on timers instead; see [HANDOFF.md](HANDOFF.md#production).

## Reconciliation

Some cases need a person: a GovInfo bill with no Congress.gov counterpart, a
committee-report member without a Bioguide ID, an unknown committee code.
`scripts/reconcile.py` shows those queues, records decisions in the seed CSVs
under `dbt_project/seeds/reconciliation/`, and applies them.

```bash
uv run python scripts/reconcile.py queues                    # open items per queue
uv run python scripts/reconcile.py list member-identities    # inspect a queue

# One decision
uv run python scripts/reconcile.py decide member-identities \
  --key source_member_key='hrpt117-1-117|CRPT-117hrpt117|4d4f...' \
  --decision assign --value N000191 --reason "Unique name/state/Congress match."

# Many decisions: export a worksheet, fill in decision/value/reason, import it
uv run python scripts/reconcile.py list match-exceptions --limit 0 --worksheet review.csv
uv run python scripts/reconcile.py decide match-exceptions --worksheet review.csv

# Load the seeds and rebuild the affected queue models
uv run python scripts/reconcile.py apply
```

| Queue | Decisions |
| --- | --- |
| `match-exceptions` | `confirmed_source_gap`, `out_of_scope`, `needs_source_refresh`, `data_error` |
| `bill-matches`, `committee-report-matches` | `accepted`, `rejected` for a candidate pair |
| `member-identities` | `assign` a Bioguide ID |
| `member-references` | `reviewed_non_federal_participant` |
| `committee-ids` | `map` to a committee code, or `reviewed_authority_exception` |

Decisions expire after a year by default (`--expires YYYY-MM-DD|never`) so
they are re-reviewed against newer source data. `list --include-reviewed`
shows items already closed, including those closed by the automatic
source-gap rules, so any decision can be revisited. Commit the seed CSVs like
code. `apply` waits for the shared ingestion lock; the scheduled lanes rebuild
the public tables on their next run.

## Tests

```bash
uv run pytest           # no database needed
uv run ruff check && uv run mypy src/
make test-db            # storage integration tests on a disposable Postgres
```

## Layout

| Path | Contents |
| --- | --- |
| `src/bicam_collection/` | Crawler engine, HTTP client, storage |
| `specs/` | YAML endpoint traversal specs |
| `migrations/` | Ordered SQL schema migrations |
| `dbt_project/` | Models, decision seeds, data tests |
| `contracts/` | Public schema contract used by validation and export |
| `scripts/` | Python entry points (crawls, backfills, exports, audits, `reconcile.py`) |
| `ops/` | Shell lanes run by systemd |
| `deploy/systemd/user/` | Production systemd units |
| `docs/` | Researcher guide, source policies, plan and status, [pipeline reference](docs/pipeline-reference.md) |
