# bicam-collection handoff

Start here. This is the map; [README.md](README.md) is the detailed reference
for each subsystem.

## What this is

BICAM is a research-grade relational database of eleven congressional data
families (bills, amendments, members, congresses, committees, committee
meetings, committee prints, committee reports, hearings, nominations,
treaties). Congress.gov is authoritative; GovInfo is supplemental evidence.

The pipeline has four layers:

1. **Crawl** — `src/bicam_collection/` is one generic engine (`SpecCrawler`)
   driven by YAML specs in `specs/`. There is no category-specific crawl code;
   a new endpoint is a new spec. Raw responses are stored immutably
   (`raw_responses`, `raw_response_versions`, `crawl_run_*` tables).
2. **Transform** — `dbt_project/` stages raw JSON, reconciles Congress.gov
   against GovInfo, and builds the public `bicam` schema
   (`models/marts/bicam/`) plus reconciliation and operations marts.
3. **Release** — `scripts/export_research_snapshot.py` writes an immutable
   candidate snapshot; `scripts/promote_research_snapshot.py` validates it and
   atomically swaps `reports/exports/latest`. Contracts in `contracts/` declare
   the public tables, keys, and foreign keys that validation enforces.
4. **Operate** — systemd user timers run bounded, restartable lanes through the
   shell wrappers in `ops/`.

API key rotation and rate limiting come from the sibling
[`rotisserie`](https://github.com/ryandelano/rotisserie) library, installed as
an editable path dependency (`../rotisserie`).

## Repository layout

| Path | Contents |
| --- | --- |
| `src/bicam_collection/` | Crawler engine, HTTP client, storage, spec loader, export helpers |
| `specs/` | YAML endpoint traversal specs (one per category) |
| `migrations/` | Ordered SQL schema migrations (`scripts/run_migrations.py`) |
| `dbt_project/` | Staging, intermediate, reconciliation, and public mart models; seeds; data tests |
| `contracts/` | Public schema and entity contracts used by validation and export |
| `scripts/` | Python entry points: crawls, backfills, exports, audits, release tooling |
| `ops/` | Shell lane wrappers run by systemd; `ops/lib/bicam_lane.sh` is the shared preamble |
| `deploy/systemd/user/` | The production systemd user units and drop-ins (source of truth) |
| `deploy/systemd/` | Lane-status drop-in and failure-alert template (not yet installed) |
| `docs/` | Researcher guide, source policies, active plan and status tracker |
| `tests/` | pytest suite and fixtures |
| `api_schemas/` | Example response shapes for Congress.gov endpoint families |

## Getting started

Prerequisites: Docker, [uv](https://docs.astral.sh/uv/), and a checkout of
`rotisserie` next to this repo:

```text
~/bicam/
├── bicam-collection/   (this repo)
├── rotisserie/         (git@github.com:ryandelano/rotisserie.git)
└── reports/            (created by the lanes; checkpoints, logs, exports)
```

```bash
cp .env.example .env          # fill in CONGRESS_API_KEYS, GOVINFO_API_KEY, DB_PASSWORD
make up && make migrate       # Postgres 16 on localhost:5433
uv sync
uv run pytest                 # ~690 tests, no database needed
uv run ruff check && uv run mypy src/
```

API keys are free from <https://api.congress.gov/sign-up/> (also valid for
GovInfo). Multiple comma-separated keys are rotated by `rotisserie`.

Tests that need external resources are skipped unless their env var is set:

| Env var | Enables |
| --- | --- |
| `BICAM_TEST_DATABASE_URL` | Storage integration tests (`make test-db` starts a disposable Postgres) |
| `BICAM_RUN_VERSION_REPLAY=1` | Bill-version replay tests (disposable database) |
| `BICAM_REPLAY_BUILD_DIR` | Raw-replay generation verification |
| `BICAM_PUBLISHED_ARCHIVE_DIR` | Contract check against the published 2025 CSV archives |

`make help` lists the Docker-based shortcuts (crawl, dbt, release checks).

## Production

Production runs on one host as systemd **user** units. All lanes share the
lock `/run/user/$UID/bicam-ingestion.lock`, so they never overlap and a
lock-busy run simply retries on the next timer.

Enabled timers:

| Timer | Schedule | Purpose |
| --- | --- | --- |
| `bicam-congress-availability-guard` | every 2 min | Pauses crawling during coordinated Congress.gov outages / 429 cooldowns |
| `bicam-congress-incremental-refresh` | every 6 h | Date-filtered refresh of the main-11 Congress.gov lists, then reconciliation |
| `bicam-congress-supplemental-refresh` | every 6 h | Supplemental endpoint families; descends only where coverage finds a gap |
| `bicam-govinfo-cursor-continuation` | every 6 h | GovInfo incremental date windows (`ops/run_govinfo_incremental_windows.sh`) |
| `bicam-govinfo-cdir-descendant-refresh` | 4× daily | GovInfo Congressional Directory descendants |
| `bicam-govinfo-supplemental-refresh` | daily 03:15 UTC | GovInfo CDIR and treaty-document refresh |
| `bicam-html-text` | every 10 min | Bounded, resumable HTML text extraction |
| `bicam-fetch-lineage-backfill` | every 15 min | Repairs legacy fetch-lineage rows in batches |
| `bicam-ingestion-report` | every 6 h | Writes `reports/ingestion-status-latest.md` |
| `bicam-ingestion-audit` | every 6 h | Read-only ingestion/reconciliation audit |
| `bicam-research-snapshot` | daily 05:00 UTC | Exports the portable research snapshot and checks freshness |

The remaining units in `deploy/systemd/user/` (historical partition queue,
per-key ramp, amendment lanes, supervisor, cooldown resume, list-page repair)
are disabled. They are kept for explicitly supervised backfills.

Useful commands:

```bash
systemctl --user list-timers 'bicam-*'
journalctl --user -u bicam-congress-incremental-refresh -n 100
cat ~/bicam/reports/ingestion-status-latest.md
```

To install or update the units from this repo, see
[deploy/systemd/user/README.md](deploy/systemd/user/README.md).

## The reports directory

`reports/` lives **outside** the repo (default `../reports`, override with
`BICAM_REPORTS_DIR`) and is several hundred GB. It is not in git and is not
reproducible from git alone. What matters in it:

| Path | Contents |
| --- | --- |
| `exports/` | Immutable research snapshots; `exports/latest` is the promoted release (bulk of the size) |
| `quality/` | Candidate exports, review packets, and quality audits referenced from the status doc |
| `deliverables/` | Packaged outputs |
| `release-manifests/` | Coverage/reconciliation manifests per release |
| `*-latest.md`, `*-checkpoint*`, `*-cursor.json` | Lane checkpoints and durable cursors; lanes resume from these, do not delete |
| `*.log` | Lane logs |

The database is the other piece of state: the `bicam-collection-postgres`
container volume. Backups are produced by the optional `postgres-backup`
sidecar (`make up-ops`).

## Current state and open work

The active plan is
[docs/research-quality-remediation-plan-20260904.md](docs/research-quality-remediation-plan-20260904.md);
its live tracker is
[docs/research-quality-implementation-status-20260905.md](docs/research-quality-implementation-status-20260905.md)
(read the "Package status" table and "Next implementation order"). In short:
the corrected release has not been promoted yet. Packages A–G (tests, child
state, public grains, contract validation, promotion, build evidence, CI) are
in progress; H (reproducibility evidence) and I (researcher documentation) are
partial. The most recent work is the Congress-119 export candidate series
under `reports/quality/congress119-export-20260910/`.

Other policy docs in `docs/` record decisions that are easy to get wrong:
source boundaries, reciprocity, hearing linkage, and GovInfo identity quality.

## Known issues and gotchas

- **Branches.** Active work is on `feature/reconciliation-foundations`, far
  ahead of `main`, which has not been updated since 2025. `origin/productionize`
  is an abandoned earlier attempt and is superseded; do not merge it.
- **rotisserie has diverged.** Its `main` on GitHub has the 0.1.x release
  rewrite. Older local fixes (funnel policy, Retry-After normalization) are on
  `origin/local-unreleased-fixes` and conflict with the release. Reconcile
  before bumping the dependency. The production host still runs the older
  local checkout.
- **Database credentials.** `.env.example` and the ops scripts use DB user
  `admin`; older notes that say `postgres/postgres` are stale.
  `ops/lib/bicam_lane.sh` prefers the password from the running container over
  `.env`.
- **Two shell runners live in `scripts/`** (`run_bicam_amendment_lane.sh`,
  `run_html_text_batches.sh`) rather than `ops/`, because live units point at
  them. Move them with the next unit update.
- **The status doc links into `../reports/`.** Those links only resolve on
  the production host.
- **Lane-status drop-in** in `deploy/systemd/bicam-.service.d/` is written
  but not installed; follow `deploy/systemd/README.md` when ready.
