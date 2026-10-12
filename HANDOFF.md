# bicam-collection handoff

Start here. The [README](README.md) explains what the pipeline does and how to
set it up and run it; [docs/pipeline-reference.md](docs/pipeline-reference.md)
has detail on each subsystem. This page covers what a new owner needs beyond
that: production, state, open work, and known issues.

The expected checkout layout:

```text
~/bicam/
├── bicam-collection/   (this repo, branch feature/reconciliation-foundations)
├── rotisserie/         (git@github.com:ryandelano/rotisserie.git)
└── reports/            (created by the lanes; checkpoints, logs, exports)
```

Tests that need external resources are skipped unless their env var is set:

| Env var | Enables |
| --- | --- |
| `BICAM_TEST_DATABASE_URL` | Storage integration tests (`make test-db` starts a disposable Postgres) |
| `BICAM_RUN_VERSION_REPLAY=1` | Bill-version replay tests (disposable database) |
| `BICAM_REPLAY_BUILD_DIR` | Raw-replay generation verification |
| `BICAM_PUBLISHED_ARCHIVE_DIR` | Contract check against the published 2025 CSV archives |

## Production

Production runs on one host as systemd **user** units. All lanes share the
lock `/run/user/$UID/bicam-ingestion.lock`, so they never overlap and a
lock-busy run simply retries on the next timer. The lanes reach Postgres with
host `psql` using the `DB_*` settings in `.env`, so `postgresql-client-16`
must be installed on the host.

Enabled timers:

| Timer | Schedule | Purpose |
| --- | --- | --- |
| `bicam-congress-availability-guard` | every 2 min | Pauses crawling during coordinated Congress.gov outages / 429 cooldowns |
| `bicam-congress-incremental-refresh` | every 6 h | Date-filtered refresh of the main-11 Congress.gov lists, then reconciliation |
| `bicam-congress-supplemental-refresh` | every 6 h | Supplemental endpoint families; descends only where coverage finds a gap |
| `bicam-govinfo-cursor-continuation` | every 6 h | GovInfo incremental date windows (`ops/run_govinfo_incremental_windows.sh`) |
| `bicam-govinfo-cdir-descendant-refresh` | 4× daily | GovInfo Congressional Directory descendants |
| `bicam-govinfo-supplemental-refresh` | daily 03:15 UTC | GovInfo CDIR and treaty-document refresh |
| `bicam-html-text` | every 10 min | Bounded, resumable HTML text extraction (`scripts/run_html_text_lane.py`) |
| `bicam-fetch-lineage-backfill` | every 15 min | Repairs legacy fetch-lineage rows in batches |
| `bicam-ingestion-report` | every 6 h | Writes `reports/ingestion-status-latest.md` |
| `bicam-ingestion-audit` | every 6 h | Read-only ingestion/reconciliation audit |
| `bicam-research-snapshot` | daily 05:00 UTC | Exports the portable research snapshot and checks freshness |

The one-time historical backfill lanes (partition queue, per-key ramp,
supervisor, amendment lanes and related units) were removed on 2026-10-12;
they are in git history and `~/bicam-archive/retired-lanes-20261012/`.

Useful commands:

```bash
systemctl --user list-timers 'bicam-*'
journalctl --user -u bicam-congress-incremental-refresh -n 100
cat ~/bicam/reports/ingestion-status-latest.md
```

To install or update the units from this repo, see
[deploy/systemd/user/README.md](deploy/systemd/user/README.md).

**Cutover pending.** The installed units still run the pre-move copies in
`~/bicam/scripts/` and `scripts/run_html_text_batches.sh`, which reach the
database through `docker exec`. Once `psql` is installed, install the units
from this repo (that also moves html-text onto the Python lane and removes the
retired backfill units); after one clean 6-hour cycle, delete `~/bicam/scripts/`
and `scripts/run_html_text_batches.sh`.

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

The database is the other piece of state (about 240 GB; most of it is the
fetch-lineage log). In production it is the `bicam-collection-postgres`
container volume, backed up by the optional `postgres-backup` sidecar
(`make up-ops`); it can be moved to an existing server with
`pg_dump -Fc`/`pg_restore` while the timers are stopped.

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

**Reconciliation decisions.** `scripts/reconcile.py queues` currently shows
every queue empty. Earlier work recorded about 300 decisions by hand, and the
rules in `reconciliation_source_gap_rules.csv` close about 359,000 match
exceptions automatically as known source gaps. Those rules, and many recorded
decisions, **expire in August 2027**; the affected items then reopen for
re-probing and review. `reconcile.py list QUEUE --include-reviewed` shows
what was closed and why.

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
  `admin`; older notes that say `postgres/postgres` are stale. Everything
  (crawler, dbt, `ops/` lanes, `reconcile.py`) reads the `DB_*` settings in
  `.env`.
- **The status doc links into `../reports/`.** Those links only resolve on
  the production host.
- **Lane-status drop-in** in `deploy/systemd/bicam-.service.d/` is written
  but not installed; follow `deploy/systemd/README.md` when ready.
