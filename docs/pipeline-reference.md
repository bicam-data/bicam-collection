# Pipeline reference

Detailed notes on each subsystem, moved here from the README on 2026-10-12. Some
passages describe how the pipeline evolved rather than only its current state;
the [README](../README.md) and [HANDOFF](../HANDOFF.md) are the entry points.

## Current Truth

- The crawler is generic over YAML specs in `specs/`; current implemented categories are `bills`, `members`, `congresses`, `committees`, `amendments`, `committee-reports`, `committee-prints`, `hearings`, `committee-meetings`, `nominations`, and `treaties`.
- The 11 in-scope entity contracts cover those main categories: identity components, canonical formats, authoritative source, and ingestion/reconciliation quality rules are validated together by `scripts/validate_contracts.py`.
- The public-product query and loading guide is maintained at [`docs/bicam-researcher-guide.md`](bicam-researcher-guide.md); it documents the plural `bicam` relations, canonical source unions, bill-version supplements, HTML text fields, reciprocity interpretation, and source-gap semantics.
- The crawler now has GovInfo collection categories, including `govinfo-committee-reports`, which traverses collection updates -> package summary -> granules -> granule summary for `CRPT`.
- Supplemental GovInfo specs now cover `govinfo-hearings` (`CHRG`) and `govinfo-committee-prints` (`CPRT`) through package and granule traversal. They are optional evidence-ingestion paths; Congress.gov remains authoritative for the corresponding identity and coverage contract.
- Raw crawl storage is centered on `raw_responses`, `crawl_runs`, and `crawl_run_fetches`.
- Crawl runs also retain `crawl_run_expected_fetches`, so coverage can report expected child URLs separately from successful, skipped, failed, and unresolved fetches.
- dbt includes source staging, reconciliation intermediates, and bill/committee-report/hearing/committee-print marts.
- `int_committee_report_bill_links` unions bill-side committee-report references with committee-report-side `associatedBill` references and retains both evidence directions and raw provenance.
- `bridge_members__legislation` unions member-side sponsored/cosponsored legislation with bill/amendment-side sponsor and cosponsor records, so the same relationship is available from either endpoint category with direction and provenance flags.
- `bridge_committees__bills` unions committee-side bill listings with bill-side committee referrals; one-sided edges are retained and classified rather than silently duplicated.
- Congress.gov is authoritative for amendments, nominations, committees, and committee meetings; those categories are cleaned and ingested without cross-source identity matching.
- GovInfo bulk collections can bootstrap selected bill and report evidence, but they are not a complete replacement for Congress.gov API collection.
- Member sponsored and cosponsored legislation can include amendments; dbt now exposes separate sponsored/cosponsored amendment staging views and derives amendment `type` from the amendment URL when the source `type` is null.
- Committee staging currently includes committee detail, committee history, committee bills, committee reports, and committee nominations. Communications are intentionally excluded for now.
- Amendment staging currently includes amendment detail, sponsors, committee sponsors, on-behalf-of sponsors, cosponsors, actions, amendments-to-amendment, texts, and three detail-derived amended-* mapping tables.
- Committee-report staging currently includes committee report detail, detail-derived committees, detail-derived bills, detail-derived treaties, and the text child endpoint.
- GovInfo committee-report reconciliation retains supported `CRPT` and
  historical `SERIALSET` package/part evidence. `SERIALSET` identities use
  `documentNumber` (not the catalog `serialSetNumber`); one representative is
  matched per canonical report while duplicate packages remain available for
  the future public union. GPO/other route leakage remains raw/staging
  evidence.
- Committee-print staging currently includes committee print detail, detail-derived committees, detail-derived bills, and the text child endpoint.
- Committee-print reconciliation now materializes `fct_committee_print_matches`, an all-package `bridge_govinfo__committee_print_packages`, and the informational `fct_committee_print_reconciliation_metrics`. CPRT collection-code filtering excludes GPO/GOVPUB leakage; legacy volume/part suffixes are normalized while every package remains in the bridge.
- Hearing staging currently includes hearing detail, detail-derived committees, detail-derived dates, and detail-derived formats, with `associatedMeeting` flattened into `event_id` and `event_id_url`.
- Committee-meeting staging currently includes detail, committees, continuations, location, videos, witnesses, witness documents, meeting documents, related bills, related treaties, and related nominations, with `hearingTranscript` flattened into hearing jacket and URL columns.
- Nomination staging currently includes detail, actions, committees, hearings, detail-derived nominees, and nominee child details, with `nominationType` flattened into `is_civilian` and `is_military`.
- Treaty staging currently includes detail, actions, committees, detail-derived countries/parties, index terms, related docs, and titles, with only `parts_count` retained from the `parts` field. Treaty committee crawling accepts the embedded URL and falls back to the documented `/treaty/{congress}/{treatyNumber}/committees` child path when older detail payloads omit that link.
- Congressional Directory ISBN observations retain package-level duplicates so the public child grain matches the published archive; the same ISBN may occur in multiple GovInfo packages.

## Docker Stack

The repo now includes a production-oriented local stack in [docker-compose.yaml](../docker-compose.yaml):

- `postgres`: persistent Postgres 16 with safer defaults, `pg_stat_statements`, and healthchecks
- `postgres-backup`: optional logical backup sidecar; each custom-format dump is
  written to a temporary file, archive-validated with `pg_restore --list`, and
  atomically renamed only after validation succeeds
- `crawler`: app image for long-running crawl commands
- `dbt`: app image for dbt runs and tests

First-time setup:

```bash
cp .env.example .env
```

Start the database:

```bash
make up
make migrate
```

Start the database plus the backup sidecar:

```bash
make up-ops
```

The sidecar is intentionally optional because a full logical dump can create
substantial database I/O. A restore smoke test should use a disposable Postgres
instance; production restores must recreate deployment-specific roles before
restoring ACLs.

Run the crawler inside the stack:

```bash
make crawl-bounded
```

Run dbt inside the stack:

```bash
make dbt-run
make dbt-test
```

The app containers build against the local [`rotisserie`](../../rotisserie) checkout, so the Docker image uses the same dependency source as local `uv`.

`make crawl-bounded` runs the declared Congress working scope with a three-page
ceiling, page size 250, one category at a time, and three in-flight requests per
category. Expand a partition or concurrency only as an explicit reviewed
operation. GovInfo evidence is collected separately by the bounded reconciliation
targets below.

Schedulers should run the lock-protected `make schedule` target. The schedule
file declares the categories, page and concurrency ceilings, dbt selector, and
quality thresholds in one reviewable document. The quality gate exits non-zero
for stale running jobs, failed reconciliation metrics, open reconciliation
exceptions, or unsatisfied latest-run fetches. A Postgres advisory lock prevents
two scheduled batches from overlapping.

## Ongoing updates

The production deployment uses polling rather than assuming a source webhook.
The GovInfo window service runs hourly (with a five-minute source-lag buffer),
advancing a shared opaque frontier in six-hour date windows. Each invocation can
complete up to four windows and uses three in-flight requests, so a short outage
is caught up in a larger bounded batch without an unbounded crawl. The Congress
incremental service runs every six hours with a 24-hour overlap and refreshes all
date-filterable main-11 lists. It refreshes the unpartitioned `/congress` index
first, resolves the highest observed Congress, and then refreshes that
Congress's hearing partition; this follows a newly opened Congress without a
code change. Its supplemental lane is also six-hourly and only traverses
descendants when the coverage ledger finds an unexplained gap. Each completed
GovInfo window also probes the bounded modern GovInfo-only hearing set through
the same resolved Congress, so newly published CHRG/JHRG packages do not wait
for the next supplemental cycle to become classified.
For a controlled recovery only, `BICAM_CURRENT_CONGRESS` can override the
resolved value for one service invocation; normal operation leaves it unset.

Every lane is restartable: date frontiers, crawl cursors, expected child edges,
and raw response versions are durable. A shared process lock serializes crawls
and reconciliation builds, while lock-busy checkpoints let the next timer retry
without spending API quota. The source-lane checkpoint `*-latest.md` pointers
identify the last completed successful run; the corresponding
`*-attempt-latest.md` pointers separately record the most recent attempt,
including a lock-busy or failed attempt, so a deferred run never hides the last
authoritative frontier. After new rows arrive,
the bounded reconciliation
models, quality gate, status report, and release manifest are refreshed; the
daily snapshot timer produces the portable research release. To deliberately
catch up a longer outage, increase the service's `BICAM_GOVINFO_WINDOW_MAX_WINDOWS`
or run a bounded historical job explicitly rather than changing the steady-state
polling cadence.

The one-time historical backfill lanes (partition queue, per-key ramp, supervisor
and related services) were removed after the backfill finished; they remain in git
history.

Common shortcuts are defined in [Makefile](../Makefile). Run `make help` to see the available targets.

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
uv run python scripts/run_crawl.py --category govinfo-hearings --mode full --max-pages 1
uv run python scripts/run_crawl.py --category govinfo-committee-prints --mode full --max-pages 1
```


When reconciliation identifies a GovInfo bill whose Congress.gov detail is
missing, backfill those canonical identities in bounded batches:

```bash
uv run python scripts/backfill_congress_bill_details.py \
  --limit 100 --concurrency 2 --request-interval 2
```

The detail backfill preserves the GovInfo response as the lineage parent,
skips stored responses and known 404s, and can be rerun safely after each
Congress.gov historical pass.

The scheduled GovInfo window service runs this counterpart probe automatically
after each successful bounded window, then refreshes the bounded bill,
committee-report, and hearing reconciliation paths (including the parents of
`fct_committee_report_matches`, `fct_hearing_matches`, and the
`bridge_hearings__committee_meetings` many-to-many bridge). It explicitly loads
the project dotenv and `dbt_project/profiles.yml`, so newly published GovInfo
records are materialized immediately or retained as auditable, expiring
Congress.gov 404/500 source gaps instead of leaving the release gate open.

The GovInfo window wrapper and the Congress refresh wrappers now share the
`/run/user/<uid>/bicam-ingestion.lock` process lock. If a GovInfo window starts
while a Congress reconciliation build owns the lock, it records a lock-busy
attempt checkpoint and exits successfully for the next persistent timer to
retry, while preserving the last successful checkpoint pointer. This keeps
newly inserted raw versions from racing a cross-source materialization.

GovInfo member references are normalized recursively across harvested bill,
committee-report, and hearing package/granule payloads. The source-neutral
`bridge_govinfo__member_references` retains the raw member object and separates
the source Bioguide from an evidence-backed canonical ID inferred from
Congress.gov terms. For consumers that need one row per person,
`stg_govinfo__member_references` exposes `bioguide_id`; composite source
objects are expanded through `bridge_govinfo__member_reference_components`, so
each named legislator receives its own linkable ID without overwriting the
immutable source JSON. Reviewed non-federal participants remain explicit
null-ID rows. The recursive `int_govinfo_identity_fields` audit also checks
Bioguide, Thomas, and authority identifiers in nested package and granule
subfields. Each hourly GovInfo window refreshes that recursive audit and runs
`govinfo_nested_identity_audit_covers_payloads`; the heavier all-category
member-reference enrichment remains in the six-hour Congress reconciliation
lane so the hourly lock stays bounded.

Modes:

- `full`: full seed traversal without incremental filtering
- `incremental`: uses the stored high-water mark when `--from-datetime` is omitted and refetches returned subtrees
- `refresh`: explicit refetch mode without automatic high-water-mark lookup
- `resume`: uses the stored high-water mark when `--from-datetime` is omitted and skips URLs already present in `raw_responses`
- `refresh_descendants`: uses a category's configured refresh root node and refetches its stored child collection URLs without revisiting the seed
- `backfill_descendants`: follows stored parent payloads and fetches only missing or failed descendants; it does not refetch an existing payload unless explicitly selected by the crawl spec.

Backfill traversal checks already-stored descendants in bounded database batches;
the `--concurrency` setting limits network work and should be raised only with a
measured API canary. This keeps large historical passes from turning stored-graph
bookkeeping into unnecessary per-URL database latency.

The crawler reads source-specific API keys plus `DATABASE_URL` from the environment. If they are not set, it will also read them from a local `.env` file.

- Congress.gov: `CONGRESS_API_KEYS`, `CONGRESSIONAL_API_KEY`, or `CONGRESS_API_KEY_*`
- GovInfo: `GOVINFO_API_KEYS`, `GOVINFO_API_KEY`, or `GOVINFO_API_KEY_*`

API key rotation is handled by the local [`rotisserie`](../../rotisserie) checkout through `uv`.

To measure the async crawler's child-resource throughput without using API quota,
run `make benchmark-crawl`. It uses the real queue, URL canonicalization, and
concurrency limiter with the real Rotisserie-backed Congress client pointed at
a deterministic in-memory transport. It reports the Rotisserie package version,
source commit, and whether that checkout is dirty:

```bash
make benchmark-crawl ARGS="--children 32 --concurrency 4 --delay-ms 20"
```

The benchmark is a comparative signal, not a substitute for a bounded live
crawl. Keep the Rotisserie checkout clean and pin its published commit in CI
when the current local revision is ready to publish; a dirty checkout is
reported explicitly so a performance result cannot be mistaken for a
reproducible dependency version. CI currently pins the last published
Rotisserie commit; update that SHA after the sibling checkout's pending changes
are committed and reviewed.

## Run the tests

The unit suite runs with no database:

```bash
uv run pytest -q
```

The storage integration tests in `tests/test_storage.py` need a real Postgres
and are skipped unless `BICAM_TEST_DATABASE_URL` is set. They deliberately
never fall back to `.env` or `DATABASE_URL`, so operational credentials cannot
be used by accident. The compose file provides a disposable `postgres-test`
service under the `test` profile: same image and init SQL as production,
tmpfs-backed, on host port 5434. `make test-db` starts it, applies the
migrations to it, and runs the full suite against it:

```bash
make test-db
make test-db-down
```

`make test-db-up` starts the test database on its own; `make test-db-down`
stops and removes only that container, so the production `postgres` service
is untouched. The default `docker compose up -d` does not start the test
service. To run pytest by hand against it, export the DSN from `.env.example`:

```bash
export BICAM_TEST_DATABASE_URL=postgresql://admin:test-password@localhost:5434/bicam_collection_test
uv run pytest -q
```

## Run dbt

```bash
export DBT_PROFILES_DIR="$PWD/dbt_project"
export DB_PASSWORD='<your-postgres-password>'
uv run dbt run --project-dir dbt_project
uv run dbt test --project-dir dbt_project
```

The bundled `profiles.yml` expects Postgres connection settings via `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `DB_NAME`, and `DB_SCHEMA`.

## Release evidence

Write a read-only, timestamped machine-readable snapshot of the main-11
coverage, child pagination, reconciliation, internal reciprocity, GovInfo
identity, and known source-error state with:

```bash
make release-manifest
```

The latest artifact is written to `../reports/main-11-release-manifest-latest.json`
and an immutable copy is kept under `../reports/release-manifests/`.  The
scheduled six-hour report can use this artifact to verify a release without
repeating the full raw-archive queries.
The `govinfo.window_coverage` section also records the completed/failed window
counts and detects gaps or overlaps from the 1789 bootstrap through the current
frontier.
The `govinfo.catalog_coverage` section records package- and granule-list to
detail parity for the four harvested GovInfo collections. Its
`package_collection_breakdown` subsection also reports the collection code and
observed date/Congress span for each package-detail population; this keeps
`CRPT`, historical `SERIALSET`, and incidental `GPO` route responses distinct.

To create a portable point-in-time data snapshot of the contract-defined
lowercase `bicam` schema, run:

```bash
make export-snapshot
```

This exports every `core-now` relation declared in
`contracts/bicam_public_schema.yaml` (currently 89 core relations; the
supplemental release adds `bills_versions` and `bills_version_links`) as
gzip-compressed CSV under `../reports/exports/<UTC timestamp>/`. Export writes
an immutable candidate and does not update the `latest` symlink until the
candidate is validated and explicitly promoted. The
accompanying `manifest.json` records the contract/schema versions,
repeatable-read database timestamp, exact row counts, contract column order,
primary keys, SHA-256 hashes, source frontiers, and candidate-bound build and
row-quality evidence. A bounded subset can be selected with, for example,
`make export-snapshot ARGS="--tables bills committee_reports"`; partial exports
are diagnostic artifacts and cannot replace the complete `latest` release.

The scheduled release wrapper performs the candidate gate and promotion. To
validate or promote a candidate manually, point the promotion command at the
exact manifest printed by the exporter; it refuses partial inventories,
header/count/hash drift, symlinked candidates, and contract-scope mismatches:

```bash
uv run python scripts/promote_research_snapshot.py \
  --candidate ../reports/exports/<UTC timestamp>/manifest.json \
  --output-dir ../reports/exports --include-supplemental --dry-run
uv run python scripts/promote_research_snapshot.py \
  --candidate ../reports/exports/<UTC timestamp>/manifest.json \
  --output-dir ../reports/exports --include-supplemental
```

Validate a built product before exporting with:

```bash
docker compose --profile tools run --rm crawler \
uv run python scripts/validate_bicam_product.py
```

Add `--include-supplemental` when the release also contains
`bills_versions` and `bills_version_links` and those relations should be
required by the check.

For a candidate-release gate, add `--check-data` (and optionally
`--max-examples N`) to validate declared unique keys, required values, and
foreign keys against the actual rows. This mode is read-only and should be
run against the exact candidate database snapshot; the default schema-only
mode is intended for inexpensive compatibility checks.

The validator reports missing or extra relations, column order and type
mismatches, and any explicit contract nullability mismatches. The database and
immutable raw-response versions remain authoritative; the export is a
portable research-facing release artifact.

Verify the latest snapshot and materialized quality state with:

```bash
make release-check
```

The check is read-only and validates manifest age, every export hash and
CSV-aware row count, zero actionable root/detail or child gaps, reconciliation
quality, and stale crawler runs.  Use `--no-file-verification` when only the
database checks are needed during an operational health probe.

The host also runs `bicam-research-snapshot.timer` once daily.  It invokes
`ops/run_research_snapshot.sh`, which takes a
non-overlapping lock, applies bounded timeouts, writes a timestamped
`reports/research-snapshot-checkpoint-*.md`, and records the freshness result
for later audit or continuation after an agent-usage reset.

The four high-volume GovInfo collections continue in gap-free hourly windows.
The smaller supplemental sources use a separate daily lane:
`bicam-govinfo-supplemental-refresh.timer` refreshes a bounded overlapping CDIR
date window and only deterministic current-Congress TDOC package IDs. It never
crawls the broad CDOC collection. The latest durable artifact is
`../reports/govinfo-supplemental-checkpoint-latest.md`.

Historical Congressional Directory descendants use a separate bounded cursor
lane: `bicam-govinfo-cdir-descendant-refresh.timer` walks stored CDIR package
details through every paginated granule list and granule detail. It processes a
small number of package roots per cycle with `backfill_refresh_descendants`, so
existing payloads are reused while missing member granules are filled in. Its
cursor and coverage report are retained at
`../reports/govinfo-cdir-descendant-checkpoint.json` and
`../reports/govinfo-cdir-descendant-refresh-latest.md`; the daily date-window
lane remains responsible for newly revised packages.

Source-facing staging models remain lightweight `view`s, while the expensive
raw JSON work is done once in materialized intermediate relations. The
`int_raw_responses__current` and `int_raw_response_versions` models provide
indexed current/versioned raw inputs. Stable ordinal keys are used for repeated
arrays such as bill text versions, bill formats, committee-report records, and
committee-report text formats. This avoids global `row_number()` identities and
lets downstream staging models reuse parsed payloads instead of reparsing every
raw response on every model.

The bounded scheduler can be previewed without crawling or taking the database
lock:

```bash
make schedule ARGS="--job bounded_congress --dry-run --verbose"
```

Run it for real only from the intended scheduler or operator session:

```bash
make schedule ARGS="--job bounded_congress"
```

The job runs the declared resume crawl, rebuilds coverage, and enforces the
configured quality gate. Edit `contracts/schedules.yaml` and rerun
`scripts/validate_contracts.py` when adding another bounded job.

## Schema migrations

SQL migrations in `migrations/` are applied once and tracked in
`schema_migrations`. Apply them before a crawl or dbt run that depends on a new
schema feature:

```bash
make migrate
```

`000_initial_schema.sql` owns the baseline tables, `001_raw_response_versions.sql`
creates an append-only record of each distinct payload observed at an endpoint,
`002_raw_response_current.sql` exposes a compatibility view for dbt, and
`003_backfill_fetch_version_links.sql` repairs historical fetch lineage where an
endpoint has only one retained version. For the large legacy backfill ledger,
`scripts/backfill_fetch_version_links.py` performs the same repair in resumable
batches. Its optional `--resolve-temporal` pass handles multi-version endpoints
only when one retained payload has a unique latest `first_seen_at` at or before
the legacy fetch event; timestamp ties remain unresolved. The optional
`--resolve-endpoint` pass also repairs early `skipped_existing` events that lack
the legacy raw-response ID by matching their endpoint and event time to an
immutable payload, again rejecting timestamp ties. Failed requests and
same-run duplicate discoveries remain intentionally unlinked because they have
no payload to reference. `007_fetch_lineage_lookup_index.sql` provides
the partial lookup index used by that repair. The
`raw_responses` interface remains current-state, while
`raw_response_versions` preserves source evidence for repeatable transformations
and reconciliation. `004_expected_fetches.sql` records each endpoint discovered
and queued during a run, including its parent endpoint and node type. The crawler
image includes the migration directory, but the crawler does not run DDL on
startup; apply migrations explicitly before first use. Migration
`015_committee_report_text_lineage.sql` adds the detail-payload version dependency
used by incremental committee-report text expansion.

## Committee-report reconciliation

The formal source-boundary rules for BILLS, CPRT, and CHRG/JHRG are documented
in [the main-11 source-boundary policy](main-11-source-boundary-policy.md).

The initial reconciliation slice compares Congress.gov and GovInfo committee
reports by a shared canonical identity: report type, number, Congress, part,
and errata status. It produces deterministic candidates in
`int_committee_report_candidates` and reviewed outcomes in
`fct_committee_report_matches`.

Add an explicit human decision to
`dbt_project/seeds/reconciliation/committee_report_match_overrides.csv` when a
candidate must be accepted or rejected outside the deterministic rule. The dbt
tests enforce unique accepted pairings and non-null canonical identities.

Source gaps are tracked separately in
`dbt_project/seeds/reconciliation/reconciliation_exception_reviews.csv`. Each
review has a disposition, reason, reviewer, timestamp, and optional expiry; the
quality gate fails when an unmatched record is not covered by a current review.

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

## Committee-print reconciliation

Congress.gov committee prints use an identity composed of chamber print type,
jacket number, and Congress (for example `hprt21612-104`). GovInfo CPRT
package IDs use the inverse order and may append volume/part suffixes. The
intermediate reconciliation models normalize those forms and accept only exact
canonical identities. `bridge_govinfo__committee_print_packages` retains every
CPRT package, including historical publications without a Congress.gov record,
unsupported `WPRT` identifiers, and malformed IDs. Package details whose
`collectionCode` is not `CPRT` are retained in raw/staging evidence but are
excluded from the print relation because the CPRT endpoint can return occasional
GPO/GOVPUB leakage.

`fct_committee_print_reconciliation_metrics` is an informational census rather
than a strict quality gate until the historical coverage boundary is reviewed;
it reports exact overlap, source-only records, legacy variants, malformed IDs,
and excluded collection leakage. The six-hour ingestion report includes this
census automatically.

## Bill reconciliation

Bill reconciliation applies the same reviewed, deterministic pattern at bill
level. Congress.gov text-version URLs provide the exact GovInfo `BILLS` package
identifiers, and the latest available GovInfo package version is matched by bill
type, number, and Congress. A bill without a collected text version remains
unmatched rather than being inferred from a title.

After collecting bills, run:

```bash
make reconcile-bills
```

The workflow first retrieves missing Congress.gov bill-text child endpoints,
then stages those text versions and retrieves only the GovInfo packages they
identify. It rebuilds `fct_bill_matches` and refreshes coverage while keeping a
default ceiling of three concurrent requests for each source. Set
`BILL_TEXT_BACKFILL_LIMIT` or `BILL_BACKFILL_LIMIT` (each default `5000`) only
when a different bounded batch is intended.

## Historical partitions and hearing relationships

Historical Congress.gov collection is partitioned by Congress number so an
oldest-first run does not depend on a global list endpoint. Each partition
follows its declared child resources and is URL-idempotent; use a small page
bound and resume descendant traversal after an interrupted batch:

```bash
uv run python scripts/run_congress_history.py \
  --congress-start 1 --congress-end 10 \
  --mode full --max-pages 3 --concurrency 1
uv run python scripts/run_congress_history.py \
  --categories bills --congress-start 25 --congress-end 25 \
  --mode backfill_descendants --max-pages 3 --concurrency 1
```

The `members` and `congresses` categories are global (not Congress-partitioned)
and can be included explicitly; the historical runner schedules each once.

The checked-in collection scope declares every Congress from 1 (1789) through
119 as required. The default three-page ceiling is still useful for a bounded
sample or a recoverable batch; complete historical runs should use the
partitioned runner with a sufficiently large `--max-pages` (or sweep the seed
list in non-overlapping batches by advancing `--seed-offset` by
`page-size * max-pages`). All fetched URLs remain idempotent, so a retry can
safely repeat a batch.

The historical runner fails a partition after `--key-wait-timeout` seconds
(120 by default) if all API keys are cooling, leaving successful raw versions
available for a later retry. The `int_hearing_committee_meeting_links` model
records both Congress.gov paths (a hearing's `associatedMeeting` and a
committee meeting's `hearingTranscript`) plus GovInfo CHRG package matches.
GovInfo hearings can therefore connect to a committee meeting transitively;
the model preserves the path used for each edge rather than assuming every
hearing is a committee meeting.
Historical runs pace requests by 250ms by default (`--request-interval`) for
small isolated batches. In the current deployment, direct sequential probes
returned `X-RateLimit-Limit: 20000` for each of 17 distinct credentials, and
requests charged one credential without decrementing a second credential's
counter. This establishes token-scoped counters for the probe, but does not
establish that the credentials have independent account-level quotas. The
client therefore treats `BICAM_CONGRESS_PER_KEY_RATE_LIMIT` as a per-token
window (shared across virtual concurrency slots), while the supervisor's
`BICAM_CONGRESS_GLOBAL_RATE_LIMIT`/pacing and coordinated-429 circuit breaker
remain a separate, reversible deployment-wide safety guard. A coordinated
429/`Retry-After` response is evidence of a broader source, gateway, network,
project, or account limiter and pauses/retries durably.
    The public API repository still
documents 5,000 requests/hour, so the runtime setting is an observed
deployment behavior—not a general Congress.gov guarantee or an assertion that
17 credentials provide 17 independent account quotas.
After a later coordinated window returned HTTP 429 with zero remaining on all
17 credentials, the autonomous supervisor temporarily retained a conservative
20,000-attempt deployment guard. All configured credentials are still used for
fair rotation and failover; the 340,000/hour theoretical token sum is not
treated as safe without a separate egress/project boundary. This guard is not
an assertion that the credentials belong to one account; the gateway or
network may apply a broader quota than the token counter exposed in headers.
Override `BICAM_CONGRESS_PER_KEY_RATE_LIMIT`,
`BICAM_CONGRESS_GLOBAL_RATE_LIMIT`,
`BICAM_CONGRESS_RATE_WINDOW_SECONDS`, or `BICAM_CONGRESS_RATE_SAFETY` only
after confirming the live response headers and terms for the credentials in
use.

The six-hour supplemental service exposes its own bounded throughput knobs:
`BICAM_SUPPLEMENTAL_CONCURRENCY`, `BICAM_SUPPLEMENTAL_CATEGORY_WORKERS`,
`BICAM_SUPPLEMENTAL_REQUEST_INTERVAL`, `BICAM_SUPPLEMENTAL_DESCENDANT_CONCURRENCY`,
and `BICAM_SUPPLEMENTAL_DESCENDANT_INTERVAL`. They default to the observed
safe 8-worker/0.25-second incremental profile and a 4-worker/0.5-second
descendant profile; changing them affects only that service, not other crawler
lanes.

Before any supplemental descendant traversal, the service runs the read-only
`check_child_coverage.py` ledger audit. When every declared child edge is
already fetched or source-classified, the descendant lane is skipped and no
API quota is spent on a redundant root walk. A nonzero missing count enables a
bounded traversal (100 batches by default). The same service performs bounded
counterpart probes for modern GovInfo-only bills, committee reports, and
hearings; recent 404s are suppressed for a configurable seven-day retry window
and remain expiring evidence in reconciliation.

For an isolated retry of a transient list/detail URL, use
`scripts/retry_failed_fetches.py --no-descendants`. This records the retry in a
separate run mode, fetches only the seed URL, and prevents a failed list page
from expanding into an unbounded child crawl; retry runs are intentionally
excluded from authoritative coverage selection. With no `--categories`
argument, the helper is restricted to the 11-category Congress.gov production
scope; paused or experimental specs require an explicit category list.

The scheduled Congress incremental service also performs a bounded source-error
re-probe after its relationship pass. It filters to the known main-11
`amendment_detail` and `bill_detail` node types, uses `--no-descendants`, and
records every attempt in the failure ledger. Configure its batch with
`BICAM_SOURCE_ERROR_RETRY_LIMIT`, `BICAM_SOURCE_ERROR_RETRY_MIN_AGE_MINUTES`
(default 120 minutes), and `BICAM_SOURCE_ERROR_RETRY_REQUEST_INTERVAL`;
persistent upstream 500s stay
classified rather than being turned into synthetic records.

## Version-level bill records

`bicam.bills` is intentionally one row per canonical bill and exposes one
representative GovInfo `version_code`.  The supplemental `bicam.bills_versions`
table retains every GovInfo BILLS package and version, keyed by
`govinfo_package_id`, with `bill_id`, `version_code`, an optional numeric
`version_variant`, the authenticated GovInfo `/htm` URL in `formatted_text`,
extracted visible text in `raw_text` when available, title fields, and timestamps.  The supplemental
`bicam.bills_version_links` table provides deterministic child-row links for
Congress.gov titles, text renditions, and actions.  Text rendition URLs use
their exact BILLS version suffix when present; display-name matching is retained
as a historical fallback.  Its `source_table` and `source_row_key` identify
the child row, while `govinfo_package_id` and `govinfo_version_code` identify
the selected GovInfo version.  Congress summary `version_code` values are CRS
summary identifiers (not BILLS package codes), so summaries remain linked by
`bill_id` and are deliberately not force-matched.
Legacy package IDs such as `BILLS-108hr10v1rh` are normalized to semantic code
`RH` with `version_variant = 1`, preserving both the version family and the
numeric revision.

The core 89-table snapshot remains unchanged. Include both supplemental bill
version relations explicitly when exporting a release:

```bash
uv run python scripts/export_research_snapshot.py --include-supplemental
```

## Source-gap operations

`public.fct_source_gap_queue` is the managed queue derived from reconciliation
exceptions.  It provides `queue_status`, `priority`, `next_action`, an expiry
or reprobe time, and the endpoint to fetch.  Generate a durable JSON/Markdown
checkpoint with:

```bash
uv run python scripts/report_source_gap_queue.py
```

Pushover support is present but disabled by default.  It only sends when the
operator explicitly supplies `--notify-pushover` and both
`PUSHOVER_TOKEN`/`PUSHOVER_USER`; no credentials are configured in this
repository. When enabled, it alerts only for `open` or `due_for_reprobe`
rows; deferred source-boundary rows remain in the durable report without
creating notification noise.

## HTML text extraction

The resumable `scripts/fetch_html_text.py` lane fetches only HTTPS HTML links
from bills, amendments, committee prints, committee reports, hearings, and
GovInfo bill-version packages.
It accepts public Congress.gov renditions and authenticated GovInfo `/htm`
renditions; GovInfo keys are selected deterministically from the configured
pool for each request and are never written into the stored URL.
It stores append-only response/extraction versions in
`public.html_text_versions`; `public.html_text_current` records the latest
attempt, while `public.html_text_successful_current` preserves the latest
successful content for public marts.  The public `*_texts.raw_text` columns
join to that stable successful view.  PDF and
XML links are preserved but never fetched by this lane.  The checkpointed
`make html-text` wrapper rotates up to three bounded batches round-robin across
the six public text tables (including GovInfo bill-version HTML), so the large bill/hearing queues cannot starve
amendments or committee documents.  A failed dbt propagation is retained as a
pending checkpoint and retried by the next timer.  Start a direct bounded batch with
the optional table selector:

```bash
uv run python scripts/fetch_html_text.py --limit 100 --concurrency 8
```

For example, `--source-table bills_texts` restricts a direct batch to bill
renditions.  The wrapper manages one lexical cursor per table; successful
rows are still skipped until their refresh age expires; non-successful
attempts use a separate one-day retry backoff by default (`--retry-failed-days`),
so a transient 5xx/timeout is not suppressed for the full 30-day success
refresh interval. Mixed Congress.gov/GovInfo hearing queues also retain
pending GovInfo URLs when the cursor is already in the Congress.gov range, so
the lower-sorting host cannot be starved until a full lexical wrap.

For an operational checkpointed run (including dbt propagation and the
successful-extraction test), use `make html-text`; tune the batch with
`BICAM_HTML_BATCH_LIMIT`, `BICAM_HTML_MAX_BATCHES`, and the pacing variables
`BICAM_HTML_CONGRESS_REQUEST_INTERVAL` and
`BICAM_HTML_GOVINFO_REQUEST_INTERVAL` (with
`BICAM_HTML_REQUEST_INTERVAL` as the fallback). Each run also writes
`reports/html-text/progress.md` and `progress.json`, with target, success,
non-success, pending counts, rolling six-hour attempt/success rates, and
separate sweep and successful-text ETAs for every public text table. Exhausted
tables are skipped within the same bounded invocation so they do not delay the
next table's batch. The report also counts distinct non-HTML rendition links
excluded from the lane (for example, a GovInfo `/txt` link), so those are not
mistaken for pending HTML work.
The installed timer uses a one-second shared pace for both Congress.gov
hostnames after observed 429 responses, and a separate 0.25-second pace for
authenticated GovInfo HTML requests. This prevents one source's limiter from
slowing the other and keeps the GovInfo key pool useful; change either value
only after a measured source-specific rate check.
Scheduled runs use two 250-row rounds so the shared ingestion lock is held for
bounded intervals even when Congress.gov responses are slow.

Bill version history is available in `bicam.bills_versions`. The supplemental
`bicam.bills_version_links` relation connects version-labelled
`bills_titles`, `bills_texts`, and `bills_actions` rows to GovInfo BILLS
packages when the Congress.gov code or display name is an exact match. For
bill-text renditions, the machine-readable suffix in the Congress.gov URL is
preferred, which covers modern rows even when the `/titles` child is sparse.
Rows
with a known Congress version label but no GovInfo counterpart remain with
`link_status = 'no_matching_govinfo_version'`. `bills_summaries.version_code`
is a CRS-summary revision identifier, not a BILLS package code, so summaries
are intentionally not presented as GovInfo version links.

The source endpoint fields are intentionally handled differently: `/titles`
provides the authoritative `billTextVersionCode` and `billTextVersionName`;
`/text` provides a version display `type` plus rendition URLs whose
`BILLS-<congress><type><number><version>` suffix is parsed when present, with an
unambiguous title-name-to-code mapping as the historical fallback; `/actions`
has no dedicated version field and is linked only when its action text exactly
equals an observed title version name. `/summaries` also exposes a
`versionCode`, but it is a CRS summary revision and is excluded from the
bill-version bridge rather than being force-matched.

For example, retrieve a bill's versions and the Congress.gov child rows that
refer to each one with:

```sql
select v.bill_id, v.version_code, v.govinfo_package_id,
       l.source_table, l.source_row_key, l.link_rule
from bicam.bills_versions as v
left join bicam.bills_version_links as l
  on l.govinfo_package_id = v.govinfo_package_id
where v.bill_id = 'hr1-119'
order by v.version_code, l.source_table, l.source_row_key;
```
