# Code Review

This review reflects the current state of `bicam-collection`, not earlier intermediate states.

## Current Truth

- The crawler engine is `SpecCrawler`, not `BillCrawler`.
- `PostgresStore` now uses `psycopg_pool.ConnectionPool`.
- `.env` parsing is shared through `config.py`.
- URL normalization and Congress API canonicalization are split.
- The CLI discovers all 13 categories from `specs/*.yaml`, including Congress.gov and GovInfo evidence categories.
- `refresh_descendants` and `backfill_descendants` are implemented through spec-defined roots and stored payloads.
- dbt now includes source staging, reconciliation intermediates/marts, and an operations coverage ledger.
- Ordered migrations own the schema; `raw_response_current` preserves the current-state interface while immutable payload versions retain evidence.
- Congress.gov is authoritative for amendments, nominations, committees, and committee meetings. GovInfo is supplemental for selected bill/report evidence, not a complete replacement.

## Validated Strengths

- The core architecture is sound: endpoint spec, crawler engine, and storage are separated cleanly.
- The spec-driven approach is real. Bills and members behavior is expressed in YAML rather than category-specific crawl code.
- `crawl_run_fetches` is a meaningful improvement over the original plan because it gives per-URL auditability.
- The crawler depends on a storage protocol rather than a concrete Postgres class, which keeps tests clean.
- Explicit crawl modes now exist and are operationally useful:
  - `full`
  - `incremental`
  - `refresh`
  - `resume`
  - `refresh_descendants`

## Review Findings

### Still Valid

1. Historical coverage remains an explicit, partitioned operating scope rather than a completeness claim; the scope contract and coverage mart make that visible.

2. Crawler structure should continue to be simplified as modes and categories accumulate. The async runner is bounded by category/concurrency settings and retains custom key-budget telemetry.

3. GovInfo bulk data can bootstrap selected collections but does not cover every Congress.gov category; API collection remains authoritative where declared.

### No Longer Valid

1. `refresh_descendants` being “partially implemented but broken”.
   This was true earlier, but it is now implemented in the engine, exposed in the CLI, covered by tests, and live-validated.

2. `batch_size` missing from `CrawlOptions`.
   It is now present.

3. `list_refresh_nodes` missing from the store protocol.
   It is now present.

### Fixed During Review Follow-up

1. Dead code: `_payload_roots` in `crawler.py` has been removed.

2. `DATABASE_URL` validation is now explicit at the settings layer.

3. `max_pages` now applies per pagination stream rather than globally per node type.

4. The CLI is category-generic and validates specs by file resolution.

5. The second category (`members`) is implemented without category-specific crawler code.

## Current Priority Order

### Fix Soon

1. Continue simplifying crawler structure as new modes and categories are added.
2. Decide the eventual historical-coverage strategy for `members` and similar categories that have alternative seed patterns.
3. Expand staging coverage before building marts.

### Revisit Before Expanding Far Beyond Bills

1. Materialize shared JSON expansions only after measured dbt pressure justifies the storage cost.
2. Add more entity contracts and golden fixtures after the bill/report reconciliation pilot.
3. Expand the declared Congress partitions through reviewed scope changes, not an unbounded full crawl.

## Current Mode Semantics

- `full`
  Traverse from the seed endpoint without incremental filtering.

- `incremental`
  Use the stored high-water mark when `--from-datetime` is omitted and refetch the returned bill subtrees.

- `refresh`
  Explicit refetch mode without automatic high-water-mark lookup.

- `resume`
  Use the stored high-water mark when `--from-datetime` is omitted and skip URLs already present in `raw_responses`.

- `refresh_descendants`
  Select stored refresh-root rows from Postgres in batches and refetch their child collection URLs without revisiting the seed.

- `backfill_descendants`
  Reuse stored parent payloads, traverse their configured child and pagination links, and fetch only missing descendants. Existing payloads are recorded as skipped with audit events.
