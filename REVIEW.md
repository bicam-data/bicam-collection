# Code Review

This review reflects the current state of `bicam-collection`, not earlier intermediate states.

## Current Truth

- The crawler engine is `SpecCrawler`, not `BillCrawler`.
- `PostgresStore` now uses `psycopg_pool.ConnectionPool`.
- `.env` parsing is shared through `config.py`.
- URL normalization and Congress API canonicalization are split.
- The CLI discovers categories from `specs/*.yaml`; current categories are `bills` and `members`.
- `refresh_descendants` is implemented through a spec-defined refresh root node type.
- dbt is initialized for sources, reusable macros, and staging models only.

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

1. The crawler is synchronous.
   This is fine for the pilot but the original architecture called for asyncio, and scale will eventually require concurrency.

2. Crawler structure should continue to be simplified as modes and categories accumulate.

3. Members currently seed from `/member`, which proves the generic engine but is not yet a full historical coverage strategy for that category.

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

1. Async concurrency model.
2. More formal mode semantics and scheduling guidance.
3. GovInfo integration.

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
