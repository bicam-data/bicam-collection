DBT project for cleaning, standardization, and curated layers.

Overview
- Sources: Reads from normalized staging schemas created by the Python normalizer.
- Layers:
  1) stg_: light typing and standardization on top of staging tables
  2) int_: structural transforms, splits/joins, deduplication, rule-based cleaning
  3) core_: harmonized, canonical entities (merge across sources)
  4) mart_: subject-area marts for downstream consumers

Schemas
- Configure schemas via dbt vars (see dbt_project.yml):
  - Inputs (staging sources for testing): dbt_bicam_stg_congressional / dbt_bicam_stg_govinfo
  - Outputs: dbt_bicam_stg (stg), dbt_bicam_int (int), dbt_bicam_core (core), dbt_bicam_mart (mart)

Running
1) Ensure dbt and the Postgres adapter are installed (e.g., `pip install dbt-postgres`).
2) Copy `profiles.example.yml` to your local dbt profiles directory and adjust credentials:
   - macOS/Linux: `~/.dbt/profiles.yml`
   - Windows: `%USERPROFILE%\.dbt\profiles.yml`
3) From repository root: `cd dbt && dbt build --select tag:congressional`

Selectively run layers
- stg only: `dbt run --select tag:stg,tag:congressional`
- stg+int: `dbt run --select tag:congressional,tag:stg+ tag:int`
- full stack: `dbt build --select tag:congressional`

Notes
- Models reference existing normalized staging tables (produced by `StreamlinedNormalizer`).
- If a column does not exist in a given table, stg models avoid failing by only computing derived fields when the column exists.
- For heavy HTML cleaning or complex date parsing, prefer Postgres UDFs later; initial macros here cover common cases.

Core merge strategy
- See `dbt/docs/core_merge_strategy.md` for how we unify Congressional and GovInfo with set/part semantics (`*_set_id`, `part_number`).

Subset run guide (raw → stg → int → core for reports)
- Inputs (raw):
  - govinfo: `bicam_raw_govinfo.congressionalreports_raw`, `bicam_raw_govinfo.congressionalreports_granules_raw`
  - congressional: `bicam_raw_congressional.committeereports_raw`, `bicam_raw_congressional.committeereports_texts_raw`
- Outputs (created by dbt): `dbt_bicam_stg`, `dbt_bicam_int`, `dbt_bicam_core`

Commands
- Stage from raw:
  - `dbt run --select stg_congressional__committeereports stg_congressional__committeereports_texts stg_govinfo__congressionalreports stg_govinfo__congressionalreports_granules`
- Intermediate:
  - `dbt run --select int_congressional__committeereports int_congressional__committeereports_texts int_govinfo__congressionalreports int_govinfo__congressionalreports_granules`
- Core merge:
  - `dbt run --select core__congressional_reports`
