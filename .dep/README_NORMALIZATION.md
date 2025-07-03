# Bicam Normalisation Layer

This directory implements the **raw → intermediate** step of the new ETL
architecture.  It converts the JSONB blobs landed by the scrapers into
tidied, column-oriented tables ready for the downstream `cleaning/` phase.

```
raw schemas          intermediate schemas          cleaned schemas           merge
──────────────────    ─────────────────────────     ────────────────────    ───────────
bicam_raw_*          bicam_intermediate_*          bicam_*                 bicam
(bills_raw)   ───▶   (bills)   ───▶                (bills…)   ───▶         (facts + dims)
```

## How it works

1.  **Specs** (`normalization/specs/<data_type>.py`)
    • `FIELD_MAP` – maps source-JSON keys to desired column names.
    • `KEY_COLUMNS` – primary/unique keys for writing.
    • `custom_transforms(lazy_df)` – optional Polars logic for extra tweaks.

2.  **Normalizer** (`normalization/normalizer.py`)
    • Reads raw rows from `bicam_raw_<source>.<data_type>_raw`.
    • Builds a Polars LazyFrame, applies `FIELD_MAP` + `custom_transforms`.
    • Materialises the result into the matching
      `bicam_intermediate_<source>.<data_type>` table using the existing
      `write_data()` helper (which adds indexes / dedup logic).

## Adding a new data type

1.  Create a spec module:

```python
# bicam_collection/normalization/specs/amendments.py
FIELD_MAP = {
    "amendmentId": "amendment_id",
    "congress":    "congress",
    # …
}

KEY_COLUMNS = ["amendment_id"]

import polars as pl

def custom_transforms(df: pl.LazyFrame) -> pl.LazyFrame:
    return (
        df.with_columns([
            pl.col("congress").cast(pl.Int64, strict=False),
            pl.col("chamber").str.to_lowercase(),
        ])
    )
```

2.  Add the raw & intermediate schema names in `RAW_SCHEMA_MAP` and
    `INTERMEDIATE_SCHEMA_MAP` inside `normalizer.py`.

3.  Execute:

```bash
DATA_TYPE=amendments python -m bicam_collection.normalization.normalizer
```

The script will read from `bicam_raw_congressional.amendments_raw`, apply the
spec, and write into `bicam_intermediate_congressional.amendments`.

4.  Point the `cleaning/datatypes/*` cleaner for that data_type at the
    *intermediate* table instead of the old staging one (usually a trivial
    schema name change).

## Swapping backend logic

All heavy lifting is done with Polars; if you prefer DuckDB/Pandas for a
particular step, wrap that inside `custom_transforms()`—the rest of the
pipeline stays intact.

## Parallel / incremental loads

`normalizer.py` currently fetches *all* rows; for high-volume tables you can
stream by `WHERE fetched_at >= :last_run` or paginate by id range.  Polars
makes it easy to process in batches—just adapt `fetch_raw_payloads()`.

## Testing instructions

To test the full Bills pipeline end-to-end:

1. Run the scrapers so raw payloads exist:
   ```bash
   python -m bicam_collection.scrapers.congressional --data-type bills
   ```
   (or whichever CLI you normally invoke)

2. Normalise the parent table and fetch all child endpoints:
   ```bash
   DATA_TYPE=bills python -m bicam_collection.normalization.normalizer
   ```
   This should create / refresh:
     • bicam_intermediate_congressional.bills
     • bicam_raw_congressional.bills_actions_raw (plus other *_raw tables)
     • bicam_intermediate_congressional.bills_cbo_cost_estimates
     • bicam_intermediate_congressional.bills_laws
     • … and any other nested tables configured.

3. Normalise the *actions* payload (to flatten recorded votes):
   ```bash
   DATA_TYPE=bills_actions python -m bicam_collection.normalization.normalizer
   ```
   This writes:
     • bicam_intermediate_congressional.bills_actions
     • bicam_intermediate_congressional.bills_actions_recorded_votes

4. Inspect counts:
   ```sql
   SELECT COUNT(*) FROM bicam_intermediate_congressional.bills;
   SELECT COUNT(*) FROM bicam_intermediate_congressional.bills_actions_recorded_votes;
   ```

5. Point your cleaning script (`cleaning/datatypes/bills.py`) at the
   `bicam_intermediate_congressional` schema and run it; the merge phase
   continues unchanged.

If something is missing, check the spec files:
• FIELD_MAP – is the key spelled exactly as it appears in the JSON?  
• CHILD_ENDPOINTS – does `url_field` match the renamed column?  
• NESTED_ARRAYS – is the JSONPath correct?

---
This README will grow as we port more data types.  Feel free to open issues or
PRs!  