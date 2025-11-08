# Core Merge Strategy: Congressional × GovInfo

This note documents how we unify Congressional and GovInfo content in the `core_` layer.

Key concepts
- Packages vs. Parts (GovInfo): Many GovInfo collections (e.g., congressional reports) publish a package that may contain multiple parts (granules). For example:
  - Set/package ID (no part): `srpt36-115`
  - Part IDs (granules): `srpt36-1-115`, `srpt36-2-115`, …
- Part number semantics:
  - If a package has no parts, we assign `part_number = 0`.
  - If there are parts, each granule has an integer `part_number >= 1`.
- Set ID derivation:
  - From an ID with parts (e.g., `srpt36-1-115`), the set ID is formed by removing the part segment: `srpt36-115`.
  - From an ID without parts, the set ID is the ID itself.

dbt helpers
- Use macros in `dbt/macros/cleaning.sql`:
  - `govinfo_set_id(id_expr)` returns the set ID string.
  - `govinfo_part_number(id_expr)` returns an integer part number (0 when no parts).

Layer responsibilities
- stg_ layer:
  - Congressional: light standardization; pass through primary keys (e.g., `report_id`, `treaty_id`, `bill_id`).
  - GovInfo: derive `package_set_id` and a `part_number`:
    - Packages (main tables): `part_number = 0`, `package_set_id = govinfo_set_id(package_id)`.
    - Granules (per‑type granule tables): parse `granule_id` to set `part_number` and `package_set_id`.
- int_ layer:
  - Deduplicate on primary key using `processed_at_ts` recency as a tie‑breaker.
  - Prepare bridge fields used for core merges (e.g., normalized report IDs, set IDs).
- core_ layer:
  - Join Congressional and GovInfo on type‑specific set IDs:
    - Congressional reports ↔ GovInfo congressional reports: join on `report_set_id` (Congress) ↔ `package_set_id` (GovInfo).
      - When Congressional is non‑parted: propagate attributes (e.g., summaries, metadata) from the matched Congressional report to all GovInfo parts (part_number 0..N) as separate rows.
    - Treaties ↔ Treaty docs: analogous set/part rules where applicable.
  - Conflict resolution and precedence:
    - Prefer Congressional authoritative metadata when overlapping with GovInfo; fall back to GovInfo when Congressional is missing.
  - Output one row per physical document unit:
    - If no parts, one row with `part_number = 0`.
    - If parts exist, one row per part with inherited Congressional attributes.

Examples
- GovInfo congressional report package `srpt36-115` with 5 parts:
  - stg_govinfo: packages → `package_set_id = 'srpt36-115'`, `part_number = 0`
  - stg_govinfo: granules → `package_set_id = 'srpt36-115'`, `part_number = 1..5`
  - core merge: Congressional report with `report_set_id = 'srpt36-115'` joins and propagates Congressional fields to all 6 rows (0..5).

Open items / future work
- Add robust normalization for Congressional report IDs to generate `report_set_id` (strip formatting, harmonize prefixes, etc.).
- Extend the same set/part pattern to other GovInfo families as needed.
- Add dbt tests asserting part_number ranges and join coverage.

