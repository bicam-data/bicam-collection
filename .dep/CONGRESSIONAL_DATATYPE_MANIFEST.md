# Adding a New Congressional Data-Type

This document walks you through wiring **any new data-type from the Congress.gov API** into the bicam-collection pipeline so that:

* raw JSON is staged in `bicam_raw_congressional.*_raw` tables (with parent/child links via `source_doc_id`),
* cleaned data can flow into staging/prod tables, and
* you can recreate everything reproducibly from SQL scripts.

---
## 1. Configuration

### 1.1 `configs/congressional_config.yaml`
Add a top-level entry:
```yaml
<datatype>:
  main: true                # it's an API parent stream
  fields: [ … ]            # prod columns
  id_fields: [ <datatype>_id ]
  related_fields: [ actions, … ]
  nested_fields: {}
```

### 1.2 Raw-schema script
Edit `db/sql/build_raw_schema_congressional.sql` → append each table name to the `tbls` array:
```sql
'${datatype}_raw',                    -- parent payloads
'${datatype}_${relation}_raw',        -- one per relation
```
Run:
```bash
psql -f db/sql/build_raw_schema_congressional.sql
```
(or `setup_database --recreate`).

---
## 2. Natural-Key Builder
Create / edit `scrapers/raw_key_builders.py`:
```python
@register("<datatype>")
def _make_id(p: dict[str, Any]) -> str | None:
    # Build <datatype>_id from raw JSON
    a, b = p.get("foo"), p.get("bar")
    return f"{a.lower()}{b}" if a and b else None
```
This is used by `export_raw_jsonb` when the payload lacks the explicit key.

---
## 3. Raw Fetchers
For every relation, implement a fetcher module, e.g. `scrapers/<datatype>_fetchers.py`:
```python
from typing import Any
from pycon.congress.abstractions import PyCongress
from pycon.congress.components import <PyConClass>
from pycon.models import ErrorResult
from .raw_fetchers import register

@register("<datatype>", "<relation>")
async def fetch_<datatype>_<relation>(payload: Any, client: PyCongress) -> list[dict]:
    obj = payload if isinstance(payload, <PyConClass>) else \
          <PyConClass>(data=payload.get("<datatype>", payload),
                       _pagination={}, _adapter=client._adapter)

    results: list[dict] = []
    async for item in obj.get_<relation>():
        if isinstance(item, ErrorResult):
            continue
        raw = getattr(item, "data", None)
        if raw:
            raw["<datatype>_id"] = getattr(obj, "<datatype>_id")
            results.append(raw)
    return results
```
Import this module somewhere (e.g. `scrapers/__init__.py`) so the decorator runs.

---
## 4. Transformer & Prod Schema (optional)
If you need cleaned data in `bicam_congressional.<datatype>`:
1. Add create-table SQL in `build_prod_schema_congressional.sql`.
2. Extend `DataTransformer` or a specialised transformer.

---
## 5. Tests
* Unit-test key builder + fetchers.
* Integration: run scraper with `specific_data_type=<datatype>` then query:
  ```sql
  SELECT COUNT(*) FROM bicam_raw_congressional.<datatype>_raw;
  SELECT DISTINCT source_doc_id FROM bicam_raw_congressional.<datatype>_<relation>_raw;
  ```

---
## 6. Documentation
Update project README or a domain doc with:
* Data-type description
* Relation list
* Natural-key formula

---
### Quick Checklist
- [ ] YAML config entry
- [ ] Tables added to `build_raw_schema_congressional.sql`
- [ ] `raw_key_builders` function registered
- [ ] Fetchers implemented & imported
- [ ] Transformer / prod schema (if needed)
- [ ] Tests
- [ ] Documentation 