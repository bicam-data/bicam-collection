"""Verify a candidate differs from its predecessor only by declared repairs."""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import json
from pathlib import Path

import pyarrow.parquet as pq

from export_bills_parquet import sha, value
from load_family_parquet import validate
from verify_family_parquet_roundtrip import row_digest


def rows_counter(path):
    result = Counter()
    for batch in pq.ParquetFile(path).iter_batches(batch_size=4096):
        result.update(row_digest(row) for row in batch.to_pylist())
    return result


def typed_digest(row, columns):
    result = {}
    for c in columns:
        v = row.get(c["name"])
        if v is not None and not isinstance(v, str):
            v = str(v).lower() if isinstance(v, bool) else str(v)
        result[c["name"]] = value(v, c["postgres_type"])
    return row_digest(result)


def verify(before, after, amendments, hearing_children):
    base, current = validate(before), validate(after)
    repair = json.loads(amendments.read_text())
    hearing = json.loads(hearing_children.read_text())
    if any(e["candidate_manifest_sha256"] != sha(before / "manifest.json") for e in (repair, hearing)):
        raise ValueError("repair evidence targets a different predecessor")
    if current["amendment_supplement_sha256"] != sha(amendments) or current["child_supplement_sha256"] != sha(hearing_children):
        raise ValueError("candidate manifest does not bind the specified supplements")
    if base["source_manifest_sha256"] != current["source_manifest_sha256"]:
        raise ValueError("candidate frozen sources differ")
    previous_tables = {t["name"]: t for t in base["tables"]}
    tables = []
    for table in current["tables"]:
        name = table["name"]
        previous = previous_tables[name]
        if previous["columns"] != table["columns"]:
            raise ValueError("column/type contract changed")
        expected = rows_counter(before / previous["file"])
        replacements = repair["root_replacements"] if name == "amendments" else []
        for entry in replacements:
            old, new = (typed_digest(entry[k], table["columns"]) for k in ("expected", "row"))
            if expected[old] != 1:
                raise ValueError("replacement does not identify exactly one predecessor row")
            expected[old] -= 1
            expected[new] += 1
        additions = repair["child_additions"].get(name, []) + [e["row"] for e in hearing["additions"].get(name, [])]
        expected.update(typed_digest(row, table["columns"]) for row in additions)
        if +expected != rows_counter(after / table["file"]):
            raise ValueError(f"unexpected changed, missing or added rows: {name}")
        tables.append({"table": name, "previous_rows": previous["row_count"], "current_rows": table["row_count"],
                       "declared_replacements": len(replacements), "declared_additions": len(additions),
                       "exact_expected_multiset": True})
    return {"schema_version": 1, "verified_at": datetime.now(timezone.utc).isoformat(),
            "previous_manifest_sha256": sha(before / "manifest.json"), "candidate_manifest_sha256": sha(after / "manifest.json"),
            "tables": tables, "tables_verified": len(tables),
            "root_replacements": sum(t["declared_replacements"] for t in tables),
            "child_additions": sum(t["declared_additions"] for t in tables), "unexpected_changes": 0,
            "release_accepted": False}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    for option in ("before", "after", "amendments", "hearing-children", "output"):
        parser.add_argument(f"--{option}", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("output exists; preserve previous validation")
    report = verify(args.before, args.after, args.amendments, args.hearing_children)
    with args.output.open("x") as stream:
        json.dump(report, stream, indent=2)
        stream.write("\n")
    print(json.dumps({k: v for k, v in report.items() if k != "tables"}))
