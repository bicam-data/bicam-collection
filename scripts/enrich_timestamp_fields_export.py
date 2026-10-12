"""Apply pre-snapshot Congress/committee timestamp evidence to a candidate."""
from __future__ import annotations

import argparse
import copy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shutil

import pyarrow as pa
import pyarrow.parquet as pq

from export_bills_parquet import digest_rows, sha
from load_family_parquet import validate


def apply(bundle, evidence_path, output):
    manifest = validate(bundle)
    evidence = json.loads(evidence_path.read_text())
    if evidence["candidate_manifest_sha256"] != sha(bundle / "manifest.json"):
        raise ValueError("timestamp evidence targets another candidate")
    congress_changes = {int(r["congress_number"]): r["updated_at"] for r in evidence["congress_records"]}
    history_changes = {(r["committee_code"], r["started_at"]): r["updated_at"] for r in evidence["committee_history_records"]}
    inventory = {t["name"]: t for t in manifest["tables"]}
    source_tables = {}
    changes = {}
    for name, field, mapping, key_fields in (
        ("congresses", "updated_at", congress_changes, ("congress_number",)),
        ("committees_history", "updated_at", history_changes, ("committee_code", "started_at")),
    ):
        table = inventory[name]
        parquet = pq.read_table(bundle / table["file"])
        before = parquet.to_pylist()
        after = []
        for row in before:
            if name == "congresses":
                key = int(row["congress_number"])
            else:
                key = tuple(str(row[k]) for k in key_fields)
            value = mapping.get(key)
            if value is not None:
                parsed_value = datetime.fromisoformat(value)
                if row[field] not in (None, "") and row[field] != parsed_value:
                    raise ValueError(f"existing timestamp contradiction in {name}")
                if row[field] in (None, ""):
                    changes[(name, key)] = value
                    row = {**row, field: parsed_value}
            after.append(row)
        source_tables[name] = (parquet, before, after)
    if not changes:
        raise ValueError("no timestamp changes")
    if output.exists():
        # Permit recovery of a directory left by an interrupted local build,
        # but never reuse a completed candidate or an unrelated directory.
        if (output / "manifest.json").exists() or (output / "timestamp-delta-validation.json").exists():
            raise ValueError("output already contains a completed candidate")
    else:
        output.mkdir(parents=True, exist_ok=False)
    updated = copy.deepcopy(manifest)
    results = []
    for table in updated["tables"]:
        path = output / table["file"]
        if table["name"] in source_tables:
            parquet, before, after = source_tables[table["name"]]
            pq.write_table(pa.Table.from_pylist(after, schema=parquet.schema), path, compression="zstd", row_group_size=8192)
            observed = pq.read_table(path).to_pylist()
            if observed != after:
                raise ValueError("timestamp serialization changed values")
            digest = hashlib.sha256()
            digest_rows(digest, observed)
            table.update({"sha256": sha(path), "bytes": path.stat().st_size, "typed_rows_sha256": digest.hexdigest(),
                          "null_counts": {c: sum(r[c] is None for r in observed) for c in parquet.column_names}})
            results.append({"table": table["name"], "changed_rows": sum(a != b for a, b in zip(before, after)),
                            "changed_field": "updated_at", "exact_delta": True})
        else:
            shutil.copyfile(bundle / table["file"], path)
            if sha(path) != table["sha256"]:
                raise ValueError("unaffected table copy changed")
            results.append({"table": table["name"], "changed_rows": 0, "changed_field": None, "exact_delta": True})
    for filename in ("inclusion.json", manifest.get("identity_decisions_file"), manifest.get("member_enrichments_file"),
                     manifest.get("directory_enrichments_file")):
        if filename:
            shutil.copyfile(bundle / filename, output / filename)
    decisions = {"schema_version": 1, "created_at": datetime.now(timezone.utc).isoformat(),
                 "previous_candidate_manifest_sha256": sha(bundle / "manifest.json"),
                 "evidence_sha256": sha(evidence_path),
                 "changes": [{"table": table, "key": list(key) if isinstance(key, tuple) else key, "updated_at": value}
                             for (table, key), value in sorted(changes.items(), key=lambda item: (item[0][0], str(item[0][1])))],
                 "production_modified": False, "release_accepted": False}
    with (output / "timestamp-enrichments.json").open("x") as stream:
        json.dump(decisions, stream, indent=2, default=str)
        stream.write("\n")
    updated.update({"created_at": datetime.now(timezone.utc).isoformat(), "previous_candidate_manifest_sha256": sha(bundle / "manifest.json"),
                    "timestamp_enrichments_file": "timestamp-enrichments.json", "timestamp_enrichments_sha256": sha(output / "timestamp-enrichments.json"),
                    "timestamp_enriched_rows": sum(r["changed_rows"] for r in results), "release_ready": False})
    with (output / "manifest.json").open("x") as stream:
        json.dump(updated, stream, indent=2)
        stream.write("\n")
    validate(output)
    validation = {"schema_version": 1, "candidate_manifest_sha256": sha(output / "manifest.json"),
                  "previous_candidate_manifest_sha256": sha(bundle / "manifest.json"), "tables": results,
                  "tables_verified": len(results), "changed_rows": sum(r["changed_rows"] for r in results),
                  "unexpected_changes": 0, "unchanged_table_files": len(results) - 2,
                  "unchanged_inclusion_ledger": sha(output / "inclusion.json") == sha(bundle / "inclusion.json"),
                  "release_accepted": False}
    with (output / "timestamp-delta-validation.json").open("x") as stream:
        json.dump(validation, stream, indent=2)
        stream.write("\n")
    return validation


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    for option in ("bundle", "evidence", "output"):
        parser.add_argument(f"--{option}", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps({k: v for k, v in apply(args.bundle, args.evidence, args.output).items() if k != "tables"}))
