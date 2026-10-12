"""Verify frozen all-family files and profile updated-since export blockers.

Offline only. Complete scans, exact hash/count/header checks. No publication,
database access, inferred timestamp substitution, or source-value mutations.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as acsv
import yaml

from bicam_collection.export_semantics import classify_recency, normalize_value, CHAMBER_COLUMNS, TYPE_FIELDS


def sha(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def audit(snapshot, contract_path, since):
    manifest_path = snapshot / "manifest.json"
    manifest_hash = sha(manifest_path)
    manifest = json.loads(manifest_path.read_text())
    if manifest.get("serialization", {}).get("csv_null") != r"\N":
        raise ValueError("explicit NULL format required")
    contract = yaml.safe_load(contract_path.read_text())
    inventory = {t["name"]: t for t in manifest["tables"]}
    reports, omitted = [], []
    for family in contract["families"]:
        for relation in [family, *family.get("children", [])]:
            name = relation["table"]
            if relation["feature_status"] != "core-now":
                omitted.append({"table": name, "reason": relation["feature_status"]})
                continue
            entry = inventory[name]
            path = snapshot / entry["file"]
            if path.is_symlink() or path.parent.resolve() != snapshot.resolve() or sha(path) != entry["sha256"]:
                raise ValueError(f"source path/hash mismatch: {name}")
            columns = relation["columns"]
            expected = list(columns)
            policy_columns = set(columns) & CHAMBER_COLUMNS
            policy_columns.update(c for t, c in TYPE_FIELDS if t == name)
            timestamps = [k for k in ("created_at", "updated_at") if k in columns]
            missing, domain, changes, exceptions, recency = Counter(), {}, Counter(), Counter(), Counter()
            n = 0
            with acsv.open_csv(
                path, read_options=acsv.ReadOptions(block_size=32 * 1024 * 1024, use_threads=False),
                parse_options=acsv.ParseOptions(newlines_in_values=True),
                convert_options=acsv.ConvertOptions(column_types={k: pa.string() for k in columns},
                    null_values=[r"\N"], strings_can_be_null=True, quoted_strings_can_be_null=False),
            ) as reader:
                if reader.schema.names != expected:
                    raise ValueError(f"header differs: {name}")
                for batch in reader:
                    n += batch.num_rows
                    for field in columns:
                        array = batch.column(field)
                        missing[field] += array.null_count
                        if field in policy_columns:
                            d = domain.setdefault(field, Counter())
                            for raw in array.to_pylist():
                                d["<SQL NULL>" if raw is None else raw] += 1
                                normalized, reason = normalize_value(name, field, raw)
                                if normalized != raw:
                                    changes[(field, str(raw), str(normalized))] += 1
                                if reason:
                                    exceptions[(field, str(raw), reason)] += 1
                    if timestamps:
                        for row in batch.select(timestamps).to_pylist():
                            recency[classify_recency(row, since)] += 1
            if n != entry["row_count"]:
                raise ValueError(f"row count differs: {name}")
            report = {"family": family["name"], "table": name, "root": name == family["table"],
                      "source_sha256": entry["sha256"], "rows": n,
                      "timestamp_columns": timestamps,
                      "timestamp_classification": dict(recency) if timestamps else {"unknown_timestamp": n},
                      "all_null_columns": [k for k in columns if n and missing[k] == n],
                      "null_counts": dict(missing), "domains": {k: dict(v) for k, v in domain.items()},
                      "proposed_normalizations": [{"column": k, "from": old, "to": new, "rows": v}
                                                   for (k, old, new), v in changes.items()],
                      "unresolved_values": [{"column": k, "value": raw, "reason": why, "rows": v}
                                            for (k, raw, why), v in exceptions.items()]}
            reports.append(report)
            print(json.dumps({"table": name, "rows": n, "recency": report["timestamp_classification"],
                              "normalizations": sum(changes.values()), "unresolved": sum(exceptions.values())}), flush=True)
    if sha(manifest_path) != manifest_hash:
        raise ValueError("source manifest changed during audit")
    return {"schema_version": 1, "created_at": datetime.now(timezone.utc).isoformat(),
            "snapshot": snapshot.name, "manifest_sha256": manifest_hash, "contract_sha256": sha(contract_path),
            "cutoff": since.isoformat(), "family_count": len(contract["families"]),
            "table_count": len(reports), "rows_scanned": sum(r["rows"] for r in reports),
            "tables": reports, "omissions": omitted, "export_ready": False,
            "snapshot_structural_failures": manifest.get("build_evidence", {}).get("data_validation", {}).get("failures", []),
            "limitations": ["Recency is per row; child-to-root propagation and dependency closure are not performed by this audit.",
                            "Unknown timestamp is not evidence of an old record; no creation/event/fetch dates are invented.",
                            "Only explicitly declared chamber/entity-type semantics are profiled; this does not certify every field.",
                            "All source-null counts exclude quoted literal NULL markers and preserve empty strings separately.",
                            "No rows changed; normalizations are proposed transformations with evidence counts."]}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--contract", type=Path, default=Path("contracts/bicam_public_schema.yaml"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--since", default="2025-01-01T00:00:00+00:00")
    args = parser.parse_args()
    since = datetime.fromisoformat(args.since)
    if since.tzinfo is None:
        parser.error("cutoff must include timezone")
    if args.output.exists():
        parser.error("output already exists; preserve earlier audit")
    report = audit(args.snapshot, args.contract, since)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x") as stream:
        json.dump(report, stream, indent=2)
        stream.write("\n")
