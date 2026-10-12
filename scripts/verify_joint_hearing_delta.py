"""Independently verify the exact joint-hearing identity change across 89 tables."""
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


def typed(row, columns):
    result = {}
    for column in columns:
        raw = row[column["name"]]
        if raw is not None and not isinstance(raw, str):
            raw = str(raw).lower() if isinstance(raw, bool) else str(raw)
        result[column["name"]] = value(raw, column["postgres_type"])
    return result


def verify(before, after):
    old, new = validate(before), validate(after)
    if new["previous_candidate_manifest_sha256"] != sha(before / "manifest.json"):
        raise ValueError("identity candidate targets a different predecessor")
    decision = json.loads((after / new["identity_decisions_file"]).read_text())
    if decision["previous_manifest_sha256"] != sha(before / "manifest.json"):
        raise ValueError("identity decision targets a different predecessor")
    aliases = {"hhrg60081-119", "shrg60081-119"}
    canonical = "jhrg60081-119"
    if decision["aliases"] != {old_id: canonical for old_id in aliases}:
        raise ValueError("unreviewed identity mappings")
    previous = {t["name"]: t for t in old["tables"]}
    results = []
    for table in new["tables"]:
        name = table["name"]
        entry = previous[name]
        if entry["columns"] != table["columns"] or entry.get("foreign_keys") != table.get("foreign_keys"):
            raise ValueError("schema contract changed during identity repair")
        rows = pq.read_table(before / entry["file"]).to_pylist()
        actual = pq.read_table(after / table["file"]).to_pylist()
        fields = ["hearing_id"] if name == "hearings" else [f for f, target in table.get("foreign_keys", {}).items() if target == "hearings.hearing_id"]
        affected = [r for r in rows if any(r[f] in aliases for f in fields)]
        expected = Counter(row_digest(r) for r in rows if not any(r[f] in aliases for f in fields))
        if affected:
            retained_source = decision["all_source_rows_affected"][name]
            if Counter(row_digest(typed(r, entry["columns"])) for r in retained_source) != Counter(map(row_digest, affected)):
                raise ValueError("alternate source observations were lost or changed in sidecar")
            if name == "hearings":
                root = next(r for r in affected if r["hearing_id"] == "hhrg60081-119")
                expected_root = {**root, "hearing_id": canonical, "chamber": "joint", "pages": 179, "session": 1}
                if typed(decision["canonical_root"], entry["columns"]) != expected_root:
                    raise ValueError("unexpected canonical root field choice")
                changed = [expected_root]
            elif name == "hearings_texts":
                preferred = next(r for r in affected if r["hearing_id"] == "hhrg60081-119")
                changed = [{**preferred, "hearing_id": canonical}]
            else:
                changed = [{**r, **{f: canonical if r[f] in aliases else r[f] for f in fields}} for r in affected]
            # Exact duplicate relationship rows collapse; conflicting payloads
            # cannot disappear unnoticed because the expected set keeps both.
            expected.update(set(row_digest(r) for r in changed))
        if expected != Counter(map(row_digest, actual)):
            raise ValueError(f"unexpected identity delta: {name}")
        if any(r[f] in aliases for r in actual for f in fields):
            raise ValueError("obsolete hearing ID remains in the exported graph")
        if not affected and table["sha256"] != entry["sha256"]:
            raise ValueError("unaffected file was rewritten")
        results.append({"table": name, "before_rows": len(rows), "after_rows": len(actual),
                        "affected_source_rows": len(affected), "exact_identity_delta": True})
    previous_inclusion = json.loads((before / "inclusion.json").read_text())
    expected_inclusion = json.loads(json.dumps(previous_inclusion))
    expected_inclusion["hearings"]["primary"] = sorted((set(previous_inclusion["hearings"]["primary"]) - aliases) | {canonical})
    expected_inclusion["hearings"]["dependency_only"] = sorted(set(previous_inclusion["hearings"]["dependency_only"]) - aliases)
    if expected_inclusion != json.loads((after / "inclusion.json").read_text()):
        raise ValueError("unexpected inclusion-scope change")
    return {"schema_version": 1, "verified_at": datetime.now(timezone.utc).isoformat(),
            "previous_manifest_sha256": sha(before / "manifest.json"), "candidate_manifest_sha256": sha(after / "manifest.json"),
            "tables": results, "tables_verified": len(results), "unexpected_changes": 0,
            "row_reduction": sum(r["before_rows"] - r["after_rows"] for r in results),
            "obsolete_hearing_references": 0, "source_alternatives_preserved": True, "release_accepted": False}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    for option in ("before", "after", "output"):
        parser.add_argument(f"--{option}", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("output exists; preserve prior validation")
    report = verify(args.before, args.after)
    with args.output.open("x") as stream:
        json.dump(report, stream, indent=2)
        stream.write("\n")
    print(json.dumps({k: v for k, v in report.items() if k != "tables"}))
