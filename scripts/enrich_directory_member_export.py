"""Apply independently recomputed directory proposals to a fresh Parquet bundle.

Only missing members_terms biography/population/other_url cells may change.
Source ranges and the compilation cutoff remain private, hash-bound evidence.
"""
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

from audit_directory_member_features import audit, TERM_KEY
from export_bills_parquet import sha, digest_rows
from load_family_parquet import validate

ALLOWED_FIELDS = {"biography", "population", "other_url"}


def key(row):
    return tuple(row[k] for k in TERM_KEY)


def selected_changes(report):
    changes = {}
    for record in report["records"]:
        fields = record["proposed_fields"]
        if not fields:
            continue
        if record["status"] != "exact_name_state_chamber_and_directory_url" or not fields.keys() <= ALLOWED_FIELDS:
            raise ValueError("unreviewed directory enrichment rule/field")
        identity = key(record["term_key"])
        if identity in changes or identity[1] != 119:
            raise ValueError("duplicate or non-119 directory term")
        if "biography" in fields and (record["prose_issues"] or "\ufffd" in fields["biography"]):
            raise ValueError("damaged biography cannot be published")
        changes[identity] = fields
    return changes


def verify_terms(before, after, changes):
    old = {key(r): r for r in before}
    new = {key(r): r for r in after}
    if len(old) != len(before) or len(new) != len(after) or old.keys() != new.keys() or not changes.keys() <= old.keys():
        raise ValueError("member-term identities/cardinality changed")
    for identity, row in old.items():
        fields = changes.get(identity, {})
        if not fields.keys() <= ALLOWED_FIELDS or any(row[k] is not None for k in fields):
            raise ValueError("only missing approved term fields may change")
        if new[identity] != {**row, **fields}:
            raise ValueError("unexpected member-term field change")


def enrich(bundle, evidence, audit_path, output):
    manifest = validate(bundle)
    reviewed = json.loads(audit_path.read_text())
    recomputed = audit(bundle, evidence)
    if {k: v for k, v in reviewed.items() if k != "created_at"} != {k: v for k, v in recomputed.items() if k != "created_at"}:
        raise ValueError("directory audit differs from frozen inputs or current rules")
    changes = selected_changes(recomputed)
    if not changes:
        raise ValueError("no safe directory changes")
    inventory = {t["name"]: t for t in manifest["tables"]}
    original = pq.read_table(bundle / inventory["members_terms"]["file"])
    before = original.to_pylist()
    after = [{**row, **changes.get(key(row), {})} for row in before]
    verify_terms(before, after, changes)
    output.mkdir(parents=True, exist_ok=False)
    updated = copy.deepcopy(manifest)
    table_results = []
    for table in updated["tables"]:
        path = output / table["file"]
        if table["name"] == "members_terms":
            pq.write_table(pa.Table.from_pylist(after, schema=original.schema), path, compression="zstd", row_group_size=8192)
            observed = pq.read_table(path).to_pylist()
            verify_terms(before, observed, changes)
            digest = hashlib.sha256()
            digest_rows(digest, observed)
            table.update({"sha256": sha(path), "bytes": path.stat().st_size, "typed_rows_sha256": digest.hexdigest(),
                          "null_counts": {c: sum(r[c] is None for r in observed) for c in original.column_names}})
        else:
            shutil.copyfile(bundle / table["file"], path)
            if sha(path) != table["sha256"]:
                raise ValueError("unaffected table copy changed")
        table_results.append({"table": table["name"], "rows": table["row_count"], "exact_delta": True,
                              "changed_fields": sorted(ALLOWED_FIELDS) if table["name"] == "members_terms" else []})
    for filename in ("inclusion.json", manifest.get("identity_decisions_file"), manifest.get("member_enrichments_file")):
        if filename:
            shutil.copyfile(bundle / filename, output / filename)
    decisions = {"schema_version": 1, "created_at": datetime.now(timezone.utc).isoformat(),
                 "previous_candidate_manifest_sha256": sha(bundle / "manifest.json"),
                 "audit_report_sha256": sha(audit_path), "capture_report_sha256": reviewed["capture_report_sha256"],
                 "source_sha256": reviewed["source_sha256"], "source_url": reviewed["source_url"],
                 "source_observed_at": reviewed["source_observed_at"], "compilation_cutoff": reviewed["compilation_cutoff"],
                 "population_semantics": "State/territory 2020 Census population, not district or current-year estimate.",
                 "biography_transformation": "Retain source spelling/case/newlines; trim line-end and outer whitespace only.",
                 "title_and_social_fields": "Unchanged; no invented granule titles or social links.",
                 "records": reviewed["records"], "unmatched_candidate_members": reviewed["unmatched_candidate_members"],
                 "production_modified": False, "release_accepted": False}
    with (output / "directory-enrichments.json").open("x") as stream:
        json.dump(decisions, stream, indent=2)
        stream.write("\n")
    updated.update({"created_at": datetime.now(timezone.utc).isoformat(), "previous_candidate_manifest_sha256": sha(bundle / "manifest.json"),
                    "directory_enrichments_file": "directory-enrichments.json",
                    "directory_enrichments_sha256": sha(output / "directory-enrichments.json"),
                    "member_terms_directory_enriched": len(changes), "release_ready": False})
    with (output / "manifest.json").open("x") as stream:
        json.dump(updated, stream, indent=2)
        stream.write("\n")
    validate(output)
    validation = {"schema_version": 1, "candidate_manifest_sha256": sha(output / "manifest.json"),
                  "previous_candidate_manifest_sha256": sha(bundle / "manifest.json"), "tables": table_results,
                  "tables_verified": len(table_results), "changed_term_rows": len(changes),
                  "changed_term_cells": sum(len(c) for c in changes.values()), "unexpected_changes": 0,
                  "unchanged_table_files": len(table_results) - 1,
                  "unchanged_inclusion_ledger": sha(output / "inclusion.json") == sha(bundle / "inclusion.json"),
                  "release_accepted": False}
    with (output / "directory-delta-validation.json").open("x") as stream:
        json.dump(validation, stream, indent=2)
        stream.write("\n")
    return validation


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    for option in ("bundle", "evidence", "audit", "output"):
        parser.add_argument(f"--{option}", type=Path, required=True)
    args = parser.parse_args()
    result = enrich(args.bundle, args.evidence, args.audit, args.output)
    print(json.dumps({k: v for k, v in result.items() if k != "tables"}))
