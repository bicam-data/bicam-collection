"""Apply evidence-bound ICPSR enrichment to a fresh, original-format candidate.

No downloads or database writes. Recomputes the audit from its frozen Voteview
CSV and original BICAM archive. Only the members.icpsr field may change.
"""
from __future__ import annotations

import argparse
import copy
from datetime import datetime, timezone
import csv
import hashlib
import io
import json
from pathlib import Path
import shutil
import zipfile

import pyarrow as pa
import pyarrow.parquet as pq

from audit_member_icpsr_export import propose, normalize_icpsr
from export_bills_parquet import sha, digest_rows
from load_family_parquet import validate


def selected_enrichments(proposals, policy="unresolved", authority=None):
    if policy not in {"unresolved", "published_bicam"}:
        raise ValueError("unreviewed multiple-ID policy")
    if policy == "published_bicam" and not authority:
        raise ValueError("published multiple-ID policy requires recorded user direction")
    reviewed_legacy = {"K000401": "22336", "V000133": "21980"}
    changes, unresolved = {}, []
    for row in proposals:
        if row["current_icpsr"] is not None:
            raise ValueError("only missing ICPSR values may be enriched")
        if (row["status"] == "exact_bioguide_congress_chamber_state"
                and row.get("published_comparison") in {"agrees", "not_available"}):
            changes[row["bioguide_id"]] = row["proposed_icpsr"]
        elif policy == "published_bicam" and row["bioguide_id"] in reviewed_legacy:
            if row["published_icpsr"] != reviewed_legacy[row["bioguide_id"]]:
                raise ValueError("reviewed published identifier differs")
            changes[row["bioguide_id"]] = row["published_icpsr"]
        else:
            unresolved.append(row)
    if len(set(changes.values())) != len(changes):
        raise ValueError("ICPSR enrichment would conflate distinct members")
    return changes, unresolved


def verify_member_changes(before, after, changes):
    before_by_id = {row["bioguide_id"]: row for row in before}
    after_by_id = {row["bioguide_id"]: row for row in after}
    if (len(before_by_id) != len(before) or len(after_by_id) != len(after)
            or before_by_id.keys() != after_by_id.keys() or not changes.keys() <= before_by_id.keys()):
        raise ValueError("member identities/cardinality changed")
    for identity, old in before_by_id.items():
        expected = {**old, "icpsr": changes[identity]} if identity in changes else old
        if after_by_id[identity] != expected:
            raise ValueError("unexpected member field change")


def enrich(bundle, evidence_folder, archive, output, policy="unresolved", authority=None):
    manifest = validate(bundle)
    report_path = evidence_folder / "report.json"
    report = json.loads(report_path.read_text())
    if report["candidate_manifest_sha256"] != sha(bundle / "manifest.json") or report["published_archive_sha256"] != sha(archive):
        raise ValueError("member evidence targets another candidate or original archive")
    source = evidence_folder / report["source_file"]
    if source.is_symlink() or source.parent.resolve() != evidence_folder.resolve() or sha(source) != report["source_sha256"]:
        raise ValueError("captured Voteview source changed")
    with source.open(newline="", encoding="utf-8-sig") as stream:
        voteview = list(csv.DictReader(stream))
    with zipfile.ZipFile(archive) as z:
        names = [n for n in z.namelist() if Path(n).name == "members.csv" and not n.startswith("__MACOSX/")]
        if len(names) != 1 or z.getinfo(names[0]).file_size > 20_000_000:
            raise ValueError("unreviewed original members archive")
        with z.open(names[0]) as stream:
            original = list(csv.DictReader(io.TextIOWrapper(stream, encoding="utf-8-sig")))
    published = {r["bioguide_id"]: normalize_icpsr(r["icpsr"]) for r in original}
    if len(published) != len(original):
        raise ValueError("duplicate original member identity")
    inventory = {t["name"]: t for t in manifest["tables"]}
    members_table = pq.read_table(bundle / inventory["members"]["file"])
    members = members_table.to_pylist()
    terms = pq.read_table(bundle / inventory["members_terms"]["file"]).to_pylist()
    proposals = propose(members, terms, voteview, published)
    if json.loads(json.dumps(proposals, default=str)) != report["proposals"]:
        raise ValueError("recomputed proposals differ from captured audit")
    changes, unresolved = selected_enrichments(proposals, policy, authority)
    enriched = [{**row, "icpsr": changes[row["bioguide_id"]]} if row["bioguide_id"] in changes else row for row in members]
    verify_member_changes(members, enriched, changes)
    output.mkdir(parents=True, exist_ok=False)
    updated = copy.deepcopy(manifest)
    table_results = []
    for table in updated["tables"]:
        if table["name"] == "members":
            path = output / table["file"]
            pq.write_table(pa.Table.from_pylist(enriched, schema=members_table.schema), path, compression="zstd", row_group_size=8192)
            observed = pq.read_table(path).to_pylist()
            verify_member_changes(members, observed, changes)
            digest = hashlib.sha256()
            digest_rows(digest, observed)
            table.update({"row_count": len(observed), "sha256": sha(path), "bytes": path.stat().st_size,
                          "typed_rows_sha256": digest.hexdigest(),
                          "null_counts": {c: sum(r[c] is None for r in observed) for c in members_table.column_names}})
        else:
            shutil.copyfile(bundle / table["file"], output / table["file"])
            if sha(output / table["file"]) != table["sha256"]:
                raise ValueError("unaffected table copy changed")
        table_results.append({"table": table["name"], "rows": table["row_count"],
                              "changed_fields": ["icpsr"] if table["name"] == "members" else [], "exact_delta": True})
    for filename in ("inclusion.json", manifest.get("identity_decisions_file")):
        if filename:
            shutil.copyfile(bundle / filename, output / filename)
    decisions = {"schema_version": 1, "created_at": datetime.now(timezone.utc).isoformat(),
        "previous_candidate_manifest_sha256": sha(bundle / "manifest.json"), "audit_report_sha256": sha(report_path),
        "voteview_source_sha256": report["source_sha256"], "original_archive_sha256": report["published_archive_sha256"],
        "changes": [{"bioguide_id": identity, "old_icpsr": None, "new_icpsr": code} for identity, code in sorted(changes.items())],
        "unresolved": unresolved, "multiple_id_policy": policy, "policy_authority": authority,
        "production_modified": False, "source_observed_at": report["observed_at"],
        "temporal_scope": "Post-snapshot auxiliary identity enrichment, not retroactive raw-source knowledge."}
    with (output / "member-enrichments.json").open("x") as stream:
        json.dump(decisions, stream, indent=2, default=str)
        stream.write("\n")
    updated.update({"created_at": datetime.now(timezone.utc).isoformat(),
        "previous_candidate_manifest_sha256": sha(bundle / "manifest.json"),
        "member_enrichments_file": "member-enrichments.json", "member_enrichments_sha256": sha(output / "member-enrichments.json"),
        "members_icpsr_enriched": len(changes), "members_icpsr_unresolved": len(unresolved), "release_ready": False})
    with (output / "manifest.json").open("x") as stream:
        json.dump(updated, stream, indent=2)
        stream.write("\n")
    validate(output)
    validation = {"schema_version": 1, "candidate_manifest_sha256": sha(output / "manifest.json"),
        "previous_candidate_manifest_sha256": sha(bundle / "manifest.json"), "tables": table_results,
        "tables_verified": len(table_results), "changed_member_cells": len(changes), "unexpected_changes": 0,
        "unchanged_table_files": len(table_results) - 1, "unchanged_inclusion_ledger": sha(output / "inclusion.json") == sha(bundle / "inclusion.json"),
        "unresolved_count": len(unresolved), "release_accepted": False}
    with (output / "member-delta-validation.json").open("x") as stream:
        json.dump(validation, stream, indent=2)
        stream.write("\n")
    return validation


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    for option in ("bundle", "evidence", "archive", "output"):
        parser.add_argument(f"--{option}", type=Path, required=True)
    parser.add_argument("--multiple-id-policy", choices=("unresolved", "published_bicam"), default="unresolved")
    parser.add_argument("--policy-authority")
    args = parser.parse_args()
    result = enrich(args.bundle, args.evidence, args.archive, args.output, args.multiple_id_policy, args.policy_authority)
    print(json.dumps({k: v for k, v in result.items() if k != "tables"}))
