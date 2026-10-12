"""Apply the specifically evidenced Congress-119/jacket-60081 joint identity.

Offline new-candidate transformation, not a general title/chamber matcher.
Preserves old public IDs, root fields and rendition alternatives in a private
decision sidecar. Every declared hearing foreign key is transformed together.
"""
from __future__ import annotations

import argparse
from collections import Counter
import copy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shutil

import pyarrow as pa
import pyarrow.parquet as pq

from export_bills_parquet import sha, digest_rows
from load_family_parquet import validate
from verify_family_parquet_roundtrip import row_digest

ALIASES = frozenset({"hhrg60081-119", "shrg60081-119"})
CANONICAL = "jhrg60081-119"
PREFERRED = "hhrg60081-119"


def checked_relative(folder, filename, digest):
    path = folder / filename
    if path.is_symlink() or not path.resolve().is_relative_to(folder.resolve()) or sha(path) != digest:
        raise ValueError("identity evidence path/hash mismatch")
    return path


def identity_fields(table):
    if table["name"] == "hearings":
        return ["hearing_id"]
    return [column for column, target in table.get("foreign_keys", {}).items() if target == "hearings.hearing_id"]


def adjudicate(bundle, manifest, exceptions):
    proposed = json.loads(exceptions.read_text())
    if proposed["candidate_manifest_sha256"] != sha(bundle / "manifest.json") or len(proposed["exceptions"]) != 1:
        raise ValueError("identity decision is not bound to this predecessor")
    exception = proposed["exceptions"][0]
    if (set(exception["source_public_ids"]) != ALIASES or exception["proposed_canonical_id"] != CANONICAL
            or exception["verified_chamber"] != "joint" or exception["congress"] != 119):
        raise ValueError("outside the specifically reviewed joint-hearing case")
    evidence_path = checked_relative(exceptions.parent, exception["evidence_report"], exception["evidence_report_sha256"])
    evidence = json.loads(evidence_path.read_text())
    opening = evidence["transcript_opening"]
    if opening["prefix_sha256"] != exception["transcript_opening_sha256"] or opening["http_status"] != 200:
        raise ValueError("official joint-hearing opening evidence differs")
    opening_path = checked_relative(evidence_path.parent, opening["file"], opening["prefix_sha256"])
    text = " ".join(opening_path.read_text().split()).upper()
    if "HOUSE-SENATE JOINT HEARING" not in text or "APRIL 8, 2025" not in text:
        raise ValueError("official opening does not corroborate this joint hearing")
    govinfo = next(r for r in evidence["govinfo_results"] if r["package_id"] == exception["govinfo_package_id"])
    if govinfo["http_status"] != 200 or govinfo["status"] != "identity_verified":
        raise ValueError("official package presence not verified")
    package_path = checked_relative(evidence_path.parent, govinfo["file"], govinfo["sanitized_payload_sha256"])
    package = json.loads(package_path.read_text())
    if (package["packageId"] != "CHRG-119hhrg60081" or str(package["congress"]) != "119"
            or package["heldDates"] != [exception["hearing_date"]]):
        raise ValueError("official package Congress/date contradicts decision")
    inventory = {t["name"]: t for t in manifest["tables"]}
    roots = pq.read_table(bundle / inventory["hearings"]["file"]).to_pylist()
    if any(r["hearing_id"] == CANONICAL for r in roots):
        raise ValueError("proposed canonical identity already exists")
    roots = [r for r in roots if r["hearing_id"] in ALIASES]
    if len(roots) != 2 or {r["loc_id"] for r in roots} != set(exception["congress_loc_ids"]):
        raise ValueError("unexpected source root identities")
    if any(r["congress"] != 119 or r["hearing_jacketnumber"] != "60081" or r["title"] != package["title"] for r in roots):
        raise ValueError("root Congress/jacket/title contradiction")
    for name, field, expected in (("hearings_dates", "hearing_date", exception["hearing_date"]),
                                  ("committee_meetings_associated_hearings", "meeting_id", exception["shared_committee_meeting_id"])):
        rows = pq.read_table(bundle / inventory[name]["file"]).to_pylist()
        for identity in ALIASES:
            if {str(r[field]) for r in rows if r["hearing_id"] == identity} != {expected}:
                raise ValueError("hearing date/shared meeting evidence differs")
    preferred = next(r for r in roots if r["hearing_id"] == PREFERRED)
    if any(r["updated_at"] is None or r["updated_at"] > preferred["updated_at"] for r in roots):
        raise ValueError("reviewed preferred source is no longer the latest Congress observation")
    # Verify the captured Congress observations, including both committees on
    # the preferred record. A joint label is not inferred from matching titles.
    congress = []
    for item in evidence["local_congress_observations"]:
        path = checked_relative(evidence_path.parent, item["file"], item["payload_sha256"])
        hearing = json.loads(path.read_text())["hearing"]
        if hearing.get("libraryOfCongressIdentifier") in exception["congress_loc_ids"]:
            congress.append(hearing)
    if len(congress) != 2 or any(h["jacketNumber"] != 60081 or h["congress"] != 119 for h in congress):
        raise ValueError("captured Congress identities do not corroborate the roots")
    preferred_source = next(h for h in congress if h["libraryOfCongressIdentifier"] == preferred["loc_id"])
    if {c["systemCode"] for c in preferred_source.get("committees", [])} != {"hssm00", "sssb00"}:
        raise ValueError("preferred official record does not identify both chamber committees")
    merged = {**preferred, "hearing_id": CANONICAL, "chamber": "joint"}
    filled = {}
    for field in ("session", "pages"):
        if merged[field] is None and package.get(field) not in (None, ""):
            merged[field] = int(package[field])
            filled[field] = {"value": merged[field], "source": "verified_exact_GovInfo_package"}
    return {"schema_version": 1, "decision_id": exception["exception_id"],
        "status": "accepted_exact_source_joint_identity", "decided_at": datetime.now(timezone.utc).isoformat(),
        "reviewer": "agent_source_adjudication_not_human_review",
        "previous_manifest_sha256": sha(bundle / "manifest.json"), "exception_sha256": sha(exceptions),
        "evidence_report_sha256": sha(evidence_path), "transcript_opening_sha256": opening["prefix_sha256"],
        "aliases": {identity: CANONICAL for identity in sorted(ALIASES)}, "canonical_id": CANONICAL,
        "preferred_source_public_id": PREFERRED, "canonical_root": merged, "source_root_observations": roots,
        "filled_root_fields": filled,
        "scalar_field_policy": "Use latest corroborated Congress source LOC/citation; retain alternate source root values in this sidecar. Citation describes source publication series, not canonical chamber.",
        "rendition_policy": "Keep the preferred House record's observed Congress URLs at the original one-row grain; preserve all alternate rows here. Do not synthesize joint source URLs.",
        "all_source_rows_affected": {}, "table_changes": [], "production_modified": False}


def transform(table, rows, canonical_root):
    fields = identity_fields(table)
    affected = [r for r in rows if any(r[f] in ALIASES for f in fields)]
    if not affected:
        return rows, [], 0
    retained = [r for r in rows if not any(r[f] in ALIASES for f in fields)]
    if table["name"] == "hearings":
        projected = [canonical_root]
    elif table["name"] == "hearings_texts":
        preferred = [r for r in affected if r["hearing_id"] == PREFERRED]
        if len(preferred) != 1 or preferred[0]["raw_text"] is not None:
            raise ValueError("unexpected preferred rendition or document body")
        projected = [{**preferred[0], "hearing_id": CANONICAL}]
    else:
        projected = [{**r, **{f: CANONICAL if r[f] in ALIASES else r[f] for f in fields}} for r in affected]
    keys = table.get("primary_key")
    if table.get("key_semantics") != "unique" or not keys:
        raise ValueError("affected table has an unreviewed grouping grain")
    seen = {tuple(r[c] for c in keys): r for r in retained}
    result = list(retained)
    for row in projected:
        key = tuple(row[c] for c in keys)
        if key in seen:
            if seen[key] != row:
                raise ValueError(f"conflicting remapped child payload: {table['name']}")
            continue
        seen[key] = row
        result.append(row)
    return result, affected, len(rows) - len(result)


def repair(bundle, exceptions, output):
    original = validate(bundle)
    if original["congress"] != 119 or original["structural_issues"]:
        raise ValueError("expected a structurally validated Congress-119 predecessor")
    decision = adjudicate(bundle, original, exceptions)
    manifest = copy.deepcopy(original)
    output.mkdir(parents=True, exist_ok=False)
    for table in manifest["tables"]:
        source, destination = bundle / table["file"], output / table["file"]
        fields = identity_fields(table)
        if not fields:
            shutil.copyfile(source, destination)
            continue
        parquet = pq.read_table(source)
        rows, affected, reduction = transform(table, parquet.to_pylist(), decision["canonical_root"])
        if not affected:
            shutil.copyfile(source, destination)
            continue
        decision["all_source_rows_affected"][table["name"]] = affected
        decision["table_changes"].append({"table": table["name"], "affected_source_rows": len(affected),
                                           "previous_rows": table["row_count"], "current_rows": len(rows), "row_reduction": reduction})
        rewritten = pa.Table.from_pylist(rows, schema=parquet.schema)
        pq.write_table(rewritten, destination, compression="zstd", row_group_size=8192)
        observed = pq.read_table(destination).to_pylist()
        if Counter(map(row_digest, rows)) != Counter(map(row_digest, observed)):
            raise ValueError("rewritten Parquet values differ")
        digest = hashlib.sha256()
        digest_rows(digest, observed)
        table.update({"row_count": len(rows), "sha256": sha(destination), "bytes": destination.stat().st_size,
                      "typed_rows_sha256": digest.hexdigest(),
                      "null_counts": {c: sum(r[c] is None for r in rows) for c in parquet.column_names}})
    inclusion = json.loads((bundle / "inclusion.json").read_text())
    if not ALIASES <= set(inclusion["hearings"]["primary"]) or CANONICAL in inclusion["hearings"]["primary"]:
        raise ValueError("reviewed roots differ from primary inclusion ledger")
    inclusion["hearings"]["primary"] = sorted((set(inclusion["hearings"]["primary"]) - ALIASES) | {CANONICAL})
    inclusion["hearings"]["dependency_only"] = sorted(set(inclusion["hearings"]["dependency_only"]) - ALIASES)
    for filename, data in (("inclusion.json", inclusion), ("identity-decisions.json", decision)):
        with (output / filename).open("x") as stream:
            json.dump(data, stream, indent=2, default=str)
            stream.write("\n")
    manifest.update({"created_at": datetime.now(timezone.utc).isoformat(),
        "previous_candidate_manifest_sha256": sha(bundle / "manifest.json"),
        "inclusion_sha256": sha(output / "inclusion.json"),
        "identity_decisions_file": "identity-decisions.json", "identity_decisions_sha256": sha(output / "identity-decisions.json"),
        "joint_hearing_roots_consolidated": 2, "joint_hearing_canonical_id": CANONICAL,
        "root_counts": {family: {reason: len(ids) for reason, ids in record.items()} for family, record in inclusion.items()},
        "release_ready": False})
    # Recheck every declared FK using the new complete bundle before publishing
    # the manifest. No assumption that only the obvious child tables link here.
    values = {}
    inventory = {t["name"]: t for t in manifest["tables"]}
    for table in manifest["tables"]:
        for target in table.get("foreign_keys", {}).values():
            if target not in values:
                name, field = target.split(".")
                values[target] = set(pq.read_table(output / inventory[name]["file"], columns=[field]).column(0).to_pylist())
        if not table.get("foreign_keys"):
            continue
        for row in pq.read_table(output / table["file"], columns=list(table["foreign_keys"])).to_pylist():
            for field, target in table["foreign_keys"].items():
                if row[field] is not None and row[field] not in values[target]:
                    raise ValueError(f"unresolved foreign key after identity repair: {table['name']}.{field}")
    with (output / "manifest.json").open("x") as stream:
        json.dump(manifest, stream, indent=2)
        stream.write("\n")
    validate(output)
    return manifest


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    for option in ("bundle", "exceptions", "output"):
        parser.add_argument(f"--{option}", type=Path, required=True)
    args = parser.parse_args()
    manifest = repair(args.bundle, args.exceptions, args.output)
    print(json.dumps({"tables": len(manifest["tables"]), "rows": sum(t["row_count"] for t in manifest["tables"]),
                      "canonical_id": manifest["joint_hearing_canonical_id"], "release_ready": False}))
