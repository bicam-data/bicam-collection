"""Capture source-backed missing hearing metadata children; read-only database."""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime, timezone
import json
from pathlib import Path
import re

import psycopg
import pyarrow.parquet as pq

from bicam_collection.config import Settings
from capture_hearing_export_parents import choose_observed_version, project_parent, sha


def metadata_row(hearing_id, package):
    project_parent(hearing_id, package)  # Validate exact package/Congress/date identity.
    other = package.get("otherIdentifier") or {}
    return {"hearing_id": hearing_id, "govinfo_package_id": package["packageId"],
            "su_doc_class_number": package.get("suDocClassNumber") or None,
            "migrated_doc_id": other.get("migrated-doc-id") or package.get("migratedDocId") or None,
            "govinfo_collection_code": package.get("collectionCode") or None}


def capture(candidate):
    manifest = json.loads((candidate / "manifest.json").read_text())
    records = {t["name"]: t for t in manifest["tables"]}
    rows = {}
    for name in ("hearings", "hearings_metadata", "hearings_metadata_granules"):
        entry = records[name]
        path = candidate / entry["file"]
        if path.parent.resolve() != candidate.resolve() or sha(path) != entry["sha256"]:
            raise ValueError("candidate file changed")
        rows[name] = pq.read_table(path, columns=["hearing_id"]).column(0).to_pylist()
    missing = {name: set(rows["hearings"]) - set(ids) for name, ids in rows.items() if name != "hearings"}
    ids = set.union(*missing.values())
    if len(ids) > 100:
        raise ValueError("bounded hearing scope exceeded")
    targets = {}
    for identity in ids:
        match = re.fullmatch(r"([hsj]hrg)([0-9]+)-([0-9]+)", identity)
        if not match:
            raise ValueError("unreviewed hearing identity")
        package = f"CHRG-{match[3]}{match[1]}{match[2]}"
        targets[f"https://api.govinfo.gov/packages/{package}/summary"] = (identity, "hearings_metadata", package)
        targets[f"https://api.govinfo.gov/packages/{package}/granules/{package}/summary"] = (identity, "hearings_metadata_granules", package)
    settings = Settings.from_env(Path(__file__).resolve().parents[1])
    with psycopg.connect(settings.database_url, options=(
        "-c default_transaction_read_only=on -c statement_timeout=8000 "
        "-c lock_timeout=1000 -c max_parallel_workers_per_gather=0")) as connection:
        raw = connection.execute("""
            select current.endpoint_url, version.id::text, version.response_hash,
                   version.first_seen_at, version.response,
                   count(*) over (partition by version.raw_response_id),
                   observed.id, observed.fetched_at, observed.outcome
            from raw_responses current
            join raw_response_versions version on version.raw_response_id = current.id
            left join lateral (
                select event.id, event.fetched_at, event.outcome
                from crawl_run_fetches event
                where event.raw_response_version_id = version.id and event.fetched_at <= %s
                order by event.fetched_at desc, event.id desc limit 1
            ) observed on true
            where current.endpoint_url = any(%s)
              and current.source = 'govinfo' and current.category = 'govinfo-hearings'
              and current.node_type in ('govinfo_hearing_package_detail', 'govinfo_hearing_granule_detail')
              and version.first_seen_at <= %s
            limit 1000
        """, (manifest["database_snapshot_at"], list(targets), manifest["database_snapshot_at"])).fetchall()
    if len(raw) >= 1000:
        raise ValueError("bounded source inventory exceeded")
    grouped = defaultdict(list)
    for row in raw:
        grouped[row[0]].append(row)
    additions = defaultdict(list)
    unmatched = []
    for url, (identity, relation, expected_package) in targets.items():
        if identity not in missing[relation]:
            continue
        if url not in grouped:
            unmatched.append({"hearing_id": identity, "table": relation, "official_url": url,
                              "status": "no_pre_snapshot_raw_at_exact_url_not_proof_of_source_absence"})
            continue
        selected = choose_observed_version(grouped[url])
        _, version_id, response_hash, first_seen, source, count, event_id, observed_at, outcome = selected
        if source.get("packageId") != expected_package:
            raise ValueError("package identity contradiction")
        if relation == "hearings_metadata":
            row = metadata_row(identity, source)
        else:
            if source.get("granuleId") != expected_package:
                raise ValueError("granule identity contradiction")
            row = {"hearing_id": identity, "govinfo_package_id": expected_package,
                   "govinfo_granule_id": source["granuleId"]}
        additions[relation].append({"row": row, "evidence": {
            "official_url": url, "raw_response_version_id": version_id,
            "raw_response_hash": response_hash, "first_observed_at": first_seen.isoformat(),
            "pre_snapshot_version_count": count, "selected_fetch_event_id": str(event_id) if event_id else None,
            "selected_fetch_observed_at": observed_at.isoformat() if observed_at else None,
            "selected_fetch_outcome": outcome}})
    return {"schema_version": 1, "captured_at": datetime.now(timezone.utc).isoformat(),
            "candidate_manifest_sha256": sha(candidate / "manifest.json"),
            "source_manifest_sha256": manifest["source_manifest_sha256"],
            "database_snapshot_at": manifest["database_snapshot_at"], "additions": dict(additions),
            "unresolved": unmatched, "production_modified": False, "applied": False,
            "limits": ["Only absent child records are proposed; no existing row is overwritten.",
                       "Exact canonical package/granule URLs checked; other aliases or granules need separate inventory evidence."]}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("output exists; preserve prior evidence")
    result = capture(args.candidate)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x") as stream:
        json.dump(result, stream, indent=2)
        stream.write("\n")
    print(json.dumps({"additions": {k: len(v) for k, v in result["additions"].items()}, "unresolved": len(result["unresolved"])}))
