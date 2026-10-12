"""Capture bounded, read-only Congress source-date evidence for a frozen export.

Require exactly one retained raw version observed by the snapshot for each
Congress, and verify its non-date root values against the frozen public row.
Do not substitute fetch/first-seen timestamps for source updateDate.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import csv
import gzip
import hashlib
import json
from pathlib import Path

import psycopg

from bicam_collection.config import Settings
from bicam_collection.export_semantics import classify_recency, parse_source_timestamp


def sha(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def capture(snapshot, cutoff):
    manifest_path = snapshot / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    table = next(t for t in manifest["tables"] if t["name"] == "congresses")
    path = snapshot / table["file"]
    if path.parent.resolve() != snapshot.resolve() or sha(path) != table["sha256"]:
        raise ValueError("frozen Congress file differs")
    with gzip.open(path, "rt", newline="") as stream:
        public = {int(row["congress_number"]): row for row in csv.DictReader(stream)}
    if len(public) != table["row_count"]:
        raise ValueError("frozen root key/count differs")
    settings = Settings.from_env(Path(__file__).resolve().parents[1])
    with psycopg.connect(settings.database_url, options=(
        "-c default_transaction_read_only=on -c statement_timeout=8000 "
        "-c lock_timeout=1000 -c max_parallel_workers_per_gather=0"
    )) as connection:
        rows = connection.execute("""
            select versions.id::text, versions.raw_response_id::text,
                   versions.response_hash, versions.first_seen_at,
                   versions.response->'congress',
                   count(*) over (partition by versions.raw_response_id)
            from raw_response_versions versions
            join raw_responses raw on raw.id = versions.raw_response_id
            where raw.source = 'congress' and raw.category = 'congresses'
              and raw.node_type = 'congress_detail'
              and versions.first_seen_at <= %s
            order by versions.raw_response_id
            limit 1000
        """, (manifest["database_snapshot_at"],)).fetchall()
    if len(rows) >= 1000:
        raise ValueError("bounded source inventory exceeded")
    records, seen = [], set()
    for version_id, raw_id, response_hash, first_seen, source, version_count in rows:
        if version_count != 1:
            raise ValueError("multiple pre-snapshot versions require fetch-event resolution")
        number = int(source["number"])
        if number in seen or number not in public:
            raise ValueError("source/root identity mismatch")
        seen.add(number)
        for field, source_field in (("name", "name"), ("start_year", "startYear"), ("end_year", "endYear")):
            if str(source[source_field]) != public[number][field]:
                raise ValueError(f"source does not reproduce frozen Congress {number} field {field}")
        updated = parse_source_timestamp(source.get("updateDate"))
        if updated is None:
            raise ValueError("missing official source updateDate")
        frozen_date = public[number]["updated_at"]
        if frozen_date not in ("", r"\N") and parse_source_timestamp(frozen_date) != updated:
            raise ValueError("existing public/source timestamp contradiction")
        records.append({"congress_number": number, "updated_at": updated.isoformat(),
                        "raw_response_version_id": version_id, "raw_response_id": raw_id,
                        "raw_response_hash": response_hash, "source_path": "$.congress.updateDate",
                        "first_observed_at": first_seen.isoformat(), "pre_snapshot_version_count": version_count,
                        "verified_root_fields": ["congress_number", "name", "start_year", "end_year"],
                        "recency": classify_recency({"updated_at": updated.isoformat()}, cutoff)})
    if seen != public.keys():
        raise ValueError("source evidence does not cover every frozen Congress")
    return {"schema_version": 1, "captured_at": datetime.now(timezone.utc).isoformat(),
            "snapshot": snapshot.name, "manifest_sha256": sha(manifest_path),
            "source_csv_sha256": table["sha256"], "database_snapshot_at": manifest["database_snapshot_at"],
            "source_date_field": "$.congress.updateDate", "cutoff": cutoff.isoformat(),
            "records": sorted(records, key=lambda r: r["congress_number"]),
            "evidence_scope": "Unique retained pre-snapshot source version plus matching frozen root fields; no claim of full pipeline generation replay.",
            "production_modified": False, "frozen_snapshot_modified": False,
            "export_enrichment_applied": False}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("output exists; preserve prior evidence")
    result = capture(args.snapshot, datetime(2025, 1, 1, tzinfo=timezone.utc))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x") as stream:
        json.dump(result, stream, indent=2)
        stream.write("\n")
    print(json.dumps({"verified_congresses": len(result["records"]),
                      "qualifying_congresses": [r["congress_number"] for r in result["records"] if r["recency"] == "qualifies"]}))
