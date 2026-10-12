"""Capture pre-snapshot source timestamps omitted by two staging projections.

The query is read-only and bounded to the frozen candidate's source snapshot.
Only the source version IDs, hashes, identity fields and extracted timestamps
are written; raw payload bodies remain in the ingestion database.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime, timezone
import json
from pathlib import Path

import psycopg
import pyarrow.parquet as pq

from bicam_collection.config import Settings
from bicam_collection.export_semantics import parse_source_timestamp
from export_bills_parquet import sha
from load_family_parquet import validate
from project_hearing_export_children import assert_no_credentials


def choose_latest(rows):
    grouped = defaultdict(list)
    for row in rows:
        grouped[row["raw_id"]].append(row)
    return [max(group, key=lambda row: row["first_seen_at"]) for group in grouped.values()]


def source_history(record):
    committee = record["response"].get("committee") or {}
    return committee.get("systemCode"), committee.get("history") or []


def capture(bundle: Path, output: Path):
    manifest = validate(bundle)
    snapshot = manifest["database_snapshot_at"]
    committees = pq.read_table(bundle / next(t["file"] for t in manifest["tables"] if t["name"] == "committees_history")).to_pylist()
    congresses = pq.read_table(bundle / next(t["file"] for t in manifest["tables"] if t["name"] == "congresses")).to_pylist()
    settings = Settings.from_env(Path(__file__).resolve().parents[1])
    with psycopg.connect(settings.database_url, options=(
        "-c default_transaction_read_only=on -c statement_timeout=15000 "
        "-c lock_timeout=1000 -c max_parallel_workers_per_gather=0"
    )) as connection:
        rows = connection.execute("""
            select v.id::text, v.raw_response_id::text, v.response_hash,
                   v.first_seen_at, v.response, r.endpoint_url, r.node_type
            from raw_response_versions v
            join raw_responses r on r.id = v.raw_response_id
            where r.source = 'congress'
              and r.node_type in ('congress_detail', 'committee_detail')
              and v.first_seen_at <= %s
            order by v.first_seen_at
        """, (snapshot,)).fetchall()
    observations = []
    for version_id, raw_id, response_hash, first_seen, response, endpoint_url, node_type in rows:
        assert_no_credentials(response)
        observations.append({"version_id": version_id, "raw_id": raw_id, "response_hash": response_hash,
                              "first_seen_at": first_seen, "response": response,
                              "endpoint_url": endpoint_url, "node_type": node_type})
    selected = choose_latest(observations)
    by_congress = {}
    by_committee = {}
    for record in selected:
        if record["node_type"] == "congress_detail":
            source = record["response"].get("congress") or {}
            number = source.get("number")
            if number is None:
                continue
            if int(number) in by_congress:
                raise ValueError("duplicate selected Congress source identity")
            by_congress[int(number)] = record
        else:
            code, history = source_history(record)
            if not code:
                continue
            if code in by_committee:
                raise ValueError("duplicate selected committee source identity")
            by_committee[code] = (record, history)
    congress_records = []
    for row in congresses:
        source = (by_congress.get(int(row["congress_number"])) or {}).get("response", {}).get("congress", {})
        if not source:
            raise ValueError(f"missing pre-snapshot Congress source: {row['congress_number']}")
        for field, source_field in (("name", "name"), ("start_year", "startYear"), ("end_year", "endYear")):
            if str(source.get(source_field)) != str(row[field]):
                raise ValueError(f"Congress identity mismatch for {row['congress_number']} {field}")
        timestamp = parse_source_timestamp(source.get("updateDate"))
        if timestamp is None:
            raise ValueError(f"missing Congress updateDate for {row['congress_number']}")
        record = by_congress[int(row["congress_number"])]
        congress_records.append({"congress_number": int(row["congress_number"]), "updated_at": timestamp.isoformat(),
                                 "source_path": "$.congress.updateDate", "source_version_id": record["version_id"],
                                 "raw_response_id": record["raw_id"], "raw_response_hash": record["response_hash"],
                                 "first_seen_at": record["first_seen_at"].isoformat()})
    history_records = []
    unresolved_history = []
    for row in committees:
        selected_record = by_committee.get(row["committee_code"])
        candidates = [] if selected_record is None else [item for item in selected_record[1]
                                                          if str(item.get("startDate", ""))[:10] == str(row["started_at"])]
        timestamps = {parse_source_timestamp(item.get("updateDate")) for item in candidates
                      if parse_source_timestamp(item.get("updateDate")) is not None}
        if len(timestamps) > 1:
            raise ValueError(f"contradictory history updateDate for {row['committee_code']} {row['started_at']}")
        if len(timestamps) == 0:
            unresolved_history.append({"committee_code": row["committee_code"], "started_at": str(row["started_at"]),
                                       "reason": "no_unique_pre_snapshot_history_updateDate"})
            continue
        timestamp = next(iter(timestamps))
        record = selected_record[0]
        history_records.append({"committee_code": row["committee_code"], "started_at": str(row["started_at"]),
                                "updated_at": timestamp.isoformat(), "source_path": "$.committee.history[*].updateDate",
                                "source_version_id": record["version_id"], "raw_response_id": record["raw_id"],
                                "raw_response_hash": record["response_hash"], "first_seen_at": record["first_seen_at"].isoformat()})
    result = {"schema_version": 1, "captured_at": datetime.now(timezone.utc).isoformat(),
              "candidate_manifest_sha256": sha(bundle / "manifest.json"), "database_snapshot_at": snapshot,
              "source_scope": "Congress detail and committee detail retained raw versions first seen on or before frozen snapshot; latest pre-snapshot version per raw endpoint selected.",
              "congress_records": sorted(congress_records, key=lambda r: r["congress_number"]),
              "committee_history_records": sorted(history_records, key=lambda r: (r["committee_code"], r["started_at"])),
              "unresolved_committee_history": unresolved_history, "production_modified": False,
              "applied_to_export": False,
              "limits": ["Source timestamps are supplemental evidence for fields omitted by staging projections.",
                         "No current/post-snapshot payload is used; only pre-snapshot versions are eligible."]}
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("x") as stream:
        json.dump(result, stream, indent=2, default=str)
        stream.write("\n")
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps({k: len(v) for k, v in capture(args.bundle, args.output).items()
                      if k.endswith("records") or k.startswith("unresolved")}))
