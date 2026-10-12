"""Audit a family Parquet candidate against the original BICAM contract.

This is an offline completeness/semantic audit. It does not decide that a
source-null field is wrong, and it does not modify the candidate. It separates
reference-archive availability, candidate availability, intentional raw-text
exclusion and newly observed Congress-119 values.
"""
from __future__ import annotations

import argparse
import csv
from collections import Counter
from datetime import datetime, timezone
import hashlib
import io
import json
from pathlib import Path
import sys
import zipfile

import pyarrow.parquet as pq
import yaml

from load_family_parquet import validate
from bicam_collection.export_semantics import semantic_pattern_valid

SEMANTIC_FIELDS = {
    "chamber", "origin_chamber", "current_chamber", "member_type", "bill_type",
    "amendment_type", "report_type", "committee_type", "entity_type", "calendar",
    "action_type", "relationship_type", "source_system", "source_system_code",
    "is_subcommittee", "is_current", "is_errata", "is_private", "is_law",
}
NULL_MARKERS = {"", r"\N"}
csv.field_size_limit(sys.maxsize)


def sha(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def field_stats(rows, fields):
    nulls = Counter()
    empties = Counter()
    domains = {field: Counter() for field in fields if field in SEMANTIC_FIELDS}
    count = 0
    for row in rows:
        count += 1
        for field in fields:
            value = row.get(field)
            if value is None:
                nulls[field] += 1
            elif value == "":
                empties[field] += 1
            if field in domains and value not in (None, ""):
                domains[field][str(value)] += 1
    return {"rows": count, "null_counts": dict(nulls), "empty_counts": dict(empties),
            "semantic_domains": {field: dict(sorted(values.items())) for field, values in domains.items()}}


def archive_rows(archive: zipfile.ZipFile, source_file: str):
    names = [name for name in archive.namelist() if Path(name).name == source_file]
    if len(names) != 1:
        raise ValueError(f"expected one {source_file} in archive, found {len(names)}")
    with archive.open(names[0]) as stream:
        reader = csv.DictReader(io.TextIOWrapper(stream, encoding="utf-8-sig", newline=""))
        fields = reader.fieldnames or []
        yield fields, None
        for row in reader:
            # Published CSV nulls are empty cells; preserve quoted text as text.
            yield fields, row


def semantic_shape(domains):
    return {value: {"case": "lower" if value.islower() else "upper" if value.isupper() else "mixed",
                    "rows": rows} for value, rows in domains.items()}


def semantic_pattern_audit(domains):
    return {field: {"invalid_values": sorted(value for value in values
                                               if not semantic_pattern_valid(field, value)),
                     "pattern_checked": field in {
                         "chamber", "origin_chamber", "current_chamber", "bill_type",
                         "amendment_type", "report_type", "source_system_code", "calendar",
                         "is_subcommittee", "is_current", "is_errata", "is_private", "is_law"}}
            for field, values in domains.items()}


def audit(bundle: Path, contract_path: Path, archive_dir: Path, output: Path | None = None):
    manifest = validate(bundle)
    contract = yaml.safe_load(contract_path.read_text())
    inventory = {table["name"]: table for table in manifest["tables"]}
    reports = []
    for family in contract["families"]:
        relations = [{"table": family["table"], **family}, *family.get("children", [])]
        archive_path = archive_dir / family["source_archive"]
        with zipfile.ZipFile(archive_path) as archive:
            for relation in relations:
                if relation.get("feature_status") != "core-now":
                    continue
                table_name = relation["table"]
                entry = inventory[table_name]
                path = bundle / entry["file"]
                columns = list(relation["columns"])
                if pq.read_schema(path).names != columns:
                    raise ValueError(f"candidate columns differ from contract: {table_name}")
                candidate = pq.read_table(path).to_pylist()
                candidate_stats = field_stats(candidate, columns)
                # Consume the one header marker before validating it. The row
                # iterator remains streaming for very large archives.
                source_iter = archive_rows(archive, relation["source_file"])
                source_fields, first_row = next(source_iter)
                if first_row is not None or source_fields != columns:
                    raise ValueError(f"archive columns differ from contract: {table_name}")

                def remaining_rows():
                    for _, row in source_iter:
                        if row is not None:
                            yield row

                source_stats = field_stats(remaining_rows(), columns)
                availability = {}
                for field in columns:
                    source_missing = source_stats["null_counts"].get(field, 0) + source_stats["empty_counts"].get(field, 0)
                    candidate_missing = candidate_stats["null_counts"].get(field, 0) + candidate_stats["empty_counts"].get(field, 0)
                    source_all = not source_stats["rows"] or source_missing == source_stats["rows"]
                    candidate_all = not candidate_stats["rows"] or candidate_missing == candidate_stats["rows"]
                    if field == "raw_text":
                        status = "excluded_raw_text_by_contract"
                    elif source_all and candidate_all:
                        status = "unavailable_in_reference_and_candidate"
                    elif source_all and not candidate_all:
                        status = "candidate_new_source_values"
                    elif not source_all and candidate_all:
                        status = "candidate_field_gap"
                    else:
                        status = "available"
                    availability[field] = {"status": status, "reference_nonempty": source_stats["rows"] - source_missing,
                                           "candidate_nonempty": candidate_stats["rows"] - candidate_missing}
                source_domains = source_stats["semantic_domains"]
                candidate_domains = candidate_stats["semantic_domains"]
                reports.append({"table": table_name, "source_archive": archive_path.name,
                                "source_sha256": sha(archive_path), "candidate_sha256": entry["sha256"],
                                "contract_columns": columns, "source": source_stats,
                                "candidate": candidate_stats, "availability": availability,
                                "semantic_shapes": {"reference": {k: semantic_shape(v) for k, v in source_domains.items()},
                                                     "candidate": {k: semantic_shape(v) for k, v in candidate_domains.items()},
                                                     "candidate_values_not_in_reference": {
                                                         k: sorted(set(candidate_domains.get(k, {})) - set(source_domains.get(k, {})))
                                                         for k in candidate_domains}},
                                "semantic_pattern_audit": {"reference": semantic_pattern_audit(source_domains),
                                                             "candidate": semantic_pattern_audit(candidate_domains)},
                                "raw_text_excluded": "raw_text" in columns})
                print(json.dumps({"table": table_name, "candidate_rows": candidate_stats["rows"],
                                  "reference_rows": source_stats["rows"],
                                  "candidate_field_gaps": sum(v["status"] == "candidate_field_gap" for v in availability.values()),
                                  "new_semantic_values": sum(len(v) for v in reports[-1]["semantic_shapes"]["candidate_values_not_in_reference"].values())}), flush=True)
    pattern_failures = [
        {"table": report["table"], "side": side, "field": field, "invalid_values": details["invalid_values"]}
        for report in reports
        for side in ("reference", "candidate")
        for field, details in report["semantic_pattern_audit"][side].items()
        if details["invalid_values"]]
    result = {"schema_version": 1, "created_at": datetime.now(timezone.utc).isoformat(),
              "candidate_manifest_sha256": sha(bundle / "manifest.json"),
              "contract_sha256": sha(contract_path), "archive_dir": str(archive_dir),
              "congress_scope": 119, "table_count": len(reports),
              "candidate_rows": sum(r["candidate"]["rows"] for r in reports), "tables": reports,
              "semantic_pattern_failures": pattern_failures,
              "candidate_semantic_pattern_failures": [item for item in pattern_failures if item["side"] == "candidate"],
              "release_accepted": False,
              "interpretation": ["Reference row counts are full published historical archives and are not expected to equal a Congress-119 candidate.",
                                  "candidate_field_gap means the reference archive had at least one value but this candidate has none; investigate source cohort before filling.",
                                  "candidate_values_not_in_reference can be a valid later-source domain (for example joint hearing); it is an audit signal, not an automatic error.",
                                  "Semantic pattern checks allow structurally valid later values; they reject invalid chambers, bill/amendment/report types, calendars, source-system codes and boolean encodings.",
                                  "raw_text is intentionally excluded from the public candidate even when the reference archive contains extracted text.",
                                  "Empty strings and SQL NULLs are counted separately in the field statistics but combined for availability."]}
    if output is not None:
        output.parent.mkdir(parents=True, exist_ok=True)
        with output.open("x") as stream:
            json.dump(result, stream, indent=2)
            stream.write("\n")
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--contract", type=Path, default=Path("contracts/bicam_public_schema.yaml"))
    parser.add_argument("--archive-dir", type=Path, default=Path("/tmp/bicam-published"))
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = audit(args.bundle, args.contract, args.archive_dir, args.output)
    print(json.dumps({k: report[k] for k in ("table_count", "candidate_rows", "release_accepted")}))
