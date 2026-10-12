"""Validate an independently filled review packet against its frozen case list.

This purposive diagnostic packet cannot estimate population precision/recall.
Empty, ambiguous, duplicate, or evidence-free reviews never complete the gate.
"""
from __future__ import annotations

import argparse
from collections import Counter
import csv
import hashlib
import json
from pathlib import Path

DECISIONS = {"linkage": {"correct_link", "incorrect_link", "correct_no_link", "missed_link", "not_applicable", "unresolved"},
             "text": {"faithful", "defective", "no_source", "no_extracted_text", "unresolved"}}
IDENTITY = ("case_id", "kind", "package_id", "input_sha256")


def read_cases(path):
    with path.open(newline="", encoding="utf-8") as stream:
        reader = csv.DictReader(stream)
        if not reader.fieldnames or len(set(reader.fieldnames)) != len(reader.fieldnames) or not set((*IDENTITY, "reviewer", "decision", "source_evidence", "notes")) <= set(reader.fieldnames):
            raise ValueError("invalid review header")
        rows = list(reader)
    result = {}
    for row in rows:
        if None in row or any(value is None for value in row.values()):
            raise ValueError("malformed review row")
        if not row["case_id"] or row["case_id"] in result:
            raise ValueError("missing or duplicate review case")
        result[row["case_id"]] = row
    if not result:
        raise ValueError("empty review packet")
    return result


def evaluate(audit_dir: Path, review_path: Path, *, independence_attested: bool = False):
    report = json.loads((audit_dir / "report.json").read_text())
    for name in ("review.csv", "evidence.json", "traces.json"):
        path = audit_dir / name
        if path.is_symlink() or not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != report["files"].get(name):
            raise ValueError(f"frozen audit artifact differs: {name}")
    expected = read_cases(audit_dir / "review.csv")
    reviews = read_cases(review_path)
    if expected.keys() != reviews.keys():
        raise ValueError("review case inventory differs from frozen sample")
    counts = {kind: Counter() for kind in DECISIONS}
    pending = []
    for case_id, row in reviews.items():
        if any(row[field] != expected[case_id][field] for field in IDENTITY):
            raise ValueError("review case identity or input binding differs")
        if row["input_sha256"] != report["input_sha256"] or row["kind"] not in DECISIONS:
            raise ValueError("invalid review input or kind")
        decision = row["decision"].strip()
        if decision and decision not in DECISIONS[row["kind"]]:
            raise ValueError(f"unsupported {row['kind']} review decision")
        completed = decision and decision != "unresolved" and row["reviewer"].strip() and row["source_evidence"].strip()
        if not completed:
            pending.append(case_id)
        counts[row["kind"]][decision or "unreviewed"] += 1
    return {"schema_version": 1, "input_sha256": report["input_sha256"], "case_count": len(reviews),
            "completed_case_count": len(reviews) - len(pending), "pending_case_ids": sorted(pending),
            "review_decisions": {kind: dict(count) for kind, count in counts.items()},
            "independence_attested": independence_attested,
            "review_packet_complete": not pending and independence_attested,
            "research_release_ready": False, "population_precision": None, "population_recall": None,
            "sampling_method": report.get("selection_method", report.get("selection", {}).get("method", "diagnostic sample")),
            "limitations": ["raw review counts are not weighted population estimates; use the packet's declared sample scope and design",
                            "independence is a reviewer attestation, not established by software",
                            "packet completion alone does not accept a research release"],
            "review_file_sha256": hashlib.sha256(review_path.read_bytes()).hexdigest()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--audit-dir", type=Path, required=True)
    parser.add_argument("--reviews", type=Path, required=True)
    parser.add_argument("--independence-attested", action="store_true")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    audit_dir = args.audit_dir.resolve()
    output = args.output.resolve()
    if output == audit_dir or audit_dir in output.parents:
        raise ValueError("review report must not mutate the frozen audit directory")
    result = evaluate(audit_dir, args.reviews, independence_attested=args.independence_attested)
    with output.open("x", encoding="utf-8") as stream:
        json.dump(result, stream, indent=2, sort_keys=True)
        stream.write("\n")
    print(json.dumps({key: result[key] for key in ("case_count", "completed_case_count", "review_packet_complete", "research_release_ready")}))


if __name__ == "__main__":
    main()
