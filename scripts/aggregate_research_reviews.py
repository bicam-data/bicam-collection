"""Aggregate independent AI assessments without attesting human independence.

Unanimous, evidenced labels are proposals, not proof. Disagreements remain
unresolved unless an explicit evidence-backed adjudication is supplied.
"""
from __future__ import annotations

import argparse
from collections import Counter
import csv
import hashlib
import json
from pathlib import Path

from evaluate_research_reviews import DECISIONS, evaluate, read_cases


DOCUMENT_REQUIRED = {"correct_link", "incorrect_link", "missed_link", "faithful", "defective"}


def validate_label(row, expected):
    if row.get("kind") != expected["kind"] or row.get("decision") not in DECISIONS[expected["kind"]]:
        raise ValueError("invalid review kind or decision")
    if not isinstance(row.get("source_document_inspected"), bool):
        raise ValueError("source_document_inspected must be boolean")
    for field in ("source_evidence", "notes"):
        if not isinstance(row.get(field), str) or not row[field].strip():
            raise ValueError(f"missing {field}")
    if row["decision"] in DOCUMENT_REQUIRED and not row["source_document_inspected"]:
        raise ValueError("substantive judgment requires document inspection")


def aggregate(audit_dir, reviewer_paths, adjudication_path=None):
    # Verify the original packet, including immutable file digests, first.
    evaluate(audit_dir, audit_dir / "review.csv")
    expected = read_cases(audit_dir / "review.csv")
    evidence = json.loads((audit_dir / "evidence.json").read_text())
    occurrences = {case["occurrence_id"] for case in evidence["cases"]}
    voters, inputs = {}, []
    for path in reviewer_paths:
        raw = path.read_bytes()
        document = json.loads(raw)
        reviewer = document.get("reviewer")
        if not isinstance(reviewer, str) or not reviewer.strip() or reviewer in voters:
            raise ValueError("missing or duplicate reviewer")
        votes = {}
        for row in document["reviews"]:
            case_id = row.get("case_id")
            if case_id not in expected or case_id in votes:
                raise ValueError("unknown or duplicate case")
            occurrence = row.get("occurrence_id")
            if occurrence not in occurrences or hashlib.sha256(
                json.dumps([occurrence, row["kind"]], sort_keys=True, ensure_ascii=False).encode()
            ).hexdigest() != case_id:
                raise ValueError("case occurrence binding differs")
            validate_label(row, expected[case_id])
            votes[case_id] = row
        if votes.keys() != expected.keys():
            raise ValueError("review inventory differs")
        voters[reviewer] = votes
        inputs.append({"path": str(path.resolve()), "sha256": hashlib.sha256(raw).hexdigest(), "reviewer": reviewer})
    if len(voters) < 2:
        raise ValueError("at least two distinct reviewers required")
    adjudications = {}
    if adjudication_path:
        raw = adjudication_path.read_bytes()
        for row in json.loads(raw)["reviews"]:
            case_id = row.get("case_id")
            if case_id not in expected or case_id in adjudications:
                raise ValueError("unknown or duplicate adjudication")
            validate_label(row, expected[case_id])
            adjudications[case_id] = row
        inputs.append({"path": str(adjudication_path.resolve()), "sha256": hashlib.sha256(raw).hexdigest(), "reviewer": "root-adjudicator"})
    output, cases = [], []
    for case_id, original in expected.items():
        votes = [{"reviewer": name, **rows[case_id]} for name, rows in voters.items()]
        counts = Counter(row["decision"] for row in votes)
        unanimous = len(counts) == 1
        decision = votes[0]["decision"] if unanimous else "unresolved"
        status = "unanimous_resolved" if unanimous and decision != "unresolved" else "unanimous_unresolved" if unanimous else "disagreement"
        selected = adjudications.get(case_id)
        if selected:
            decision = selected["decision"]
        sources = selected["source_evidence"] if selected else " | ".join(dict.fromkeys(row["source_evidence"] for row in votes))
        notes = selected["notes"] if selected else " | ".join(f"{row['reviewer']}: {row['notes']}" for row in votes)
        output.append({**original, "reviewer": "AI-assisted/root-adjudicator" if selected else "AI-assisted/consensus",
                       "decision": decision, "source_evidence": sources,
                       "notes": f"{original['notes']}; AI-assisted only; agreement={status}; {notes}"})
        cases.append({"case_id": case_id, "kind": original["kind"], "votes": votes,
                      "vote_counts": dict(counts), "agreement": status,
                      "adjudication": selected, "final_decision": decision})
    return output, {"schema_version": 1, "review_type": "AI-assisted, not independent human validation",
                    "independence_attested": False, "research_release_ready": False,
                    "policy": "Unanimous evidenced proposals; disagreements unresolved unless explicitly adjudicated. Agreement is not proof.",
                    "inputs": inputs, "reviewer_count": len(voters), "case_count": len(cases),
                    "agreement_counts": dict(Counter(case["agreement"] for case in cases)),
                    "adjudicated_count": len(adjudications), "cases": cases}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--audit-dir", type=Path, required=True)
    parser.add_argument("--reviewer", type=Path, action="append", required=True)
    parser.add_argument("--adjudications", type=Path)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    audit, output = args.audit_dir.resolve(), args.output_dir.resolve()
    if output == audit or audit in output.parents:
        raise ValueError("output must not modify the frozen packet")
    rows, report = aggregate(audit, args.reviewer, args.adjudications)
    output.mkdir(parents=True, exist_ok=False)
    with (output / "review.csv").open("x", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    evaluation = evaluate(audit, output / "review.csv", independence_attested=False)
    for name, value in (("consensus.json", report), ("evaluation.json", evaluation)):
        with (output / name).open("x", encoding="utf-8") as stream:
            json.dump(value, stream, indent=2, sort_keys=True)
            stream.write("\n")
    print(json.dumps({"agreement_counts": report["agreement_counts"], "adjudicated_count": report["adjudicated_count"],
                      "completed_case_count": evaluation["completed_case_count"], "research_release_ready": False}))


if __name__ == "__main__":
    main()
