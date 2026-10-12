"""Audit source-advertised child counts without treating them as row contracts.

Congress.gov detail payloads expose fields such as ``subjects.count`` and
``actions_count``.  Those values are useful source evidence, but they are not
always the cardinality of the returned child array (for example a payload may
advertise one subject while returning an empty ``legislativeSubjects`` array).
This offline audit makes that distinction explicit for a 119th-Congress
Parquet candidate and never changes the candidate.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path

import pyarrow.parquet as pq

from load_family_parquet import validate


# Only mappings represented by a public child relation are included.  Other
# root count fields (for example bills.amendments_count) describe endpoints
# whose public BICAM grain is represented on the opposite side or not exposed
# as a standalone child table.
COUNT_RELATIONS = {
    ("bills", "actions_count"): ("bills_actions", "bill_id"),
    ("bills", "cosponsors_count"): ("bills_cosponsors", "bill_id"),
    ("bills", "summaries_count"): ("bills_summaries", "bill_id"),
    ("bills", "subjects_count"): ("bills_subjects", "bill_id"),
    ("bills", "titles_count"): ("bills_titles", "bill_id"),
    ("bills", "texts_count"): ("bills_texts", "bill_id"),
    ("amendments", "actions_count"): ("amendments_actions", "amendment_id"),
    ("amendments", "cosponsors_count"): ("amendments_cosponsors", "amendment_id"),
    ("amendments", "amendments_to_amendment_count"):
        ("amendments_amended_amendments", "amendment_id"),
    ("committee_reports", "texts_count"): ("committee_reports_texts", "report_id"),
    ("nominations", "actions_count"): ("nominations_actions", "nomination_id"),
}


def sha(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def _rows(bundle: Path, name: str, columns: list[str] | None = None) -> list[dict]:
    return pq.read_table(bundle / f"{name}.parquet", columns=columns).to_pylist()


def _summary(roots: list[dict], children: list[dict], count_field: str, owner: str, congress: int) -> dict:
    child_counts = Counter(row.get(owner) for row in children)
    exact = under = over = zero_positive = 0
    advertised_total = observed_total = 0
    examples: list[dict] = []
    for root in roots:
        if root.get("congress") != congress:
            continue
        advertised = root.get(count_field)
        if advertised is None:
            continue
        advertised = int(advertised)
        observed = child_counts[root.get("bill_id") or root.get("amendment_id")
                                or root.get("committee_code") or root.get("report_id")
                                or root.get("nomination_id") or root.get("treaty_id")]
        advertised_total += advertised
        observed_total += observed
        diff = observed - advertised
        if diff == 0:
            exact += 1
        elif diff < 0:
            under += 1
            if observed == 0 and advertised > 0:
                zero_positive += 1
        else:
            over += 1
        if diff and len(examples) < 12:
            identity = next((root.get(key) for key in (
                "bill_id", "amendment_id", "committee_code", "report_id",
                "nomination_id", "treaty_id") if root.get(key) is not None), None)
            examples.append({"id": identity, "advertised": advertised, "observed": observed})
    return {
        "root_rows_with_count": exact + under + over,
        "advertised_total": advertised_total,
        "observed_total": observed_total,
        "exact_rows": exact,
        "under_rows": under,
        "over_rows": over,
        "zero_observed_positive_advertised": zero_positive,
        "examples": examples,
    }


def audit(bundle: Path, output: Path, congress: int = 119) -> dict:
    manifest = validate(bundle)
    inventory = {entry["name"]: entry for entry in manifest["tables"]}
    reports = []
    for (root, count_field), (child, owner) in COUNT_RELATIONS.items():
        if root not in inventory or child not in inventory:
            continue
        root_columns = ["congress", count_field, "bill_id", "amendment_id", "committee_code",
                        "report_id", "nomination_id", "treaty_id"]
        root_schema = pq.read_schema(bundle / inventory[root]["file"])
        root_columns = [column for column in root_columns if column in root_schema.names]
        child_schema = pq.read_schema(bundle / inventory[child]["file"])
        if owner not in child_schema.names:
            continue
        summary = _summary(_rows(bundle, root, root_columns), _rows(bundle, child, [owner]),
                           count_field, owner, congress)
        reports.append({"root": root, "count_field": count_field, "child": child,
                        "owner": owner, **summary})
    result = {
        "schema_version": 1,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "candidate_manifest_sha256": sha(bundle / "manifest.json"),
        "congress": congress,
        "relations_checked": len(reports),
        "relations": reports,
        "production_modified": False,
        "interpretation": [
            "Advertised counts are source evidence, not a public relational cardinality contract.",
            "Under-counts with an empty child table can reflect an upstream payload whose count is nonzero while its returned array is empty.",
            "True fetch completeness is evaluated separately by the child-coverage ledger and source-error classification.",
        ],
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("x", encoding="utf-8") as stream:
        json.dump(result, stream, indent=2)
        stream.write("\n")
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--congress", type=int, default=119)
    args = parser.parse_args()
    report = audit(args.bundle, args.output, args.congress)
    print(json.dumps({"relations_checked": report["relations_checked"],
                      "candidate_manifest_sha256": report["candidate_manifest_sha256"]}))
