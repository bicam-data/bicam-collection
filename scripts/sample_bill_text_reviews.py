"""Prepare a stratified, occurrence-level review sample from a frozen export.

Includes text rows with no generated version link, so review can investigate
candidate-generation misses. No labels or accuracy estimates are invented.
"""
from __future__ import annotations

from collections import Counter
import argparse
import csv
import gzip
import hashlib
import importlib.util
import json
import os
import re
from pathlib import Path
import sqlite3
import sys
import tempfile

SPEC = importlib.util.spec_from_file_location("_sample_csv", Path(__file__).with_name("evaluate_research_snapshot.py"))
CSV_MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CSV_MODULE)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def source_scope(row):
    """Scope describes eligibility for BILLS matching, not product inclusion."""
    urls = ' '.join(row.get(field) or '' for field in ('formatted_text', 'pdf', 'xml'))
    if '/plaws/' in urls.lower() or 'plaw-' in urls.lower() or (row.get('type') or '').lower() in {'public law', 'private law'}:
        return 'law_rendition'
    if int(row['bill_id'].rsplit('-', 1)[1]) < 103:
        return 'pre_103'
    if re.search(r'bills-\d+', urls, re.I):
        return 'bills_scope'
    return 'scope_unverified'


def text_presence(row):
    value = (row.get('raw_text') or '').strip()
    return 'prose_present' if value and not re.fullmatch(r'https?://\S+', value) else 'prose_absent'


def stratum(row, status, text_quality_strata=False):
    congress = int(row["bill_id"].rsplit("-", 1)[1])
    era = "through_106" if congress <= 106 else "107_113" if congress <= 113 else "114_onward"
    base = f"{era}/{status}"
    return f"{base}/{source_scope(row)}/{text_presence(row)}" if text_quality_strata else base


def select_lowest(selected, bucket, priority, row, count):
    retained = selected.setdefault(bucket, [])
    retained.append((priority, row))
    retained.sort(key=lambda item: item[0])
    del retained[count:]


def sample(manifest_path, output, per_stratum=3, *, text_quality_strata=False, min_nonempty=0):
    if not 1 <= per_stratum <= 20:
        raise ValueError("per-stratum must be between 1 and 20")
    if not 0 <= min_nonempty <= 500:
        raise ValueError('min-nonempty must be between 0 and 500')
    manifest = json.loads(manifest_path.read_text())
    manifest_hash = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    serialization = manifest.get("serialization")
    marker = "" if serialization is None else serialization.get("csv_null")
    if serialization is not None and (not isinstance(marker, str) or any(serialization.get(k) != v for k, v in {"format": "csv.gz", "header": True, "encoding": "UTF-8", "newline": "LF"}.items())):
        raise ValueError("unsupported export serialization")
    entries = {}
    for table in ("bills_texts", "bills_version_links"):
        found = [row for row in manifest["tables"] if row["name"] == table]
        if len(found) != 1 or found[0]["file"] != f"{table}.csv.gz":
            raise ValueError("missing or duplicate review source table")
        entry = found[0]
        path = manifest_path.parent / entry["file"]
        if path.is_symlink() or CSV_MODULE._hash_file(path) != (entry["bytes"], entry["sha256"]):
            raise ValueError("review source file differs from manifest")
        entries[table] = entry
    output.mkdir(parents=True, exist_ok=False, mode=0o700)
    csv.field_size_limit(sys.maxsize)
    def rows(table):
        observed = 0
        with gzip.open(manifest_path.parent / entries[table]["file"], "rt", encoding="utf-8", newline="") as stream:
            reader = CSV_MODULE._csv_rows(stream, marker)
            header = next(reader)
            for observed, values in enumerate(reader, 1):
                yield observed, dict(zip(header, values, strict=True))
        if observed != entries[table]["row_count"]:
            raise ValueError("review source row count differs from manifest")
    selected = {}
    counts = Counter()
    presence_counts = Counter()
    scope_counts = Counter()
    with tempfile.TemporaryDirectory(prefix="bicam-review-index-") as temporary:
        with sqlite3.connect(Path(temporary) / "links.sqlite") as db:
            db.execute("pragma cache_size=-8192")
            db.execute("create table links (row_key text primary key, link_json text not null)")
            for _, row in rows("bills_version_links"):
                if row["source_table"] == "bills_texts":
                    db.execute("insert into links values (?, ?)", (row["source_row_key"], json.dumps(row)))
            db.commit()
            for ordinal, row in rows("bills_texts"):
                row_key = "|".join(row.get(field) or "" for field in ("bill_id", "date", "type"))
                hit = db.execute("select link_json from links where row_key=?", (row_key,)).fetchone()
                link = json.loads(hit[0]) if hit else None
                status = link["link_status"] if link else "no_candidate"
                bucket = stratum(row, status, text_quality_strata)
                counts[bucket] += 1
                presence_counts[text_presence(row)] += 1
                scope_counts[source_scope(row)] += 1
                identity = digest([manifest_hash, "bills_texts", ordinal])
                case = {"occurrence_id": identity, "csv_record_ordinal": ordinal, "stratum": bucket,
                        "public_row": row, "recorded_link": link,
                        "source_scope": source_scope(row), "text_presence": text_presence(row)}
                select_lowest(selected, bucket, identity, case, per_stratum)
    cases = [row for bucket in sorted(selected) for _, row in selected[bucket]]
    present_sample = sum(case['text_presence'] == 'prose_present' for case in cases)
    if present_sample < min_nonempty:
        raise ValueError(f'only {present_sample} prose samples; require {min_nonempty}; increase per-stratum or await a newer candidate')
    for case in cases:
        population = counts[case["stratum"]]
        case["stratum_population"] = population
        case["inclusion_probability"] = len(selected[case["stratum"]]) / population
    selection = 'lowest deterministic hash ranks within era/link-status' + ('/source-scope/text-presence' if text_quality_strata else '') + ' strata'
    evidence = {"manifest_sha256": manifest_hash, "selection": selection,
                "unit": "exported bills_texts occurrence; grouping keys need not be unique",
                "cases": cases}
    for name, value in (("evidence.json", evidence), ("traces.json", {"scope": "export occurrence locators, not raw-source lineage", "manifest_sha256": manifest_hash})):
        (output / name).write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    with (output / "review.csv").open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=["case_id", "kind", "package_id", "input_sha256", "reviewer", "decision", "source_evidence", "notes"])
        writer.writeheader()
        for case in cases:
            for kind in ("linkage", "text"):
                writer.writerow({"case_id": digest([case["occurrence_id"], kind]), "kind": kind,
                                 "package_id": case["recorded_link"].get("govinfo_package_id") if case["recorded_link"] else "",
                                 "input_sha256": manifest_hash, "notes": f"occurrence={case['occurrence_id']} stratum={case['stratum']}"})
    report = {"schema_version": 1, "input_sha256": manifest_hash, "source_manifest": str(manifest_path.resolve()),
              "source_tables": entries, "selection_method": evidence["selection"], "stratum_population": dict(sorted(counts.items())),
              "stratum_sample": {bucket: len(values) for bucket, values in sorted(selected.items())},
              "sampled_occurrences": len(cases), "review_cases": len(cases) * 2,
              "source_generated_at": manifest.get('generated_at'),
              "text_presence_population": dict(presence_counts), "source_scope_population": dict(scope_counts),
              "prose_sample_count": present_sample, "minimum_prose_samples": min_nonempty,
              "text_quality_strata": text_quality_strata,
              "independent_review_complete": False, "research_release_ready": False,
              "limitations": ["only bills_texts occurrences, not all entities or all possible links",
                              "no_candidate is absence of a recorded link, not proof of a missing true match",
                              "shared source_row_key may attach a group-level link to multiple renditions",
                              "precision/recall and extraction-quality estimates require independently completed labels and appropriate weights"]}
    report["files"] = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in output.iterdir() if p.is_file()}
    (output / "report.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--per-stratum", type=int, default=3)
    parser.add_argument('--text-quality-strata', action='store_true', help='Separate prose presence and law/pre-103 eligibility from link status.')
    parser.add_argument('--min-nonempty', type=int, default=0, help='Fail unless at least this many sampled occurrences contain prose.')
    args = parser.parse_args()
    os.umask(0o077)
    report = sample(args.manifest.resolve(), args.output_dir.resolve(), args.per_stratum,
                    text_quality_strata=args.text_quality_strata, min_nonempty=args.min_nonempty)
    print(json.dumps({k: report[k] for k in ("stratum_population", "stratum_sample", "sampled_occurrences", "review_cases")}, indent=2))


if __name__ == "__main__":
    main()
