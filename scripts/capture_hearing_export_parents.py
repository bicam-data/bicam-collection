"""Prepare missing hearing parents from bounded retained official package evidence.

No PostgreSQL writes or public snapshot mutations. Proposed additions are bound
to the source snapshot and unique pre-snapshot raw package versions. They must
still be applied and validated by the all-family exporter.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from collections import defaultdict
import hashlib
import json
from pathlib import Path
import re

import psycopg
import pyarrow as pa
import pyarrow.csv as acsv

from bicam_collection.config import Settings
from bicam_collection.export_semantics import parse_source_timestamp


def sha(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def read_columns(path, columns):
    with acsv.open_csv(path, read_options=acsv.ReadOptions(block_size=32 * 1024 * 1024, use_threads=False),
        parse_options=acsv.ParseOptions(newlines_in_values=True),
        convert_options=acsv.ConvertOptions(include_columns=columns,
            column_types={k: pa.string() for k in columns}, null_values=[r"\N"],
            strings_can_be_null=True, quoted_strings_can_be_null=False)) as reader:
        for batch in reader:
            yield from batch.to_pylist()


def project_parent(expected_id, package):
    match = re.fullmatch(r"CHRG-([0-9]+)([hsj]hrg)([0-9]+)(.*)", package.get("packageId", ""))
    if not match:
        raise ValueError("unsupported official package identity")
    congress, kind, jacket, part = match.groups()
    jacket = jacket.lstrip("0") or "0"
    if f"{kind}{jacket}-{congress}" != expected_id or str(package.get("congress")) != congress:
        raise ValueError("source package/Congress identity contradiction")
    updated = parse_source_timestamp(package.get("lastModified"))
    if updated is None:
        raise ValueError("package source update timestamp missing")
    title = package.get("title")
    def integer(field):
        return None if package.get(field) in (None, "") else int(package[field])
    return {"hearing_id": expected_id, "hearing_jacketnumber": jacket, "loc_id": None,
            "title": title, "congress": int(congress), "session": integer("session"),
            "chamber": {"hhrg": "house", "shrg": "senate", "jhrg": "joint"}[kind],
            # Match the existing public GovInfo projection; this is not identity evidence.
            "is_appropriation": None if title is None else "appropriation" in title.lower(),
            "hearing_number": None, "part_number": part or None, "citation": None,
            "pages": integer("pages"), "updated_at": updated.isoformat()}


def capture(snapshot):
    manifest_path = snapshot / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    tables = {t["name"]: t for t in manifest["tables"]}
    files = {}
    for name in ("hearings", "hearings_texts"):
        entry = tables[name]
        path = snapshot / entry["file"]
        if path.is_symlink() or path.parent.resolve() != snapshot.resolve() or sha(path) != entry["sha256"]:
            raise ValueError("frozen input differs")
        files[name] = path
    root_rows = list(read_columns(files["hearings"], ["hearing_id"]))
    roots = {r["hearing_id"] for r in root_rows}
    if len(roots) != len(root_rows) or len(root_rows) != tables["hearings"]["row_count"]:
        raise ValueError("root count/key mismatch")
    missing, text_count = {}, 0
    for row in read_columns(files["hearings_texts"], ["hearing_id", "pdf", "formatted_text"]):
        text_count += 1
        if row["hearing_id"] not in roots:
            if row["hearing_id"] in missing:
                raise ValueError("duplicate missing hearing text key")
            missing[row["hearing_id"]] = row
    if text_count != tables["hearings_texts"]["row_count"] or len(missing) > 100:
        raise ValueError("text count differs or bounded missing-parent scope exceeded")
    urls = {}
    for hearing_id in missing:
        match = re.fullmatch(r"([hsj]hrg)([0-9]+)-([0-9]+)", hearing_id)
        if not match:
            raise ValueError("unreviewed missing hearing ID")
        kind, jacket, congress = match.groups()
        package_id = f"CHRG-{congress}{kind}{jacket}"
        urls[f"https://api.govinfo.gov/packages/{package_id}/summary"] = hearing_id
        for field in ("pdf", "formatted_text"):
            link = missing[hearing_id][field]
            if link and not link.startswith(f"https://api.govinfo.gov/packages/{package_id}/"):
                raise ValueError("frozen rendition contradicts expected package")
    settings = Settings.from_env(Path(__file__).resolve().parents[1])
    with psycopg.connect(settings.database_url, options=(
        "-c default_transaction_read_only=on -c statement_timeout=8000 "
        "-c lock_timeout=1000 -c max_parallel_workers_per_gather=0"
    )) as connection:
        rows = connection.execute("""
            select raw.endpoint_url, version.id::text, version.response_hash,
                   version.first_seen_at, version.response,
                   count(*) over (partition by version.raw_response_id),
                   observed.id, observed.fetched_at, observed.outcome
            from raw_responses raw
            join raw_response_versions version on version.raw_response_id = raw.id
            left join lateral (
                select event.id, event.fetched_at, event.outcome
                from crawl_run_fetches event
                where event.raw_response_version_id = version.id
                  and event.fetched_at <= %s
                order by event.fetched_at desc, event.id desc
                limit 1
            ) observed on true
            where raw.endpoint_url = any(%s)
              and raw.source = 'govinfo' and raw.category = 'govinfo-hearings'
              and raw.node_type = 'govinfo_hearing_package_detail'
              and version.first_seen_at <= %s
            limit 1000
        """, (manifest["database_snapshot_at"], list(urls), manifest["database_snapshot_at"])).fetchall()
    if len(rows) >= 1000:
        raise ValueError("bounded raw inventory exceeded")
    grouped = defaultdict(list)
    for row in rows:
        grouped[row[0]].append(row)
    additions = []
    for versions_for_url in grouped.values():
        chosen = choose_observed_version(versions_for_url)
        url, version_id, response_hash, first_seen, source, versions, event_id, observed_at, outcome = chosen
        hearing_id = urls[url]
        additions.append({"row": project_parent(hearing_id, source), "evidence": {
            "official_metadata_url": url, "raw_response_version_id": version_id,
            "raw_response_hash": response_hash, "first_observed_at": first_seen.isoformat(),
            "pre_snapshot_version_count": versions, "source_package_id": source["packageId"],
            "selected_fetch_event_id": str(event_id) if event_id is not None else None,
            "selected_fetch_observed_at": observed_at.isoformat() if observed_at else None,
            "selected_fetch_outcome": outcome,
            "version_selection": "latest pre-snapshot exact-version fetch event" if versions > 1 else "unique retained pre-snapshot version",
            "source_projection": {k: source.get(k) for k in ("packageId", "congress", "session", "title", "pages", "lastModified")},
            "frozen_text_row": missing[hearing_id],
            "appropriation_rule": "Existing public GovInfo title-keyword projection, not an identity/matching rule."}})
    found = {a["row"]["hearing_id"] for a in additions}
    if len(found) != len(additions) or found != missing.keys():
        raise ValueError("not every missing parent has one verified pre-snapshot source package")
    return {"schema_version": 1, "captured_at": datetime.now(timezone.utc).isoformat(),
            "snapshot": snapshot.name, "manifest_sha256": sha(manifest_path),
            "source_files": {n: tables[n]["sha256"] for n in files},
            "missing_parent_count": len(missing), "proposed_additions": additions,
            "applied": False, "production_modified": False,
            "evidence_scope": "Source-backed missing root projection, not full frozen-input generation replay.",
            "limits": ["NULL source fields remain NULL.", "No title-based identity match or cross-Congress relink.",
                       "Other missing metadata/children and family completeness require separate validation."]}


def choose_observed_version(rows):
    if len(rows) == 1:
        return rows[0]
    if any(r[7] is None for r in rows):
        raise ValueError("multiple versions require observed pre-snapshot fetch events for every version")
    ranked = sorted(rows, key=lambda r: r[7], reverse=True)
    if ranked[0][7] == ranked[1][7]:
        raise ValueError("ambiguous latest source-version observation")
    return ranked[0]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("output already exists")
    result = capture(args.snapshot)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x") as stream:
        json.dump(result, stream, indent=2)
        stream.write("\n")
    print(json.dumps({"verified_missing_parents": result["missing_parent_count"], "applied": False}))
