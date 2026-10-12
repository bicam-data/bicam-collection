"""Bounded read-only real-data comparison of bill-version publication SQL.

Writes PRIVATE diagnostic/review artifacts, never database objects or release
pointers. A purposive bill sample is not population coverage or accuracy.
"""
from __future__ import annotations

import argparse
import csv
from datetime import date, datetime
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time
from uuid import UUID

import jinja2
import psycopg
from psycopg.rows import dict_row

from bicam_collection.config import Settings

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PACKAGES = ["BILLS-108hr10v1rh", "BILLS-118hr1ih", "BILLS-119hr1ih", "BILLS-119hr1enr"]
MAX_ROWS = 2000


def json_default(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, (UUID, Decimal)):
        return str(value)
    raise TypeError(type(value).__name__)


def encoded(value):
    return (json.dumps(value, sort_keys=True, default=json_default, ensure_ascii=False) + "\n").encode()


def sha(value):
    return hashlib.sha256(encoded(value)).hexdigest()


def render_model(body: str, macros: str) -> str:
    env = jinja2.Environment(undefined=jinja2.StrictUndefined)
    refs = {"bridge_govinfo__bill_versions": "audit_bridge", "bills_versions": "audit_versions",
            "bills_titles": "audit_titles", "bills_texts": "audit_texts", "bills_actions": "audit_actions",
            "int_raw_responses__current": "public.int_raw_responses__current"}
    return env.from_string(macros + "\n" + body).render(
        config=lambda **kwargs: "", ref=lambda name: refs[name],
        source=lambda schema, name: 'public.raw_response_versions' if (schema, name) == ('raw', 'raw_response_versions') else (_ for _ in ()).throw(ValueError("unexpected source")),
    )


def changes(before, after, key):
    old = {row[key]: row for row in before}
    new = {row[key]: row for row in after}
    if len(old) != len(before) or len(new) != len(after):
        raise ValueError("comparison input violates its row key")
    return {
        "added": sorted(new.keys() - old.keys()), "removed": sorted(old.keys() - new.keys()),
        "changed": [{"key": value, "fields": {field: {"before": old[value].get(field), "after": new[value].get(field)}
                                               for field in sorted(old[value].keys() | new[value].keys())
                                               if old[value].get(field) != new[value].get(field)}}
                    for value in sorted(old.keys() & new.keys()) if old[value] != new[value]],
    }


def audit(connection, package_ids, output: Path):
    if not 1 <= len(package_ids) <= 8 or len(set(package_ids)) != len(package_ids) or any(not re.fullmatch(r"BILLS-[0-9]+[a-z]+[0-9]+[a-z0-9]+", value) for value in package_ids):
        raise ValueError("request 1-8 unique BILLS package IDs")
    output.mkdir(parents=True, exist_ok=False, mode=0o700)
    timings = {}
    def query(name, statement, parameters=None):
        started = time.monotonic()
        rows = connection.execute(statement, parameters).fetchmany(MAX_ROWS + 1)
        timings[name] = round(time.monotonic() - started, 3)
        if len(rows) > MAX_ROWS:
            raise ValueError(f"{name} exceeded {MAX_ROWS}-row bound; no partial success")
        if len(encoded(rows)) > 64 * 1024 * 1024:
            raise ValueError(f"{name} exceeded 64-MiB artifact bound")
        return rows

    connection.execute("set transaction isolation level repeatable read, read only")
    connection.execute("set local statement_timeout = '10s'")
    connection.execute("set local lock_timeout = '1s'")
    connection.execute("set local work_mem = '8MB'")
    connection.execute("set local max_parallel_workers_per_gather = 0")
    connection.execute("set local timezone = 'UTC'")
    snapshot = connection.execute("select transaction_timestamp() as captured_at, pg_current_snapshot()::text as snapshot").fetchone()
    found = query("requested_packages", "select package_id,canonical_bill_id from public.bridge_govinfo__bill_versions where package_id=any(%s)", (package_ids,))
    if not found:
        raise ValueError("no requested package found")
    bill_ids = sorted({row["canonical_bill_id"] for row in found})
    params = {"bill_ids": bill_ids}
    inputs = {"bridge": query("bridge", "select * from public.bridge_govinfo__bill_versions where canonical_bill_id=any(%s)", (bill_ids,))}
    for table in ("titles", "texts", "actions"):
        inputs[table] = query(table, f"select * from bicam.bills_{table} where bill_id=any(%s)", (bill_ids,))
    version_ids = list({value for row in inputs["bridge"] for value in (row["govinfo_raw_response_version_id"], row["congress_raw_response_version_id"]) if value})
    raw_ids = list({row["govinfo_raw_id"] for row in inputs["bridge"]})
    inputs["raw_current"] = query("raw_current", "select * from public.int_raw_responses__current where raw_id=any(%s)", (raw_ids,))
    version_ids = list(set(version_ids) | {row["raw_response_version_id"] for row in inputs["raw_current"] if row["raw_response_version_id"]})
    inputs["raw_versions"] = query("raw_versions", "select * from public.raw_response_versions where id=any(%s)", (version_ids,))
    urls = sorted({row["response"].get("download", {}).get("txtLink") for row in inputs["raw_versions"] if row["response"].get("download", {}).get("txtLink")})
    inputs["html"] = query("html", "select * from public.html_text_successful_current where url=any(%s)", (urls,))
    ctes = "with audit_bridge as materialized (select * from public.bridge_govinfo__bill_versions where canonical_bill_id=any(%(bill_ids)s))"
    for table in ("titles", "texts", "actions"):
        ctes += f", audit_{table} as materialized (select * from bicam.bills_{table} where bill_id=any(%(bill_ids)s))"
    macros = "\n".join((ROOT / f"dbt_project/macros/{name}.sql").read_text() for name in ("hash_to_uuid", "govinfo_bill_versions"))
    results = {}
    code = {"macros.sql": macros}
    for revision in ("baseline", "candidate"):
        bodies = {}
        for name in ("bills_versions", "bills_version_links"):
            relative = f"dbt_project/models/marts/bicam/{name}.sql"
            body = (ROOT / relative).read_text() if revision == "candidate" else subprocess.check_output(["git", "show", f"HEAD:{relative}"], cwd=ROOT, text=True)
            code[f"{revision}_{name}.sql"] = body
            bodies[name] = render_model(body, macros)
        scope = ctes + ", audit_versions as materialized (" + bodies["bills_versions"] + ")"
        results[revision] = {name: query(f"{revision}_{name}", scope + " select * from (" + body + ") as result", params) for name, body in bodies.items()}
    results["published"] = {name: query(f"published_{name}", f"select * from bicam.{name} where bill_id=any(%s)", (bill_ids,)) for name in ("bills_versions", "bills_version_links")}
    evidence = {"snapshot": snapshot, "selection": {"method": "purposive diagnostic sample; all versions/children of selected bills", "requested_packages": package_ids, "found_packages": found, "bill_ids": bill_ids}, "inputs": inputs, "outputs": results}
    input_digest = sha(inputs)
    versions = {str(row["id"]): row for row in inputs["raw_versions"]}
    html = {row["url"]: row for row in inputs["html"]}
    candidate_rows = {row["govinfo_package_id"]: row for row in results["candidate"]["bills_versions"]}
    traces = []
    for bridge in inputs["bridge"]:
        raw = versions.get(str(bridge["govinfo_raw_response_version_id"]))
        if raw and raw["raw_response_id"] != bridge["govinfo_raw_id"]:
            raw = None
        public = candidate_rows[bridge["package_id"]]
        traces.append({"govinfo_package_id": bridge["package_id"], "public_row": public, "input_bundle_sha256": input_digest,
                       "raw_version_id": str(raw["id"]) if raw else None, "raw_version_sha256": sha(raw) if raw else None,
                       "html_extraction_id": str(html[public["formatted_text"]]["id"]) if public["formatted_text"] in html else None,
                       "source_resolution": "exact_version" if raw else "unresolved"})
    report = {"schema_version": 1, "scope": "read-only bill publication candidate comparison, not a release build",
              "snapshot": snapshot, "input_sha256": input_digest, "selection": evidence["selection"],
              "timings_seconds": timings, "independent_review_complete": False, "research_release_ready": False,
              "comparison": {name: changes(results["baseline"][name], results["candidate"][name], key) for name, key in (("bills_versions", "govinfo_package_id"), ("bills_version_links", "link_id"))},
              "row_counts": {rev: {name: len(rows) for name, rows in tables.items()} for rev, tables in results.items()}}
    for name, value in (("evidence.json", evidence), ("traces.json", traces)):
        (output / name).write_bytes(encoded(value))
    for name, value in code.items():
        (output / name).write_text(value)
    with (output / "review.csv").open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=["case_id", "kind", "package_id", "input_sha256", "reviewer", "decision", "source_evidence", "notes"])
        writer.writeheader()
        for trace in traces:
            for kind in ("linkage", "text"):
                writer.writerow({"case_id": sha([kind, trace["govinfo_package_id"], input_digest]), "kind": kind, "package_id": trace["govinfo_package_id"], "input_sha256": input_digest})
    report["files"] = {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in output.iterdir() if path.is_file()}
    (output / "report.json").write_bytes(encoded(report))
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--package", action="append")
    args = parser.parse_args()
    os.umask(0o077)
    settings = Settings.from_env(ROOT)
    with psycopg.connect(settings.database_url, row_factory=dict_row, application_name="bicam_bounded_bill_candidate_audit") as connection:
        report = audit(connection, args.package or DEFAULT_PACKAGES, args.output_dir.resolve())
    print(json.dumps({"row_counts": report["row_counts"], "changes": {name: {kind: len(rows) for kind, rows in value.items()} for name, value in report["comparison"].items()}, "review_complete": False}, indent=2))


if __name__ == "__main__":
    main()
