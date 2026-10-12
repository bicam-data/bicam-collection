"""Replay real bill-version publication SQL on synthetic, frozen boundary inputs.

Requires an explicit empty, loopback-only database named bicam_replay_*.
Never loads .env or DATABASE_URL. Does not rebuild upstream reconciliation or
evaluate empirical linkage accuracy. Outputs are diagnostic, not release-ready.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime
from decimal import Decimal
import gzip
import hashlib
import io
from importlib.metadata import version
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import tempfile
from typing import Any

import psycopg
from psycopg.conninfo import conninfo_to_dict
from psycopg.rows import dict_row
from psycopg import sql
import yaml
import zlib

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "tests/fixtures/bill_version_replay"
BOUNDARIES = {
    "bridge_govinfo__bill_versions": "input_bridge",
    "int_raw_responses__current": "input_current",
    "bills_titles": "input_titles",
    "bills_texts": "input_texts",
    "bills_actions": "input_actions",
}
INPUT_TABLES = [*BOUNDARIES.values(), "input_raw_versions", "input_child_evidence", "html_text_successful_current"]
PUBLIC_TABLES = ["bills_versions", "bills_version_links"]


def _json_default(value: Any) -> str:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError(f"unsupported replay value: {type(value).__name__}")


def canonical_bytes(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, ensure_ascii=False, default=_json_default, separators=(",", ":")) + "\n").encode()


def digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def deterministic_gzip(payload: bytes) -> bytes:
    output = io.BytesIO()
    with gzip.GzipFile(filename="", mode="wb", fileobj=output, mtime=0) as stream:
        stream.write(payload)
    return output.getvalue()


def validate_test_url(value: str | None) -> dict[str, str]:
    if not value:
        raise ValueError("BICAM_TEST_DATABASE_URL is required; no production DSN fallback")
    config = conninfo_to_dict(value)
    if config.get("host") not in {"127.0.0.1", "localhost", "::1"} or config.get("hostaddr") or config.get("service"):
        raise ValueError("replay database must use an explicit loopback host without service/hostaddr overrides")
    if not config.get("dbname", "").startswith("bicam_replay_"):
        raise ValueError("replay database name must start with bicam_replay_")
    if not config.get("user") or not config.get("port", "").isdigit():
        raise ValueError("replay requires an explicit database user and numeric port")
    if any(os.environ.get(key) for key in ("PGHOSTADDR", "PGSERVICE", "PGSERVICEFILE")):
        raise ValueError("replay refuses ambient PostgreSQL connection overrides")
    return config


def _rows(connection: psycopg.Connection, schema: str, table: str) -> list[dict]:
    rows = connection.execute(sql.SQL("select * from {}.{}").format(sql.Identifier(schema), sql.Identifier(table))).fetchall()
    return sorted(rows, key=canonical_bytes)


def _inputs(connection: psycopg.Connection) -> dict[str, list[dict]]:
    return {name: _rows(connection, "public", name) for name in INPUT_TABLES}


def _archive_project(output: Path) -> Path:
    project = output / "project"
    (project / "models").mkdir(parents=True)
    (project / "macros").mkdir()
    (project / "tests").mkdir()
    shutil.copyfile(ROOT / "dbt_project/tests/govinfo_bill_version_codes_are_normalized.sql", project / "tests/govinfo_bill_version_codes_are_normalized.sql")
    for name in PUBLIC_TABLES:
        shutil.copyfile(ROOT / f"dbt_project/models/marts/bicam/{name}.sql", project / f"models/{name}.sql")
    for name in ("hash_to_uuid", "generate_schema_name", "govinfo_bill_versions"):
        shutil.copyfile(ROOT / f"dbt_project/macros/{name}.sql", project / f"macros/{name}.sql")
    for name, boundary in BOUNDARIES.items():
        (project / f"models/{name}.sql").write_text("{{ config(materialized='view') }}\nselect * from public." + boundary + "\n")
    (project / "models/sources.yml").write_text(yaml.safe_dump({
        "version": 2, "sources": [{"name": "raw", "schema": "public", "tables": [{"name": "raw_response_versions"}]}],
    }))
    (project / "dbt_project.yml").write_text(yaml.safe_dump({
        "name": "bicam_version_replay", "version": "1.0.0", "config-version": 2,
        "profile": "bicam_version_replay", "model-paths": ["models"], "macro-paths": ["macros"],
    }))
    for name in ("inputs.sql", "traces.sql", "link_traces.sql"):
        shutil.copyfile(FIXTURE / name, output / name)
    shutil.copytree(FIXTURE.parent / 'bill_source_identity', output / 'bill_source_identity')
    shutil.copyfile(Path(__file__), output / "replay_bill_version_slice.py")
    shutil.copyfile(ROOT / "uv.lock", output / "uv.lock")
    (output / "environment.json").write_bytes(canonical_bytes({
        "python": platform.python_version(), "zlib": zlib.ZLIB_RUNTIME_VERSION,
        "packages": {name: version(name) for name in ("dbt-core", "dbt-postgres", "psycopg", "PyYAML")},
    }))
    (output / "README.md").write_text("""# Frozen bill-version publication replay

Synthetic boundary inputs, not an independently adjudicated source corpus.
The real publication models/macros are in project/. Upstream reconciliation
is frozen in inputs.sql, not recomputed. No credentials are included.

To rebuild without the operational database or original source checkout:

1. Install the dbt-core and dbt-postgres versions in environment.json.
2. Create an EMPTY disposable PostgreSQL database and load inputs.sql using
   psql with ON_ERROR_STOP=1. Never load this fixture into a live database.
3. Supply a private dbt profiles.yml with profile bicam_version_replay,
   target replay, schema public, and the disposable database credentials.
4. Run dbt run --project-dir project --profiles-dir YOUR_PRIVATE_PROFILE_DIR
   --full-refresh --threads 1. Repeat to check stable results.
5. Run traces.sql and link_traces.sql to inspect exact fixture input mappings.

build-1/ and build-2/ contain dbt manifests, run results, logs, and canonical
JSON gzip files. Public rows are sorted by compact UTF-8 JSON with sorted keys;
dates/timestamps and decimal values are serialized as strings. Gzip uses an
empty filename and mtime=0. Compare with the hashes in report.json. The source
replay script is archived for inspection; its convenience CLI expects the
repository layout. uv.lock records the repository environment, including its
local Rotisserie dependency; Rotisserie is NOT needed for this small project.

A successful repeat proves deterministic publication-layer output only.
Legacy-link and exact-source selection regressions are checked in this slice.
Full upstream replay and independently adjudicated evaluation remain open.
""")
    return project


def _check_results(tables: dict[str, list[dict]], traces: list[dict]) -> None:
    versions = {row["govinfo_package_id"]: row for row in tables["bills_versions"]}
    assert len(versions) == 7, "fixture lost a bill version"
    star = versions['BILLS-119hr3426eh1s']
    assert (star['version_code'], star['rendition_suffix'], star['starprint_number'], star['version_variant']) == ('EH', 'eh1s', 1, None)
    assert versions["BILLS-108hr10v1rh"]["version_code"] == "RH"
    assert versions["BILLS-108hr10v1rh"]["version_variant"] == 1
    assert versions["BILLS-108hr10v1rh"]["raw_text"] is None
    assert versions["BILLS-118hr1rh"]["raw_text"] == "Synthetic original text."
    assert versions["BILLS-118hr1rh"]["formatted_text"] == "https://fixture.test/old/BILLS-118hr1rh.htm"
    for package in ("BILLS-118hr2ih", "BILLS-118hr3ih", "BILLS-118hr4ih"):
        assert versions[package]["formatted_text"] is None
        assert versions[package]["raw_text"] is None
    assert versions["BILLS-118hr1ih"]["raw_text"].endswith("café")
    links = tables["bills_version_links"]
    assert len(links) == 13, "fixture link inventory changed"
    star_link = next(row for row in links if row['bill_id'] == 'hr3426-119')
    assert (star_link['source_version_code'], star_link['govinfo_package_id'], star_link['link_status']) == ('EH', 'BILLS-119hr3426eh1s', 'linked')
    assert len([row for row in links if row["link_status"] == "no_matching_govinfo_version"]) == 2
    legacy = [row for row in links if row["bill_id"] == "hr10-108"]
    assert len(legacy) == 1 and legacy[0]["link_status"] == "linked"
    assert legacy[0]["source_version_code"] == "RH"
    by_name = {row["source_version_name"]: row for row in links if row["source_table"] == "bills_texts"}
    assert by_name["Numeric variant"]["govinfo_version_code"] == "RH"
    assert by_name["PDF fallback"]["govinfo_version_code"] == "IH"
    assert "Unrelated URL" not in by_name
    assert {row["source_row_key"] for row in links if row["source_table"] == "bills_actions"} == {"a-exact"}
    assert not any(row["source_table"] == "bills_texts" and row["source_version_name"] == "Shared ambiguous label" for row in links)
    for row in links:
        if row["link_status"] == "linked":
            target = versions[row["govinfo_package_id"]]
            assert (target["bill_id"], target["version_code"]) == (row["bill_id"], row["govinfo_version_code"])
    assert len(traces) == len(versions)
    mixed = [row for row in traces if row["mixed_raw_versions"]]
    assert not mixed
    assert sum(row["unresolved_raw_version"] for row in traces) == 3
    for trace in traces:
        if trace["unresolved_raw_version"]:
            assert trace["rendition_source_version"] is None
            assert trace["public_row"]["formatted_text"] is None
            assert trace["public_row"]["raw_text"] is None
            continue
        assert trace["bridge_source_version"] is not None
        assert trace["rendition_source_version"] is not None
        assert trace["public_row"]["formatted_text"] == trace["rendition_source_version"]["response"]["download"]["txtLink"]
        assert trace["public_row"]["raw_text"] == (trace["html_extraction_input"]["text_content"] if trace["html_extraction_input"] else None)


def run_replay(test_url: str, output: Path) -> dict:
    if not __debug__:
        raise ValueError("replay checks require Python assertions; do not run with -O")
    config = validate_test_url(test_url)
    # Check before any filesystem or database mutation. Never reuse an existing
    # database containing relations, even if its name happens to match.
    with psycopg.connect(test_url, row_factory=dict_row) as connection:
        existing = connection.execute("""select count(*) as count from pg_class c
            join pg_namespace n on n.oid=c.relnamespace
            where n.nspname not in ('pg_catalog', 'information_schema')
              and n.nspname not like 'pg_toast%' and n.nspname not like 'pg_temp%'""").fetchone()["count"]
        if existing:
            raise ValueError("replay requires an empty disposable database")
        output.mkdir(parents=True, exist_ok=False)
        project = _archive_project(output)
        connection.execute((output / "inputs.sql").read_text())
        frozen = _inputs(connection)
    frozen_bytes = canonical_bytes(frozen)
    (output / "frozen-inputs.json").write_bytes(frozen_bytes)
    # Profiles contain environment references only, and are not distributed.
    profile = {"bicam_version_replay": {"target": "replay", "outputs": {"replay": {
        "type": "postgres", "host": config["host"], "port": int(config["port"]),
        "user": config["user"], "dbname": config["dbname"], "schema": "public", "threads": 1,
        "password": "{{ env_var('DBT_ENV_SECRET_REPLAY_PASSWORD', '') }}",
        "connect_timeout": 10,
    }}}}
    env = {key: value for key, value in os.environ.items() if not key.startswith(("DBT_", "PG"))}
    env["DBT_ENV_SECRET_REPLAY_PASSWORD"] = config.get("password", "unused-fixture-password")
    env["DBT_SEND_ANONYMOUS_USAGE_STATS"] = "false"
    builds = []
    with tempfile.TemporaryDirectory(prefix="bicam-replay-profile-") as temporary:
        Path(temporary, "profiles.yml").write_text(yaml.safe_dump(profile))
        for iteration in (1, 2):
            build_dir = output / f"build-{iteration}"
            build_dir.mkdir()
            command = ["dbt", "run", "--project-dir", str(project), "--profiles-dir", temporary,
                       "--target-path", str(build_dir / "target"), "--log-path", str(build_dir / "logs"),
                       "--full-refresh", "--no-partial-parse", "--threads", "1"]
            with (build_dir / "stdout.log").open("w") as stream:
                subprocess.run(command, env=env, stdout=stream, stderr=subprocess.STDOUT, check=True, timeout=120)
            results = json.loads((build_dir / "target/run_results.json").read_text())["results"]
            expected_nodes = {f"model.bicam_version_replay.{name}" for name in [*BOUNDARIES, *PUBLIC_TABLES]}
            if len(results) != len(expected_nodes) or {row["unique_id"] for row in results} != expected_nodes or any(row["status"] != "success" for row in results):
                raise ValueError("dbt did not complete every expected replay model")
            test_command = ["dbt", "test", "--project-dir", str(project), "--profiles-dir", temporary,
                            "--target-path", str(build_dir / "test-target"), "--log-path", str(build_dir / "logs"),
                            "--no-partial-parse", "--threads", "1"]
            with (build_dir / "test-stdout.log").open("w") as stream:
                subprocess.run(test_command, env=env, stdout=stream, stderr=subprocess.STDOUT, check=True, timeout=120)
            test_results = json.loads((build_dir / "test-target/run_results.json").read_text())["results"]
            if len(test_results) != 1 or test_results[0]["status"] != "pass":
                raise ValueError("version-code normalization SQL regression test did not pass")
            with psycopg.connect(test_url, row_factory=dict_row) as connection:
                connection.execute("set transaction isolation level repeatable read, read only")
                if canonical_bytes(_inputs(connection)) != frozen_bytes:
                    raise ValueError("frozen replay inputs changed")
                tables = {name: _rows(connection, "bicam", name) for name in PUBLIC_TABLES}
                traces = connection.execute((output / "traces.sql").read_text()).fetchall()
                link_traces = connection.execute((output / "link_traces.sql").read_text()).fetchall()
            _check_results(tables, traces)
            if len(link_traces) != len(tables["bills_version_links"]) or any(not row["child_inputs"] for row in link_traces):
                raise ValueError("bill-version link trace is missing its boundary inputs")
            artifacts = {}
            for name, rows in {**tables, "bills_versions_traces": traces, "bills_version_links_traces": link_traces}.items():
                payload = canonical_bytes(rows)
                compressed = deterministic_gzip(payload)
                (build_dir / f"{name}.json.gz").write_bytes(compressed)
                artifacts[name] = {"rows": len(rows), "canonical_sha256": digest(payload), "gzip_sha256": digest(compressed)}
            builds.append(artifacts)
    if builds[0] != builds[1]:
        raise ValueError("repeated frozen-input builds differ")
    report = {
        "schema_version": 1, "replay_passed": True,
        "scope": "synthetic bill-version publication boundary; not full raw-to-public reconciliation",
        "research_release_ready": False, "mixed_raw_version_rows": sum(row["mixed_raw_versions"] for row in traces),
        "unresolved_raw_version_rows": sum(row["unresolved_raw_version"] for row in traces),
        "frozen_inputs_sha256": digest(frozen_bytes), "builds": builds,
        "diagnostics": {
            "mixed_sources": [{"govinfo_package_id": row["govinfo_package_id"],
                               "bridge_version_id": row["bridge_source_version"]["version_id"],
                               "rendition_version_id": row["rendition_source_version"]["version_id"]}
                              for row in traces if row["mixed_raw_versions"]],
            "legacy_text_links": [row for row in tables["bills_version_links"]
                                  if row["bill_id"] == "hr10-108" and row["source_table"] == "bills_texts"],
        },
        "limitations": ["upstream normalization and reconciliation are frozen inputs, not replayed",
                        "link child evidence is normalized boundary evidence, not raw API lineage",
                        "synthetic labels are not independently adjudicated accuracy evidence",
                        "missing exact raw versions retain their packages with null renditions",
                        "HTML text is the latest successful extraction for the pinned URL, not an as-of reconstruction"],
    }
    report["files"] = {path.relative_to(output).as_posix(): digest(path.read_bytes())
                       for path in sorted(output.rglob("*")) if path.is_file()}
    (output / "report.json").write_bytes(canonical_bytes(report))
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    report = run_replay(os.environ.get("BICAM_TEST_DATABASE_URL"), args.output_dir.resolve())
    print(json.dumps({key: value for key, value in report.items() if key != "files"}, indent=2, default=_json_default))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
