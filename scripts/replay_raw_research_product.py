"""Build/export the complete public product twice from frozen synthetic raw JSON.

Only accepts two explicitly named, empty disposable databases. Does not load
production settings or promote either export. Independent research review is
not replaced by synthetic reproducibility evidence.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import uuid

import psycopg
import yaml

from bicam_collection.migrations import apply_migrations

try:
    from scripts import load_ci_fixtures, export_research_snapshot, validate_bicam_product
    from scripts.replay_bill_version_slice import validate_test_url, canonical_bytes, digest
except ModuleNotFoundError:
    def _sibling(name):
        spec = importlib.util.spec_from_file_location(f"_raw_replay_{name}", Path(__file__).with_name(f"{name}.py"))
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        return module
    load_ci_fixtures = _sibling("load_ci_fixtures")
    export_research_snapshot = _sibling("export_research_snapshot")
    validate_bicam_product = _sibling("validate_bicam_product")
    _replay = _sibling("replay_bill_version_slice")
    validate_test_url, canonical_bytes, digest = _replay.validate_test_url, _replay.canonical_bytes, _replay.digest

ROOT = Path(__file__).resolve().parents[1]
FIXED_TIME = datetime(2026, 9, 1, tzinfo=timezone.utc)
SELECTORS = ["+path:models/marts/bicam", "+fct_reconciliation_metrics", "+fct_congress_internal_reciprocity_metrics", "+fct_root_detail_coverage", "+fct_collection_coverage", "+fct_source_gap_queue", "+fct_bill_text_source_exceptions"]


def frozen_rows():
    rows = []
    for index, (endpoint, source, category, node, response, parent) in enumerate(load_ci_fixtures._fixture_rows()):
        content = json.dumps(response, sort_keys=True)
        source_hash = hashlib.md5(content.encode(), usedforsecurity=False).hexdigest()
        rows.append({"raw_id": str(uuid.uuid5(uuid.NAMESPACE_URL, endpoint)),
                     "version_id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"version:{endpoint}:{source_hash}")),
                     "endpoint": endpoint, "source": source, "category": category, "node_type": node,
                     "parent_id": str(uuid.uuid5(uuid.NAMESPACE_URL, parent)) if parent else None,
                     "response": response, "response_hash": source_hash,
                     "observed_at": (FIXED_TIME + timedelta(milliseconds=index)).isoformat()})
    return rows


def require_empty(url):
    config = validate_test_url(url)
    with psycopg.connect(url) as conn:
        count = conn.execute("""select count(*) from pg_class c join pg_namespace n on n.oid=c.relnamespace
            where n.nspname not in ('pg_catalog','information_schema') and n.nspname not like 'pg_toast%'""").fetchone()[0]
        if count:
            raise ValueError("raw replay requires empty disposable databases")
    return config


def load_raw(url, rows, migrations):
    apply_migrations(database_url=url, migrations_dir=migrations)
    with psycopg.connect(url) as conn:
        for row in rows:
            conn.execute("""insert into raw_responses(id,endpoint_url,source,category,node_type,parent_id,response,response_hash,fetched_at)
                values(%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s)""",
                (row["raw_id"], row["endpoint"], row["source"], row["category"], row["node_type"], row["parent_id"], json.dumps(row["response"]), row["response_hash"], row["observed_at"]))
            conn.execute("""insert into raw_response_versions(id,raw_response_id,endpoint_url,source,category,node_type,parent_id,response,response_hash,first_seen_at,last_seen_at)
                values(%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s,%s)""",
                (row["version_id"], row["raw_id"], row["endpoint"], row["source"], row["category"], row["node_type"], row["parent_id"], json.dumps(row["response"]), row["response_hash"], row["observed_at"], row["observed_at"]))


def archive(output):
    project = output / "project"
    for directory in ("models", "macros", "seeds", "tests"):
        for source in sorted((ROOT / "dbt_project" / directory).rglob("*")):
            if source.is_file() and source.suffix in {".sql", ".yml", ".yaml", ".csv"}:
                dest = project / source.relative_to(ROOT / "dbt_project")
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(source, dest)
    shutil.copyfile(ROOT / "dbt_project/dbt_project.yml", project / "dbt_project.yml")
    shutil.copytree(ROOT / "migrations", output / "migrations", ignore=shutil.ignore_patterns("__pycache__"))
    shutil.copyfile(ROOT / "contracts/bicam_public_schema.yaml", output / "contract.yaml")
    shutil.copyfile(ROOT / "uv.lock", output / "uv.lock")
    shutil.copyfile(Path(__file__), output / "replay_raw_research_product.py")
    shutil.copyfile(ROOT / "scripts/sql/bill_publication_lineage.sql", output / "bill_publication_lineage.sql")
    return project


def raw_state(conn):
    return {table: [row[0] for row in conn.execute(f"select to_jsonb(r) from {table} r order by id").fetchall()]
            for table in ("raw_responses", "raw_response_versions", "html_text_versions")}


def run(urls, output):
    if len(set(urls)) != 2:
        raise ValueError("two different disposable databases are required")
    configs = [require_empty(url) for url in urls]
    if (configs[0]["host"], configs[0]["port"], configs[0]["dbname"]) == (configs[1]["host"], configs[1]["port"], configs[1]["dbname"]):
        raise ValueError("database identities must differ")
    output.mkdir(parents=True, exist_ok=False)
    project = archive(output)
    rows = frozen_rows()
    (output / "raw_fixture.json").write_bytes(canonical_bytes(rows))
    contract = validate_bicam_product.load_contract(output / "contract.yaml")
    relations = validate_bicam_product.contract_relations(contract, include_supplemental=True)
    source_files = {p.relative_to(output).as_posix(): digest(p.read_bytes()) for p in sorted(output.rglob("*")) if p.is_file()}
    input_id = digest(canonical_bytes(source_files))
    all_builds = []
    for index, (url, config) in enumerate(zip(urls, configs, strict=True), 1):
        directory = output / f"build-{index}"
        directory.mkdir()
        load_raw(url, rows, output / "migrations")
        with psycopg.connect(url) as conn:
            frozen_state = raw_state(conn)
        (directory / "frozen-database-inputs.json").write_bytes(canonical_bytes(frozen_state))
        profile = {"bicam_collection": {"target": "replay", "outputs": {"replay": {
            "type": "postgres", "host": config["host"], "port": int(config["port"]), "user": config["user"],
            "password": "{{ env_var('DBT_ENV_SECRET_REPLAY_PASSWORD') }}", "dbname": config["dbname"], "schema": "public", "threads": 2,
        }}}}
        env = {k: v for k, v in os.environ.items() if not k.startswith(("PG", "DBT_", "DB_"))}
        env.update({"DBT_ENV_SECRET_REPLAY_PASSWORD": config.get("password", "unused-fixture-password"), "DBT_SEND_ANONYMOUS_USAGE_STATS": "false", "BICAM_CI_FIXTURES": "1"})
        with tempfile.TemporaryDirectory(prefix="bicam-raw-profile-") as temporary:
            Path(temporary, "profiles.yml").write_text(yaml.safe_dump(profile))
            def dbt(command, extra, label):
                target = directory / label
                args = ["dbt", command, "--project-dir", str(project), "--profiles-dir", temporary,
                        "--target-path", str(target), "--log-path", str(directory / "logs"), "--no-partial-parse", *extra]
                with (directory / f"{label}.log").open("w") as log:
                    subprocess.run(args, env=env, stdout=log, stderr=subprocess.STDOUT, check=True, timeout=600)
                result = json.loads((target / "run_results.json").read_text())
                if not result["results"] or any(row["status"] not in {"success", "pass"} for row in result["results"]):
                    raise ValueError(f"incomplete {label}")
                return result
            dbt("seed", ["--full-refresh"], "seed")
            result = dbt("run", ["--full-refresh", "--select", *SELECTORS], "run")
            required = {f"model.bicam_collection.{row['table']}" for row in relations}
            if not required <= {row["unique_id"] for row in result["results"]}:
                raise ValueError("not every public relation was built")
            dbt("test", ["--select", "govinfo_bill_version_codes_are_normalized", "bill_text_parent_lineage_propagates", "bill_title_observations_retain_lineage", "bill_action_observations_retain_lineage", "bills_recorded_vote_keys_are_preserved", "nominations_nominees_preserve_duplicate_ordinals", "bill_text_source_exceptions_preserve_identity", "bills_version_links_text_url_codes_match_source"], "test")
        with psycopg.connect(url) as conn:
            if raw_state(conn) != frozen_state:
                raise ValueError("raw inputs changed during build")
            validation = validate_bicam_product.validate_product_data(conn, contract, include_supplemental=True)
            (directory / "data-validation.json").write_bytes(canonical_bytes(validation))
            if not validation["valid"]:
                raise ValueError("public row contract failed")
            with conn.cursor(row_factory=psycopg.rows.dict_row) as cursor:
                lineage = cursor.execute((output / "bill_publication_lineage.sql").read_text()).fetchall()
            for table in ("bills_versions", "bills_titles", "bills_actions", "bills_texts"):
                count = conn.execute(f"select count(*) from bicam.{table}").fetchone()[0]
                if sum(row["relation"] == table for row in lineage) != count:
                    raise ValueError(f"missing public-row source trace for {table}")
            if any(not item["resolved"] for row in lineage for item in row["evidence"]):
                raise ValueError("fixture source trace is not backed by frozen raw evidence")
            (directory / "bill-row-lineage.json").write_bytes(canonical_bytes(lineage))
        manifest = export_research_snapshot.export_snapshot(database_url=url, output_dir=directory / "candidate", include_supplemental=True, include_source_evidence=True, source_evidence_limit=100, contract_path=output / "contract.yaml")
        tables = {row["name"]: {"rows": row["row_count"], "sha256": row["sha256"]} for row in manifest["tables"]}
        manifest_path = manifest["_candidate_manifest_path"]
        binding = {"schema_version": 1, "input_generation_id": input_id, "input_files": source_files,
                   "candidate_manifest_sha256": digest(Path(manifest_path).read_bytes()),
                   "dbt_run_results_sha256": digest((directory / "run/run_results.json").read_bytes()),
                   "dbt_manifest_sha256": digest((directory / "run/manifest.json").read_bytes()),
                   "dbt_test_results_sha256": digest((directory / "test/run_results.json").read_bytes()),
                   "frozen_database_inputs_sha256": digest(canonical_bytes(frozen_state)),
                   "bill_row_lineage_sha256": digest(canonical_bytes(lineage)),
                   "completed_public_relations": sorted(row["table"] for row in relations),
                   "public_contract_passed": True, "independent_review_complete": False,
                   "scope": "synthetic frozen-raw product replay; not production generation evidence"}
        (directory / "generation-binding.json").write_bytes(canonical_bytes(binding))
        all_builds.append({"candidate": str(manifest_path), "tables": tables, "binding": binding})
        print(f"completed isolated raw build {index}: {len(tables)} public relations", flush=True)
    if all_builds[0]["tables"] != all_builds[1]["tables"]:
        raise ValueError("independent raw-to-public rebuilds differ")
    report = {"schema_version": 1, "replay_passed": True, "research_release_ready": False,
              "input_generation_id": input_id, "raw_payload_count": len(rows), "builds": all_builds,
              "limitations": ["synthetic corpus: empty relations and upstream source gaps must remain explicit",
                              "no independent empirical linkage/text adjudication", "no production promotion performed"]}
    (output / "report.json").write_bytes(canonical_bytes(report))
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    run([os.environ["BICAM_TEST_DATABASE_URL"], os.environ["BICAM_SECOND_TEST_DATABASE_URL"]], args.output_dir.resolve())


if __name__ == "__main__":
    main()
