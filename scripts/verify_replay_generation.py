"""Offline check of a synthetic replay artifact's input/build/candidate binding."""
from __future__ import annotations

import argparse
from collections import Counter
import csv
from datetime import datetime
import gzip
import hashlib
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.evaluate_research_snapshot import _csv_rows  # noqa: E402
from scripts.promote_research_snapshot import validate_candidate  # noqa: E402
from scripts.validate_bicam_product import contract_relations, load_contract  # noqa: E402


def canonical(value):
    return (json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":")) + "\n").encode()


def checked_path(root, name):
    path = Path(name)
    if path.is_absolute() or not path.parts or ".." in path.parts or path.as_posix() != name:
        raise ValueError("unsafe generation evidence path")
    target = root / path
    if any((root / Path(*path.parts[:index])).is_symlink() for index in range(1, len(path.parts) + 1)) or not target.is_file():
        raise ValueError("missing or symlinked generation evidence")
    return target


def file_hash(path):
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def normalized(value, column_type):
    if value is None:
        return None
    if column_type == "boolean":
        if value is True or isinstance(value, str) and value in ("t", "true"):
            return "true"
        if value is False or isinstance(value, str) and value in ("f", "false"):
            return "false"
        raise ValueError("invalid boolean in trace")
    if column_type in ("timestamptz", "timestamp with time zone"):
        parsed = datetime.fromisoformat(str(value))
        if parsed.utcoffset() is None:
            raise ValueError("trace timestamp lacks timezone")
        return str(parsed.timestamp())
    return str(value)


def verify(build: Path):
    if build.is_symlink() or not build.is_dir():
        raise ValueError("invalid replay build directory")
    root = build.parent
    binding = json.loads(checked_path(build, "generation-binding.json").read_text())
    if binding.get("schema_version") != 1 or binding.get("scope") != "synthetic frozen-raw product replay; not production generation evidence":
        raise ValueError("unsupported generation evidence scope")
    sources = binding["input_files"]
    if not isinstance(sources, dict) or not {"raw_fixture.json", "project/dbt_project.yml", "contract.yaml"} <= sources.keys():
        raise ValueError("missing generation source inventory")
    for name, digest in sources.items():
        if file_hash(checked_path(root, name)) != digest:
            raise ValueError(f"frozen source bytes changed: {name}")
    if hashlib.sha256(canonical(sources)).hexdigest() != binding["input_generation_id"]:
        raise ValueError("input generation ID differs")
    manifests = list((build / "candidate").glob("*/manifest.json"))
    if len(manifests) != 1:
        raise ValueError("expected exactly one candidate")
    candidate = manifests[0]
    bound = {"dbt_run_results_sha256": "run/run_results.json", "dbt_manifest_sha256": "run/manifest.json",
             "dbt_test_results_sha256": "test/run_results.json", "frozen_database_inputs_sha256": "frozen-database-inputs.json",
             "bill_row_lineage_sha256": "bill-row-lineage.json"}
    for field, name in bound.items():
        if file_hash(checked_path(build, name)) != binding.get(field):
            raise ValueError(f"generation artifact differs: {name}")
    if file_hash(candidate) != binding["candidate_manifest_sha256"]:
        raise ValueError("bound candidate manifest differs")
    contract = load_contract(root / "contract.yaml")
    relations = contract_relations(contract, include_supplemental=True)
    names = sorted(row["table"] for row in relations)
    if binding["completed_public_relations"] != names:
        raise ValueError("generation public inventory incomplete")
    results = json.loads((build / "run/run_results.json").read_text())["results"]
    successful = {row["unique_id"] for row in results if row["status"] == "success"}
    if not {f"model.bicam_collection.{name}" for name in names} <= successful or any(row["status"] != "success" for row in results):
        raise ValueError("generation contains failed or unbuilt public nodes")
    tests = json.loads((build / "test/run_results.json").read_text())["results"]
    if not tests or any(row["status"] != "pass" for row in tests):
        raise ValueError("generation SQL tests incomplete")
    validation = validate_candidate(candidate, contract_path=root / "contract.yaml", output_dir=candidate.parent.parent, include_supplemental=True, require_source_evidence=True)
    lineage = json.loads((build / "bill-row-lineage.json").read_text())
    raw = json.loads((build / "frozen-database-inputs.json").read_text())
    versions = {row["id"]: row for row in raw["raw_response_versions"]}
    expected_tables = {"bills_versions", "bills_titles", "bills_actions", "bills_texts"}
    if any(row["relation"] not in expected_tables for row in lineage):
        raise ValueError("unexpected row lineage relation")
    columns = {row["table"]: row["columns"] for row in relations}
    for row in lineage:
        if not row["evidence"]:
            raise ValueError("empty row evidence")
        for item in row["evidence"]:
            source = versions.get(item["raw_version_id"])
            if not item["resolved"] or source is None or source["response_hash"] != item["source_response_hash"]:
                raise ValueError("row lineage is not bound to a frozen raw version")
    manifest = json.loads(candidate.read_text())
    marker = manifest["serialization"]["csv_null"]
    csv.field_size_limit(sys.maxsize)
    for table in sorted(expected_tables):
        coltypes = columns[table]
        def key(values):
            return tuple(normalized(values[column], kind) for column, kind in coltypes.items())
        traced = Counter(key(row["public_row"]) for row in lineage if row["relation"] == table)
        with gzip.open(candidate.parent / f"{table}.csv.gz", "rt", encoding="utf-8", newline="") as stream:
            rows = _csv_rows(stream, marker)
            header = next(rows)
            exported = Counter(key(dict(zip(header, row, strict=True))) for row in rows)
        if traced != exported:
            raise ValueError(f"row traces differ from exported rows: {table}")
    return {"valid": True, "scope": binding["scope"], "public_relation_count": validation["relation_count"],
            "traced_bill_rows": len(lineage), "traced_relations": sorted(expected_tables),
            "input_generation_id": binding["input_generation_id"], "research_release_ready": False,
            "limitations": ["bill-row traces cover four relations only; other rows are generation-bound",
                            "synthetic sample and fixture-safe tests are not empirical release acceptance"]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("build", type=Path)
    args = parser.parse_args()
    print(json.dumps(verify(args.build.resolve()), indent=2))


if __name__ == "__main__":
    main()
