"""Validate one immutable snapshot candidate and atomically select it as latest.

Export and promotion are intentionally separate operations. A candidate must
match the bundled contract, contain the complete declared relation inventory,
carry a matching portable provenance sidecar, and pass byte/header/row-count
checks before the mutable ``latest`` symlink is changed. Any failure leaves
the previously accepted target untouched.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import csv
import fcntl
import gzip
import hashlib
import importlib.util
import json
from pathlib import Path
import os
import sys
from typing import Any

try:  # Works both as ``python -m scripts...`` and as a direct script.
    from scripts.validate_bicam_product import DEFAULT_CONTRACT, contract_relations, load_contract
except ModuleNotFoundError:  # pragma: no cover - depends on invocation path
    _validator_spec = importlib.util.spec_from_file_location(
        "_bicam_product_validator", Path(__file__).with_name("validate_bicam_product.py")
    )
    if _validator_spec is None or _validator_spec.loader is None:
        raise ImportError("unable to load validate_bicam_product.py")
    _validator = importlib.util.module_from_spec(_validator_spec)
    _validator_spec.loader.exec_module(_validator)
    DEFAULT_CONTRACT = _validator.DEFAULT_CONTRACT
    contract_relations = _validator.contract_relations
    load_contract = _validator.load_contract

try:
    from scripts.verify_source_evidence import verify_bundle as _verify_source_evidence_bundle
except ModuleNotFoundError:  # pragma: no cover - direct-script fallback
    _source_evidence_spec = importlib.util.spec_from_file_location(
        "_bicam_verify_source_evidence", Path(__file__).with_name("verify_source_evidence.py")
    )
    if _source_evidence_spec is None or _source_evidence_spec.loader is None:
        raise ImportError("unable to load verify_source_evidence.py")
    _source_evidence = importlib.util.module_from_spec(_source_evidence_spec)
    _source_evidence_spec.loader.exec_module(_source_evidence)
    _verify_source_evidence_bundle = _source_evidence.verify_bundle


def _hash_file(path: Path) -> tuple[int, str]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
            size += len(chunk)
    return size, digest.hexdigest()


def _json_digest(value: object) -> str:
    """Hash the exporter's canonical JSON representation of metadata."""

    payload = json.dumps(value, indent=2, sort_keys=True, default=str) + "\n"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _count_rows_and_header(path: Path) -> tuple[list[str], int]:
    csv.field_size_limit(sys.maxsize)
    with gzip.open(path, "rt", encoding="utf-8", newline="") as stream:
        reader = csv.reader(stream)
        try:
            header = next(reader)
        except StopIteration as error:
            raise ValueError(f"empty CSV artifact: {path.name}") from error
        rows = sum(1 for _ in reader)
    return header, rows


def _candidate_root(manifest_path: Path, output_dir: Path) -> tuple[Path, Path]:
    manifest = manifest_path.expanduser()
    if manifest.is_symlink():
        raise ValueError("candidate manifest must not be a symlink")
    candidate_dir_path = manifest.parent
    if candidate_dir_path.is_symlink():
        raise ValueError("candidate directory must not be a symlink")
    candidate_dir = candidate_dir_path.resolve()
    root = output_dir.expanduser().resolve()
    if candidate_dir.parent != root:
        raise ValueError(f"candidate must be a direct child of {root}")
    if manifest.name != "manifest.json":
        raise ValueError("candidate file must be named manifest.json")
    if not candidate_dir.is_dir():
        raise ValueError("candidate directory does not exist")
    return root, candidate_dir


@contextmanager
def _promotion_lock(root: Path):
    """Serialize promotion attempts without changing the accepted pointer."""

    lock_path = root / ".promotion.lock"
    with lock_path.open("a", encoding="utf-8") as stream:
        fcntl.flock(stream.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(stream.fileno(), fcntl.LOCK_UN)


def validate_candidate(
    manifest_path: Path,
    *,
    contract_path: Path = DEFAULT_CONTRACT,
    output_dir: Path | None = None,
    include_supplemental: bool = False,
    require_source_evidence: bool = False,
) -> dict[str, Any]:
    """Validate candidate identity, inventory, headers, hashes, and counts."""

    manifest_path = manifest_path.expanduser()
    root = (output_dir or manifest_path.parent.parent).expanduser()
    root, candidate_dir = _candidate_root(manifest_path, root)
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise ValueError(f"manifest is not valid JSON: {manifest_path}") from error
    if not isinstance(manifest, dict):
        raise ValueError("manifest must be an object")
    contract = load_contract(contract_path)
    scope = manifest.get("scope")
    if not isinstance(scope, dict):
        raise ValueError("manifest has no scope object")
    feature_status = str(scope.get("feature_status", ""))
    require_supplemental = include_supplemental or feature_status == "core-now+supplemental"
    relations = contract_relations(contract, include_supplemental=require_supplemental)
    expected_names = [str(relation["table"]) for relation in relations]
    if manifest.get("contract_kind") != contract.get("contract_kind"):
        raise ValueError("candidate contract kind does not match bundled contract")
    if manifest.get("product") != contract.get("product", {}).get("name"):
        raise ValueError("candidate product does not match bundled contract")
    if manifest.get("schema") != "bicam":
        raise ValueError("candidate must target the bicam schema")
    serialization = manifest.get("serialization")
    if serialization is not None:
        if not isinstance(serialization, dict):
            raise ValueError("candidate serialization metadata must be an object")
        expected_serialization = {
            "format": "csv.gz",
            "encoding": "UTF-8",
            "newline": "LF",
            "header": True,
            "csv_null": r"\N",
        }
        if serialization != expected_serialization:
            raise ValueError("candidate serialization metadata does not declare the supported CSV encoding")
    source_evidence = manifest.get("source_evidence")
    if require_source_evidence and not isinstance(source_evidence, dict):
        raise ValueError("candidate source evidence is required")
    source_evidence_root: str | None = None
    if source_evidence is not None:
        if not isinstance(source_evidence, dict) or source_evidence.get("schema_version") != 1:
            raise ValueError("candidate source evidence metadata is unsupported")
        bundle_path = source_evidence.get("bundle_path")
        if not isinstance(bundle_path, str) or not bundle_path or Path(bundle_path).is_absolute():
            raise ValueError("candidate source evidence bundle path is invalid")
        bundle_relative = Path(bundle_path)
        if ".." in bundle_relative.parts:
            raise ValueError("candidate source evidence bundle path escapes the candidate")
        if len(bundle_relative.parts) != 2 or bundle_relative.parts[0] != "source-evidence" or bundle_relative.as_posix() != bundle_path:
            raise ValueError("candidate source evidence bundle path must be source-evidence/<bundle>")
        evidence_root = candidate_dir / "source-evidence"
        if evidence_root.is_symlink() or not evidence_root.is_dir():
            raise ValueError("candidate source evidence directory is missing or symlinked")
        if {path.name for path in evidence_root.iterdir()} != {bundle_relative.name}:
            raise ValueError("candidate source evidence directory contains undeclared entries")
        bundle_dir = candidate_dir / bundle_relative
        evidence_report = _verify_source_evidence_bundle(bundle_dir)
        if evidence_report.get("manifest_sha256") != source_evidence.get("manifest_sha256"):
            raise ValueError("candidate source evidence manifest digest differs")
        evidence_manifest = json.loads((bundle_dir / "manifest.json").read_text(encoding="utf-8"))
        if not evidence_report.get("database_snapshot_at") or evidence_report["database_snapshot_at"] != manifest.get("database_snapshot_at"):
            raise ValueError("candidate source evidence is not bound to the database snapshot")
        if source_evidence.get("database_snapshot_at") != manifest.get("database_snapshot_at"):
            raise ValueError("candidate source evidence metadata is not bound to the database snapshot")
        for field in ("observation_count", "unique_payload_count", "selection"):
            if type(source_evidence.get(field)) is not type(evidence_manifest.get(field)) or source_evidence.get(field) != evidence_manifest.get(field):
                raise ValueError(f"candidate source evidence {field} differs")
        if require_source_evidence and evidence_report["observation_count"] == 0:
            raise ValueError("candidate required source evidence is empty")
        source_evidence_root = bundle_relative.parts[0]
    expected_scope_tables = [{"name": name, "relation": name} for name in expected_names]
    if scope.get("relation_count") != len(expected_names) or scope.get("tables") != expected_scope_tables:
        raise ValueError("candidate scope does not match the declared relation inventory")
    tables = manifest.get("tables")
    if not isinstance(tables, list):
        raise ValueError("manifest tables must be a list")
    entries: dict[str, dict[str, Any]] = {}
    for entry in tables:
        if not isinstance(entry, dict) or not isinstance(entry.get("name"), str):
            raise ValueError("manifest table entry is invalid")
        name = str(entry["name"])
        if name in entries:
            raise ValueError(f"duplicate manifest relation: {name}")
        entries[name] = entry
    if list(entries) != expected_names:
        raise ValueError(
            "candidate relation inventory differs from contract: "
            f"expected={expected_names!r} actual={list(entries)!r}"
        )
    if manifest.get("contract_version") != contract.get("contract_version"):
        raise ValueError("candidate contract version does not match bundled contract")
    if manifest.get("snapshot_directory") != candidate_dir.name:
        raise ValueError("candidate directory does not match manifest snapshot_directory")
    build_evidence = manifest.get("build_evidence")
    if not isinstance(build_evidence, dict):
        raise ValueError("candidate has no build evidence")
    if build_evidence.get("status") != "complete":
        raise ValueError("candidate build evidence is not complete")
    if build_evidence.get("quality_status") != "pass":
        raise ValueError("candidate build quality is not passing")
    if build_evidence.get("contract_version") != contract.get("contract_version"):
        raise ValueError("candidate build evidence contract version does not match")
    if build_evidence.get("database_snapshot_at") != manifest.get("database_snapshot_at"):
        raise ValueError("candidate build evidence is not bound to the database snapshot")
    if not isinstance(build_evidence.get("build_id"), str) or not build_evidence["build_id"]:
        raise ValueError("candidate build evidence has no build ID")
    if not isinstance(build_evidence.get("source_frontiers"), list) or not build_evidence["source_frontiers"]:
        raise ValueError("candidate build evidence has no source frontiers")
    contract_bytes, contract_digest = _hash_file(contract_path)
    if build_evidence.get("contract_sha256") != contract_digest:
        raise ValueError("candidate build evidence contract digest differs from bundled contract")
    if build_evidence.get("contract_bytes") != contract_bytes:
        raise ValueError("candidate build evidence contract size differs from bundled contract")
    source_bundle = build_evidence.get("source_bundle")
    if require_supplemental:
        if not isinstance(source_bundle, dict) or source_bundle.get("schema_version") != 1:
            raise ValueError("supplemental candidate has no supported source bundle evidence")
        if source_bundle.get("reproduction_level") != "metadata-bound; raw payload bundle not included":
            raise ValueError("supplemental candidate source bundle has an unsupported reproduction level")
        if not isinstance(source_bundle.get("source_allowlist"), list) or not source_bundle["source_allowlist"]:
            raise ValueError("supplemental candidate source bundle has no source allowlist")
        if not isinstance(source_bundle.get("source_tree_sha256"), str) or not source_bundle["source_tree_sha256"]:
            raise ValueError("supplemental candidate source bundle has no source digest")
        lock = source_bundle.get("dependency_lock")
        if not isinstance(lock, dict) or not lock.get("path") or not lock.get("sha256"):
            raise ValueError("supplemental candidate source bundle has no dependency lock digest")
        dependencies = source_bundle.get("local_dependencies")
        if not isinstance(dependencies, list):
            raise ValueError("supplemental candidate source bundle has no dependency revision list")
        if not isinstance(build_evidence.get("source_bundle_sha256"), str):
            raise ValueError("supplemental candidate has no source bundle digest")
    if source_bundle is not None:
        if build_evidence.get("source_bundle_sha256") != _json_digest(source_bundle):
            raise ValueError("candidate source bundle digest differs from its evidence")
    data_validation = build_evidence.get("data_validation")
    if not isinstance(data_validation, dict) or data_validation.get("valid") is not True:
        raise ValueError("candidate row-level data validation is not passing")
    checks = data_validation.get("checks")
    if (
        not isinstance(checks, list)
        or not checks
        or data_validation.get("check_count") != len(checks)
        or data_validation.get("failure_count") != 0
        or data_validation.get("failures")
    ):
        raise ValueError("candidate row-level data validation is incomplete or non-vacuous")
    provenance_path = candidate_dir / "provenance.json"
    if not provenance_path.is_file() or provenance_path.is_symlink():
        raise ValueError("candidate provenance sidecar is missing or symlinked")
    try:
        provenance = json.loads(provenance_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise ValueError("candidate provenance sidecar is not valid JSON") from error
    if not isinstance(provenance, dict) or provenance.get("provenance_schema_version") != 1:
        raise ValueError("candidate provenance sidecar has an unsupported schema")
    for field in (
        "product",
        "schema",
        "contract_kind",
        "contract_version",
        "snapshot_directory",
        "generated_at",
        "database_snapshot_at",
        "scope",
    ):
        if provenance.get(field) != manifest.get(field):
            raise ValueError(f"candidate provenance sidecar does not match manifest field {field}")
    if "serialization" in manifest and provenance.get("serialization") != serialization:
        raise ValueError("candidate provenance sidecar does not match serialization metadata")
    if "source_evidence" in manifest and provenance.get("source_evidence") != source_evidence:
        raise ValueError("candidate provenance sidecar does not match source evidence metadata")
    _, manifest_sha256 = _hash_file(manifest_path)
    if provenance.get("manifest_sha256") != manifest_sha256:
        raise ValueError("candidate provenance sidecar manifest digest differs")
    provenance_evidence = provenance.get("build_evidence")
    if not isinstance(provenance_evidence, dict):
        raise ValueError("candidate provenance sidecar has no build evidence")
    for field in (
        "build_id",
        "status",
        "quality_status",
        "contract_sha256",
        "pipeline_sha256",
        "source_bundle_sha256",
        "database_snapshot_at",
        "relation_counts",
        "source_frontiers",
        "source_bundle",
        "quality",
    ):
        if provenance_evidence.get(field) != build_evidence.get(field):
            raise ValueError(f"candidate provenance sidecar does not match build evidence field {field}")
    provenance_validation = provenance_evidence.get("data_validation")
    if not isinstance(provenance_validation, dict):
        raise ValueError("candidate provenance sidecar has no validation summary")
    for field in ("valid", "check_count", "failure_count"):
        if provenance_validation.get(field) != data_validation.get(field):
            raise ValueError(f"candidate provenance validation summary does not match {field}")
    expected_files = {"manifest.json", "provenance.json"}
    if source_evidence_root is not None:
        expected_files.add(source_evidence_root)
    verified: list[dict[str, Any]] = []
    relation_by_name = {str(relation["table"]): relation for relation in relations}
    evidence_counts = build_evidence.get("relation_counts")
    expected_counts = [
        {"relation": name, "row_count": int(entries[name].get("row_count", -1))}
        for name in expected_names
    ]
    if evidence_counts != expected_counts:
        raise ValueError("candidate build evidence relation counts do not match manifest")
    for name in expected_names:
        entry = entries[name]
        if entry.get("relation") != name or entry.get("format") != "csv.gz":
            raise ValueError(f"{name}: manifest relation metadata is inconsistent")
        file_name = entry.get("file")
        if file_name != f"{name}.csv.gz" or Path(str(file_name)).name != file_name:
            raise ValueError(f"{name}: unsafe or unexpected artifact filename {file_name!r}")
        expected_files.add(str(file_name))
        file_path = candidate_dir / str(file_name)
        if not file_path.is_file() or file_path.is_symlink():
            raise ValueError(f"{name}: artifact file is missing or symlinked")
        size, digest = _hash_file(file_path)
        if size != entry.get("bytes") or digest != entry.get("sha256"):
            raise ValueError(f"{name}: artifact bytes differ from manifest")
        header, row_count = _count_rows_and_header(file_path)
        expected_header = list(relation_by_name[name]["columns"])
        if header != expected_header:
            raise ValueError(f"{name}: CSV header differs from contract")
        if entry.get("contract_columns") != relation_by_name[name]["columns"]:
            raise ValueError(f"{name}: manifest contract columns differ from contract")
        expected_rows = entry.get("row_count")
        if not isinstance(expected_rows, int) or row_count != expected_rows:
            raise ValueError(f"{name}: row count differs from manifest")
        verified.append({"name": name, "row_count": row_count, "bytes": size, "sha256": digest})
    actual_files = {path.name for path in candidate_dir.iterdir()}
    extras = sorted(actual_files - expected_files)
    if extras:
        raise ValueError(f"candidate contains undeclared files: {extras}")
    return {
        "valid": True,
        "candidate": str(manifest_path.resolve()),
        "candidate_directory": str(candidate_dir),
        "contract_version": contract["contract_version"],
        "relation_count": len(expected_names),
        "verified_files": verified,
    }


def promote_candidate(
    manifest_path: Path,
    *,
    contract_path: Path = DEFAULT_CONTRACT,
    output_dir: Path | None = None,
    include_supplemental: bool = False,
    require_source_evidence: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Validate and, unless dry-run, atomically update the ``latest`` link."""

    root = (output_dir or manifest_path.expanduser().parent.parent).expanduser()
    with _promotion_lock(root):
        report = validate_candidate(
            manifest_path,
            contract_path=contract_path,
            output_dir=root,
            include_supplemental=include_supplemental,
            require_source_evidence=require_source_evidence,
        )
        if dry_run:
            report["promoted"] = False
            return report
        latest = root / "latest"
        if latest.exists() and not latest.is_symlink():
            raise ValueError("latest must be a symlink before promotion")
        temporary = root / f".latest-{os.getpid()}"
        if temporary.exists() or temporary.is_symlink():
            temporary.unlink()
        temporary.symlink_to(Path(report["candidate_directory"]).name, target_is_directory=True)
        temporary.replace(latest)
        report["promoted"] = True
        report["latest"] = str(latest)
        return report


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", type=Path, required=True, help="Exact candidate manifest.json to validate/promote.")
    parser.add_argument("--output-dir", type=Path, default=Path("../reports/exports"))
    parser.add_argument("--contract", type=Path, default=DEFAULT_CONTRACT)
    parser.add_argument("--include-supplemental", action="store_true")
    parser.add_argument("--require-source-evidence", action="store_true")
    parser.add_argument("--dry-run", action="store_true", help="Validate without changing latest.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        report = promote_candidate(
            args.candidate,
            contract_path=args.contract,
            output_dir=args.output_dir,
            include_supplemental=args.include_supplemental,
            require_source_evidence=args.require_source_evidence,
            dry_run=args.dry_run,
        )
    except (OSError, ValueError, KeyError, json.JSONDecodeError) as error:
        print(json.dumps({"valid": False, "error": str(error)}, indent=2, sort_keys=True), file=sys.stderr)
        return 1
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
