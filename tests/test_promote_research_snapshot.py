from __future__ import annotations

import gzip
import hashlib
import importlib.util
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest
import yaml



SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "promote_research_snapshot.py"
SPEC = importlib.util.spec_from_file_location("promote_research_snapshot", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)

SOURCE_SPEC = importlib.util.spec_from_file_location(
    "export_source_evidence_for_promotion_test",
    Path(__file__).resolve().parents[1] / "scripts" / "export_source_evidence.py",
)
assert SOURCE_SPEC is not None and SOURCE_SPEC.loader is not None
SOURCE_MODULE = importlib.util.module_from_spec(SOURCE_SPEC)
sys.modules[SOURCE_SPEC.name] = SOURCE_MODULE
SOURCE_SPEC.loader.exec_module(SOURCE_MODULE)


def _contract(path: Path) -> None:
    path.write_text(
        yaml.safe_dump(
            {
                "contract_version": 1,
                "contract_kind": "public_product_schema",
                "product": {"name": "bicam", "postgres_schema": "bicam"},
                "families": [
                    {
                        "table": "bills",
                        "feature_status": "core-now",
                        "columns": {"bill_id": "text", "title": "text"},
                        "primary_key": ["bill_id"],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )


def _candidate(root: Path, contract_path: Path, *, body: str = "b1,One\n") -> Path:
    candidate_dir = root / "20260905T000000Z"
    candidate_dir.mkdir(parents=True)
    artifact = candidate_dir / "bills.csv.gz"
    with gzip.open(artifact, "wt", encoding="utf-8", newline="") as stream:
        stream.write("bill_id,title\n")
        stream.write(body)
    payload = artifact.read_bytes()
    contract_payload = contract_path.read_bytes()
    contract_digest = hashlib.sha256(contract_payload).hexdigest()
    manifest = {
        "schema_version": 2,
        "contract_version": 1,
        "contract_kind": "public_product_schema",
        "product": "bicam",
        "schema": "bicam",
        "database_snapshot_at": "2026-09-05T00:00:00+00:00",
        "snapshot_directory": candidate_dir.name,
        "scope": {
            "feature_status": "core-now",
            "relation_count": 1,
            "tables": [{"name": "bills", "relation": "bills"}],
        },
        "tables": [
            {
                "name": "bills",
                "relation": "bills",
                "relation_kind": "r",
                "file": "bills.csv.gz",
                "format": "csv.gz",
                "row_count": 1,
                "bytes": len(payload),
                "sha256": hashlib.sha256(payload).hexdigest(),
                "contract_columns": {"bill_id": "text", "title": "text"},
            }
        ],
        "build_evidence": {
            "build_id": "build-1",
            "status": "complete",
            "quality_status": "pass",
            "contract_version": 1,
            "contract_sha256": contract_digest,
            "contract_bytes": len(contract_payload),
            "database_snapshot_at": "2026-09-05T00:00:00+00:00",
            "source_frontiers": [{"source": "fixture", "category": "bills"}],
            "relation_counts": [{"relation": "bills", "row_count": 1}],
            "data_validation": {
                "valid": True,
                "checks": [{"check_id": "fixture", "valid": True}],
                "failures": [],
                "check_count": 1,
                "failure_count": 0,
            },
        },
    }
    manifest_path = candidate_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    manifest_sha256 = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    evidence = manifest["build_evidence"]
    (candidate_dir / "provenance.json").write_text(
        json.dumps(
            {
                "provenance_schema_version": 1,
                "product": manifest["product"],
                "schema": manifest["schema"],
                "contract_kind": manifest["contract_kind"],
                "contract_version": manifest["contract_version"],
                "snapshot_directory": manifest["snapshot_directory"],
                "generated_at": manifest.get("generated_at"),
                "database_snapshot_at": manifest["database_snapshot_at"],
                "manifest_sha256": manifest_sha256,
                "scope": manifest["scope"],
                "build_evidence": {
                    "build_id": evidence["build_id"],
                    "status": evidence["status"],
                    "quality_status": evidence["quality_status"],
                    "contract_sha256": evidence["contract_sha256"],
                    "pipeline_sha256": evidence.get("pipeline_sha256"),
                    "database_snapshot_at": evidence["database_snapshot_at"],
                    "relation_counts": evidence["relation_counts"],
                    "source_frontiers": evidence["source_frontiers"],
                    "quality": evidence.get("quality"),
                    "data_validation": {
                        "valid": evidence["data_validation"]["valid"],
                        "check_count": evidence["data_validation"]["check_count"],
                        "failure_count": evidence["data_validation"]["failure_count"],
                    },
                },
            }
        ),
        encoding="utf-8",
    )
    return manifest_path


def test_validate_candidate_checks_contract_inventory_and_bytes(tmp_path: Path) -> None:
    contract_path = tmp_path / "contract.yaml"
    _contract(contract_path)
    manifest = _candidate(tmp_path / "exports", contract_path)

    report = MODULE.validate_candidate(manifest, contract_path=contract_path, output_dir=tmp_path / "exports")

    assert report["valid"] is True
    assert report["relation_count"] == 1
    assert report["verified_files"][0]["row_count"] == 1


def test_candidate_without_bound_build_evidence_cannot_promote(tmp_path: Path) -> None:
    contract_path = tmp_path / "contract.yaml"
    _contract(contract_path)
    root = tmp_path / "exports"
    candidate = _candidate(root, contract_path)
    manifest = json.loads(candidate.read_text(encoding="utf-8"))
    del manifest["build_evidence"]
    candidate.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(ValueError, match="build evidence"):
        MODULE.validate_candidate(candidate, contract_path=contract_path, output_dir=root)


def test_research_candidate_can_require_embedded_source_evidence(tmp_path: Path) -> None:
    contract_path = tmp_path / "contract.yaml"
    _contract(contract_path)
    root = tmp_path / "exports"
    candidate = _candidate(root, contract_path)

    with pytest.raises(ValueError, match="source evidence is required"):
        MODULE.validate_candidate(
            candidate,
            contract_path=contract_path,
            output_dir=root,
            require_source_evidence=True,
        )


def _candidate_with_source_evidence(tmp_path: Path, *, empty: bool = False) -> tuple[Path, Path, Path]:
    contract_path = tmp_path / "contract.yaml"
    _contract(contract_path)
    root = tmp_path / "exports"
    candidate = _candidate(root, contract_path)
    source_bundle = SOURCE_MODULE.write_bundle(
        [] if empty else [
            (
                "version-1",
                "raw-1",
                "https://api.example.test/bill/119/hr/1?api_key=secret",
                "congress",
                "bills",
                "bill_detail",
                None,
                "hash-1",
                datetime(2026, 9, 5, tzinfo=timezone.utc),
                datetime(2026, 9, 5, tzinfo=timezone.utc),
                "crawl-1",
                "crawl-1",
                {"bill": {"number": 1}},
            )
        ],
        candidate.parent / "source-evidence",
        selection={"limit": 1},
        database_snapshot_at=datetime(2026, 9, 5, tzinfo=timezone.utc),
        generated_at=datetime(2026, 9, 5, 1, tzinfo=timezone.utc),
    )
    bundle_path = Path(source_bundle["bundle_dir"]).relative_to(candidate.parent).as_posix()
    manifest = json.loads(candidate.read_text(encoding="utf-8"))
    manifest["source_evidence"] = {
        "schema_version": 1,
        "bundle_path": bundle_path,
        "manifest_sha256": source_bundle["manifest_sha256"],
        "database_snapshot_at": "2026-09-05T00:00:00+00:00",
        "observation_count": source_bundle["observation_count"],
        "unique_payload_count": source_bundle["unique_payload_count"],
        "selection": {"limit": 1},
    }
    candidate.write_text(json.dumps(manifest), encoding="utf-8")
    sidecar = json.loads((candidate.parent / "provenance.json").read_text(encoding="utf-8"))
    sidecar["manifest_sha256"] = hashlib.sha256(candidate.read_bytes()).hexdigest()
    sidecar["source_evidence"] = manifest["source_evidence"]
    (candidate.parent / "provenance.json").write_text(json.dumps(sidecar), encoding="utf-8")
    return candidate, contract_path, root


def test_embedded_source_evidence_is_verified_and_snapshot_bound(tmp_path: Path) -> None:
    candidate, contract_path, root = _candidate_with_source_evidence(tmp_path)

    report = MODULE.validate_candidate(
        candidate,
        contract_path=contract_path,
        output_dir=root,
        require_source_evidence=True,
    )

    assert report["valid"] is True


@pytest.mark.parametrize("field,value,error", [
    ("unique_payload_count", 2, "unique_payload_count differs"),
    ("observation_count", True, "observation_count differs"),
    ("selection", {"limit": 100}, "selection differs"),
    ("bundle_path", ".", "must be source-evidence"),
    ("bundle_path", "source-evidence/./20260905T010000Z", "must be source-evidence"),
])
def test_embedded_source_metadata_must_match_bundle(tmp_path: Path, field, value, error) -> None:
    candidate, contract_path, root = _candidate_with_source_evidence(tmp_path)
    manifest = json.loads(candidate.read_text())
    manifest["source_evidence"][field] = value
    candidate.write_text(json.dumps(manifest))
    sidecar_path = candidate.parent / "provenance.json"
    sidecar = json.loads(sidecar_path.read_text())
    sidecar["manifest_sha256"] = hashlib.sha256(candidate.read_bytes()).hexdigest()
    sidecar["source_evidence"] = manifest["source_evidence"]
    sidecar_path.write_text(json.dumps(sidecar))
    with pytest.raises(ValueError, match=error):
        MODULE.validate_candidate(candidate, contract_path=contract_path, output_dir=root)


def test_embedded_source_evidence_rejects_undeclared_sibling(tmp_path: Path) -> None:
    candidate, contract_path, root = _candidate_with_source_evidence(tmp_path)
    (candidate.parent / "source-evidence" / "unrelated-bundle").mkdir()
    with pytest.raises(ValueError, match="contains undeclared entries"):
        MODULE.validate_candidate(candidate, contract_path=contract_path, output_dir=root)


def test_embedded_source_evidence_rejects_symlinked_parent(tmp_path: Path) -> None:
    candidate, contract_path, root = _candidate_with_source_evidence(tmp_path)
    directory = candidate.parent / "source-evidence"
    target = tmp_path / "external-evidence"
    directory.rename(target)
    directory.symlink_to(target, target_is_directory=True)
    with pytest.raises(ValueError, match="directory is missing or symlinked"):
        MODULE.validate_candidate(candidate, contract_path=contract_path, output_dir=root)


def test_required_source_evidence_cannot_be_empty(tmp_path: Path) -> None:
    candidate, contract_path, root = _candidate_with_source_evidence(tmp_path, empty=True)
    assert MODULE.validate_candidate(candidate, contract_path=contract_path, output_dir=root)["valid"] is True
    with pytest.raises(ValueError, match="required source evidence is empty"):
        MODULE.validate_candidate(candidate, contract_path=contract_path, output_dir=root, require_source_evidence=True)


def test_missing_snapshots_cannot_satisfy_source_binding(tmp_path: Path) -> None:
    candidate, contract_path, root = _candidate_with_source_evidence(tmp_path)
    manifest = json.loads(candidate.read_text())
    manifest["database_snapshot_at"] = None
    manifest["source_evidence"]["database_snapshot_at"] = None
    evidence_dir = candidate.parent / manifest["source_evidence"]["bundle_path"]
    evidence_path = evidence_dir / "manifest.json"
    evidence = json.loads(evidence_path.read_text())
    evidence["database_snapshot_at"] = None
    evidence_path.write_text(json.dumps(evidence))
    digest = hashlib.sha256(evidence_path.read_bytes()).hexdigest()
    (evidence_dir / "manifest.sha256").write_text(f"{digest}  manifest.json\n")
    manifest["source_evidence"]["manifest_sha256"] = digest
    candidate.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="not bound to the database snapshot"):
        MODULE.validate_candidate(candidate, contract_path=contract_path, output_dir=root)


def test_vacuous_row_validation_cannot_promote(tmp_path: Path) -> None:
    contract_path = tmp_path / "contract.yaml"
    _contract(contract_path)
    root = tmp_path / "exports"
    candidate = _candidate(root, contract_path)
    manifest = json.loads(candidate.read_text(encoding="utf-8"))
    manifest["build_evidence"]["data_validation"] = {
        "valid": True,
        "checks": [],
        "failures": [],
        "check_count": 0,
        "failure_count": 0,
    }
    candidate.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(ValueError, match="non-vacuous"):
        MODULE.validate_candidate(candidate, contract_path=contract_path, output_dir=root)


def test_dry_run_does_not_change_existing_latest(tmp_path: Path) -> None:
    contract_path = tmp_path / "contract.yaml"
    _contract(contract_path)
    root = tmp_path / "exports"
    candidate = _candidate(root, contract_path)
    old = root / "old"
    old.mkdir()
    (old / "marker").write_text("old", encoding="utf-8")
    (root / "latest").symlink_to("old", target_is_directory=True)

    report = MODULE.promote_candidate(
        candidate,
        contract_path=contract_path,
        output_dir=root,
        dry_run=True,
    )

    assert report["promoted"] is False
    assert (root / "latest").resolve() == old.resolve()


def test_promotion_atomically_selects_validated_candidate(tmp_path: Path) -> None:
    contract_path = tmp_path / "contract.yaml"
    _contract(contract_path)
    root = tmp_path / "exports"
    candidate = _candidate(root, contract_path)

    report = MODULE.promote_candidate(candidate, contract_path=contract_path, output_dir=root)

    assert report["promoted"] is True
    assert (root / "latest").is_symlink()
    assert (root / "latest").resolve() == candidate.parent.resolve()


def test_invalid_candidate_leaves_previous_latest_untouched(tmp_path: Path) -> None:
    contract_path = tmp_path / "contract.yaml"
    _contract(contract_path)
    root = tmp_path / "exports"
    candidate = _candidate(root, contract_path)
    old = root / "old"
    old.mkdir()
    (old / "marker").write_text("old", encoding="utf-8")
    (root / "latest").symlink_to("old", target_is_directory=True)
    with gzip.open(candidate.parent / "bills.csv.gz", "wt", encoding="utf-8", newline="") as stream:
        stream.write("title,bill_id\nOne,b1\n")
    manifest_data = json.loads(candidate.read_text(encoding="utf-8"))
    payload = (candidate.parent / "bills.csv.gz").read_bytes()
    manifest_data["tables"][0]["bytes"] = len(payload)
    manifest_data["tables"][0]["sha256"] = hashlib.sha256(payload).hexdigest()
    candidate.write_text(json.dumps(manifest_data), encoding="utf-8")
    sidecar = json.loads((candidate.parent / "provenance.json").read_text(encoding="utf-8"))
    sidecar["manifest_sha256"] = hashlib.sha256(candidate.read_bytes()).hexdigest()
    (candidate.parent / "provenance.json").write_text(json.dumps(sidecar), encoding="utf-8")

    with pytest.raises(ValueError, match="CSV header"):
        MODULE.promote_candidate(candidate, contract_path=contract_path, output_dir=root)

    assert (root / "latest").resolve() == old.resolve()


def test_hash_drift_and_partial_inventory_are_rejected(tmp_path: Path) -> None:
    contract_path = tmp_path / "contract.yaml"
    _contract(contract_path)
    root = tmp_path / "exports"
    candidate = _candidate(root, contract_path)
    with (candidate.parent / "bills.csv.gz").open("ab") as stream:
        stream.write(b"drift")

    with pytest.raises(ValueError, match="bytes differ"):
        MODULE.validate_candidate(candidate, contract_path=contract_path, output_dir=root)

    two_relation_contract = tmp_path / "two-relation.yaml"
    document = yaml.safe_load(contract_path.read_text(encoding="utf-8"))
    document["families"].append(
        {
            "table": "members",
            "feature_status": "core-now",
            "columns": {"bioguide_id": "text"},
            "primary_key": ["bioguide_id"],
        }
    )
    two_relation_contract.write_text(yaml.safe_dump(document), encoding="utf-8")
    with pytest.raises(ValueError, match="scope"):
        MODULE.validate_candidate(candidate, contract_path=two_relation_contract, output_dir=root)


def test_provenance_sidecar_drift_is_rejected(tmp_path: Path) -> None:
    contract_path = tmp_path / "contract.yaml"
    _contract(contract_path)
    root = tmp_path / "exports"
    candidate = _candidate(root, contract_path)
    sidecar = candidate.parent / "provenance.json"
    payload = json.loads(sidecar.read_text(encoding="utf-8"))
    payload["manifest_sha256"] = "0" * 64
    sidecar.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="provenance sidecar manifest digest"):
        MODULE.validate_candidate(candidate, contract_path=contract_path, output_dir=root)


def test_source_bundle_digest_drift_is_rejected(tmp_path: Path) -> None:
    contract_path = tmp_path / "contract.yaml"
    _contract(contract_path)
    root = tmp_path / "exports"
    candidate = _candidate(root, contract_path)
    manifest = json.loads(candidate.read_text(encoding="utf-8"))
    manifest["build_evidence"]["source_bundle"] = {
        "schema_version": 1,
        "source_allowlist": ["src"],
    }
    manifest["build_evidence"]["source_bundle_sha256"] = "0" * 64
    candidate.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(ValueError, match="source bundle digest"):
        MODULE.validate_candidate(candidate, contract_path=contract_path, output_dir=root)


def test_missing_provenance_sidecar_is_rejected(tmp_path: Path) -> None:
    contract_path = tmp_path / "contract.yaml"
    _contract(contract_path)
    root = tmp_path / "exports"
    candidate = _candidate(root, contract_path)
    (candidate.parent / "provenance.json").unlink()

    with pytest.raises(ValueError, match="provenance sidecar is missing"):
        MODULE.validate_candidate(candidate, contract_path=contract_path, output_dir=root)


def test_symlinked_candidate_directory_is_rejected(tmp_path: Path) -> None:
    contract_path = tmp_path / "contract.yaml"
    _contract(contract_path)
    root = tmp_path / "exports"
    real = tmp_path / "real"
    real.mkdir()
    candidate = _candidate(tmp_path, contract_path)
    candidate_dir = candidate.parent
    candidate_dir.rename(real / candidate_dir.name)
    (root).mkdir()
    (root / candidate_dir.name).symlink_to(real / candidate_dir.name, target_is_directory=True)

    with pytest.raises(ValueError, match="symlink"):
        MODULE.validate_candidate(root / candidate_dir.name / "manifest.json", contract_path=contract_path, output_dir=root)
