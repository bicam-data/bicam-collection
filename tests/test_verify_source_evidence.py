from __future__ import annotations

import importlib.util
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXPORT_SCRIPT = PROJECT_ROOT / "scripts" / "export_source_evidence.py"
VERIFY_SCRIPT = PROJECT_ROOT / "scripts" / "verify_source_evidence.py"


def _load(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _row(payload: object) -> tuple[object, ...]:
    return (
        "version-1", "raw-1", "https://api.example.test/bill/1", "congress", "bills",
        "bill_detail", None, "source-hash", datetime(2026, 9, 1, tzinfo=timezone.utc),
        datetime(2026, 9, 2, tzinfo=timezone.utc), None, None, payload,
    )


def test_verify_bundle_accepts_export_and_rejects_tamper(tmp_path: Path) -> None:
    exporter = _load(EXPORT_SCRIPT, "export_source_evidence_for_verify")
    verifier = _load(VERIFY_SCRIPT, "verify_source_evidence")
    result = exporter.write_bundle(
        [_row({"bill": "hr1"})],
        tmp_path,
        selection={"limit": 1},
        generated_at=datetime(2026, 9, 6, 8, 30, tzinfo=timezone.utc),
    )
    bundle = Path(result["bundle_dir"])
    report = verifier.verify_bundle(bundle)
    assert report["valid"] is True
    assert "not public-row reproduction" in report["verification_scope"]
    payload = next((bundle / "payloads").glob("*.json"))
    payload.write_text('{"bill":"changed"}\n', encoding="utf-8")
    try:
        verifier.verify_bundle(bundle)
    except ValueError as error:
        assert "payload bytes differ" in str(error)
    else:
        raise AssertionError("tampered source evidence unexpectedly verified")


@pytest.fixture
def evidence(tmp_path: Path):
    exporter = _load(EXPORT_SCRIPT, "export_source_evidence_for_negative_tests")
    verifier = _load(VERIFY_SCRIPT, "verify_source_evidence_negative_tests")
    result = exporter.write_bundle(
        [_row({"bill": "hr1"})], tmp_path / "bundles", selection={"limit": 1},
        generated_at=datetime(2026, 9, 6, 8, 30, tzinfo=timezone.utc),
    )
    return verifier, Path(result["bundle_dir"])


def _rewrite_manifest(bundle: Path, manifest: dict) -> None:
    # Rehash the sidecar so these test semantics, not just byte tampering.
    path = bundle / "manifest.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    (bundle / "manifest.sha256").write_text(f"{digest}  manifest.json\n", encoding="ascii")


@pytest.mark.parametrize("field,value", [
    ("file", "payloads/different.json"), ("bytes", 999), ("sha256", []),
])
def test_observation_reference_must_match_inventory(evidence, field, value) -> None:
    verifier, bundle = evidence
    manifest = json.loads((bundle / "manifest.json").read_text())
    manifest["observations"][0]["payload"][field] = value
    _rewrite_manifest(bundle, manifest)
    with pytest.raises(ValueError, match="payload"):
        verifier.verify_bundle(bundle)


@pytest.mark.parametrize("field,value,error", [
    ("observation_count", True, "observation_count"),
    ("unique_payload_count", 2, "unique_payload_count"),
    ("schema_version", True, "schema"),
    ("selection", [], "selection"),
    ("selection", {"limit": 0}, "limit"),
    ("selection", {"limit": True}, "limit"),
    ("selection", {"category": "hearings"}, "selection category"),
    ("generated_at", "2026-09-06", "timezone-aware"),
    ("database_snapshot_at", "not a timestamp", "timezone-aware"),
    ("reproduction_level", "complete historical reconstruction", "reproduction level"),
])
def test_manifest_metadata_is_validated(evidence, field, value, error) -> None:
    verifier, bundle = evidence
    manifest = json.loads((bundle / "manifest.json").read_text())
    manifest[field] = value
    _rewrite_manifest(bundle, manifest)
    with pytest.raises(ValueError, match=error):
        verifier.verify_bundle(bundle)


@pytest.mark.parametrize("field,value,error", [
    ("raw_response_version_id", None, "raw_response_version_id"),
    ("raw_response_id", "", "raw_response_id"),
    ("endpoint", " ", "endpoint"),
    ("source", None, "source"),
    ("first_seen_at", "2026-09-03T00:00:00+00:00", "reversed"),
    ("last_seen_at", "2026-09-02T00:00:00", "timezone-aware"),
])
def test_observation_identity_and_times_are_required(evidence, field, value, error) -> None:
    verifier, bundle = evidence
    manifest = json.loads((bundle / "manifest.json").read_text())
    manifest["observations"][0][field] = value
    _rewrite_manifest(bundle, manifest)
    with pytest.raises(ValueError, match=error):
        verifier.verify_bundle(bundle)


def test_duplicate_version_cannot_inflate_evidence_count(evidence) -> None:
    verifier, bundle = evidence
    manifest = json.loads((bundle / "manifest.json").read_text())
    manifest["observations"].append(manifest["observations"][0].copy())
    manifest["observation_count"] = 2
    manifest["selection"]["limit"] = 2
    _rewrite_manifest(bundle, manifest)
    with pytest.raises(ValueError, match="duplicates a raw version ID"):
        verifier.verify_bundle(bundle)


@pytest.mark.parametrize("relative", ["extra", "payloads/extra"])
def test_empty_undeclared_directories_are_rejected(evidence, relative) -> None:
    verifier, bundle = evidence
    (bundle / relative).mkdir()
    with pytest.raises(ValueError, match="undeclared"):
        verifier.verify_bundle(bundle)


def test_payload_directory_symlink_is_rejected(evidence, tmp_path: Path) -> None:
    verifier, bundle = evidence
    destination = tmp_path / "external-payloads"
    (bundle / "payloads").rename(destination)
    (bundle / "payloads").symlink_to(destination, target_is_directory=True)
    with pytest.raises(ValueError, match="symlinked"):
        verifier.verify_bundle(bundle)


def test_duplicate_json_keys_are_rejected(evidence) -> None:
    verifier, bundle = evidence
    path = bundle / "manifest.json"
    contents = path.read_text()
    path.write_text(contents.replace('"schema_version": 1', '"schema_version": 2, "schema_version": 1'))
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    (bundle / "manifest.sha256").write_text(f"{digest}  manifest.json\n")
    with pytest.raises(ValueError, match="repeats JSON key"):
        verifier.verify_bundle(bundle)


def test_distinct_versions_can_share_one_payload(tmp_path: Path) -> None:
    exporter = _load(EXPORT_SCRIPT, "export_source_evidence_shared")
    verifier = _load(VERIFY_SCRIPT, "verify_source_evidence_shared")
    first = _row({"bill": "hr1"})
    second = ("version-2", *first[1:])
    result = exporter.write_bundle([first, second], tmp_path, selection={"limit": 2})
    report = verifier.verify_bundle(Path(result["bundle_dir"]))
    assert report["observation_count"] == 2
    assert report["unique_payload_count"] == 1
