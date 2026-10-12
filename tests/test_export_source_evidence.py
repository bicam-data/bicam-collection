from __future__ import annotations

import importlib.util
import json
import sys
from datetime import datetime, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = PROJECT_ROOT / "scripts" / "export_source_evidence.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("export_source_evidence", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _row(version: str, endpoint: str, payload: object) -> tuple[object, ...]:
    return (
        version,
        "raw-1",
        endpoint,
        "congress",
        "bills",
        "bill_detail",
        None,
        "source-hash",
        datetime(2026, 9, 1, tzinfo=timezone.utc),
        datetime(2026, 9, 2, tzinfo=timezone.utc),
        "crawl-1",
        "crawl-2",
        payload,
    )


def test_sanitize_endpoint_redacts_credentials_and_keeps_identity() -> None:
    script = _load_module()
    sanitized = script.sanitize_endpoint(
        "https://api.congress.gov/v3/bill/119/hr/1?api_key=secret&format=json&offset=0"
    )
    assert "secret" not in sanitized
    assert "api_key=%5Bredacted%5D" in sanitized
    assert sanitized.endswith("format=json&offset=0") or sanitized.endswith("offset=0&format=json")


def test_sanitize_endpoint_strips_userinfo_and_fragment() -> None:
    script = _load_module()
    sanitized = script.sanitize_endpoint("https://user:password@api.example.test:8443/bill/1?API_KEY=secret#token")
    assert sanitized == "https://api.example.test:8443/bill/1?API_KEY=%5Bredacted%5D"


def test_selection_endpoint_is_sanitized_without_mutating_input(tmp_path: Path) -> None:
    script = _load_module()
    selection = {"endpoint_prefix": "https://user:password@api.example.test/bill?token=secret", "limit": 1}
    manifest = script.write_bundle([], tmp_path, selection=selection)
    assert "secret" in selection["endpoint_prefix"]
    assert "secret" not in json.dumps(manifest)
    assert "password" not in json.dumps(manifest)


def test_write_bundle_deduplicates_payloads_and_is_verifiable(tmp_path: Path) -> None:
    script = _load_module()
    manifest = script.write_bundle(
        [
            _row("v1", "https://api.example.test/bill/1?api_key=secret", {"b": 2, "a": 1}),
            _row("v2", "https://api.example.test/bill/2", {"a": 1, "b": 2}),
        ],
        tmp_path,
        selection={"limit": 2, "category": "bills"},
        generated_at=datetime(2026, 9, 6, 8, 0, tzinfo=timezone.utc),
    )
    bundle = Path(manifest["bundle_dir"])
    assert manifest["observation_count"] == 2
    assert manifest["unique_payload_count"] == 1
    assert not list(tmp_path.glob(".source-evidence-*"))
    assert len(list((bundle / "payloads").glob("*.json"))) == 1
    loaded = json.loads((bundle / "manifest.json").read_text(encoding="utf-8"))
    assert loaded["observations"][0]["endpoint"].find("secret") == -1
    digest_line = (bundle / "manifest.sha256").read_text(encoding="ascii").strip()
    assert digest_line.split()[0] == manifest["manifest_sha256"]


def test_query_rows_requires_positive_bound() -> None:
    script = _load_module()
    try:
        script._query_rows(object(), limit=0, source=None, category=None, node_type=None, endpoint_prefix=None)
    except ValueError as error:
        assert "positive" in str(error)
    else:
        raise AssertionError("expected non-positive evidence limit to fail")
