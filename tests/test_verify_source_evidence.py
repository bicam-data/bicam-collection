from __future__ import annotations

import importlib.util
import sys
from datetime import datetime, timezone
from pathlib import Path


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
    payload = next((bundle / "payloads").glob("*.json"))
    payload.write_text('{"bill":"changed"}\n', encoding="utf-8")
    try:
        verifier.verify_bundle(bundle)
    except ValueError as error:
        assert "payload bytes differ" in str(error)
    else:
        raise AssertionError("tampered source evidence unexpectedly verified")
