import gzip
import hashlib
import importlib.util
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest
import yaml

pytest.importorskip("pyarrow")
spec = importlib.util.spec_from_file_location("updated_audit", Path(__file__).parents[1] / "scripts/audit_updated_export_readiness.py")
audit_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(audit_module)


@pytest.fixture
def fixture(tmp_path):
    p = tmp_path / "bills.csv.gz"
    with gzip.open(p, "wt") as f:
        f.write('bill_id,updated_at,chamber\nhr1-119,2025-01-01T00:00:00Z,House\nhr2-119,\\N,UNKNOWN\n')
    contract = {"families": [{"name": "bills", "table": "bills", "feature_status": "core-now",
        "columns": {"bill_id": "text", "updated_at": "timestamptz", "chamber": "text"}}]}
    c = tmp_path / "contract.yaml"
    c.write_text(yaml.safe_dump(contract, sort_keys=False))
    m = {"serialization": {"csv_null": r"\N"}, "tables": [{"name": "bills", "file": p.name,
        "row_count": 2, "sha256": hashlib.sha256(p.read_bytes()).hexdigest()}]}
    (tmp_path / "manifest.json").write_text(json.dumps(m))
    return tmp_path, c


def test_audit_distinguishes_missing_date_and_unknown_domain(fixture):
    root, contract = fixture
    r = audit_module.audit(root, contract, datetime(2025, 1, 1, tzinfo=timezone.utc))
    t = r["tables"][0]
    assert t["timestamp_classification"] == {"qualifies": 1, "unknown_timestamp": 1}
    assert t["proposed_normalizations"] == [
        {"column": "chamber", "from": "House", "to": "house", "rows": 1},
        {"column": "chamber", "from": "UNKNOWN", "to": "unknown", "rows": 1},
    ]
    assert t["unresolved_values"][0]["value"] == "UNKNOWN"
    assert r["export_ready"] is False


def test_changed_frozen_file_is_rejected(fixture):
    root, contract = fixture
    (root / "bills.csv.gz").write_bytes(b"changed")
    with pytest.raises(ValueError, match="source path/hash mismatch"):
        audit_module.audit(root, contract, datetime(2025, 1, 1, tzinfo=timezone.utc))
