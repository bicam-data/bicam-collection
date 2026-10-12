import importlib.util
from pathlib import Path

import pytest

pytest.importorskip("pyarrow")
spec = importlib.util.spec_from_file_location("amendment_capture", Path(__file__).parents[1] / "scripts/capture_missing_amendment_details.py")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def test_recursive_credentials_are_not_persisted():
    raw = {"api_key": "secret", "child": [{"url": "https://example.test/?api_key=secret", "apiKey": "secret"}]}
    cleaned = module.sanitize(raw, "secret")
    assert "secret" not in str(cleaned)
    assert cleaned == {"child": [{"url": "https://example.test/?api_key=[REDACTED]"}]}


def test_exact_identity_is_required():
    module.verify_identity("hamdt230-119", {"congress": 119, "type": "HAMDT", "number": "230"})
    for obj in ({"congress": 118, "type": "HAMDT", "number": "230"},
                {"congress": 119, "type": "SAMDT", "number": "230"},
                {"congress": 119, "type": "HAMDT", "number": "231"}):
        with pytest.raises(ValueError):
            module.verify_identity("hamdt230-119", obj)
