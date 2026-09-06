from __future__ import annotations

import importlib.util
from pathlib import Path
import sys


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "backfill_congress_law_details.py"
SPEC = importlib.util.spec_from_file_location("backfill_congress_law_details", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
module = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = module
SPEC.loader.exec_module(module)


def test_law_type_code_normalizes_documented_values() -> None:
    assert module.law_type_code("Public Law") == "pub"
    assert module.law_type_code("private law") == "priv"
    assert module.law_type_code("bill") is None


def test_law_detail_url_uses_law_number_not_bill_number() -> None:
    assert module.law_detail_url(
        congress=119,
        law_type="Public Law",
        law_number="119-37",
    ) == "https://api.congress.gov/v3/law/119/pub/37?format=json"


def test_law_detail_url_rejects_malformed_identity() -> None:
    assert module.law_detail_url(congress=119, law_type="Public Law", law_number="unknown") is None
    assert module.law_detail_url(congress=0, law_type="Public Law", law_number="119-37") is None
