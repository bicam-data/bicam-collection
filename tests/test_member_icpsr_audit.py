import importlib.util
from pathlib import Path
import sys

import pytest

pytest.importorskip("pyarrow")
scripts = Path(__file__).parents[1] / "scripts"
spec = importlib.util.spec_from_file_location("member_icpsr_audit", scripts / "audit_member_icpsr_export.py")
module = importlib.util.module_from_spec(spec)
sys.path.insert(0, str(scripts))
try:
    spec.loader.exec_module(module)
finally:
    sys.path.pop(0)


def test_original_text_id_padding_and_nonintegral_rejection():
    assert module.normalize_icpsr("02605") == "02605"
    assert module.normalize_icpsr("2605.0") == "02605"
    assert module.normalize_icpsr("") is None
    with pytest.raises(ValueError):
        module.normalize_icpsr("2605.5")


def proposal(source, published=None):
    member = {"bioguide_id": "V000133", "direct_order_name": "Jefferson Van Drew", "icpsr": None}
    term = {"bioguide_id": "V000133", "congress": 119, "chamber": "house", "state_code": "NJ"}
    rows = [{"bioguide_id": "V000133", "congress": "119", "chamber": "House", "state_abbrev": "NJ", **r} for r in source]
    return module.propose([member], [term], rows, published or {})[0]


def test_multiple_legitimate_role_ids_are_not_arbitrarily_selected():
    result = proposal([{"icpsr": "21980"}, {"icpsr": "91980"}])
    assert result["status"] == "ambiguous_or_conflicting_context"
    assert "proposed_icpsr" not in result


def test_published_difference_is_visible_not_silently_replaced():
    result = proposal([{"icpsr": "91980"}], {"V000133": "21980"})
    assert result["published_comparison"] == "differs_requires_review"
    assert result["current_icpsr"] is None


def test_exact_bioguide_with_different_state_is_not_a_supported_match():
    assert proposal([{"icpsr": "21980", "state_abbrev": "NY"}])["status"] == "ambiguous_or_conflicting_context"
