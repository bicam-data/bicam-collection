from __future__ import annotations

from pathlib import Path

import yaml

from bicam_collection.examples import load_example_payload
from bicam_collection.selectors import select, select_one
from bicam_collection.spec_loader import load_spec


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SPEC = load_spec(PROJECT_ROOT / "specs" / "summaries.yaml")


def test_standalone_summary_contract_has_bounded_incremental_filters() -> None:
    raw = yaml.safe_load((PROJECT_ROOT / "specs" / "summaries.yaml").read_text(encoding="utf-8"))
    assert raw["coverage"]["scope"] == "documented_api"
    node = SPEC.nodes["summary_list"]
    assert node.incremental_query_params == ["fromDateTime", "toDateTime"]
    assert node.pagination_path == "$.pagination"
    assert node.next_page_path == "$.pagination.next"
    assert SPEC.nodes["summary_congress_list"].item_path == "$.summaries[*]"
    assert SPEC.nodes["summary_bill_type_list"].item_path == "$.summaries[*]"


def test_standalone_summary_fixture_preserves_nested_bill_identity() -> None:
    payload = load_example_payload(PROJECT_ROOT / "api_schemas/summaries/example_summaries.json")
    assert len(select(payload, SPEC.nodes["summary_list"].item_path)) == 2
    assert select_one(payload, "$.summaries[*].bill.type") == "HR"
    assert select_one(payload, "$.summaries[*].bill.number") == "3076"
    assert select_one(payload, "$.summaries[*].bill.congress") == 117
    assert select_one(payload, "$.summaries[*].versionCode") == "00"
    assert select_one(payload, SPEC.nodes["summary_list"].next_page_path)
