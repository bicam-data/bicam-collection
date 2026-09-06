from __future__ import annotations

from pathlib import Path

import pytest

from bicam_collection.spec_loader import load_spec


DATE_FILTERED_CATEGORIES = (
    "amendments",
    "bills",
    "committee-meetings",
    "committee-prints",
    "committee-reports",
    "committees",
    "crs-reports",
    "members",
    "nominations",
    "summaries",
    "treaties",
)
PROJECT_ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("category", DATE_FILTERED_CATEGORIES)
def test_congress_list_declares_supported_incremental_date_filters(category: str) -> None:
    spec = load_spec(PROJECT_ROOT / "specs" / f"{category}.yaml")
    list_node = spec.nodes[spec.seed.node_type]

    assert list_node.incremental_query_params == ["fromDateTime", "toDateTime"]


@pytest.mark.parametrize(
    "category",
    (
        "bound-congressional-record",
        "congresses",
        "congressional-record",
        "daily-congressional-record",
        "hearings",
        "house-communications",
        "house-requirements",
        "house-votes",
        "laws",
        "senate-communications",
    ),
)
def test_unfiltered_lists_do_not_claim_incremental_date_filters(category: str) -> None:
    spec = load_spec(PROJECT_ROOT / "specs" / f"{category}.yaml")
    list_node = spec.nodes[spec.seed.node_type]

    assert list_node.incremental_query_params == []
