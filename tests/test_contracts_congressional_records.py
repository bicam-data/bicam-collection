from __future__ import annotations

import unittest
from pathlib import Path

from bicam_collection.examples import load_example_payload
from bicam_collection.selectors import select, select_one
from bicam_collection.spec_loader import load_spec


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class CongressionalRecordExampleContractTests(unittest.TestCase):
    def test_daily_record_list_and_child_contracts(self) -> None:
        spec = load_spec(PROJECT_ROOT / "specs" / "daily-congressional-record.yaml")
        payload = load_example_payload(
            PROJECT_ROOT / "api_schemas" / "daily-congressional-record" / "example_daily_congressional_record.json"
        )
        self.assertEqual(len(select(payload, spec.nodes["daily_congressional_record_list"].item_path)), 1)
        self.assertTrue(select_one(payload, spec.nodes["daily_congressional_record_list"].next_page_path))
        self.assertTrue(select_one(payload, "$.dailyCongressionalRecord[*].url"))

    def test_daily_record_detail_and_articles_contracts(self) -> None:
        spec = load_spec(PROJECT_ROOT / "specs" / "daily-congressional-record.yaml")
        detail = load_example_payload(
            PROJECT_ROOT
            / "api_schemas"
            / "daily-congressional-record"
            / "example_daily_congressional_record_detail.json"
        )
        articles = load_example_payload(
            PROJECT_ROOT
            / "api_schemas"
            / "daily-congressional-record"
            / "example_daily_congressional_record_articles.json"
        )
        self.assertEqual(select_one(detail, spec.nodes["daily_congressional_record_detail"].data_path), detail["issue"])
        self.assertTrue(select_one(detail, "$.issue.fullIssue.articles.url"))
        self.assertEqual(len(select(articles, spec.nodes["daily_congressional_record_articles"].item_path)), 1)

    def test_bound_record_list_and_detail_contracts(self) -> None:
        spec = load_spec(PROJECT_ROOT / "specs" / "bound-congressional-record.yaml")
        listing = load_example_payload(
            PROJECT_ROOT / "api_schemas" / "bound-congressional-record" / "example_bound_congressional_record.json"
        )
        detail = load_example_payload(
            PROJECT_ROOT
            / "api_schemas"
            / "bound-congressional-record"
            / "example_bound_congressional_record_detail.json"
        )
        self.assertEqual(len(select(listing, spec.nodes["bound_congressional_record_list"].item_path)), 1)
        self.assertTrue(select_one(listing, spec.nodes["bound_congressional_record_list"].next_page_path))
        self.assertTrue(select_one(listing, "$.boundCongressionalRecord[*].url"))
        self.assertEqual(len(select(detail, spec.nodes["bound_congressional_record_detail"].item_path)), 1)
        self.assertEqual(len(select_one(detail, "$.boundCongressionalRecord[*].sections") or []), 1)

    def test_legacy_congressional_record_index_contract(self) -> None:
        spec = load_spec(PROJECT_ROOT / "specs" / "congressional-record.yaml")
        payload = load_example_payload(
            PROJECT_ROOT / "api_schemas" / "congressional-record" / "example_congressional_record.json"
        )
        self.assertEqual(len(select(payload, spec.nodes["congressional_record_list"].item_path)), 1)
        self.assertEqual(select_one(payload, "$.Results.IndexStart"), 1)
        self.assertEqual(select_one(payload, "$.Results.Issues[*].Id"), 26958)


if __name__ == "__main__":
    unittest.main()
