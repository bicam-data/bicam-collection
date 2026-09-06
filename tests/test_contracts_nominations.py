from __future__ import annotations

import unittest
from pathlib import Path

from bicam_collection.examples import load_example_payload
from bicam_collection.selectors import select, select_one
from bicam_collection.spec_loader import load_spec


PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXAMPLES_DIR = PROJECT_ROOT / "api_schemas" / "nominations"
SPEC = load_spec(PROJECT_ROOT / "specs" / "nominations.yaml")


class NominationExampleContractTests(unittest.TestCase):
    def test_nomination_list_contract(self) -> None:
        payload = load_example_payload(EXAMPLES_DIR / "example_nomination.json")
        self.assertEqual(len(select(payload, SPEC.nodes["nomination_list"].item_path)), 1)
        self.assertTrue(select_one(payload, SPEC.nodes["nomination_list"].next_page_path))
        self.assertEqual(
            SPEC.nodes["nomination_list"].incremental_query_params,
            ["fromDateTime", "toDateTime"],
        )

    def test_nomination_detail_contract(self) -> None:
        payload = load_example_payload(EXAMPLES_DIR / "example_nominationdetail.json")
        self.assertIsNotNone(select_one(payload, "$.nomination"))
        self.assertEqual(select_one(payload, "$.nomination.nominationType.isCivilian"), True)
        self.assertEqual(select_one(payload, "$.nomination.nominationType.isMilitary"), False)
        self.assertEqual(len(select(payload, "$.nomination.nominees[*]")), 1)
        self.assertEqual(
            select_one(payload, "$.nomination.nominees[*].url"),
            "https://api.congress.gov/v3/nomination/118/1980/1?format=json",
        )

    def test_nomination_actions_contract(self) -> None:
        payload = load_example_payload(EXAMPLES_DIR / "example_nomination_action.json")
        self.assertEqual(len(select(payload, "$.actions[*]")), 1)

    def test_nomination_committees_contract(self) -> None:
        payload = load_example_payload(EXAMPLES_DIR / "example_nomination_committee.json")
        self.assertEqual(len(select(payload, "$.committees[*]")), 1)

    def test_nomination_hearings_contract(self) -> None:
        payload = load_example_payload(EXAMPLES_DIR / "example_nomination_hearing.json")
        self.assertEqual(len(select(payload, "$.hearings[*]")), 1)

    def test_nomination_nominee_contract(self) -> None:
        payload = load_example_payload(EXAMPLES_DIR / "example_nomination_nominee.json")
        self.assertEqual(len(select(payload, "$.nominees[*]")), 1)
        self.assertEqual(select_one(payload, "$.nominees[*].lastName"), "Doe")
        node = SPEC.nodes["nomination_nominee_detail"]
        self.assertEqual(node.pagination_path, "$.pagination")
        self.assertEqual(node.next_page_path, "$.pagination.next")
        self.assertEqual(node.default_query_params, {"format": "json", "limit": 250})


if __name__ == "__main__":
    unittest.main()
