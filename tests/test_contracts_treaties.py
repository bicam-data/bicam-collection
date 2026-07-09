from __future__ import annotations

import unittest
from pathlib import Path

from bicam_collection.examples import load_example_payload
from bicam_collection.selectors import select, select_one
from bicam_collection.spec_loader import load_spec


PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXAMPLES_DIR = PROJECT_ROOT / "api_schemas" / "treaties"
SPEC = load_spec(PROJECT_ROOT / "specs" / "treaties.yaml")


class TreatyExampleContractTests(unittest.TestCase):
    def test_treaty_list_contract(self) -> None:
        payload = load_example_payload(EXAMPLES_DIR / "example_treaty.json")
        self.assertEqual(len(select(payload, SPEC.nodes["treaty_list"].item_path)), 1)
        self.assertTrue(select_one(payload, SPEC.nodes["treaty_list"].next_page_path))

    def test_treaty_detail_contract(self) -> None:
        payload = load_example_payload(EXAMPLES_DIR / "example_treatydetail.json")
        self.assertEqual(len(select(payload, "$.treaty[*]")), 1)
        self.assertEqual(len(select(payload, "$.treaty[*].countriesParties[*]")), 1)
        self.assertEqual(len(select(payload, "$.treaty[*].indexTerms[*]")), 1)
        self.assertEqual(len(select(payload, "$.treaty[*].relatedDocs[*]")), 1)
        self.assertEqual(len(select(payload, "$.treaty[*].titles[*]")), 1)
        self.assertEqual(select_one(payload, "$.treaty[*].parts.count"), 2)
        self.assertEqual(
            select_one(payload, "$.treaty[*].actions.url"),
            "https://api.congress.gov/v3/treaty/119/3/A/actions?format=json",
        )
        self.assertEqual(
            select_one(payload, "$.treaty[*].committees.url"),
            "https://api.congress.gov/v3/treaty/119/3/A/committees?format=json",
        )

    def test_treaty_actions_contract(self) -> None:
        payload = load_example_payload(EXAMPLES_DIR / "example_treaty_action.json")
        self.assertEqual(len(select(payload, "$.actions[*]")), 1)
        self.assertEqual(select_one(payload, "$.actions[*].committee.systemCode"), "ssfr00")

    def test_treaty_committees_contract(self) -> None:
        payload = load_example_payload(EXAMPLES_DIR / "example_treaty_committee.json")
        self.assertEqual(len(select(payload, "$.treatyCommittees[*]")), 1)
        self.assertEqual(len(select(payload, "$.treatyCommittees[*].activities[*]")), 1)
        self.assertEqual(len(select(payload, "$.treatyCommittees[*].subcommittees[*]")), 1)


if __name__ == "__main__":
    unittest.main()
