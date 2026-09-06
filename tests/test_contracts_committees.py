from __future__ import annotations

import unittest
from pathlib import Path

from bicam_collection.examples import load_example_payload
from bicam_collection.selectors import select, select_one
from bicam_collection.spec_loader import load_spec


PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXAMPLES_DIR = PROJECT_ROOT / "api_schemas" / "committees"
SPEC = load_spec(PROJECT_ROOT / "specs" / "committees.yaml")


class CommitteeExampleContractTests(unittest.TestCase):
    def test_committee_list_contract(self) -> None:
        payload = load_example_payload(EXAMPLES_DIR / "example_committee.json")
        self.assertEqual(len(select(payload, SPEC.nodes["committee_list"].item_path)), 1)
        self.assertTrue(select_one(payload, SPEC.nodes["committee_list"].next_page_path))

    def test_committee_detail_contract(self) -> None:
        payload = load_example_payload(EXAMPLES_DIR / "example_committeedetail.json")
        self.assertIsNotNone(select_one(payload, "$.committee"))
        self.assertEqual(select_one(payload, "$.committee.systemCode"), "sseg00")
        self.assertEqual(len(select(payload, "$.committee.history[*]")), 1)

    def test_committee_bills_contract(self) -> None:
        payload = load_example_payload(EXAMPLES_DIR / "example_committee_bills.json")
        self.assertEqual(len(select(payload, "$.committee-bills.bills[*]")), 1)

    def test_committee_reports_contract(self) -> None:
        payload = load_example_payload(EXAMPLES_DIR / "example_committee_reports.json")
        self.assertEqual(len(select(payload, "$.reports[*]")), 1)

    def test_committee_nominations_contract(self) -> None:
        payload = load_example_payload(EXAMPLES_DIR / "example_committee_nominations.json")
        self.assertEqual(len(select(payload, "$.nominations[*]")), 1)

    def test_committee_communications_contract(self) -> None:
        payload = load_example_payload(EXAMPLES_DIR / "example_committee_communications.json")
        self.assertEqual(len(select(payload, "$.houseCommunications[*]")), 1)
        self.assertTrue(select_one(payload, SPEC.nodes["committee_communications"].next_page_path))


if __name__ == "__main__":
    unittest.main()
