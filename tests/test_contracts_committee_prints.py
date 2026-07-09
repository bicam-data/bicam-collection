from __future__ import annotations

import unittest
from pathlib import Path

from bicam_collection.examples import load_example_payload
from bicam_collection.selectors import select, select_one
from bicam_collection.spec_loader import load_spec


PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXAMPLES_DIR = PROJECT_ROOT / "api_schemas" / "committee-prints"
SPEC = load_spec(PROJECT_ROOT / "specs" / "committee-prints.yaml")


class CommitteePrintExampleContractTests(unittest.TestCase):
    def test_committee_print_list_contract(self) -> None:
        payload = load_example_payload(EXAMPLES_DIR / "example_committee_print.json")
        self.assertEqual(len(select(payload, SPEC.nodes["committee_print_list"].item_path)), 1)
        self.assertTrue(select_one(payload, SPEC.nodes["committee_print_list"].next_page_path))

    def test_committee_print_detail_contract(self) -> None:
        payload = load_example_payload(EXAMPLES_DIR / "example_committee_printdetail.json")
        self.assertEqual(len(select(payload, "$.committeePrint[*]")), 1)
        self.assertEqual(len(select(payload, "$.committeePrint[*].committees[*]")), 1)
        self.assertEqual(len(select(payload, "$.committeePrint[*].associatedBills[*]")), 1)
        self.assertEqual(
            select_one(payload, "$.committeePrint[*].text.url"),
            "https://api.congress.gov/v3/committee-print/117/house/47777/text?format=json",
        )

    def test_committee_print_text_contract(self) -> None:
        payload = load_example_payload(EXAMPLES_DIR / "example_committee_print_text.json")
        self.assertEqual(len(select(payload, "$.text[*]")), 2)
        self.assertEqual(len(select(payload, "$.text[*].url")), 2)


if __name__ == "__main__":
    unittest.main()
