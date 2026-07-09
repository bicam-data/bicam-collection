from __future__ import annotations

import unittest
from pathlib import Path

from bicam_collection.examples import load_example_payload
from bicam_collection.selectors import select, select_one
from bicam_collection.spec_loader import load_spec


PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXAMPLES_DIR = PROJECT_ROOT / "api_schemas" / "amendments"
SPEC = load_spec(PROJECT_ROOT / "specs" / "amendments.yaml")


class AmendmentExampleContractTests(unittest.TestCase):
    def test_amendment_list_contract(self) -> None:
        payload = load_example_payload(EXAMPLES_DIR / "example_amendment.json")
        self.assertEqual(len(select(payload, SPEC.nodes["amendment_list"].item_path)), 1)
        self.assertTrue(select_one(payload, SPEC.nodes["amendment_list"].next_page_path))

    def test_amendment_detail_contract(self) -> None:
        payload = load_example_payload(EXAMPLES_DIR / "example_amendmentdetail.json")
        self.assertIsNotNone(select_one(payload, "$.amendment"))
        self.assertEqual(select_one(payload, "$.amendment.amendedBill.number"), "4350")
        self.assertEqual(select_one(payload, "$.amendment.amendedAmendment.number"), "3867")
        self.assertEqual(select_one(payload, "$.amendment.amendedTreaty.treatyNumber"), "1")
        self.assertEqual(len(select(payload, "$.amendment.sponsors[*]")), 1)
        self.assertEqual(len(select(payload, "$.amendment.onBehalfOfSponsor[*]")), 1)

    def test_amendment_actions_contract(self) -> None:
        payload = load_example_payload(EXAMPLES_DIR / "example_amendment_action.json")
        self.assertEqual(len(select(payload, "$.actions[*]")), 1)

    def test_amendment_cosponsors_contract(self) -> None:
        payload = load_example_payload(EXAMPLES_DIR / "example_amendment_cosponsor.json")
        self.assertEqual(len(select(payload, "$.cosponsors[*]")), 1)

    def test_amendments_to_amendment_contract(self) -> None:
        payload = load_example_payload(EXAMPLES_DIR / "example_amendment_amendment.json")
        self.assertEqual(len(select(payload, "$.amendments[*]")), 1)

    def test_amendment_text_contract(self) -> None:
        payload = load_example_payload(EXAMPLES_DIR / "example_amendment_text.json")
        self.assertEqual(len(select(payload, "$.textVersions[*]")), 1)


if __name__ == "__main__":
    unittest.main()
