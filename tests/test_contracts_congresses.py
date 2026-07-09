from __future__ import annotations

import unittest
from pathlib import Path

from bicam_collection.examples import load_example_payload
from bicam_collection.selectors import select, select_one
from bicam_collection.spec_loader import load_spec


PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXAMPLES_DIR = PROJECT_ROOT / "api_schemas" / "congresses"
SPEC = load_spec(PROJECT_ROOT / "specs" / "congresses.yaml")


class CongressExampleContractTests(unittest.TestCase):
    def test_congress_list_contract(self) -> None:
        payload = load_example_payload(EXAMPLES_DIR / "example_congress.json")
        self.assertEqual(len(select(payload, SPEC.nodes["congress_list"].item_path)), 1)
        self.assertTrue(select_one(payload, SPEC.nodes["congress_list"].next_page_path))

    def test_congress_detail_contract(self) -> None:
        payload = load_example_payload(EXAMPLES_DIR / "example_congressdetail.json")
        self.assertIsNotNone(select_one(payload, "$.congress"))
        self.assertEqual(select_one(payload, "$.congress.number"), 119)
        self.assertEqual(len(select(payload, "$.congress.sessions[*]")), 4)


if __name__ == "__main__":
    unittest.main()
