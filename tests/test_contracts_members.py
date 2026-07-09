from __future__ import annotations

import unittest
from pathlib import Path

from bicam_collection.examples import load_example_payload
from bicam_collection.selectors import select, select_one
from bicam_collection.spec_loader import load_spec


PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXAMPLES_DIR = PROJECT_ROOT / "api_schemas" / "members"
SPEC = load_spec(PROJECT_ROOT / "specs" / "members.yaml")


class MemberExampleContractTests(unittest.TestCase):
    def test_member_list_contract(self) -> None:
        payload = load_example_payload(EXAMPLES_DIR / "example_member.json")
        self.assertEqual(len(select(payload, SPEC.nodes["member_list"].item_path)), 1)
        self.assertTrue(select_one(payload, SPEC.nodes["member_list"].next_page_path))
        self.assertEqual(
            SPEC.nodes["member_list"].incremental_query_params,
            ["fromDateTime", "toDateTime"],
        )

    def test_member_detail_contract(self) -> None:
        payload = load_example_payload(EXAMPLES_DIR / "example_memberdetail.json")
        self.assertIsNotNone(select_one(payload, "$.member"))
        self.assertEqual(
            select_one(payload, "$.member.sponsoredLegislation.url"),
            "https://api.congress.gov/v3/member/L000174/sponsored-legislation",
        )
        self.assertEqual(
            select_one(payload, "$.member.cosponsoredLegislation.url"),
            "https://api.congress.gov/v3/member/L000174/cosponsored-legislation",
        )

    def test_member_sponsored_legislation_contract(self) -> None:
        payload = load_example_payload(EXAMPLES_DIR / "example_member_sponsored_legislation.json")
        self.assertEqual(len(select(payload, "$.sponsoredLegislation[*]")), 1)
        self.assertTrue(select_one(payload, "$.pagination.next"))

    def test_member_cosponsored_legislation_contract(self) -> None:
        payload = load_example_payload(EXAMPLES_DIR / "example_member_cosponsored_legislation.json")
        self.assertEqual(len(select(payload, "$.cosponsoredLegislation[*]")), 1)
        self.assertTrue(select_one(payload, "$.pagination.next"))


if __name__ == "__main__":
    unittest.main()
