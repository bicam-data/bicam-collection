from __future__ import annotations

import unittest
from pathlib import Path

from bicam_collection.examples import load_example_payload
from bicam_collection.selectors import select, select_one
from bicam_collection.spec_loader import load_spec


PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXAMPLES_DIR = PROJECT_ROOT / "api_schemas" / "bills"
SPEC = load_spec(PROJECT_ROOT / "specs" / "bills.yaml")


class ExampleContractTests(unittest.TestCase):
    def test_bill_list_contract(self) -> None:
        payload = load_example_payload(EXAMPLES_DIR / "example_bill.md")
        self.assertEqual(len(select(payload, SPEC.nodes["bill_list"].item_path)), 1)
        self.assertTrue(select_one(payload, SPEC.nodes["bill_list"].next_page_path))

    def test_bill_detail_contract(self) -> None:
        payload = load_example_payload(EXAMPLES_DIR / "example_billdetail.md")
        self.assertIsNotNone(select_one(payload, "$.bill"))
        self.assertEqual(
            select_one(payload, "$.bill.textVersions.url"),
            "https://api.congress.gov/v3/bill/117/hr/3076/text?format=json",
        )
        self.assertEqual(
            select(payload, "$.bill.committeeReports[*].url"),
            [
                "https://api.congress.gov/v3/committee-report/117/HRPT/89?format=json",
                "https://api.congress.gov/v3/committee-report/117/HRPT/89?format=json",
            ],
        )

    def test_actions_contract(self) -> None:
        payload = load_example_payload(EXAMPLES_DIR / "example_bill_action.md")
        self.assertEqual(len(select(payload, "$.actions[*]")), 1)
        self.assertEqual(
            select(payload, "$.actions[*].committees[*].url"),
            ["https://api.congress.gov/v3/committee/house/hswm00?format=json"],
        )

    def test_amendments_contract(self) -> None:
        payload = load_example_payload(EXAMPLES_DIR / "example_bill_amendment.md")
        self.assertEqual(len(select(payload, "$.amendments[*]")), 1)

    def test_committees_contract(self) -> None:
        payload = load_example_payload(EXAMPLES_DIR / "example_bill_committee.md")
        self.assertEqual(len(select(payload, "$.committees[*]")), 1)
        self.assertEqual(len(select(payload, "$.committees[*].subcommittees[*].url")), 1)

    def test_cosponsors_contract(self) -> None:
        payload = load_example_payload(EXAMPLES_DIR / "example_bill_cosponsor.md")
        self.assertEqual(len(select(payload, "$.cosponsors[*]")), 1)
        self.assertEqual(select_one(payload, "$.cosponsors[*].bioguideId"), "F000450")

    def test_related_bills_contract(self) -> None:
        payload = load_example_payload(EXAMPLES_DIR / "example_bill_relatedbill.md")
        self.assertEqual(len(select(payload, "$.relatedBills[*]")), 1)

    def test_subjects_contract(self) -> None:
        payload = load_example_payload(EXAMPLES_DIR / "example_bill_subject.md")
        self.assertEqual(len(select(payload, "$.subjects.legislativeSubjects[*]")), 2)
        self.assertEqual(
            select_one(payload, "$.subjects.policyArea.name"),
            "Government Operations and Politics",
        )

    def test_summaries_contract(self) -> None:
        payload = load_example_payload(EXAMPLES_DIR / "example_bill_summary.md")
        self.assertEqual(len(select(payload, "$.summaries[*]")), 2)

    def test_text_contract(self) -> None:
        payload = load_example_payload(EXAMPLES_DIR / "example_bill_text.md")
        self.assertEqual(len(select(payload, "$.textVersions[*]")), 1)
        self.assertEqual(len(select(payload, "$.textVersions[*].formats[*].url")), 3)

    def test_titles_contract(self) -> None:
        payload = load_example_payload(EXAMPLES_DIR / "example_bill_title.md")
        self.assertEqual(len(select(payload, "$.titles[*]")), 1)


if __name__ == "__main__":
    unittest.main()
