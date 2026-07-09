from __future__ import annotations

import unittest
from pathlib import Path

from bicam_collection.examples import load_example_payload
from bicam_collection.selectors import select, select_one
from bicam_collection.spec_loader import load_spec


PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXAMPLES_DIR = PROJECT_ROOT / "api_schemas" / "committee-reports"
SPEC = load_spec(PROJECT_ROOT / "specs" / "committee-reports.yaml")


class CommitteeReportExampleContractTests(unittest.TestCase):
    def test_committee_report_list_contract(self) -> None:
        payload = load_example_payload(EXAMPLES_DIR / "example_committee_report.json")
        self.assertEqual(len(select(payload, SPEC.nodes["committee_report_list"].item_path)), 1)
        self.assertTrue(select_one(payload, SPEC.nodes["committee_report_list"].next_page_path))

    def test_committee_report_detail_contract(self) -> None:
        payload = load_example_payload(EXAMPLES_DIR / "example_committee_reportdetail.json")
        self.assertEqual(len(select(payload, "$.committeeReports[*]")), 1)
        self.assertEqual(len(select(payload, "$.committeeReports[*].committees[*]")), 1)
        self.assertEqual(len(select(payload, "$.committeeReports[*].associatedBill[*]")), 1)
        self.assertEqual(
            select_one(payload, "$.committeeReports[*].text.url"),
            "https://api.congress.gov/v3/committee-report/119/HRPT/1/text?format=json",
        )

    def test_committee_report_treaty_detail_contract(self) -> None:
        payload = load_example_payload(EXAMPLES_DIR / "example_committee_reportdetail_treaty.json")
        self.assertEqual(len(select(payload, "$.committeeReports[*].associatedTreaties[*]")), 1)
        self.assertEqual(
            select_one(payload, "$.committeeReports[*].associatedTreaties[*].url"),
            "https://api.congress.gov/v3/treaty/112/8?format=json",
        )

    def test_committee_report_text_contract(self) -> None:
        payload = load_example_payload(EXAMPLES_DIR / "example_committee_report_text.json")
        self.assertEqual(len(select(payload, "$.text[*]")), 2)
        self.assertEqual(len(select(payload, "$.text[*].formats[*].url")), 2)


if __name__ == "__main__":
    unittest.main()
