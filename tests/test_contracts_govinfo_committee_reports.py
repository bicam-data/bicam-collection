from __future__ import annotations

import unittest
from pathlib import Path

from bicam_collection.examples import load_example_payload
from bicam_collection.selectors import select, select_one
from bicam_collection.spec_loader import load_spec


PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXAMPLES_DIR = PROJECT_ROOT / "api_schemas" / "govinfo-committee-reports"
SPEC = load_spec(PROJECT_ROOT / "specs" / "govinfo-committee-reports.yaml")


class GovInfoCommitteeReportContractTests(unittest.TestCase):
    def test_collection_contract(self) -> None:
        payload = load_example_payload(EXAMPLES_DIR / "example_collection.json")
        self.assertEqual(len(select(payload, SPEC.nodes["govinfo_committee_report_package_list"].item_path)), 2)
        self.assertTrue(select_one(payload, SPEC.nodes["govinfo_committee_report_package_list"].next_page_path))

    def test_package_summary_contract(self) -> None:
        payload = load_example_payload(EXAMPLES_DIR / "example_package_summary.json")
        self.assertEqual(select_one(payload, "$.packageId"), "CRPT-119hrpt173")
        self.assertEqual(
            select_one(payload, "$.granulesLink"),
            "https://api.govinfo.gov/packages/CRPT-119hrpt173/granules?offsetMark=*&pageSize=100",
        )

    def test_granules_contract(self) -> None:
        payload = load_example_payload(EXAMPLES_DIR / "example_granules.json")
        self.assertEqual(len(select(payload, SPEC.nodes["govinfo_committee_report_granule_list"].item_path)), 1)
        self.assertEqual(
            select_one(payload, "$.granules[*].granuleLink"),
            "https://api.govinfo.gov/packages/CRPT-119hrpt173/granules/CRPT-119hrpt173/summary",
        )

    def test_granule_summary_contract(self) -> None:
        payload = load_example_payload(EXAMPLES_DIR / "example_granule_summary.json")
        self.assertEqual(select_one(payload, "$.granuleId"), "CRPT-119hrpt173")
        self.assertEqual(select_one(payload, "$.collectionCode"), "CRPT")


if __name__ == "__main__":
    unittest.main()
