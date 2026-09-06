from __future__ import annotations

import unittest
from pathlib import Path

from bicam_collection.examples import load_example_payload
from bicam_collection.selectors import select, select_one
from bicam_collection.spec_loader import load_spec


PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXAMPLES_DIR = PROJECT_ROOT / "api_schemas" / "crs-reports"
SPEC = load_spec(PROJECT_ROOT / "specs" / "crs-reports.yaml")


class CrsReportExampleContractTests(unittest.TestCase):
    def test_list_contract_and_detail_edge(self) -> None:
        payload = load_example_payload(EXAMPLES_DIR / "example_crsreport.json")
        self.assertEqual(len(select(payload, SPEC.nodes["crs_report_list"].item_path)), 1)
        self.assertTrue(select_one(payload, SPEC.nodes["crs_report_list"].next_page_path))
        self.assertEqual(select_one(payload, "$.CRSReports[*].id"), "R47175")

    def test_detail_contract(self) -> None:
        payload = load_example_payload(EXAMPLES_DIR / "example_crsreportdetail.json")
        self.assertEqual(select_one(payload, "$.id"), "R47175")
        self.assertEqual(len(select(payload, "$.authors[*]")), 1)
        self.assertEqual(len(select(payload, "$.topics[*]")), 1)


if __name__ == "__main__":
    unittest.main()
