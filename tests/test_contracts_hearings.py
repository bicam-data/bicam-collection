from __future__ import annotations

import unittest
from pathlib import Path

from bicam_collection.examples import load_example_payload
from bicam_collection.selectors import select, select_one
from bicam_collection.spec_loader import load_spec


PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXAMPLES_DIR = PROJECT_ROOT / "api_schemas" / "hearings"
SPEC = load_spec(PROJECT_ROOT / "specs" / "hearings.yaml")


class HearingExampleContractTests(unittest.TestCase):
    def test_hearing_list_contract(self) -> None:
        payload = load_example_payload(EXAMPLES_DIR / "example_hearing.json")
        self.assertEqual(len(select(payload, SPEC.nodes["hearing_list"].item_path)), 1)
        self.assertTrue(select_one(payload, SPEC.nodes["hearing_list"].next_page_path))

    def test_hearing_detail_contract(self) -> None:
        payload = load_example_payload(EXAMPLES_DIR / "example_hearingdetail.json")
        self.assertIsNotNone(select_one(payload, "$.hearing"))
        self.assertEqual(len(select(payload, "$.hearing.committees[*]")), 1)
        self.assertEqual(len(select(payload, "$.hearing.dates[*]")), 1)
        self.assertEqual(len(select(payload, "$.hearing.formats[*]")), 2)
        self.assertEqual(select_one(payload, "$.hearing.associatedMeeting.eventId"), "118987")
        self.assertEqual(
            select_one(payload, "$.hearing.associatedMeeting.url"),
            "https://api.congress.gov/v3/committee-meeting/119/house/118987?format=json",
        )


if __name__ == "__main__":
    unittest.main()
