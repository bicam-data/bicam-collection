from __future__ import annotations

import unittest
from pathlib import Path

from bicam_collection.examples import load_example_payload
from bicam_collection.selectors import select, select_one
from bicam_collection.spec_loader import load_spec


PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXAMPLES_DIR = PROJECT_ROOT / "api_schemas" / "committee-meetings"
SPEC = load_spec(PROJECT_ROOT / "specs" / "committee-meetings.yaml")


class CommitteeMeetingExampleContractTests(unittest.TestCase):
    def test_committee_meeting_list_contract(self) -> None:
        payload = load_example_payload(EXAMPLES_DIR / "example_committee_meeting.json")
        self.assertEqual(len(select(payload, SPEC.nodes["committee_meeting_list"].item_path)), 1)
        self.assertTrue(select_one(payload, SPEC.nodes["committee_meeting_list"].next_page_path))

    def test_committee_meeting_detail_contract(self) -> None:
        payload = load_example_payload(EXAMPLES_DIR / "example_committee_meetingdetail.json")
        self.assertIsNotNone(select_one(payload, "$.committeeMeeting"))
        self.assertEqual(len(select(payload, "$.committeeMeeting.committees[*]")), 1)
        self.assertEqual(len(select(payload, "$.committeeMeeting.continuations[*]")), 1)
        self.assertEqual(len(select(payload, "$.committeeMeeting.videos[*]")), 1)
        self.assertEqual(len(select(payload, "$.committeeMeeting.witnesses[*]")), 1)
        self.assertEqual(len(select(payload, "$.committeeMeeting.witnessDocuments[*]")), 1)
        self.assertEqual(len(select(payload, "$.committeeMeeting.meetingDocuments[*]")), 1)
        self.assertEqual(len(select(payload, "$.committeeMeeting.relatedItems.bills[*]")), 1)
        self.assertEqual(len(select(payload, "$.committeeMeeting.relatedItems.treaties[*]")), 1)
        self.assertEqual(len(select(payload, "$.committeeMeeting.relatedItems.nominations[*]")), 1)
        self.assertEqual(select_one(payload, "$.committeeMeeting.hearingTranscript[*].jacketNumber"), 62435)
        self.assertEqual(
            select_one(payload, "$.committeeMeeting.hearingTranscript[*].url"),
            "https://api.congress.gov/v3/hearing/119/house/62435?format=json",
        )


if __name__ == "__main__":
    unittest.main()
