from __future__ import annotations

import unittest
from pathlib import Path

from crawler_fakes import FakeClient, FakeStore
from bicam_collection.crawler import CrawlOptions, SpecCrawler
from bicam_collection.examples import load_example_payload
from bicam_collection.spec_loader import load_spec
from bicam_collection.urls import canonicalize_congress_api_url, with_query_params


PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXAMPLES_DIR = PROJECT_ROOT / "api_schemas" / "committee-meetings"


def _load(name: str) -> dict:
    return load_example_payload(EXAMPLES_DIR / name)


class CommitteeMeetingCrawlerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.spec = load_spec(PROJECT_ROOT / "specs" / "committee-meetings.yaml")

    def test_committee_meeting_crawl_fetches_list_and_detail_only(self) -> None:
        meeting_list = _load("example_committee_meeting.json")
        meeting_detail = _load("example_committee_meetingdetail.json")
        meeting_list["pagination"]["next"] = ""

        seed_url = with_query_params("https://api.congress.gov/v3/committee-meeting?format=json", {"limit": 250})
        detail_url = canonicalize_congress_api_url("https://api.congress.gov/v3/committee-meeting/119/house/119029?format=json")

        client = FakeClient({seed_url: meeting_list, detail_url: meeting_detail})
        store = FakeStore()
        crawler = SpecCrawler(
            base_url="https://api.congress.gov/v3",
            spec=self.spec,
            client=client,
            store=store,
        )

        crawler.run(CrawlOptions(mode="full"))

        self.assertEqual(set(store.raw_by_url), {seed_url, detail_url})


if __name__ == "__main__":
    unittest.main()
