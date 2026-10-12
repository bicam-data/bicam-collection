from __future__ import annotations

import unittest
from pathlib import Path

from crawler_fakes import FakeClient, FakeStore
from bicam_collection.crawler import CrawlOptions, SpecCrawler
from bicam_collection.examples import load_example_payload
from bicam_collection.spec_loader import load_spec
from bicam_collection.urls import canonicalize_congress_api_url, with_query_params


PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXAMPLES_DIR = PROJECT_ROOT / "api_schemas" / "nominations"


def _load(name: str) -> dict:
    return load_example_payload(EXAMPLES_DIR / name)


class NominationCrawlerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.spec = load_spec(PROJECT_ROOT / "specs" / "nominations.yaml")

    def test_nomination_crawl_follows_detail_children_and_nominee_detail(self) -> None:
        nomination_list = _load("example_nomination.json")
        nomination_detail = _load("example_nominationdetail.json")
        actions = _load("example_nomination_action.json")
        committees = _load("example_nomination_committee.json")
        hearings = _load("example_nomination_hearing.json")
        nominee = _load("example_nomination_nominee.json")
        nomination_list["pagination"]["next"] = ""
        actions["pagination"]["next"] = ""
        hearings["pagination"]["next"] = ""

        seed_url = with_query_params("https://api.congress.gov/v3/nomination?format=json", {"limit": 250})
        detail_url = canonicalize_congress_api_url("https://api.congress.gov/v3/nomination/118/1980?format=json")
        actions_url = canonicalize_congress_api_url("https://api.congress.gov/v3/nomination/118/1980/actions?format=json&limit=250")
        committees_url = canonicalize_congress_api_url("https://api.congress.gov/v3/nomination/118/1980/committees?format=json&limit=250")
        hearings_url = canonicalize_congress_api_url("https://api.congress.gov/v3/nomination/118/1980/hearings?format=json&limit=250")
        nominee_url = canonicalize_congress_api_url(
            "https://api.congress.gov/v3/nomination/118/1980/1?format=json&limit=250"
        )

        client = FakeClient(
            {
                seed_url: nomination_list,
                detail_url: nomination_detail,
                actions_url: actions,
                committees_url: committees,
                hearings_url: hearings,
                nominee_url: nominee,
            }
        )
        store = FakeStore()
        crawler = SpecCrawler(
            base_url="https://api.congress.gov/v3",
            spec=self.spec,
            client=client,
            store=store,
        )

        crawler.run(CrawlOptions(mode="full"))

        self.assertIn(detail_url, store.raw_by_url)
        self.assertIn(actions_url, store.raw_by_url)
        self.assertIn(committees_url, store.raw_by_url)
        self.assertIn(hearings_url, store.raw_by_url)
        self.assertIn(nominee_url, store.raw_by_url)


if __name__ == "__main__":
    unittest.main()
