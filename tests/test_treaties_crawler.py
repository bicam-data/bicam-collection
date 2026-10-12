from __future__ import annotations

import unittest
from pathlib import Path

from crawler_fakes import FakeClient, FakeStore
from bicam_collection.crawler import CrawlOptions, SpecCrawler
from bicam_collection.examples import load_example_payload
from bicam_collection.spec_loader import load_spec
from bicam_collection.urls import canonicalize_congress_api_url, with_query_params


PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXAMPLES_DIR = PROJECT_ROOT / "api_schemas" / "treaties"


def _load(name: str) -> dict:
    return load_example_payload(EXAMPLES_DIR / name)


class TreatyCrawlerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.spec = load_spec(PROJECT_ROOT / "specs" / "treaties.yaml")

    def test_treaty_crawl_follows_detail_actions_and_committees(self) -> None:
        treaty_list = _load("example_treaty.json")
        treaty_detail = _load("example_treatydetail.json")
        treaty_actions = _load("example_treaty_action.json")
        treaty_committees = _load("example_treaty_committee.json")
        treaty_list["pagination"]["next"] = ""

        seed_url = with_query_params("https://api.congress.gov/v3/treaty?format=json", {"limit": 250})
        detail_url = canonicalize_congress_api_url("https://api.congress.gov/v3/treaty/119/3/A?format=json")
        actions_url = canonicalize_congress_api_url("https://api.congress.gov/v3/treaty/119/3/A/actions?format=json&limit=250")
        committees_url = canonicalize_congress_api_url(
            "https://api.congress.gov/v3/treaty/119/3/A/committees?format=json&limit=250"
        )

        client = FakeClient(
            {
                seed_url: treaty_list,
                detail_url: treaty_detail,
                actions_url: treaty_actions,
                committees_url: treaty_committees,
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

    def test_treaty_crawl_constructs_committees_url_when_detail_omits_link(self) -> None:
        treaty_list = _load("example_treaty.json")
        treaty_detail = _load("example_treatydetail.json")
        treaty_actions = _load("example_treaty_action.json")
        treaty_committees = _load("example_treaty_committee.json")
        treaty_detail["treaty"][0].pop("committees", None)
        treaty_list["pagination"]["next"] = ""

        seed_url = with_query_params("https://api.congress.gov/v3/treaty?format=json", {"limit": 250})
        detail_url = canonicalize_congress_api_url("https://api.congress.gov/v3/treaty/119/3/A?format=json")
        actions_url = canonicalize_congress_api_url("https://api.congress.gov/v3/treaty/119/3/A/actions?format=json&limit=250")
        committees_url = canonicalize_congress_api_url(
            "https://api.congress.gov/v3/treaty/119/3/A/committees?format=json&limit=250"
        )

        client = FakeClient(
            {
                seed_url: treaty_list,
                detail_url: treaty_detail,
                actions_url: treaty_actions,
                committees_url: treaty_committees,
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

        self.assertIn(committees_url, store.raw_by_url)


if __name__ == "__main__":
    unittest.main()
