from __future__ import annotations

import unittest
from pathlib import Path

from crawler_fakes import FakeClient, FakeStore
from bicam_collection.crawler import CrawlOptions, SpecCrawler
from bicam_collection.examples import load_example_payload
from bicam_collection.spec_loader import load_spec
from bicam_collection.urls import canonicalize_congress_api_url, with_query_params


PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXAMPLES_DIR = PROJECT_ROOT / "api_schemas" / "congresses"


def _load(name: str) -> dict:
    return load_example_payload(EXAMPLES_DIR / name)


class CongressCrawlerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.spec = load_spec(PROJECT_ROOT / "specs" / "congresses.yaml")

    def test_congress_crawl_fetches_list_and_detail_only(self) -> None:
        congress_list = _load("example_congress.json")
        congress_detail = _load("example_congressdetail.json")
        congress_list["pagination"]["next"] = ""

        seed_url = with_query_params("https://api.congress.gov/v3/congress?format=json", {"limit": 250})
        detail_url = canonicalize_congress_api_url("https://api.congress.gov/v3/congress/119?format=json")

        client = FakeClient({seed_url: congress_list, detail_url: congress_detail})
        store = FakeStore()
        crawler = SpecCrawler(
            base_url="https://api.congress.gov/v3",
            spec=self.spec,
            client=client,
            store=store,
        )

        crawler.run(CrawlOptions(mode="full"))

        self.assertEqual(client.calls, [seed_url, detail_url])
        self.assertIn(detail_url, store.raw_by_url)


if __name__ == "__main__":
    unittest.main()
