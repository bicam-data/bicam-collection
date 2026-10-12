from __future__ import annotations

import unittest
from pathlib import Path

from crawler_fakes import FakeClient, FakeStore, http_error
from bicam_collection.crawler import CrawlOptions, SpecCrawler
from bicam_collection.examples import load_example_payload
from bicam_collection.spec_loader import load_spec
from bicam_collection.urls import canonicalize_congress_api_url, with_query_params


PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXAMPLES_DIR = PROJECT_ROOT / "api_schemas" / "hearings"


def _load(name: str) -> dict:
    return load_example_payload(EXAMPLES_DIR / name)


class HearingCrawlerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.spec = load_spec(PROJECT_ROOT / "specs" / "hearings.yaml")

    def test_hearing_crawl_fetches_list_and_detail_only(self) -> None:
        hearing_list = _load("example_hearing.json")
        hearing_detail = _load("example_hearingdetail.json")
        hearing_list["pagination"]["next"] = ""

        seed_url = with_query_params("https://api.congress.gov/v3/hearing?format=json", {"limit": 250})
        detail_url = canonicalize_congress_api_url("https://api.congress.gov/v3/hearing/119/house/62790?format=json")

        client = FakeClient({seed_url: hearing_list, detail_url: hearing_detail})
        store = FakeStore()
        crawler = SpecCrawler(
            base_url="https://api.congress.gov/v3",
            spec=self.spec,
            client=client,
            store=store,
        )

        crawler.run(CrawlOptions(mode="full"))

        self.assertEqual(set(store.raw_by_url), {seed_url, detail_url})

    def test_hearing_list_retries_known_source_error_with_legacy_query_order(self) -> None:
        hearing_list = {
            "hearings": [],
            "pagination": {"count": 0, "next": "", "prev": ""},
            "request": {"format": "json"},
        }
        seed_url = with_query_params(
            "https://api.congress.gov/v3/hearing?format=json", {"limit": 250}
        )
        canonical_url = (
            "https://api.congress.gov/v3/hearing?format=json&limit=250&offset=34250"
        )
        fallback_url = (
            "https://api.congress.gov/v3/hearing?offset=34250&limit=250&format=json"
        )
        seed_payload = dict(hearing_list)
        seed_payload["pagination"] = {
            "count": 0,
            "next": fallback_url,
            "prev": "",
        }
        client = FakeClient(
            {seed_url: seed_payload, fallback_url: hearing_list},
            failures={canonical_url: http_error(500, "500 Server Error source_error=no_reverse_match", canonical_url)},
        )
        store = FakeStore()
        crawler = SpecCrawler(
            base_url="https://api.congress.gov/v3",
            spec=self.spec,
            client=client,
            store=store,
        )

        crawler.run(CrawlOptions(mode="full", max_pages=2))

        self.assertEqual(client.calls, [seed_url, canonical_url, fallback_url])
        self.assertIn(canonical_url, store.raw_by_url)


if __name__ == "__main__":
    unittest.main()
