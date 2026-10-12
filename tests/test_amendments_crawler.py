from __future__ import annotations

import unittest
from pathlib import Path

from crawler_fakes import FakeClient, FakeStore
from bicam_collection.crawler import CrawlOptions, SpecCrawler
from bicam_collection.examples import load_example_payload
from bicam_collection.spec_loader import load_spec
from bicam_collection.urls import canonicalize_congress_api_url, with_query_params


PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXAMPLES_DIR = PROJECT_ROOT / "api_schemas" / "amendments"


def _load(name: str) -> dict:
    return load_example_payload(EXAMPLES_DIR / name)


class AmendmentCrawlerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.spec = load_spec(PROJECT_ROOT / "specs" / "amendments.yaml")

    def test_amendment_crawl_follows_configured_children(self) -> None:
        amendment_list = _load("example_amendment.json")
        amendment_detail = _load("example_amendmentdetail.json")
        actions = _load("example_amendment_action.json")
        cosponsors = _load("example_amendment_cosponsor.json")
        amendments_to_amendment = _load("example_amendment_amendment.json")
        texts = _load("example_amendment_text.json")
        amendment_list["amendments"][0]["url"] = "https://api.congress.gov/v3/amendment/117/samdt/3892?format=json"
        amendment_list["pagination"]["next"] = ""
        actions["pagination"]["next"] = ""
        cosponsors["pagination"]["next"] = ""
        amendments_to_amendment["pagination"]["next"] = ""

        seed_url = with_query_params(
            "https://api.congress.gov/v3/amendment?format=json",
            {"limit": 250, "sort": "updateDate+desc"},
        )
        detail_url = canonicalize_congress_api_url("https://api.congress.gov/v3/amendment/117/samdt/3892?format=json")
        actions_url = canonicalize_congress_api_url("https://api.congress.gov/v3/amendment/117/samdt/3892/actions?format=json&limit=250")
        cosponsors_url = canonicalize_congress_api_url("https://api.congress.gov/v3/amendment/117/samdt/3892/cosponsors?format=json&limit=250")
        amendments_url = canonicalize_congress_api_url("https://api.congress.gov/v3/amendment/117/samdt/2137/amendments?format=json&limit=250")
        texts_url = canonicalize_congress_api_url("https://api.congress.gov/v3/amendment/117/samdt/3892/text?format=json&limit=250")

        client = FakeClient(
            {
                seed_url: amendment_list,
                detail_url: amendment_detail,
                actions_url: actions,
                cosponsors_url: cosponsors,
                amendments_url: amendments_to_amendment,
                texts_url: texts,
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
        self.assertIn(cosponsors_url, store.raw_by_url)
        self.assertIn(amendments_url, store.raw_by_url)
        self.assertIn(texts_url, store.raw_by_url)


if __name__ == "__main__":
    unittest.main()
