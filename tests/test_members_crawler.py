from __future__ import annotations

import unittest
from pathlib import Path

from crawler_fakes import FakeClient, FakeStore
from bicam_collection.crawler import CrawlOptions, SpecCrawler
from bicam_collection.examples import load_example_payload
from bicam_collection.spec_loader import load_spec
from bicam_collection.storage import StoredNode
from bicam_collection.urls import canonicalize_congress_api_url, with_query_params


PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXAMPLES_DIR = PROJECT_ROOT / "api_schemas" / "members"


def _load(name: str) -> dict:
    return load_example_payload(EXAMPLES_DIR / name)


class MemberCrawlerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.spec = load_spec(PROJECT_ROOT / "specs" / "members.yaml")

    def test_recursive_crawl_follows_member_collection_urls_only(self) -> None:
        member_list = _load("example_member.json")
        member_detail = _load("example_memberdetail.json")
        sponsored = _load("example_member_sponsored_legislation.json")
        cosponsored = _load("example_member_cosponsored_legislation.json")

        member_list["members"][0]["url"] = "https://api.congress.gov/v3/member/L000174?format=json"
        member_list["pagination"]["next"] = ""
        sponsored["pagination"]["next"] = ""
        cosponsored["pagination"]["next"] = ""

        seed_url = with_query_params("https://api.congress.gov/v3/member?format=json", {"limit": 250})
        detail_url = canonicalize_congress_api_url("https://api.congress.gov/v3/member/L000174?format=json")
        sponsored_url = canonicalize_congress_api_url(
            "https://api.congress.gov/v3/member/L000174/sponsored-legislation?format=json&limit=250"
        )
        cosponsored_url = canonicalize_congress_api_url(
            "https://api.congress.gov/v3/member/L000174/cosponsored-legislation?format=json&limit=250"
        )

        client = FakeClient(
            {
                seed_url: member_list,
                detail_url: member_detail,
                sponsored_url: sponsored,
                cosponsored_url: cosponsored,
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

        self.assertIn(sponsored_url, store.raw_by_url)
        self.assertIn(cosponsored_url, store.raw_by_url)
        self.assertFalse(any("/amendment/" in url for url in client.calls))

    def test_refresh_descendants_uses_member_detail_roots(self) -> None:
        member_detail = _load("example_memberdetail.json")
        sponsored = _load("example_member_sponsored_legislation.json")
        cosponsored = _load("example_member_cosponsored_legislation.json")
        sponsored["pagination"]["next"] = ""
        cosponsored["pagination"]["next"] = ""

        sponsored_url = canonicalize_congress_api_url(
            "https://api.congress.gov/v3/member/L000174/sponsored-legislation?format=json&limit=250"
        )
        cosponsored_url = canonicalize_congress_api_url(
            "https://api.congress.gov/v3/member/L000174/cosponsored-legislation?format=json&limit=250"
        )
        client = FakeClient(
            {
                sponsored_url: sponsored,
                cosponsored_url: cosponsored,
            }
        )
        store = FakeStore()
        store.refresh_nodes = [
            StoredNode(raw_id="raw-member-1", endpoint_url=canonicalize_congress_api_url("https://api.congress.gov/v3/member/L000174?format=json"), response=member_detail)
        ]
        crawler = SpecCrawler(
            base_url="https://api.congress.gov/v3",
            spec=self.spec,
            client=client,
            store=store,
        )

        crawler.run(CrawlOptions(mode="refresh_descendants", batch_size=1))

        self.assertEqual(client.calls, [sponsored_url, cosponsored_url])
        self.assertIn(sponsored_url, store.raw_by_url)
        self.assertIn(cosponsored_url, store.raw_by_url)


if __name__ == "__main__":
    unittest.main()
