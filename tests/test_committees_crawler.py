from __future__ import annotations

import unittest
from pathlib import Path

from bicam_collection.crawler import CrawlOptions, SpecCrawler
from bicam_collection.examples import load_example_payload
from bicam_collection.spec_loader import load_spec
from bicam_collection.urls import canonicalize_congress_api_url, with_query_params


PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXAMPLES_DIR = PROJECT_ROOT / "api_schemas" / "committees"


class FakeClient:
    def __init__(self, payloads: dict[str, dict]) -> None:
        self.payloads = payloads
        self.calls: list[str] = []

    def get_json(self, url: str) -> tuple[dict, int]:
        self.calls.append(url)
        if url not in self.payloads:
            raise AssertionError(f"Unexpected URL fetch: {url}")
        return self.payloads[url], 200


class FakeStore:
    def __init__(self) -> None:
        self.raw_by_url: dict[str, dict] = {}
        self.fetch_events: list[dict] = []
        self.finished: tuple[str, int, object | None] | None = None
        self.refresh_nodes: list[object] = []

    def start_run(self, category: str, mode: str, high_water_mark=None) -> str:
        return "run-1"

    def get_latest_high_water_mark(self, category: str):
        return None

    def finish_run(self, run_id: str, status: str, items_fetched: int, high_water_mark=None) -> None:
        self.finished = (status, items_fetched, high_water_mark)

    def has_response(self, endpoint_url: str) -> bool:
        return endpoint_url in self.raw_by_url

    def list_refresh_nodes(self, *, category: str, node_type: str, limit: int) -> list[object]:
        return self.refresh_nodes[:limit]

    def upsert_response(self, **kwargs) -> str:
        self.raw_by_url[kwargs["endpoint_url"]] = kwargs
        return f'raw-{len(self.raw_by_url)}'

    def record_fetch(self, **kwargs) -> None:
        self.fetch_events.append(kwargs)


def _load(name: str) -> dict:
    return load_example_payload(EXAMPLES_DIR / name)


class CommitteeCrawlerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.spec = load_spec(PROJECT_ROOT / "specs" / "committees.yaml")

    def test_committee_crawl_follows_configured_collections_and_ignores_communications(self) -> None:
        committee_list = _load("example_committee.json")
        committee_detail = _load("example_committeedetail.json")
        committee_bills = _load("example_committee_bills.json")
        committee_reports = _load("example_committee_reports.json")
        committee_nominations = _load("example_committee_nominations.json")
        committee_list["pagination"]["next"] = ""
        committee_bills["pagination"]["next"] = ""
        committee_reports["pagination"]["next"] = ""
        committee_nominations["pagination"]["next"] = ""

        seed_url = with_query_params("https://api.congress.gov/v3/committee?format=json", {"limit": 250})
        detail_url = canonicalize_congress_api_url("https://api.congress.gov/v3/committee/senate/sseg00?format=json")
        bills_url = canonicalize_congress_api_url("https://api.congress.gov/v3/committee/senate/sseg00/bills?format=json&limit=250")
        reports_url = canonicalize_congress_api_url("https://api.congress.gov/v3/committee/senate/sseg00/reports?format=json&limit=250")
        nominations_url = canonicalize_congress_api_url("https://api.congress.gov/v3/committee/senate/sseg00/nominations?format=json&limit=250")

        client = FakeClient(
            {
                seed_url: committee_list,
                detail_url: committee_detail,
                bills_url: committee_bills,
                reports_url: committee_reports,
                nominations_url: committee_nominations,
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
        self.assertIn(bills_url, store.raw_by_url)
        self.assertIn(reports_url, store.raw_by_url)
        self.assertIn(nominations_url, store.raw_by_url)
        self.assertFalse(any("communication" in url for url in client.calls))


if __name__ == "__main__":
    unittest.main()
