from __future__ import annotations

import unittest
from pathlib import Path

from bicam_collection.crawler import CrawlOptions, SpecCrawler
from bicam_collection.examples import load_example_payload
from bicam_collection.spec_loader import load_spec
from bicam_collection.urls import canonicalize_congress_api_url, with_query_params


PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXAMPLES_DIR = PROJECT_ROOT / "api_schemas" / "committee-meetings"


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
