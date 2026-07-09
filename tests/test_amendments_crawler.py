from __future__ import annotations

import unittest
from pathlib import Path

from bicam_collection.crawler import CrawlOptions, SpecCrawler
from bicam_collection.examples import load_example_payload
from bicam_collection.spec_loader import load_spec
from bicam_collection.urls import canonicalize_congress_api_url, with_query_params


PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXAMPLES_DIR = PROJECT_ROOT / "api_schemas" / "amendments"


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

        seed_url = with_query_params("https://api.congress.gov/v3/amendment?format=json", {"limit": 250})
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
