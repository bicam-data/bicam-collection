from __future__ import annotations

import unittest
from pathlib import Path

from bicam_collection.crawler import CrawlOptions, SpecCrawler
from bicam_collection.examples import load_example_payload
from bicam_collection.spec_loader import load_spec
from bicam_collection.urls import canonicalize_govinfo_url, with_query_params


PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXAMPLES_DIR = PROJECT_ROOT / "api_schemas" / "govinfo-committee-reports"


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
        return f"raw-{len(self.raw_by_url)}"

    def record_fetch(self, **kwargs) -> None:
        self.fetch_events.append(kwargs)


def _load(name: str) -> dict:
    return load_example_payload(EXAMPLES_DIR / name)


class GovInfoCommitteeReportCrawlerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.spec = load_spec(PROJECT_ROOT / "specs" / "govinfo-committee-reports.yaml")

    def test_govinfo_committee_report_crawl_follows_package_and_granule_links(self) -> None:
        package_list = _load("example_collection.json")
        package_summary = _load("example_package_summary.json")
        granule_list = _load("example_granules.json")
        granule_summary = _load("example_granule_summary.json")
        package_list["packages"] = [package_list["packages"][1]]
        package_list["count"] = 1
        package_list["nextPage"] = None

        seed_url = with_query_params(
            "https://api.govinfo.gov/collections/CRPT/2025-10-08T00:00:00Z/2025-10-08T23:59:59Z",
            {"offsetMark": "*", "pageSize": 100},
        )
        package_url = canonicalize_govinfo_url("https://api.govinfo.gov/packages/CRPT-119hrpt173/summary")
        granules_url = with_query_params(
            canonicalize_govinfo_url("https://api.govinfo.gov/packages/CRPT-119hrpt173/granules?offsetMark=*&pageSize=100"),
            {"offsetMark": "*", "pageSize": 100},
        )
        granule_detail_url = canonicalize_govinfo_url(
            "https://api.govinfo.gov/packages/CRPT-119hrpt173/granules/CRPT-119hrpt173/summary"
        )

        client = FakeClient(
            {
                seed_url: package_list,
                package_url: package_summary,
                granules_url: granule_list,
                granule_detail_url: granule_summary,
            }
        )
        store = FakeStore()
        crawler = SpecCrawler(
            base_url="https://api.govinfo.gov",
            spec=self.spec,
            client=client,
            store=store,
        )

        crawler.run(
            CrawlOptions(
                mode="incremental",
                from_datetime="2025-10-08T00:00:00Z",
                to_datetime="2025-10-08T23:59:59Z",
            )
        )

        self.assertEqual(
            set(store.raw_by_url),
            {seed_url, package_url, granules_url, granule_detail_url},
        )


if __name__ == "__main__":
    unittest.main()
