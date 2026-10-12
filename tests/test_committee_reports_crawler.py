from __future__ import annotations

import unittest
from pathlib import Path

from crawler_fakes import FakeClient, FakeStore
from bicam_collection.crawler import CrawlOptions, SpecCrawler
from bicam_collection.examples import load_example_payload
from bicam_collection.spec_loader import load_spec
from bicam_collection.urls import canonicalize_congress_api_url, with_query_params


PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXAMPLES_DIR = PROJECT_ROOT / "api_schemas" / "committee-reports"


def _load(name: str) -> dict:
    return load_example_payload(EXAMPLES_DIR / name)


class CommitteeReportCrawlerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.spec = load_spec(PROJECT_ROOT / "specs" / "committee-reports.yaml")

    def test_committee_report_crawl_follows_detail_and_text_child(self) -> None:
        report_list = _load("example_committee_report.json")
        report_detail = _load("example_committee_reportdetail.json")
        report_text = _load("example_committee_report_text.json")
        report_list["pagination"]["next"] = ""

        seed_url = with_query_params("https://api.congress.gov/v3/committee-report?format=json", {"limit": 250})
        detail_url = canonicalize_congress_api_url("https://api.congress.gov/v3/committee-report/119/HRPT/1?format=json")
        text_url = canonicalize_congress_api_url("https://api.congress.gov/v3/committee-report/119/HRPT/1/text?format=json&limit=250")

        client = FakeClient(
            {
                seed_url: report_list,
                detail_url: report_detail,
                text_url: report_text,
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
        self.assertIn(text_url, store.raw_by_url)


if __name__ == "__main__":
    unittest.main()
