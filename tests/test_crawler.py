from __future__ import annotations

import asyncio
import unittest
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

from crawler_fakes import AsyncFakeClient, AsyncFakeStore, FakeClient, FakeStore, http_error
from bicam_collection.client import FetchMetadata
from bicam_collection.crawler import AsyncSpecCrawler, CrawlOptions, SpecCrawler
from bicam_collection.examples import load_example_payload
from bicam_collection.spec_loader import load_spec
from bicam_collection.storage import StoredNode
from bicam_collection.urls import canonicalize_congress_api_url, with_query_params


PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXAMPLES_DIR = PROJECT_ROOT / "api_schemas" / "bills"
COMMITTEE_REPORT_EXAMPLES_DIR = PROJECT_ROOT / "api_schemas" / "committee-reports"


class AsyncTrackingFakeClient(AsyncFakeClient):
    def __init__(self, payloads: dict[str, dict], delays: dict[str, float]) -> None:
        super().__init__(payloads)
        self.delays = delays
        self.active = 0
        self.max_active = 0

    async def get_json(self, url: str, endpoint_name: str | None = None, **kwargs: object):  # type: ignore[override]
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            await asyncio.sleep(self.delays.get(url, 0.0))
            return await super().get_json(url)
        finally:
            self.active -= 1


def _load(name: str) -> dict:
    return load_example_payload(EXAMPLES_DIR / name)


class CrawlerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.spec = load_spec(PROJECT_ROOT / "specs" / "bills.yaml")

    def test_partition_required_seed_rejects_bare_law_route(self) -> None:
        spec = load_spec(PROJECT_ROOT / "specs" / "laws.yaml")
        crawler = SpecCrawler(
            base_url="https://api.congress.gov/v3",
            spec=spec,
            client=FakeClient({}),
            store=FakeStore(),
        )

        with self.assertRaisesRegex(ValueError, "requires an explicit partitioned seed path"):
            crawler.run(CrawlOptions(mode="full"))

    def test_recursive_crawl_only_follows_collection_urls(self) -> None:
        bill_list = _load("example_bill.md")
        bill_detail = _load("example_billdetail.md")
        actions = _load("example_bill_action.md")
        amendments = _load("example_bill_amendment.md")
        committees = _load("example_bill_committee.md")
        cosponsors = _load("example_bill_cosponsor.md")
        related = _load("example_bill_relatedbill.md")
        subjects = _load("example_bill_subject.md")
        summaries = _load("example_bill_summary.md")
        text_versions = _load("example_bill_text.md")
        titles = _load("example_bill_title.md")
        committee_report_detail = load_example_payload(COMMITTEE_REPORT_EXAMPLES_DIR / "example_committee_reportdetail.json")
        bill_list["pagination"]["next"] = ""
        actions["pagination"]["next"] = ""
        actions["pagination"]["prev"] = ""
        amendments["pagination"]["next"] = ""
        cosponsors["pagination"]["next"] = ""

        seed_url = with_query_params("https://api.congress.gov/v3/bill?format=json", {"limit": 250})
        detail_url = canonicalize_congress_api_url("http://api.congress.gov/v3/bill/119/s/3281?format=xml")

        payloads = {
            seed_url: bill_list,
            detail_url: bill_detail,
            canonicalize_congress_api_url("https://api.congress.gov/v3/bill/117/hr/3076/actions?format=json&limit=250"): actions,
            canonicalize_congress_api_url("https://api.congress.gov/v3/bill/117/hr/3076/amendments?format=json&limit=250"): amendments,
            canonicalize_congress_api_url("https://api.congress.gov/v3/bill/117/hr/3076/committees?format=json&limit=250"): committees,
            canonicalize_congress_api_url("https://api.congress.gov/v3/bill/117/hr/3076/cosponsors?format=json&limit=250"): cosponsors,
            canonicalize_congress_api_url("https://api.congress.gov/v3/bill/117/hr/3076/relatedbills?format=json&limit=250"): related,
            canonicalize_congress_api_url("https://api.congress.gov/v3/bill/117/hr/3076/subjects?format=json&limit=250"): subjects,
            canonicalize_congress_api_url("https://api.congress.gov/v3/bill/117/hr/3076/summaries?format=json&limit=250"): summaries,
            canonicalize_congress_api_url("https://api.congress.gov/v3/bill/117/hr/3076/text?format=json&limit=250"): text_versions,
            canonicalize_congress_api_url("https://api.congress.gov/v3/bill/117/hr/3076/titles?format=json&limit=250"): titles,
            canonicalize_congress_api_url("https://api.congress.gov/v3/committee-report/117/HRPT/89?format=json"): committee_report_detail,
        }

        client = FakeClient(payloads)
        store = FakeStore()
        crawler = SpecCrawler(
            base_url="https://api.congress.gov/v3",
            spec=self.spec,
            client=client,
            store=store,
        )

        run_id = crawler.run(CrawlOptions(mode="full"))

        self.assertEqual(run_id, "run-1")
        self.assertEqual(store.finished, ("completed", len(store.raw_by_url), datetime(2025, 11, 21, 0, 0, tzinfo=timezone.utc)))
        self.assertIn(canonicalize_congress_api_url("https://api.congress.gov/v3/bill/117/hr/3076/relatedbills?format=json&limit=250"), store.raw_by_url)
        self.assertNotIn(canonicalize_congress_api_url("https://api.congress.gov/v3/bill/117/s/1720?format=json"), store.raw_by_url)

        called_hosts = {urlparse(url).hostname for url in client.calls}
        self.assertEqual(called_hosts, {"api.congress.gov"})
        self.assertFalse(any("senate.gov" in url for url in client.calls))
        self.assertFalse(any("congress.gov/117/bills" in url for url in client.calls))
        self.assertFalse(any("/member/" in url for url in client.calls))
        self.assertEqual(
            [url for url in client.calls if "/committee-report/" in url],
            [canonicalize_congress_api_url("https://api.congress.gov/v3/committee-report/117/HRPT/89?format=json")],
        )
        self.assertFalse(any("/committee-report/117/HRPT/89/text" in url for url in client.calls))
        self.assertFalse(any("/amendment/" in url for url in client.calls))
        self.assertFalse(any("/committee/house/" in url for url in client.calls))
        self.assertFalse(any("/bill/117/s/1720?format=json" in url for url in client.calls))

    def test_pagination_next_is_followed(self) -> None:
        first_page = _load("example_bill.md")
        second_page = {"bills": [], "pagination": {"count": 425279, "next": "", "prev": ""}, "request": {"format": "json"}}
        first_page["bills"] = []
        first_page["pagination"]["next"] = "https://api.congress.gov/v3/bill?format=json&limit=250&offset=20"

        payloads = {
            "https://api.congress.gov/v3/bill?format=json&limit=250": first_page,
            "https://api.congress.gov/v3/bill?format=json&limit=250&offset=20": second_page,
        }
        client = FakeClient(payloads)
        store = FakeStore()
        crawler = SpecCrawler(
            base_url="https://api.congress.gov/v3",
            spec=self.spec,
            client=client,
            store=store,
        )

        crawler.run(CrawlOptions(mode="full", max_pages=2))
        self.assertEqual(client.calls, list(payloads.keys()))

    def test_max_pages_applies_per_pagination_stream_not_globally_per_node_type(self) -> None:
        bill_list = {
            "bills": [
                {"url": "https://api.congress.gov/v3/bill/119/hr/1?format=json"},
                {"url": "https://api.congress.gov/v3/bill/119/hr/2?format=json"},
            ],
            "pagination": {"count": 2, "next": "", "prev": ""},
            "request": {"format": "json"},
        }
        detail_one = _load("example_billdetail.md")
        detail_two = _load("example_billdetail.md")
        detail_one["bill"]["actions"]["url"] = "https://api.congress.gov/v3/bill/119/hr/1/actions?format=json"
        detail_two["bill"]["actions"]["url"] = "https://api.congress.gov/v3/bill/119/hr/2/actions?format=json"
        for payload in (detail_one, detail_two):
            payload["bill"]["amendments"]["url"] = ""
            payload["bill"]["committees"]["url"] = ""
            payload["bill"]["cosponsors"]["url"] = ""
            payload["bill"]["relatedBills"]["url"] = ""
            payload["bill"]["subjects"]["url"] = ""
            payload["bill"]["summaries"]["url"] = ""
            payload["bill"]["textVersions"]["url"] = ""
            payload["bill"]["titles"]["url"] = ""
            payload["bill"]["committeeReports"] = []
        actions_one_page_one = {"actions": [], "pagination": {"count": 2, "next": "https://api.congress.gov/v3/bill/119/hr/1/actions?format=json&limit=250&offset=20", "prev": ""}, "request": {"format": "json"}}
        actions_one_page_two = {"actions": [], "pagination": {"count": 2, "next": "", "prev": ""}, "request": {"format": "json"}}
        actions_two_page_one = {"actions": [], "pagination": {"count": 2, "next": "https://api.congress.gov/v3/bill/119/hr/2/actions?format=json&limit=250&offset=20", "prev": ""}, "request": {"format": "json"}}
        actions_two_page_two = {"actions": [], "pagination": {"count": 2, "next": "", "prev": ""}, "request": {"format": "json"}}

        payloads = {
            "https://api.congress.gov/v3/bill?format=json&limit=250": bill_list,
            "https://api.congress.gov/v3/bill/119/hr/1?format=json": detail_one,
            "https://api.congress.gov/v3/bill/119/hr/2?format=json": detail_two,
            "https://api.congress.gov/v3/bill/119/hr/1/actions?format=json&limit=250": actions_one_page_one,
            "https://api.congress.gov/v3/bill/119/hr/1/actions?format=json&limit=250&offset=20": actions_one_page_two,
            "https://api.congress.gov/v3/bill/119/hr/2/actions?format=json&limit=250": actions_two_page_one,
            "https://api.congress.gov/v3/bill/119/hr/2/actions?format=json&limit=250&offset=20": actions_two_page_two,
        }
        client = FakeClient(payloads)
        store = FakeStore()
        crawler = SpecCrawler(
            base_url="https://api.congress.gov/v3",
            spec=self.spec,
            client=client,
            store=store,
        )

        crawler.run(CrawlOptions(mode="full", max_pages=2))

        self.assertIn("https://api.congress.gov/v3/bill/119/hr/1/actions?format=json&limit=250&offset=20", client.calls)
        self.assertIn("https://api.congress.gov/v3/bill/119/hr/2/actions?format=json&limit=250&offset=20", client.calls)

    def test_incremental_params_only_applied_to_supported_nodes(self) -> None:
        bill_list = _load("example_bill.md")
        bill_detail = _load("example_billdetail.md")
        committees = _load("example_bill_committee.md")
        bill_list["pagination"]["next"] = ""
        bill_detail["bill"]["actions"]["url"] = ""
        bill_detail["bill"]["amendments"]["url"] = ""
        bill_detail["bill"]["cosponsors"]["url"] = ""
        bill_detail["bill"]["relatedBills"]["url"] = ""
        bill_detail["bill"]["subjects"]["url"] = ""
        bill_detail["bill"]["summaries"]["url"] = ""
        bill_detail["bill"]["textVersions"]["url"] = ""
        bill_detail["bill"]["titles"]["url"] = ""
        bill_detail["bill"]["committeeReports"] = []
        bill_detail["bill"]["sponsors"] = []
        bill_detail["bill"]["committeeReports"] = []
        bill_detail["bill"]["onBehalfOfSponsor"]["url"] = ""
        committees["committees"][0]["url"] = ""
        committees["committees"][0]["subcommittees"][0]["url"] = ""
        payloads = {
            "https://api.congress.gov/v3/bill?format=json&fromDateTime=2026-01-01T00%3A00%3A00Z&limit=250": bill_list,
            canonicalize_congress_api_url("http://api.congress.gov/v3/bill/119/s/3281?format=xml"): bill_detail,
            canonicalize_congress_api_url("https://api.congress.gov/v3/bill/117/hr/3076/committees?format=json&limit=250"): committees,
        }
        client = FakeClient(payloads)
        store = FakeStore()
        crawler = SpecCrawler(
            base_url="https://api.congress.gov/v3",
            spec=self.spec,
            client=client,
            store=store,
        )
        crawler.run(CrawlOptions(mode="incremental", from_datetime="2026-01-01T00:00:00Z", max_pages=1))

        self.assertIn("fromDateTime=2026-01-01T00%3A00%3A00Z", client.calls[0])
        committee_calls = [url for url in client.calls if "/committees?" in url]
        self.assertEqual(len(committee_calls), 1)
        self.assertNotIn("fromDateTime=", committee_calls[0])

    def test_incremental_skips_existing_urls_and_children(self) -> None:
        bill_list = _load("example_bill.md")
        bill_detail = _load("example_billdetail.md")
        bill_list["pagination"]["next"] = ""

        seed_url = "https://api.congress.gov/v3/bill?format=json&fromDateTime=2026-01-01T00%3A00%3A00Z&limit=250"
        detail_url = canonicalize_congress_api_url("http://api.congress.gov/v3/bill/119/s/3281?format=xml")
        client = FakeClient({seed_url: bill_list, detail_url: bill_detail})
        store = FakeStore()
        store.add_stored(detail_url, {})
        crawler = SpecCrawler(
            base_url="https://api.congress.gov/v3",
            spec=self.spec,
            client=client,
            store=store,
        )

        crawler.run(CrawlOptions(mode="resume", from_datetime="2026-01-01T00:00:00Z", max_pages=1))

        self.assertEqual(client.calls, [seed_url])
        self.assertNotIn(detail_url, store.raw_by_url)
        self.assertIn(
            {
                "crawl_run_id": "run-1",
                "endpoint_url": detail_url,
                "raw_response_id": None,
                "raw_response_version_id": None,
                "parent_endpoint_url": seed_url,
                "node_type": "bill_detail",
                "follow_mode": "recurse",
                "http_status": None,
                "outcome": "skipped_existing",
            },
            store.fetch_events,
        )

    def test_incremental_refetches_existing_urls(self) -> None:
        bill_list = _load("example_bill.md")
        bill_detail = _load("example_billdetail.md")
        bill_list["pagination"]["next"] = ""
        bill_detail["bill"]["actions"]["url"] = ""
        bill_detail["bill"]["amendments"]["url"] = ""
        bill_detail["bill"]["committees"]["url"] = ""
        bill_detail["bill"]["cosponsors"]["url"] = ""
        bill_detail["bill"]["relatedBills"]["url"] = ""
        bill_detail["bill"]["subjects"]["url"] = ""
        bill_detail["bill"]["summaries"]["url"] = ""
        bill_detail["bill"]["textVersions"]["url"] = ""
        bill_detail["bill"]["titles"]["url"] = ""
        bill_detail["bill"]["committeeReports"] = []

        seed_url = "https://api.congress.gov/v3/bill?format=json&fromDateTime=2026-01-01T00%3A00%3A00Z&limit=250"
        detail_url = canonicalize_congress_api_url("http://api.congress.gov/v3/bill/119/s/3281?format=xml")
        client = FakeClient({seed_url: bill_list, detail_url: bill_detail})
        store = FakeStore()
        store.add_stored(detail_url, {})
        crawler = SpecCrawler(
            base_url="https://api.congress.gov/v3",
            spec=self.spec,
            client=client,
            store=store,
        )

        crawler.run(CrawlOptions(mode="incremental", from_datetime="2026-01-01T00:00:00Z", max_pages=1))

        self.assertEqual(client.calls, [seed_url, detail_url])
        self.assertIn(detail_url, store.raw_by_url)

    def test_incremental_does_not_rewalk_unchanged_detail_children(self) -> None:
        bill_list = _load("example_bill.md")
        bill_detail = _load("example_billdetail.md")
        bill_list["pagination"]["next"] = ""
        seed_url = "https://api.congress.gov/v3/bill?format=json&fromDateTime=2026-01-01T00%3A00%3A00Z&limit=250"
        detail_url = canonicalize_congress_api_url(
            "http://api.congress.gov/v3/bill/119/s/3281?format=xml"
        )
        client = FakeClient({seed_url: bill_list, detail_url: bill_detail})
        store = FakeStore()
        store.add_stored(detail_url, bill_detail, "raw-existing-detail")
        crawler = SpecCrawler(
            base_url="https://api.congress.gov/v3",
            spec=self.spec,
            client=client,
            store=store,
        )

        crawler.run(
            CrawlOptions(
                mode="incremental",
                from_datetime="2026-01-01T00:00:00Z",
                max_pages=1,
            )
        )

        self.assertEqual(client.calls, [seed_url, detail_url])

    def test_incremental_uses_latest_high_water_mark_when_from_datetime_is_omitted(self) -> None:
        bill_list = _load("example_bill.md")
        bill_list["bills"] = []
        bill_list["pagination"]["next"] = ""
        latest_high_water_mark = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
        seed_url = "https://api.congress.gov/v3/bill?format=json&fromDateTime=2026-01-02T03%3A04%3A05Z&limit=250"

        client = FakeClient({seed_url: bill_list})
        store = FakeStore(latest_high_water_mark=latest_high_water_mark)
        crawler = SpecCrawler(
            base_url="https://api.congress.gov/v3",
            spec=self.spec,
            client=client,
            store=store,
        )

        crawler.run(CrawlOptions(mode="incremental", max_pages=1))

        self.assertEqual(client.calls[0], seed_url)

    def test_resume_uses_latest_high_water_mark_when_from_datetime_is_omitted(self) -> None:
        bill_list = _load("example_bill.md")
        bill_list["bills"] = []
        bill_list["pagination"]["next"] = ""
        latest_high_water_mark = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
        seed_url = "https://api.congress.gov/v3/bill?format=json&fromDateTime=2026-01-02T03%3A04%3A05Z&limit=250"

        client = FakeClient({seed_url: bill_list})
        store = FakeStore(latest_high_water_mark=latest_high_water_mark)
        crawler = SpecCrawler(
            base_url="https://api.congress.gov/v3",
            spec=self.spec,
            client=client,
            store=store,
        )

        crawler.run(CrawlOptions(mode="resume", max_pages=1))

        self.assertEqual(client.calls[0], seed_url)

    def test_incremental_high_water_mark_does_not_move_backward_from_effective_from_datetime(self) -> None:
        bill_list = _load("example_bill.md")
        bill_list["bills"] = [bill_list["bills"][0]]
        bill_list["bills"][0]["updateDate"] = "2026-03-28"
        bill_list["pagination"]["next"] = ""

        seed_url = "https://api.congress.gov/v3/bill?format=json&fromDateTime=2026-03-28T18%3A00%3A00Z&limit=250"
        detail_url = canonicalize_congress_api_url("http://api.congress.gov/v3/bill/119/s/3281?format=xml")
        detail_payload = _load("example_billdetail.md")
        detail_payload["bill"]["actions"]["url"] = ""
        detail_payload["bill"]["amendments"]["url"] = ""
        detail_payload["bill"]["committees"]["url"] = ""
        detail_payload["bill"]["cosponsors"]["url"] = ""
        detail_payload["bill"]["relatedBills"]["url"] = ""
        detail_payload["bill"]["subjects"]["url"] = ""
        detail_payload["bill"]["summaries"]["url"] = ""
        detail_payload["bill"]["textVersions"]["url"] = ""
        detail_payload["bill"]["titles"]["url"] = ""
        detail_payload["bill"]["committeeReports"] = []

        client = FakeClient({seed_url: bill_list, detail_url: detail_payload})
        store = FakeStore()
        crawler = SpecCrawler(
            base_url="https://api.congress.gov/v3",
            spec=self.spec,
            client=client,
            store=store,
        )

        crawler.run(CrawlOptions(mode="incremental", from_datetime="2026-03-28T18:00:00Z", max_pages=1))

        self.assertEqual(store.finished, ("completed", len(store.raw_by_url), datetime(2026, 3, 28, 18, 0, tzinfo=timezone.utc)))

    def test_page_size_overrides_default_limit(self) -> None:
        bill_list = {"bills": [], "pagination": {"count": 0, "next": "", "prev": ""}, "request": {"format": "json"}}
        payloads = {
            "https://api.congress.gov/v3/bill?format=json&limit=25": bill_list,
        }
        client = FakeClient(payloads)
        store = FakeStore()
        crawler = SpecCrawler(
            base_url="https://api.congress.gov/v3",
            spec=self.spec,
            client=client,
            store=store,
        )

        crawler.run(CrawlOptions(mode="full", max_pages=1, page_size=25))

        self.assertEqual(client.calls, ["https://api.congress.gov/v3/bill?format=json&limit=25"])

    def test_page_size_overrides_limit_in_explicit_seed_path(self) -> None:
        cosponsor_list = {
            "cosponsors": [],
            "pagination": {"count": 0, "next": "", "prev": ""},
            "request": {"format": "json"},
        }
        expected_url = (
            "https://api.congress.gov/v3/bill/117/hr/1/cosponsors"
            "?format=json&limit=25"
        )
        client = FakeClient({expected_url: cosponsor_list})
        store = FakeStore()
        crawler = SpecCrawler(
            base_url="https://api.congress.gov/v3",
            spec=self.spec,
            client=client,
            store=store,
        )

        crawler.run(
            CrawlOptions(
                mode="refresh",
                seed_path="/bill/117/hr/1/cosponsors?format=json&limit=250",
                seed_node_type="bill_cosponsors",
                max_pages=1,
                page_size=25,
            )
        )

        self.assertEqual(client.calls, [expected_url])

    def test_page_size_override_preserves_govinfo_next_page_cursor(self) -> None:
        spec = load_spec(PROJECT_ROOT / "specs" / "govinfo-congressional-directories.yaml")
        crawler = SpecCrawler(
            base_url="https://api.govinfo.gov",
            spec=spec,
            client=FakeClient({}),
            store=FakeStore(),
        )
        node = spec.nodes["govinfo_congressional_directory_package_list"]

        augmented = crawler._augment_url(
            "https://api.govinfo.gov/collections/CDIR/1900-01-01/2026-08-31"
            "?offsetMark=next-cursor&pageSize=100",
            node,
            CrawlOptions(mode="full", page_size=25),
        )

        self.assertIn("offsetMark=next-cursor", augmented)
        self.assertIn("pageSize=25", augmented)
        self.assertNotIn("offsetMark=%2A", augmented)

    def test_refresh_descendants_uses_stored_bill_details_as_roots(self) -> None:
        bill_detail = _load("example_billdetail.md")
        bill_detail["bill"]["amendments"]["url"] = ""
        bill_detail["bill"]["committees"]["url"] = ""
        bill_detail["bill"]["cosponsors"]["url"] = ""
        bill_detail["bill"]["relatedBills"]["url"] = ""
        bill_detail["bill"]["subjects"]["url"] = ""
        bill_detail["bill"]["summaries"]["url"] = ""
        bill_detail["bill"]["textVersions"]["url"] = ""
        bill_detail["bill"]["titles"]["url"] = ""
        bill_detail["bill"]["committeeReports"] = []
        actions = _load("example_bill_action.md")
        actions["pagination"]["next"] = ""
        actions["pagination"]["prev"] = ""

        action_url = canonicalize_congress_api_url("https://api.congress.gov/v3/bill/117/hr/3076/actions?format=json&limit=250")
        client = FakeClient({action_url: actions})
        store = FakeStore()
        store.refresh_nodes = [
            StoredNode(raw_id="raw-bill-1", endpoint_url=canonicalize_congress_api_url("http://api.congress.gov/v3/bill/119/s/3281?format=xml"), response=bill_detail)
        ]
        crawler = SpecCrawler(
            base_url="https://api.congress.gov/v3",
            spec=self.spec,
            client=client,
            store=store,
        )

        run_id = crawler.run(CrawlOptions(mode="refresh_descendants", batch_size=1))

        self.assertEqual(run_id, "run-1")
        self.assertEqual(client.calls, [action_url])
        self.assertIn(action_url, store.raw_by_url)
        self.assertEqual(store.finished, ("completed", 1, None))

    def test_backfill_descendants_reuses_stored_parents_and_fetches_missing_children(self) -> None:
        bill_list = _load("example_bill.md")
        bill_detail = _load("example_billdetail.md")
        actions = _load("example_bill_action.md")
        bill_list["pagination"]["next"] = ""
        bill_detail["bill"]["amendments"]["url"] = ""
        bill_detail["bill"]["committees"]["url"] = ""
        bill_detail["bill"]["cosponsors"]["url"] = ""
        bill_detail["bill"]["relatedBills"]["url"] = ""
        bill_detail["bill"]["subjects"]["url"] = ""
        bill_detail["bill"]["summaries"]["url"] = ""
        bill_detail["bill"]["textVersions"]["url"] = ""
        bill_detail["bill"]["titles"]["url"] = ""
        bill_detail["bill"]["committeeReports"] = []
        actions["pagination"]["next"] = ""
        actions["pagination"]["prev"] = ""

        seed_url = with_query_params("https://api.congress.gov/v3/bill?format=json", {"limit": 250})
        detail_url = canonicalize_congress_api_url("http://api.congress.gov/v3/bill/119/s/3281?format=xml")
        action_url = canonicalize_congress_api_url("https://api.congress.gov/v3/bill/117/hr/3076/actions?format=json&limit=250")
        client = FakeClient({action_url: actions})
        store = FakeStore()
        store.add_stored(seed_url, bill_list, "raw-seed")
        store.add_stored(detail_url, bill_detail, "raw-detail")

        crawler = SpecCrawler(base_url="https://api.congress.gov/v3", spec=self.spec, client=client, store=store)
        crawler.run(CrawlOptions(mode="backfill_descendants", max_pages=1))

        self.assertEqual(client.calls, [action_url])
        self.assertIn(action_url, store.raw_by_url)
        self.assertTrue(any(event["outcome"] == "skipped_existing_backfill" for event in store.fetch_events))

    def test_failed_request_is_recorded_and_run_continues(self) -> None:
        bill_list = _load("example_bill.md")
        bill_detail = _load("example_billdetail.md")
        bill_list["pagination"]["next"] = ""
        bill_detail["bill"]["amendments"]["url"] = ""
        bill_detail["bill"]["committees"]["url"] = ""
        bill_detail["bill"]["cosponsors"]["url"] = ""
        bill_detail["bill"]["relatedBills"]["url"] = ""
        bill_detail["bill"]["subjects"]["url"] = ""
        bill_detail["bill"]["summaries"]["url"] = ""
        bill_detail["bill"]["titles"]["url"] = ""
        bill_detail["bill"]["committeeReports"] = []
        text_url = canonicalize_congress_api_url("https://api.congress.gov/v3/bill/117/hr/3076/text?format=json&limit=250")

        seed_url = with_query_params("https://api.congress.gov/v3/bill?format=json", {"limit": 250})
        detail_url = canonicalize_congress_api_url("http://api.congress.gov/v3/bill/119/s/3281?format=xml")

        client = FakeClient(
            {
                seed_url: bill_list,
                detail_url: bill_detail,
            },
            failures={text_url: http_error(500, "500 Server Error", text_url)},
        )
        store = FakeStore()
        crawler = SpecCrawler(
            base_url="https://api.congress.gov/v3",
            spec=self.spec,
            client=client,
            store=store,
        )

        run_id = crawler.run(CrawlOptions(mode="full", max_pages=1))

        self.assertEqual(run_id, "run-1")
        self.assertEqual(store.finished, ("completed", len(store.raw_by_url), datetime(2025, 11, 21, 0, 0, tzinfo=timezone.utc)))
        failed_request = next(request for request in store.failed_requests if request["endpoint_url"] == text_url)
        self.assertEqual(failed_request["crawl_run_id"], "run-1")
        self.assertEqual(failed_request["category"], "bills")
        self.assertEqual(failed_request["node_type"], "bill_text")
        self.assertEqual(failed_request["parent_endpoint_url"], detail_url)
        self.assertEqual(failed_request["http_status"], 500)
        self.assertEqual(failed_request["error"], "500 Server Error")

        failed_fetch = next(fetch for fetch in store.fetch_events if fetch["endpoint_url"] == text_url)
        self.assertEqual(failed_fetch["crawl_run_id"], "run-1")
        self.assertIsNone(failed_fetch["raw_response_id"])
        self.assertEqual(failed_fetch["parent_endpoint_url"], detail_url)
        self.assertEqual(failed_fetch["node_type"], "bill_text")
        self.assertEqual(failed_fetch["follow_mode"], "recurse")
        self.assertEqual(failed_fetch["http_status"], 500)
        self.assertEqual(failed_fetch["outcome"], "failed")

    def test_fetch_telemetry_is_recorded_for_successful_requests(self) -> None:
        bill_list = {"bills": [], "pagination": {"count": 0, "next": "", "prev": ""}, "request": {"format": "json"}}
        seed_url = with_query_params("https://api.congress.gov/v3/bill?format=json", {"limit": 250})
        client = FakeClient(
            {seed_url: bill_list},
            metadata=FetchMetadata(
                status_code=200,
                api_key_name="alpha",
                duration_ms=321,
                attempt_count=2,
                rate_limit_attempts=1,
                pool_allocation={"endpoint_name": "bills", "selected_key_name": "alpha"},
            ),
        )
        store = FakeStore()
        crawler = SpecCrawler(
            base_url="https://api.congress.gov/v3",
            spec=self.spec,
            client=client,
            store=store,
        )

        crawler.run(CrawlOptions(mode="full", max_pages=1))

        fetch = next(event for event in store.fetch_events if event["endpoint_url"] == seed_url)
        self.assertEqual(fetch["duration_ms"], 321)
        self.assertEqual(fetch["attempt_count"], 2)
        self.assertEqual(fetch["rate_limit_attempts"], 1)
        self.assertEqual(fetch["api_key_name"], "alpha")
        self.assertEqual(fetch["pool_allocation"], {"endpoint_name": "bills", "selected_key_name": "alpha"})
        self.assertIn(
            {
                "crawl_run_id": "run-1",
                "endpoint_url": seed_url,
                "parent_endpoint_url": None,
                "node_type": "bill_list",
                "follow_mode": "recurse",
            },
            store.expected_fetches,
        )



class AsyncCrawlerTests(unittest.IsolatedAsyncioTestCase):
    async def test_page_size_override_preserves_govinfo_next_page_cursor(self) -> None:
        spec = load_spec(PROJECT_ROOT / "specs" / "govinfo-congressional-directories.yaml")
        crawler = AsyncSpecCrawler(
            base_url="https://api.govinfo.gov",
            spec=spec,
            client=AsyncFakeClient({}),
            store=AsyncFakeStore(),
        )
        node = spec.nodes["govinfo_congressional_directory_package_list"]

        augmented = crawler._augment_url(
            "https://api.govinfo.gov/collections/CDIR/1900-01-01/2026-08-31"
            "?offsetMark=next-cursor&pageSize=100",
            node,
            CrawlOptions(mode="full", page_size=25),
        )

        self.assertIn("offsetMark=next-cursor", augmented)
        self.assertIn("pageSize=25", augmented)
        self.assertNotIn("offsetMark=%2A", augmented)

    def setUp(self) -> None:
        self.spec = load_spec(PROJECT_ROOT / "specs" / "bills.yaml")

    async def test_partition_required_seed_rejects_bare_law_route(self) -> None:
        spec = load_spec(PROJECT_ROOT / "specs" / "laws.yaml")
        crawler = AsyncSpecCrawler(
            base_url="https://api.congress.gov/v3",
            spec=spec,
            client=AsyncFakeClient({}),
            store=AsyncFakeStore(),
        )

        with self.assertRaisesRegex(ValueError, "requires an explicit partitioned seed path"):
            await crawler.run(CrawlOptions(mode="full"))

    async def test_async_recursive_crawl_only_follows_collection_urls(self) -> None:
        bill_list = _load("example_bill.md")
        bill_detail = _load("example_billdetail.md")
        actions = _load("example_bill_action.md")
        amendments = _load("example_bill_amendment.md")
        committees = _load("example_bill_committee.md")
        cosponsors = _load("example_bill_cosponsor.md")
        related = _load("example_bill_relatedbill.md")
        subjects = _load("example_bill_subject.md")
        summaries = _load("example_bill_summary.md")
        text_versions = _load("example_bill_text.md")
        titles = _load("example_bill_title.md")
        committee_report_detail = load_example_payload(COMMITTEE_REPORT_EXAMPLES_DIR / "example_committee_reportdetail.json")
        bill_list["pagination"]["next"] = ""
        actions["pagination"]["next"] = ""
        actions["pagination"]["prev"] = ""
        amendments["pagination"]["next"] = ""
        cosponsors["pagination"]["next"] = ""

        seed_url = with_query_params("https://api.congress.gov/v3/bill?format=json", {"limit": 250})
        detail_url = canonicalize_congress_api_url("http://api.congress.gov/v3/bill/119/s/3281?format=xml")

        payloads = {
            seed_url: bill_list,
            detail_url: bill_detail,
            canonicalize_congress_api_url("https://api.congress.gov/v3/bill/117/hr/3076/actions?format=json&limit=250"): actions,
            canonicalize_congress_api_url("https://api.congress.gov/v3/bill/117/hr/3076/amendments?format=json&limit=250"): amendments,
            canonicalize_congress_api_url("https://api.congress.gov/v3/bill/117/hr/3076/committees?format=json&limit=250"): committees,
            canonicalize_congress_api_url("https://api.congress.gov/v3/bill/117/hr/3076/cosponsors?format=json&limit=250"): cosponsors,
            canonicalize_congress_api_url("https://api.congress.gov/v3/bill/117/hr/3076/relatedbills?format=json&limit=250"): related,
            canonicalize_congress_api_url("https://api.congress.gov/v3/bill/117/hr/3076/subjects?format=json&limit=250"): subjects,
            canonicalize_congress_api_url("https://api.congress.gov/v3/bill/117/hr/3076/summaries?format=json&limit=250"): summaries,
            canonicalize_congress_api_url("https://api.congress.gov/v3/bill/117/hr/3076/text?format=json&limit=250"): text_versions,
            canonicalize_congress_api_url("https://api.congress.gov/v3/bill/117/hr/3076/titles?format=json&limit=250"): titles,
            canonicalize_congress_api_url("https://api.congress.gov/v3/committee-report/117/HRPT/89?format=json"): committee_report_detail,
        }
        client = AsyncFakeClient(payloads)
        store = AsyncFakeStore()
        crawler = AsyncSpecCrawler(
            base_url="https://api.congress.gov/v3",
            spec=self.spec,
            client=client,
            store=store,
            concurrency=4,
        )

        run_id = await crawler.run(CrawlOptions(mode="full"))

        self.assertEqual(run_id, "run-1")
        self.assertEqual(store.finished, ("completed", len(store.raw_by_url), datetime(2025, 11, 21, 0, 0, tzinfo=timezone.utc)))
        self.assertIn(canonicalize_congress_api_url("https://api.congress.gov/v3/bill/117/hr/3076/relatedbills?format=json&limit=250"), store.raw_by_url)
        self.assertFalse(any("/bill/117/s/1720?format=json" in url for url in client.calls))

    async def test_async_failed_request_is_recorded_and_run_continues(self) -> None:
        bill_list = _load("example_bill.md")
        bill_detail = _load("example_billdetail.md")
        bill_list["pagination"]["next"] = ""
        bill_detail["bill"]["amendments"]["url"] = ""
        bill_detail["bill"]["committees"]["url"] = ""
        bill_detail["bill"]["cosponsors"]["url"] = ""
        bill_detail["bill"]["relatedBills"]["url"] = ""
        bill_detail["bill"]["subjects"]["url"] = ""
        bill_detail["bill"]["summaries"]["url"] = ""
        bill_detail["bill"]["titles"]["url"] = ""
        bill_detail["bill"]["committeeReports"] = []
        text_url = canonicalize_congress_api_url("https://api.congress.gov/v3/bill/117/hr/3076/text?format=json&limit=250")

        seed_url = with_query_params("https://api.congress.gov/v3/bill?format=json", {"limit": 250})
        detail_url = canonicalize_congress_api_url("http://api.congress.gov/v3/bill/119/s/3281?format=xml")

        client = AsyncFakeClient(
            {
                seed_url: bill_list,
                detail_url: bill_detail,
            },
            failures={text_url: http_error(500, "500 Server Error", text_url)},
        )
        store = AsyncFakeStore()
        crawler = AsyncSpecCrawler(
            base_url="https://api.congress.gov/v3",
            spec=self.spec,
            client=client,
            store=store,
            concurrency=3,
        )

        run_id = await crawler.run(CrawlOptions(mode="full", max_pages=1))

        self.assertEqual(run_id, "run-1")
        failed_request = next(request for request in store.failed_requests if request["endpoint_url"] == text_url)
        self.assertEqual(failed_request["http_status"], 500)
        failed_fetch = next(fetch for fetch in store.fetch_events if fetch["endpoint_url"] == text_url)
        self.assertEqual(failed_fetch["outcome"], "failed")

    async def test_async_incremental_does_not_rewalk_unchanged_detail_children(self) -> None:
        bill_list = _load("example_bill.md")
        bill_detail = _load("example_billdetail.md")
        bill_list["pagination"]["next"] = ""
        seed_url = "https://api.congress.gov/v3/bill?format=json&fromDateTime=2026-01-01T00%3A00%3A00Z&limit=250"
        detail_url = canonicalize_congress_api_url(
            "http://api.congress.gov/v3/bill/119/s/3281?format=xml"
        )
        client = AsyncFakeClient({seed_url: bill_list, detail_url: bill_detail})
        store = AsyncFakeStore()
        store.add_stored(detail_url, bill_detail, "raw-existing-detail")
        crawler = AsyncSpecCrawler(
            base_url="https://api.congress.gov/v3",
            spec=self.spec,
            client=client,
            store=store,
            concurrency=3,
        )

        await crawler.run(
            CrawlOptions(
                mode="incremental",
                from_datetime="2026-01-01T00:00:00Z",
                max_pages=1,
            )
        )

        self.assertEqual(client.calls, [seed_url, detail_url])

    async def test_async_crawler_fetches_sibling_families_concurrently_with_cap(self) -> None:
        bill_list = {"bills": [{"url": "https://api.congress.gov/v3/bill/119/hr/1?format=json"}], "pagination": {"count": 1, "next": "", "prev": ""}, "request": {"format": "json"}}
        detail = _load("example_billdetail.md")
        detail["bill"]["actions"]["url"] = "https://api.congress.gov/v3/bill/119/hr/1/actions?format=json"
        detail["bill"]["amendments"]["url"] = "https://api.congress.gov/v3/bill/119/hr/1/amendments?format=json"
        detail["bill"]["committees"]["url"] = "https://api.congress.gov/v3/bill/119/hr/1/committees?format=json"
        detail["bill"]["cosponsors"]["url"] = ""
        detail["bill"]["relatedBills"]["url"] = ""
        detail["bill"]["subjects"]["url"] = ""
        detail["bill"]["summaries"]["url"] = ""
        detail["bill"]["textVersions"]["url"] = ""
        detail["bill"]["titles"]["url"] = ""
        detail["bill"]["committeeReports"] = []
        seed_url = "https://api.congress.gov/v3/bill?format=json&limit=250"
        detail_url = "https://api.congress.gov/v3/bill/119/hr/1?format=json"
        child_urls = {
            canonicalize_congress_api_url("https://api.congress.gov/v3/bill/119/hr/1/actions?format=json&limit=250"): {"actions": [], "pagination": {"count": 0, "next": "", "prev": ""}, "request": {"format": "json"}},
            canonicalize_congress_api_url("https://api.congress.gov/v3/bill/119/hr/1/amendments?format=json&limit=250"): {"amendments": [], "pagination": {"count": 0, "next": "", "prev": ""}, "request": {"format": "json"}},
            canonicalize_congress_api_url("https://api.congress.gov/v3/bill/119/hr/1/committees?format=json&limit=250"): {"committees": [], "pagination": {"count": 0, "next": "", "prev": ""}, "request": {"format": "json"}},
        }
        payloads = {
            seed_url: bill_list,
            detail_url: detail,
            **child_urls,
        }
        delays = {url: 0.05 for url in child_urls}
        client = AsyncTrackingFakeClient(payloads, delays=delays)
        store = AsyncFakeStore()
        crawler = AsyncSpecCrawler(
            base_url="https://api.congress.gov/v3",
            spec=self.spec,
            client=client,
            store=store,
            concurrency=2,
        )

        await crawler.run(CrawlOptions(mode="full", max_pages=1))

        self.assertGreaterEqual(client.max_active, 2)
        self.assertLessEqual(client.max_active, 2)
