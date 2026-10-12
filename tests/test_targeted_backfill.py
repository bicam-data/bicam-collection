from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from bicam_collection.client import FetchMetadata
from bicam_collection.targeted_backfill import (
    TargetedCandidate,
    TargetedFamily,
    run_targeted_backfill,
)


FAMILY = TargetedFamily(
    category="hearings",
    node_type="hearing_detail",
    endpoint_name="congress-test-family",
    endpoint_priority=3,
    run_mode="targeted_test_backfill",
)


def _candidate(number: int) -> TargetedCandidate:
    return TargetedCandidate(
        endpoint_url=f"https://api.congress.gov/v3/hearing/119/house/{number}?format=json",
        parent_id=f"parent-{number}",
        parent_endpoint_url=f"https://api.govinfo.gov/packages/CHRG-119hhrg{number}/summary",
    )


def _metadata(status_code: int | None = 200) -> FetchMetadata:
    return FetchMetadata(
        status_code=status_code,
        api_key_name="key-1",
        attempt_count=1,
        rate_limit_attempts=0,
        duration_ms=7,
        pool_allocation=None,
    )


class FakeClient:
    """Succeeds unless the URL is listed in ``failures``; tracks in-flight requests."""

    def __init__(self, failures: dict[str, Exception] | None = None, delay: float = 0.0) -> None:
        self.failures = failures or {}
        self.delay = delay
        self.in_flight = 0
        self.max_in_flight = 0
        self.calls: list[tuple[str, str | None, int]] = []

    async def get_json(self, url, endpoint_name=None, *, endpoint_priority=1, endpoint_reserve=1):
        self.calls.append((url, endpoint_name, endpoint_priority))
        self.in_flight += 1
        self.max_in_flight = max(self.max_in_flight, self.in_flight)
        try:
            await asyncio.sleep(self.delay)
            if url in self.failures:
                raise self.failures[url]
            return SimpleNamespace(payload={"hearing": {"url": url}}, metadata=_metadata())
        finally:
            self.in_flight -= 1


class FakeStore:
    def __init__(self) -> None:
        self.started: list[tuple[str, str]] = []
        self.upserts: list[dict] = []
        self.fetches: list[dict] = []
        self.failures: list[dict] = []
        self.finished: list[tuple[str, str, int]] = []
        self.closed = False

    async def start_run(self, category, mode):
        self.started.append((category, mode))
        return "run-1"

    async def upsert_response(self, **kwargs):
        self.upserts.append(kwargs)
        return SimpleNamespace(raw_response_id="raw-1", raw_response_version_id="version-1")

    async def record_fetch(self, **kwargs):
        self.fetches.append(kwargs)

    async def record_failed_request(self, **kwargs):
        self.failures.append(kwargs)

    async def finish_run(self, run_id, status, items_fetched):
        self.finished.append((run_id, status, items_fetched))

    async def close(self):
        self.closed = True


def test_success_path_stores_payload_and_records_fetch_with_family_labels() -> None:
    client = FakeClient()
    store = FakeStore()
    candidate = _candidate(1)

    outcome = asyncio.run(
        run_targeted_backfill(FAMILY, [candidate], client=client, store=store, concurrency=2)
    )

    assert (outcome.run_id, outcome.fetched, outcome.failed) == ("run-1", 1, 0)
    assert store.started == [("hearings", "targeted_test_backfill")]
    assert client.calls == [(candidate.endpoint_url, "congress-test-family", 3)]
    assert store.upserts == [
        {
            "endpoint_url": candidate.endpoint_url,
            "source": "congress",
            "category": "hearings",
            "node_type": "hearing_detail",
            "parent_id": "parent-1",
            "response": {"hearing": {"url": candidate.endpoint_url}},
            "latest_crawl_run_id": "run-1",
        }
    ]
    assert len(store.fetches) == 1
    fetch = store.fetches[0]
    assert fetch["outcome"] == "fetched"
    assert fetch["follow_mode"] == "targeted"
    assert fetch["raw_response_id"] == "raw-1"
    assert fetch["raw_response_version_id"] == "version-1"
    assert fetch["parent_endpoint_url"] == candidate.parent_endpoint_url
    assert fetch["http_status"] == 200
    assert fetch["duration_ms"] == 7
    assert fetch["api_key_name"] == "key-1"
    assert store.failures == []
    assert store.finished == [("run-1", "completed", 1)]
    assert store.closed


def test_failure_records_failed_request_using_response_status_and_fetch_metadata() -> None:
    candidate = _candidate(2)
    error = RuntimeError("404 Client Error")
    error.response = SimpleNamespace(status_code=404)  # type: ignore[attr-defined]
    error.fetch_metadata = _metadata(status_code=None)  # type: ignore[attr-defined]
    client = FakeClient(failures={candidate.endpoint_url: error})
    store = FakeStore()

    outcome = asyncio.run(
        run_targeted_backfill(FAMILY, [candidate], client=client, store=store, concurrency=1)
    )

    assert (outcome.fetched, outcome.failed) == (0, 1)
    assert store.upserts == []
    assert len(store.failures) == 1
    failure = store.failures[0]
    assert failure["category"] == "hearings"
    assert failure["node_type"] == "hearing_detail"
    assert failure["endpoint_url"] == candidate.endpoint_url
    assert failure["parent_endpoint_url"] == candidate.parent_endpoint_url
    assert failure["http_status"] == 404
    assert failure["error"] == "404 Client Error"
    assert failure["duration_ms"] == 7
    assert failure["attempt_count"] == 1
    # Reconciliation-probe families keep only the failed_requests row.
    assert store.fetches == []
    assert store.finished == [("run-1", "completed", 0)]
    assert store.closed


def test_failure_without_fetch_metadata_measures_its_own_duration() -> None:
    candidate = _candidate(3)
    client = FakeClient(failures={candidate.endpoint_url: ValueError("boom")})
    store = FakeStore()

    asyncio.run(run_targeted_backfill(FAMILY, [candidate], client=client, store=store, concurrency=1))

    failure = store.failures[0]
    assert failure["http_status"] is None
    assert failure["attempt_count"] is None
    assert failure["duration_ms"] >= 1


def test_relationship_families_also_record_a_failed_fetch_row() -> None:
    family = TargetedFamily(
        category="hearings",
        node_type="hearing_detail",
        endpoint_name="congress-test-family",
        endpoint_priority=3,
        run_mode="targeted_test_backfill",
        record_failed_fetches=True,
    )
    candidate = _candidate(4)
    error = RuntimeError("500 Server Error")
    error.response = SimpleNamespace(status_code=500)  # type: ignore[attr-defined]
    client = FakeClient(failures={candidate.endpoint_url: error})
    store = FakeStore()

    asyncio.run(run_targeted_backfill(family, [candidate], client=client, store=store, concurrency=1))

    assert len(store.failures) == 1
    assert len(store.fetches) == 1
    fetch = store.fetches[0]
    assert fetch["outcome"] == "failed"
    assert fetch["raw_response_id"] is None
    assert fetch["http_status"] == 500
    assert fetch["duration_ms"] == store.failures[0]["duration_ms"]
    assert store.finished == [("run-1", "completed", 0)]


def test_fail_run_on_any_failure_marks_the_run_failed() -> None:
    family = TargetedFamily(
        category="laws",
        node_type="law_detail",
        endpoint_name="congress-laws-root-detail",
        endpoint_priority=4,
        run_mode="targeted_law_detail_backfill",
        fail_run_on_any_failure=True,
    )
    failing = _candidate(5)
    client = FakeClient(failures={failing.endpoint_url: RuntimeError("nope")})
    store = FakeStore()

    outcome = asyncio.run(
        run_targeted_backfill(
            family, [_candidate(6), failing], client=client, store=store, concurrency=2
        )
    )

    assert (outcome.fetched, outcome.failed) == (1, 1)
    assert store.finished == [("run-1", "failed", 1)]


def test_concurrency_bounds_in_flight_requests() -> None:
    client = FakeClient(delay=0.01)
    store = FakeStore()
    candidates = [_candidate(number) for number in range(10)]

    outcome = asyncio.run(
        run_targeted_backfill(FAMILY, candidates, client=client, store=store, concurrency=3)
    )

    assert outcome.fetched == 10
    assert client.max_in_flight == 3


def test_batch_size_bounds_scheduled_tasks_below_concurrency() -> None:
    client = FakeClient(delay=0.01)
    store = FakeStore()
    candidates = [_candidate(number) for number in range(6)]

    asyncio.run(
        run_targeted_backfill(
            FAMILY, candidates, client=client, store=store, concurrency=8, batch_size=2
        )
    )

    assert client.max_in_flight == 2
    assert len(store.fetches) == 6


def test_after_success_runs_after_store_and_its_failure_counts_against_the_candidate() -> None:
    good = _candidate(7)
    bad = _candidate(8)
    client = FakeClient()
    store = FakeStore()
    seen: list[str] = []

    async def after_success(candidate: TargetedCandidate) -> None:
        assert store.fetches[-1]["endpoint_url"] == candidate.endpoint_url
        seen.append(candidate.endpoint_url)
        if candidate is bad:
            raise RuntimeError("descendant crawl failed")

    outcome = asyncio.run(
        run_targeted_backfill(
            FAMILY, [good, bad], client=client, store=store, concurrency=1, after_success=after_success
        )
    )

    assert seen == [good.endpoint_url, bad.endpoint_url]
    assert (outcome.fetched, outcome.failed) == (1, 1)
    assert store.failures[0]["endpoint_url"] == bad.endpoint_url
    assert store.failures[0]["error"] == "descendant crawl failed"


def test_store_failure_marks_run_failed_and_still_closes_the_store() -> None:
    class BrokenStore(FakeStore):
        async def record_failed_request(self, **kwargs):
            raise ConnectionError("database went away")

    candidate = _candidate(9)
    client = FakeClient(failures={candidate.endpoint_url: RuntimeError("nope")})
    store = BrokenStore()

    with pytest.raises(ConnectionError):
        asyncio.run(run_targeted_backfill(FAMILY, [candidate], client=client, store=store, concurrency=1))

    assert store.finished == [("run-1", "failed", 0)]
    assert store.closed
