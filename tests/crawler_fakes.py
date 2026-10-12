"""In-memory client and store that satisfy the crawler's client and store protocols."""

from __future__ import annotations

from datetime import datetime
from typing import Any

import requests

from bicam_collection.client import FetchMetadata, FetchResult
from bicam_collection.storage import StoredNode, StoredResponseRef


def http_error(status_code: int | None, message: str, url: str = "") -> requests.HTTPError:
    response = requests.Response()
    response.status_code = status_code  # type: ignore[assignment]
    response.url = url
    return requests.HTTPError(message, response=response)


class FakeClient:
    def __init__(
        self,
        payloads: dict[str, dict],
        *,
        failures: dict[str, Exception] | None = None,
        metadata: FetchMetadata | None = None,
    ) -> None:
        self.payloads = payloads
        self.failures = failures or {}
        self.metadata = metadata or FetchMetadata(status_code=200)
        self.calls: list[str] = []

    def get_json(
        self, url: str, endpoint_name: str | None = None, *, endpoint_priority: int = 1, endpoint_reserve: int = 1
    ) -> FetchResult:
        self.calls.append(url)
        if url in self.failures:
            raise self.failures[url]
        if url not in self.payloads:
            raise AssertionError(f"Unexpected URL fetch: {url}")
        return FetchResult(payload=self.payloads[url], metadata=self.metadata)


class AsyncFakeClient(FakeClient):
    async def get_json(  # type: ignore[override]
        self, url: str, endpoint_name: str | None = None, *, endpoint_priority: int = 1, endpoint_reserve: int = 1
    ) -> FetchResult:
        return super().get_json(url)


class FakeStore:
    def __init__(self, latest_high_water_mark: datetime | None = None) -> None:
        self.latest_high_water_mark = latest_high_water_mark
        self.stored: dict[str, StoredNode] = {}
        self.raw_by_url: dict[str, dict[str, Any]] = {}
        self.refresh_nodes: list[StoredNode] = []
        self.fetch_events: list[dict[str, Any]] = []
        self.expected_fetches: list[dict[str, Any]] = []
        self.failed_requests: list[dict[str, Any]] = []
        self.finished: tuple[str, int, datetime | None] | None = None

    def add_stored(self, endpoint_url: str, response: dict, raw_id: str = "raw-existing") -> None:
        self.stored[endpoint_url] = StoredNode(
            raw_id=raw_id, endpoint_url=endpoint_url, response=response, raw_response_version_id=f"{raw_id}-version"
        )

    def start_run(self, category: str, mode: str, high_water_mark: datetime | None = None) -> str:
        return "run-1"

    def get_latest_high_water_mark(self, category: str) -> datetime | None:
        return self.latest_high_water_mark

    def finish_run(self, run_id: str, status: str, items_fetched: int, high_water_mark: datetime | None = None) -> None:
        self.finished = (status, items_fetched, high_water_mark)

    def has_response(self, endpoint_url: str) -> bool:
        return endpoint_url in self.stored

    def get_stored_node(self, endpoint_url: str) -> StoredNode | None:
        return self.stored.get(endpoint_url)

    def list_refresh_nodes(
        self, *, category: str, node_type: str, limit: int, after_endpoint_url: str | None = None
    ) -> list[StoredNode]:
        return self.refresh_nodes[:limit]

    def upsert_response(self, **kwargs: Any) -> StoredResponseRef:
        endpoint_url = kwargs["endpoint_url"]
        self.raw_by_url[endpoint_url] = kwargs
        raw_id = f"raw-{len(self.raw_by_url)}"
        self.add_stored(endpoint_url, kwargs["response"], raw_id)
        return StoredResponseRef(raw_id, f"{raw_id}-version", "hash")

    def record_fetch(self, **kwargs: Any) -> None:
        self.fetch_events.append(kwargs)

    def record_expected_fetch(self, **kwargs: Any) -> None:
        self.expected_fetches.append(kwargs)

    def record_failed_request(self, **kwargs: Any) -> None:
        self.failed_requests.append(kwargs)


class AsyncFakeStore(FakeStore):
    async def start_run(self, category: str, mode: str, high_water_mark: datetime | None = None) -> str:  # type: ignore[override]
        return super().start_run(category, mode, high_water_mark)

    async def get_latest_high_water_mark(self, category: str) -> datetime | None:  # type: ignore[override]
        return super().get_latest_high_water_mark(category)

    async def finish_run(self, run_id: str, status: str, items_fetched: int, high_water_mark: datetime | None = None) -> None:  # type: ignore[override]
        super().finish_run(run_id, status, items_fetched, high_water_mark)

    async def get_stored_nodes(self, endpoint_urls: list[str]) -> dict[str, StoredNode]:
        return {url: self.stored[url] for url in endpoint_urls if url in self.stored}

    async def list_refresh_nodes(  # type: ignore[override]
        self, *, category: str, node_type: str, limit: int, after_endpoint_url: str | None = None
    ) -> list[StoredNode]:
        return super().list_refresh_nodes(category=category, node_type=node_type, limit=limit)

    async def upsert_response(self, **kwargs: Any) -> StoredResponseRef:  # type: ignore[override]
        return super().upsert_response(**kwargs)

    async def record_fetch(self, **kwargs: Any) -> None:  # type: ignore[override]
        super().record_fetch(**kwargs)

    async def record_fetch_batch(self, records: list[dict[str, Any]]) -> None:
        self.fetch_events.extend(records)

    async def record_expected_fetch_batch(self, records: list[dict[str, Any]]) -> None:
        self.expected_fetches.extend(records)

    async def record_failed_request(self, **kwargs: Any) -> None:  # type: ignore[override]
        super().record_failed_request(**kwargs)
