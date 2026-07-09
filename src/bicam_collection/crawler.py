from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
import inspect
from math import ceil
from typing import Any, Protocol

from bicam_collection.selectors import select, select_one
from bicam_collection.spec_loader import CrawlSpec, EdgeSpec, NodeSpec
from bicam_collection.urls import build_seed_url, canonicalize_url, is_allowed_host, with_query_params


AUTO_HIGH_WATER_MARK_MODES = {"incremental", "resume"}
SKIP_EXISTING_MODES = {"resume"}
DESCENDANT_REFRESH_MODE = "refresh_descendants"


@dataclass(slots=True)
class CrawlOptions:
    mode: str
    from_datetime: str | None = None
    to_datetime: str | None = None
    max_pages: int | None = None
    batch_size: int | None = None
    page_size: int | None = None


@dataclass(slots=True)
class QueueItem:
    url: str
    node_type: str
    follow_mode: str
    parent_raw_id: str | None = None
    parent_endpoint_url: str | None = None
    pagination_key: str | None = None


@dataclass(slots=True)
class StoredNode:
    raw_id: str
    endpoint_url: str
    response: dict


class StoreProtocol(Protocol):
    def start_run(self, category: str, mode: str, high_water_mark: datetime | None = None) -> str: ...
    def get_latest_high_water_mark(self, category: str) -> datetime | None: ...
    def finish_run(
        self,
        run_id: str,
        status: str,
        items_fetched: int,
        high_water_mark: datetime | None = None,
    ) -> None: ...
    def has_response(self, endpoint_url: str) -> bool: ...
    def list_refresh_nodes(self, *, category: str, node_type: str, limit: int) -> list[StoredNode]: ...
    def upsert_response(
        self,
        *,
        endpoint_url: str,
        source: str,
        category: str,
        node_type: str,
        parent_id: str | None,
        response: dict,
        latest_crawl_run_id: str,
    ) -> str: ...
    def record_fetch(
        self,
        *,
        crawl_run_id: str,
        endpoint_url: str,
        raw_response_id: str | None,
        parent_endpoint_url: str | None,
        node_type: str,
        follow_mode: str,
        http_status: int | None,
        outcome: str,
        duration_ms: int | None = None,
        attempt_count: int | None = None,
        rate_limit_attempts: int | None = None,
        api_key_name: str | None = None,
        pool_allocation: dict[str, Any] | None = None,
    ) -> None: ...
    def record_failed_request(
        self,
        *,
        crawl_run_id: str,
        category: str,
        node_type: str,
        endpoint_url: str,
        parent_endpoint_url: str | None,
        http_status: int | None,
        error: str,
        duration_ms: int | None = None,
        attempt_count: int | None = None,
        rate_limit_attempts: int | None = None,
        api_key_name: str | None = None,
        pool_allocation: dict[str, Any] | None = None,
    ) -> None: ...


class ReporterProtocol(Protocol):
    def on_seed_progress(
        self,
        *,
        category: str,
        page_number: int,
        total_pages: int | None,
        items_seen: int,
        total_items: int | None,
    ) -> None: ...

    def on_url_fetched(
        self,
        *,
        category: str,
        node_type: str,
        endpoint_url: str,
        http_status: int | None,
        total_fetched: int,
        duration_ms: int | None = None,
        attempt_count: int | None = None,
        rate_limit_attempts: int | None = None,
        api_key_name: str | None = None,
        pool_allocation: dict[str, Any] | None = None,
    ) -> None: ...

    def on_request_failed(
        self,
        *,
        category: str,
        node_type: str,
        endpoint_url: str,
        http_status: int | None,
        error: str,
        total_failed: int,
        duration_ms: int | None = None,
        attempt_count: int | None = None,
        rate_limit_attempts: int | None = None,
        api_key_name: str | None = None,
        pool_allocation: dict[str, Any] | None = None,
    ) -> None: ...

    def on_run_finished(
        self,
        *,
        category: str,
        run_id: str,
        status: str,
        items_fetched: int,
    ) -> None: ...


class AsyncStoreProtocol(Protocol):
    async def start_run(self, category: str, mode: str, high_water_mark: datetime | None = None) -> str: ...
    async def get_latest_high_water_mark(self, category: str) -> datetime | None: ...
    async def finish_run(
        self,
        run_id: str,
        status: str,
        items_fetched: int,
        high_water_mark: datetime | None = None,
    ) -> None: ...
    async def has_response(self, endpoint_url: str) -> bool: ...
    async def list_refresh_nodes(self, *, category: str, node_type: str, limit: int) -> list[StoredNode]: ...
    async def upsert_response(
        self,
        *,
        endpoint_url: str,
        source: str,
        category: str,
        node_type: str,
        parent_id: str | None,
        response: dict,
        latest_crawl_run_id: str,
    ) -> str: ...
    async def record_fetch(
        self,
        *,
        crawl_run_id: str,
        endpoint_url: str,
        raw_response_id: str | None,
        parent_endpoint_url: str | None,
        node_type: str,
        follow_mode: str,
        http_status: int | None,
        outcome: str,
        duration_ms: int | None = None,
        attempt_count: int | None = None,
        rate_limit_attempts: int | None = None,
        api_key_name: str | None = None,
        pool_allocation: dict[str, Any] | None = None,
    ) -> None: ...
    async def record_failed_request(
        self,
        *,
        crawl_run_id: str,
        category: str,
        node_type: str,
        endpoint_url: str,
        parent_endpoint_url: str | None,
        http_status: int | None,
        error: str,
        duration_ms: int | None = None,
        attempt_count: int | None = None,
        rate_limit_attempts: int | None = None,
        api_key_name: str | None = None,
        pool_allocation: dict[str, Any] | None = None,
    ) -> None: ...


def _unique_nonempty(values: list[Any]) -> list[str]:
    seen: set[str] = set()
    normalized: list[str] = []
    for value in values:
        if not isinstance(value, str):
            continue
        candidate = value.strip()
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        normalized.append(candidate)
    return normalized


def _parse_timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    candidate = value.strip()
    if not candidate:
        return None
    if candidate.endswith("Z"):
        candidate = candidate[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _format_timestamp(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


@dataclass(slots=True)
class FetchMetadata:
    status_code: int | None = None
    api_key_name: str | None = None
    attempt_count: int | None = None
    rate_limit_attempts: int | None = None
    duration_ms: int | None = None
    pool_allocation: dict[str, Any] | None = None


def _normalize_fetch_result(result: Any) -> tuple[dict[str, Any], FetchMetadata]:
    if isinstance(result, tuple):
        payload, status_code = result
        return payload, FetchMetadata(status_code=status_code)
    payload = getattr(result, "payload", None)
    metadata = getattr(result, "metadata", None)
    if not isinstance(payload, dict) or metadata is None:
        raise TypeError("Client get_json() must return either (payload, status_code) or an object with payload/metadata")
    return payload, FetchMetadata(
        status_code=getattr(metadata, "status_code", None),
        api_key_name=getattr(metadata, "api_key_name", None),
        attempt_count=getattr(metadata, "attempt_count", None),
        rate_limit_attempts=getattr(metadata, "rate_limit_attempts", None),
        duration_ms=getattr(metadata, "duration_ms", None),
        pool_allocation=getattr(metadata, "pool_allocation", None),
    )


def _supported_client_kwargs(client: Any, kwargs: dict[str, Any]) -> dict[str, Any]:
    try:
        parameters = inspect.signature(client.get_json).parameters
    except (TypeError, ValueError):
        return {}
    if any(parameter.kind == inspect.Parameter.VAR_KEYWORD for parameter in parameters.values()):
        return kwargs
    return {name: value for name, value in kwargs.items() if name in parameters}


def _estimate_remaining_work(
    *,
    seed_total_items: int | None,
    seed_items_seen: int,
    queue_size: int,
) -> int:
    if seed_total_items is not None:
        return max(1, (seed_total_items - seed_items_seen) + queue_size)
    return max(1, queue_size + 1)


def _remaining_work_priority(remaining_work: int) -> int:
    if remaining_work >= 100_000:
        return 1
    if remaining_work >= 25_000:
        return 2
    if remaining_work >= 5_000:
        return 3
    if remaining_work >= 1_000:
        return 4
    return 5


class SpecCrawler:
    def __init__(
        self,
        *,
        base_url: str,
        spec: CrawlSpec,
        client: Any,
        store: StoreProtocol,
        reporter: ReporterProtocol | None = None,
    ) -> None:
        self._base_url = base_url
        self._spec = spec
        self._client = client
        self._store = store
        self._reporter = reporter

    def _endpoint_request_kwargs(
        self,
        *,
        seed_total_items: int | None,
        seed_items_seen: int,
        queue_size: int,
    ) -> dict[str, Any]:
        remaining_work = _estimate_remaining_work(
            seed_total_items=seed_total_items,
            seed_items_seen=seed_items_seen,
            queue_size=queue_size,
        )
        return {
            "endpoint_name": self._spec.category,
            "endpoint_priority": _remaining_work_priority(remaining_work),
            "endpoint_reserve": 1,
        }

    def _canonicalize_url(self, url: str) -> str:
        return canonicalize_url(url, self._spec.source)

    def _resolve_seed_path(self, options: CrawlOptions) -> str:
        replacements = {
            "from_datetime": options.from_datetime,
            "to_datetime": options.to_datetime,
            "from_date": options.from_datetime[:10] if options.from_datetime else None,
            "to_date": options.to_datetime[:10] if options.to_datetime else None,
        }
        path = self._spec.seed.path
        for name, value in replacements.items():
            placeholder = "{" + name + "}"
            if placeholder not in path:
                continue
            if value is None:
                raise ValueError(f"Missing required value for seed path placeholder '{placeholder}' in category '{self._spec.category}'")
            path = path.replace(placeholder, value)
        return path

    def _effective_page_size(self, node: NodeSpec, options: CrawlOptions) -> int | None:
        if options.page_size is not None:
            if "limit" in node.default_query_params or "pageSize" in node.default_query_params:
                return options.page_size
        for key in ("limit", "pageSize"):
            raw_value = node.default_query_params.get(key)
            if raw_value is None:
                continue
            try:
                return int(raw_value)
            except (TypeError, ValueError):
                return None
        return None

    def run(self, options: CrawlOptions) -> str:
        if options.mode == DESCENDANT_REFRESH_MODE:
            return self._run_refresh_descendants(options)

        effective_from_datetime = options.from_datetime
        if options.mode in AUTO_HIGH_WATER_MARK_MODES and not effective_from_datetime:
            effective_from_datetime = _format_timestamp(self._store.get_latest_high_water_mark(self._spec.category))

        effective_options = CrawlOptions(
            mode=options.mode,
            from_datetime=effective_from_datetime,
            to_datetime=options.to_datetime,
            max_pages=options.max_pages,
            batch_size=options.batch_size,
            page_size=options.page_size,
        )

        run_id = self._store.start_run(self._spec.category, options.mode)
        queue = deque(
            [
                QueueItem(
                    url=self._augment_url(
                        build_seed_url(self._base_url, self._resolve_seed_path(effective_options)),
                        self._spec.nodes[self._spec.seed.node_type],
                        effective_options,
                    ),
                    node_type=self._spec.seed.node_type,
                    follow_mode="recurse",
                    pagination_key=self._spec.seed.node_type,
                )
            ]
        )
        high_water_mark = _parse_timestamp(effective_options.from_datetime)
        items_fetched = 0
        try:
            items_fetched, high_water_mark = self._drain_queue(
                run_id=run_id,
                queue=queue,
                options=effective_options,
                high_water_mark=high_water_mark,
            )
            self._store.finish_run(run_id, "completed", items_fetched, high_water_mark=high_water_mark)
            if self._reporter is not None:
                self._reporter.on_run_finished(
                    category=self._spec.category,
                    run_id=run_id,
                    status="completed",
                    items_fetched=items_fetched,
                )
            return run_id
        except Exception:
            self._store.finish_run(run_id, "failed", items_fetched, high_water_mark=high_water_mark)
            if self._reporter is not None:
                self._reporter.on_run_finished(
                    category=self._spec.category,
                    run_id=run_id,
                    status="failed",
                    items_fetched=items_fetched,
                )
            raise

    def _run_refresh_descendants(self, options: CrawlOptions) -> str:
        refresh_root_node_type = self._spec.refresh_root_node_type
        if not refresh_root_node_type:
            raise ValueError(f"refresh_descendants is not supported for category '{self._spec.category}'")

        run_id = self._store.start_run(self._spec.category, options.mode)
        batch_size = options.batch_size or 100
        root_node = self._spec.nodes[refresh_root_node_type]
        refresh_roots = self._store.list_refresh_nodes(
            category=self._spec.category,
            node_type=refresh_root_node_type,
            limit=batch_size,
        )
        queue: deque[QueueItem] = deque()
        try:
            for root in refresh_roots:
                for edge in root_node.child_edges:
                    self._enqueue_edge(
                        queue,
                        root.response,
                        edge,
                        root.raw_id,
                        root.endpoint_url,
                        CrawlOptions(
                            mode=options.mode,
                            from_datetime=options.from_datetime,
                            to_datetime=options.to_datetime,
                            max_pages=options.max_pages,
                            batch_size=options.batch_size,
                            page_size=options.page_size,
                        ),
                    )

            items_fetched, _ = self._drain_queue(
                run_id=run_id,
                queue=queue,
                options=CrawlOptions(
                    mode=options.mode,
                    from_datetime=options.from_datetime,
                    to_datetime=options.to_datetime,
                    max_pages=options.max_pages,
                    batch_size=options.batch_size,
                    page_size=options.page_size,
                ),
                high_water_mark=None,
            )
            self._store.finish_run(run_id, "completed", items_fetched)
            if self._reporter is not None:
                self._reporter.on_run_finished(
                    category=self._spec.category,
                    run_id=run_id,
                    status="completed",
                    items_fetched=items_fetched,
                )
            return run_id
        except Exception:
            self._store.finish_run(run_id, "failed", 0)
            if self._reporter is not None:
                self._reporter.on_run_finished(
                    category=self._spec.category,
                    run_id=run_id,
                    status="failed",
                    items_fetched=0,
                )
            raise

    def _drain_queue(
        self,
        *,
        run_id: str,
        queue: deque[QueueItem],
        options: CrawlOptions,
        high_water_mark: datetime | None,
    ) -> tuple[int, datetime | None]:
        seen_urls: set[str] = set()
        items_fetched = 0
        pages_fetched: dict[str, int] = {}
        seed_pages_fetched = 0
        seed_items_seen = 0
        seed_total_items: int | None = None
        seed_total_pages: int | None = None
        failed_requests = 0
        while queue:
            job = queue.popleft()
            if not is_allowed_host(job.url, self._spec.url_allowlist_hosts):
                continue
            canonical_url = self._canonicalize_url(job.url)
            if canonical_url in seen_urls:
                self._store.record_fetch(
                    crawl_run_id=run_id,
                    endpoint_url=canonical_url,
                    raw_response_id=None,
                    parent_endpoint_url=job.parent_endpoint_url,
                    node_type=job.node_type,
                    follow_mode=job.follow_mode,
                    http_status=None,
                    outcome="skipped_seen",
                )
                continue
            seen_urls.add(canonical_url)

            if options.mode in SKIP_EXISTING_MODES and self._store.has_response(canonical_url):
                self._store.record_fetch(
                    crawl_run_id=run_id,
                    endpoint_url=canonical_url,
                    raw_response_id=None,
                    parent_endpoint_url=job.parent_endpoint_url,
                    node_type=job.node_type,
                    follow_mode=job.follow_mode,
                    http_status=None,
                    outcome="skipped_existing",
                )
                continue

            try:
                request_kwargs = self._endpoint_request_kwargs(
                    seed_total_items=seed_total_items,
                    seed_items_seen=seed_items_seen,
                    queue_size=len(queue),
                )
                payload, fetch_metadata = _normalize_fetch_result(
                    self._client.get_json(canonical_url, **_supported_client_kwargs(self._client, request_kwargs))
                )
            except Exception as exc:
                response = getattr(exc, "response", None)
                http_status = getattr(response, "status_code", None)
                metadata = getattr(exc, "fetch_metadata", FetchMetadata(status_code=http_status))
                self._store.record_failed_request(
                    crawl_run_id=run_id,
                    category=self._spec.category,
                    node_type=job.node_type,
                    endpoint_url=canonical_url,
                    parent_endpoint_url=job.parent_endpoint_url,
                    http_status=http_status,
                    error=str(exc),
                    duration_ms=metadata.duration_ms,
                    attempt_count=metadata.attempt_count,
                    rate_limit_attempts=metadata.rate_limit_attempts,
                    api_key_name=metadata.api_key_name,
                    pool_allocation=metadata.pool_allocation,
                )
                self._store.record_fetch(
                    crawl_run_id=run_id,
                    endpoint_url=canonical_url,
                    raw_response_id=None,
                    parent_endpoint_url=job.parent_endpoint_url,
                    node_type=job.node_type,
                    follow_mode=job.follow_mode,
                    http_status=http_status,
                    outcome="failed",
                    duration_ms=metadata.duration_ms,
                    attempt_count=metadata.attempt_count,
                    rate_limit_attempts=metadata.rate_limit_attempts,
                    api_key_name=metadata.api_key_name,
                    pool_allocation=metadata.pool_allocation,
                )
                failed_requests += 1
                if self._reporter is not None:
                    self._reporter.on_request_failed(
                        category=self._spec.category,
                        node_type=job.node_type,
                        endpoint_url=canonical_url,
                        http_status=http_status,
                        error=str(exc),
                        total_failed=failed_requests,
                        duration_ms=metadata.duration_ms,
                        attempt_count=metadata.attempt_count,
                        rate_limit_attempts=metadata.rate_limit_attempts,
                        api_key_name=metadata.api_key_name,
                        pool_allocation=metadata.pool_allocation,
                    )
                continue
            node = self._spec.nodes.get(job.node_type)
            if node is not None and node.high_water_mark_path:
                for raw_value in select(payload, node.high_water_mark_path):
                    parsed = _parse_timestamp(raw_value)
                    if parsed is None:
                        continue
                    if high_water_mark is None or parsed > high_water_mark:
                        high_water_mark = parsed
            if job.node_type == self._spec.seed.node_type and node is not None:
                seed_pages_fetched += 1
                if node.item_path:
                    seed_items_seen += len(select(payload, node.item_path))
                if seed_total_items is None and node.pagination_path:
                    raw_total = select_one(payload, f"{node.pagination_path}.count")
                    if isinstance(raw_total, int):
                        seed_total_items = raw_total
                    elif isinstance(raw_total, str) and raw_total.strip().isdigit():
                        seed_total_items = int(raw_total.strip())
                    effective_limit = self._effective_page_size(node, options)
                    if seed_total_items is not None and effective_limit is not None:
                        try:
                            seed_total_pages = ceil(seed_total_items / effective_limit)
                        except (TypeError, ValueError, ZeroDivisionError):
                            seed_total_pages = None
                if self._reporter is not None:
                    self._reporter.on_seed_progress(
                        category=self._spec.category,
                        page_number=seed_pages_fetched,
                        total_pages=seed_total_pages,
                        items_seen=seed_items_seen,
                        total_items=seed_total_items,
                    )
            raw_id = self._store.upsert_response(
                endpoint_url=canonical_url,
                source=self._spec.source,
                category=self._spec.category,
                node_type=job.node_type,
                parent_id=job.parent_raw_id,
                response=payload,
                latest_crawl_run_id=run_id,
            )
            self._store.record_fetch(
                crawl_run_id=run_id,
                endpoint_url=canonical_url,
                raw_response_id=raw_id,
                parent_endpoint_url=job.parent_endpoint_url,
                node_type=job.node_type,
                follow_mode=job.follow_mode,
                http_status=fetch_metadata.status_code,
                outcome="fetched",
                duration_ms=fetch_metadata.duration_ms,
                attempt_count=fetch_metadata.attempt_count,
                rate_limit_attempts=fetch_metadata.rate_limit_attempts,
                api_key_name=fetch_metadata.api_key_name,
                pool_allocation=fetch_metadata.pool_allocation,
            )
            items_fetched += 1
            if self._reporter is not None:
                self._reporter.on_url_fetched(
                    category=self._spec.category,
                    node_type=job.node_type,
                    endpoint_url=canonical_url,
                    http_status=fetch_metadata.status_code,
                    total_fetched=items_fetched,
                    duration_ms=fetch_metadata.duration_ms,
                    attempt_count=fetch_metadata.attempt_count,
                    rate_limit_attempts=fetch_metadata.rate_limit_attempts,
                    api_key_name=fetch_metadata.api_key_name,
                    pool_allocation=fetch_metadata.pool_allocation,
                )

            if job.follow_mode != "recurse":
                continue

            node = self._spec.nodes.get(job.node_type)
            if node is None:
                continue

            next_page_url = select_one(payload, node.next_page_path) if node.next_page_path else None
            if isinstance(next_page_url, str) and next_page_url.strip():
                pagination_key = job.pagination_key or f"{job.node_type}:{canonical_url}"
                pages_fetched[pagination_key] = pages_fetched.get(pagination_key, 0) + 1
                if options.max_pages is None or pages_fetched[pagination_key] < options.max_pages:
                    queue.append(
                        QueueItem(
                            url=self._augment_url(next_page_url, node, options),
                            node_type=job.node_type,
                            follow_mode="recurse",
                            parent_raw_id=job.parent_raw_id,
                            parent_endpoint_url=job.parent_endpoint_url,
                            pagination_key=pagination_key,
                        )
                    )

            for edge in node.child_edges:
                self._enqueue_edge(queue, payload, edge, raw_id, canonical_url, options)
            for edge in node.linked_item_edges:
                self._enqueue_edge(queue, payload, edge, raw_id, canonical_url, options)
        return items_fetched, high_water_mark

    def _enqueue_edge(
        self,
        queue: deque[QueueItem],
        payload: dict[str, Any],
        edge: EdgeSpec,
        parent_raw_id: str,
        parent_endpoint_url: str,
        options: CrawlOptions,
    ) -> None:
        node = self._spec.nodes.get(edge.target)
        for raw_url in _unique_nonempty(select(payload, edge.url_path)):
            if not raw_url.startswith("http"):
                continue
            if not is_allowed_host(raw_url, self._spec.url_allowlist_hosts):
                continue
            normalized = self._canonicalize_url(raw_url)
            if node is not None:
                normalized = self._augment_url(normalized, node, options)
            queue.append(
                QueueItem(
                    url=normalized,
                    node_type=edge.target,
                    follow_mode=edge.follow_mode,
                    parent_raw_id=parent_raw_id,
                    parent_endpoint_url=parent_endpoint_url,
                    pagination_key=normalized if edge.follow_mode == "recurse" else None,
                )
            )

    def _augment_url(self, url: str, node: NodeSpec, options: CrawlOptions) -> str:
        params: dict[str, str | int | None] = dict(node.default_query_params)
        if options.page_size is not None:
            if "limit" in params:
                params["limit"] = options.page_size
            if "pageSize" in params:
                params["pageSize"] = options.page_size
        if "fromDateTime" in node.incremental_query_params:
            params["fromDateTime"] = options.from_datetime
        if "toDateTime" in node.incremental_query_params:
            params["toDateTime"] = options.to_datetime
        return with_query_params(self._canonicalize_url(url), params)


@dataclass(slots=True)
class AsyncFetchResult:
    job: QueueItem
    payload: dict[str, Any] | None
    fetch_metadata: FetchMetadata
    error: str | None = None


class AsyncSpecCrawler:
    def __init__(
        self,
        *,
        base_url: str,
        spec: CrawlSpec,
        client: Any,
        store: AsyncStoreProtocol,
        reporter: ReporterProtocol | None = None,
        concurrency: int = 5,
    ) -> None:
        self._base_url = base_url
        self._spec = spec
        self._client = client
        self._store = store
        self._reporter = reporter
        self._concurrency = max(1, concurrency)
        self._semaphore = asyncio.Semaphore(self._concurrency)

    def _endpoint_request_kwargs(
        self,
        *,
        seed_total_items: int | None,
        seed_items_seen: int,
        queue_size: int,
    ) -> dict[str, Any]:
        remaining_work = _estimate_remaining_work(
            seed_total_items=seed_total_items,
            seed_items_seen=seed_items_seen,
            queue_size=queue_size,
        )
        return {
            "endpoint_name": self._spec.category,
            "endpoint_priority": _remaining_work_priority(remaining_work),
            "endpoint_reserve": 1,
        }

    def _canonicalize_url(self, url: str) -> str:
        return canonicalize_url(url, self._spec.source)

    def _resolve_seed_path(self, options: CrawlOptions) -> str:
        replacements = {
            "from_datetime": options.from_datetime,
            "to_datetime": options.to_datetime,
            "from_date": options.from_datetime[:10] if options.from_datetime else None,
            "to_date": options.to_datetime[:10] if options.to_datetime else None,
        }
        path = self._spec.seed.path
        for name, value in replacements.items():
            placeholder = "{" + name + "}"
            if placeholder not in path:
                continue
            if value is None:
                raise ValueError(f"Missing required value for seed path placeholder '{placeholder}' in category '{self._spec.category}'")
            path = path.replace(placeholder, value)
        return path

    def _effective_page_size(self, node: NodeSpec, options: CrawlOptions) -> int | None:
        if options.page_size is not None:
            if "limit" in node.default_query_params or "pageSize" in node.default_query_params:
                return options.page_size
        for key in ("limit", "pageSize"):
            raw_value = node.default_query_params.get(key)
            if raw_value is None:
                continue
            try:
                return int(raw_value)
            except (TypeError, ValueError):
                return None
        return None

    async def run(self, options: CrawlOptions) -> str:
        if options.mode == DESCENDANT_REFRESH_MODE:
            return await self._run_refresh_descendants(options)

        effective_from_datetime = options.from_datetime
        if options.mode in AUTO_HIGH_WATER_MARK_MODES and not effective_from_datetime:
            effective_from_datetime = _format_timestamp(await self._store.get_latest_high_water_mark(self._spec.category))

        effective_options = CrawlOptions(
            mode=options.mode,
            from_datetime=effective_from_datetime,
            to_datetime=options.to_datetime,
            max_pages=options.max_pages,
            batch_size=options.batch_size,
            page_size=options.page_size,
        )

        run_id = await self._store.start_run(self._spec.category, options.mode)
        queue = deque(
            [
                QueueItem(
                    url=self._augment_url(
                        build_seed_url(self._base_url, self._resolve_seed_path(effective_options)),
                        self._spec.nodes[self._spec.seed.node_type],
                        effective_options,
                    ),
                    node_type=self._spec.seed.node_type,
                    follow_mode="recurse",
                    pagination_key=self._spec.seed.node_type,
                )
            ]
        )
        high_water_mark = _parse_timestamp(effective_options.from_datetime)
        items_fetched = 0
        try:
            items_fetched, high_water_mark = await self._drain_queue(
                run_id=run_id,
                queue=queue,
                options=effective_options,
                high_water_mark=high_water_mark,
            )
            await self._store.finish_run(run_id, "completed", items_fetched, high_water_mark=high_water_mark)
            if self._reporter is not None:
                self._reporter.on_run_finished(
                    category=self._spec.category,
                    run_id=run_id,
                    status="completed",
                    items_fetched=items_fetched,
                )
            return run_id
        except Exception:
            await self._store.finish_run(run_id, "failed", items_fetched, high_water_mark=high_water_mark)
            if self._reporter is not None:
                self._reporter.on_run_finished(
                    category=self._spec.category,
                    run_id=run_id,
                    status="failed",
                    items_fetched=items_fetched,
                )
            raise

    async def _run_refresh_descendants(self, options: CrawlOptions) -> str:
        refresh_root_node_type = self._spec.refresh_root_node_type
        if not refresh_root_node_type:
            raise ValueError(f"refresh_descendants is not supported for category '{self._spec.category}'")

        run_id = await self._store.start_run(self._spec.category, options.mode)
        batch_size = options.batch_size or 100
        root_node = self._spec.nodes[refresh_root_node_type]
        refresh_roots = await self._store.list_refresh_nodes(
            category=self._spec.category,
            node_type=refresh_root_node_type,
            limit=batch_size,
        )
        queue: deque[QueueItem] = deque()
        try:
            for root in refresh_roots:
                for edge in root_node.child_edges:
                    self._enqueue_edge(
                        queue,
                        root.response,
                        edge,
                        root.raw_id,
                        root.endpoint_url,
                        CrawlOptions(
                            mode=options.mode,
                            from_datetime=options.from_datetime,
                            to_datetime=options.to_datetime,
                            max_pages=options.max_pages,
                            batch_size=options.batch_size,
                            page_size=options.page_size,
                        ),
                    )

            items_fetched, _ = await self._drain_queue(
                run_id=run_id,
                queue=queue,
                options=CrawlOptions(
                    mode=options.mode,
                    from_datetime=options.from_datetime,
                    to_datetime=options.to_datetime,
                    max_pages=options.max_pages,
                    batch_size=options.batch_size,
                    page_size=options.page_size,
                ),
                high_water_mark=None,
            )
            await self._store.finish_run(run_id, "completed", items_fetched)
            if self._reporter is not None:
                self._reporter.on_run_finished(
                    category=self._spec.category,
                    run_id=run_id,
                    status="completed",
                    items_fetched=items_fetched,
                )
            return run_id
        except Exception:
            await self._store.finish_run(run_id, "failed", 0)
            if self._reporter is not None:
                self._reporter.on_run_finished(
                    category=self._spec.category,
                    run_id=run_id,
                    status="failed",
                    items_fetched=0,
                )
            raise

    async def _drain_queue(
        self,
        *,
        run_id: str,
        queue: deque[QueueItem],
        options: CrawlOptions,
        high_water_mark: datetime | None,
    ) -> tuple[int, datetime | None]:
        seen_urls: set[str] = set()
        items_fetched = 0
        pages_fetched: dict[str, int] = {}
        seed_pages_fetched = 0
        seed_items_seen = 0
        seed_total_items: int | None = None
        seed_total_pages: int | None = None
        failed_requests = 0
        while queue:
            batch: list[QueueItem] = []
            while queue and len(batch) < self._concurrency:
                job = queue.popleft()
                if not is_allowed_host(job.url, self._spec.url_allowlist_hosts):
                    continue
                canonical_url = self._canonicalize_url(job.url)
                if canonical_url in seen_urls:
                    await self._store.record_fetch(
                        crawl_run_id=run_id,
                        endpoint_url=canonical_url,
                        raw_response_id=None,
                        parent_endpoint_url=job.parent_endpoint_url,
                        node_type=job.node_type,
                        follow_mode=job.follow_mode,
                        http_status=None,
                        outcome="skipped_seen",
                    )
                    continue
                if options.mode in SKIP_EXISTING_MODES and await self._store.has_response(canonical_url):
                    await self._store.record_fetch(
                        crawl_run_id=run_id,
                        endpoint_url=canonical_url,
                        raw_response_id=None,
                        parent_endpoint_url=job.parent_endpoint_url,
                        node_type=job.node_type,
                        follow_mode=job.follow_mode,
                        http_status=None,
                        outcome="skipped_existing",
                    )
                    seen_urls.add(canonical_url)
                    continue
                seen_urls.add(canonical_url)
                batch.append(
                    QueueItem(
                        url=canonical_url,
                        node_type=job.node_type,
                        follow_mode=job.follow_mode,
                        parent_raw_id=job.parent_raw_id,
                        parent_endpoint_url=job.parent_endpoint_url,
                        pagination_key=job.pagination_key,
                    )
                )

            if not batch:
                continue

            endpoint_request_kwargs = self._endpoint_request_kwargs(
                seed_total_items=seed_total_items,
                seed_items_seen=seed_items_seen,
                queue_size=len(queue) + len(batch),
            )
            results = await asyncio.gather(*(self._fetch_job(job, endpoint_request_kwargs=endpoint_request_kwargs) for job in batch))
            for result in results:
                if result.error is not None or result.payload is None:
                    failed_requests += 1
                    await self._store.record_failed_request(
                        crawl_run_id=run_id,
                        category=self._spec.category,
                        node_type=result.job.node_type,
                        endpoint_url=result.job.url,
                        parent_endpoint_url=result.job.parent_endpoint_url,
                        http_status=result.fetch_metadata.status_code,
                        error=result.error or "Unknown fetch error",
                        duration_ms=result.fetch_metadata.duration_ms,
                        attempt_count=result.fetch_metadata.attempt_count,
                        rate_limit_attempts=result.fetch_metadata.rate_limit_attempts,
                        api_key_name=result.fetch_metadata.api_key_name,
                        pool_allocation=result.fetch_metadata.pool_allocation,
                    )
                    await self._store.record_fetch(
                        crawl_run_id=run_id,
                        endpoint_url=result.job.url,
                        raw_response_id=None,
                        parent_endpoint_url=result.job.parent_endpoint_url,
                        node_type=result.job.node_type,
                        follow_mode=result.job.follow_mode,
                        http_status=result.fetch_metadata.status_code,
                        outcome="failed",
                        duration_ms=result.fetch_metadata.duration_ms,
                        attempt_count=result.fetch_metadata.attempt_count,
                        rate_limit_attempts=result.fetch_metadata.rate_limit_attempts,
                        api_key_name=result.fetch_metadata.api_key_name,
                        pool_allocation=result.fetch_metadata.pool_allocation,
                    )
                    if self._reporter is not None:
                        self._reporter.on_request_failed(
                            category=self._spec.category,
                            node_type=result.job.node_type,
                            endpoint_url=result.job.url,
                            http_status=result.fetch_metadata.status_code,
                            error=result.error or "Unknown fetch error",
                            total_failed=failed_requests,
                            duration_ms=result.fetch_metadata.duration_ms,
                            attempt_count=result.fetch_metadata.attempt_count,
                            rate_limit_attempts=result.fetch_metadata.rate_limit_attempts,
                            api_key_name=result.fetch_metadata.api_key_name,
                            pool_allocation=result.fetch_metadata.pool_allocation,
                        )
                    continue

                node = self._spec.nodes.get(result.job.node_type)
                if node is not None and node.high_water_mark_path:
                    for raw_value in select(result.payload, node.high_water_mark_path):
                        parsed = _parse_timestamp(raw_value)
                        if parsed is None:
                            continue
                        if high_water_mark is None or parsed > high_water_mark:
                            high_water_mark = parsed
                if result.job.node_type == self._spec.seed.node_type and node is not None:
                    seed_pages_fetched += 1
                    if node.item_path:
                        seed_items_seen += len(select(result.payload, node.item_path))
                    if seed_total_items is None and node.pagination_path:
                        raw_total = select_one(result.payload, f"{node.pagination_path}.count")
                        if isinstance(raw_total, int):
                            seed_total_items = raw_total
                        elif isinstance(raw_total, str) and raw_total.strip().isdigit():
                            seed_total_items = int(raw_total.strip())
                        effective_limit = self._effective_page_size(node, options)
                        if seed_total_items is not None and effective_limit is not None:
                            try:
                                seed_total_pages = ceil(seed_total_items / effective_limit)
                            except (TypeError, ValueError, ZeroDivisionError):
                                seed_total_pages = None
                    if self._reporter is not None:
                        self._reporter.on_seed_progress(
                            category=self._spec.category,
                            page_number=seed_pages_fetched,
                            total_pages=seed_total_pages,
                            items_seen=seed_items_seen,
                            total_items=seed_total_items,
                        )

                raw_id = await self._store.upsert_response(
                    endpoint_url=result.job.url,
                    source=self._spec.source,
                    category=self._spec.category,
                    node_type=result.job.node_type,
                    parent_id=result.job.parent_raw_id,
                    response=result.payload,
                    latest_crawl_run_id=run_id,
                )
                await self._store.record_fetch(
                    crawl_run_id=run_id,
                    endpoint_url=result.job.url,
                    raw_response_id=raw_id,
                    parent_endpoint_url=result.job.parent_endpoint_url,
                    node_type=result.job.node_type,
                    follow_mode=result.job.follow_mode,
                    http_status=result.fetch_metadata.status_code,
                    outcome="fetched",
                    duration_ms=result.fetch_metadata.duration_ms,
                    attempt_count=result.fetch_metadata.attempt_count,
                    rate_limit_attempts=result.fetch_metadata.rate_limit_attempts,
                    api_key_name=result.fetch_metadata.api_key_name,
                    pool_allocation=result.fetch_metadata.pool_allocation,
                )
                items_fetched += 1
                if self._reporter is not None:
                    self._reporter.on_url_fetched(
                        category=self._spec.category,
                        node_type=result.job.node_type,
                        endpoint_url=result.job.url,
                        http_status=result.fetch_metadata.status_code,
                        total_fetched=items_fetched,
                        duration_ms=result.fetch_metadata.duration_ms,
                        attempt_count=result.fetch_metadata.attempt_count,
                        rate_limit_attempts=result.fetch_metadata.rate_limit_attempts,
                        api_key_name=result.fetch_metadata.api_key_name,
                        pool_allocation=result.fetch_metadata.pool_allocation,
                    )

                if result.job.follow_mode != "recurse":
                    continue
                if node is None:
                    continue

                next_page_url = select_one(result.payload, node.next_page_path) if node.next_page_path else None
                if isinstance(next_page_url, str) and next_page_url.strip():
                    pagination_key = result.job.pagination_key or f"{result.job.node_type}:{result.job.url}"
                    pages_fetched[pagination_key] = pages_fetched.get(pagination_key, 0) + 1
                    if options.max_pages is None or pages_fetched[pagination_key] < options.max_pages:
                        queue.append(
                            QueueItem(
                                url=self._augment_url(next_page_url, node, options),
                                node_type=result.job.node_type,
                                follow_mode="recurse",
                                parent_raw_id=result.job.parent_raw_id,
                                parent_endpoint_url=result.job.parent_endpoint_url,
                                pagination_key=pagination_key,
                            )
                        )

                for edge in node.child_edges:
                    self._enqueue_edge(queue, result.payload, edge, raw_id, result.job.url, options)
                for edge in node.linked_item_edges:
                    self._enqueue_edge(queue, result.payload, edge, raw_id, result.job.url, options)
        return items_fetched, high_water_mark

    async def _fetch_job(self, job: QueueItem, *, endpoint_request_kwargs: dict[str, Any]) -> AsyncFetchResult:
        async with self._semaphore:
            try:
                payload, fetch_metadata = _normalize_fetch_result(
                    await self._client.get_json(
                        job.url,
                        **_supported_client_kwargs(self._client, endpoint_request_kwargs),
                    )
                )
                return AsyncFetchResult(job=job, payload=payload, fetch_metadata=fetch_metadata)
            except Exception as exc:
                response = getattr(exc, "response", None)
                http_status = getattr(response, "status_code", None)
                metadata = getattr(exc, "fetch_metadata", FetchMetadata(status_code=http_status))
                return AsyncFetchResult(
                    job=job,
                    payload=None,
                    fetch_metadata=metadata,
                    error=str(exc),
                )

    def _enqueue_edge(
        self,
        queue: deque[QueueItem],
        payload: dict[str, Any],
        edge: EdgeSpec,
        parent_raw_id: str,
        parent_endpoint_url: str,
        options: CrawlOptions,
    ) -> None:
        node = self._spec.nodes.get(edge.target)
        for raw_url in _unique_nonempty(select(payload, edge.url_path)):
            if not raw_url.startswith("http"):
                continue
            if not is_allowed_host(raw_url, self._spec.url_allowlist_hosts):
                continue
            normalized = self._canonicalize_url(raw_url)
            if node is not None:
                normalized = self._augment_url(normalized, node, options)
            queue.append(
                QueueItem(
                    url=normalized,
                    node_type=edge.target,
                    follow_mode=edge.follow_mode,
                    parent_raw_id=parent_raw_id,
                    parent_endpoint_url=parent_endpoint_url,
                    pagination_key=normalized if edge.follow_mode == "recurse" else None,
                )
            )

    def _augment_url(self, url: str, node: NodeSpec, options: CrawlOptions) -> str:
        params: dict[str, str | int | None] = dict(node.default_query_params)
        if options.page_size is not None:
            if "limit" in params:
                params["limit"] = options.page_size
            if "pageSize" in params:
                params["pageSize"] = options.page_size
        if "fromDateTime" in node.incremental_query_params:
            params["fromDateTime"] = options.from_datetime
        if "toDateTime" in node.incremental_query_params:
            params["toDateTime"] = options.to_datetime
        return with_query_params(self._canonicalize_url(url), params)
