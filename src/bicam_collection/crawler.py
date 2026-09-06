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
from bicam_collection.storage import StoredNode, StoredResponseRef
from bicam_collection.urls import (
    append_url_path,
    build_seed_url,
    canonicalize_url,
    congress_source_fallback_url,
    is_allowed_host,
    normalize_congress_treaty_committees_url,
    replace_query_params,
    with_query_params,
)


AUTO_HIGH_WATER_MARK_MODES = {"incremental", "resume"}
SKIP_EXISTING_MODES = {"resume"}
DESCENDANT_REFRESH_MODE = "refresh_descendants"
BACKFILL_DESCENDANTS_MODE = "backfill_descendants"
BACKFILL_REFRESH_DESCENDANTS_MODE = "backfill_refresh_descendants"
DESCENDANT_REFRESH_MODES = {DESCENDANT_REFRESH_MODE, BACKFILL_REFRESH_DESCENDANTS_MODE}
BACKFILL_MODES = {BACKFILL_DESCENDANTS_MODE, BACKFILL_REFRESH_DESCENDANTS_MODE}
# Existing descendants are common in historical backfills. Keep network
# concurrency bounded by the caller, but inspect a moderately larger stored
# URL batch so a large already-ingested partition does not spend most of its
# time on one database round trip per network-sized group.
_BACKFILL_STORED_LOOKUP_BATCH_SIZE = 100


def _normalize_edge_url(url: str, edge: EdgeSpec) -> str:
    if edge.url_normalizer == "congress_treaty_committees":
        return normalize_congress_treaty_committees_url(url)
    return url


@dataclass(slots=True)
class CrawlOptions:
    mode: str
    from_datetime: str | None = None
    to_datetime: str | None = None
    seed_path: str | None = None
    seed_node_type: str | None = None
    max_pages: int | None = None
    batch_size: int | None = None
    page_size: int | None = None
    refresh_after_endpoint_url: str | None = None


@dataclass(slots=True)
class QueueItem:
    url: str
    node_type: str
    follow_mode: str
    parent_raw_id: str | None = None
    parent_endpoint_url: str | None = None
    pagination_key: str | None = None


def _response_ids(response_ref: Any) -> tuple[str, str | None]:
    """Accept legacy fake stores while using exact version IDs in Postgres."""
    raw_response_id = getattr(response_ref, "raw_response_id", response_ref)
    raw_response_version_id = getattr(response_ref, "raw_response_version_id", None)
    return str(raw_response_id), raw_response_version_id


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
    def get_stored_node(self, endpoint_url: str) -> StoredNode | None: ...
    def list_refresh_nodes(
        self,
        *,
        category: str,
        node_type: str,
        limit: int,
        after_endpoint_url: str | None = None,
    ) -> list[StoredNode]: ...
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
    ) -> str | StoredResponseRef: ...
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
        raw_response_version_id: str | None = None,
        duration_ms: int | None = None,
        attempt_count: int | None = None,
        rate_limit_attempts: int | None = None,
        api_key_name: str | None = None,
        pool_allocation: dict[str, Any] | None = None,
    ) -> None: ...
    def record_expected_fetch(
        self,
        *,
        crawl_run_id: str,
        endpoint_url: str,
        parent_endpoint_url: str | None,
        node_type: str,
        follow_mode: str,
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
    async def get_stored_node(self, endpoint_url: str) -> StoredNode | None: ...
    async def get_stored_nodes(self, endpoint_urls: list[str]) -> dict[str, StoredNode]: ...
    async def list_refresh_nodes(
        self,
        *,
        category: str,
        node_type: str,
        limit: int,
        after_endpoint_url: str | None = None,
    ) -> list[StoredNode]: ...
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
    ) -> str | StoredResponseRef: ...
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
        raw_response_version_id: str | None = None,
        duration_ms: int | None = None,
        attempt_count: int | None = None,
        rate_limit_attempts: int | None = None,
        api_key_name: str | None = None,
        pool_allocation: dict[str, Any] | None = None,
    ) -> None: ...
    async def record_expected_fetch(
        self,
        *,
        crawl_run_id: str,
        endpoint_url: str,
        parent_endpoint_url: str | None,
        node_type: str,
        follow_mode: str,
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


def _record_expected_fetch(
    store: Any,
    *,
    crawl_run_id: str,
    endpoint_url: str,
    parent_endpoint_url: str | None,
    node_type: str,
    follow_mode: str,
) -> None:
    """Record queue discovery when the backing store supports it.

    The optional lookup keeps the crawler compatible with the lightweight fake
    stores used by contract tests and with downstream integrations that have
    not yet adopted migration 004.
    """
    method = getattr(store, "record_expected_fetch", None)
    if method is not None:
        method(
            crawl_run_id=crawl_run_id,
            endpoint_url=endpoint_url,
            parent_endpoint_url=parent_endpoint_url,
            node_type=node_type,
            follow_mode=follow_mode,
        )


async def _record_expected_fetch_async(
    store: Any,
    *,
    crawl_run_id: str,
    endpoint_url: str,
    parent_endpoint_url: str | None,
    node_type: str,
    follow_mode: str,
) -> None:
    method = getattr(store, "record_expected_fetch", None)
    if method is None:
        return
    result = method(
        crawl_run_id=crawl_run_id,
        endpoint_url=endpoint_url,
        parent_endpoint_url=parent_endpoint_url,
        node_type=node_type,
        follow_mode=follow_mode,
    )
    if inspect.isawaitable(result):
        await result


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
        self.last_refresh_endpoint_url: str | None = None
        self.last_refresh_root_count = 0

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
        path = options.seed_path or self._spec.seed.path
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
        if options.mode in DESCENDANT_REFRESH_MODES:
            return self._run_refresh_descendants(options)
        if self._spec.seed.requires_partition and options.seed_path is None:
            raise ValueError(
                f"Category '{self._spec.category}' requires an explicit partitioned seed path"
            )

        effective_from_datetime = options.from_datetime
        if options.mode in AUTO_HIGH_WATER_MARK_MODES and not effective_from_datetime:
            effective_from_datetime = _format_timestamp(self._store.get_latest_high_water_mark(self._spec.category))

        seed_node_type = options.seed_node_type or self._spec.seed.node_type
        if seed_node_type not in self._spec.nodes:
            raise ValueError(f"Unknown seed node type '{seed_node_type}' for category '{self._spec.category}'")
        effective_options = CrawlOptions(
            mode=options.mode,
            from_datetime=effective_from_datetime,
            to_datetime=options.to_datetime,
            seed_path=options.seed_path,
            seed_node_type=seed_node_type,
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
                        self._spec.nodes[seed_node_type],
                        effective_options,
                    ),
                    node_type=seed_node_type,
                    follow_mode="recurse",
                    pagination_key=seed_node_type,
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
        except (asyncio.CancelledError, KeyboardInterrupt):
            self._store.finish_run(run_id, "failed", items_fetched, high_water_mark=high_water_mark)
            if self._reporter is not None:
                self._reporter.on_run_finished(
                    category=self._spec.category,
                    run_id=run_id,
                    status="failed",
                    items_fetched=items_fetched,
                )
            raise
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
        queue: deque[QueueItem] = deque()
        self.last_refresh_endpoint_url = None
        self.last_refresh_root_count = 0
        try:
            refresh_kwargs: dict[str, Any] = {
                "category": self._spec.category,
                "node_type": refresh_root_node_type,
                "limit": batch_size,
            }
            if options.refresh_after_endpoint_url is not None:
                refresh_kwargs["after_endpoint_url"] = options.refresh_after_endpoint_url
            refresh_roots = self._store.list_refresh_nodes(**refresh_kwargs)
            self.last_refresh_root_count = len(refresh_roots)
            if refresh_roots:
                self.last_refresh_endpoint_url = refresh_roots[-1].endpoint_url
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
                            seed_path=options.seed_path,
                            seed_node_type=options.seed_node_type,
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
                    seed_path=options.seed_path,
                    seed_node_type=options.seed_node_type,
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
        seed_node_type = options.seed_node_type or self._spec.seed.node_type
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
            _record_expected_fetch(
                self._store,
                crawl_run_id=run_id,
                endpoint_url=canonical_url,
                parent_endpoint_url=job.parent_endpoint_url,
                node_type=job.node_type,
                follow_mode=job.follow_mode,
            )
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

            if options.mode in BACKFILL_MODES and self._store.has_response(canonical_url):
                stored = self._store.get_stored_node(canonical_url)
                self._store.record_fetch(
                    crawl_run_id=run_id,
                    endpoint_url=canonical_url,
                    raw_response_id=stored.raw_id if stored else None,
                    raw_response_version_id=(
                        getattr(stored, "raw_response_version_id", None)
                        if stored
                        else None
                    ),
                    parent_endpoint_url=job.parent_endpoint_url,
                    node_type=job.node_type,
                    follow_mode=job.follow_mode,
                    http_status=None,
                    outcome="skipped_existing_backfill",
                )
                if stored and job.follow_mode == "recurse":
                    node = self._spec.nodes.get(job.node_type)
                    if node is not None:
                        next_page_url = select_one(stored.response, node.next_page_path) if node.next_page_path else None
                        if isinstance(next_page_url, str) and next_page_url.strip():
                            pagination_key = job.pagination_key or f"{job.node_type}:{canonical_url}"
                            pages_fetched[pagination_key] = pages_fetched.get(pagination_key, 0) + 1
                            if options.max_pages is None or pages_fetched[pagination_key] < options.max_pages:
                                # Prioritize pagination over the child edges
                                # already queued from this stored page.  A
                                # backfill can otherwise inspect a large
                                # candidate batch of descendants before it
                                # reaches the next list page, delaying full
                                # partition discovery even when those child
                                # records are already stored.
                                queue.appendleft(
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
                            self._enqueue_edge(queue, stored.response, edge, stored.raw_id, canonical_url, options)
                        for edge in node.linked_item_edges:
                            self._enqueue_edge(queue, stored.response, edge, stored.raw_id, canonical_url, options)
                continue

            # Incremental child suppression is an optimization, not a new
            # hard requirement on lightweight/third-party stores.  Stores
            # that have not implemented node retrieval retain the original
            # refetch-and-recurse behavior.
            get_stored_node = getattr(self._store, "get_stored_node", None)
            prior_stored = (
                get_stored_node(canonical_url)
                if options.mode == "incremental" and get_stored_node is not None
                else None
            )

            try:
                request_kwargs = self._endpoint_request_kwargs(
                    seed_total_items=seed_total_items,
                    seed_items_seen=seed_items_seen,
                    queue_size=len(queue),
                )
                try:
                    payload, fetch_metadata = _normalize_fetch_result(
                        self._client.get_json(canonical_url, **_supported_client_kwargs(self._client, request_kwargs))
                    )
                except Exception as exc:
                    # Congress.gov has one known deterministic hearing-list
                    # NoReverseMatch at a page offset when parameters are
                    # sorted canonically. Retry that exact page once using the
                    # order emitted by the older successful link, while
                    # retaining the canonical URL as the storage identity.
                    fallback_url = congress_source_fallback_url(
                        canonical_url, node_type=job.node_type
                    )
                    response = getattr(exc, "response", None)
                    if (
                        fallback_url is None
                        or getattr(response, "status_code", None) != 500
                        or "source_error=no_reverse_match" not in str(exc)
                    ):
                        raise
                    payload, fetch_metadata = _normalize_fetch_result(
                        self._client.get_json(
                            fallback_url,
                            **_supported_client_kwargs(self._client, request_kwargs),
                        )
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
            if job.node_type == seed_node_type and node is not None:
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
            response_ref = self._store.upsert_response(
                endpoint_url=canonical_url,
                source=self._spec.source,
                category=self._spec.category,
                node_type=job.node_type,
                parent_id=job.parent_raw_id,
                response=payload,
                latest_crawl_run_id=run_id,
            )
            raw_id, raw_version_id = _response_ids(response_ref)
            self._store.record_fetch(
                crawl_run_id=run_id,
                endpoint_url=canonical_url,
                raw_response_id=raw_id,
                parent_endpoint_url=job.parent_endpoint_url,
                node_type=job.node_type,
                follow_mode=job.follow_mode,
                http_status=fetch_metadata.status_code,
                outcome="fetched",
                raw_response_version_id=raw_version_id,
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
                    queue.appendleft(
                        QueueItem(
                            url=self._augment_url(next_page_url, node, options),
                            node_type=job.node_type,
                            follow_mode="recurse",
                            parent_raw_id=job.parent_raw_id,
                            parent_endpoint_url=job.parent_endpoint_url,
                            pagination_key=pagination_key,
                        )
                    )

            # An incremental run must refetch the detail itself so raw-version
            # history and update-date changes remain observable.  Re-walking
            # an unchanged parent's complete child tree, however, turns a
            # small update window into tens of thousands of redundant calls.
            # Preserve pagination above, then let the separate descendant
            # refresh/audit lanes handle independently changing children.
            if prior_stored is not None and prior_stored.response == payload:
                continue

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
        raw_urls = _unique_nonempty(select(payload, edge.url_path))
        if not raw_urls and edge.fallback_url_suffix:
            raw_urls = [append_url_path(parent_endpoint_url, edge.fallback_url_suffix)]
        for raw_url in raw_urls:
            if not raw_url.startswith("http"):
                continue
            if not is_allowed_host(raw_url, self._spec.url_allowlist_hosts):
                continue
            normalized = _normalize_edge_url(self._canonicalize_url(raw_url), edge)
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
        page_size_overrides: dict[str, int] = {}
        if options.page_size is not None:
            if "limit" in params:
                params["limit"] = options.page_size
                page_size_overrides["limit"] = options.page_size
            if "pageSize" in params:
                params["pageSize"] = options.page_size
                page_size_overrides["pageSize"] = options.page_size
        if "fromDateTime" in node.incremental_query_params:
            params["fromDateTime"] = options.from_datetime
        if "toDateTime" in node.incremental_query_params:
            params["toDateTime"] = options.to_datetime
        # Add missing defaults first so seed URLs receive their cursor and
        # date bounds. An explicit page-size then replaces only the size
        # parameter. Replacing every default here would reset a nextPage
        # offsetMark/offset back to the first page.
        augmented = with_query_params(self._canonicalize_url(url), params)
        if page_size_overrides:
            return replace_query_params(augmented, page_size_overrides)
        return augmented


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
        self.last_refresh_endpoint_url: str | None = None
        self.last_refresh_root_count = 0

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
        path = options.seed_path or self._spec.seed.path
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
        if options.mode in DESCENDANT_REFRESH_MODES:
            return await self._run_refresh_descendants(options)
        if self._spec.seed.requires_partition and options.seed_path is None:
            raise ValueError(
                f"Category '{self._spec.category}' requires an explicit partitioned seed path"
            )

        effective_from_datetime = options.from_datetime
        if options.mode in AUTO_HIGH_WATER_MARK_MODES and not effective_from_datetime:
            effective_from_datetime = _format_timestamp(await self._store.get_latest_high_water_mark(self._spec.category))

        seed_node_type = options.seed_node_type or self._spec.seed.node_type
        if seed_node_type not in self._spec.nodes:
            raise ValueError(f"Unknown seed node type '{seed_node_type}' for category '{self._spec.category}'")
        effective_options = CrawlOptions(
            mode=options.mode,
            from_datetime=effective_from_datetime,
            to_datetime=options.to_datetime,
            seed_path=options.seed_path,
            seed_node_type=seed_node_type,
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
                        self._spec.nodes[seed_node_type],
                        effective_options,
                    ),
                    node_type=seed_node_type,
                    follow_mode="recurse",
                    pagination_key=seed_node_type,
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
        except asyncio.CancelledError:
            await self._store.finish_run(run_id, "failed", items_fetched, high_water_mark=high_water_mark)
            if self._reporter is not None:
                self._reporter.on_run_finished(
                    category=self._spec.category,
                    run_id=run_id,
                    status="failed",
                    items_fetched=items_fetched,
                )
            raise
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
        queue: deque[QueueItem] = deque()
        self.last_refresh_endpoint_url = None
        self.last_refresh_root_count = 0
        try:
            refresh_kwargs: dict[str, Any] = {
                "category": self._spec.category,
                "node_type": refresh_root_node_type,
                "limit": batch_size,
            }
            if options.refresh_after_endpoint_url is not None:
                refresh_kwargs["after_endpoint_url"] = options.refresh_after_endpoint_url
            refresh_roots = await self._store.list_refresh_nodes(**refresh_kwargs)
            self.last_refresh_root_count = len(refresh_roots)
            if refresh_roots:
                self.last_refresh_endpoint_url = refresh_roots[-1].endpoint_url
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
                            seed_path=options.seed_path,
                            seed_node_type=options.seed_node_type,
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
                    seed_path=options.seed_path,
                    seed_node_type=options.seed_node_type,
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
        seed_node_type = options.seed_node_type or self._spec.seed.node_type
        seen_urls: set[str] = set()
        items_fetched = 0
        pages_fetched: dict[str, int] = {}
        seed_pages_fetched = 0
        seed_items_seen = 0
        seed_total_items: int | None = None
        seed_total_pages: int | None = None
        failed_requests = 0
        pending_network: deque[QueueItem] = deque()
        while queue or pending_network:
            batch: list[QueueItem] = []
            candidates: list[QueueItem] = []
            # Existing descendants are common in a backfill.  Inspect a bounded
            # candidate batch at once so stored-node lookups do not become one
            # database round trip per URL.  Network work remains limited by the
            # semaphore in _fetch_job.
            # Keep ordinary crawls bounded to the network batch. For
            # backfills, a larger stored lookup batch amortizes indexed
            # raw-response lookups across already-ingested descendants. Any
            # missing URLs still flow through ``pending_network`` and are
            # drained at the normal network concurrency, so API pressure is
            # unchanged; the bound prevents pagination from being starved by
            # an unbounded candidate expansion.
            candidate_limit = self._concurrency
            if options.mode in BACKFILL_MODES:
                candidate_limit = max(
                    candidate_limit,
                    min(
                        _BACKFILL_STORED_LOOKUP_BATCH_SIZE,
                        options.page_size or _BACKFILL_STORED_LOOKUP_BATCH_SIZE,
                    ),
                )
            if pending_network:
                while pending_network and len(batch) < self._concurrency:
                    batch.append(pending_network.popleft())
            else:
                expected_items: list[QueueItem] = []
                skipped_seen_records: list[dict[str, Any]] = []
                while queue and len(candidates) < candidate_limit:
                    job = queue.popleft()
                    if not is_allowed_host(job.url, self._spec.url_allowlist_hosts):
                        continue
                    canonical_url = self._canonicalize_url(job.url)
                    expected_items.append(
                        QueueItem(
                            url=canonical_url,
                            node_type=job.node_type,
                            follow_mode=job.follow_mode,
                            parent_raw_id=job.parent_raw_id,
                            parent_endpoint_url=job.parent_endpoint_url,
                            pagination_key=job.pagination_key,
                        )
                    )
                    if canonical_url in seen_urls:
                        skipped_seen_records.append(
                            {
                                "crawl_run_id": run_id,
                                "endpoint_url": canonical_url,
                                "raw_response_id": None,
                                "parent_endpoint_url": job.parent_endpoint_url,
                                "node_type": job.node_type,
                                "follow_mode": job.follow_mode,
                                "http_status": None,
                                "outcome": "skipped_seen",
                            }
                        )
                        continue
                    seen_urls.add(canonical_url)
                    candidates.append(
                        QueueItem(
                            url=canonical_url,
                            node_type=job.node_type,
                            follow_mode=job.follow_mode,
                            parent_raw_id=job.parent_raw_id,
                            parent_endpoint_url=job.parent_endpoint_url,
                            pagination_key=job.pagination_key,
                        )
                    )

                # Commit telemetry in one transaction when the backing store
                # supports it.  Keep the per-row fallback for lightweight
                # stores used by contract tests and downstream integrations.
                expected_batch = getattr(self._store, "record_expected_fetch_batch", None)
                if expected_batch is not None:
                    result = expected_batch(
                        [
                            {
                                "crawl_run_id": run_id,
                                "endpoint_url": item.url,
                                "parent_endpoint_url": item.parent_endpoint_url,
                                "node_type": item.node_type,
                                "follow_mode": item.follow_mode,
                            }
                            for item in expected_items
                        ]
                    )
                    if inspect.isawaitable(result):
                        await result
                else:
                    await asyncio.gather(
                        *(
                            _record_expected_fetch_async(
                                self._store,
                                crawl_run_id=run_id,
                                endpoint_url=item.url,
                                parent_endpoint_url=item.parent_endpoint_url,
                                node_type=item.node_type,
                                follow_mode=item.follow_mode,
                            )
                            for item in expected_items
                        )
                    )

                if skipped_seen_records:
                    fetch_batch = getattr(self._store, "record_fetch_batch", None)
                    if fetch_batch is not None:
                        result = fetch_batch(skipped_seen_records)
                        if inspect.isawaitable(result):
                            await result
                    else:
                        await asyncio.gather(
                            *(self._store.record_fetch(**record) for record in skipped_seen_records)
                        )

                stored_nodes: dict[str, StoredNode] = {}
                if (
                    options.mode in SKIP_EXISTING_MODES
                    or options.mode in BACKFILL_MODES
                    or options.mode == "incremental"
                ):
                    bulk_lookup = getattr(self._store, "get_stored_nodes", None)
                    if bulk_lookup is not None:
                        stored_nodes = await bulk_lookup([job.url for job in candidates])
                    else:
                        # Keep lightweight test stores and third-party store
                        # implementations compatible while they adopt the bulk API.
                        stored_results = await asyncio.gather(
                            *(self._store.get_stored_node(job.url) for job in candidates)
                        )
                        stored_nodes = {
                            stored.endpoint_url: stored
                            for stored in stored_results
                            if stored is not None
                        }

                skipped_existing_records: list[dict[str, Any]] = []
                for job in candidates:
                    stored = stored_nodes.get(job.url)
                    if options.mode in SKIP_EXISTING_MODES and stored is not None:
                        skipped_existing_records.append(
                            {
                                "crawl_run_id": run_id,
                                "endpoint_url": job.url,
                                "raw_response_id": None,
                                "parent_endpoint_url": job.parent_endpoint_url,
                                "node_type": job.node_type,
                                "follow_mode": job.follow_mode,
                                "http_status": None,
                                "outcome": "skipped_existing",
                            }
                        )
                        continue
                    if options.mode in BACKFILL_MODES and stored is not None:
                        skipped_existing_records.append(
                            {
                                "crawl_run_id": run_id,
                                "endpoint_url": job.url,
                                "raw_response_id": stored.raw_id,
                                "raw_response_version_id": getattr(
                                    stored, "raw_response_version_id", None
                                ),
                                "parent_endpoint_url": job.parent_endpoint_url,
                                "node_type": job.node_type,
                                "follow_mode": job.follow_mode,
                                "http_status": None,
                                "outcome": "skipped_existing_backfill",
                            }
                        )
                        if job.follow_mode == "recurse":
                            node = self._spec.nodes.get(job.node_type)
                            if node is not None:
                                next_page_url = select_one(stored.response, node.next_page_path) if node.next_page_path else None
                                if isinstance(next_page_url, str) and next_page_url.strip():
                                    pagination_key = job.pagination_key or f"{job.node_type}:{job.url}"
                                    pages_fetched[pagination_key] = pages_fetched.get(pagination_key, 0) + 1
                                    if options.max_pages is None or pages_fetched[pagination_key] < options.max_pages:
                                        # Keep list pagination ahead of the
                                        # descendant fan-out for stored pages;
                                        # descendants remain in the deque and
                                        # are drained after the partition is
                                        # discovered.
                                        queue.appendleft(
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
                                    self._enqueue_edge(queue, stored.response, edge, stored.raw_id, job.url, options)
                                for edge in node.linked_item_edges:
                                    self._enqueue_edge(queue, stored.response, edge, stored.raw_id, job.url, options)
                        continue

                    batch.append(job)

                if skipped_existing_records:
                    fetch_batch = getattr(self._store, "record_fetch_batch", None)
                    if fetch_batch is not None:
                        result = fetch_batch(skipped_existing_records)
                        if inspect.isawaitable(result):
                            await result
                    else:
                        await asyncio.gather(
                            *(self._store.record_fetch(**record) for record in skipped_existing_records)
                        )

                if len(batch) > self._concurrency:
                    pending_network.extend(batch[self._concurrency :])
                    batch = batch[: self._concurrency]

            if not batch:
                continue

            endpoint_request_kwargs = self._endpoint_request_kwargs(
                seed_total_items=seed_total_items,
                seed_items_seen=seed_items_seen,
                queue_size=len(queue) + len(batch),
            )
            results = await asyncio.gather(
                *(self._fetch_job(job, endpoint_request_kwargs=endpoint_request_kwargs) for job in batch)
            )
            # Network work is concurrent, so persist the corresponding raw
            # response and fetch telemetry concurrently as well.  The old
            # loop committed two or three database transactions serially for
            # every response, leaving most API-key slots idle while a batch
            # was written.  The store's connection pool still bounds database
            # pressure; queue expansion remains ordered below.
            persisted_results = await asyncio.gather(
                *(self._persist_fetch_result(run_id, result) for result in results)
            )
            for result, response_ref in zip(results, persisted_results, strict=True):
                if result.error is not None or result.payload is None:
                    failed_requests += 1
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
                if result.job.node_type == seed_node_type and node is not None:
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

                assert response_ref is not None
                raw_id, raw_version_id = _response_ids(response_ref)
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
                        queue.appendleft(
                            QueueItem(
                                url=self._augment_url(next_page_url, node, options),
                                node_type=result.job.node_type,
                                follow_mode="recurse",
                                parent_raw_id=result.job.parent_raw_id,
                                parent_endpoint_url=result.job.parent_endpoint_url,
                                pagination_key=pagination_key,
                            )
                        )

                prior_stored = stored_nodes.get(result.job.url)
                if (
                    options.mode == "incremental"
                    and prior_stored is not None
                    and prior_stored.response == result.payload
                ):
                    continue

                for edge in node.child_edges:
                    self._enqueue_edge(queue, result.payload, edge, raw_id, result.job.url, options)
                for edge in node.linked_item_edges:
                    self._enqueue_edge(queue, result.payload, edge, raw_id, result.job.url, options)
        return items_fetched, high_water_mark

    async def _persist_fetch_result(
        self,
        run_id: str,
        result: AsyncFetchResult,
    ) -> str | StoredResponseRef | None:
        """Persist one network result, allowing a batch to share DB latency."""
        if result.error is not None or result.payload is None:
            await asyncio.gather(
                self._store.record_failed_request(
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
                ),
                self._store.record_fetch(
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
                ),
            )
            return None

        response_ref = await self._store.upsert_response(
            endpoint_url=result.job.url,
            source=self._spec.source,
            category=self._spec.category,
            node_type=result.job.node_type,
            parent_id=result.job.parent_raw_id,
            response=result.payload,
            latest_crawl_run_id=run_id,
        )
        raw_id, raw_version_id = _response_ids(response_ref)
        await self._store.record_fetch(
            crawl_run_id=run_id,
            endpoint_url=result.job.url,
            raw_response_id=raw_id,
            parent_endpoint_url=result.job.parent_endpoint_url,
            node_type=result.job.node_type,
            follow_mode=result.job.follow_mode,
            http_status=result.fetch_metadata.status_code,
            outcome="fetched",
            raw_response_version_id=raw_version_id,
            duration_ms=result.fetch_metadata.duration_ms,
            attempt_count=result.fetch_metadata.attempt_count,
            rate_limit_attempts=result.fetch_metadata.rate_limit_attempts,
            api_key_name=result.fetch_metadata.api_key_name,
            pool_allocation=result.fetch_metadata.pool_allocation,
        )
        return response_ref

    async def _fetch_job(self, job: QueueItem, *, endpoint_request_kwargs: dict[str, Any]) -> AsyncFetchResult:
        async with self._semaphore:
            try:
                try:
                    payload, fetch_metadata = _normalize_fetch_result(
                        await self._client.get_json(
                            job.url,
                            **_supported_client_kwargs(self._client, endpoint_request_kwargs),
                        )
                    )
                except Exception as exc:
                    # See the synchronous path above. Keep this fallback
                    # bounded to the explicitly classified source defect so a
                    # generic 500 is never replayed opportunistically.
                    fallback_url = congress_source_fallback_url(
                        job.url, node_type=job.node_type
                    )
                    response = getattr(exc, "response", None)
                    if (
                        fallback_url is None
                        or getattr(response, "status_code", None) != 500
                        or "source_error=no_reverse_match" not in str(exc)
                    ):
                        raise
                    payload, fetch_metadata = _normalize_fetch_result(
                        await self._client.get_json(
                            fallback_url,
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
        raw_urls = _unique_nonempty(select(payload, edge.url_path))
        if not raw_urls and edge.fallback_url_suffix:
            raw_urls = [append_url_path(parent_endpoint_url, edge.fallback_url_suffix)]
        for raw_url in raw_urls:
            if not raw_url.startswith("http"):
                continue
            if not is_allowed_host(raw_url, self._spec.url_allowlist_hosts):
                continue
            normalized = _normalize_edge_url(self._canonicalize_url(raw_url), edge)
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
        page_size_overrides: dict[str, int] = {}
        if options.page_size is not None:
            if "limit" in params:
                params["limit"] = options.page_size
                page_size_overrides["limit"] = options.page_size
            if "pageSize" in params:
                params["pageSize"] = options.page_size
                page_size_overrides["pageSize"] = options.page_size
        if "fromDateTime" in node.incremental_query_params:
            params["fromDateTime"] = options.from_datetime
        if "toDateTime" in node.incremental_query_params:
            params["toDateTime"] = options.to_datetime
        augmented = with_query_params(self._canonicalize_url(url), params)
        if page_size_overrides:
            return replace_query_params(augmented, page_size_overrides)
        return augmented
