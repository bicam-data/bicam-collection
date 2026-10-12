from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from math import ceil
from typing import Any, Protocol

from bicam_collection.client import FetchMetadata, FetchResult
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
STORED_LOOKUP_MODES = SKIP_EXISTING_MODES | BACKFILL_MODES | {"incremental"}
# Backfills mostly revisit stored URLs, so the async crawler checks this many
# against the database per round trip; only missing ones go to the network.
_BACKFILL_STORED_LOOKUP_BATCH_SIZE = 100
_DEFAULT_REFRESH_BATCH_SIZE = 100


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


@dataclass(slots=True)
class _SeedProgress:
    pages_fetched: int = 0
    items_seen: int = 0
    total_items: int | None = None
    total_pages: int | None = None


class StoreProtocol(Protocol):
    def start_run(self, category: str, mode: str, high_water_mark: datetime | None = None) -> str: ...
    def get_latest_high_water_mark(self, category: str) -> datetime | None: ...
    def finish_run(self, run_id: str, status: str, items_fetched: int, high_water_mark: datetime | None = None) -> None: ...
    def has_response(self, endpoint_url: str) -> bool: ...
    def get_stored_node(self, endpoint_url: str) -> StoredNode | None: ...
    def list_refresh_nodes(
        self, *, category: str, node_type: str, limit: int, after_endpoint_url: str | None = None
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
    ) -> StoredResponseRef: ...
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


class AsyncStoreProtocol(Protocol):
    async def start_run(self, category: str, mode: str, high_water_mark: datetime | None = None) -> str: ...
    async def get_latest_high_water_mark(self, category: str) -> datetime | None: ...
    async def finish_run(self, run_id: str, status: str, items_fetched: int, high_water_mark: datetime | None = None) -> None: ...
    async def get_stored_nodes(self, endpoint_urls: list[str]) -> dict[str, StoredNode]: ...
    async def list_refresh_nodes(
        self, *, category: str, node_type: str, limit: int, after_endpoint_url: str | None = None
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
    ) -> StoredResponseRef: ...
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
    async def record_fetch_batch(self, records: list[dict[str, Any]]) -> None: ...
    async def record_expected_fetch_batch(self, records: list[dict[str, Any]]) -> None: ...
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


class ReporterProtocol(Protocol):
    def on_seed_progress(
        self, *, category: str, page_number: int, total_pages: int | None, items_seen: int, total_items: int | None
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
    def on_run_finished(self, *, category: str, run_id: str, status: str, items_fetched: int) -> None: ...


def _unique_nonempty(values: list[Any]) -> list[str]:
    seen: set[str] = set()
    unique: list[str] = []
    for value in values:
        if not isinstance(value, str):
            continue
        candidate = value.strip()
        if candidate and candidate not in seen:
            seen.add(candidate)
            unique.append(candidate)
    return unique


def _parse_timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _format_timestamp(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _telemetry(metadata: FetchMetadata) -> dict[str, Any]:
    return {
        "duration_ms": metadata.duration_ms,
        "attempt_count": metadata.attempt_count,
        "rate_limit_attempts": metadata.rate_limit_attempts,
        "api_key_name": metadata.api_key_name,
        "pool_allocation": metadata.pool_allocation,
    }


def _failure_metadata(exc: Exception) -> FetchMetadata:
    metadata = getattr(exc, "fetch_metadata", None)
    if metadata is not None:
        return metadata
    response = getattr(exc, "response", None)
    return FetchMetadata(status_code=getattr(response, "status_code", None))


def _source_fallback_url(exc: Exception, job: QueueItem) -> str | None:
    # Congress.gov raises a deterministic NoReverseMatch for one hearing-list
    # page when its query is sorted canonically; the older parameter order
    # works. The canonical URL stays the storage identity.
    response = getattr(exc, "response", None)
    if getattr(response, "status_code", None) != 500 or "source_error=no_reverse_match" not in str(exc):
        return None
    return congress_source_fallback_url(job.url, node_type=job.node_type)


def _remaining_work_priority(remaining_work: int) -> int:
    for priority, threshold in enumerate((100_000, 25_000, 5_000, 1_000), start=1):
        if remaining_work >= threshold:
            return priority
    return 5


class _CrawlerBase:
    """Spec traversal shared by the sync and async crawlers; subclasses do the I/O."""

    def __init__(self, *, base_url: str, spec: CrawlSpec, client: Any, store: Any, reporter: ReporterProtocol | None) -> None:
        self._base_url = base_url
        self._spec = spec
        self._client = client
        self._store = store
        self._reporter = reporter
        self.last_refresh_endpoint_url: str | None = None
        self.last_refresh_root_count = 0

    def _endpoint_request_kwargs(self, progress: _SeedProgress, queue_size: int) -> dict[str, Any]:
        if progress.total_items is not None:
            remaining_work = max(1, progress.total_items - progress.items_seen + queue_size)
        else:
            remaining_work = queue_size + 1
        return {
            "endpoint_name": self._spec.category,
            "endpoint_priority": _remaining_work_priority(remaining_work),
            "endpoint_reserve": 1,
        }

    def _canonicalize_url(self, url: str) -> str:
        return canonicalize_url(url, self._spec.source)

    def _seed_options(self, options: CrawlOptions, from_datetime: str | None) -> CrawlOptions:
        if self._spec.seed.requires_partition and options.seed_path is None:
            raise ValueError(f"Category '{self._spec.category}' requires an explicit partitioned seed path")
        seed_node_type = options.seed_node_type or self._spec.seed.node_type
        if seed_node_type not in self._spec.nodes:
            raise ValueError(f"Unknown seed node type '{seed_node_type}' for category '{self._spec.category}'")
        return replace(options, from_datetime=from_datetime, seed_node_type=seed_node_type)

    def _seed_queue(self, options: CrawlOptions) -> deque[QueueItem]:
        assert options.seed_node_type is not None
        seed_url = build_seed_url(self._base_url, self._resolve_seed_path(options))
        return deque([
            QueueItem(
                url=self._augment_url(seed_url, self._spec.nodes[options.seed_node_type], options),
                node_type=options.seed_node_type,
                follow_mode="recurse",
                pagination_key=options.seed_node_type,
            )
        ])

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

    def _refresh_root_node_type(self) -> str:
        node_type = self._spec.refresh_root_node_type
        if not node_type:
            raise ValueError(f"refresh_descendants is not supported for category '{self._spec.category}'")
        return node_type

    def _queue_refresh_roots(self, roots: list[StoredNode], options: CrawlOptions) -> deque[QueueItem]:
        self.last_refresh_root_count = len(roots)
        self.last_refresh_endpoint_url = roots[-1].endpoint_url if roots else None
        root_node = self._spec.nodes[self._refresh_root_node_type()]
        queue: deque[QueueItem] = deque()
        for root in roots:
            for edge in root_node.child_edges:
                self._enqueue_edge(queue, root.response, edge, root.raw_id, root.endpoint_url, options)
        return queue

    def _effective_page_size(self, node: NodeSpec, options: CrawlOptions) -> int | None:
        for key in ("limit", "pageSize"):
            raw_value = node.default_query_params.get(key)
            if raw_value is None:
                continue
            if options.page_size is not None:
                return options.page_size
            try:
                return int(raw_value)
            except ValueError:
                return None
        return None

    def _observe_payload(
        self,
        job: QueueItem,
        payload: dict[str, Any],
        options: CrawlOptions,
        progress: _SeedProgress,
        high_water_mark: datetime | None,
    ) -> datetime | None:
        """Advance the high-water mark and report seed-list progress for one fetched page."""
        node = self._spec.nodes[job.node_type]
        if node.high_water_mark_path:
            for raw_value in select(payload, node.high_water_mark_path):
                parsed = _parse_timestamp(raw_value)
                if parsed is not None and (high_water_mark is None or parsed > high_water_mark):
                    high_water_mark = parsed
        if job.node_type != options.seed_node_type:
            return high_water_mark

        progress.pages_fetched += 1
        if node.item_path:
            progress.items_seen += len(select(payload, node.item_path))
        if progress.total_items is None and node.pagination_path:
            raw_total = select_one(payload, f"{node.pagination_path}.count")
            if isinstance(raw_total, int):
                progress.total_items = raw_total
            elif isinstance(raw_total, str) and raw_total.strip().isdigit():
                progress.total_items = int(raw_total)
            page_size = self._effective_page_size(node, options)
            if progress.total_items is not None and page_size:
                progress.total_pages = ceil(progress.total_items / page_size)
        if self._reporter is not None:
            self._reporter.on_seed_progress(
                category=self._spec.category,
                page_number=progress.pages_fetched,
                total_pages=progress.total_pages,
                items_seen=progress.items_seen,
                total_items=progress.total_items,
            )
        return high_water_mark

    def _follow(
        self,
        queue: deque[QueueItem],
        job: QueueItem,
        payload: dict[str, Any],
        raw_id: str,
        options: CrawlOptions,
        pages_fetched: dict[str, int],
        *,
        unchanged: bool = False,
    ) -> None:
        if job.follow_mode != "recurse":
            return
        node = self._spec.nodes[job.node_type]
        next_page_url = select_one(payload, node.next_page_path) if node.next_page_path else None
        if isinstance(next_page_url, str) and next_page_url.strip():
            pagination_key = job.pagination_key or f"{job.node_type}:{job.url}"
            pages_fetched[pagination_key] = pages_fetched.get(pagination_key, 0) + 1
            if options.max_pages is None or pages_fetched[pagination_key] < options.max_pages:
                # Pages jump the queue so a whole list is discovered before
                # its (possibly very large) child fan-out is worked through.
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
        # An incremental run refetches each changed detail so its version
        # history is kept, but re-walking an unchanged parent's child tree
        # would turn a small window into tens of thousands of calls. The
        # descendant refresh lanes cover children that change on their own.
        if unchanged:
            return
        for edge in [*node.child_edges, *node.linked_item_edges]:
            self._enqueue_edge(queue, payload, edge, raw_id, job.url, options)

    def _enqueue_edge(
        self,
        queue: deque[QueueItem],
        payload: dict[str, Any],
        edge: EdgeSpec,
        parent_raw_id: str,
        parent_endpoint_url: str,
        options: CrawlOptions,
    ) -> None:
        raw_urls = _unique_nonempty(select(payload, edge.url_path))
        if not raw_urls and edge.fallback_url_suffix:
            raw_urls = [append_url_path(parent_endpoint_url, edge.fallback_url_suffix)]
        for raw_url in raw_urls:
            if not raw_url.startswith("http") or not is_allowed_host(raw_url, self._spec.url_allowlist_hosts):
                continue
            url = self._canonicalize_url(raw_url)
            if edge.url_normalizer == "congress_treaty_committees":
                url = normalize_congress_treaty_committees_url(url)
            url = self._augment_url(url, self._spec.nodes[edge.target], options)
            queue.append(
                QueueItem(
                    url=url,
                    node_type=edge.target,
                    follow_mode=edge.follow_mode,
                    parent_raw_id=parent_raw_id,
                    parent_endpoint_url=parent_endpoint_url,
                    pagination_key=url if edge.follow_mode == "recurse" else None,
                )
            )

    def _augment_url(self, url: str, node: NodeSpec, options: CrawlOptions) -> str:
        params: dict[str, str | int | None] = dict(node.default_query_params)
        page_size_overrides: dict[str, int] = {}
        if options.page_size is not None:
            for key in ("limit", "pageSize"):
                if key in params:
                    params[key] = options.page_size
                    page_size_overrides[key] = options.page_size
        if "fromDateTime" in node.incremental_query_params:
            params["fromDateTime"] = options.from_datetime
        if "toDateTime" in node.incremental_query_params:
            params["toDateTime"] = options.to_datetime
        # Defaults only fill in missing parameters, so a nextPage URL keeps its
        # offset/offsetMark; an explicit page size then replaces just the size.
        augmented = with_query_params(self._canonicalize_url(url), params)
        if page_size_overrides:
            return replace_query_params(augmented, page_size_overrides)
        return augmented

    def _fetch_record(
        self,
        run_id: str,
        job: QueueItem,
        outcome: str,
        *,
        stored: StoredNode | None = None,
        response_ref: StoredResponseRef | None = None,
        metadata: FetchMetadata | None = None,
    ) -> dict[str, Any]:
        record: dict[str, Any] = {
            "crawl_run_id": run_id,
            "endpoint_url": job.url,
            "raw_response_id": None,
            "raw_response_version_id": None,
            "parent_endpoint_url": job.parent_endpoint_url,
            "node_type": job.node_type,
            "follow_mode": job.follow_mode,
            "http_status": None,
            "outcome": outcome,
        }
        if stored is not None:
            record["raw_response_id"] = stored.raw_id
            record["raw_response_version_id"] = stored.raw_response_version_id
        if response_ref is not None:
            record["raw_response_id"] = response_ref.raw_response_id
            record["raw_response_version_id"] = response_ref.raw_response_version_id
        if metadata is not None:
            record["http_status"] = metadata.status_code
            record.update(_telemetry(metadata))
        return record

    def _failed_request_record(self, run_id: str, job: QueueItem, error: str, metadata: FetchMetadata) -> dict[str, Any]:
        return {
            "crawl_run_id": run_id,
            "category": self._spec.category,
            "node_type": job.node_type,
            "endpoint_url": job.url,
            "parent_endpoint_url": job.parent_endpoint_url,
            "http_status": metadata.status_code,
            "error": error,
            **_telemetry(metadata),
        }

    def _report_fetched(self, job: QueueItem, metadata: FetchMetadata, total_fetched: int) -> None:
        if self._reporter is not None:
            self._reporter.on_url_fetched(
                category=self._spec.category,
                node_type=job.node_type,
                endpoint_url=job.url,
                http_status=metadata.status_code,
                total_fetched=total_fetched,
                **_telemetry(metadata),
            )

    def _report_failed(self, job: QueueItem, error: str, metadata: FetchMetadata, total_failed: int) -> None:
        if self._reporter is not None:
            self._reporter.on_request_failed(
                category=self._spec.category,
                node_type=job.node_type,
                endpoint_url=job.url,
                http_status=metadata.status_code,
                error=error,
                total_failed=total_failed,
                **_telemetry(metadata),
            )

    def _report_finished(self, run_id: str, status: str, items_fetched: int) -> None:
        if self._reporter is not None:
            self._reporter.on_run_finished(
                category=self._spec.category, run_id=run_id, status=status, items_fetched=items_fetched
            )


class SpecCrawler(_CrawlerBase):
    def __init__(
        self,
        *,
        base_url: str,
        spec: CrawlSpec,
        client: Any,
        store: StoreProtocol,
        reporter: ReporterProtocol | None = None,
    ) -> None:
        super().__init__(base_url=base_url, spec=spec, client=client, store=store, reporter=reporter)

    def run(self, options: CrawlOptions) -> str:
        if options.mode in DESCENDANT_REFRESH_MODES:
            return self._run_refresh_descendants(options)
        from_datetime = options.from_datetime
        if options.mode in AUTO_HIGH_WATER_MARK_MODES and not from_datetime:
            from_datetime = _format_timestamp(self._store.get_latest_high_water_mark(self._spec.category))
        options = self._seed_options(options, from_datetime)

        run_id = self._store.start_run(self._spec.category, options.mode)
        high_water_mark = _parse_timestamp(options.from_datetime)
        items_fetched = 0
        try:
            items_fetched, high_water_mark = self._drain_queue(
                run_id=run_id, queue=self._seed_queue(options), options=options, high_water_mark=high_water_mark
            )
            self._store.finish_run(run_id, "completed", items_fetched, high_water_mark=high_water_mark)
            self._report_finished(run_id, "completed", items_fetched)
            return run_id
        except (asyncio.CancelledError, KeyboardInterrupt, Exception):
            self._store.finish_run(run_id, "failed", items_fetched, high_water_mark=high_water_mark)
            self._report_finished(run_id, "failed", items_fetched)
            raise

    def _run_refresh_descendants(self, options: CrawlOptions) -> str:
        node_type = self._refresh_root_node_type()
        run_id = self._store.start_run(self._spec.category, options.mode)
        try:
            roots = self._store.list_refresh_nodes(
                category=self._spec.category,
                node_type=node_type,
                limit=options.batch_size or _DEFAULT_REFRESH_BATCH_SIZE,
                after_endpoint_url=options.refresh_after_endpoint_url,
            )
            queue = self._queue_refresh_roots(roots, options)
            items_fetched, _ = self._drain_queue(run_id=run_id, queue=queue, options=options, high_water_mark=None)
            self._store.finish_run(run_id, "completed", items_fetched)
            self._report_finished(run_id, "completed", items_fetched)
            return run_id
        except Exception:
            self._store.finish_run(run_id, "failed", 0)
            self._report_finished(run_id, "failed", 0)
            raise

    def _get_json(self, job: QueueItem, request_kwargs: dict[str, Any]) -> FetchResult:
        try:
            return self._client.get_json(job.url, **request_kwargs)
        except Exception as exc:
            fallback_url = _source_fallback_url(exc, job)
            if fallback_url is None:
                raise
            return self._client.get_json(fallback_url, **request_kwargs)

    def _drain_queue(
        self,
        *,
        run_id: str,
        queue: deque[QueueItem],
        options: CrawlOptions,
        high_water_mark: datetime | None,
    ) -> tuple[int, datetime | None]:
        options = replace(options, seed_node_type=options.seed_node_type or self._spec.seed.node_type)
        seen_urls: set[str] = set()
        pages_fetched: dict[str, int] = {}
        progress = _SeedProgress()
        items_fetched = 0
        failed_requests = 0
        while queue:
            job = queue.popleft()
            if not is_allowed_host(job.url, self._spec.url_allowlist_hosts):
                continue
            job = replace(job, url=self._canonicalize_url(job.url))
            self._store.record_expected_fetch(
                crawl_run_id=run_id,
                endpoint_url=job.url,
                parent_endpoint_url=job.parent_endpoint_url,
                node_type=job.node_type,
                follow_mode=job.follow_mode,
            )
            if job.url in seen_urls:
                self._store.record_fetch(**self._fetch_record(run_id, job, "skipped_seen"))
                continue
            seen_urls.add(job.url)

            if options.mode in SKIP_EXISTING_MODES and self._store.has_response(job.url):
                self._store.record_fetch(**self._fetch_record(run_id, job, "skipped_existing"))
                continue

            prior_stored = self._store.get_stored_node(job.url) if options.mode in BACKFILL_MODES | {"incremental"} else None
            if options.mode in BACKFILL_MODES and prior_stored is not None:
                self._store.record_fetch(**self._fetch_record(run_id, job, "skipped_existing_backfill", stored=prior_stored))
                self._follow(queue, job, prior_stored.response, prior_stored.raw_id, options, pages_fetched)
                continue

            try:
                result = self._get_json(job, self._endpoint_request_kwargs(progress, len(queue)))
            except Exception as exc:
                metadata = _failure_metadata(exc)
                self._store.record_failed_request(**self._failed_request_record(run_id, job, str(exc), metadata))
                self._store.record_fetch(**self._fetch_record(run_id, job, "failed", metadata=metadata))
                failed_requests += 1
                self._report_failed(job, str(exc), metadata, failed_requests)
                continue

            high_water_mark = self._observe_payload(job, result.payload, options, progress, high_water_mark)
            response_ref = self._store.upsert_response(
                endpoint_url=job.url,
                source=self._spec.source,
                category=self._spec.category,
                node_type=job.node_type,
                parent_id=job.parent_raw_id,
                response=result.payload,
                latest_crawl_run_id=run_id,
            )
            self._store.record_fetch(
                **self._fetch_record(run_id, job, "fetched", response_ref=response_ref, metadata=result.metadata)
            )
            items_fetched += 1
            self._report_fetched(job, result.metadata, items_fetched)
            unchanged = prior_stored is not None and prior_stored.response == result.payload
            self._follow(queue, job, result.payload, response_ref.raw_response_id, options, pages_fetched, unchanged=unchanged)
        return items_fetched, high_water_mark


@dataclass(slots=True)
class AsyncFetchResult:
    job: QueueItem
    payload: dict[str, Any] | None
    fetch_metadata: FetchMetadata
    error: str | None = None


class AsyncSpecCrawler(_CrawlerBase):
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
        super().__init__(base_url=base_url, spec=spec, client=client, store=store, reporter=reporter)
        self._concurrency = max(1, concurrency)
        self._semaphore = asyncio.Semaphore(self._concurrency)

    async def run(self, options: CrawlOptions) -> str:
        if options.mode in DESCENDANT_REFRESH_MODES:
            return await self._run_refresh_descendants(options)
        from_datetime = options.from_datetime
        if options.mode in AUTO_HIGH_WATER_MARK_MODES and not from_datetime:
            from_datetime = _format_timestamp(await self._store.get_latest_high_water_mark(self._spec.category))
        options = self._seed_options(options, from_datetime)

        run_id = await self._store.start_run(self._spec.category, options.mode)
        high_water_mark = _parse_timestamp(options.from_datetime)
        items_fetched = 0
        try:
            items_fetched, high_water_mark = await self._drain_queue(
                run_id=run_id, queue=self._seed_queue(options), options=options, high_water_mark=high_water_mark
            )
            await self._store.finish_run(run_id, "completed", items_fetched, high_water_mark=high_water_mark)
            self._report_finished(run_id, "completed", items_fetched)
            return run_id
        except (asyncio.CancelledError, KeyboardInterrupt, Exception):
            await self._store.finish_run(run_id, "failed", items_fetched, high_water_mark=high_water_mark)
            self._report_finished(run_id, "failed", items_fetched)
            raise

    async def _run_refresh_descendants(self, options: CrawlOptions) -> str:
        node_type = self._refresh_root_node_type()
        run_id = await self._store.start_run(self._spec.category, options.mode)
        try:
            roots = await self._store.list_refresh_nodes(
                category=self._spec.category,
                node_type=node_type,
                limit=options.batch_size or _DEFAULT_REFRESH_BATCH_SIZE,
                after_endpoint_url=options.refresh_after_endpoint_url,
            )
            queue = self._queue_refresh_roots(roots, options)
            items_fetched, _ = await self._drain_queue(run_id=run_id, queue=queue, options=options, high_water_mark=None)
            await self._store.finish_run(run_id, "completed", items_fetched)
            self._report_finished(run_id, "completed", items_fetched)
            return run_id
        except Exception:
            await self._store.finish_run(run_id, "failed", 0)
            self._report_finished(run_id, "failed", 0)
            raise

    async def _drain_queue(
        self,
        *,
        run_id: str,
        queue: deque[QueueItem],
        options: CrawlOptions,
        high_water_mark: datetime | None,
    ) -> tuple[int, datetime | None]:
        options = replace(options, seed_node_type=options.seed_node_type or self._spec.seed.node_type)
        candidate_limit = self._concurrency
        if options.mode in BACKFILL_MODES:
            batch_size = min(_BACKFILL_STORED_LOOKUP_BATCH_SIZE, options.page_size or _BACKFILL_STORED_LOOKUP_BATCH_SIZE)
            candidate_limit = max(candidate_limit, batch_size)
        seen_urls: set[str] = set()
        pages_fetched: dict[str, int] = {}
        progress = _SeedProgress()
        items_fetched = 0
        failed_requests = 0
        stored_nodes: dict[str, StoredNode] = {}
        # URLs already checked against the store that still need fetching;
        # drained at network concurrency before more of the queue is examined.
        pending_network: deque[QueueItem] = deque()
        while queue or pending_network:
            if not pending_network:
                stored_nodes = await self._take_candidates(
                    run_id, queue, options, candidate_limit, seen_urls, pages_fetched, pending_network
                )
            batch = [pending_network.popleft() for _ in range(min(self._concurrency, len(pending_network)))]
            if not batch:
                continue

            request_kwargs = self._endpoint_request_kwargs(progress, len(queue) + len(batch))
            results = await asyncio.gather(*(self._fetch_job(job, request_kwargs) for job in batch))
            response_refs = await asyncio.gather(*(self._persist_fetch_result(run_id, result) for result in results))
            for result, response_ref in zip(results, response_refs, strict=True):
                job = result.job
                if result.payload is None or response_ref is None:
                    failed_requests += 1
                    self._report_failed(job, result.error or "Unknown fetch error", result.fetch_metadata, failed_requests)
                    continue
                high_water_mark = self._observe_payload(job, result.payload, options, progress, high_water_mark)
                items_fetched += 1
                self._report_fetched(job, result.fetch_metadata, items_fetched)
                prior_stored = stored_nodes.get(job.url)
                unchanged = options.mode == "incremental" and prior_stored is not None and prior_stored.response == result.payload
                self._follow(queue, job, result.payload, response_ref.raw_response_id, options, pages_fetched, unchanged=unchanged)
        return items_fetched, high_water_mark

    async def _take_candidates(
        self,
        run_id: str,
        queue: deque[QueueItem],
        options: CrawlOptions,
        candidate_limit: int,
        seen_urls: set[str],
        pages_fetched: dict[str, int],
        pending_network: deque[QueueItem],
    ) -> dict[str, StoredNode]:
        """Pop up to ``candidate_limit`` new URLs, settle the ones already stored, and queue the rest for fetching."""
        expected: list[QueueItem] = []
        skipped: list[dict[str, Any]] = []
        candidates: list[QueueItem] = []
        while queue and len(candidates) < candidate_limit:
            job = queue.popleft()
            if not is_allowed_host(job.url, self._spec.url_allowlist_hosts):
                continue
            job = replace(job, url=self._canonicalize_url(job.url))
            expected.append(job)
            if job.url in seen_urls:
                skipped.append(self._fetch_record(run_id, job, "skipped_seen"))
                continue
            seen_urls.add(job.url)
            candidates.append(job)

        await self._store.record_expected_fetch_batch([
            {
                "crawl_run_id": run_id,
                "endpoint_url": job.url,
                "parent_endpoint_url": job.parent_endpoint_url,
                "node_type": job.node_type,
                "follow_mode": job.follow_mode,
            }
            for job in expected
        ])

        stored_nodes: dict[str, StoredNode] = {}
        if options.mode in STORED_LOOKUP_MODES and candidates:
            stored_nodes = await self._store.get_stored_nodes([job.url for job in candidates])

        for job in candidates:
            stored = stored_nodes.get(job.url)
            if stored is not None and options.mode in SKIP_EXISTING_MODES:
                skipped.append(self._fetch_record(run_id, job, "skipped_existing"))
            elif stored is not None and options.mode in BACKFILL_MODES:
                skipped.append(self._fetch_record(run_id, job, "skipped_existing_backfill", stored=stored))
                self._follow(queue, job, stored.response, stored.raw_id, options, pages_fetched)
            else:
                pending_network.append(job)

        if skipped:
            await self._store.record_fetch_batch(skipped)
        return stored_nodes

    async def _persist_fetch_result(self, run_id: str, result: AsyncFetchResult) -> StoredResponseRef | None:
        if result.payload is None:
            error = result.error or "Unknown fetch error"
            await asyncio.gather(
                self._store.record_failed_request(**self._failed_request_record(run_id, result.job, error, result.fetch_metadata)),
                self._store.record_fetch(**self._fetch_record(run_id, result.job, "failed", metadata=result.fetch_metadata)),
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
        await self._store.record_fetch(
            **self._fetch_record(run_id, result.job, "fetched", response_ref=response_ref, metadata=result.fetch_metadata)
        )
        return response_ref

    async def _fetch_job(self, job: QueueItem, request_kwargs: dict[str, Any]) -> AsyncFetchResult:
        async with self._semaphore:
            try:
                try:
                    result = await self._client.get_json(job.url, **request_kwargs)
                except Exception as exc:
                    fallback_url = _source_fallback_url(exc, job)
                    if fallback_url is None:
                        raise
                    result = await self._client.get_json(fallback_url, **request_kwargs)
            except Exception as exc:
                return AsyncFetchResult(job=job, payload=None, fetch_metadata=_failure_metadata(exc), error=str(exc))
            return AsyncFetchResult(job=job, payload=result.payload, fetch_metadata=result.metadata)
