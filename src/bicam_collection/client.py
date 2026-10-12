from __future__ import annotations

import asyncio
import os
import contextlib
from dataclasses import dataclass, replace
import logging
from pathlib import Path
import threading
import time
from typing import Any
from urllib.parse import urlparse

import requests
from rotisserie import KeyConfig, KeyPool, WorkWeightedPolicy
from rotisserie.state import KeyState

from bicam_collection.config import parse_dotenv
from bicam_collection.urls import with_query_params


LOGGER = logging.getLogger(__name__)
_WAIT_LOG_INTERVAL_SECONDS = 5.0

# Congress.gov advertises a 20,000-request hourly window per token.
_CONGRESS_RATE_LIMIT = 20_000
_CONGRESS_RATE_WINDOW_SECONDS = 3_600
_CONGRESS_RATE_SAFETY = 0.95

# Plain HTTP 500 is excluded: Congress.gov returns a known set of
# deterministic 500s that belong in the failure queue, not in a retry loop.
# Only 500s whose body names a service outage are retried.
_TRANSIENT_RETRY_STATUS_CODES = frozenset({408, 425, 429, 502, 503, 504, 520, 522, 523, 524})
_TRANSIENT_500_MARKERS = ("misconf", "valkey", "temporarily unavailable", "upstream connect error")
# Tags appended to terminal 500 errors so the failure queue can tell source
# data defects apart from outages.
_SOURCE_ERROR_MARKERS = {
    "membertermdoesnotexist": "member_term_does_not_exist",
    "noreversematch": "no_reverse_match",
}
_TRANSIENT_RETRY_BASE_SECONDS = 0.5
_TRANSIENT_RETRY_MAX_SECONDS = 30.0
# A Retry-After this long means the whole account window is exhausted;
# retrying on another key would just spread the 429 across every key, so the
# URL is left for the scheduled retry pass.
_LONG_RATE_LIMIT_RETRY_AFTER_SECONDS = 60.0

_SHARED_POOLS: dict[tuple[str | None, int, str, str, str], KeyPool] = {}
_SHARED_POOLS_LOCK = threading.Lock()


@dataclass(slots=True)
class FetchMetadata:
    status_code: int | None = None
    api_key_name: str | None = None
    attempt_count: int | None = None
    rate_limit_attempts: int | None = None
    duration_ms: int | None = None
    pool_allocation: dict[str, Any] | None = None


@dataclass(slots=True)
class FetchResult:
    payload: dict[str, Any]
    metadata: FetchMetadata


_FETCH_METADATA_COLUMNS = ("status_code", "duration_ms", "attempt_count", "rate_limit_attempts", "api_key_name", "pool_allocation")


def fetch_metadata_values(metadata: FetchMetadata | None) -> dict[str, Any]:
    """Map a fetch outcome onto the ``record_fetch`` / ``record_failed_request`` keyword arguments."""
    if metadata is None:
        return {column: None for column in _FETCH_METADATA_COLUMNS}
    return {column: getattr(metadata, column) for column in _FETCH_METADATA_COLUMNS}


def _setting_value(env_path: Path | None, name: str) -> str | None:
    value = os.environ.get(name)
    if value is not None and value.strip():
        return value.strip()
    return parse_dotenv(env_path).get(name)


def _load_key_configs(env_path: Path | None, *, explicit_names: tuple[str, ...], indexed_prefix: str) -> list[KeyConfig]:
    env_values = {**parse_dotenv(env_path), **os.environ}
    key_configs: list[KeyConfig] = []
    for name in explicit_names:
        raw_value = env_values.get(name, "")
        if "," in raw_value:
            tokens = [token.strip() for token in raw_value.split(",") if token.strip()]
            for index, token in enumerate(tokens, start=1):
                key_configs.append(KeyConfig(name=f"{name.lower()}_{index}", token=token))
        elif raw_value:
            key_configs.append(KeyConfig(name=name.lower(), token=raw_value))
    for name, value in sorted(env_values.items()):
        if name.startswith(indexed_prefix) and value.strip():
            key_configs.append(KeyConfig(name=name[len(indexed_prefix) :].lower(), token=value.strip()))
    return key_configs


def _select_key_shard(key_configs: list[KeyConfig], *, pool_namespace: str) -> list[KeyConfig]:
    """Keep this process's round-robin share of the keys, per ``BICAM_<NAMESPACE>_KEY_SHARD=slot/total``.

    Each process paces its own keys, so parallel processes only add
    throughput when they own disjoint key sets.
    """
    variable = f"BICAM_{pool_namespace.upper()}_KEY_SHARD"
    raw_shard = os.environ.get(variable, "").strip()
    if not raw_shard:
        return key_configs
    try:
        raw_slot, raw_total = raw_shard.split("/", 1)
        slot, total = int(raw_slot), int(raw_total)
    except ValueError as exc:
        raise ValueError(f"{variable} must use SLOT/TOTAL syntax, got {raw_shard!r}") from exc
    if total < 1 or not 0 <= slot < total:
        raise ValueError(f"{variable} must satisfy 0 <= SLOT < TOTAL, got {raw_shard!r}")
    selected = [key for index, key in enumerate(key_configs) if index % total == slot]
    if not selected:
        raise ValueError(f"{variable} selected no configured keys (configured={len(key_configs)}, shard={raw_shard!r})")
    return selected


def _congress_key_slots(env_path: Path | None) -> int:
    raw_slots = _setting_value(env_path, "BICAM_CONGRESS_KEY_SLOTS")
    return min(4, max(1, int(raw_slots))) if raw_slots else 1


def _congress_per_key_window(env_path: Path | None, key_count: int, *, key_slot_count: int) -> tuple[int, int]:
    """Return the request budget for each key handle in the pool.

    ``BICAM_CONGRESS_PER_KEY_RATE_LIMIT`` is a per-credential budget, split
    only across the virtual slots of that credential, so duplicating a token
    never creates quota. Without it, the older aggregate
    ``BICAM_CONGRESS_GLOBAL_RATE_LIMIT`` is split across every handle.
    """
    raw_per_key_limit = _setting_value(env_path, "BICAM_CONGRESS_PER_KEY_RATE_LIMIT")
    raw_global_limit = _setting_value(env_path, "BICAM_CONGRESS_GLOBAL_RATE_LIMIT")
    raw_window = _setting_value(env_path, "BICAM_CONGRESS_RATE_WINDOW_SECONDS")
    raw_safety = _setting_value(env_path, "BICAM_CONGRESS_RATE_SAFETY")
    window_seconds = max(1, int(raw_window)) if raw_window else _CONGRESS_RATE_WINDOW_SECONDS
    safety = min(1.0, max(0.1, float(raw_safety) if raw_safety else _CONGRESS_RATE_SAFETY))
    if raw_per_key_limit:
        budget, handles = max(1, int(raw_per_key_limit)), key_slot_count
    else:
        budget = max(1, int(raw_global_limit)) if raw_global_limit else _CONGRESS_RATE_LIMIT
        handles = key_count
    return max(1, int(budget * safety) // max(1, handles)), window_seconds


def _is_transient_response(response: requests.Response) -> bool:
    if response.status_code in _TRANSIENT_RETRY_STATUS_CODES:
        return True
    if response.status_code != 500:
        return False
    body = response.text.lower()
    return any(marker in body for marker in _TRANSIENT_500_MARKERS)


def _annotate_source_error(error: requests.HTTPError, response: requests.Response) -> None:
    if response.status_code != 500:
        return
    try:
        payload = response.json()
    except ValueError:
        return
    if not isinstance(payload, dict) or not isinstance(payload.get("error"), str):
        return
    compact_error = "".join(payload["error"].lower().split())
    for marker, code in _SOURCE_ERROR_MARKERS.items():
        if marker in compact_error:
            error.args = (f"{error} source_error={code}",)
            return


def _transient_retry_delay(response: requests.Response | None, attempt_count: int) -> float:
    """Honor a numeric Retry-After, else back off exponentially; capped so one URL cannot stall a crawl."""
    retry_after = response.headers.get("Retry-After") if response is not None else None
    if retry_after:
        with contextlib.suppress(ValueError):
            return min(_TRANSIENT_RETRY_MAX_SECONDS, max(0.0, float(retry_after)))
    return min(_TRANSIENT_RETRY_MAX_SECONDS, _TRANSIENT_RETRY_BASE_SECONDS * 2 ** max(0, attempt_count - 1))


def _is_long_rate_limit(response: requests.Response) -> bool:
    if response.status_code != 429:
        return False
    try:
        return float(response.headers.get("Retry-After", "")) >= _LONG_RATE_LIMIT_RETRY_AFTER_SECONDS
    except ValueError:
        return False


def clear_shared_key_pools() -> None:
    with _SHARED_POOLS_LOCK:
        _SHARED_POOLS.clear()


def _build_key_pool(key_configs: list[KeyConfig], *, retries: int) -> KeyPool:
    return KeyPool(
        key_configs,
        auth_header="x-api-key",
        auth_scheme="",
        retry_attempts=retries,
        policy=WorkWeightedPolicy(),
        # Spread spare keys across active endpoints so one category cannot starve the rest.
        distribute=True,
    )


def _shared_pool(env_path: Path | None, retries: int, pool_namespace: str, key_configs: list[KeyConfig]) -> KeyPool:
    namespace = pool_namespace.upper()
    cache_key = (
        str(env_path.resolve()) if env_path is not None else None,
        retries,
        pool_namespace,
        os.environ.get(f"BICAM_{namespace}_KEY_SHARD", "").strip(),
        _setting_value(env_path, f"BICAM_{namespace}_KEY_SLOTS") or "",
    )
    with _SHARED_POOLS_LOCK:
        if cache_key not in _SHARED_POOLS:
            _SHARED_POOLS[cache_key] = _build_key_pool(key_configs, retries=retries)
        return _SHARED_POOLS[cache_key]


def _is_available(key: KeyState, now: float) -> bool:
    return key.in_use_by is None and key.next_available_at(now) <= now and (key.remaining is None or key.remaining > 0)


def _pool_allocation_snapshot(
    pool: KeyPool, *, endpoint: str, endpoint_priority: int, endpoint_reserve: int, selected_key_name: str | None
) -> dict[str, Any]:
    now = time.time()
    with pool._lock:
        active_endpoints = []
        for endpoint_name, info in sorted(pool._endpoints.items(), key=lambda item: (item[1].priority, item[0])):
            assigned_keys = [key.name for key in pool._keys if key.assigned_to == endpoint_name]
            in_use_keys = [key.name for key in pool._keys if key.in_use_by == endpoint_name]
            active_endpoints.append({
                "name": endpoint_name,
                "priority": info.priority,
                "reserve": info.reserve,
                "assigned_key_names": assigned_keys,
                "assigned_key_count": len(assigned_keys),
                "in_use_key_names": in_use_keys,
                "in_use_key_count": len(in_use_keys),
            })
        available_keys = [key.name for key in pool._keys if _is_available(key, now)]
        cooling_keys = [key.name for key in pool._keys if key.next_available_at(now) > now]
        return {
            "endpoint_name": endpoint,
            "endpoint_priority": endpoint_priority,
            "endpoint_reserve": endpoint_reserve,
            "selected_key_name": selected_key_name,
            "total_key_count": len(pool._keys),
            "available_key_names": available_keys,
            "available_key_count": len(available_keys),
            "cooling_key_names": cooling_keys,
            "cooling_key_count": len(cooling_keys),
            "active_endpoints": active_endpoints,
        }


def _take_key_with_wait(
    pool: KeyPool,
    endpoint: str,
    *,
    timeout_seconds: float | None,
    active_key_budget: int | None = None,
    endpoint_active_cap: int | None = None,
) -> KeyState:
    """Lease a key, waiting for one to free up under the active-key budget and per-endpoint cap.

    Reimplements ``KeyPool.take_key`` on the pool's internals because the
    stock version neither waits nor enforces these caps.
    """
    deadline = None if timeout_seconds is None else time.monotonic() + max(0.01, timeout_seconds)
    next_notice_at = 0.0
    while True:
        constrained_reason: str | None = None
        endpoint_cap = endpoint_active_cap
        with pool._lock:
            active_endpoints = sorted(pool._endpoints)
            in_use_total = sum(1 for key in pool._keys if key.in_use_by is not None)
            endpoint_in_use = sum(1 for key in pool._keys if key.in_use_by == endpoint)
            if endpoint_cap is None and active_key_budget is not None and len(active_endpoints) > 1:
                endpoint_cap = max(1, -(-active_key_budget // len(active_endpoints)))
            if active_key_budget is not None and in_use_total >= active_key_budget:
                candidate = None
                constrained_reason = "active_budget"
            elif endpoint_cap is not None and endpoint_in_use >= endpoint_cap:
                candidate = None
                constrained_reason = "endpoint_cap"
            else:
                candidate = pool._pick_candidate(endpoint)
            if candidate is not None:
                if candidate.remaining is not None:
                    candidate.remaining -= 1
                candidate.in_use_by = endpoint
                return candidate
            now = pool._now()
            # Wake when the next idle key leaves cooldown (exhausted keys
            # included, or waiters spin for a whole quota window). Leased keys
            # report "available now" while still owned, so when every key is
            # leased, poll briefly for a release instead.
            idle_wake_times = [key.next_available_at(now) for key in pool._keys if key.in_use_by is None]
            wake = min(idle_wake_times) if idle_wake_times else now + 0.05
            available_count = sum(1 for key in pool._keys if _is_available(key, now))
            cooling_count = sum(1 for key in pool._keys if key.next_available_at(now) > now)

        remaining = None if deadline is None else deadline - time.monotonic()
        if remaining is not None and remaining <= 0:
            raise TimeoutError(f"Timed out waiting for an available API key for endpoint '{endpoint}'")
        delay = max(0.01, wake - time.time()) if constrained_reason is None else 0.05
        if remaining is not None:
            delay = min(remaining, delay)
        if time.monotonic() >= next_notice_at:
            active_summary = ",".join(active_endpoints[:5]) or "-"
            if len(active_endpoints) > 5:
                active_summary += f",+{len(active_endpoints) - 5}"
            if constrained_reason is None:
                LOGGER.warning(
                    "All API keys unavailable in shared pool; waiting %.2fs for endpoint '%s' (active_endpoints=%s, available_keys=%d, cooling_keys=%d, in_use_keys=%d, endpoint_in_use=%d)",
                    delay, endpoint, active_summary, available_count, cooling_count, in_use_total, endpoint_in_use,
                )
            else:
                LOGGER.info(
                    "Key acquisition throttled by %s; waiting %.2fs for endpoint '%s' (active_endpoints=%s, in_use_keys=%d, endpoint_in_use=%d, endpoint_cap=%s, active_budget=%s)",
                    constrained_reason, delay, endpoint, active_summary, in_use_total, endpoint_in_use, endpoint_cap, active_key_budget,
                )
            next_notice_at = time.monotonic() + _WAIT_LOG_INTERVAL_SECONDS
        time.sleep(delay)


class _RotatingApiClient:
    explicit_key_env_names: tuple[str, ...]
    indexed_key_prefix: str
    pool_namespace: str
    default_retries: int
    auth_header_name: str | None = None
    auth_query_param_name: str | None = None

    def __init__(
        self,
        env_path: Path | None = None,
        timeout: float = 30.0,
        retries: int | None = None,
        *,
        use_shared_pool: bool = True,
        key_wait_timeout: float | None = None,
        active_key_budget: int | None = None,
        endpoint_active_cap: int | None = None,
        request_interval: float = 0.0,
    ) -> None:
        retries = self.default_retries if retries is None else retries
        self._timeout = timeout
        self._key_wait_timeout = key_wait_timeout
        self._request_interval = max(0.0, request_interval)
        self._request_pacer_lock = threading.Lock()
        self._next_request_at = 0.0
        self._session = requests.Session()

        key_configs = _load_key_configs(
            env_path, explicit_names=self.explicit_key_env_names, indexed_prefix=self.indexed_key_prefix
        )
        if self.pool_namespace == "congress":
            # Slots duplicate each credential so it can serve concurrent
            # requests; the budget is computed before sharding so every
            # shard applies the same per-token limit.
            key_slot_count = _congress_key_slots(env_path)
            if key_slot_count > 1:
                key_configs = [
                    replace(key, name=f"{key.name}__slot{slot}")
                    for key in key_configs
                    for slot in range(1, key_slot_count + 1)
                ]
            per_key_window = _congress_per_key_window(env_path, len(key_configs), key_slot_count=key_slot_count)
            key_configs = [replace(key, per_window=per_key_window) for key in key_configs]
        key_configs = _select_key_shard(key_configs, pool_namespace=self.pool_namespace)
        if not key_configs:
            names = ", ".join([*self.explicit_key_env_names, f"{self.indexed_key_prefix}*"])
            raise ValueError(f"No {self.pool_namespace} API keys found in {names}")

        if use_shared_pool:
            self._pool = _shared_pool(env_path, retries, self.pool_namespace, key_configs)
        else:
            self._pool = _build_key_pool(key_configs, retries=retries)
        if active_key_budget is not None:
            self._active_key_budget: int | None = max(1, active_key_budget)
        elif len(key_configs) >= 8:
            # Hold two keys back so the whole set is never exhausted in lockstep.
            self._active_key_budget = len(key_configs) - 2
        else:
            self._active_key_budget = None
        self._endpoint_active_cap = None if endpoint_active_cap is None else max(1, endpoint_active_cap)

    def get_json(
        self,
        url: str,
        endpoint_name: str | None = None,
        *,
        endpoint_priority: int = 1,
        endpoint_reserve: int = 1,
    ) -> FetchResult:
        endpoint = endpoint_name or urlparse(url).path
        endpoint_priority = max(1, endpoint_priority)
        endpoint_reserve = max(1, endpoint_reserve)
        started_at = time.perf_counter()
        attempt_count = 0
        rate_limit_attempts = 0
        retry_get = "GET" in self._pool._retry_methods
        max_attempts = max(1, self._pool._max_attempts)

        def metadata(status_code: int | None, key_name: str | None, pool_allocation: dict[str, Any] | None) -> FetchMetadata:
            return FetchMetadata(
                status_code=status_code,
                api_key_name=key_name,
                attempt_count=attempt_count,
                rate_limit_attempts=rate_limit_attempts,
                duration_ms=max(1, round((time.perf_counter() - started_at) * 1000)),
                pool_allocation=pool_allocation,
            )

        def snapshot(selected_key_name: str | None) -> dict[str, Any]:
            return _pool_allocation_snapshot(
                self._pool,
                endpoint=endpoint,
                endpoint_priority=endpoint_priority,
                endpoint_reserve=endpoint_reserve,
                selected_key_name=selected_key_name,
            )

        with self._pool.endpoint(endpoint, reserve=endpoint_reserve, priority=endpoint_priority):
            while attempt_count < max_attempts:
                self._pace_request()
                try:
                    key = _take_key_with_wait(
                        self._pool,
                        endpoint,
                        timeout_seconds=self._key_wait_timeout,
                        active_key_budget=self._active_key_budget,
                        endpoint_active_cap=self._endpoint_active_cap,
                    )
                except TimeoutError as exc:
                    timeout_error = requests.Timeout(str(exc))
                    timeout_error.fetch_metadata = metadata(None, None, snapshot(None))  # type: ignore[attr-defined]
                    raise timeout_error from exc
                attempt_count += 1
                pool_allocation = snapshot(key.name)
                request_url, headers = self._prepare_request(url, key.token)
                try:
                    response = self._session.get(request_url, headers=headers, timeout=self._timeout)
                except requests.RequestException as exc:
                    self._pool.mark_result(key, None, {}, exc)
                    exc.fetch_metadata = metadata(None, key.name, pool_allocation)  # type: ignore[attr-defined]
                    if attempt_count < max_attempts and retry_get:
                        time.sleep(_transient_retry_delay(None, attempt_count))
                        continue
                    raise

                self._pool.mark_result(key, response.status_code, dict(response.headers), None)
                if response.status_code == 429:
                    rate_limit_attempts += 1
                    # The server's quota headers tell a per-key limit apart from an account-wide one.
                    LOGGER.warning(
                        "API rate limit response endpoint=%s key=%s limit=%s remaining=%s retry_after=%s",
                        endpoint,
                        key.name,
                        response.headers.get("X-RateLimit-Limit"),
                        response.headers.get("X-RateLimit-Remaining"),
                        response.headers.get("Retry-After"),
                    )
                fetch_metadata = metadata(response.status_code, key.name, pool_allocation)
                if (
                    _is_transient_response(response)
                    and attempt_count < max_attempts
                    and retry_get
                    and not _is_long_rate_limit(response)
                ):
                    retry_delay = _transient_retry_delay(response, attempt_count)
                    response.close()
                    time.sleep(retry_delay)
                    continue
                try:
                    response.raise_for_status()
                except requests.HTTPError as exc:
                    _annotate_source_error(exc, response)
                    exc.fetch_metadata = fetch_metadata  # type: ignore[attr-defined]
                    raise
                return FetchResult(payload=response.json(), metadata=fetch_metadata)

        raise AssertionError("the retry loop only exits by returning or raising")

    def _pace_request(self) -> None:
        if self._request_interval <= 0:
            return
        with self._request_pacer_lock:
            now = time.monotonic()
            wait_for = max(0.0, self._next_request_at - now)
            self._next_request_at = max(now, self._next_request_at) + self._request_interval
        if wait_for > 0:
            time.sleep(wait_for)

    def _prepare_request(self, url: str, token: str) -> tuple[str, dict[str, str]]:
        if self.auth_header_name is not None:
            return url, {self.auth_header_name: token}
        assert self.auth_query_param_name is not None
        return with_query_params(url, {self.auth_query_param_name: token}), {}


class CongressClient(_RotatingApiClient):
    explicit_key_env_names = ("CONGRESS_API_KEYS", "CONGRESSIONAL_API_KEY")
    indexed_key_prefix = "CONGRESS_API_KEY_"
    pool_namespace = "congress"
    auth_header_name = "x-api-key"
    # One alternate key at most: a longer chain fans a 429 across the whole
    # pool and can trigger an account-wide cooldown.
    default_retries = 2


class GovInfoClient(_RotatingApiClient):
    explicit_key_env_names = ("GOVINFO_API_KEYS", "GOVINFO_API_KEY")
    indexed_key_prefix = "GOVINFO_API_KEY_"
    pool_namespace = "govinfo"
    auth_query_param_name = "api_key"
    default_retries = 3


class _AsyncClient:
    """Runs a synchronous client's blocking requests in worker threads."""

    _sync_client: _RotatingApiClient

    async def get_json(
        self,
        url: str,
        endpoint_name: str | None = None,
        *,
        endpoint_priority: int = 1,
        endpoint_reserve: int = 1,
    ) -> FetchResult:
        return await asyncio.to_thread(
            self._sync_client.get_json,
            url,
            endpoint_name,
            endpoint_priority=endpoint_priority,
            endpoint_reserve=endpoint_reserve,
        )


class AsyncCongressClient(_AsyncClient):
    def __init__(self, env_path: Path | None = None, **options: Any) -> None:
        self._sync_client = CongressClient(env_path, **options)


class AsyncGovInfoClient(_AsyncClient):
    def __init__(self, env_path: Path | None = None, **options: Any) -> None:
        self._sync_client = GovInfoClient(env_path, **options)
