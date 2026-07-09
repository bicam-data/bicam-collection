from __future__ import annotations

import asyncio
import os
import contextlib
from dataclasses import dataclass
import logging
from pathlib import Path
import threading
import time
from typing import Any
from urllib.parse import urlparse

import requests
from rotisserie import KeyConfig, KeyPool, WorkWeightedPolicy

from bicam_collection.config import parse_dotenv
from bicam_collection.urls import with_query_params


def _load_key_configs(
    env_path: Path | None,
    *,
    explicit_names: list[str],
    indexed_prefix: str,
) -> list[KeyConfig]:
    file_values = parse_dotenv(env_path)
    env_values = {**file_values, **os.environ}
    key_configs: list[KeyConfig] = []

    for name in explicit_names:
        raw_value = env_values.get(name, "")
        if not raw_value:
            continue
        if "," in raw_value:
            parts = [part.strip() for part in raw_value.split(",") if part.strip()]
            key_configs.extend(
                KeyConfig(name=f"{name.lower()}_{idx + 1}", token=part)
                for idx, part in enumerate(parts)
            )
        else:
            key_configs.append(KeyConfig(name=name.lower(), token=raw_value))

    for name, value in sorted(env_values.items()):
        if not name.startswith(indexed_prefix) or not value.strip():
            continue
        key_configs.append(KeyConfig(name=name[len(indexed_prefix) :].lower(), token=value.strip()))

    return key_configs


_SHARED_POOLS: dict[tuple[str | None, int, str], KeyPool] = {}
_SHARED_POOLS_LOCK = threading.Lock()
_WAIT_LOG_INTERVAL_SECONDS = 5.0
LOGGER = logging.getLogger(__name__)


def clear_shared_key_pools() -> None:
    with _SHARED_POOLS_LOCK:
        _SHARED_POOLS.clear()


def _shared_pool_key(env_path: Path | None, retries: int, pool_namespace: str) -> tuple[str | None, int, str]:
    return (str(env_path.resolve()) if env_path is not None else None, retries, pool_namespace)


def _build_key_pool(key_configs: list[KeyConfig], *, retries: int) -> KeyPool:
    return KeyPool(
        key_configs,
        auth_header="x-api-key",
        auth_scheme="",
        retry_attempts=retries,
        policy=WorkWeightedPolicy(),
        # Spread surplus keys across active endpoints instead of funneling them
        # into a single endpoint. This reduces cross-category starvation.
        distribute=True,
    )


def _get_or_create_shared_pool(
    *,
    env_path: Path | None,
    retries: int,
    key_configs: list[KeyConfig],
    pool_namespace: str,
) -> KeyPool:
    cache_key = _shared_pool_key(env_path, retries, pool_namespace)
    with _SHARED_POOLS_LOCK:
        pool = _SHARED_POOLS.get(cache_key)
        if pool is None:
            pool = _build_key_pool(key_configs, retries=retries)
            _SHARED_POOLS[cache_key] = pool
        return pool


@dataclass(slots=True)
class FetchMetadata:
    status_code: int | None
    api_key_name: str | None
    attempt_count: int
    rate_limit_attempts: int
    duration_ms: int
    pool_allocation: dict[str, Any] | None = None


@dataclass(slots=True)
class FetchResult:
    payload: dict[str, Any]
    metadata: FetchMetadata


def _build_pool_allocation_snapshot(
    pool: KeyPool,
    *,
    endpoint: str,
    endpoint_priority: int,
    endpoint_reserve: int,
    selected_key_name: str | None,
) -> dict[str, Any] | None:
    lock = getattr(pool, "_lock", None)
    keys = getattr(pool, "_keys", None)
    endpoints = getattr(pool, "_endpoints", None)
    if lock is None or keys is None or endpoints is None:
        return None

    now = time.time()
    with lock:
        active_endpoints = []
        for endpoint_name, info in sorted(endpoints.items(), key=lambda item: (item[1].priority, item[0])):
            assigned_keys = [key.name for key in keys if key.assigned_to == endpoint_name]
            in_use_keys = [key.name for key in keys if key.in_use_by == endpoint_name]
            active_endpoints.append(
                {
                    "name": endpoint_name,
                    "priority": info.priority,
                    "reserve": info.reserve,
                    "assigned_key_names": assigned_keys,
                    "assigned_key_count": len(assigned_keys),
                    "in_use_key_names": in_use_keys,
                    "in_use_key_count": len(in_use_keys),
                }
            )

        available_keys = [
            key.name
            for key in keys
            if key.in_use_by is None
            and key.next_available_at(now) <= now
            and (key.remaining is None or key.remaining > 0)
        ]
        cooling_keys = [key.name for key in keys if key.next_available_at(now) > now]
        return {
            "endpoint_name": endpoint,
            "endpoint_priority": endpoint_priority,
            "endpoint_reserve": endpoint_reserve,
            "selected_key_name": selected_key_name,
            "total_key_count": len(keys),
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
) -> Any:
    deadline = None if timeout_seconds is None else time.monotonic() + max(0.01, timeout_seconds)
    pick_candidate = getattr(pool, "_pick_candidate", None)
    lock = getattr(pool, "_lock", None)
    now_func = getattr(pool, "_now", time.time)
    keys = getattr(pool, "_keys", None)
    endpoints = getattr(pool, "_endpoints", None)
    next_notice_at = 0.0

    if pick_candidate is None or lock is None or keys is None:
        return pool.take_key(endpoint)

    while True:
        constrained_reason: str | None = None
        effective_endpoint_cap: int | None = endpoint_active_cap
        with lock:
            active_endpoints = sorted(endpoints.keys()) if isinstance(endpoints, dict) else []
            in_use_total = sum(1 for key_state in keys if key_state.in_use_by is not None)
            endpoint_in_use = sum(1 for key_state in keys if key_state.in_use_by == endpoint)
            if effective_endpoint_cap is None and active_key_budget is not None and len(active_endpoints) > 1:
                effective_endpoint_cap = max(
                    1,
                    (active_key_budget + len(active_endpoints) - 1) // len(active_endpoints),
                )
            if active_key_budget is not None and in_use_total >= active_key_budget:
                key = None
                constrained_reason = "active_budget"
            elif effective_endpoint_cap is not None and endpoint_in_use >= effective_endpoint_cap:
                key = None
                constrained_reason = "endpoint_cap"
            else:
                key = pick_candidate(endpoint)
            if key is not None:
                if key.remaining is not None:
                    key.remaining -= 1
                key.in_use_by = endpoint
                return key
            now = now_func()
            wake = min(key_state.next_available_at(now) for key_state in keys)
            available_count = sum(
                1
                for key_state in keys
                if key_state.in_use_by is None
                and key_state.next_available_at(now) <= now
                and (key_state.remaining is None or key_state.remaining > 0)
            )
            cooling_count = sum(1 for key_state in keys if key_state.next_available_at(now) > now)

        remaining = None if deadline is None else deadline - time.monotonic()
        if remaining is not None and remaining <= 0:
            raise TimeoutError(f"Timed out waiting for an available API key for endpoint '{endpoint}'")
        if constrained_reason is None:
            delay = max(0.01, wake - time.time())
        else:
            delay = 0.05
        if remaining is not None:
            delay = min(remaining, delay)
        if time.monotonic() >= next_notice_at:
            active_summary = ",".join(active_endpoints[:5]) if active_endpoints else "-"
            if len(active_endpoints) > 5:
                active_summary = f"{active_summary},+{len(active_endpoints) - 5}"
            if constrained_reason is None:
                LOGGER.warning(
                    "All API keys unavailable in shared pool; waiting %.2fs for endpoint '%s' (active_endpoints=%s, available_keys=%d, cooling_keys=%d, in_use_keys=%d, endpoint_in_use=%d)",
                    delay,
                    endpoint,
                    active_summary,
                    available_count,
                    cooling_count,
                    in_use_total,
                    endpoint_in_use,
                )
            else:
                LOGGER.info(
                    "Key acquisition throttled by %s; waiting %.2fs for endpoint '%s' (active_endpoints=%s, in_use_keys=%d, endpoint_in_use=%d, endpoint_cap=%s, active_budget=%s)",
                    constrained_reason,
                    delay,
                    endpoint,
                    active_summary,
                    in_use_total,
                    endpoint_in_use,
                    str(effective_endpoint_cap),
                    str(active_key_budget),
                )
            next_notice_at = time.monotonic() + _WAIT_LOG_INTERVAL_SECONDS
        time.sleep(delay)


class _RotatingApiClient:
    def __init__(
        self,
        env_path: Path | None = None,
        timeout: float = 30.0,
        retries: int = 3,
        *,
        use_shared_pool: bool = True,
        key_wait_timeout: float | None = None,
        active_key_budget: int | None = None,
        endpoint_active_cap: int | None = None,
        explicit_key_env_names: list[str],
        indexed_key_prefix: str,
        missing_keys_message: str,
        pool_namespace: str,
        auth_header_name: str | None = None,
        auth_query_param_name: str | None = None,
    ) -> None:
        self._timeout = timeout
        self._retries = retries
        self._key_wait_timeout = key_wait_timeout
        self._session = requests.Session()
        self._auth_header_name = auth_header_name
        self._auth_query_param_name = auth_query_param_name
        if bool(self._auth_header_name) == bool(self._auth_query_param_name):
            raise ValueError("Exactly one auth mechanism must be configured")
        key_configs = _load_key_configs(
            env_path,
            explicit_names=explicit_key_env_names,
            indexed_prefix=indexed_key_prefix,
        )
        if not key_configs:
            raise ValueError(missing_keys_message)
        if use_shared_pool:
            self._pool = _get_or_create_shared_pool(
                env_path=env_path,
                retries=retries,
                key_configs=key_configs,
                pool_namespace=pool_namespace,
            )
        else:
            self._pool = _build_key_pool(key_configs, retries=retries)
        if active_key_budget is not None:
            self._active_key_budget = max(1, int(active_key_budget))
        elif len(key_configs) >= 8:
            # Keep a small reserve to avoid synchronized exhaustion of all keys.
            self._active_key_budget = len(key_configs) - 2
        else:
            self._active_key_budget = None
        self._endpoint_active_cap = None if endpoint_active_cap is None else max(1, int(endpoint_active_cap))

    def get_json(
        self,
        url: str,
        endpoint_name: str | None = None,
        *,
        endpoint_priority: int = 1,
        endpoint_reserve: int = 1,
    ) -> FetchResult:
        endpoint = endpoint_name or urlparse(url).path
        started_at = time.perf_counter()
        attempt_count = 0
        rate_limit_attempts = 0
        last_error: BaseException | None = None
        last_response: requests.Response | None = None
        retryable_methods = getattr(self._pool, "_retry_methods", {"GET"})
        max_attempts = max(1, int(getattr(self._pool, "_max_attempts", self._retries)))

        with self._pool.endpoint(endpoint, reserve=max(1, endpoint_reserve), priority=max(1, endpoint_priority)):
            while attempt_count < max_attempts:
                try:
                    key = _take_key_with_wait(
                        self._pool,
                        endpoint,
                        timeout_seconds=self._key_wait_timeout,
                        active_key_budget=self._active_key_budget,
                        endpoint_active_cap=self._endpoint_active_cap,
                    )
                    attempt_count += 1
                    pool_allocation = _build_pool_allocation_snapshot(
                        self._pool,
                        endpoint=endpoint,
                        endpoint_priority=max(1, endpoint_priority),
                        endpoint_reserve=max(1, endpoint_reserve),
                        selected_key_name=key.name,
                    )
                    request_url, headers = self._prepare_request(url, key.token)
                    response = self._session.get(
                        request_url,
                        headers=headers,
                        timeout=self._timeout,
                    )
                except TimeoutError as exc:
                    timeout_error = requests.Timeout(str(exc))
                    setattr(
                        timeout_error,
                        "fetch_metadata",
                        FetchMetadata(
                            status_code=None,
                            api_key_name=None,
                            attempt_count=attempt_count,
                            rate_limit_attempts=rate_limit_attempts,
                            duration_ms=max(1, round((time.perf_counter() - started_at) * 1000)),
                            pool_allocation=_build_pool_allocation_snapshot(
                                self._pool,
                                endpoint=endpoint,
                                endpoint_priority=max(1, endpoint_priority),
                                endpoint_reserve=max(1, endpoint_reserve),
                                selected_key_name=None,
                            ),
                        ),
                    )
                    raise timeout_error
                except requests.RequestException as exc:
                    self._pool.mark_result(key, None, {}, exc)
                    setattr(
                        exc,
                        "fetch_metadata",
                        FetchMetadata(
                            status_code=None,
                            api_key_name=key.name,
                            attempt_count=attempt_count,
                            rate_limit_attempts=rate_limit_attempts,
                            duration_ms=max(1, round((time.perf_counter() - started_at) * 1000)),
                            pool_allocation=pool_allocation,
                        ),
                    )
                    last_error = exc
                    if attempt_count < max_attempts and "GET" in retryable_methods:
                        continue
                    raise

                self._pool.mark_result(key, response.status_code, dict(response.headers), None)
                current_rate_limit_attempts = rate_limit_attempts + (1 if response.status_code == 429 else 0)
                metadata = FetchMetadata(
                    status_code=response.status_code,
                    api_key_name=key.name,
                    attempt_count=attempt_count,
                    rate_limit_attempts=current_rate_limit_attempts,
                    duration_ms=max(1, round((time.perf_counter() - started_at) * 1000)),
                    pool_allocation=pool_allocation,
                )
                if response.status_code == 429 and attempt_count < max_attempts and "GET" in retryable_methods:
                    rate_limit_attempts += 1
                    last_response = response
                    with contextlib.suppress(Exception):
                        response.close()
                    continue
                try:
                    response.raise_for_status()
                except requests.HTTPError as exc:
                    setattr(exc, "fetch_metadata", metadata)
                    raise
                return FetchResult(payload=response.json(), metadata=metadata)

        if last_response is not None:
            error = requests.HTTPError(f"{last_response.status_code} Server Error: rate limited for url: {url}", response=last_response)
            setattr(
                error,
                "fetch_metadata",
                FetchMetadata(
                    status_code=last_response.status_code,
                    api_key_name=None,
                    attempt_count=attempt_count,
                    rate_limit_attempts=rate_limit_attempts,
                    duration_ms=max(1, round((time.perf_counter() - started_at) * 1000)),
                    pool_allocation=None,
                ),
            )
            raise error
        if last_error is not None:
            raise last_error
        raise RuntimeError(f"Failed to fetch {url}")

    def _prepare_request(self, url: str, token: str) -> tuple[str, dict[str, str]]:
        if self._auth_header_name is not None:
            return url, {self._auth_header_name: token}
        return with_query_params(url, {self._auth_query_param_name: token}), {}


class CongressClient(_RotatingApiClient):
    def __init__(
        self,
        env_path: Path | None = None,
        timeout: float = 30.0,
        retries: int = 3,
        *,
        use_shared_pool: bool = True,
        key_wait_timeout: float | None = None,
        active_key_budget: int | None = None,
        endpoint_active_cap: int | None = None,
    ) -> None:
        super().__init__(
            env_path=env_path,
            timeout=timeout,
            retries=retries,
            use_shared_pool=use_shared_pool,
            key_wait_timeout=key_wait_timeout,
            active_key_budget=active_key_budget,
            endpoint_active_cap=endpoint_active_cap,
            explicit_key_env_names=["CONGRESS_API_KEYS", "CONGRESSIONAL_API_KEY"],
            indexed_key_prefix="CONGRESS_API_KEY_",
            missing_keys_message="No Congress API keys found in CONGRESS_API_KEYS, CONGRESSIONAL_API_KEY, or CONGRESS_API_KEY_*",
            pool_namespace="congress",
            auth_header_name="x-api-key",
        )


class GovInfoClient(_RotatingApiClient):
    def __init__(
        self,
        env_path: Path | None = None,
        timeout: float = 30.0,
        retries: int = 3,
        *,
        use_shared_pool: bool = True,
        key_wait_timeout: float | None = None,
        active_key_budget: int | None = None,
        endpoint_active_cap: int | None = None,
    ) -> None:
        super().__init__(
            env_path=env_path,
            timeout=timeout,
            retries=retries,
            use_shared_pool=use_shared_pool,
            key_wait_timeout=key_wait_timeout,
            active_key_budget=active_key_budget,
            endpoint_active_cap=endpoint_active_cap,
            explicit_key_env_names=["GOVINFO_API_KEYS", "GOVINFO_API_KEY"],
            indexed_key_prefix="GOVINFO_API_KEY_",
            missing_keys_message="No GovInfo API keys found in GOVINFO_API_KEYS, GOVINFO_API_KEY, or GOVINFO_API_KEY_*",
            pool_namespace="govinfo",
            auth_query_param_name="api_key",
        )


class AsyncCongressClient:
    def __init__(
        self,
        env_path: Path | None = None,
        timeout: float = 30.0,
        retries: int = 3,
        *,
        use_shared_pool: bool = True,
        key_wait_timeout: float | None = None,
        active_key_budget: int | None = None,
        endpoint_active_cap: int | None = None,
    ) -> None:
        self._sync_client = CongressClient(
            env_path=env_path,
            timeout=timeout,
            retries=retries,
            use_shared_pool=use_shared_pool,
            key_wait_timeout=key_wait_timeout,
            active_key_budget=active_key_budget,
            endpoint_active_cap=endpoint_active_cap,
        )

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


class AsyncGovInfoClient:
    def __init__(
        self,
        env_path: Path | None = None,
        timeout: float = 30.0,
        retries: int = 3,
        *,
        use_shared_pool: bool = True,
        key_wait_timeout: float | None = None,
        active_key_budget: int | None = None,
        endpoint_active_cap: int | None = None,
    ) -> None:
        self._sync_client = GovInfoClient(
            env_path=env_path,
            timeout=timeout,
            retries=retries,
            use_shared_pool=use_shared_pool,
            key_wait_timeout=key_wait_timeout,
            active_key_budget=active_key_budget,
            endpoint_active_cap=endpoint_active_cap,
        )

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
