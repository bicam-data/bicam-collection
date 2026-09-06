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

from bicam_collection.config import parse_dotenv
from bicam_collection.urls import with_query_params


def _load_key_configs(
    env_path: Path | None,
    *,
    explicit_names: list[str],
    indexed_prefix: str,
    per_window: tuple[int, int] | None = None,
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
                KeyConfig(
                    name=f"{name.lower()}_{idx + 1}",
                    token=part,
                    per_window=per_window,
                )
                for idx, part in enumerate(parts)
            )
        else:
            key_configs.append(
                KeyConfig(name=name.lower(), token=raw_value, per_window=per_window)
            )

    for name, value in sorted(env_values.items()):
        if not name.startswith(indexed_prefix) or not value.strip():
            continue
        key_configs.append(
            KeyConfig(
                name=name[len(indexed_prefix) :].lower(),
                token=value.strip(),
                per_window=per_window,
            )
        )

    return key_configs


def _select_key_shard(
    key_configs: list[KeyConfig],
    *,
    pool_namespace: str,
) -> list[KeyConfig]:
    """Select a deterministic, non-overlapping key shard for this process.

    A request pacer belongs to one client process, so several clients can
    safely increase aggregate throughput only when each process owns a
    disjoint subset of the configured keys.  ``BICAM_CONGRESS_KEY_SHARD``
    (or the corresponding upper-case namespace) uses ``slot/total`` syntax;
    key positions are assigned round-robin to keep shard sizes balanced.
    """

    variable = f"BICAM_{pool_namespace.upper()}_KEY_SHARD"
    raw_shard = os.environ.get(variable, "").strip()
    if not raw_shard:
        return key_configs
    try:
        raw_slot, raw_total = raw_shard.split("/", 1)
        slot = int(raw_slot)
        total = int(raw_total)
    except ValueError as exc:
        raise ValueError(f"{variable} must use SLOT/TOTAL syntax, got {raw_shard!r}") from exc
    if total < 1 or slot < 0 or slot >= total:
        raise ValueError(f"{variable} must satisfy 0 <= SLOT < TOTAL, got {raw_shard!r}")

    selected = [key for index, key in enumerate(key_configs) if index % total == slot]
    if not selected:
        raise ValueError(
            f"{variable} selected no configured keys (configured={len(key_configs)}, shard={raw_shard!r})"
        )
    return selected


def _setting_value(env_path: Path | None, name: str) -> str | None:
    """Return an operator setting from the process environment or dotenv file."""

    value = os.environ.get(name)
    if value is not None and value.strip():
        return value.strip()
    return parse_dotenv(env_path).get(name)


def _congress_per_key_window(
    env_path: Path | None,
    key_count: int,
    *,
    key_slot_count: int = 1,
) -> tuple[int, int]:
    """Return the local window for each Rotisserie key handle.

    ``BICAM_CONGRESS_PER_KEY_RATE_LIMIT`` is a credential-scoped budget.  It
    is divided only across virtual handles for the *same* credential, so
    duplicating a token to support concurrent requests cannot create extra
    quota.  ``BICAM_CONGRESS_GLOBAL_RATE_LIMIT`` remains a backwards-
    compatible fallback for older callers that intentionally supplied one
    aggregate budget; it is not used to infer the quota of unrelated keys.
    Any deployment-wide gateway/egress policy is separate from this per-token
    accounting. The legacy aggregate setting is retained for callers that
    still use aggregate pacing/ETA controls; it must not be interpreted as an
    account quota shared by unrelated credentials.
    """

    raw_per_key_limit = _setting_value(env_path, "BICAM_CONGRESS_PER_KEY_RATE_LIMIT")
    raw_limit = _setting_value(env_path, "BICAM_CONGRESS_GLOBAL_RATE_LIMIT")
    raw_window = _setting_value(env_path, "BICAM_CONGRESS_RATE_WINDOW_SECONDS")
    raw_safety = _setting_value(env_path, "BICAM_CONGRESS_RATE_SAFETY")
    try:
        global_limit = max(1, int(raw_limit)) if raw_limit else _CONGRESS_GLOBAL_RATE_LIMIT
    except (TypeError, ValueError):
        global_limit = _CONGRESS_GLOBAL_RATE_LIMIT
    try:
        window_seconds = max(1, int(raw_window)) if raw_window else _CONGRESS_RATE_WINDOW_SECONDS
    except (TypeError, ValueError):
        window_seconds = _CONGRESS_RATE_WINDOW_SECONDS
    try:
        safety = float(raw_safety) if raw_safety else _CONGRESS_RATE_SAFETY
    except (TypeError, ValueError):
        safety = _CONGRESS_RATE_SAFETY
    safety = min(1.0, max(0.1, safety))
    if raw_per_key_limit:
        try:
            credential_limit = max(1, int(raw_per_key_limit))
        except (TypeError, ValueError):
            credential_limit = _CONGRESS_GLOBAL_RATE_LIMIT
        # Virtual slots share one credential's window.  The count is computed
        # before key sharding, so every shard applies the same token budget.
        per_key_limit = max(1, int(credential_limit * safety) // max(1, key_slot_count))
    else:
        # Legacy aggregate mode, retained for non-Congress callers and old
        # one-off scripts. New services should set the explicit per-key value.
        per_key_limit = max(1, int(global_limit * safety) // max(1, key_count))
    return per_key_limit, window_seconds


def _congress_key_slots(env_path: Path | None) -> int:
    """Return local concurrency slots allocated per Congress credential."""

    raw_slots = _setting_value(env_path, "BICAM_CONGRESS_KEY_SLOTS")
    try:
        slots = int(raw_slots) if raw_slots else 1
    except (TypeError, ValueError):
        slots = 1
    return min(4, max(1, slots))


def _expand_congress_key_slots(key_configs: list[KeyConfig], slots: int) -> list[KeyConfig]:
    """Duplicate credentials into local slots without changing their tokens."""

    if slots <= 1:
        return key_configs
    return [
        replace(key, name=f"{key.name}__slot{slot}")
        for key in key_configs
        for slot in range(1, slots + 1)
    ]


_SHARED_POOLS: dict[tuple[str | None, int, str, str, str], KeyPool] = {}
_SHARED_POOLS_LOCK = threading.Lock()
_WAIT_LOG_INTERVAL_SECONDS = 5.0
LOGGER = logging.getLogger(__name__)
# The live deployment currently advertises a 20,000-request window in the
# response headers. That is a token-level observation, not proof that account
# quotas are independent. Rotisserie tracks the configured local window per
# token; the supervisor may export a provisional aggregate after considering
# account ownership, published policy, and coordinated 429 evidence. Keep this
# conservative standalone default; operators can override it when a source
# publishes a different limit or the credential set changes.
_CONGRESS_GLOBAL_RATE_LIMIT = 20_000
_CONGRESS_RATE_WINDOW_SECONDS = 3_600
_CONGRESS_RATE_SAFETY = 0.95

# Congress.gov occasionally returns gateway/service errors for otherwise valid
# child URLs.  Treat the transient set the same way as 429s so a single edge
# failure does not become a permanent missing record.  Deliberately exclude
# HTTP 500 from the status set: Congress.gov has a small, known set of
# deterministic 500s that should remain in the exception queue rather than
# being replayed on every request.  _is_transient_response below admits only
# 500 responses whose body identifies the service-side persistence/gateway
# outage we have observed in production.
_TRANSIENT_RETRY_STATUS_CODES = frozenset({408, 425, 429, 502, 503, 504, 520, 522, 523, 524})
_TRANSIENT_500_MARKERS = (
    "misconf",
    "valkey",
    "temporarily unavailable",
    "upstream connect error",
)
# Congress.gov's JSON error body is more informative than requests' generic
# ``500 Server Error`` text. Keep a small, stable classification for source
# data defects so the durable failure queue can distinguish them from a
# recoverable gateway outage without retaining an entire response body.
_SOURCE_ERROR_MARKERS = {
    "memberterm doesnotexist": "member_term_does_not_exist",
    "noreversematch": "no_reverse_match",
}
_TRANSIENT_RETRY_BASE_SECONDS = 0.5
_TRANSIENT_RETRY_MAX_SECONDS = 30.0
# A long server-provided delay is evidence of a coordinated source/account
# window, not a single-key blip.  Retrying immediately with another credential
# fans one 429 into a burst against every key.  Leave those URLs in the
# durable failure queue for the scheduled retry pass instead.
_LONG_RATE_LIMIT_RETRY_AFTER_SECONDS = 60.0


def _is_transient_response(response: requests.Response) -> bool:
    """Return whether a response is safe to replay on the same GET URL.

    Most HTTP 500 responses from Congress.gov are deterministic source-side
    exceptions.  A recent Valkey persistence outage, however, surfaced as a
    500 JSON response and was therefore being recorded permanently even when
    a retry would succeed.  Restrict the 500 exception to explicit service
    outage markers; all other 500s retain the existing non-retry behavior.
    """

    if response.status_code in _TRANSIENT_RETRY_STATUS_CODES:
        return True
    if response.status_code != 500:
        return False
    try:
        body = response.text.lower()
    except Exception:
        return False
    return any(marker in body for marker in _TRANSIENT_500_MARKERS)


def _source_error_code(response: requests.Response) -> str | None:
    """Return a compact source-error code for a terminal HTTP 500 response."""

    if response.status_code != 500:
        return None
    try:
        payload = response.json()
    except (ValueError, requests.exceptions.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    raw_error = payload.get("error")
    if not isinstance(raw_error, str):
        return None
    normalized = "".join(raw_error.lower().split())
    for marker, code in _SOURCE_ERROR_MARKERS.items():
        if "".join(marker.lower().split()) in normalized:
            return code
    return None


def _annotate_source_error(error: requests.HTTPError, response: requests.Response) -> None:
    """Attach a safe, bounded source classification to an HTTP error."""

    code = _source_error_code(response)
    if code is None:
        return
    message = str(error)
    if f"source_error={code}" not in message:
        error.args = (f"{message} source_error={code}",)


def _transient_retry_delay(
    response: requests.Response | None,
    attempt_count: int,
) -> float:
    """Return a bounded delay before replaying a transient GET.

    Congress.gov can return a coordinated 503/520 burst across many child
    endpoints.  Replaying every response immediately makes that outage worse
    and fills the durable failure queue with requests that were sent during the
    same edge incident.  Honor a numeric Retry-After when present; otherwise
    use a short exponential delay capped so a single URL cannot stall a crawl
    indefinitely.
    """

    if response is not None:
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            with contextlib.suppress(TypeError, ValueError):
                return min(_TRANSIENT_RETRY_MAX_SECONDS, max(0.0, float(retry_after)))
    exponent = max(0, attempt_count - 1)
    return min(_TRANSIENT_RETRY_MAX_SECONDS, _TRANSIENT_RETRY_BASE_SECONDS * (2**exponent))


def clear_shared_key_pools() -> None:
    with _SHARED_POOLS_LOCK:
        _SHARED_POOLS.clear()


def _shared_pool_key(
    env_path: Path | None,
    retries: int,
    pool_namespace: str,
) -> tuple[str | None, int, str, str, str]:
    shard = os.environ.get(f"BICAM_{pool_namespace.upper()}_KEY_SHARD", "").strip()
    slots = _setting_value(env_path, f"BICAM_{pool_namespace.upper()}_KEY_SLOTS") or ""
    return (str(env_path.resolve()) if env_path is not None else None, retries, pool_namespace, shard, slots)


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


_FETCH_METADATA_COLUMNS = (
    "status_code",
    "duration_ms",
    "attempt_count",
    "rate_limit_attempts",
    "api_key_name",
    "pool_allocation",
)


def fetch_metadata_values(metadata: FetchMetadata | None) -> dict[str, Any]:
    """Map a fetch outcome onto the ``record_fetch`` / ``record_failed_request`` keyword arguments."""
    if metadata is None:
        return {column: None for column in _FETCH_METADATA_COLUMNS}
    return {column: getattr(metadata, column) for column in _FETCH_METADATA_COLUMNS}


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
            # ``next_available_at`` describes cooldown, not lease release.
            # An in-use key can report ``now`` while another request owns it;
            # including those keys makes the waiter spin at the 10ms floor
            # whenever every key is leased.  Wait for the next cooldown only
            # among idle keys.  Include exhausted idle keys: their
            # ``next_available_at`` is the end of the rate window, and
            # excluding them makes every waiter spin at the 50ms floor for
            # the entire quota cooldown.  Use a short lease-release poll only
            # when all keys are currently in use.
            idle_wake_times = [
                key_state.next_available_at(now)
                for key_state in keys
                if key_state.in_use_by is None
            ]
            wake = min(idle_wake_times) if idle_wake_times else now + 0.05
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
        request_interval: float = 0.0,
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
        self._request_interval = max(0.0, float(request_interval))
        self._request_pacer_lock = threading.Lock()
        self._next_request_at = 0.0
        self._session = requests.Session()
        if auth_header_name is not None and auth_query_param_name is None:
            self._auth_placement, self._auth_name = "header", auth_header_name
        elif auth_query_param_name is not None and auth_header_name is None:
            self._auth_placement, self._auth_name = "query", auth_query_param_name
        else:
            raise ValueError("Exactly one auth mechanism must be configured")
        key_configs = _load_key_configs(
            env_path,
            explicit_names=explicit_key_env_names,
            indexed_prefix=indexed_key_prefix,
            per_window=None,
        )
        if pool_namespace == "congress":
            key_slot_count = _congress_key_slots(env_path)
            key_configs = _expand_congress_key_slots(key_configs, key_slot_count)
            per_key_window = _congress_per_key_window(
                env_path,
                len(key_configs),
                key_slot_count=key_slot_count,
            )
            key_configs = [replace(key, per_window=per_key_window) for key in key_configs]
        key_configs = _select_key_shard(key_configs, pool_namespace=pool_namespace)
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
            self._active_key_budget: int | None = max(1, int(active_key_budget))
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
                    self._pace_request()
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
                        time.sleep(_transient_retry_delay(None, attempt_count))
                        continue
                    raise

                self._pool.mark_result(key, response.status_code, dict(response.headers), None)
                if response.status_code == 429:
                    # Preserve the server's view of the quota in the durable
                    # worker log.  Rotisserie tracks per-key windows locally,
                    # but Congress.gov may also enforce an account or source
                    # window; these headers distinguish the two cases on the
                    # next run without logging the secret token.
                    LOGGER.warning(
                        "API rate limit response endpoint=%s key=%s limit=%s remaining=%s retry_after=%s",
                        endpoint,
                        key.name,
                        response.headers.get("X-RateLimit-Limit"),
                        response.headers.get("X-RateLimit-Remaining"),
                        response.headers.get("Retry-After"),
                    )
                current_rate_limit_attempts = rate_limit_attempts + (1 if response.status_code == 429 else 0)
                metadata = FetchMetadata(
                    status_code=response.status_code,
                    api_key_name=key.name,
                    attempt_count=attempt_count,
                    rate_limit_attempts=current_rate_limit_attempts,
                    duration_ms=max(1, round((time.perf_counter() - started_at) * 1000)),
                    pool_allocation=pool_allocation,
                )
                if (
                    _is_transient_response(response)
                    and attempt_count < max_attempts
                    and "GET" in retryable_methods
                ):
                    retry_after_header = response.headers.get("Retry-After")
                    long_rate_limit_cooldown = False
                    if response.status_code == 429 and retry_after_header:
                        with contextlib.suppress(TypeError, ValueError):
                            long_rate_limit_cooldown = (
                                float(retry_after_header)
                                >= _LONG_RATE_LIMIT_RETRY_AFTER_SECONDS
                            )
                    if long_rate_limit_cooldown:
                        # The source has supplied a coordinated cooldown.  A
                        # durable retry is safer than consuming another key
                        # while the whole account window is exhausted.
                        try:
                            response.raise_for_status()
                        except requests.HTTPError as exc:
                            _annotate_source_error(exc, response)
                            setattr(exc, "fetch_metadata", metadata)
                            raise
                    if response.status_code == 429:
                        rate_limit_attempts += 1
                    last_response = response
                    retry_delay = _transient_retry_delay(response, attempt_count)
                    with contextlib.suppress(Exception):
                        response.close()
                    time.sleep(retry_delay)
                    continue
                try:
                    response.raise_for_status()
                except requests.HTTPError as exc:
                    _annotate_source_error(exc, response)
                    setattr(exc, "fetch_metadata", metadata)
                    raise
                return FetchResult(payload=response.json(), metadata=metadata)

        if last_response is not None:
            error = requests.HTTPError(
                f"{last_response.status_code} Server Error after {attempt_count} attempt(s) for url: {url}",
                response=last_response,
            )
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
        if self._auth_placement == "header":
            return url, {self._auth_name: token}
        return with_query_params(url, {self._auth_name: token}), {}


class CongressClient(_RotatingApiClient):
    def __init__(
        self,
        env_path: Path | None = None,
        timeout: float = 30.0,
        # Keep rate-limit failures bounded to one alternate key.  A longer
        # retry chain can fan one 429 across an entire shared key pool and
        # trigger an account-wide cooldown; failed URLs are durably queued
        # for the later retry pass instead.
        retries: int = 2,
        *,
        use_shared_pool: bool = True,
        key_wait_timeout: float | None = None,
        active_key_budget: int | None = None,
        endpoint_active_cap: int | None = None,
        request_interval: float = 0.0,
    ) -> None:
        super().__init__(
            env_path=env_path,
            timeout=timeout,
            retries=retries,
            use_shared_pool=use_shared_pool,
            key_wait_timeout=key_wait_timeout,
            active_key_budget=active_key_budget,
            endpoint_active_cap=endpoint_active_cap,
            request_interval=request_interval,
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
        request_interval: float = 0.0,
    ) -> None:
        super().__init__(
            env_path=env_path,
            timeout=timeout,
            retries=retries,
            use_shared_pool=use_shared_pool,
            key_wait_timeout=key_wait_timeout,
            active_key_budget=active_key_budget,
            endpoint_active_cap=endpoint_active_cap,
            request_interval=request_interval,
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
        retries: int = 2,
        *,
        use_shared_pool: bool = True,
        key_wait_timeout: float | None = None,
        active_key_budget: int | None = None,
        endpoint_active_cap: int | None = None,
        request_interval: float = 0.0,
    ) -> None:
        self._sync_client = CongressClient(
            env_path=env_path,
            timeout=timeout,
            retries=retries,
            use_shared_pool=use_shared_pool,
            key_wait_timeout=key_wait_timeout,
            active_key_budget=active_key_budget,
            endpoint_active_cap=endpoint_active_cap,
            request_interval=request_interval,
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
        request_interval: float = 0.0,
    ) -> None:
        self._sync_client = GovInfoClient(
            env_path=env_path,
            timeout=timeout,
            retries=retries,
            use_shared_pool=use_shared_pool,
            key_wait_timeout=key_wait_timeout,
            active_key_budget=active_key_budget,
            endpoint_active_cap=endpoint_active_cap,
            request_interval=request_interval,
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
