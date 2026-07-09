from __future__ import annotations

import logging
import tempfile
import threading
import time
import unittest
from pathlib import Path

import requests

from bicam_collection.client import CongressClient, GovInfoClient, clear_shared_key_pools


class FakeKey:
    def __init__(self, name: str, token: str) -> None:
        self.name = name
        self.token = token
        self.assigned_to: str | None = None
        self.in_use_by: str | None = None
        self.remaining: int | None = None

    def next_available_at(self, now: float) -> float:
        return now


class FakeEndpointInfo:
    def __init__(self, name: str, priority: int, reserve: int) -> None:
        self.name = name
        self.priority = priority
        self.reserve = reserve


class FakeEndpointLease:
    def __init__(self, pool: "FakePool", name: str, reserve: int, priority: int) -> None:
        self._pool = pool
        self._name = name
        self._reserve = reserve
        self._priority = priority

    def __enter__(self):
        with self._pool._lock:
            self._pool._endpoints[self._name] = FakeEndpointInfo(self._name, self._priority, self._reserve)
            for key in self._pool._keys[: self._reserve]:
                key.assigned_to = self._name
        return self

    def __exit__(self, exc_type, exc, tb):
        with self._pool._lock:
            self._pool._endpoints.pop(self._name, None)
            for key in self._pool._keys:
                if key.assigned_to == self._name:
                    key.assigned_to = None
        return False


class FakePool:
    def __init__(self, keys: list[FakeKey]) -> None:
        self._keys = list(keys)
        self._retry_methods = {"GET"}
        self._max_attempts = len(keys)
        self.marked_results: list[tuple[str, int | None]] = []
        self.endpoint_calls: list[tuple[str, int, int]] = []
        self._lock = threading.Lock()
        self._endpoints: dict[str, FakeEndpointInfo] = {}

    def endpoint(self, name: str, reserve: int = 1, priority: int = 1) -> FakeEndpointLease:
        self.endpoint_calls.append((name, reserve, priority))
        return FakeEndpointLease(self, name, reserve, priority)

    def take_key(self, endpoint: str) -> FakeKey:
        key = self._keys.pop(0)
        key.in_use_by = endpoint
        self._keys.append(key)
        return key

    def mark_result(self, key: FakeKey, status_code: int | None, headers: dict[str, str], error: BaseException | None) -> None:
        self.marked_results.append((key.name, status_code))
        key.in_use_by = None


class ExhaustedPool:
    def __init__(self) -> None:
        self._retry_methods = {"GET"}
        self._max_attempts = 1
        self._lock = threading.Lock()
        self._endpoints: dict[str, FakeEndpointInfo] = {}
        self._keys = [FakeKey("alpha", "A")]

    def endpoint(self, name: str, reserve: int = 1, priority: int = 1) -> FakeEndpointLease:
        return FakeEndpointLease(self, name, reserve, priority)

    def _now(self) -> float:
        return time.time()

    def _pick_candidate(self, endpoint: str):
        return None


class FakeSession:
    def __init__(self, responses: list[requests.Response | BaseException]) -> None:
        self._responses = list(responses)
        self.calls: list[dict[str, object]] = []

    def get(self, url: str, *, headers: dict[str, str], timeout: float) -> requests.Response:
        self.calls.append({"url": url, "headers": headers, "timeout": timeout})
        response = self._responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return response


def _build_response(status_code: int, url: str, payload: str) -> requests.Response:
    response = requests.Response()
    response.status_code = status_code
    response.url = url
    response._content = payload.encode("utf-8")
    response.headers["Content-Type"] = "application/json"
    return response


class ClientTests(unittest.TestCase):
    def tearDown(self) -> None:
        clear_shared_key_pools()

    def _build_client(self) -> CongressClient:
        with tempfile.TemporaryDirectory() as tmpdir:
            env_path = Path(tmpdir) / ".env"
            env_path.write_text("CONGRESS_API_KEYS=alpha,beta\n", encoding="utf-8")
            client = CongressClient(env_path=env_path, retries=2)
        return client

    def test_get_json_rotates_keys_after_429_and_returns_telemetry(self) -> None:
        url = "https://api.congress.gov/v3/bill?format=json"
        client = self._build_client()
        client._pool = FakePool([FakeKey("alpha", "A"), FakeKey("beta", "B")])
        client._session = FakeSession(
            [
                _build_response(429, url, '{"error":"rate limited"}'),
                _build_response(200, url, '{"ok": true}'),
            ]
        )

        result = client.get_json(url)

        self.assertEqual(result.payload, {"ok": True})
        self.assertEqual(result.metadata.status_code, 200)
        self.assertEqual(result.metadata.api_key_name, "beta")
        self.assertEqual(result.metadata.attempt_count, 2)
        self.assertEqual(result.metadata.rate_limit_attempts, 1)
        self.assertGreaterEqual(result.metadata.duration_ms, 1)
        self.assertIsNotNone(result.metadata.pool_allocation)
        self.assertEqual(result.metadata.pool_allocation["endpoint_name"], "/v3/bill")
        self.assertEqual(result.metadata.pool_allocation["selected_key_name"], "beta")
        self.assertEqual(client._session.calls[0]["headers"], {"x-api-key": "A"})
        self.assertEqual(client._session.calls[1]["headers"], {"x-api-key": "B"})
        self.assertEqual(client._pool.endpoint_calls, [("/v3/bill", 1, 1)])

    def test_get_json_attaches_fetch_metadata_to_transport_errors(self) -> None:
        url = "https://api.congress.gov/v3/bill?format=json"
        client = self._build_client()
        client._pool = FakePool([FakeKey("alpha", "A")])
        client._session = FakeSession([requests.Timeout("timed out")])

        with self.assertRaises(requests.Timeout) as exc_context:
            client.get_json(url)

        metadata = getattr(exc_context.exception, "fetch_metadata", None)
        self.assertIsNotNone(metadata)
        self.assertEqual(metadata.api_key_name, "alpha")
        self.assertEqual(metadata.attempt_count, 1)
        self.assertEqual(metadata.rate_limit_attempts, 0)
        self.assertEqual(metadata.pool_allocation["selected_key_name"], "alpha")

    def test_get_json_uses_endpoint_priority_and_reserve(self) -> None:
        url = "https://api.congress.gov/v3/bill?format=json"
        client = self._build_client()
        client._pool = FakePool([FakeKey("alpha", "A")])
        client._session = FakeSession([_build_response(200, url, '{"ok": true}')])

        client.get_json(url, "bills", endpoint_priority=3, endpoint_reserve=2)

        self.assertEqual(client._pool.endpoint_calls, [("bills", 2, 3)])

    def test_govinfo_client_sends_api_key_as_query_param(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            env_path = Path(tmpdir) / ".env"
            env_path.write_text("GOVINFO_API_KEY=alpha\n", encoding="utf-8")
            client = GovInfoClient(env_path=env_path, retries=1)

        url = "https://api.govinfo.gov/packages/CRPT-119hrpt173/summary"
        client._pool = FakePool([FakeKey("alpha", "A")])
        client._session = FakeSession([_build_response(200, f"{url}?api_key=A", '{"ok": true}')])

        result = client.get_json(url, "govinfo-committee-reports")

        self.assertEqual(result.payload, {"ok": True})
        self.assertEqual(client._session.calls[0]["url"], f"{url}?api_key=A")
        self.assertEqual(client._session.calls[0]["headers"], {})

    def test_clients_share_pool_for_same_env_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            env_path = Path(tmpdir) / ".env"
            env_path.write_text("CONGRESS_API_KEYS=alpha,beta\n", encoding="utf-8")

            left = CongressClient(env_path=env_path, retries=2)
            right = CongressClient(env_path=env_path, retries=2)

        self.assertIs(left._pool, right._pool)

    def test_client_can_opt_out_of_shared_pool(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            env_path = Path(tmpdir) / ".env"
            env_path.write_text("CONGRESS_API_KEYS=alpha,beta\n", encoding="utf-8")

            left = CongressClient(env_path=env_path, retries=2, use_shared_pool=False)
            right = CongressClient(env_path=env_path, retries=2, use_shared_pool=False)

        self.assertIsNot(left._pool, right._pool)

    def test_get_json_times_out_when_no_key_becomes_available(self) -> None:
        url = "https://api.congress.gov/v3/bill?format=json"
        client = self._build_client()
        client._pool = ExhaustedPool()
        client._key_wait_timeout = 0.01

        with self.assertLogs("bicam_collection.client", level=logging.WARNING) as logs, self.assertRaises(requests.Timeout) as exc_context:
            client.get_json(url, "bills")

        metadata = getattr(exc_context.exception, "fetch_metadata", None)
        self.assertIsNotNone(metadata)
        self.assertIsNone(metadata.api_key_name)
        self.assertEqual(metadata.attempt_count, 0)
        self.assertEqual(metadata.pool_allocation["endpoint_name"], "bills")
        self.assertIn("All API keys unavailable in shared pool", logs.output[0])


if __name__ == "__main__":
    unittest.main()
