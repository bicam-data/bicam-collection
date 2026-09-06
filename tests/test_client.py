from __future__ import annotations

import logging
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import requests

from bicam_collection.client import CongressClient, GovInfoClient, _take_key_with_wait, clear_shared_key_pools


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


class CooldownKey(FakeKey):
    def __init__(self) -> None:
        super().__init__("cooling", "token")
        self.remaining = 0
        self.window_ends_at = time.time() + 60

    def next_available_at(self, now: float) -> float:
        return self.window_ends_at


class CooldownPool(ExhaustedPool):
    def __init__(self) -> None:
        super().__init__()
        self._keys = [CooldownKey()]


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

    def test_get_json_does_not_rotate_after_long_rate_limit_cooldown(self) -> None:
        url = "https://api.congress.gov/v3/bill?format=json"
        first = _build_response(429, url, '{"error":"rate limited"}')
        first.headers["Retry-After"] = "1800"
        client = self._build_client()
        client._pool = FakePool([FakeKey("alpha", "A"), FakeKey("beta", "B")])
        client._session = FakeSession([first])

        with self.assertRaises(requests.HTTPError) as exc_context:
            client.get_json(url)

        self.assertEqual(len(client._session.calls), 1)
        metadata = getattr(exc_context.exception, "fetch_metadata", None)
        self.assertIsNotNone(metadata)
        self.assertEqual(metadata.attempt_count, 1)
        self.assertEqual(metadata.rate_limit_attempts, 1)

    def test_get_json_retries_transient_gateway_error(self) -> None:
        url = "https://api.congress.gov/v3/bill/119/hr/1/actions?format=json"
        client = self._build_client()
        client._pool = FakePool([FakeKey("alpha", "A"), FakeKey("beta", "B")])
        client._session = FakeSession(
            [
                _build_response(503, url, '{"error":"temporarily unavailable"}'),
                _build_response(200, url, '{"ok": true}'),
            ]
        )

        result = client.get_json(url)

        self.assertEqual(result.payload, {"ok": True})
        self.assertEqual(result.metadata.attempt_count, 2)
        self.assertEqual(result.metadata.rate_limit_attempts, 0)
        self.assertEqual(client._session.calls[0]["headers"], {"x-api-key": "A"})
        self.assertEqual(client._session.calls[1]["headers"], {"x-api-key": "B"})

    def test_get_json_backs_off_before_replaying_transient_gateway_error(self) -> None:
        url = "https://api.congress.gov/v3/bill/119/hr/1/actions?format=json"
        client = self._build_client()
        client._pool = FakePool([FakeKey("alpha", "A"), FakeKey("beta", "B")])
        client._session = FakeSession(
            [
                _build_response(503, url, '{"error":"temporarily unavailable"}'),
                _build_response(200, url, '{"ok": true}'),
            ]
        )

        with patch("bicam_collection.client.time.sleep") as sleep:
            result = client.get_json(url)

        self.assertEqual(result.payload, {"ok": True})
        sleep.assert_called_once_with(0.5)

    def test_get_json_honors_retry_after_for_transient_gateway_error(self) -> None:
        url = "https://api.congress.gov/v3/bill/119/hr/1/actions?format=json"
        first = _build_response(503, url, '{"error":"temporarily unavailable"}')
        first.headers["Retry-After"] = "7"
        client = self._build_client()
        client._pool = FakePool([FakeKey("alpha", "A"), FakeKey("beta", "B")])
        client._session = FakeSession([first, _build_response(200, url, '{"ok": true}')])

        with patch("bicam_collection.client.time.sleep") as sleep:
            client.get_json(url)

        sleep.assert_called_once_with(7.0)

    def test_get_json_retries_transient_backend_500(self) -> None:
        url = "https://api.congress.gov/v3/bill/114/hr/4250?format=json"
        client = self._build_client()
        client._pool = FakePool([FakeKey("alpha", "A"), FakeKey("beta", "B")])
        client._session = FakeSession(
            [
                _build_response(
                    500,
                    url,
                    '{"error":"MISCONF Valkey is configured to save RDB snapshots"}',
                ),
                _build_response(200, url, '{"ok": true}'),
            ]
        )

        result = client.get_json(url)

        self.assertEqual(result.payload, {"ok": True})
        self.assertEqual(result.metadata.attempt_count, 2)
        self.assertEqual(client._session.calls[0]["headers"], {"x-api-key": "A"})
        self.assertEqual(client._session.calls[1]["headers"], {"x-api-key": "B"})

    def test_get_json_does_not_retry_unclassified_500(self) -> None:
        url = "https://api.congress.gov/v3/bill/16/cen_doc_h/58?format=json"
        client = self._build_client()
        client._pool = FakePool([FakeKey("alpha", "A"), FakeKey("beta", "B")])
        client._session = FakeSession([_build_response(500, url, '{"error":"source error"}')])

        with self.assertRaises(requests.HTTPError):
            client.get_json(url)

        self.assertEqual(len(client._session.calls), 1)

    def test_get_json_classifies_member_term_source_500(self) -> None:
        url = "https://api.congress.gov/v3/bill/106/sres/218/cosponsors?format=json"
        client = self._build_client()
        client._pool = FakePool([FakeKey("alpha", "A"), FakeKey("beta", "B")])
        client._session = FakeSession(
            [
                _build_response(
                    500,
                    url,
                    '{"error":"MemberTerm DoesNotExist: missing member term"}',
                )
            ]
        )

        with self.assertRaises(requests.HTTPError) as exc_context:
            client.get_json(url)

        self.assertIn("source_error=member_term_does_not_exist", str(exc_context.exception))
        self.assertEqual(len(client._session.calls), 1)

    def test_get_json_classifies_no_reverse_match_source_500(self) -> None:
        url = "https://api.congress.gov/v3/hearing/109?format=json&limit=250&offset=2750"
        client = self._build_client()
        client._pool = FakePool([FakeKey("alpha", "A"), FakeKey("beta", "B")])
        client._session = FakeSession(
            [
                _build_response(
                    500,
                    url,
                    '{"error":"Reverse for hearing_detail NoReverseMatch"}',
                )
            ]
        )

        with self.assertRaises(requests.HTTPError) as exc_context:
            client.get_json(url)

        self.assertIn("source_error=no_reverse_match", str(exc_context.exception))

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

    def test_client_can_select_a_non_overlapping_key_shard(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            env_path = Path(tmpdir) / ".env"
            env_path.write_text("CONGRESS_API_KEYS=a,b,c,d,e\n", encoding="utf-8")
            with patch.dict("os.environ", {"BICAM_CONGRESS_KEY_SHARD": "0/2"}, clear=False):
                left = CongressClient(env_path=env_path, retries=2)
            with patch.dict("os.environ", {"BICAM_CONGRESS_KEY_SHARD": "1/2"}, clear=False):
                right = CongressClient(env_path=env_path, retries=2)

        self.assertEqual([key.token for key in left._pool._keys], ["a", "c", "e"])
        self.assertEqual([key.token for key in right._pool._keys], ["b", "d"])
        self.assertIsNot(left._pool, right._pool)

    def test_congress_source_budget_is_split_before_sharding(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            env_path = Path(tmpdir) / ".env"
            env_path.write_text("CONGRESS_API_KEYS=a,b,c,d\n", encoding="utf-8")
            with patch.dict(
                "os.environ",
                {
                    "BICAM_CONGRESS_GLOBAL_RATE_LIMIT": "100",
                    "BICAM_CONGRESS_RATE_WINDOW_SECONDS": "60",
                    "BICAM_CONGRESS_RATE_SAFETY": "1.0",
                    "BICAM_CONGRESS_KEY_SHARD": "0/2",
                },
                clear=False,
            ):
                client = CongressClient(env_path=env_path, retries=1)

        self.assertEqual([key.token for key in client._pool._keys], ["a", "c"])
        self.assertEqual(client._pool._per_window, {"congress_api_keys_1": (25, 60), "congress_api_keys_3": (25, 60)})

    def test_congress_key_slots_split_same_budget_across_virtual_slots(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            env_path = Path(tmpdir) / ".env"
            env_path.write_text("CONGRESS_API_KEYS=a,b\n", encoding="utf-8")
            with patch.dict(
                "os.environ",
                {
                    "BICAM_CONGRESS_KEY_SLOTS": "2",
                    "BICAM_CONGRESS_GLOBAL_RATE_LIMIT": "100",
                    "BICAM_CONGRESS_RATE_WINDOW_SECONDS": "60",
                    "BICAM_CONGRESS_RATE_SAFETY": "1.0",
                },
                clear=False,
            ):
                client = CongressClient(env_path=env_path, retries=1)

        self.assertEqual([key.token for key in client._pool._keys], ["a", "a", "b", "b"])
        self.assertEqual(
            client._pool._per_window,
            {
                "congress_api_keys_1__slot1": (25, 60),
                "congress_api_keys_1__slot2": (25, 60),
                "congress_api_keys_2__slot1": (25, 60),
                "congress_api_keys_2__slot2": (25, 60),
            },
        )

    def test_congress_per_key_budget_is_not_divided_across_unrelated_keys(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            env_path = Path(tmpdir) / ".env"
            env_path.write_text("CONGRESS_API_KEYS=a,b\n", encoding="utf-8")
            with patch.dict(
                "os.environ",
                {
                    "BICAM_CONGRESS_KEY_SLOTS": "2",
                    "BICAM_CONGRESS_PER_KEY_RATE_LIMIT": "20000",
                    "BICAM_CONGRESS_RATE_WINDOW_SECONDS": "3600",
                    "BICAM_CONGRESS_RATE_SAFETY": "0.95",
                },
                clear=False,
            ):
                client = CongressClient(env_path=env_path, retries=1)

        self.assertEqual(
            client._pool._per_window,
            {
                "congress_api_keys_1__slot1": (9500, 3600),
                "congress_api_keys_1__slot2": (9500, 3600),
                "congress_api_keys_2__slot1": (9500, 3600),
                "congress_api_keys_2__slot2": (9500, 3600),
            },
        )

    def test_key_shard_rejects_empty_shards(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            env_path = Path(tmpdir) / ".env"
            env_path.write_text("CONGRESS_API_KEYS=a\n", encoding="utf-8")
            with patch.dict("os.environ", {"BICAM_CONGRESS_KEY_SHARD": "1/2"}, clear=False):
                with self.assertRaisesRegex(ValueError, "selected no configured keys"):
                    CongressClient(env_path=env_path, retries=2)

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

    def test_key_wait_sleeps_until_exhausted_window_ends(self) -> None:
        pool = CooldownPool()

        with patch(
            "bicam_collection.client.time.sleep",
            side_effect=AssertionError("stop after first wait"),
        ) as sleep, self.assertRaises(AssertionError):
            _take_key_with_wait(pool, "bills", timeout_seconds=None)

        self.assertGreaterEqual(sleep.call_args.args[0], 59.0)


if __name__ == "__main__":
    unittest.main()
