#!/usr/bin/env python3
"""Measure the crawler's bounded child-resource concurrency without network I/O.

The benchmark deliberately uses the real :class:`AsyncSpecCrawler`, URL
canonicalization, and Rotisserie-backed client with a deterministic in-memory
transport/store. That makes the result useful for comparing crawler/Rotisserie
revisions without spending API quota or depending on source availability.
"""

from __future__ import annotations

import argparse
import asyncio
import importlib.metadata
import json
import os
from pathlib import Path
import subprocess
import threading
import time

from bicam_collection.client import AsyncCongressClient, clear_shared_key_pools
from bicam_collection.crawler import AsyncSpecCrawler, CrawlOptions
from bicam_collection.storage import StoredNode, StoredResponseRef
from bicam_collection.spec_loader import CrawlSpec, EdgeSpec, NodeSpec, SeedSpec


class BenchmarkStore:
    def __init__(self) -> None:
        self.stored: dict[str, StoredNode] = {}
        self.expected_fetches = 0
        self.fetched = 0

    async def start_run(self, category: str, mode: str, high_water_mark=None) -> str:
        return "benchmark-run"

    async def get_latest_high_water_mark(self, category: str):
        return None

    async def finish_run(self, run_id: str, status: str, items_fetched: int, high_water_mark=None) -> None:
        self.fetched = items_fetched

    async def get_stored_nodes(self, endpoint_urls: list[str]) -> dict[str, StoredNode]:
        return {url: self.stored[url] for url in endpoint_urls if url in self.stored}

    async def list_refresh_nodes(self, **kwargs) -> list[StoredNode]:
        return []

    async def upsert_response(self, **kwargs) -> StoredResponseRef:
        raw_id = f"benchmark-raw-{len(self.stored) + 1}"
        self.stored[kwargs["endpoint_url"]] = StoredNode(raw_id, kwargs["endpoint_url"], kwargs["response"])
        return StoredResponseRef(raw_id, f"{raw_id}-version", "hash")

    async def record_expected_fetch_batch(self, records: list[dict]) -> None:
        self.expected_fetches += len(records)

    async def record_fetch(self, **kwargs) -> None:
        return None

    async def record_fetch_batch(self, records: list[dict]) -> None:
        return None

    async def record_failed_request(self, **kwargs) -> None:
        raise RuntimeError(f"benchmark request failed: {kwargs}")


class BenchmarkResponse:
    status_code = 200
    headers: dict[str, str] = {}

    def __init__(self, payload: dict) -> None:
        self._payload = payload

    def json(self) -> dict:
        return self._payload

    def raise_for_status(self) -> None:
        return None

    def close(self) -> None:
        return None


class BenchmarkSession:
    """Thread-safe fake transport used by the real Rotisserie-backed client."""

    def __init__(self, children: int, delay_seconds: float) -> None:
        self.children = children
        self.delay_seconds = delay_seconds
        self.active = 0
        self.max_active = 0
        self.calls: list[str] = []
        self._lock = threading.Lock()

    def get(self, url: str, **kwargs) -> BenchmarkResponse:
        with self._lock:
            self.active += 1
            self.max_active = max(self.max_active, self.active)
            self.calls.append(url)
        try:
            time.sleep(self.delay_seconds)
            if "/root" in url:
                return BenchmarkResponse({
                    "items": [
                        {"url": f"https://example.test/child/{index}"}
                        for index in range(self.children)
                    ]
                })
            return BenchmarkResponse({})
        finally:
            with self._lock:
                self.active -= 1


class RotisserieBenchmarkClient:
    def __init__(self, children: int, concurrency: int, delay_seconds: float) -> None:
        # This is process-local and never sends a request.  Two extra keys
        # offset the client's normal reserve, keeping the transport benchmark
        # focused on crawler concurrency rather than API-key starvation.
        os.environ["CONGRESS_API_KEYS"] = ",".join(
            f"benchmark-token-{index}" for index in range(max(3, concurrency + 2))
        )
        clear_shared_key_pools()
        self._async_client = AsyncCongressClient(use_shared_pool=False, retries=1)
        self.session = BenchmarkSession(children, delay_seconds)
        self._async_client._sync_client._session = self.session

    async def get_json(self, url: str, **kwargs):
        return await self._async_client.get_json(url, **kwargs)


def _benchmark_spec() -> CrawlSpec:
    return CrawlSpec(
        category="benchmark",
        source="congress",
        url_allowlist_hosts=["example.test"],
        seed=SeedSpec(path="/root", node_type="root"),
        refresh_root_node_type=None,
        nodes={
            "root": NodeSpec(
                name="root",
                child_edges=[
                    EdgeSpec(target="child", url_path="$.items[*].url", follow_mode="child")
                ],
            ),
            "child": NodeSpec(name="child"),
        },
    )


def _git_state(path: Path) -> dict[str, str | bool | None]:
    if not (path / ".git").exists():
        return {"path": str(path), "commit": None, "dirty": None}
    commit_result = subprocess.run(
        ["git", "-C", str(path), "rev-parse", "HEAD"],
        check=False,
        capture_output=True,
        text=True,
    )
    status_result = subprocess.run(
        ["git", "-C", str(path), "status", "--porcelain"],
        check=False,
        capture_output=True,
        text=True,
    )
    return {
        "path": str(path),
        "commit": commit_result.stdout.strip() if commit_result.returncode == 0 else None,
        "dirty": bool(status_result.stdout.strip()) if status_result.returncode == 0 else None,
    }


async def _run(*, children: int, concurrency: int, delay_seconds: float) -> dict:
    client = RotisserieBenchmarkClient(children, concurrency, delay_seconds)
    store = BenchmarkStore()
    crawler = AsyncSpecCrawler(
        base_url="https://example.test",
        spec=_benchmark_spec(),
        client=client,
        store=store,
        concurrency=concurrency,
    )
    started = time.perf_counter()
    run_id = await crawler.run(CrawlOptions(mode="full"))
    elapsed = time.perf_counter() - started
    fetched = store.fetched
    return {
        "run_id": run_id,
        "children": children,
        "concurrency": concurrency,
        "delay_ms": round(delay_seconds * 1000, 3),
        "expected_fetches": store.expected_fetches,
        "fetched": fetched,
        "max_active": client.session.max_active,
        "elapsed_seconds": round(elapsed, 6),
        "urls_per_second": round(fetched / elapsed, 3) if elapsed else None,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--children", type=int, default=32, help="synthetic child URLs (default: 32)")
    parser.add_argument("--concurrency", type=int, default=4, help="crawler concurrency cap (default: 4)")
    parser.add_argument("--delay-ms", type=float, default=20.0, help="deterministic per-request delay (default: 20)")
    args = parser.parse_args()
    if args.children < 1 or args.concurrency < 1 or args.delay_ms < 0:
        parser.error("children and concurrency must be positive; delay-ms cannot be negative")

    import rotisserie

    package_root = Path(rotisserie.__file__).resolve().parents[2]
    result = asyncio.run(
        _run(
            children=args.children,
            concurrency=args.concurrency,
            delay_seconds=args.delay_ms / 1000,
        )
    )
    result["rotisserie"] = {
        "version": importlib.metadata.version("rotisserie"),
        **_git_state(package_root),
    }
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
