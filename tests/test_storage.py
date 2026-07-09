from __future__ import annotations

import asyncio
import os
import unittest
import uuid
from datetime import datetime, timezone
from pathlib import Path

import psycopg

from bicam_collection.config import Settings
from bicam_collection.storage import AsyncPostgresStore


PROJECT_ROOT = Path(__file__).resolve().parents[1]


@unittest.skipUnless((PROJECT_ROOT / ".env").exists(), "requires local .env-backed Postgres")
class AsyncPostgresStoreTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        os.environ.pop("DATABASE_URL", None)
        self.settings = Settings.from_env(PROJECT_ROOT)
        self.store = AsyncPostgresStore(self.settings.database_url)
        self.test_suffix = uuid.uuid4().hex
        self.test_category = f"storage-test-{self.test_suffix}"
        self.test_urls: list[str] = []
        self.run_ids: list[str] = []

    async def asyncTearDown(self) -> None:
        with psycopg.connect(self.settings.database_url) as conn:
            if self.test_urls:
                conn.execute("DELETE FROM crawl_run_fetches WHERE endpoint_url = ANY(%s)", (self.test_urls,))
                conn.execute("DELETE FROM failed_requests WHERE endpoint_url = ANY(%s)", (self.test_urls,))
                conn.execute("DELETE FROM raw_response_versions WHERE endpoint_url = ANY(%s)", (self.test_urls,))
                conn.execute("DELETE FROM raw_responses WHERE endpoint_url = ANY(%s)", (self.test_urls,))
            if self.run_ids:
                conn.execute("DELETE FROM crawl_runs WHERE id = ANY(%s::uuid[])", (self.run_ids,))
            conn.commit()
        await self.store.close()

    async def test_async_store_run_lifecycle_and_response_queries(self) -> None:
        high_water_mark = datetime(2026, 4, 6, 12, 0, tzinfo=timezone.utc)
        run_id = await self.store.start_run(self.test_category, "full", high_water_mark=high_water_mark)
        self.run_ids.append(run_id)

        self.assertEqual(await self.store.get_latest_high_water_mark(self.test_category), None)

        url = f"https://example.test/raw/{self.test_suffix}"
        self.test_urls.append(url)
        raw_id = await self.store.upsert_response(
            endpoint_url=url,
            source="congress",
            category=self.test_category,
            node_type="detail",
            parent_id=None,
            response={"id": self.test_suffix, "updatedAt": "2026-04-06T12:00:00Z"},
            latest_crawl_run_id=run_id,
        )

        self.assertTrue(await self.store.has_response(url))
        refresh_nodes = await self.store.list_refresh_nodes(category=self.test_category, node_type="detail", limit=10)
        self.assertEqual(len(refresh_nodes), 1)
        self.assertEqual(refresh_nodes[0].raw_id, raw_id)
        self.assertEqual(refresh_nodes[0].endpoint_url, url)

        await self.store.record_fetch(
            crawl_run_id=run_id,
            endpoint_url=url,
            raw_response_id=raw_id,
            parent_endpoint_url=None,
            node_type="detail",
            follow_mode="recurse",
            http_status=200,
            outcome="fetched",
            duration_ms=12,
            attempt_count=1,
            rate_limit_attempts=0,
            api_key_name="alpha",
            pool_allocation={"endpoint_name": "members", "selected_key_name": "alpha"},
        )
        await self.store.record_failed_request(
            crawl_run_id=run_id,
            category=self.test_category,
            node_type="detail",
            endpoint_url=url,
            parent_endpoint_url=None,
            http_status=500,
            error="boom",
            duration_ms=25,
            attempt_count=2,
            rate_limit_attempts=1,
            api_key_name="beta",
            pool_allocation={"endpoint_name": "members", "selected_key_name": "beta"},
        )
        await self.store.finish_run(run_id, "completed", 1, high_water_mark=high_water_mark)

        self.assertEqual(await self.store.get_latest_high_water_mark(self.test_category), high_water_mark)

        with psycopg.connect(self.settings.database_url) as conn:
            fetch_row = conn.execute(
                """
                SELECT outcome, duration_ms, attempt_count, rate_limit_attempts,
                       api_key_name, pool_allocation, raw_response_version_id is not null
                FROM crawl_run_fetches
                WHERE crawl_run_id = %s
                """,
                (run_id,),
            ).fetchone()
            failed_row = conn.execute(
                """
                SELECT error, duration_ms, attempt_count, rate_limit_attempts, api_key_name, pool_allocation
                FROM failed_requests
                WHERE crawl_run_id = %s
                """,
                (run_id,),
            ).fetchone()
            conn.commit()

        self.assertEqual(fetch_row, ("fetched", 12, 1, 0, "alpha", {"endpoint_name": "members", "selected_key_name": "alpha"}, True))
        self.assertEqual(failed_row, ("boom", 25, 2, 1, "beta", {"endpoint_name": "members", "selected_key_name": "beta"}))


@unittest.skipUnless((PROJECT_ROOT / ".env").exists(), "requires local .env-backed Postgres")
class AsyncPostgresStoreAsyncioRunTests(unittest.TestCase):
    def setUp(self) -> None:
        os.environ.pop("DATABASE_URL", None)
        self.settings = Settings.from_env(PROJECT_ROOT)
        self.test_suffix = uuid.uuid4().hex
        self.test_category = f"storage-test-{self.test_suffix}"
        self.test_url = f"https://example.test/asyncio-run/{self.test_suffix}"
        self.run_id: str | None = None

    def tearDown(self) -> None:
        with psycopg.connect(self.settings.database_url) as conn:
            conn.execute("DELETE FROM crawl_run_fetches WHERE endpoint_url = %s", (self.test_url,))
            conn.execute("DELETE FROM failed_requests WHERE endpoint_url = %s", (self.test_url,))
            conn.execute("DELETE FROM raw_response_versions WHERE endpoint_url = %s", (self.test_url,))
            conn.execute("DELETE FROM raw_responses WHERE endpoint_url = %s", (self.test_url,))
            if self.run_id is not None:
                conn.execute("DELETE FROM crawl_runs WHERE id = %s::uuid", (self.run_id,))
            conn.commit()

    def test_async_store_can_be_driven_via_asyncio_run(self) -> None:
        async def scenario() -> tuple[bool, str]:
            store = AsyncPostgresStore(self.settings.database_url)
            run_id = await store.start_run(self.test_category, "full")
            await store.upsert_response(
                endpoint_url=self.test_url,
                source="congress",
                category=self.test_category,
                node_type="detail",
                parent_id=None,
                response={"ok": True},
                latest_crawl_run_id=run_id,
            )
            has_response = await store.has_response(self.test_url)
            await store.finish_run(run_id, "completed", 1)
            await store.close()
            return has_response, run_id

        has_response, self.run_id = asyncio.run(scenario())
        self.assertTrue(has_response)
        self.assertIsNotNone(self.run_id)

    def test_async_store_schema_initialization_is_safe_under_concurrency(self) -> None:
        run_ids: list[str] = []
        test_categories = [f"{self.test_category}-a", f"{self.test_category}-b"]

        async def scenario() -> list[str]:
            stores = [AsyncPostgresStore(self.settings.database_url), AsyncPostgresStore(self.settings.database_url)]
            try:
                started_run_ids = await asyncio.gather(*(store.start_run(category, "full") for store, category in zip(stores, test_categories, strict=True)))
                await asyncio.gather(*(store.finish_run(run_id, "completed", 0) for store, run_id in zip(stores, started_run_ids, strict=True)))
                return list(started_run_ids)
            finally:
                await asyncio.gather(*(store.close() for store in stores))

        run_ids = asyncio.run(scenario())
        self.run_id = run_ids[0]

        with psycopg.connect(self.settings.database_url) as conn:
            conn.execute("DELETE FROM crawl_runs WHERE id = ANY(%s::uuid[])", (run_ids,))
            conn.commit()

        self.run_id = None
        self.assertEqual(len(run_ids), 2)
