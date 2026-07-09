from __future__ import annotations

import asyncio
import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from psycopg_pool import AsyncConnectionPool, ConnectionPool


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS crawl_runs (
    id UUID PRIMARY KEY,
    category TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    mode TEXT NOT NULL,
    high_water_mark TIMESTAMPTZ,
    items_fetched INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'running'
);

CREATE TABLE IF NOT EXISTS raw_responses (
    id UUID PRIMARY KEY,
    endpoint_url TEXT NOT NULL UNIQUE,
    source TEXT NOT NULL,
    category TEXT NOT NULL,
    node_type TEXT NOT NULL,
    parent_id UUID REFERENCES raw_responses(id),
    response JSONB NOT NULL,
    response_hash TEXT NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    latest_crawl_run_id UUID REFERENCES crawl_runs(id)
);

CREATE INDEX IF NOT EXISTS idx_raw_responses_category ON raw_responses(source, category);
CREATE INDEX IF NOT EXISTS idx_raw_responses_node_type ON raw_responses(node_type);
CREATE INDEX IF NOT EXISTS idx_raw_responses_parent_id ON raw_responses(parent_id);
CREATE INDEX IF NOT EXISTS idx_raw_responses_latest_run ON raw_responses(latest_crawl_run_id);

CREATE TABLE IF NOT EXISTS crawl_run_fetches (
    id UUID PRIMARY KEY,
    crawl_run_id UUID NOT NULL REFERENCES crawl_runs(id),
    endpoint_url TEXT NOT NULL,
    raw_response_id UUID REFERENCES raw_responses(id),
    parent_endpoint_url TEXT,
    node_type TEXT NOT NULL,
    follow_mode TEXT NOT NULL,
    http_status INTEGER,
    outcome TEXT NOT NULL,
    duration_ms INTEGER,
    attempt_count INTEGER,
    rate_limit_attempts INTEGER,
    api_key_name TEXT,
    pool_allocation JSONB,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_crawl_run_fetches_run_id ON crawl_run_fetches(crawl_run_id);
CREATE INDEX IF NOT EXISTS idx_crawl_run_fetches_endpoint_url ON crawl_run_fetches(endpoint_url);

CREATE TABLE IF NOT EXISTS failed_requests (
    id UUID PRIMARY KEY,
    crawl_run_id UUID NOT NULL REFERENCES crawl_runs(id),
    category TEXT NOT NULL,
    node_type TEXT NOT NULL,
    endpoint_url TEXT NOT NULL,
    parent_endpoint_url TEXT,
    http_status INTEGER,
    error TEXT NOT NULL,
    duration_ms INTEGER,
    attempt_count INTEGER,
    rate_limit_attempts INTEGER,
    api_key_name TEXT,
    pool_allocation JSONB,
    failed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_failed_requests_run_id ON failed_requests(crawl_run_id);
CREATE INDEX IF NOT EXISTS idx_failed_requests_endpoint_url ON failed_requests(endpoint_url);
ALTER TABLE crawl_run_fetches ADD COLUMN IF NOT EXISTS duration_ms INTEGER;
ALTER TABLE crawl_run_fetches ADD COLUMN IF NOT EXISTS attempt_count INTEGER;
ALTER TABLE crawl_run_fetches ADD COLUMN IF NOT EXISTS rate_limit_attempts INTEGER;
ALTER TABLE crawl_run_fetches ADD COLUMN IF NOT EXISTS api_key_name TEXT;
ALTER TABLE crawl_run_fetches ADD COLUMN IF NOT EXISTS pool_allocation JSONB;
ALTER TABLE failed_requests ADD COLUMN IF NOT EXISTS duration_ms INTEGER;
ALTER TABLE failed_requests ADD COLUMN IF NOT EXISTS attempt_count INTEGER;
ALTER TABLE failed_requests ADD COLUMN IF NOT EXISTS rate_limit_attempts INTEGER;
ALTER TABLE failed_requests ADD COLUMN IF NOT EXISTS api_key_name TEXT;
ALTER TABLE failed_requests ADD COLUMN IF NOT EXISTS pool_allocation JSONB;
"""

SCHEMA_LOCK_KEY = 3_117_250_001


@dataclass(slots=True)
class CrawlRunResult:
    run_id: str
    items_fetched: int


@dataclass(slots=True)
class StoredNode:
    raw_id: str
    endpoint_url: str
    response: dict


def _serialize_response(response: dict) -> tuple[str, str]:
    response_json = json.dumps(response, sort_keys=True)
    response_hash = hashlib.md5(response_json.encode("utf-8"), usedforsecurity=False).hexdigest()
    return response_json, response_hash


def _serialize_optional_json(value: dict[str, Any] | None) -> str | None:
    if value is None:
        return None
    return json.dumps(value, sort_keys=True)


class PostgresStore:
    def __init__(self, database_url: str) -> None:
        if not database_url:
            raise ValueError("DATABASE_URL is required")
        self._database_url = database_url
        self._pool = ConnectionPool(conninfo=self._database_url, min_size=1, max_size=10, open=True)
        self._ensure_schema()

    def _execute(
        self,
        query: str,
        params: tuple[Any, ...] = (),
        *,
        fetchone: bool = False,
        fetchall: bool = False,
    ) -> Any:
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                if fetchone:
                    return cur.fetchone()
                if fetchall:
                    return cur.fetchall()
            conn.commit()
        return None

    def _ensure_schema(self) -> None:
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_advisory_lock(%s)", (SCHEMA_LOCK_KEY,))
                try:
                    cur.execute(SCHEMA_SQL)
                finally:
                    cur.execute("SELECT pg_advisory_unlock(%s)", (SCHEMA_LOCK_KEY,))
            conn.commit()

    def start_run(
        self,
        category: str,
        mode: str,
        high_water_mark: datetime | None = None,
    ) -> str:
        run_id = str(uuid.uuid4())
        self._execute(
            """
            INSERT INTO crawl_runs (id, category, mode, high_water_mark, status)
            VALUES (%s, %s, %s, %s, 'running')
            """,
            (run_id, category, mode, high_water_mark),
        )
        return run_id

    def get_latest_high_water_mark(self, category: str) -> datetime | None:
        row = self._execute(
            """
            SELECT high_water_mark
            FROM crawl_runs
            WHERE category = %s
              AND status = 'completed'
              AND high_water_mark IS NOT NULL
            ORDER BY high_water_mark DESC, finished_at DESC
            LIMIT 1
            """,
            (category,),
            fetchone=True,
        )
        if row is None:
            return None
        return row[0]

    def finish_run(
        self,
        run_id: str,
        status: str,
        items_fetched: int,
        high_water_mark: datetime | None = None,
    ) -> None:
        self._execute(
            """
            UPDATE crawl_runs
            SET finished_at = NOW(),
                status = %s,
                items_fetched = %s,
                high_water_mark = COALESCE(%s, high_water_mark)
            WHERE id = %s
            """,
            (status, items_fetched, high_water_mark, run_id),
        )

    def has_response(self, endpoint_url: str) -> bool:
        row = self._execute(
            """
            SELECT 1
            FROM raw_responses
            WHERE endpoint_url = %s
            LIMIT 1
            """,
            (endpoint_url,),
            fetchone=True,
        )
        return row is not None

    def list_refresh_nodes(
        self,
        *,
        category: str,
        node_type: str,
        limit: int,
    ) -> list[StoredNode]:
        rows = self._execute(
            """
            SELECT id, endpoint_url, response
            FROM raw_responses
            WHERE category = %s
              AND node_type = %s
            ORDER BY fetched_at ASC, endpoint_url ASC
            LIMIT %s
            """,
            (category, node_type, limit),
            fetchall=True,
        )
        return [
            StoredNode(
                raw_id=str(row[0]),
                endpoint_url=row[1],
                response=row[2],
            )
            for row in rows
        ]

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
    ) -> str:
        raw_id = str(uuid.uuid4())
        response_json, response_hash = _serialize_response(response)
        stored_id = self._execute(
            """
            INSERT INTO raw_responses (
                id, endpoint_url, source, category, node_type, parent_id,
                response, response_hash, latest_crawl_run_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s)
            ON CONFLICT (endpoint_url) DO UPDATE
            SET source = EXCLUDED.source,
                category = EXCLUDED.category,
                node_type = EXCLUDED.node_type,
                parent_id = EXCLUDED.parent_id,
                response = EXCLUDED.response,
                response_hash = EXCLUDED.response_hash,
                fetched_at = NOW(),
                latest_crawl_run_id = EXCLUDED.latest_crawl_run_id
            RETURNING id
            """,
            (
                raw_id,
                endpoint_url,
                source,
                category,
                node_type,
                parent_id,
                response_json,
                response_hash,
                latest_crawl_run_id,
            ),
            fetchone=True,
        )[0]
        return str(stored_id)

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
    ) -> None:
        self._execute(
            """
            INSERT INTO crawl_run_fetches (
                id, crawl_run_id, endpoint_url, raw_response_id,
                parent_endpoint_url, node_type, follow_mode, http_status, outcome,
                duration_ms, attempt_count, rate_limit_attempts, api_key_name, pool_allocation
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            """,
            (
                str(uuid.uuid4()),
                crawl_run_id,
                endpoint_url,
                raw_response_id,
                parent_endpoint_url,
                node_type,
                follow_mode,
                http_status,
                outcome,
                duration_ms,
                attempt_count,
                rate_limit_attempts,
                api_key_name,
                _serialize_optional_json(pool_allocation),
            ),
        )

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
    ) -> None:
        self._execute(
            """
            INSERT INTO failed_requests (
                id, crawl_run_id, category, node_type, endpoint_url,
                parent_endpoint_url, http_status, error,
                duration_ms, attempt_count, rate_limit_attempts, api_key_name, pool_allocation
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            """,
            (
                str(uuid.uuid4()),
                crawl_run_id,
                category,
                node_type,
                endpoint_url,
                parent_endpoint_url,
                http_status,
                error,
                duration_ms,
                attempt_count,
                rate_limit_attempts,
                api_key_name,
                _serialize_optional_json(pool_allocation),
            ),
        )


class AsyncPostgresStore:
    def __init__(self, database_url: str) -> None:
        if not database_url:
            raise ValueError("DATABASE_URL is required")
        self._database_url = database_url
        self._pool = AsyncConnectionPool(conninfo=self._database_url, min_size=1, max_size=10, open=False)
        self._initialized = False
        self._initializing_lock = None

    async def _ensure_initialized(self) -> None:
        if self._initialized:
            return
        if self._initializing_lock is None:
            self._initializing_lock = asyncio.Lock()
        async with self._initializing_lock:
            if self._initialized:
                return
            await self._pool.open()
            async with self._pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT pg_advisory_lock(%s)", (SCHEMA_LOCK_KEY,))
                    try:
                        await cur.execute(SCHEMA_SQL)
                    finally:
                        await cur.execute("SELECT pg_advisory_unlock(%s)", (SCHEMA_LOCK_KEY,))
                await conn.commit()
            self._initialized = True

    async def _execute(
        self,
        query: str,
        params: tuple[Any, ...] = (),
        *,
        fetchone: bool = False,
        fetchall: bool = False,
    ) -> Any:
        await self._ensure_initialized()
        async with self._pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, params)
                if fetchone:
                    return await cur.fetchone()
                if fetchall:
                    return await cur.fetchall()
            await conn.commit()
        return None

    async def close(self) -> None:
        await self._pool.close()
        self._initialized = False

    async def cleanup_stale_runs(self, *, categories: list[str], stale_before: datetime) -> int:
        row = await self._execute(
            """
            UPDATE crawl_runs
            SET finished_at = NOW(),
                status = 'failed'
            WHERE status = 'running'
              AND category = ANY(%s)
              AND started_at < %s
            RETURNING id
            """,
            (categories, stale_before),
            fetchall=True,
        )
        return len(row or [])

    async def start_run(
        self,
        category: str,
        mode: str,
        high_water_mark: datetime | None = None,
    ) -> str:
        run_id = str(uuid.uuid4())
        await self._execute(
            """
            INSERT INTO crawl_runs (id, category, mode, high_water_mark, status)
            VALUES (%s, %s, %s, %s, 'running')
            """,
            (run_id, category, mode, high_water_mark),
        )
        return run_id

    async def get_latest_high_water_mark(self, category: str) -> datetime | None:
        row = await self._execute(
            """
            SELECT high_water_mark
            FROM crawl_runs
            WHERE category = %s
              AND status = 'completed'
              AND high_water_mark IS NOT NULL
            ORDER BY high_water_mark DESC, finished_at DESC
            LIMIT 1
            """,
            (category,),
            fetchone=True,
        )
        if row is None:
            return None
        return row[0]

    async def finish_run(
        self,
        run_id: str,
        status: str,
        items_fetched: int,
        high_water_mark: datetime | None = None,
    ) -> None:
        await self._execute(
            """
            UPDATE crawl_runs
            SET finished_at = NOW(),
                status = %s,
                items_fetched = %s,
                high_water_mark = COALESCE(%s, high_water_mark)
            WHERE id = %s
            """,
            (status, items_fetched, high_water_mark, run_id),
        )

    async def has_response(self, endpoint_url: str) -> bool:
        row = await self._execute(
            """
            SELECT 1
            FROM raw_responses
            WHERE endpoint_url = %s
            LIMIT 1
            """,
            (endpoint_url,),
            fetchone=True,
        )
        return row is not None

    async def list_refresh_nodes(
        self,
        *,
        category: str,
        node_type: str,
        limit: int,
    ) -> list[StoredNode]:
        rows = await self._execute(
            """
            SELECT id, endpoint_url, response
            FROM raw_responses
            WHERE category = %s
              AND node_type = %s
            ORDER BY fetched_at ASC, endpoint_url ASC
            LIMIT %s
            """,
            (category, node_type, limit),
            fetchall=True,
        )
        return [
            StoredNode(
                raw_id=str(row[0]),
                endpoint_url=row[1],
                response=row[2],
            )
            for row in rows
        ]

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
    ) -> str:
        raw_id = str(uuid.uuid4())
        response_json, response_hash = _serialize_response(response)
        stored_row = await self._execute(
            """
            INSERT INTO raw_responses (
                id, endpoint_url, source, category, node_type, parent_id,
                response, response_hash, latest_crawl_run_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s)
            ON CONFLICT (endpoint_url) DO UPDATE
            SET source = EXCLUDED.source,
                category = EXCLUDED.category,
                node_type = EXCLUDED.node_type,
                parent_id = EXCLUDED.parent_id,
                response = EXCLUDED.response,
                response_hash = EXCLUDED.response_hash,
                fetched_at = NOW(),
                latest_crawl_run_id = EXCLUDED.latest_crawl_run_id
            RETURNING id
            """,
            (
                raw_id,
                endpoint_url,
                source,
                category,
                node_type,
                parent_id,
                response_json,
                response_hash,
                latest_crawl_run_id,
            ),
            fetchone=True,
        )
        return str(stored_row[0])

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
    ) -> None:
        await self._execute(
            """
            INSERT INTO crawl_run_fetches (
                id, crawl_run_id, endpoint_url, raw_response_id,
                parent_endpoint_url, node_type, follow_mode, http_status, outcome,
                duration_ms, attempt_count, rate_limit_attempts, api_key_name, pool_allocation
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            """,
            (
                str(uuid.uuid4()),
                crawl_run_id,
                endpoint_url,
                raw_response_id,
                parent_endpoint_url,
                node_type,
                follow_mode,
                http_status,
                outcome,
                duration_ms,
                attempt_count,
                rate_limit_attempts,
                api_key_name,
                _serialize_optional_json(pool_allocation),
            ),
        )

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
    ) -> None:
        await self._execute(
            """
            INSERT INTO failed_requests (
                id, crawl_run_id, category, node_type, endpoint_url,
                parent_endpoint_url, http_status, error,
                duration_ms, attempt_count, rate_limit_attempts, api_key_name, pool_allocation
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            """,
            (
                str(uuid.uuid4()),
                crawl_run_id,
                category,
                node_type,
                endpoint_url,
                parent_endpoint_url,
                http_status,
                error,
                duration_ms,
                attempt_count,
                rate_limit_attempts,
                api_key_name,
                _serialize_optional_json(pool_allocation),
            ),
        )
