from __future__ import annotations

import asyncio
import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from psycopg_pool import AsyncConnectionPool, ConnectionPool


@dataclass(slots=True)
class CrawlRunResult:
    run_id: str
    items_fetched: int


@dataclass(slots=True)
class StoredNode:
    raw_id: str
    endpoint_url: str
    response: dict


@dataclass(slots=True, frozen=True)
class StoredResponseRef:
    raw_response_id: str
    raw_response_version_id: str
    response_hash: str


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

    def get_stored_node(self, endpoint_url: str) -> StoredNode | None:
        row = self._execute(
            "SELECT id, endpoint_url, response FROM raw_responses WHERE endpoint_url = %s LIMIT 1",
            (endpoint_url,),
            fetchone=True,
        )
        if row is None:
            return None
        return StoredNode(raw_id=str(row[0]), endpoint_url=row[1], response=row[2])

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
    ) -> StoredResponseRef:
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
        version_id = self._execute(
            """
            INSERT INTO raw_response_versions (
                id, raw_response_id, endpoint_url, source, category, node_type, parent_id,
                response, response_hash, first_crawl_run_id, latest_crawl_run_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s)
            ON CONFLICT (raw_response_id, response_hash) DO UPDATE
            SET last_seen_at = NOW(),
                latest_crawl_run_id = EXCLUDED.latest_crawl_run_id
            RETURNING id
            """,
            (
                str(uuid.uuid4()),
                stored_id,
                endpoint_url,
                source,
                category,
                node_type,
                parent_id,
                response_json,
                response_hash,
                latest_crawl_run_id,
                latest_crawl_run_id,
            ),
            fetchone=True,
        )[0]
        return StoredResponseRef(str(stored_id), str(version_id), response_hash)

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
    ) -> None:
        self._execute(
            """
            INSERT INTO crawl_run_fetches (
                id, crawl_run_id, endpoint_url, raw_response_id, raw_response_version_id,
                parent_endpoint_url, node_type, follow_mode, http_status, outcome,
                duration_ms, attempt_count, rate_limit_attempts, api_key_name, pool_allocation
            )
            VALUES (%s, %s, %s, %s,
                COALESCE(%s, (SELECT id FROM raw_response_versions
                 WHERE raw_response_id = %s AND latest_crawl_run_id = %s
                 ORDER BY last_seen_at DESC LIMIT 1)),
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            """,
            (
                str(uuid.uuid4()),
                crawl_run_id,
                endpoint_url,
                raw_response_id,
                raw_response_version_id,
                raw_response_id,
                crawl_run_id,
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

    async def get_stored_node(self, endpoint_url: str) -> StoredNode | None:
        row = await self._execute(
            "SELECT id, endpoint_url, response FROM raw_responses WHERE endpoint_url = %s LIMIT 1",
            (endpoint_url,),
            fetchone=True,
        )
        if row is None:
            return None
        return StoredNode(raw_id=str(row[0]), endpoint_url=row[1], response=row[2])

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
    ) -> StoredResponseRef:
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
        version_row = await self._execute(
            """
            INSERT INTO raw_response_versions (
                id, raw_response_id, endpoint_url, source, category, node_type, parent_id,
                response, response_hash, first_crawl_run_id, latest_crawl_run_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s)
            ON CONFLICT (raw_response_id, response_hash) DO UPDATE
            SET last_seen_at = NOW(),
                latest_crawl_run_id = EXCLUDED.latest_crawl_run_id
            RETURNING id
            """,
            (
                str(uuid.uuid4()),
                stored_row[0],
                endpoint_url,
                source,
                category,
                node_type,
                parent_id,
                response_json,
                response_hash,
                latest_crawl_run_id,
                latest_crawl_run_id,
            ),
            fetchone=True,
        )
        return StoredResponseRef(str(stored_row[0]), str(version_row[0]), response_hash)

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
    ) -> None:
        await self._execute(
            """
            INSERT INTO crawl_run_fetches (
                id, crawl_run_id, endpoint_url, raw_response_id, raw_response_version_id,
                parent_endpoint_url, node_type, follow_mode, http_status, outcome,
                duration_ms, attempt_count, rate_limit_attempts, api_key_name, pool_allocation
            )
            VALUES (%s, %s, %s, %s,
                COALESCE(%s, (SELECT id FROM raw_response_versions
                 WHERE raw_response_id = %s AND latest_crawl_run_id = %s
                 ORDER BY last_seen_at DESC LIMIT 1)),
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            """,
            (
                str(uuid.uuid4()),
                crawl_run_id,
                endpoint_url,
                raw_response_id,
                raw_response_version_id,
                raw_response_id,
                crawl_run_id,
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
