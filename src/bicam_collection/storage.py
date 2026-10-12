from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from psycopg_pool import AsyncConnectionPool, ConnectionPool


_SENSITIVE_QUERY_PARAMETER_RE = re.compile(r"([?&](?:api_key|apikey|access_token|token)=)[^&\s]+", flags=re.IGNORECASE)


@dataclass(slots=True)
class StoredNode:
    raw_id: str
    endpoint_url: str
    response: dict
    raw_response_version_id: str | None = None


@dataclass(slots=True, frozen=True)
class StoredResponseRef:
    raw_response_id: str
    raw_response_version_id: str
    response_hash: str


_START_RUN_SQL = """
    INSERT INTO crawl_runs (id, category, mode, high_water_mark, status)
    VALUES (%s, %s, %s, %s, 'running')
"""

_LATEST_HIGH_WATER_MARK_SQL = """
    SELECT high_water_mark
    FROM crawl_runs
    WHERE category = %s AND status = 'completed' AND high_water_mark IS NOT NULL
    ORDER BY high_water_mark DESC, finished_at DESC
    LIMIT 1
"""

_FINISH_RUN_SQL = """
    UPDATE crawl_runs
    SET finished_at = NOW(), status = %s, items_fetched = %s, high_water_mark = COALESCE(%s, high_water_mark)
    WHERE id = %s
"""

_HAS_RESPONSE_SQL = "SELECT 1 FROM raw_responses WHERE endpoint_url = %s LIMIT 1"

# Each stored node carries the id of the version matching its current payload.
_STORED_NODE_SELECT = """
    SELECT raw.id, raw.endpoint_url, raw.response, versions.id
    FROM raw_responses AS raw
    LEFT JOIN raw_response_versions AS versions
      ON versions.raw_response_id = raw.id AND versions.response_hash = raw.response_hash
"""
_STORED_NODE_SQL = _STORED_NODE_SELECT + "WHERE raw.endpoint_url = %s LIMIT 1"
_STORED_NODES_SQL = _STORED_NODE_SELECT + "WHERE raw.endpoint_url = ANY(%s)"
_REFRESH_NODES_SQL = _STORED_NODE_SELECT + """
    WHERE raw.category = %s AND raw.node_type = %s AND raw.endpoint_url > COALESCE(%s, '')
    ORDER BY raw.endpoint_url
    LIMIT %s
"""

# raw_responses holds the latest payload per URL; raw_response_versions keeps
# one row per distinct payload, so an unchanged refetch only bumps last_seen_at.
_UPSERT_RESPONSE_SQL = """
    WITH stored_response AS (
        INSERT INTO raw_responses (
            id, endpoint_url, source, category, node_type, parent_id,
            response, response_hash, latest_crawl_run_id
        )
        VALUES (%(raw_id)s, %(endpoint_url)s, %(source)s, %(category)s, %(node_type)s, %(parent_id)s,
                %(response)s::jsonb, %(response_hash)s, %(crawl_run_id)s)
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
    )
    INSERT INTO raw_response_versions (
        id, raw_response_id, endpoint_url, source, category, node_type, parent_id,
        response, response_hash, first_crawl_run_id, latest_crawl_run_id
    )
    SELECT %(version_id)s, stored_response.id, %(endpoint_url)s, %(source)s, %(category)s, %(node_type)s, %(parent_id)s,
           %(response)s::jsonb, %(response_hash)s, %(crawl_run_id)s, %(crawl_run_id)s
    FROM stored_response
    ON CONFLICT (raw_response_id, response_hash) DO UPDATE
    SET last_seen_at = NOW(), latest_crawl_run_id = EXCLUDED.latest_crawl_run_id
    RETURNING raw_response_id, id
"""

_RECORD_FETCH_SQL = """
    INSERT INTO crawl_run_fetches (
        id, crawl_run_id, endpoint_url, raw_response_id, raw_response_version_id,
        parent_endpoint_url, node_type, follow_mode, http_status, outcome,
        duration_ms, attempt_count, rate_limit_attempts, api_key_name, pool_allocation
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
"""

_RECORD_EXPECTED_FETCH_SQL = """
    INSERT INTO crawl_run_expected_fetches (id, crawl_run_id, endpoint_url, parent_endpoint_url, node_type, follow_mode)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (crawl_run_id, endpoint_url, node_type) DO NOTHING
"""

_RECORD_FAILED_REQUEST_SQL = """
    INSERT INTO failed_requests (
        id, crawl_run_id, category, node_type, endpoint_url,
        parent_endpoint_url, http_status, error,
        duration_ms, attempt_count, rate_limit_attempts, api_key_name, pool_allocation
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
"""

_CLEANUP_STALE_RUNS_SQL = """
    UPDATE crawl_runs
    SET finished_at = NOW(), status = 'failed'
    WHERE status = 'running' AND category = ANY(%s) AND started_at < %s
    RETURNING id
"""


def _optional_json(value: dict[str, Any] | None) -> str | None:
    return None if value is None else json.dumps(value, sort_keys=True)


def _redact_error(error: str) -> str:
    """Remove credentials that HTTP client errors may echo in request URLs."""
    return _SENSITIVE_QUERY_PARAMETER_RE.sub(r"\1[REDACTED]", error)


def _stored_node(row: tuple[Any, ...]) -> StoredNode:
    return StoredNode(
        raw_id=str(row[0]),
        endpoint_url=row[1],
        response=row[2],
        raw_response_version_id=None if row[3] is None else str(row[3]),
    )


def _upsert_params(
    *,
    endpoint_url: str,
    source: str,
    category: str,
    node_type: str,
    parent_id: str | None,
    response: dict,
    latest_crawl_run_id: str,
) -> dict[str, Any]:
    response_json = json.dumps(response, sort_keys=True)
    return {
        "raw_id": str(uuid.uuid4()),
        "version_id": str(uuid.uuid4()),
        "endpoint_url": endpoint_url,
        "source": source,
        "category": category,
        "node_type": node_type,
        "parent_id": parent_id,
        "response": response_json,
        "response_hash": hashlib.md5(response_json.encode("utf-8"), usedforsecurity=False).hexdigest(),
        "crawl_run_id": latest_crawl_run_id,
    }


def _fetch_params(
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
) -> tuple[Any, ...]:
    if raw_response_id is not None and raw_response_version_id is None:
        raise ValueError(f"raw_response_version_id is required when raw_response_id is set (outcome={outcome!r})")
    return (
        str(uuid.uuid4()), crawl_run_id, endpoint_url, raw_response_id, raw_response_version_id,
        parent_endpoint_url, node_type, follow_mode, http_status, outcome,
        duration_ms, attempt_count, rate_limit_attempts, api_key_name, _optional_json(pool_allocation),
    )


def _expected_fetch_params(
    *, crawl_run_id: str, endpoint_url: str, parent_endpoint_url: str | None, node_type: str, follow_mode: str
) -> tuple[Any, ...]:
    return (str(uuid.uuid4()), crawl_run_id, endpoint_url, parent_endpoint_url, node_type, follow_mode)


def _failed_request_params(
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
) -> tuple[Any, ...]:
    return (
        str(uuid.uuid4()), crawl_run_id, category, node_type, endpoint_url,
        parent_endpoint_url, http_status, _redact_error(error),
        duration_ms, attempt_count, rate_limit_attempts, api_key_name, _optional_json(pool_allocation),
    )


class PostgresStore:
    def __init__(self, database_url: str) -> None:
        self._pool = ConnectionPool(conninfo=database_url, min_size=1, max_size=10, open=True)

    def _execute(self, query: str, params: Any = (), *, fetchone: bool = False, fetchall: bool = False) -> Any:
        result: Any = None
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                if fetchone:
                    result = cur.fetchone()
                elif fetchall:
                    result = cur.fetchall()
            conn.commit()
        return result

    def start_run(self, category: str, mode: str, high_water_mark: datetime | None = None) -> str:
        run_id = str(uuid.uuid4())
        self._execute(_START_RUN_SQL, (run_id, category, mode, high_water_mark))
        return run_id

    def get_latest_high_water_mark(self, category: str) -> datetime | None:
        row = self._execute(_LATEST_HIGH_WATER_MARK_SQL, (category,), fetchone=True)
        return None if row is None else row[0]

    def finish_run(self, run_id: str, status: str, items_fetched: int, high_water_mark: datetime | None = None) -> None:
        self._execute(_FINISH_RUN_SQL, (status, items_fetched, high_water_mark, run_id))

    def has_response(self, endpoint_url: str) -> bool:
        return self._execute(_HAS_RESPONSE_SQL, (endpoint_url,), fetchone=True) is not None

    def get_stored_node(self, endpoint_url: str) -> StoredNode | None:
        row = self._execute(_STORED_NODE_SQL, (endpoint_url,), fetchone=True)
        return None if row is None else _stored_node(row)

    def list_refresh_nodes(
        self, *, category: str, node_type: str, limit: int, after_endpoint_url: str | None = None
    ) -> list[StoredNode]:
        rows = self._execute(_REFRESH_NODES_SQL, (category, node_type, after_endpoint_url, limit), fetchall=True)
        return [_stored_node(row) for row in rows]

    def upsert_response(self, **kwargs: Any) -> StoredResponseRef:
        params = _upsert_params(**kwargs)
        row = self._execute(_UPSERT_RESPONSE_SQL, params, fetchone=True)
        return StoredResponseRef(str(row[0]), str(row[1]), params["response_hash"])

    def record_fetch(self, **kwargs: Any) -> None:
        self._execute(_RECORD_FETCH_SQL, _fetch_params(**kwargs))

    def record_expected_fetch(self, **kwargs: Any) -> None:
        self._execute(_RECORD_EXPECTED_FETCH_SQL, _expected_fetch_params(**kwargs))

    def record_failed_request(self, **kwargs: Any) -> None:
        self._execute(_RECORD_FAILED_REQUEST_SQL, _failed_request_params(**kwargs))


class AsyncPostgresStore:
    def __init__(self, database_url: str) -> None:
        # Crawls run several processes at once against a modest
        # max_connections, so each process keeps its pool small.
        self._pool = AsyncConnectionPool(
            conninfo=database_url,
            min_size=1,
            max_size=max(1, int(os.getenv("BICAM_DB_POOL_MAX_SIZE", "2"))),
            open=False,
        )
        self._opened = False
        self._open_lock = asyncio.Lock()

    async def _connection_pool(self) -> AsyncConnectionPool:
        if not self._opened:
            async with self._open_lock:
                if not self._opened:
                    await self._pool.open()
                    self._opened = True
        return self._pool

    async def _execute(self, query: str, params: Any = (), *, fetchone: bool = False, fetchall: bool = False) -> Any:
        pool = await self._connection_pool()
        result: Any = None
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, params)
                if fetchone:
                    result = await cur.fetchone()
                elif fetchall:
                    result = await cur.fetchall()
            await conn.commit()
        return result

    async def _execute_many(self, query: str, params: list[tuple[Any, ...]]) -> None:
        if not params:
            return
        pool = await self._connection_pool()
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.executemany(query, params)
            await conn.commit()

    async def close(self) -> None:
        await self._pool.close()
        self._opened = False

    async def cleanup_stale_runs(self, *, categories: list[str], stale_before: datetime) -> int:
        rows = await self._execute(_CLEANUP_STALE_RUNS_SQL, (categories, stale_before), fetchall=True)
        return len(rows)

    async def start_run(self, category: str, mode: str, high_water_mark: datetime | None = None) -> str:
        run_id = str(uuid.uuid4())
        await self._execute(_START_RUN_SQL, (run_id, category, mode, high_water_mark))
        return run_id

    async def get_latest_high_water_mark(self, category: str) -> datetime | None:
        row = await self._execute(_LATEST_HIGH_WATER_MARK_SQL, (category,), fetchone=True)
        return None if row is None else row[0]

    async def finish_run(self, run_id: str, status: str, items_fetched: int, high_water_mark: datetime | None = None) -> None:
        await self._execute(_FINISH_RUN_SQL, (status, items_fetched, high_water_mark, run_id))

    async def has_response(self, endpoint_url: str) -> bool:
        return await self._execute(_HAS_RESPONSE_SQL, (endpoint_url,), fetchone=True) is not None

    async def get_stored_node(self, endpoint_url: str) -> StoredNode | None:
        row = await self._execute(_STORED_NODE_SQL, (endpoint_url,), fetchone=True)
        return None if row is None else _stored_node(row)

    async def get_stored_nodes(self, endpoint_urls: list[str]) -> dict[str, StoredNode]:
        if not endpoint_urls:
            return {}
        rows = await self._execute(_STORED_NODES_SQL, (endpoint_urls,), fetchall=True)
        return {row[1]: _stored_node(row) for row in rows}

    async def list_refresh_nodes(
        self, *, category: str, node_type: str, limit: int, after_endpoint_url: str | None = None
    ) -> list[StoredNode]:
        rows = await self._execute(_REFRESH_NODES_SQL, (category, node_type, after_endpoint_url, limit), fetchall=True)
        return [_stored_node(row) for row in rows]

    async def upsert_response(self, **kwargs: Any) -> StoredResponseRef:
        params = _upsert_params(**kwargs)
        row = await self._execute(_UPSERT_RESPONSE_SQL, params, fetchone=True)
        return StoredResponseRef(str(row[0]), str(row[1]), params["response_hash"])

    async def record_fetch(self, **kwargs: Any) -> None:
        await self._execute(_RECORD_FETCH_SQL, _fetch_params(**kwargs))

    async def record_fetch_batch(self, records: list[dict[str, Any]]) -> None:
        await self._execute_many(_RECORD_FETCH_SQL, [_fetch_params(**record) for record in records])

    async def record_expected_fetch(self, **kwargs: Any) -> None:
        await self._execute(_RECORD_EXPECTED_FETCH_SQL, _expected_fetch_params(**kwargs))

    async def record_expected_fetch_batch(self, records: list[dict[str, Any]]) -> None:
        await self._execute_many(_RECORD_EXPECTED_FETCH_SQL, [_expected_fetch_params(**record) for record in records])

    async def record_failed_request(self, **kwargs: Any) -> None:
        await self._execute(_RECORD_FAILED_REQUEST_SQL, _failed_request_params(**kwargs))
