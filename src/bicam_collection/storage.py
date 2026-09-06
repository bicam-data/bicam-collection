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


_SENSITIVE_QUERY_PARAMETER_RE = re.compile(
    r"([?&](?:api_key|apikey|access_token|token)=)[^&\s]+",
    flags=re.IGNORECASE,
)


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


def _serialize_response(response: dict) -> tuple[str, str]:
    response_json = json.dumps(response, sort_keys=True)
    response_hash = hashlib.md5(response_json.encode("utf-8"), usedforsecurity=False).hexdigest()
    return response_json, response_hash


def _serialize_optional_json(value: dict[str, Any] | None) -> str | None:
    if value is None:
        return None
    return json.dumps(value, sort_keys=True)


def _redact_error(error: str | None) -> str | None:
    """Remove credentials that HTTP client errors may echo in request URLs."""

    if error is None:
        return None
    return _SENSITIVE_QUERY_PARAMETER_RE.sub(r"\1[REDACTED]", error)


_UPSERT_RESPONSE_SQL = """
    WITH stored_response AS (
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
    )
    INSERT INTO raw_response_versions (
        id, raw_response_id, endpoint_url, source, category, node_type, parent_id,
        response, response_hash, first_crawl_run_id, latest_crawl_run_id
    )
    SELECT %s, stored_response.id, %s, %s, %s, %s, %s,
           %s::jsonb, %s, %s, %s
    FROM stored_response
    ON CONFLICT (raw_response_id, response_hash) DO UPDATE
    SET last_seen_at = NOW(),
        latest_crawl_run_id = EXCLUDED.latest_crawl_run_id
    RETURNING raw_response_id, id
"""


def _upsert_response_params(
    *,
    endpoint_url: str,
    source: str,
    category: str,
    node_type: str,
    parent_id: str | None,
    response_json: str,
    response_hash: str,
    latest_crawl_run_id: str,
) -> tuple[Any, ...]:
    return (
        str(uuid.uuid4()),
        endpoint_url,
        source,
        category,
        node_type,
        parent_id,
        response_json,
        response_hash,
        latest_crawl_run_id,
        str(uuid.uuid4()),
        endpoint_url,
        source,
        category,
        node_type,
        parent_id,
        response_json,
        response_hash,
        latest_crawl_run_id,
        latest_crawl_run_id,
    )


def _validate_fetch_version_reference(
    *,
    raw_response_id: str | None,
    raw_response_version_id: str | None,
    outcome: str,
) -> None:
    if raw_response_id is not None and raw_response_version_id is None:
        raise ValueError(
            "raw_response_version_id is required when raw_response_id is set "
            f"(outcome={outcome!r})"
        )


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
            """
            SELECT raw.id, raw.endpoint_url, raw.response, versions.id
            FROM raw_responses AS raw
            LEFT JOIN raw_response_versions AS versions
              ON versions.raw_response_id = raw.id
             AND versions.response_hash = raw.response_hash
            WHERE raw.endpoint_url = %s
            LIMIT 1
            """,
            (endpoint_url,),
            fetchone=True,
        )
        if row is None:
            return None
        return StoredNode(
            raw_id=str(row[0]),
            endpoint_url=row[1],
            response=row[2],
            raw_response_version_id=str(row[3]) if row[3] is not None else None,
        )

    def list_refresh_nodes(
        self,
        *,
        category: str,
        node_type: str,
        limit: int,
        after_endpoint_url: str | None = None,
    ) -> list[StoredNode]:
        rows = self._execute(
            """
            SELECT raw.id, raw.endpoint_url, raw.response, versions.id
            FROM raw_responses AS raw
            LEFT JOIN raw_response_versions AS versions
              ON versions.raw_response_id = raw.id
             AND versions.response_hash = raw.response_hash
            WHERE raw.category = %s
              AND raw.node_type = %s
              AND raw.endpoint_url > COALESCE(%s, '')
            ORDER BY raw.endpoint_url ASC
            LIMIT %s
            """,
            (category, node_type, after_endpoint_url, limit),
            fetchall=True,
        )
        return [
            StoredNode(
                raw_id=str(row[0]),
                endpoint_url=row[1],
                response=row[2],
                raw_response_version_id=str(row[3]) if row[3] is not None else None,
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
        response_json, response_hash = _serialize_response(response)
        stored_row = self._execute(
            _UPSERT_RESPONSE_SQL,
            _upsert_response_params(
                endpoint_url=endpoint_url,
                source=source,
                category=category,
                node_type=node_type,
                parent_id=parent_id,
                response_json=response_json,
                response_hash=response_hash,
                latest_crawl_run_id=latest_crawl_run_id,
            ),
            fetchone=True,
        )
        return StoredResponseRef(str(stored_row[0]), str(stored_row[1]), response_hash)

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
        _validate_fetch_version_reference(
            raw_response_id=raw_response_id,
            raw_response_version_id=raw_response_version_id,
            outcome=outcome,
        )
        self._execute(
            """
            INSERT INTO crawl_run_fetches (
                id, crawl_run_id, endpoint_url, raw_response_id, raw_response_version_id,
                parent_endpoint_url, node_type, follow_mode, http_status, outcome,
                duration_ms, attempt_count, rate_limit_attempts, api_key_name, pool_allocation
            )
            VALUES (%s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            """,
            (
                str(uuid.uuid4()),
                crawl_run_id,
                endpoint_url,
                raw_response_id,
                raw_response_version_id,
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

    def record_expected_fetch(
        self,
        *,
        crawl_run_id: str,
        endpoint_url: str,
        parent_endpoint_url: str | None,
        node_type: str,
        follow_mode: str,
    ) -> None:
        self._execute(
            """
            INSERT INTO crawl_run_expected_fetches (
                id, crawl_run_id, endpoint_url, parent_endpoint_url,
                node_type, follow_mode
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (crawl_run_id, endpoint_url, node_type) DO NOTHING
            """,
            (
                str(uuid.uuid4()),
                crawl_run_id,
                endpoint_url,
                parent_endpoint_url,
                node_type,
                follow_mode,
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
                _redact_error(error),
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
        # A crawl process can have many async workers, and historical
        # backfills may run several processes at once.  Ten connections per
        # process exhausts the database's modest max_connections quickly.
        # Keep the default small while allowing deployments to tune it
        # explicitly without changing crawler code.
        try:
            pool_max_size = max(1, int(os.getenv("BICAM_DB_POOL_MAX_SIZE", "2")))
        except ValueError:
            pool_max_size = 2
        self._pool = AsyncConnectionPool(
            conninfo=self._database_url,
            min_size=1,
            max_size=pool_max_size,
            open=False,
        )
        self._initialized = False
        self._initializing_lock: asyncio.Lock | None = None

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
        result: Any = None
        async with self._pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, params)
                if fetchone:
                    result = await cur.fetchone()
                elif fetchall:
                    result = await cur.fetchall()
            await conn.commit()
        return result

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
            """
            SELECT raw.id, raw.endpoint_url, raw.response, versions.id
            FROM raw_responses AS raw
            LEFT JOIN raw_response_versions AS versions
              ON versions.raw_response_id = raw.id
             AND versions.response_hash = raw.response_hash
            WHERE raw.endpoint_url = %s
            LIMIT 1
            """,
            (endpoint_url,),
            fetchone=True,
        )
        if row is None:
            return None
        return StoredNode(
            raw_id=str(row[0]),
            endpoint_url=row[1],
            response=row[2],
            raw_response_version_id=str(row[3]) if row[3] is not None else None,
        )

    async def get_stored_nodes(self, endpoint_urls: list[str]) -> dict[str, StoredNode]:
        """Return stored nodes for a bounded set of URLs in one query.

        Backfill traversal frequently encounters URLs whose payloads are already
        present.  Looking them up one at a time turns descendant expansion into
        a database round-trip bottleneck, so the async crawler uses this method
        for bounded batches.  The empty case avoids issuing an ``ANY`` query.
        """
        if not endpoint_urls:
            return {}
        rows = await self._execute(
            """
            SELECT raw.id, raw.endpoint_url, raw.response, versions.id
            FROM raw_responses AS raw
            LEFT JOIN raw_response_versions AS versions
              ON versions.raw_response_id = raw.id
             AND versions.response_hash = raw.response_hash
            WHERE raw.endpoint_url = ANY(%s)
            """,
            (endpoint_urls,),
            fetchall=True,
        )
        return {
            row[1]: StoredNode(
                raw_id=str(row[0]),
                endpoint_url=row[1],
                response=row[2],
                raw_response_version_id=str(row[3]) if row[3] is not None else None,
            )
            for row in rows or []
        }

    async def list_refresh_nodes(
        self,
        *,
        category: str,
        node_type: str,
        limit: int,
        after_endpoint_url: str | None = None,
    ) -> list[StoredNode]:
        rows = await self._execute(
            """
            SELECT raw.id, raw.endpoint_url, raw.response, versions.id
            FROM raw_responses AS raw
            LEFT JOIN raw_response_versions AS versions
              ON versions.raw_response_id = raw.id
             AND versions.response_hash = raw.response_hash
            WHERE raw.category = %s
              AND raw.node_type = %s
              AND raw.endpoint_url > COALESCE(%s, '')
            ORDER BY raw.endpoint_url ASC
            LIMIT %s
            """,
            (category, node_type, after_endpoint_url, limit),
            fetchall=True,
        )
        return [
            StoredNode(
                raw_id=str(row[0]),
                endpoint_url=row[1],
                response=row[2],
                raw_response_version_id=str(row[3]) if row[3] is not None else None,
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
        response_json, response_hash = _serialize_response(response)
        stored_row = await self._execute(
            _UPSERT_RESPONSE_SQL,
            _upsert_response_params(
                endpoint_url=endpoint_url,
                source=source,
                category=category,
                node_type=node_type,
                parent_id=parent_id,
                response_json=response_json,
                response_hash=response_hash,
                latest_crawl_run_id=latest_crawl_run_id,
            ),
            fetchone=True,
        )
        return StoredResponseRef(str(stored_row[0]), str(stored_row[1]), response_hash)

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
        _validate_fetch_version_reference(
            raw_response_id=raw_response_id,
            raw_response_version_id=raw_response_version_id,
            outcome=outcome,
        )
        await self._execute(
            """
            INSERT INTO crawl_run_fetches (
                id, crawl_run_id, endpoint_url, raw_response_id, raw_response_version_id,
                parent_endpoint_url, node_type, follow_mode, http_status, outcome,
                duration_ms, attempt_count, rate_limit_attempts, api_key_name, pool_allocation
            )
            VALUES (%s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            """,
            (
                str(uuid.uuid4()),
                crawl_run_id,
                endpoint_url,
                raw_response_id,
                raw_response_version_id,
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

    async def record_fetch_batch(self, records: list[dict[str, Any]]) -> None:
        """Record fetch telemetry with one transaction for a queue batch.

        Historical crawls can discover hundreds of already-seen URLs at once.
        The single-row method is intentionally retained for callers that need
        it, but using one ``executemany``/commit here avoids a WAL flush per
        telemetry row.
        """
        if not records:
            return
        for record in records:
            _validate_fetch_version_reference(
                raw_response_id=record.get("raw_response_id"),
                raw_response_version_id=record.get("raw_response_version_id"),
                outcome=record["outcome"],
            )
        await self._ensure_initialized()
        query = """
            INSERT INTO crawl_run_fetches (
                id, crawl_run_id, endpoint_url, raw_response_id, raw_response_version_id,
                parent_endpoint_url, node_type, follow_mode, http_status, outcome,
                duration_ms, attempt_count, rate_limit_attempts, api_key_name, pool_allocation
            )
            VALUES (%s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
        """
        params = [
            (
                str(uuid.uuid4()),
                record["crawl_run_id"],
                record["endpoint_url"],
                record.get("raw_response_id"),
                record.get("raw_response_version_id"),
                record.get("parent_endpoint_url"),
                record["node_type"],
                record["follow_mode"],
                record.get("http_status"),
                record["outcome"],
                record.get("duration_ms"),
                record.get("attempt_count"),
                record.get("rate_limit_attempts"),
                record.get("api_key_name"),
                _serialize_optional_json(record.get("pool_allocation")),
            )
            for record in records
        ]
        async with self._pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.executemany(query, params)
            await conn.commit()

    async def record_expected_fetch(
        self,
        *,
        crawl_run_id: str,
        endpoint_url: str,
        parent_endpoint_url: str | None,
        node_type: str,
        follow_mode: str,
    ) -> None:
        await self._execute(
            """
            INSERT INTO crawl_run_expected_fetches (
                id, crawl_run_id, endpoint_url, parent_endpoint_url,
                node_type, follow_mode
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (crawl_run_id, endpoint_url, node_type) DO NOTHING
            """,
            (
                str(uuid.uuid4()),
                crawl_run_id,
                endpoint_url,
                parent_endpoint_url,
                node_type,
                follow_mode,
            ),
        )

    async def record_expected_fetch_batch(self, records: list[dict[str, Any]]) -> None:
        """Record queue-discovery telemetry with one transaction for a batch."""
        if not records:
            return
        await self._ensure_initialized()
        query = """
            INSERT INTO crawl_run_expected_fetches (
                id, crawl_run_id, endpoint_url, parent_endpoint_url,
                node_type, follow_mode
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (crawl_run_id, endpoint_url, node_type) DO NOTHING
        """
        params = [
            (
                str(uuid.uuid4()),
                record["crawl_run_id"],
                record["endpoint_url"],
                record.get("parent_endpoint_url"),
                record["node_type"],
                record["follow_mode"],
            )
            for record in records
        ]
        async with self._pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.executemany(query, params)
            await conn.commit()

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
                _redact_error(error),
                duration_ms,
                attempt_count,
                rate_limit_attempts,
                api_key_name,
                _serialize_optional_json(pool_allocation),
            ),
        )
