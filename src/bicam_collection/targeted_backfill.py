"""Fetch-and-record loop shared by the targeted backfill scripts.

Each script selects missing detail URLs with its own SQL and hands them here
as ``TargetedCandidate`` rows; only the selection and labels differ.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
import time

from bicam_collection.client import AsyncCongressClient, fetch_metadata_values
from bicam_collection.storage import AsyncPostgresStore


@dataclass(frozen=True, slots=True)
class TargetedFamily:
    category: str
    node_type: str
    endpoint_name: str
    endpoint_priority: int
    run_mode: str
    source: str = "congress"
    # Also write a failed crawl_run_fetches row; the meeting-to-hearing
    # candidate SQL reads it to defer recent 404s.
    record_failed_fetches: bool = False
    fail_run_on_any_failure: bool = False


@dataclass(frozen=True, slots=True)
class TargetedCandidate:
    endpoint_url: str
    parent_id: str | None
    parent_endpoint_url: str


@dataclass(frozen=True, slots=True)
class TargetedRunOutcome:
    run_id: str
    fetched: int
    failed: int


async def run_targeted_backfill(
    family: TargetedFamily,
    candidates: list[TargetedCandidate],
    *,
    client: AsyncCongressClient,
    store: AsyncPostgresStore,
    concurrency: int,
    batch_size: int | None = None,
    after_success: Callable[[TargetedCandidate], Awaitable[None]] | None = None,
) -> TargetedRunOutcome:
    """Fetch every candidate, record each outcome, and close the run and store.

    ``concurrency`` bounds in-flight requests and ``batch_size`` how many tasks
    are scheduled at once. An exception in ``after_success`` counts as a
    failure of that candidate.
    """
    run_id = await store.start_run(family.category, family.run_mode)
    semaphore = asyncio.Semaphore(concurrency)
    fetched = 0
    failed = 0

    async def fetch(candidate: TargetedCandidate) -> None:
        nonlocal fetched, failed
        started_at = time.perf_counter()
        try:
            async with semaphore:
                result = await client.get_json(
                    candidate.endpoint_url,
                    endpoint_name=family.endpoint_name,
                    endpoint_priority=family.endpoint_priority,
                )
            telemetry = fetch_metadata_values(result.metadata)
            http_status = telemetry.pop("status_code")
            response_ref = await store.upsert_response(
                endpoint_url=candidate.endpoint_url,
                source=family.source,
                category=family.category,
                node_type=family.node_type,
                parent_id=candidate.parent_id,
                response=result.payload,
                latest_crawl_run_id=run_id,
            )
            await store.record_fetch(
                crawl_run_id=run_id,
                endpoint_url=candidate.endpoint_url,
                raw_response_id=response_ref.raw_response_id,
                raw_response_version_id=response_ref.raw_response_version_id,
                parent_endpoint_url=candidate.parent_endpoint_url,
                node_type=family.node_type,
                follow_mode="targeted",
                http_status=http_status,
                outcome="fetched",
                **telemetry,
            )
            if after_success is not None:
                await after_success(candidate)
            fetched += 1
        except Exception as exc:
            telemetry = fetch_metadata_values(getattr(exc, "fetch_metadata", None))
            http_status = getattr(getattr(exc, "response", None), "status_code", telemetry.pop("status_code"))
            if telemetry["duration_ms"] is None:
                telemetry["duration_ms"] = max(1, round((time.perf_counter() - started_at) * 1000))
            await store.record_failed_request(
                crawl_run_id=run_id,
                category=family.category,
                node_type=family.node_type,
                endpoint_url=candidate.endpoint_url,
                parent_endpoint_url=candidate.parent_endpoint_url,
                http_status=http_status,
                error=str(exc),
                **telemetry,
            )
            if family.record_failed_fetches:
                await store.record_fetch(
                    crawl_run_id=run_id,
                    endpoint_url=candidate.endpoint_url,
                    raw_response_id=None,
                    parent_endpoint_url=candidate.parent_endpoint_url,
                    node_type=family.node_type,
                    follow_mode="targeted",
                    http_status=http_status,
                    outcome="failed",
                    **telemetry,
                )
            failed += 1

    step = batch_size or max(1, len(candidates))
    batches = [candidates[start : start + step] for start in range(0, len(candidates), step)]
    try:
        for batch in batches:
            await asyncio.gather(*(fetch(candidate) for candidate in batch))
        status = "failed" if failed and family.fail_run_on_any_failure else "completed"
        await store.finish_run(run_id, status, fetched)
    except BaseException:
        await store.finish_run(run_id, "failed", fetched)
        raise
    finally:
        await store.close()
    return TargetedRunOutcome(run_id=run_id, fetched=fetched, failed=failed)
