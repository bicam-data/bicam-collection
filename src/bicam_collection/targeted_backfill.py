"""Shared fetch-and-record loop for targeted Congress.gov backfills.

Every targeted backfill script selects a bounded set of missing detail URLs
with family-specific SQL, then fetches each URL through the async client and
records the outcome.  Only the selection and a handful of labels differ, so
the scripts describe their family with :class:`TargetedFamily`, map their
rows onto :class:`TargetedCandidate`, and hand both to
:func:`run_targeted_backfill`.  The scripts keep their own progress lines and
exit codes because wrapper shells parse that stdout.
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
    # Relationship repairs also write a failed crawl_run_fetches row next to
    # the failed_requests row; the meeting-to-hearing candidate SQL reads that
    # ledger to defer recent 404s.  Reconciliation probes keep only the
    # failed_requests row.
    record_failed_fetches: bool = False
    # Law details mark the crawl run itself failed when any fetch failed.
    # The other families always finish "completed" and surface failures
    # through their exit code or summary line.
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

    ``concurrency`` bounds in-flight HTTP requests; store writes happen outside
    that bound.  ``batch_size`` bounds how many fetch tasks are scheduled at
    once, or schedules all of them when ``None``.  ``after_success`` runs after
    a candidate is stored and still counts as part of that candidate's fetch,
    so an exception there is recorded as a failure of the candidate.
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
            metadata = fetch_metadata_values(result.metadata)
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
                http_status=metadata["status_code"],
                outcome="fetched",
                duration_ms=metadata["duration_ms"],
                attempt_count=metadata["attempt_count"],
                rate_limit_attempts=metadata["rate_limit_attempts"],
                api_key_name=metadata["api_key_name"],
                pool_allocation=metadata["pool_allocation"],
            )
            if after_success is not None:
                await after_success(candidate)
            fetched += 1
        except Exception as exc:
            response = getattr(exc, "response", None)
            metadata = fetch_metadata_values(getattr(exc, "fetch_metadata", None))
            duration_ms = metadata["duration_ms"]
            if duration_ms is None:
                duration_ms = max(1, round((time.perf_counter() - started_at) * 1000))
            http_status = getattr(response, "status_code", metadata["status_code"])
            await store.record_failed_request(
                crawl_run_id=run_id,
                category=family.category,
                node_type=family.node_type,
                endpoint_url=candidate.endpoint_url,
                parent_endpoint_url=candidate.parent_endpoint_url,
                http_status=http_status,
                error=str(exc),
                duration_ms=duration_ms,
                attempt_count=metadata["attempt_count"],
                rate_limit_attempts=metadata["rate_limit_attempts"],
                api_key_name=metadata["api_key_name"],
                pool_allocation=metadata["pool_allocation"],
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
                    duration_ms=duration_ms,
                    attempt_count=metadata["attempt_count"],
                    rate_limit_attempts=metadata["rate_limit_attempts"],
                    api_key_name=metadata["api_key_name"],
                    pool_allocation=metadata["pool_allocation"],
                )
            failed += 1

    if batch_size is None:
        batches = [candidates]
    else:
        batches = [
            candidates[start : start + batch_size]
            for start in range(0, len(candidates), batch_size)
        ]
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
