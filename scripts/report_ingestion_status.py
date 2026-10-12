"""Write a lightweight six-hour ingestion and reconciliation status report.

The report is intentionally read-only.  It uses the crawler's durable run and
fetch tables for live progress, and the materialized dbt coverage/reconciliation
facts for the last verified quality snapshot.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
import os
import re
from pathlib import Path
import tempfile

import psycopg
import requests

from bicam_collection.config import Settings, parse_dotenv


@dataclass(frozen=True, slots=True)
class RunStatus:
    category: str
    run_id: str
    mode: str
    status: str
    started_at: datetime
    root_started_at: datetime | None
    fetched: int
    successful: int
    failed: int
    skipped: int
    recent_successes: int
    expected: int
    latest_congress: int | None
    recent_skips: int = 0
    # A paginated list response can reveal a larger queue than has been
    # materialized in crawl_run_expected_fetches while the crawler is still
    # discovering descendants. Keep that observed seed total for ETAs.
    observed_total: int | None = None


@dataclass(frozen=True, slots=True)
class GovInfoWindowStatus:
    category: str
    completed_through: datetime | None
    window_start: datetime
    window_end: datetime
    status: str
    list_pages: int | None
    fetch_events: int | None
    failed_events: int | None


@dataclass(frozen=True, slots=True)
class InternalReciprocityStatus:
    """Materialized Congress.gov same-source relationship audit row."""

    relationship_type: str
    relationship_count: int
    reciprocal_count: int
    unavailable_count: int
    contradiction_count: int
    unclassified_count: int
    reciprocal_rate: float | None
    contradiction_rate: float | None
    quality_status: str
    release_acceptance_status: str = "accepted_with_source_boundaries"
    asymmetric_count: int = 0


@dataclass(frozen=True, slots=True)
class GovInfoIdentityExceptionStatus:
    """Pending GovInfo member-reference identity review bucket."""

    review_reason: str
    exception_count: int


@dataclass(frozen=True, slots=True)
class GovInfoMemberReferenceMetricStatus:
    """Canonical member-ID enrichment and nested-identity census."""

    source_reference_count: int
    source_bioguide_count: int
    inferred_bioguide_count: int
    resolved_reference_count: int
    unresolved_reference_count: int
    composite_parent_count: int
    resolved_composite_parent_count: int
    component_row_count: int
    component_linkable_count: int
    nested_valid_bioguide_count: int
    nested_invalid_bioguide_count: int
    nested_thomas_or_authority_count: int
    nested_non_identifier_count: int


@dataclass(frozen=True, slots=True)
class CommitteePrintMetricStatus:
    """Informational CPRT census/overlap snapshot."""

    congress_print_count: int
    govinfo_package_count: int
    govinfo_representative_count: int
    exact_match_count: int
    congress_only_count: int
    govinfo_only_representative_count: int
    unparseable_package_count: int
    duplicate_variant_package_count: int
    excluded_non_cprt_count: int
    exact_title_agreement_count: int
    exact_match_rate: float
    quality_status: str
    observed_at: datetime | None


_ROOT_CANDIDATE_RE = re.compile(r"\[root-detail\] candidates=(\d+)")
_WAIT_RE = re.compile(r"waiting ([0-9]+(?:\.[0-9]+)?)s")
_CHILD_MISSING_RE = re.compile(r"^(bills|amendments)\s+.*\smissing=(\d+)\s*$", re.MULTILINE)
_CHILD_EXPECTED_RE = re.compile(r"^child_coverage_expected_total=(\d+)\s*$", re.MULTILINE)
_CHILD_TOTAL_RE = re.compile(r"^child_coverage_missing_total=(\d+)\s*$", re.MULTILINE)
_CHILD_PAGINATION_SOURCE_GAP_RE = re.compile(
    r"^child_coverage_pagination_source_gap_items_total=(\d+)\s*$",
    re.MULTILINE,
)
_CHILD_PAGINATION_MISSING_RE = re.compile(
    r"^child_coverage_pagination_missing_items_total=(\d+)\s*$",
    re.MULTILINE,
)

# Congress API windows are rolling. A short burst can be much faster than the
# sustainable deployment estimate and would otherwise make the ETA report
# claim completion hours too early. The per-credential window and any local
# aggregate setting are reported independently; the latter is not interpreted
# as an account quota.
_DEFAULT_CONGRESS_EFFECTIVE_RATE_CAP = 19_000.0

# The production Congress.gov dataset is intentionally limited to these
# endpoint families.  Other historical crawler rows remain queryable, but
# must not appear as active scope in the six-hour status artifact.
TARGET_CONGRESS_CATEGORIES = (
    "bills",
    "amendments",
    "members",
    "congresses",
    "hearings",
    "committees",
    "committee-meetings",
    "committee-prints",
    "committee-reports",
    "treaties",
    "nominations",
)


def _configured_congress_rate_cap() -> float:
    """Return the sustainable Congress attempts/hour cap used for ETAs."""

    # The report-specific override is already an effective deployment cap
    # (the systemd service sets it to the 0.20s pacer's 18,000 attempts/hour),
    # so do not apply the credential safety factor a second time.  The legacy
    # global setting remains a configured quota and is safety-adjusted below.
    raw_report_cap = os.environ.get("BICAM_REPORT_CONGRESS_RATE_CAP")
    if raw_report_cap is not None:
        try:
            return max(1.0, float(raw_report_cap))
        except (TypeError, ValueError):
            pass
    raw_limit = os.environ.get("BICAM_CONGRESS_GLOBAL_RATE_LIMIT")
    raw_safety = os.environ.get("BICAM_CONGRESS_RATE_SAFETY", "0.95")
    try:
        limit = float(raw_limit) if raw_limit else 20_000.0
    except (TypeError, ValueError):
        limit = 20_000.0
    try:
        safety = float(raw_safety)
    except (TypeError, ValueError):
        safety = 0.95
    cap = limit * min(1.0, max(0.1, safety))
    return max(1.0, cap or _DEFAULT_CONGRESS_EFFECTIVE_RATE_CAP)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("/tmp/bicam-ingestion-status-latest.md"),
    )
    parser.add_argument(
        "--history-dir",
        type=Path,
        default=Path("/tmp/bicam-ingestion-reports"),
    )
    return parser.parse_args()


def _fmt_time(value: datetime | None) -> str:
    if value is None:
        return "—"
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def _fmt_duration(hours: float | None) -> str:
    if hours is None:
        return "unknown"
    if hours < 1 / 60:
        return "<1m"
    minutes = round(hours * 60)
    if minutes < 60:
        return f"{minutes}m"
    days, remainder = divmod(minutes, 24 * 60)
    hours_part, minutes_part = divmod(remainder, 60)
    if days:
        return f"{days}d {hours_part}h"
    return f"{hours_part}h {minutes_part}m"


def _root_log_summary(category: str, started_at: datetime) -> tuple[int, float | None]:
    """Return candidate count and any observed key-wait seconds for this run."""

    project_tmp = Path("/tmp")
    candidates = 0
    wait_seconds: float | None = None
    start_epoch = started_at.timestamp() - 300
    paths = sorted(
        project_tmp.glob(f"bicam-modern-root-{category}-*-attempt-*.log"),
        key=lambda path: path.stat().st_mtime,
    )
    for path in paths:
        try:
            if path.stat().st_mtime < start_epoch:
                continue
            text = path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        matches = _ROOT_CANDIDATE_RE.findall(text)
        if matches:
            candidates += int(matches[-1])
        waits = [float(value) for value in _WAIT_RE.findall(text)]
        if waits:
            # Logs retain earlier quota windows.  The final observed wait is
            # the live estimate; using the historical maximum can turn a
            # completed cooldown into a stale multi-hour ETA.
            wait_seconds = max(wait_seconds or 0.0, waits[-1])
    return candidates, wait_seconds


def _query(connection: psycopg.Connection, sql: str, params: tuple[object, ...] = ()) -> list[tuple]:
    return list(connection.execute(sql, params).fetchall())


def _load_govinfo_windows(connection: psycopg.Connection) -> list[GovInfoWindowStatus]:
    rows = _query(
        connection,
        """
        with completed as (
            select category, max(window_end) as completed_through
            from govinfo_collection_windows
            where status = 'completed'
            group by category
        ), latest as (
            select distinct on (category)
                category, window_start, window_end, status,
                list_pages, fetch_events, failed_events
            from govinfo_collection_windows
            order by category, window_end desc, started_at desc
        )
        select latest.category, completed.completed_through,
               latest.window_start, latest.window_end, latest.status,
               latest.list_pages, latest.fetch_events, latest.failed_events
        from latest
        left join completed using (category)
        order by latest.category
        """,
    )
    return [GovInfoWindowStatus(*row) for row in rows]


def _govinfo_health(settings: Settings, checked_at: datetime) -> str:
    """Return a redacted, read-only health status for the GovInfo API.

    Materialized GovInfo rows can remain valid after a credential expires.  A
    small live probe keeps the six-hour report from presenting that old
    snapshot as evidence that future GovInfo refreshes are still possible.
    Never include the key or response body in the artifact.
    """

    values = {**parse_dotenv(settings.env_path), **os.environ}
    raw_values = [values.get("GOVINFO_API_KEYS", ""), values.get("GOVINFO_API_KEY", "")]
    raw_values.extend(
        value
        for name, value in sorted(values.items())
        if name.startswith("GOVINFO_API_KEY_") and value.strip()
    )
    keys: list[str] = []
    for raw_value in raw_values:
        for part in raw_value.split(","):
            key = part.strip()
            if key and key not in keys:
                keys.append(key)
    if not keys:
        return "State: **not_configured** (no GovInfo API key is configured)."
    statuses: list[int] = []
    request_errors: list[str] = []
    for key in keys:
        try:
            response = requests.get(
                f"{settings.govinfo_api_base_url}/packages/CRPT-118hrpt1/summary",
                params={"api_key": key},
                timeout=10,
            )
        except requests.RequestException as exc:
            request_errors.append(type(exc).__name__)
            continue
        statuses.append(response.status_code)
        if 200 <= response.status_code < 300:
            return (
                f"State: **healthy** (HTTP {response.status_code}; {len(keys)} configured key(s); "
                f"read-only probe at {_fmt_time(checked_at)})."
            )
    if statuses and all(status in {401, 403} for status in statuses):
        return (
            f"State: **unauthorized** (HTTP {statuses[-1]}; {len(keys)} configured key(s); "
            f"read-only probe at {_fmt_time(checked_at)})."
        )
    if statuses and max(statuses) >= 500:
        return (
            f"State: **unavailable** (HTTP {max(statuses)}; {len(keys)} configured key(s); "
            f"read-only probe at {_fmt_time(checked_at)})."
        )
    if statuses:
        return (
            f"State: **reachable** (HTTP {statuses[-1]}; {len(keys)} configured key(s); "
            f"read-only probe at {_fmt_time(checked_at)})."
        )
    return (
        f"State: **unavailable** ({len(keys)} configured key(s); "
        f"probe errors={','.join(request_errors) or 'unknown'} at {_fmt_time(checked_at)})."
    )


def _congress_rate_scope(
    settings: Settings,
    checked_at: datetime,
    *,
    active_crawl: bool = False,
) -> str:
    """Return a redacted, read-only summary of Congress rate-limit headers.

    The API exposes counters on responses, but those counters do not identify
    the account or organization that owns a credential. Probe each configured
    token separately so the durable report can distinguish token-scoped header
    behavior from a documented account-level quota. Never persist tokens,
    response bodies, or full request URLs containing credentials.
    """

    values = {**parse_dotenv(settings.env_path), **os.environ}
    per_key_limit = values.get("BICAM_CONGRESS_PER_KEY_RATE_LIMIT", "")
    aggregate_limit = values.get("BICAM_CONGRESS_GLOBAL_RATE_LIMIT", "")
    rate_scope_config = []
    if per_key_limit.strip():
        rate_scope_config.append(f"per-key scheduler={per_key_limit.strip()}/h")
    if aggregate_limit.strip():
        rate_scope_config.append(f"local aggregate setting={aggregate_limit.strip()}/h (not per-account quota)")
    rate_scope_summary = "; ".join(rate_scope_config) or "scheduler settings unreported"
    raw_values = [values.get("CONGRESS_API_KEYS", ""), values.get("CONGRESSIONAL_API_KEY", "")]
    raw_values.extend(
        value
        for name, value in sorted(values.items())
        if name.startswith("CONGRESS_API_KEY_") and value.strip()
    )
    keys: list[str] = []
    for raw_value in raw_values:
        for part in raw_value.split(","):
            key = part.strip()
            if key and key not in keys:
                keys.append(key)
    if not keys:
        return "State: **not_configured** (no Congress API key is configured)."
    if active_crawl:
        return (
            f"State: **deferred** ({len(keys)} configured key(s); an active Congress "
            "crawler or recent quota cooldown owns the request window; no probe issued; "
            "per-key header limit is established from prior probes; broader aggregate limiter scope "
            f"remains under test; {rate_scope_summary}; at {_fmt_time(checked_at)})."
        )

    statuses: list[int] = []
    limits: list[int] = []
    remaining: list[int] = []
    request_errors: list[str] = []
    endpoint = f"{settings.congress_api_base_url}/bill/119/hr/1?format=json"
    for key in keys:
        try:
            response = requests.get(
                endpoint,
                headers={"x-api-key": key},
                timeout=10,
            )
        except requests.RequestException as exc:
            request_errors.append(type(exc).__name__)
            continue
        statuses.append(response.status_code)
        for header_name, target in (
            ("X-RateLimit-Limit", limits),
            ("X-RateLimit-Remaining", remaining),
        ):
            raw_value = response.headers.get(header_name)
            try:
                if raw_value is not None:
                    target.append(int(raw_value))
            except (TypeError, ValueError):
                continue

    if statuses and all(status in {401, 403} for status in statuses):
        return (
            f"State: **unauthorized** (HTTP {statuses[-1]}; {len(keys)} configured key(s); "
            f"read-only probe at {_fmt_time(checked_at)})."
        )
    if not statuses:
        return (
            f"State: **unavailable** ({len(keys)} configured key(s); "
            f"probe errors={','.join(request_errors) or 'unknown'} at {_fmt_time(checked_at)})."
        )

    status_counts: dict[int, int] = {}
    for status in statuses:
        status_counts[status] = status_counts.get(status, 0) + 1
    status_summary = ", ".join(f"HTTP {status}: {count}" for status, count in sorted(status_counts.items()))
    limit_summary = (
        f"advertised limit={min(limits):,}–{max(limits):,}"
        if limits
        else "advertised limit=unreported"
    )
    remaining_summary = (
        f"remaining={min(remaining):,}–{max(remaining):,}"
        if remaining
        else "remaining=unreported"
    )
    state = "healthy" if any(200 <= status < 300 for status in statuses) else "reachable"
    return (
        f"State: **{state}** ({status_summary}; {len(keys)} configured key(s); "
        f"{limit_summary}; {remaining_summary}; per-key header limit observed; "
        "credential ownership is not inferred and broader aggregate limiter scope remains under test; "
        f"{rate_scope_summary}; read-only probe at {_fmt_time(checked_at)})."
    )


def _load_child_backlog(audit_path: Path) -> tuple[int | None, int | None, str | None]:
    """Read the most recent durable child-coverage audit, if available.

    The active partition runner only knows about the four Congress jobs it has
    started so far; later partitions are queued in the runner and therefore do
    not appear in ``crawl_run_expected_fetches`` yet.  The six-hour audit
    expands all stored child edges and is the authoritative estimate for that
    downstream backlog.  Keep the report read-only and tolerate a missing or
    partially-written audit artifact.
    """

    try:
        text = audit_path.read_text(encoding="utf-8")
    except OSError:
        return None, None, None
    matches = [int(value) for _category, value in _CHILD_MISSING_RE.findall(text)]
    # New audits report the expected edge count separately.  Fall back to the
    # legacy field for older artifacts, where it was the only total present.
    total_match = _CHILD_EXPECTED_RE.search(text) or _CHILD_TOTAL_RE.search(text)
    if not matches and total_match is None:
        return None, None, None
    total = int(total_match.group(1)) if total_match else None
    category_total = sum(matches)
    generated = text.splitlines()[0].strip() if text.splitlines() else None
    if generated and generated.startswith("Child-coverage audit checkpoint:"):
        generated = generated.split(":", 1)[1].strip()
    # Standalone checkpoints created by older audit wrappers may begin
    # directly with the first category row rather than a timestamp.  Do not
    # expose that row as the report's audit label; use the artifact mtime.
    if generated and not re.match(
        r"^(?:[A-Z][a-z]{2} |Child-coverage audit checkpoint:|\d{4}-\d{2}-\d{2} )",
        generated,
    ):
        try:
            generated = datetime.fromtimestamp(
                audit_path.stat().st_mtime, timezone.utc
            ).strftime("%Y-%m-%d %H:%M UTC")
        except OSError:
            generated = "latest durable audit"
    return category_total, total, generated


def _load_child_backlog_by_category(audit_path: Path) -> dict[str, int]:
    """Return durable missing-child counts keyed by source category."""

    try:
        text = audit_path.read_text(encoding="utf-8")
    except OSError:
        return {}
    by_category: dict[str, int] = {}
    for category, value in _CHILD_MISSING_RE.findall(text):
        by_category[category] = by_category.get(category, 0) + int(value)
    return by_category


def _load_child_pagination_gaps(audit_path: Path) -> tuple[int | None, int | None]:
    """Return item-level source-gap and unclassified-missing counts."""

    try:
        text = audit_path.read_text(encoding="utf-8")
    except OSError:
        return None, None
    source_gap = _CHILD_PAGINATION_SOURCE_GAP_RE.search(text)
    missing = _CHILD_PAGINATION_MISSING_RE.search(text)
    return (
        int(source_gap.group(1)) if source_gap else None,
        int(missing.group(1)) if missing else None,
    )


def _newest_child_audit_path(report_dir: Path) -> Path:
    """Choose the newest durable child audit artifact.

    The full ingestion audit and the standalone child-coverage audit are
    written by different scheduled/continuation paths.  During a long crawl
    the standalone artifact can be newer than the last full audit, so always
    prefer the most recently modified valid file.
    """

    candidates = [
        report_dir / "ingestion-audit-latest.txt",
        report_dir / "child-coverage-audit-latest.txt",
        # The supplemental refresh writes its authoritative audit as a
        # checkpoint markdown file, while the older audit timer uses .txt.
        # Include both formats so a newer zero-gap checkpoint cannot be hidden
        # behind a stale legacy symlink.
        report_dir / "child-coverage-checkpoint-latest.md",
    ]
    # Continuation/audit jobs also retain immutable timestamped child audits.
    # Include them so a newer standalone audit is not hidden behind a stale
    # latest symlink (or a status report generated before the symlink update).
    candidates.extend(report_dir.glob("child-coverage-audit-*.txt"))
    candidates.extend(report_dir.glob("child-coverage-checkpoint-*.md"))
    candidates.extend((report_dir / "child-coverage-history").glob("*.txt"))
    existing: list[tuple[float, Path]] = []
    for candidate in candidates:
        try:
            existing.append((candidate.stat().st_mtime, candidate))
        except OSError:
            continue
    if not existing:
        return candidates[0]
    return max(existing, key=lambda item: item[0])[1]


def _load_guard_state(state_path: Path) -> str | None:
    """Return a compact human-readable API guard state artifact."""

    try:
        values = dict(
            line.split("=", 1)
            for line in state_path.read_text(encoding="utf-8").splitlines()
            if "=" in line
        )
    except OSError:
        return None
    state = values.get("state")
    generated_at = values.get("generated_at")
    if not state or not generated_at:
        return None
    last_429 = values.get("last_http_429_at")
    last_429_text = f"; last HTTP 429 {last_429}" if last_429 and last_429 != "never" else ""
    return (
        f"State: **{state}** (last check {generated_at}; "
        f"2-minute window: {values.get('failures_2m', '—')} failures, "
        f"{values.get('fetched_2m', '—')} successes, "
        f"{values.get('http_503_2m', '—')} HTTP 503s, "
        f"{values.get('http_429_2m', '—')} HTTP 429s{last_429_text})."
    )


def _guard_has_recent_429(guard_text: str | None, now: datetime, *, within_seconds: int = 3600) -> bool:
    """Return whether the guard artifact records a recent quota response."""

    if not guard_text:
        return False
    match = re.search(r"last HTTP 429 (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)", guard_text)
    if not match:
        return False
    try:
        observed = datetime.fromisoformat(match.group(1).replace("Z", "+00:00"))
    except ValueError:
        return False
    return 0 <= (now - observed).total_seconds() <= within_seconds


def _load_runs(connection: psycopg.Connection) -> list[RunStatus]:
    rows = _query(
        connection,
        """
        with active_categories as (
            select distinct category
            from crawl_runs
            where status = 'running'
              and category = any(%s)
        ),
        inactive_runs as (
            select
                runs.*,
                coalesce(stats.failed, 0) as run_failed
            from crawl_runs as runs
            left join lateral (
                select count(*) filter (where fetches.outcome = 'failed') as failed
                from crawl_run_fetches as fetches
                where fetches.crawl_run_id = runs.id
            ) as stats on true
            where runs.status <> 'running'
              and runs.category = any(%s)
        ),
        latest_inactive_runs as (
            select distinct on (category) inactive.*
            from inactive_runs as inactive
            order by category,
                     -- Retry-only seeds are intentionally separate from the
                     -- authoritative historical crawl.  Prefer the latest
                     -- full/backfill run for current-Congress reporting so a
                     -- one-row retry failure cannot make an entire category
                     -- appear to have regressed to that retry's partition.
                     (inactive.mode not like 'retry%%') desc,
                     -- A recently interrupted historical worker is the
                     -- authoritative continuation point; do not let an older
                     -- failed partition win merely because it has a failure.
                     inactive.started_at desc
        ),
        selected_runs as (
            select runs.id, runs.category, runs.started_at, runs.finished_at,
                   runs.mode, runs.high_water_mark, runs.items_fetched, runs.status
            from crawl_runs as runs
            join active_categories using (category)
            where runs.status = 'running'
            union all
            select inactive.id, inactive.category, inactive.started_at,
                   inactive.finished_at, inactive.mode, inactive.high_water_mark,
                   inactive.items_fetched, inactive.status
            from latest_inactive_runs as inactive
            where not exists (
                select 1
                from active_categories
                where active_categories.category = inactive.category
            )
        ),
        per_run as (
            select runs.id::text as run_id,
                   runs.category,
                   runs.mode,
                   runs.status,
                   runs.started_at,
                   coalesce(stats.fetched, 0) as fetched,
                   coalesce(stats.successful, 0) as successful,
                   coalesce(stats.failed, 0) as failed,
                   coalesce(stats.skipped, 0) as skipped,
                   coalesce(stats.recent_successes, 0) as recent_successes,
                   coalesce(stats.recent_skips, 0) as recent_skips,
                   greatest(
                       coalesce(expected.count, 0) - coalesce(stats.skipped, 0),
                       0
                   ) as expected,
                   stats.latest_congress,
                   stats.observed_total
            from selected_runs as runs
            left join lateral (
                select count(*) as fetched,
                       count(*) filter (where fetches.outcome = 'fetched') as successful,
                       count(*) filter (where fetches.outcome = 'failed') as failed,
                       count(*) filter (where fetches.outcome like 'skipped%%') as skipped,
                       count(*) filter (
                       where fetches.outcome = 'fetched'
                         and fetches.fetched_at >= now() - interval '15 minutes'
                       ) as recent_successes,
                       count(*) filter (
                           where fetches.outcome like 'skipped%%'
                             and fetches.fetched_at >= now() - interval '15 minutes'
                       ) as recent_skips,
                       max((regexp_match(
                           fetches.endpoint_url,
                           '/(?:bill|amendment|committee-report|committee-print|hearing|committee-meeting|treaty|nomination)/([0-9]+)/'
                       ))[1]::int) as latest_congress
                       , max(
                           case
                               when fetches.node_type like '%%_list'
                                and responses.response #>> '{pagination,count}' ~ '^[0-9]+$'
                               then (responses.response #>> '{pagination,count}')::bigint
                           end
                       ) as observed_total
                from crawl_run_fetches as fetches
                left join raw_responses as responses
                  on responses.id = fetches.raw_response_id
                where fetches.crawl_run_id = runs.id
            ) as stats on true
            left join lateral (
                select count(*)
                from crawl_run_expected_fetches as expected_fetches
                where expected_fetches.crawl_run_id = runs.id
            ) as expected on true
        ),
        recent_by_category as (
            -- A partitioned historical worker starts a new crawl_run for
            -- every Congress.  During the few minutes after that boundary,
            -- the newest run may contain only skipped-existing events even
            -- though the category is actively fetching in the preceding
            -- partition.  Use the rolling category-wide window for ETA
            -- reporting so a partition transition cannot manufacture an
            -- artificially slow rate.  Seed-only retry runs are deliberately
            -- excluded: a two-URL repair must not make an otherwise idle
            -- descendant sweep look like it has sustained network throughput.
                   select runs.category,
                   count(*) filter (
                       where fetches.outcome = 'fetched'
                         and fetches.fetched_at >= now() - interval '15 minutes'
                   ) as recent_successes,
                   count(*) filter (
                       where fetches.outcome like 'skipped%%'
                         and fetches.fetched_at >= now() - interval '15 minutes'
                   ) as recent_skips
            from crawl_runs as runs
            join crawl_run_fetches as fetches on fetches.crawl_run_id = runs.id
            where runs.category = any(%s)
              and runs.mode not like 'retry%%'
            group by runs.category
        ),
        active as (
            select category,
                   min(run_id) as run_id,
                   case when count(distinct mode) = 1 then min(mode) else 'aggregate' end as mode,
                   'running'::text as status,
                   min(started_at) as started_at,
                   min(started_at) filter (where mode = 'targeted_root_detail_backfill') as root_started_at,
                   sum(fetched) as fetched,
                   sum(successful) as successful,
                   sum(failed) as failed,
                   sum(skipped) as skipped,
                   max(coalesce(recent_by_category.recent_successes, 0)) as recent_successes,
                   max(coalesce(recent_by_category.recent_skips, 0)) as recent_skips,
                   sum(expected) as expected,
                   max(latest_congress) as latest_congress,
                   max(observed_total) as observed_total
            from per_run
            left join recent_by_category using (category)
            where status = 'running'
            group by category
        )
        select run_id, category, mode, status, started_at, root_started_at,
               fetched, successful, failed, skipped, recent_successes,
               recent_skips,
               expected, latest_congress, observed_total
        from active
        union all
        select run_id, category, mode, status, started_at, null::timestamptz as root_started_at,
               fetched, successful, failed, skipped, recent_successes,
               recent_skips,
               expected, latest_congress, observed_total
        from per_run
        where status <> 'running'
        order by category
        """,
        (list(TARGET_CONGRESS_CATEGORIES),) * 3,
    )
    statuses: list[RunStatus] = []
    for row in rows:
        (
            run_id,
            category,
            mode,
            status,
            started_at,
            root_started_at,
            fetched,
            successful,
            failed,
            skipped,
            recent_successes,
            recent_skips,
            expected,
            latest_congress,
            observed_total,
        ) = row
        statuses.append(
            RunStatus(
                category=category,
                run_id=run_id,
                mode=mode,
                status=status,
                started_at=started_at,
                root_started_at=root_started_at,
                fetched=int(fetched),
                successful=int(successful),
                failed=int(failed),
                skipped=int(skipped),
                recent_successes=int(recent_successes),
                expected=int(expected),
                latest_congress=int(latest_congress) if latest_congress is not None else None,
                recent_skips=int(recent_skips),
                observed_total=int(observed_total) if observed_total is not None else None,
            )
        )
    return statuses


def _load_coverage(connection: psycopg.Connection) -> dict[str, tuple[float, float, float, float, float, int | None]]:
    rows = _query(
        connection,
        """
        select category,
               coalesce(sum(current_response_count), 0),
               coalesce(sum(expected_fetch_count), 0),
               coalesce(sum(successful_fetch_count), 0),
               coalesce(sum(failed_fetch_count), 0),
               coalesce(sum(unsatisfied_fetch_count + child_unsatisfied_fetch_count), 0),
               max(nullif(partition_value, 'unpartitioned')::int)
        from fct_collection_coverage
        where category = any(%s)
        group by category
        """,
        (list(TARGET_CONGRESS_CATEGORIES),),
    )
    return {str(row[0]): tuple(row[1:]) for row in rows}


def _load_reconciliation(connection: psycopg.Connection) -> list[tuple]:
    return _query(
        connection,
        """
        select entity, authoritative_source, candidate_count, accepted_count,
               ambiguous_count, unmatched_count, open_exception_count,
               ambiguous_rate, authoritative_match_rate,
               eligible_authoritative_match_rate,
               authoritative_source_gap_count, quality_status
        from fct_reconciliation_metrics
        order by entity
        """,
    )


def _load_committee_print_metrics(
    connection: psycopg.Connection,
) -> CommitteePrintMetricStatus | None:
    row = connection.execute(
        """
        select congress_print_count, govinfo_package_count,
               govinfo_representative_count, exact_match_count,
               congress_only_count, govinfo_only_representative_count,
               unparseable_package_count, duplicate_variant_package_count,
               excluded_non_cprt_count, exact_title_agreement_count,
               exact_match_rate, quality_status, observed_at
        from fct_committee_print_reconciliation_metrics
        order by observed_at desc
        limit 1
        """
    ).fetchone()
    if row is None:
        return None
    return CommitteePrintMetricStatus(
        congress_print_count=int(row[0]),
        govinfo_package_count=int(row[1]),
        govinfo_representative_count=int(row[2]),
        exact_match_count=int(row[3]),
        congress_only_count=int(row[4]),
        govinfo_only_representative_count=int(row[5]),
        unparseable_package_count=int(row[6]),
        duplicate_variant_package_count=int(row[7]),
        excluded_non_cprt_count=int(row[8]),
        exact_title_agreement_count=int(row[9]),
        exact_match_rate=float(row[10]),
        quality_status=str(row[11]),
        observed_at=row[12],
    )


def _load_internal_reciprocity(connection: psycopg.Connection) -> list[InternalReciprocityStatus]:
    """Load the same-source Congress.gov relationship audit metrics."""

    rows = _query(
        connection,
        """
        select relationship_type, relationship_count, reciprocal_count,
               unavailable_count, asymmetric_count, contradiction_count,
               unclassified_count,
               reciprocal_rate, contradiction_rate, quality_status
        from fct_congress_internal_reciprocity_metrics
        order by relationship_type
        """,
    )
    return [
        InternalReciprocityStatus(
            relationship_type=str(row[0]),
            relationship_count=int(row[1]),
            reciprocal_count=int(row[2]),
            unavailable_count=int(row[3]),
            asymmetric_count=int(row[4]),
            contradiction_count=int(row[5]),
            unclassified_count=int(row[6]),
            reciprocal_rate=float(row[7]) if row[7] is not None else None,
            contradiction_rate=float(row[8]) if row[8] is not None else None,
            quality_status=str(row[9]),
            release_acceptance_status=(
                "reject_no_evidence" if int(row[1]) == 0
                else "reject_contradiction" if int(row[5]) > 0
                else "reject_unclassified" if int(row[6]) > 0
                else "accepted_with_source_boundaries"
            ),
        )
        for row in rows
    ]


def _load_govinfo_identity_exceptions(
    connection: psycopg.Connection,
) -> list[GovInfoIdentityExceptionStatus]:
    """Load unresolved nested GovInfo member-reference counts."""

    rows = _query(
        connection,
        """
        select review_reason, count(*)
        from fct_govinfo_member_reference_identity_exceptions
        where review_status = 'pending_manual_review'
        group by review_reason
        order by review_reason
        """,
    )
    return [
        GovInfoIdentityExceptionStatus(str(row[0]), int(row[1]))
        for row in rows
    ]


def _load_govinfo_member_reference_metrics(
    connection: psycopg.Connection,
) -> GovInfoMemberReferenceMetricStatus | None:
    """Load source-vs-enriched member IDs without scanning raw JSON."""

    rows = _query(
        connection,
        """
        select metric_name, metric_value
        from fct_govinfo_data_quality_metrics
        where metric_name in (
            'member_reference_total',
            'member_reference_source_bioguide',
            'member_reference_inferred_bioguide',
            'member_reference_resolved',
            'member_reference_unresolved_missing_source_identifier',
            'nested_valid_bioguide',
            'nested_invalid_bioguide',
            'nested_thomas_or_authority_id_total',
            'nested_non_identifier_text',
            'member_reference_component_total',
            'member_reference_component_resolved'
        )
        """,
    )
    values = {str(row[0]): int(row[1]) for row in rows}
    if not values:
        return None
    composite_parent_counts = _query(
        connection,
        """
        select
            count(*) filter (where multi_legislator_flag),
            count(*) filter (
                where multi_legislator_flag
                  and resolved_bioguide_id is not null
            )
        from bridge_govinfo__member_references
        """,
    )
    return GovInfoMemberReferenceMetricStatus(
        source_reference_count=values.get('member_reference_total', 0),
        source_bioguide_count=values.get('member_reference_source_bioguide', 0),
        inferred_bioguide_count=values.get('member_reference_inferred_bioguide', 0),
        resolved_reference_count=values.get('member_reference_resolved', 0),
        unresolved_reference_count=values.get(
            'member_reference_unresolved_missing_source_identifier', 0
        ),
        composite_parent_count=int(composite_parent_counts[0][0]),
        resolved_composite_parent_count=int(composite_parent_counts[0][1]),
        component_row_count=values.get('member_reference_component_total', 0),
        component_linkable_count=values.get('member_reference_component_resolved', 0),
        nested_valid_bioguide_count=values.get('nested_valid_bioguide', 0),
        nested_invalid_bioguide_count=values.get('nested_invalid_bioguide', 0),
        nested_thomas_or_authority_count=values.get(
            'nested_thomas_or_authority_id_total', 0
        ),
        nested_non_identifier_count=values.get('nested_non_identifier_text', 0),
    )


def _rate_and_eta(
    run: RunStatus,
    *,
    child_backlog: int | None = None,
    child_backlog_by_category: dict[str, int] | None = None,
    congress_rate_cap_per_hour: float | None = None,
) -> tuple[str, str]:
    if run.status != "running":
        return "—", "—"
    # ``recent_successes`` is category-wide (it can include a preceding
    # partition or run), so divide by the same fixed 15-minute window for all
    # modes. Using the new run's short elapsed time would turn a normal
    # partition transition into an impossible burst rate and an optimistic
    # ETA—especially after a service restart.
    window_hours = 0.25
    rate_per_hour = run.recent_successes / window_hours
    traversal_rate_per_hour = run.recent_skips / window_hours
    if (
        congress_rate_cap_per_hour is not None
        and run.category in {"bills", "amendments"}
        and run.mode in {"aggregate", "backfill_descendants", "backfill_refresh_descendants"}
    ):
        # Do not extrapolate a short burst across the rolling API window. The
        # observed rate can exceed the sustainable aggregate before Rotisserie
        # reaches each key's local reserve and begins waiting.
        rate_per_hour = min(rate_per_hour, congress_rate_cap_per_hour)
    # Full/backfill/aggregate runs fan out every source-list item and discover
    # those descendants lazily, so use the source-reported list total when it
    # is larger than the durable expected-fetch ledger. Incremental runs
    # deliberately suppress unchanged parent child trees; their expected-fetch
    # ledger is the correct changed-work denominator and a full seed total
    # would turn a small refresh into a multi-hour false ETA.
    observed_total_modes = {
        "aggregate",
        "full",
        "backfill_descendants",
        "backfill_refresh_descendants",
        "targeted_root_detail_backfill",
    }
    total = (
        max(run.expected, run.observed_total or 0)
        if run.mode in observed_total_modes
        else run.expected
    )
    if run.category in {"bills", "amendments"}:
        # A category can have root-detail and descendant runs at the same
        # time.  Use the root lane's start time when locating its candidate
        # logs; the aggregate category start may be much later because a
        # descendant pre-pass started afterward.
        log_start = run.root_started_at or run.started_at
        candidate_total, wait_seconds = _root_log_summary(run.category, log_start)
        if candidate_total:
            total = max(total, candidate_total)
        cooldown_hours = (wait_seconds or 0.0) / 3600 if wait_seconds and wait_seconds >= 60 else 0.0
        if cooldown_hours and rate_per_hour < 1000:
            return "key cooldown", _fmt_duration(cooldown_hours)
        if wait_seconds and rate_per_hour == 0:
            return "waiting", _fmt_duration(wait_seconds / 3600)
        if rate_per_hour <= 0 and traversal_rate_per_hour > 0:
            return (
                f"0/h network; {traversal_rate_per_hour:.0f}/h traversal",
                "pending network edges",
            )
        # A unified descendant run discovers later Congress partitions only
        # after the current four workers finish.  Its durable expected-fetch
        # count therefore under-reports the real queue.  Prefer the latest
        # child-coverage audit for that phase so the report does not claim
        # completion in minutes while millions of child URLs remain.
        category_backlog = (child_backlog_by_category or {}).get(run.category, child_backlog)
        if run.mode in {"aggregate", "backfill_descendants", "backfill_refresh_descendants"} and category_backlog:
            if rate_per_hour <= 0:
                return "idle", "unknown"
            return f"{rate_per_hour:.0f}/h", _fmt_duration(category_backlog / rate_per_hour)
        if total == 0:
            return (f"{rate_per_hour:.0f}/h" if rate_per_hour > 0 else "idle"), "unknown"
    if rate_per_hour <= 0:
        return "idle", "unknown"
    if total <= run.successful:
        if run.mode == "aggregate":
            return f"{rate_per_hour:.0f}/h", "unknown"
        return f"{rate_per_hour:.0f}/h", "<1m"
    eta_hours = (total - run.successful) / rate_per_hour
    if run.category in {"bills", "amendments"} and cooldown_hours:
        return f"{rate_per_hour:.0f}/h", (
            f"{_fmt_duration(cooldown_hours)} cooldown + {_fmt_duration(eta_hours)}"
        )
    return f"{rate_per_hour:.0f}/h", _fmt_duration(eta_hours)


def render_report(
    *,
    statuses: list[RunStatus],
    coverage: dict[str, tuple[float, float, float, float, float, int | None]],
    reconciliation: list[tuple],
    internal_reciprocity: list[InternalReciprocityStatus] | None = None,
    govinfo_identity_exceptions: list[GovInfoIdentityExceptionStatus] | None = None,
    govinfo_member_reference_metrics: GovInfoMemberReferenceMetricStatus | None = None,
    generated_at: datetime,
    child_backlog: int | None = None,
    child_backlog_total: int | None = None,
    child_audit_generated: str | None = None,
    child_backlog_by_category: dict[str, int] | None = None,
    child_pagination_source_gap_items: int | None = None,
    child_pagination_missing_items: int | None = None,
    availability_guard: str | None = None,
    congress_rate_scope: str | None = None,
    govinfo_health: str | None = None,
    govinfo_windows: list[GovInfoWindowStatus] | None = None,
    committee_print_metrics: CommitteePrintMetricStatus | None = None,
    congress_rate_cap_per_hour: float | None = None,
    scope_congress_end: int = 119,
) -> str:
    lines = [
        "# Bicam ingestion status",
        "",
        f"Generated: {_fmt_time(generated_at)}",
        f"Scope: Congress 1–{scope_congress_end} (1789–present); live run state comes from durable crawler tables.",
        "",
    ]
    if availability_guard:
        lines.extend(["## API availability guard", "", availability_guard, ""])
    if congress_rate_scope:
        lines.extend(["## Congress API rate scope", "", congress_rate_scope, ""])
    if govinfo_health:
        lines.extend(["## GovInfo API health", "", govinfo_health, ""])
    if govinfo_windows:
        common_frontier = min(
            (row.completed_through for row in govinfo_windows if row.completed_through is not None),
            default=None,
        )
        lines.extend(
            [
                "## GovInfo incremental coverage",
                "",
                f"Shared completed frontier: **{_fmt_time(common_frontier)}**.",
                "",
                "| Collection | Completed through | Latest window | Status | List pages | Fetch events | Failed events |",
                "|---|---|---|---|---:|---:|---:|",
            ]
        )
        for row in govinfo_windows:
            window = f"{_fmt_time(row.window_start)} → {_fmt_time(row.window_end)}"
            lines.append(
                f"| {row.category} | {_fmt_time(row.completed_through)} | {window} "
                f"| {row.status} | {row.list_pages or 0} | {row.fetch_events or 0} "
                f"| {row.failed_events or 0} |"
            )
        lines.append("")
    lines.extend(
        [
            "## Data-type progress",
            "",
            "| Data type | Current Congress | Latest/active mode | Status | Events (selected/active run new/fail/skip) | Rate | ETA |",
            "|---|---:|---|---|---:|---:|---:|",
        ]
    )
    guard_paused = availability_guard is not None and "State: **paused" in availability_guard
    guard_quota_cooldown = availability_guard is not None and "State: **quota_cooldown" in availability_guard
    for run in statuses:
        if guard_paused and run.status == "running":
            rate, eta = "paused", "retry scheduled"
        # The guard is source-wide, but separate processes may still have a
        # bounded reserve and be making progress (for example the amendment
        # refresh lane while the larger bill lane waits).  Only label a lane
        # as waiting when its own rolling window is idle; otherwise report its
        # observed rate and ETA instead of hiding useful progress.
        elif (
            guard_quota_cooldown
            and run.status == "running"
            and run.recent_successes == 0
            and run.recent_skips == 0
        ):
            rate, eta = "quota cooldown", "waiting for reset"
        else:
            rate, eta = _rate_and_eta(
                run,
                child_backlog=child_backlog,
                child_backlog_by_category=child_backlog_by_category,
                congress_rate_cap_per_hour=congress_rate_cap_per_hour,
            )
        current_congress = run.latest_congress
        if run.category in coverage:
            # A completed supplemental/probe run can contain only one older
            # partition (for example a GovInfo-first bill probe), even though
            # the materialized archive is complete through a later Congress.
            # Report the furthest authoritative partition observed by either
            # source so the progress table cannot regress from 119 to the
            # partition touched by a one-row repair.
            coverage_congress = coverage[run.category][5]
            if coverage_congress is not None:
                current_congress = max(
                    current_congress or coverage_congress,
                    coverage_congress,
                )
        current = str(current_congress) if current_congress is not None else "—"
        status = (
            "paused" if guard_paused and run.status == "running"
            else (
                "waiting_quota"
                if guard_quota_cooldown
                and run.status == "running"
                and run.recent_successes == 0
                and run.recent_skips == 0
                else run.status
            )
        )
        lines.append(
            f"| {run.category} | {current} | {run.mode} | {status} "
            f"| {run.fetched} ({run.successful}/{run.failed}/{run.skipped}) | {rate} | {eta} |"
        )

    lines.extend(
        [
            "",
            "## Last materialized coverage snapshot",
            "",
            "| Data type | Responses | Expected | Successful | Failed | Unsatisfied | Last Congress partition |",
            "|---|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for category in sorted(coverage):
        responses, expected, successful, failed, unsatisfied, partition = coverage[category]
        partition_text = str(partition) if partition is not None else "—"
        lines.append(
            f"| {category} | {responses:.0f} | {expected:.0f} | {successful:.0f} "
            f"| {failed:.0f} | {unsatisfied:.0f} | {partition_text} |"
        )

    lines.extend(
        [
            "",
            "## Reconciliation quality",
            "",
            "| Entity | Candidates | Accepted | Raw match rate | Eligible match rate | Source gaps | Ambiguous | Unmatched | Quality |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---|",
        ]
    )
    for row in reconciliation:
        (
            entity,
            _source,
            candidates,
            accepted,
            ambiguous,
            unmatched,
            open_exceptions,
            ambiguous_rate,
            match_rate,
            eligible_match_rate,
            source_gap_count,
            quality,
        ) = row
        lines.append(
            f"| {entity} | {candidates} | {accepted} | {float(match_rate or 0):.2%} "
            f"| {float(eligible_match_rate or 0):.2%} | {source_gap_count} "
            f"| {float(ambiguous_rate or 0):.2%} | {unmatched} | {quality} |"
        )
    if not reconciliation:
        lines.append("| — | — | — | — | — | — | — | no materialized metrics |")

    if committee_print_metrics is not None:
        metric = committee_print_metrics
        lines.extend(
            [
                "",
                "## Committee-print CPRT census",
                "",
                f"Observed {_fmt_time(metric.observed_at)}; this is an informational source-boundary metric, not a strict quality gate.",
                "",
                "| Congress.gov prints | CPRT packages | CPRT representatives | Exact links | Congress-only | GovInfo-only representatives | Legacy variants | Unsupported/malformed | Excluded non-CPRT | Exact overlap | Title-agreeing links |",
                "|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
                f"| {metric.congress_print_count} | {metric.govinfo_package_count} | {metric.govinfo_representative_count} | {metric.exact_match_count} | {metric.congress_only_count} | {metric.govinfo_only_representative_count} | {metric.duplicate_variant_package_count} | {metric.unparseable_package_count} | {metric.excluded_non_cprt_count} | {metric.exact_match_rate:.2%} | {metric.exact_title_agreement_count} |",
                f"Status: **{metric.quality_status}**.",
            ]
        )

    if internal_reciprocity:
        lines.extend(
            [
                "",
                "## Congress.gov internal reciprocity",
                "",
                "| Relationship | Edges | Reciprocal | Reciprocal rate | Counterpart unavailable | Asymmetric source | Contradictions | Quality | Release acceptance |",
                "|---|---:|---:|---:|---:|---:|---:|---|---|",
            ]
        )
        for row in internal_reciprocity:
            lines.append(
                f"| {row.relationship_type} | {row.relationship_count} | {row.reciprocal_count} "
                f"| {float(row.reciprocal_rate or 0):.2%} | {row.unavailable_count} "
                f"| {row.asymmetric_count} | {row.contradiction_count} | {row.quality_status} | {row.release_acceptance_status} |"
            )
    else:
        lines.extend(
            [
                "",
                "## Congress.gov internal reciprocity",
                "",
                "No materialized internal reciprocity metrics.",
            ]
        )

    if govinfo_identity_exceptions:
        lines.extend(
            [
                "",
                "## GovInfo member-reference identity review",
                "",
                "| Review reason | Pending references |",
                "|---|---:|",
            ]
        )
        for row in govinfo_identity_exceptions:
            lines.append(f"| {row.review_reason} | {row.exception_count} |")
    else:
        lines.extend(
            [
                "",
                "## GovInfo member-reference identity review",
                "",
                "No pending identity exceptions.",
            ]
        )

    if govinfo_member_reference_metrics is not None:
        metric = govinfo_member_reference_metrics
        enriched_count = metric.resolved_reference_count - metric.source_bioguide_count
        person_rows = (
            metric.source_reference_count
            - metric.composite_parent_count
            + metric.component_row_count
        )
        person_linkable = (
            metric.resolved_reference_count
            - metric.resolved_composite_parent_count
            + metric.component_linkable_count
        )
        person_unresolved = max(0, person_rows - person_linkable)
        lines.extend(
            [
                "",
                "## GovInfo member-reference enrichment",
                "",
                "| Source references | Source Bioguide IDs | Enriched IDs | Person-level rows | Linkable person rows | Unresolved person rows | Composite parents | Component rows |",
                "|---:|---:|---:|---:|---:|---:|---:|---:|",
                f"| {metric.source_reference_count} | {metric.source_bioguide_count} | {enriched_count} "
                f"| {person_rows} | {person_linkable} | {person_unresolved} "
                f"| {metric.composite_parent_count} | {metric.component_row_count} |",
                f"Nested package/granule identity audit: {metric.nested_valid_bioguide_count} valid Bioguide; "
                f"{metric.nested_thomas_or_authority_count} Thomas/authority identifiers; "
                f"{metric.nested_invalid_bioguide_count} invalid-format; "
                f"{metric.nested_non_identifier_count} non-identifier values.",
                "Composite parent objects remain source-faithful; each named legislator is linkable through the component row.",
            ]
        )

    if child_backlog is not None:
        audit_label = child_audit_generated or "latest durable audit"
        total_label = f"; all declared child edges: {child_backlog_total}" if child_backlog_total is not None else ""
        lines.extend(
            [
                "",
                "## Live descendant backlog",
                "",
                f"Latest child-coverage audit ({audit_label}): bills + amendments missing **{child_backlog}** responses{total_label}.",
                "This backlog is separate from cross-source identity matching and is included in the active descendant ETA.",
            ]
        )
        if child_pagination_source_gap_items is not None:
            lines.append(
                "Paginated bill→cosponsor child-item audit (known source-error pages): **"
                + str(child_pagination_source_gap_items)
                + " source-gap items** are explicitly classified; unclassified item shortfall: **"
                + str(child_pagination_missing_items or 0)
                + "**."
            )

    lines.extend(
        [
            "",
            "Notes:",
            "- `Current Congress` is the highest Congress number observed in the active run; completed categories fall back to the last dbt coverage partition.",
            "- Live event totals are for the selected completed run or the aggregate active runs. In the materialized coverage snapshot, `Expected`/`Successful`/`Failed` describe the latest completed non-retry run per Congress partition, while `Responses` is the cumulative current-state archive count; full-scope root/child completeness comes from the dedicated coverage ledgers. GovInfo date-window completion is authoritative in the incremental coverage ledger and its timestamped checkpoint artifacts. `skipped_existing` descendant traversal records are included in the `skip` value; `new` counts network-fetched payloads and is the value used for rate/ETA.",
            "- Raw match rate includes records explicitly classified as source gaps; eligible match rate excludes those reviewed gaps and measures the actual cross-source overlap.",
            "- Reconciliation match rates use each entity contract's authoritative-source denominator; candidate counts retain source-only records from the participating source contract (historical SERIALSET evidence is excluded from the CRPT relation).",
            "- Bill reconciliation currently has a documented GovInfo BILLS coverage boundary at Congress 103; interpret the raw bill overlap together with eligible match rate and source-gap counts.",
            "- Reconciliation values are the last materialized dbt snapshot and may lag the live crawler until the post-crawl dbt build runs.",
            f"- Congress bill/amendment ETAs cap short-window bursts at the sustainable aggregate rate (currently {_configured_congress_rate_cap():,.0f} effective attempts/hour); rolling-window waits are not extrapolated as instantaneous throughput.",
            "- A zero rate with a nonzero ETA means the worker is waiting on API quota, a retry backoff, or a source-side exception—not that the queue is complete.",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> int:
    args = parse_args()
    project_root = Path(__file__).resolve().parents[1]
    settings = Settings.from_env(project_root)
    generated_at = datetime.now(timezone.utc)
    audit_path = _newest_child_audit_path(project_root.parent / "reports")
    guard_state_path = project_root.parent / "reports" / "availability-guard-latest.txt"
    child_backlog, child_backlog_total, child_audit_generated = _load_child_backlog(audit_path)
    child_backlog_by_category = _load_child_backlog_by_category(audit_path)
    child_pagination_source_gap_items, child_pagination_missing_items = _load_child_pagination_gaps(audit_path)
    availability_guard = _load_guard_state(guard_state_path)
    with psycopg.connect(settings.database_url) as connection:
        connection.execute("set statement_timeout='30s'")
        scope_congress_end = connection.execute(
            """
            select coalesce(
                max((regexp_match(endpoint_url, '/congress/([0-9]+)(?:[?/]|$)'))[1]::integer),
                119
            )
              from raw_responses
             where source = 'congress'
               and category = 'congresses'
               and node_type = 'congress_detail'
            """
        ).fetchone()[0]
        statuses = _load_runs(connection)
        coverage = _load_coverage(connection)
        reconciliation = _load_reconciliation(connection)
        committee_print_metrics = _load_committee_print_metrics(connection)
        internal_reciprocity = _load_internal_reciprocity(connection)
        govinfo_identity_exceptions = _load_govinfo_identity_exceptions(connection)
        govinfo_member_reference_metrics = _load_govinfo_member_reference_metrics(connection)
        govinfo_windows = _load_govinfo_windows(connection)
    active_congress_crawl = any(
        run.status == "running" and run.category in {"bills", "amendments"}
        for run in statuses
    )
    active_congress_categories = {
        run.category
        for run in statuses
        if run.status == "running" and run.category in {"bills", "amendments"}
    }
    congress_rate_cap_per_hour = (
        _configured_congress_rate_cap() / max(1, len(active_congress_categories))
        if active_congress_crawl
        else None
    )
    congress_rate_scope = _congress_rate_scope(
        settings,
        generated_at,
        active_crawl=active_congress_crawl or _guard_has_recent_429(availability_guard, generated_at),
    )
    govinfo_health = _govinfo_health(settings, generated_at)
    report = render_report(
        statuses=statuses,
        coverage=coverage,
        reconciliation=reconciliation,
        committee_print_metrics=committee_print_metrics,
        internal_reciprocity=internal_reciprocity,
        govinfo_identity_exceptions=govinfo_identity_exceptions,
        govinfo_member_reference_metrics=govinfo_member_reference_metrics,
        generated_at=generated_at,
        child_backlog=child_backlog,
        child_backlog_total=child_backlog_total,
        child_audit_generated=child_audit_generated,
        child_backlog_by_category=child_backlog_by_category,
        child_pagination_source_gap_items=child_pagination_source_gap_items,
        child_pagination_missing_items=child_pagination_missing_items,
        availability_guard=availability_guard,
        congress_rate_scope=congress_rate_scope,
        govinfo_health=govinfo_health,
        govinfo_windows=govinfo_windows,
        congress_rate_cap_per_hour=congress_rate_cap_per_hour,
        scope_congress_end=scope_congress_end,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.history_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=args.output.parent, delete=False
    ) as temporary:
        temporary.write(report)
        temporary_path = Path(temporary.name)
    temporary_path.replace(args.output)
    history_path = args.history_dir / f"{generated_at.strftime('%Y%m%dT%H%M%SZ')}.md"
    history_path.write_text(report, encoding="utf-8")
    print(report, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
