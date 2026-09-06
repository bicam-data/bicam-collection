"""Fetch and extract visible text from Congress.gov and GovInfo HTML renditions.

This is a resumable, bounded lane.  It only reads HTML-looking links from the
public rendition tables (including GovInfo bill-version packages) and never
requests PDF or XML URLs.  Results are
stored as append-only versions by URL and response hash; the public dbt text
models join the latest successful extraction back to their source link.
"""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
from html.parser import HTMLParser
import json
import os
import re
import time
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse
import uuid

import psycopg
import requests

from bicam_collection.config import Settings


URL_SQL = """
with targets as (
    select formatted_text as url from bicam.bills_texts where formatted_text is not null
    union
    select formatted_text from bicam.committee_reports_texts where formatted_text is not null
    union
    select formatted_text from bicam.hearings_texts where formatted_text is not null
    union
    select html from bicam.amendments_texts where html is not null
    union
    select html from bicam.committee_prints_texts where html is not null
    union
    select formatted_text from bicam.bills_versions where formatted_text is not null
)
select targets.url
from targets
left join public.html_text_current as current on current.url = targets.url
where targets.url !~* '/crec/'
  and (
       current.id is null
       or current.fetched_at < now() - (
       case when current.extraction_status = 'success' then %s else %s end
       * interval '1 day'
       )
  )
order by targets.url
"""
TABLE_TARGET_COLUMNS = {
    "amendments_texts": "html",
    "bills_texts": "formatted_text",
    "committee_prints_texts": "html",
    "committee_reports_texts": "formatted_text",
    "hearings_texts": "formatted_text",
    "bills_versions": "formatted_text",
}
INSERT_SQL = """
insert into html_text_versions
    (id, url, response_hash, content_type, http_status, fetched_at,
     extraction_status, text_content, error)
values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
on conflict (url, response_hash) do nothing
"""
HTML_URL_RE = re.compile(r"(?:\.html?|/htm)(?:[?#]|$)", re.IGNORECASE)
ALLOWED_HOSTS = {"congress.gov", "www.congress.gov", "api.govinfo.gov"}
GOVINFO_HOST = "api.govinfo.gov"
CONGRESS_HOSTS = {"congress.gov", "www.congress.gov"}
BLOCK_TAGS = {
    "address", "article", "aside", "blockquote", "br", "dd", "div", "dl", "dt",
    "fieldset", "figcaption", "figure", "footer", "form", "h1", "h2", "h3", "h4",
    "h5", "h6", "header", "hr", "li", "main", "nav", "ol", "p", "pre", "section",
    "table", "tbody", "td", "tfoot", "th", "thead", "tr", "ul",
}
SKIP_TAGS = {"script", "style", "noscript", "template", "svg"}


def is_supported_html_url(value: str) -> bool:
    parsed = urlparse(value)
    if parsed.scheme != "https" or parsed.hostname not in ALLOWED_HOSTS:
        return False
    # Amendment ``html`` fields currently point to Congressional Record pages,
    # not amendment-text renditions. Congressional Record is intentionally
    # paused in the main-11 scope, so retain those links as source data but do
    # not enqueue them for extraction.
    if parsed.hostname in CONGRESS_HOSTS and "/crec/" in parsed.path.lower():
        return False
    return bool(HTML_URL_RE.search(parsed.path + ("?" + parsed.query if parsed.query else "")))


def request_url_for(value: str) -> str:
    """Add a rotated GovInfo API key only to the outbound request URL.

    Stored/source URLs remain unchanged.  GovInfo HTML renditions are exposed
    as authenticated ``/htm`` API paths, while Congress.gov renditions are
    public and must not receive an unrelated query parameter.
    """

    parsed = urlparse(value)
    if parsed.hostname != GOVINFO_HOST:
        return value
    keys = [key.strip() for key in os.environ.get("GOVINFO_API_KEY", "").split(",") if key.strip()]
    if not keys or any(name.lower() == "api_key" for name, _ in parse_qsl(parsed.query, keep_blank_values=True)):
        return value
    index = int.from_bytes(sha256(value.encode("utf-8")).digest()[:4], "big") % len(keys)
    query = parse_qsl(parsed.query, keep_blank_values=True)
    query.append(("api_key", keys[index]))
    return urlunparse(parsed._replace(query=urlencode(query)))


def resume_urls(urls: list[str], after: str | None) -> list[str]:
    """Apply a cursor without starving the lower-sorting GovInfo host."""

    if not after:
        return urls
    after_host = urlparse(after).hostname
    return [
        url
        for url in urls
        if url > after
        or (after_host in CONGRESS_HOSTS and urlparse(url).hostname == GOVINFO_HOST)
    ]


class VisibleTextParser(HTMLParser):
    """Small dependency-free parser that preserves document block breaks."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self.skip_depth = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = tag.lower()
        if tag in SKIP_TAGS:
            self.skip_depth += 1
        if not self.skip_depth and tag in BLOCK_TAGS:
            self.parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if tag in SKIP_TAGS and self.skip_depth:
            self.skip_depth -= 1
        if not self.skip_depth and tag in BLOCK_TAGS:
            self.parts.append("\n")

    def handle_data(self, data: str) -> None:
        if not self.skip_depth:
            self.parts.append(data)


def extract_visible_text(content: bytes, content_type: str | None = None) -> str:
    charset = None
    if content_type:
        match = re.search(r"charset=([\w.-]+)", content_type, re.IGNORECASE)
        if match:
            charset = match.group(1)
    try:
        decoded = content.decode(charset or "utf-8", errors="replace")
    except LookupError:
        decoded = content.decode("utf-8", errors="replace")
    # Some GovInfo HTML renditions contain embedded NUL control bytes. They
    # are legal in the HTTP payload but PostgreSQL text fields reject them;
    # remove only this non-document control character while retaining the raw
    # response hash and all visible content around it.
    decoded = decoded.replace("\x00", "")
    parser = VisibleTextParser()
    parser.feed(decoded)
    parser.close()
    lines = [re.sub(r"[ \t]+", " ", line).strip() for line in "".join(parser.parts).replace("\r", "").split("\n")]
    output: list[str] = []
    blank = False
    for line in lines:
        if not line:
            if output and not blank:
                output.append("")
            blank = True
            continue
        output.append(line)
        blank = False
    return "\n".join(output).strip()


@dataclass(frozen=True, slots=True)
class FetchResult:
    url: str
    response_hash: str
    content_type: str | None
    http_status: int | None
    extraction_status: str
    text_content: str | None
    error: str | None
    fetched_at: datetime


RETRYABLE_HTTP_STATUSES = {429, 500, 502, 503, 504}


def _fetch_sync(
    url: str,
    timeout: float,
    max_bytes: int,
    *,
    retry: bool = True,
) -> FetchResult:
    """Fetch one URL synchronously.

    Direct callers retain the original three-attempt behavior.  The async
    batch path passes ``retry=False`` and performs retries through its shared
    host limiter instead; otherwise a retry would bypass pacing and amplify
    source-side 429 responses.
    """
    fetched_at = datetime.now(timezone.utc)
    # ``requests`` applies its timeout to socket operations, not to the whole
    # streamed response.  A source that keeps a connection alive while
    # sending bytes can therefore hold one bounded-batch worker forever.  Keep
    # a generous total budget for large HTML pages, but make every extraction
    # demonstrably finite so the scheduler can advance its cursor.
    operation_timeout = max(float(timeout), 0.1)
    overall_deadline = time.monotonic() + max(operation_timeout * 3.0, operation_timeout + 10.0)
    try:
        response = None
        for attempt in range(3 if retry else 1):
            remaining = overall_deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("overall HTML fetch deadline exceeded")
            response = requests.get(
                request_url_for(url),
                headers={"User-Agent": "BICAM-research-html-extractor/1.0"},
                timeout=min(operation_timeout, remaining),
                allow_redirects=True,
                stream=True,
            )
            if response.status_code not in RETRYABLE_HTTP_STATUSES or attempt == 2 or not retry:
                break
            retry_after = response.headers.get("retry-after")
            response.close()
            try:
                delay = min(30.0, max(0.5, float(retry_after))) if retry_after else 2.0 ** attempt
            except ValueError:
                delay = 2.0 ** attempt
            time.sleep(delay)
        assert response is not None
        final_host = urlparse(response.url).hostname
        try:
            if final_host not in ALLOWED_HOSTS:
                raise ValueError(f"redirected to disallowed host {final_host!r}")
            content_length = response.headers.get("content-length")
            if content_length and int(content_length) > max_bytes:
                raise ValueError(f"response exceeds {max_bytes} byte limit")
            chunks: list[bytes] = []
            received = 0
            for chunk in response.iter_content(chunk_size=64 * 1024):
                if time.monotonic() > overall_deadline:
                    raise TimeoutError("overall HTML fetch deadline exceeded while reading response")
                if not chunk:
                    continue
                received += len(chunk)
                if received > max_bytes:
                    raise ValueError(f"response exceeds {max_bytes} byte limit")
                chunks.append(chunk)
            content = b"".join(chunks)
            response_hash = sha256(content).hexdigest()
            content_type = response.headers.get("content-type")
            if response.status_code >= 400:
                return FetchResult(url, response_hash, content_type, response.status_code, "http_error", None, f"HTTP {response.status_code}", fetched_at)
            text = extract_visible_text(content, content_type)
        finally:
            response.close()
        if not text:
            return FetchResult(url, response_hash, content_type, response.status_code, "empty", "", "no visible text", fetched_at)
        return FetchResult(url, response_hash, content_type, response.status_code, "success", text, None, fetched_at)
    except Exception as error:  # network and parser failures are queue evidence
        detail = f"{type(error).__name__}: {error}"
        response_hash = sha256(f"error|{detail}|{fetched_at.isoformat()}".encode()).hexdigest()
        return FetchResult(url, response_hash, None, None, "fetch_error", None, detail, fetched_at)


async def fetch_urls(
    urls: list[str],
    *,
    concurrency: int,
    request_interval: float,
    timeout: float,
    max_bytes: int,
    congress_request_interval: float | None = None,
    govinfo_request_interval: float | None = None,
) -> list[FetchResult]:
    semaphore = asyncio.Semaphore(max(1, concurrency))
    pacing_locks: dict[str, asyncio.Lock] = {}
    next_requests: dict[str, float] = {}

    def pacing_host(url: str) -> str:
        """Return a shared limiter key for equivalent source hostnames."""

        hostname = (urlparse(url).hostname or "").lower()
        if hostname in CONGRESS_HOSTS:
            return "congress.gov"
        return hostname

    def pacing_interval(host: str) -> float:
        if host == "congress.gov" and congress_request_interval is not None:
            return congress_request_interval
        if host == GOVINFO_HOST and govinfo_request_interval is not None:
            return govinfo_request_interval
        return request_interval

    async def one(url: str) -> FetchResult:
        async with semaphore:
            host = pacing_host(url)
            pacing_lock = pacing_locks.setdefault(host, asyncio.Lock())

            async def paced_fetch() -> FetchResult:
                # Every attempt, including a retry, must reserve a slot from
                # the same host limiter.  This prevents concurrent workers
                # from turning a 429 response into an unpaced retry burst.
                async with pacing_lock:
                    now = time.monotonic()
                    delay = max(0.0, next_requests.get(host, 0.0) - now)
                    next_requests[host] = max(now, next_requests.get(host, 0.0)) + max(0.0, pacing_interval(host))
                if delay:
                    await asyncio.sleep(delay)
                return await asyncio.to_thread(_fetch_sync, url, timeout, max_bytes, retry=False)

            result = await paced_fetch()
            for attempt in range(2):
                if result.http_status not in RETRYABLE_HTTP_STATUSES:
                    break
                # Keep the backoff outside the limiter, then reserve another
                # paced slot for the retry.  This is intentionally bounded so
                # one problematic URL cannot monopolize a worker.
                await asyncio.sleep(2.0**attempt)
                result = await paced_fetch()
            return result

    return await asyncio.gather(*(one(url) for url in urls))


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--after", help="Resume after this URL in lexical order.")
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--request-interval", type=float, default=0.25)
    parser.add_argument(
        "--congress-request-interval",
        type=float,
        help="Override the pacing interval for Congress.gov HTML requests.",
    )
    parser.add_argument(
        "--govinfo-request-interval",
        type=float,
        help="Override the pacing interval for GovInfo HTML requests.",
    )
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--max-bytes", type=int, default=20_000_000)
    parser.add_argument(
        "--source-table",
        choices=sorted(TABLE_TARGET_COLUMNS),
        help="Restrict this batch to one public text table (used by the fair scheduler).",
    )
    parser.add_argument(
        "--refresh-days",
        type=float,
        default=30.0,
        help="Refetch successful URLs older than this many days (default: 30).",
    )
    parser.add_argument(
        "--retry-failed-days",
        type=float,
        default=1.0,
        help="Retry non-successful URLs older than this many days (default: 1).",
    )
    parser.add_argument("--database-url")
    return parser.parse_args(argv)


def target_urls(
    database_url: str,
    *,
    after: str | None,
    limit: int,
    refresh_days: float,
    retry_failed_days: float = 1.0,
    source_table: str | None = None,
) -> list[str]:
    if source_table is not None and source_table not in TABLE_TARGET_COLUMNS:
        raise ValueError(f"unsupported source table: {source_table}")
    with psycopg.connect(database_url) as connection:
        if source_table is None:
            query = URL_SQL
        else:
            # The table name and target column are selected from the constant
            # allow-list above; they are never caller-provided SQL fragments.
            column = TABLE_TARGET_COLUMNS[source_table]
            query = f"""
            select distinct source.{column} as url
            from bicam.{source_table} as source
            left join public.html_text_current as current
              on current.url = source.{column}
            where source.{column} is not null
              and source.{column} !~* '/crec/'
              and (
                    current.id is null
                    or current.fetched_at < now() - (
                        case when current.extraction_status = 'success' then %s else %s end
                        * interval '1 day'
                    )
              )
            order by source.{column}
            """
        urls = [
            str(row[0])
            for row in connection.execute(
                query,
                (max(0.0, refresh_days), max(0.0, retry_failed_days)),
            ).fetchall()
        ]
    urls = [url for url in urls if is_supported_html_url(url)]
    urls = resume_urls(urls, after)
    return urls[: max(1, min(limit, 10_000))]


def store_results(database_url: str, results: list[FetchResult]) -> None:
    with psycopg.connect(database_url) as connection:
        with connection.transaction():
            for result in results:
                connection.execute(
                    INSERT_SQL,
                    (
                        uuid.uuid5(uuid.NAMESPACE_URL, result.url + "|" + result.response_hash),
                        result.url,
                        result.response_hash,
                        result.content_type,
                        result.http_status,
                        result.fetched_at,
                        result.extraction_status,
                        result.text_content.replace("\x00", "") if result.text_content is not None else None,
                        result.error,
                    ),
                )


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.limit < 1:
        raise SystemExit("--limit must be positive")
    database_url = args.database_url or Settings.from_env(Path(__file__).resolve().parents[1]).database_url
    urls = target_urls(
        database_url,
        after=args.after,
        limit=args.limit,
        refresh_days=args.refresh_days,
        retry_failed_days=args.retry_failed_days,
        source_table=args.source_table,
    )
    results = asyncio.run(
        fetch_urls(
            urls,
            concurrency=args.concurrency,
            request_interval=args.request_interval,
            congress_request_interval=args.congress_request_interval,
            govinfo_request_interval=args.govinfo_request_interval,
            timeout=args.timeout,
            max_bytes=args.max_bytes,
        )
    )
    store_results(database_url, results)
    counts: dict[str, int] = {}
    for result in results:
        counts[result.extraction_status] = counts.get(result.extraction_status, 0) + 1
    last_url = urls[-1] if urls else args.after
    print(json.dumps({"requested": len(urls), "last_url": last_url, "statuses": counts}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
