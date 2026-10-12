from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import sys
from pathlib import Path
from urllib.parse import parse_qs, urlparse

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pytest

from scripts.fetch_html_text import (
    FetchResult,
    TABLE_TARGET_COLUMNS,
    _fetch_sync,
    extract_visible_text,
    fetch_urls,
    is_supported_html_url,
    parse_args,
    request_url_for,
    resume_urls,
    target_urls,
)
import yaml


class _FakeResponse:
    def __init__(self, content: bytes, *, content_length: str | None = None) -> None:
        self.url = "https://www.congress.gov/example.htm"
        self.status_code = 200
        self.headers = {
            "content-type": "text/html; charset=utf-8",
            **({"content-length": content_length} if content_length is not None else {}),
        }
        self._content = content
        self.closed = False

    def iter_content(self, *, chunk_size: int):
        del chunk_size
        midpoint = max(1, len(self._content) // 2)
        return iter((self._content[:midpoint], self._content[midpoint:]))

    def close(self) -> None:
        self.closed = True


def test_supported_urls_are_congress_html_only() -> None:
    assert is_supported_html_url("https://www.congress.gov/117/bills/hr1/BILLS-117hr1ih.htm")
    assert is_supported_html_url("https://congress.gov/109/chrg/x/generated/item.html?download=1")
    assert is_supported_html_url("https://api.govinfo.gov/packages/CHRG-104hhrg1/granules/CHRG-104hhrg1/htm")
    assert not is_supported_html_url(
        "https://www.congress.gov/117/crec/2023/01/01/169/1/modified/CREC-2023-01-01-pt1-PgH1.htm"
    )
    assert not is_supported_html_url("https://www.congress.gov/117/bills/hr1/BILLS-117hr1ih.pdf")
    assert not is_supported_html_url("https://api.govinfo.gov/packages/CHRG-104hhrg1/summary")
    assert not is_supported_html_url("http://www.congress.gov/117/bills/hr1/BILLS-117hr1ih.htm")
    assert not is_supported_html_url("https://example.org/item.htm")


def test_target_query_excludes_paused_congressional_record_links() -> None:
    from scripts.fetch_html_text import URL_SQL

    assert "targets.url !~* '/crec/'" in URL_SQL
    assert "and (\n       current.id is null" in URL_SQL


def test_visible_text_strips_non_document_markup_and_decodes_entities() -> None:
    html = b"<html><head><style>ignore</style></head><body><h1>Title</h1><script>ignore</script><p>Hello &amp; world</p><pre>A\nB</pre></body></html>"
    assert extract_visible_text(html, "text/html; charset=utf-8") == "Title\n\nHello & world\n\nA\nB"


def test_visible_text_removes_govinfo_nul_control_bytes() -> None:
    html = b"<html><body><p>Before\x00After</p></body></html>"
    assert extract_visible_text(html, "text/html; charset=utf-8") == "BeforeAfter"


def test_source_table_selector_is_explicit() -> None:
    assert parse_args(["--source-table", "bills_texts"]).source_table == "bills_texts"
    assert parse_args(["--source-table", "bills_versions"]).source_table == "bills_versions"
    assert parse_args(["--retry-failed-days", "2"]).retry_failed_days == 2
    with pytest.raises(ValueError, match="unsupported source table"):
        target_urls("postgresql://unused", after=None, limit=1, refresh_days=30, source_table="unknown")


def test_contract_html_sources_are_registered_with_extractor() -> None:
    contract_path = Path(__file__).resolve().parents[1] / "contracts" / "bicam_public_schema.yaml"
    contract = yaml.safe_load(contract_path.read_text(encoding="utf-8"))
    relations = []
    for family in contract["families"]:
        relations.extend([family, *family.get("children", [])])
    relations.extend(contract.get("supplemental_relations", []))

    contract_sources = {
        relation["table"]
        for relation in relations
        if isinstance(relation.get("columns"), dict)
        and {"html", "formatted_text"}.intersection(relation["columns"])
    }

    assert contract_sources == set(TABLE_TARGET_COLUMNS)


def test_govinfo_request_key_is_not_persisted_in_source_url(monkeypatch) -> None:
    monkeypatch.setenv("GOVINFO_API_KEY", "first-key,second-key")
    source_url = "https://api.govinfo.gov/packages/CHRG-104hhrg1/granules/CHRG-104hhrg1/htm"
    outbound = request_url_for(source_url)
    assert outbound.startswith(source_url + "?")
    assert parse_qs(urlparse(outbound).query)["api_key"][0] in {"first-key", "second-key"}
    assert request_url_for("https://www.congress.gov/117/bills/hr1/BILLS-117hr1ih.htm") == "https://www.congress.gov/117/bills/hr1/BILLS-117hr1ih.htm"


def test_congress_cursor_does_not_starve_govinfo_urls() -> None:
    congress_url = "https://www.congress.gov/108/bills/hr1/BILLS-108hr1ih.htm"
    govinfo_url = "https://api.govinfo.gov/packages/CHRG-104hhrg1/granules/CHRG-104hhrg1/htm"
    assert resume_urls([govinfo_url, congress_url], congress_url) == [govinfo_url]


def test_fetch_streams_bounded_content_and_closes_response(monkeypatch) -> None:
    response = _FakeResponse(b"<html><body><p>hello</p></body></html>")
    calls: dict[str, object] = {}

    def fake_get(*args, **kwargs):
        calls.update(kwargs)
        return response

    monkeypatch.setattr("scripts.fetch_html_text.requests.get", fake_get)
    result = _fetch_sync("https://www.congress.gov/example.htm", timeout=1, max_bytes=1_000)
    assert result.extraction_status == "success"
    assert result.text_content == "hello"
    assert calls["stream"] is True
    assert response.closed


def test_fetch_quarantines_real_identity_conflict(monkeypatch) -> None:
    body = (Path(__file__).parent / 'fixtures/bill_source_identity/hres15-109.contradictory.html').read_bytes()
    response = _FakeResponse(body)
    monkeypatch.setattr("scripts.fetch_html_text.requests.get", lambda *args, **kwargs: response)
    result = _fetch_sync("https://www.congress.gov/109/bills/hres15/BILLS-109hres15ih.htm", timeout=1, max_bytes=10000)
    assert result.extraction_status == "identity_conflict"
    assert result.text_content is None
    assert 'body_congress' in result.error
    assert result.http_status == 200
    assert response.closed


def test_fetch_rejects_content_length_before_reading(monkeypatch) -> None:
    response = _FakeResponse(b"0123456789", content_length="10")
    monkeypatch.setattr("scripts.fetch_html_text.requests.get", lambda *args, **kwargs: response)
    result = _fetch_sync("https://www.congress.gov/example.htm", timeout=1, max_bytes=5)
    assert result.extraction_status == "fetch_error"
    assert "exceeds 5 byte limit" in (result.error or "")
    assert response.closed


def test_host_specific_html_pacing_options_are_parseable() -> None:
    args = parse_args([
        "--congress-request-interval", "1.0",
        "--govinfo-request-interval", "0.25",
    ])
    assert args.congress_request_interval == 1.0
    assert args.govinfo_request_interval == 0.25


def test_async_retries_use_the_paced_fetch_path(monkeypatch) -> None:
    calls: list[bool] = []
    statuses = iter((429, 200))

    def fake_fetch(url: str, timeout: float, max_bytes: int, *, retry: bool) -> FetchResult:
        del timeout, max_bytes
        calls.append(retry)
        status = next(statuses)
        now = datetime.now(timezone.utc)
        return FetchResult(
            url=url,
            response_hash=f"hash-{status}",
            content_type="text/html",
            http_status=status,
            extraction_status="success" if status == 200 else "http_error",
            text_content="ok" if status == 200 else None,
            error=None if status == 200 else f"HTTP {status}",
            fetched_at=now,
        )

    monkeypatch.setattr("scripts.fetch_html_text._fetch_sync", fake_fetch)
    result = asyncio.run(
        fetch_urls(
            ["https://www.congress.gov/example.htm"],
            concurrency=1,
            request_interval=0,
            congress_request_interval=0,
            timeout=1,
            max_bytes=1_000,
        )
    )

    assert result[0].http_status == 200
    # The async path owns retry pacing; the synchronous worker is invoked one
    # attempt at a time and never performs an unpaced retry internally.
    assert calls == [False, False]
