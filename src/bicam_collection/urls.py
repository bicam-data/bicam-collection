from __future__ import annotations

from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse


def build_seed_url(base_url: str, path: str) -> str:
    return urljoin(base_url.rstrip("/") + "/", path.lstrip("/"))


def is_allowed_host(url: str, allowed_hosts: list[str]) -> bool:
    parsed = urlparse(url)
    hostname = (parsed.hostname or "").lower()
    return hostname in {host.lower() for host in allowed_hosts}


def normalize_url(url: str) -> str:
    parsed = urlparse(url)
    if not parsed.netloc:
        raise ValueError(f"Expected absolute URL, got: {url}")
    normalized = parsed._replace(
        scheme="https",
        netloc=parsed.netloc.lower(),
        fragment="",
    )
    return urlunparse(normalized)


def with_query_params(url: str, params: dict[str, str | int | None]) -> str:
    parsed = urlparse(url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    for key, value in params.items():
        if value is None:
            continue
        query.setdefault(key, str(value))
    return urlunparse(parsed._replace(query=urlencode(query, doseq=True)))


def replace_query_params(url: str, params: dict[str, str | int | None]) -> str:
    parsed = urlparse(url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    for key, value in params.items():
        if value is None:
            query.pop(key, None)
            continue
        query[key] = str(value)
    return urlunparse(parsed._replace(query=urlencode(query, doseq=True)))


def canonicalize_congress_api_url(url: str) -> str:
    return replace_query_params(normalize_url(url), {"format": "json"})


def canonicalize_govinfo_url(url: str) -> str:
    return replace_query_params(normalize_url(url), {"api_key": None})


def canonicalize_url(url: str, source: str) -> str:
    if source == "congress":
        return canonicalize_congress_api_url(url)
    if source == "govinfo":
        return canonicalize_govinfo_url(url)
    raise ValueError(f"Unsupported source '{source}'")
