from __future__ import annotations

import re
from collections.abc import Mapping
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse


def _sorted_query(query: str) -> str:
    # Pagination links arrive with the same parameters in different orders,
    # and the URL is the storage key, so sort them (keeping repeats).
    return urlencode(sorted(parse_qsl(query, keep_blank_values=True)), doseq=True)


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
        query=_sorted_query(parsed.query),
        fragment="",
    )
    return urlunparse(normalized)


def with_query_params(url: str, params: Mapping[str, str | int | None]) -> str:
    parsed = urlparse(url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    for key, value in params.items():
        if value is None:
            continue
        query.setdefault(key, str(value))
    return urlunparse(parsed._replace(query=_sorted_query(urlencode(query, doseq=True))))


def replace_query_params(url: str, params: Mapping[str, str | int | None]) -> str:
    parsed = urlparse(url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    for key, value in params.items():
        if value is None:
            query.pop(key, None)
            continue
        query[key] = str(value)
    return urlunparse(parsed._replace(query=_sorted_query(urlencode(query, doseq=True))))


def _remove_query_parameters(url: str, names: set[str]) -> str:
    parsed = urlparse(url)
    excluded = {name.lower() for name in names}
    query = [
        (key, value)
        for key, value in parse_qsl(parsed.query, keep_blank_values=True)
        if key.lower() not in excluded
    ]
    return urlunparse(
        parsed._replace(query=_sorted_query(urlencode(query, doseq=True)))
    )


def append_url_path(url: str, suffix: str) -> str:
    """Append a safe path suffix while preserving the URL's query string."""
    normalized_suffix = suffix.strip().strip("/")
    if not normalized_suffix:
        raise ValueError("URL path suffix must not be empty")
    parsed = urlparse(url)
    path = parsed.path.rstrip("/") + "/" + normalized_suffix
    return urlunparse(parsed._replace(path=path))


def normalize_congress_treaty_committees_url(url: str) -> str:
    """Strip a trailing slash but keep the optional treaty part (``/treaty/118/1/A/committees``);
    Congress.gov 404s some multipart treaties without it."""
    parsed = urlparse(url)
    path = re.sub(r"/committees/+$", "/committees", parsed.path)
    if not re.search(
        r"/treaty/[0-9]+/[0-9]+(?:/[^/]+)?/committees$", path
    ):
        raise ValueError(f"Unexpected treaty committees URL: {url}")
    return urlunparse(parsed._replace(path=path))


def canonicalize_congress_api_url(url: str) -> str:
    # Canonical URLs are permanent storage keys, so a key passed as a query
    # parameter must never end up in one.
    without_credentials = _remove_query_parameters(
        normalize_url(url), {"api_key", "apikey"}
    )
    return replace_query_params(without_credentials, {"format": "json"})


def canonicalize_govinfo_url(url: str) -> str:
    return replace_query_params(normalize_url(url), {"api_key": None})


def canonicalize_url(url: str, source: str) -> str:
    if source == "congress":
        return canonicalize_congress_api_url(url)
    if source == "govinfo":
        return canonicalize_govinfo_url(url)
    raise ValueError(f"Unsupported source '{source}'")


def congress_source_fallback_url(url: str, *, node_type: str) -> str | None:
    """Reorder a hearing-list query to ``offset,limit,format``.

    Some hearing-list offsets return a NoReverseMatch 500 in canonical order
    but succeed in the order older Congress links used. Returns None for any
    other endpoint.
    """
    if node_type != "hearing_list":
        return None
    parsed = urlparse(url)
    if parsed.path.rstrip("/") != "/v3/hearing":
        return None
    pairs = parse_qsl(parsed.query, keep_blank_values=True)
    names = {key for key, _ in pairs}
    if not {"offset", "limit", "format"}.issubset(names):
        return None
    preferred = {"offset": 0, "limit": 1, "format": 2}
    ordered = sorted(
        enumerate(pairs),
        key=lambda item: (preferred.get(item[1][0], 3), item[1][0], item[1][1], item[0]),
    )
    query = urlencode([pair for _, pair in ordered], doseq=True)
    fallback = urlunparse(parsed._replace(query=query))
    return fallback if fallback != url else None
