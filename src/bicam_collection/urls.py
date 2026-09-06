from __future__ import annotations

import re
from collections.abc import Mapping
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse


def _sorted_query(query: str) -> str:
    """Return a deterministic encoding for an URL query string.

    Congress.gov pagination links have been observed with the same parameters
    in different orders (for example ``offset,limit,format`` versus
    ``offset,format,limit``).  Query order is not meaningful for these APIs,
    but it is part of the stored URL key, so normalize it before deduplication.
    Keep repeated parameters rather than collapsing them while sorting by key
    and value.
    """

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
    """Remove query parameters by name without treating case as significant."""
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
    """Canonicalize a treaty-committee child without losing its treaty part.

    Most treaty routes are ``/treaty/{congress}/{number}/committees``, but
    multipart treaties use ``/treaty/{congress}/{number}/{part}/committees``.
    The latter is not equivalent to the unsuffixed route: Congress.gov returns
    404 for some multipart treaties when the part (for example ``A`` or ``B``)
    is omitted.  Preserve whichever route the detail payload or fallback
    parent supplied while removing only a redundant trailing slash.
    """
    parsed = urlparse(url)
    path = re.sub(r"/committees/+$", "/committees", parsed.path)
    if not re.search(
        r"/treaty/[0-9]+/[0-9]+(?:/[^/]+)?/committees$", path
    ):
        raise ValueError(f"Unexpected treaty committees URL: {url}")
    return urlunparse(parsed._replace(path=path))


def canonicalize_congress_api_url(url: str) -> str:
    # Congress authentication is normally sent in the ``X-Api-Key`` header,
    # but the public API also accepts a query parameter.  Canonical URLs are
    # persisted as durable raw-archive keys, so never allow a credential to
    # become part of that identity if a caller supplies one.
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
    """Return a source-compatible retry spelling for a known Congress defect.

    The hearing list endpoint has, at specific offsets, returned a Django
    ``NoReverseMatch`` 500 when its query parameters are sent in the normal
    canonical order (``format,limit,offset``).  The same page is served when
    the parameters use the order emitted by older Congress links
    (``offset,limit,format``).  Keep canonical URLs stable for storage and
    deduplication, but allow the crawler one bounded request fallback when the
    source explicitly identifies this error.  Other endpoints and query
    shapes intentionally return no fallback.
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
