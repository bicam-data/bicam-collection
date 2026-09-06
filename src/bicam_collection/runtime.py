from __future__ import annotations

from pathlib import Path
from typing import Any

from bicam_collection.client import AsyncCongressClient, AsyncGovInfoClient, CongressClient, GovInfoClient
from bicam_collection.config import Settings
from bicam_collection.spec_loader import CrawlSpec

_CLIENT_TYPES: dict[str, tuple[type[Any], type[Any]]] = {
    "congress": (CongressClient, AsyncCongressClient),
    "govinfo": (GovInfoClient, AsyncGovInfoClient),
}


def base_url_for_spec(settings: Settings, spec: CrawlSpec) -> str:
    return settings.api_base_url_for_source(spec.source)


def build_client(
    *,
    spec: CrawlSpec,
    env_path: Path,
    use_async: bool,
    key_wait_timeout: float | None = None,
    request_interval: float = 0.0,
    active_key_budget: int | None = None,
) -> Any:
    if spec.source not in _CLIENT_TYPES:
        raise ValueError(f"Unsupported source '{spec.source}'")
    sync_type, async_type = _CLIENT_TYPES[spec.source]
    client_type = async_type if use_async else sync_type
    return client_type(
        env_path=env_path,
        key_wait_timeout=key_wait_timeout,
        request_interval=request_interval,
        active_key_budget=active_key_budget,
    )
