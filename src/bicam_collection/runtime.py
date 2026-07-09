from __future__ import annotations

from pathlib import Path
from typing import Any

from bicam_collection.client import AsyncCongressClient, AsyncGovInfoClient, CongressClient, GovInfoClient
from bicam_collection.config import Settings
from bicam_collection.spec_loader import CrawlSpec


def base_url_for_spec(settings: Settings, spec: CrawlSpec) -> str:
    return settings.api_base_url_for_source(spec.source)


def build_client(*, spec: CrawlSpec, env_path: Path, use_async: bool) -> Any:
    if spec.source == "congress":
        return AsyncCongressClient(env_path=env_path) if use_async else CongressClient(env_path=env_path)
    if spec.source == "govinfo":
        return AsyncGovInfoClient(env_path=env_path) if use_async else GovInfoClient(env_path=env_path)
    raise ValueError(f"Unsupported source '{spec.source}'")
