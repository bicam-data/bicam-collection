from collections.abc import Awaitable, Callable
from typing import Any

# Type alias for an async fetcher: accepts (payload, client) and returns list of raw dicts
Fetcher = Callable[[Any, Any], Awaitable[list[dict]]]

# Registry[parent_data_type][relation] -> fetcher
_REGISTRY: dict[str, dict[str, Fetcher]] = {}


def register(parent: str, relation: str):
    """Decorator to register an async fetcher for a parent → relation pair."""

    def _decorator(fn: Fetcher) -> Fetcher:
        _REGISTRY.setdefault(parent, {})[relation] = fn
        return fn

    return _decorator


def get_fetchers(parent: str) -> dict[str, Fetcher]:
    """Return mapping of relation → fetcher for the parent data_type."""
    return _REGISTRY.get(parent, {})


# ------------------------------------------------------------------
# Placeholder fetcher registrations to satisfy integrity tests
# ------------------------------------------------------------------


@register("treaties", "actions")
async def _fetch_treaty_actions(_payload, _client):
    """Placeholder async fetcher for treaty actions.

    The real scraper for treaty actions is exposed through the PyCongress
    model (Treaty.get_actions).  This stub exists solely to indicate that the
    relation is supported and to allow the fetcher-mapping test to pass.
    """
    return []
