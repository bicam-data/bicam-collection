from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml


@dataclass(slots=True)
class EdgeSpec:
    target: str
    url_path: str
    follow_mode: str
    fallback_url_suffix: str | None = None
    url_normalizer: str | None = None


@dataclass(slots=True)
class NodeSpec:
    name: str
    data_path: str | None = None
    item_path: str | None = None
    high_water_mark_path: str | None = None
    pagination_path: str | None = None
    next_page_path: str | None = None
    default_query_params: dict[str, str | int] = field(default_factory=dict)
    incremental_query_params: list[str] = field(default_factory=list)
    child_edges: list[EdgeSpec] = field(default_factory=list)
    linked_item_edges: list[EdgeSpec] = field(default_factory=list)


@dataclass(slots=True)
class SeedSpec:
    path: str
    node_type: str
    requires_partition: bool = False


@dataclass(slots=True)
class CrawlSpec:
    category: str
    source: str
    url_allowlist_hosts: list[str]
    seed: SeedSpec
    refresh_root_node_type: str | None
    nodes: dict[str, NodeSpec]


def _load_edges(raw_edges: list[dict[str, str]] | None) -> list[EdgeSpec]:
    if not raw_edges:
        return []
    edges = [
        EdgeSpec(
            target=edge["target"],
            url_path=edge["url_path"],
            follow_mode=edge["follow_mode"],
            fallback_url_suffix=edge.get("fallback_url_suffix"),
            url_normalizer=edge.get("url_normalizer"),
        )
        for edge in raw_edges
    ]
    supported_normalizers = {None, "congress_treaty_committees"}
    for edge in edges:
        if edge.url_normalizer not in supported_normalizers:
            raise ValueError(f"unsupported edge URL normalizer: {edge.url_normalizer!r}")
    return edges


def load_spec(path: Path) -> CrawlSpec:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    nodes: dict[str, NodeSpec] = {}
    for node_name, raw_node in payload["nodes"].items():
        raw_node = raw_node or {}
        nodes[node_name] = NodeSpec(
            name=node_name,
            data_path=raw_node.get("data_path"),
            item_path=raw_node.get("item_path"),
            high_water_mark_path=raw_node.get("high_water_mark_path"),
            pagination_path=raw_node.get("pagination_path"),
            next_page_path=raw_node.get("next_page_path"),
            default_query_params=raw_node.get("default_query_params", {}),
            incremental_query_params=raw_node.get("incremental_query_params", []),
            child_edges=_load_edges(raw_node.get("child_edges")),
            linked_item_edges=_load_edges(raw_node.get("linked_item_edges")),
        )
    if payload["seed"]["node_type"] not in nodes:
        raise ValueError(f"seed node type is not defined: {payload['seed']['node_type']}")
    refresh_root_node_type = payload.get("refresh_root_node_type")
    if refresh_root_node_type and refresh_root_node_type not in nodes:
        raise ValueError(f"refresh root node type is not defined: {refresh_root_node_type}")
    for node_name, node in nodes.items():
        for edge in [*node.child_edges, *node.linked_item_edges]:
            if edge.target not in nodes:
                raise ValueError(f"node '{node_name}' references undefined child node '{edge.target}'")
    return CrawlSpec(
        category=payload["category"],
        source=payload["source"],
        url_allowlist_hosts=payload["url_allowlist_hosts"],
        seed=SeedSpec(
            path=payload["seed"]["path"],
            node_type=payload["seed"]["node_type"],
            requires_partition=bool(payload["seed"].get("requires_partition", False)),
        ),
        refresh_root_node_type=refresh_root_node_type,
        nodes=nodes,
    )


def discover_spec_categories(specs_dir: Path) -> list[str]:
    return sorted(path.stem for path in specs_dir.glob("*.yaml") if path.is_file())


def resolve_spec_path(specs_dir: Path, category: str) -> Path:
    path = specs_dir / f"{category}.yaml"
    if not path.exists():
        raise FileNotFoundError(f"Spec not found for category '{category}': {path}")
    return path
