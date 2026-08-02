from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


VALID_WORKFLOWS = {"ingestion_only", "reconciliation", "evidence_ingestion"}
VALID_BULK_MODES = {False, "optional_incomplete"}


def load_contract_document(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as handle:
        document = yaml.safe_load(handle)
    if not isinstance(document, dict):
        raise ValueError(f"Contract document must be a mapping: {path}")
    return document


def validate_source_strategy(document: dict[str, Any], spec_categories: set[str]) -> None:
    if document.get("version") != 1:
        raise ValueError("source strategy version must be 1")
    categories = document.get("categories")
    if not isinstance(categories, dict):
        raise ValueError("source strategy categories must be a mapping")
    if set(categories) != spec_categories:
        missing = sorted(spec_categories - set(categories))
        extra = sorted(set(categories) - spec_categories)
        raise ValueError(f"source strategy categories differ from specs: missing={missing}, extra={extra}")
    for category, config in categories.items():
        if not isinstance(config, dict):
            raise ValueError(f"source strategy entry must be a mapping: {category}")
        if not config.get("entity") or not config.get("authoritative_source"):
            raise ValueError(f"source strategy entry lacks entity/source: {category}")
        if config.get("workflow") not in VALID_WORKFLOWS:
            raise ValueError(f"invalid workflow for {category}: {config.get('workflow')}")
        if config.get("bulk_bootstrap") not in VALID_BULK_MODES:
            raise ValueError(f"invalid bulk_bootstrap mode for {category}")
        secondary = config.get("secondary_sources", [])
        if not isinstance(secondary, list):
            raise ValueError(f"secondary_sources must be a list: {category}")
        if config["workflow"] == "reconciliation" and not secondary:
            raise ValueError(f"reconciliation category needs a secondary source: {category}")
        if config["workflow"] == "ingestion_only" and secondary:
            raise ValueError(f"ingestion-only category cannot declare secondary sources: {category}")


def validate_collection_scope(document: dict[str, Any], strategy_categories: set[str]) -> None:
    if document.get("version") != 1:
        raise ValueError("collection scope version must be 1")
    categories = document.get("categories")
    if not isinstance(categories, dict):
        raise ValueError("collection scope categories must be a mapping")
    if not set(categories).issubset(strategy_categories):
        raise ValueError("collection scope contains an unknown category")
    for category, config in categories.items():
        congresses = config.get("congresses")
        if not isinstance(congresses, list) or not congresses or any(not isinstance(value, int) for value in congresses):
            raise ValueError(f"scope congresses must be a non-empty integer list: {category}")
        if len(congresses) != len(set(congresses)):
            raise ValueError(f"scope congresses must be unique: {category}")
        if config.get("completeness") not in {"sampled", "observed_window"}:
            raise ValueError(f"invalid completeness mode: {category}")
