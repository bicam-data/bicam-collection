from __future__ import annotations

from pathlib import Path
import re
from typing import Any

import yaml


VALID_WORKFLOWS = {"ingestion_only", "reconciliation", "evidence_ingestion"}
VALID_BULK_MODES = {False, "optional_incomplete"}
ENTITY_NAME_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")
CANONICAL_COLUMN_PATTERN = re.compile(r"^canonical_[a-z0-9_]+$")


def load_contract_document(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as handle:
        document = yaml.safe_load(handle)
    if not isinstance(document, dict):
        raise ValueError(f"Contract document must be a mapping: {path}")
    return document


def validate_entity_contract(document: dict[str, Any], *, path: Path | None = None) -> None:
    """Validate one source-neutral entity contract before dbt consumes it."""
    location = f" ({path})" if path else ""
    entity = document.get("entity")
    if not isinstance(entity, str) or not ENTITY_NAME_PATTERN.fullmatch(entity):
        raise ValueError(f"invalid entity name{location}: {entity!r}")
    if document.get("version") != 1:
        raise ValueError(f"entity contract version must be 1{location}")

    identity = document.get("identity")
    if not isinstance(identity, dict):
        raise ValueError(f"entity identity must be a mapping{location}")
    components = identity.get("components")
    if (
        not isinstance(components, list)
        or not components
        or any(not isinstance(component, str) or not component for component in components)
        or len(components) != len(set(components))
    ):
        raise ValueError(f"entity identity components must be unique non-empty strings{location}")
    if not isinstance(identity.get("canonical_format"), str) or not identity["canonical_format"]:
        raise ValueError(f"entity identity needs canonical_format{location}")
    canonical_column = identity.get("canonical_column")
    if not isinstance(canonical_column, str) or not CANONICAL_COLUMN_PATTERN.fullmatch(canonical_column):
        raise ValueError(f"entity identity needs canonical_column canonical_*{location}")

    sources = document.get("sources")
    if not isinstance(sources, dict) or not sources:
        raise ValueError(f"entity sources must be a non-empty mapping{location}")
    for source_name, source_config in sources.items():
        if not isinstance(source_name, str) or not isinstance(source_config, dict):
            raise ValueError(f"invalid entity source mapping{location}")
        for required in ("model", "natural_key"):
            if not isinstance(source_config.get(required), str) or not source_config[required]:
                raise ValueError(f"entity source {source_name} needs {required}{location}")

    workflow = document.get("workflow")
    if workflow not in VALID_WORKFLOWS:
        raise ValueError(f"invalid entity workflow{location}: {workflow!r}")
    matching = document.get("matching")
    if workflow == "reconciliation":
        if len(sources) < 2 or not isinstance(matching, dict):
            raise ValueError(f"reconciliation entity needs multiple sources and matching rules{location}")
        if not matching.get("automatic_rule"):
            raise ValueError(f"reconciliation entity needs automatic_rule{location}")
        if matching.get("fuzzy_match_requires_review") is not True:
            raise ValueError(f"fuzzy matching must require review{location}")
    elif matching is not None:
        raise ValueError(f"non-reconciliation entity cannot define matching rules{location}")

    quality = document.get("quality")
    if not isinstance(quality, dict) or "max_missing_canonical_identity" not in quality:
        raise ValueError(f"entity quality needs max_missing_canonical_identity{location}")


def validate_entity_contract_registry(directory: Path) -> dict[str, dict[str, Any]]:
    contracts: dict[str, dict[str, Any]] = {}
    for path in sorted(directory.glob("*.yaml")):
        document = load_contract_document(path)
        validate_entity_contract(document, path=path)
        entity = document["entity"]
        if entity in contracts:
            raise ValueError(f"duplicate entity contract: {entity}")
        contracts[entity] = document
    if not contracts:
        raise ValueError(f"no entity contracts found: {directory}")
    return contracts


def render_canonical_id(contract: dict[str, Any], values: dict[str, Any]) -> str | None:
    """Render a contract's canonical format for fixture and rule validation."""
    identity = contract["identity"]
    if any(values.get(component) in (None, "") for component in identity["components"]):
        return None
    rendered = identity["canonical_format"]
    optional_errata = "e" if values.get("is_errata") in (True, "true", "t", "1", 1) else ""
    rendered = rendered.replace("[e]", optional_errata)
    for component in identity["components"]:
        value = values[component]
        if component.endswith("_type") or component == "report_type":
            value = str(value).lower()
        rendered = rendered.replace("{" + component + "}", str(value))
    return rendered


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
        if config.get("completeness") not in {"sampled", "observed_window", "full"}:
            raise ValueError(f"invalid completeness mode: {category}")


def validate_schedule(document: dict[str, Any], spec_categories: set[str]) -> None:
    if document.get("version") != 1:
        raise ValueError("schedule version must be 1")
    if not isinstance(document.get("lock_key"), str) or not document["lock_key"]:
        raise ValueError("schedule lock_key must be non-empty")
    jobs = document.get("jobs")
    if not isinstance(jobs, dict) or not jobs:
        raise ValueError("schedule jobs must be a non-empty mapping")
    for job_name, job in jobs.items():
        if not isinstance(job_name, str) or not isinstance(job, dict):
            raise ValueError(f"invalid schedule job: {job_name}")
        categories = job.get("categories")
        if not isinstance(categories, list) or not categories or not set(categories).issubset(spec_categories):
            raise ValueError(f"schedule job has unknown/empty categories: {job_name}")
        if job.get("mode") not in {"resume", "incremental", "refresh"}:
            raise ValueError(f"schedule job mode must be bounded: {job_name}")
        # A low page cap is unsafe for incremental windows: advancing a
        # category high-water mark after only the first few update-sorted
        # pages can strand older records from the same window permanently.
        # Keep the crawl bounded, but allow enough pages to reach the API's
        # terminal pagination URL within one lock-held batch.
        for key, upper_bound in (("max_pages", 1000), ("page_size", 1000), ("concurrency", 8), ("category_workers", 2)):
            value = job.get(key)
            if not isinstance(value, int) or value < 1 or value > upper_bound:
                raise ValueError(f"schedule job {job_name} has invalid {key}: {value!r}")
        overlap_hours = job.get("overlap_hours", 0)
        if not isinstance(overlap_hours, (int, float)) or overlap_hours < 0 or overlap_hours > 168:
            raise ValueError(f"schedule job {job_name} has invalid overlap_hours: {overlap_hours!r}")
        if not isinstance(job.get("dbt_select"), str) or not job["dbt_select"]:
            raise ValueError(f"schedule job needs dbt_select: {job_name}")
        if job.get("quality_gate") is not True:
            raise ValueError(f"schedule job must enable quality_gate: {job_name}")
        missing_scope = job.get("max_missing_full_scope_partitions", 0)
        if not isinstance(missing_scope, int) or missing_scope < 0:
            raise ValueError(f"schedule job has invalid max_missing_full_scope_partitions: {job_name}")
