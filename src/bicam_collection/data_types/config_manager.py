#!/usr/bin/env python3
"""Centralised configuration management for data-type YAML files.

This supersedes the ad-hoc helpers that lived in *schema_loader.py* by
introducing a **single-responsibility** model hierarchy:

    • ApiConfig        – API-layer behaviour
    • SchemaConfig     – DB schema specifics
    • ProcessingConfig – ETL / pipeline tuning parameters

All three are aggregated into a `DataTypeConfig` which is what the rest of the
codebase should depend on.

A singleton `config_manager` instance is exported at module import time so
call-sites can simply:

    cfg = config_manager.get("bills")

For tests a fresh manager can be spawned or the global cleared via
``config_manager.clear()``.
"""
from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Typed section models
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class ApiConfig:
    outer_field: str
    expected_key: str
    page_size: int = 250
    retry_attempts: int = 3


@dataclass(slots=True)
class SchemaConfig:
    table_name: str
    is_main: bool = False
    create_raw: bool = False
    id_fields: list[str] = field(default_factory=list)
    related_tables: list[str] = field(default_factory=list)
    # Field definitions preserved as opaque mappings for now – to be refined
    fields: list[Mapping[str, Any]] = field(default_factory=list)


@dataclass(slots=True)
class ProcessingConfig:
    checkpoint_frequency: int = 100
    batch_size: int = 1000


@dataclass(slots=True)
class DataTypeConfig:
    # --- meta ---
    name: str
    description: str | None = None

    # --- nested sections ---
    api: ApiConfig = field(default_factory=ApiConfig)
    schema: SchemaConfig = field(default_factory=SchemaConfig)
    processing: ProcessingConfig = field(default_factory=ProcessingConfig)

    # ------------------------------------------------------------------
    # Convenience helpers so callers don't need to drill down every time
    # ------------------------------------------------------------------
    @property
    def id_field(self) -> str:
        """First ID field (most data-types have exactly one)."""
        if not self.schema.id_fields:
            raise ValueError(f"{self.name} config: schema.id_fields is empty")
        return self.schema.id_fields[0]

    @property
    def related_tables(self) -> list[str]:
        return self.schema.related_tables

    @property
    def table_name(self) -> str:
        return self.schema.table_name


# ---------------------------------------------------------------------------
# Config manager implementation
# ---------------------------------------------------------------------------


class ConfigManager:
    """Loads *new-style* YAML files and returns typed config objects."""

    def __init__(self):
        # Cache by absolute file path so re-loading the same file is cheap
        self._file_cache: dict[str, DataTypeConfig] = {}
        # Map *data_type_name* → DataTypeConfig for quick lookup
        self._name_cache: dict[str, DataTypeConfig] = {}

    # -------------------- public API --------------------
    def get(
        self, data_type_name: str, *, search_paths: Iterable[Path] | None = None
    ) -> DataTypeConfig:
        """Return *DataTypeConfig* for *data_type_name*.

        If *search_paths* is *None* we default to locating a
        `config.yaml` under `data_types/congressional/{name}/` (relative to
        this file).
        """
        if data_type_name in self._name_cache:
            return self._name_cache[data_type_name]

        if not search_paths:
            default_dir = (
                Path(__file__).resolve().parent / "congressional" / data_type_name
            )
            search_paths = [default_dir]

        for base in search_paths:
            candidate = base / "config.yaml"
            if candidate.exists():
                cfg = self._load_file(candidate)
                if cfg.name != data_type_name:
                    logger.debug(
                        "Loaded config name (%s) doesn't match request (%s) from %s",
                        cfg.name,
                        data_type_name,
                        candidate,
                    )
                self._name_cache[data_type_name] = cfg
                return cfg

        raise FileNotFoundError(
            f"Could not locate config.yaml for data-type '{data_type_name}'"
        )

    def clear(self) -> None:
        """Empty all caches – useful in tests."""
        self._file_cache.clear()
        self._name_cache.clear()

    # -------------------- internal helpers --------------------
    def _load_file(self, path: Path) -> DataTypeConfig:
        abs_path = str(path.resolve())
        if abs_path in self._file_cache:
            return self._file_cache[abs_path]

        with open(path, encoding="utf-8") as fh:
            raw = yaml.safe_load(fh)

        # Basic structural validation
        try:
            name = raw["name"]
            api_cfg = ApiConfig(**raw["api"])
            schema_cfg = SchemaConfig(**raw["schema"])
            proc_cfg = ProcessingConfig(**raw.get("processing", {}))
        except (TypeError, KeyError) as exc:
            raise ValueError(f"Malformed config file {path}: {exc}") from exc

        cfg = DataTypeConfig(
            name=name,
            description=raw.get("description"),
            api=api_cfg,
            schema=schema_cfg,
            processing=proc_cfg,
        )

        self._file_cache[abs_path] = cfg
        # Also prime name_cache so future lookups are O(1)
        self._name_cache[name] = cfg
        logger.info("Loaded data-type config '%s' from %s", name, path)
        return cfg


# Global singleton instance that most of the code should use
config_manager = ConfigManager()
