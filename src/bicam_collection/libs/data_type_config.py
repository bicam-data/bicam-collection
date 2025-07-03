"""Typed models for data-type YAML configuration.

This module defines *pure-data* dataclasses only – **no I/O**.  Loading and
caching is handled by :pymod:`bicam_collection.libs.data_type_registry`.
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

__all__ = [
    "ApiConfig",
    "SchemaConfig",
    "ProcessingConfig",
    "DataTypeConfig",
]


@dataclass(slots=True)
class ApiConfig:
    outer_field: str | None = None
    expected_key: str | list[str] | None = None
    page_size: int = 250
    retry_attempts: int = 3


@dataclass(slots=True)
class SchemaConfig:
    table_name: str
    is_main: bool = False
    create_raw: bool = False
    id_fields: list[str] = field(default_factory=list)
    related_tables: list[str] = field(default_factory=list)
    fields: list[Mapping[str, Any]] = field(default_factory=list)


@dataclass(slots=True)
class ProcessingConfig:
    checkpoint_frequency: int = 100
    batch_size: int = 1000


@dataclass(slots=True)
class DataTypeConfig:
    # --- meta -----------------------------------------------------------------
    name: str
    description: str | None = None

    # --- nested sections ------------------------------------------------------
    api: ApiConfig = field(default_factory=ApiConfig)
    schema: SchemaConfig = field(default_factory=lambda: SchemaConfig(table_name=""))
    processing: ProcessingConfig = field(default_factory=ProcessingConfig)

    # ------------------------------------------------------------------
    # Convenience helpers so callers don't need to drill down every time
    # ------------------------------------------------------------------
    @property
    def id_field(self) -> str:
        """Return the primary *id* field for the data-type (first in list)."""
        if not self.schema.id_fields:
            raise ValueError(f"{self.name} config: schema.id_fields is empty")
        return self.schema.id_fields[0]

    @property
    def related_tables(self) -> list[str]:
        return self.schema.related_tables

    @property
    def table_name(self) -> str:
        return self.schema.table_name
