"""Data-type registry – split out of *data_type_router.py* so that
router (Dagster-facing factory) and registry (metadata store) live in
separate modules.

The registry now exposes ``get_data_type_config`` which returns the
typed :class:`bicam_collection.libs.data_type_config.DataTypeConfig`
objects parsed directly from the YAML *config.yaml* files that live
next to each data-type package.
"""

from __future__ import annotations

import dataclasses
import logging
from pathlib import Path

import yaml

from .data_type_config import (
    ApiConfig,
    DataTypeConfig,
    ProcessingConfig,
    SchemaConfig,
)

logger = logging.getLogger(__name__)

# forward refs (avoid circular until we split fully)
AbstractFetcher = "AbstractFetcher"
AbstractDatabaseNormalizer = "AbstractDatabaseNormalizer"
AbstractCleaner = "AbstractCleaner"


class DataTypeRegistry:  # noqa: R0902 – central singleton, lots of stuff
    """Central store for data-type → component mapping & metadata."""

    def __init__(self):
        # data_type → (fetcher, normalizer, cleaner, config_file, data_source)
        self._registry: dict[
            str,
            tuple[
                type[AbstractFetcher],
                type[AbstractDatabaseNormalizer],
                type[AbstractCleaner],
                str,
                str,
            ],
        ] = {}

    # ------------------------------------------------------------------
    # Registration helpers
    # ------------------------------------------------------------------
    def register_data_type(
        self,
        data_type: str,
        *,
        fetcher_class: type[AbstractFetcher],
        normalizer_class: type[AbstractDatabaseNormalizer],
        cleaner_class: type[AbstractCleaner],
        config_file: str,
        data_source: str,
    ) -> None:
        self._registry[data_type] = (
            fetcher_class,
            normalizer_class,
            cleaner_class,
            config_file,
            data_source,
        )
        logger.info("Registered data-type %s (source=%s)", data_type, data_source)

    # ------------------------------------------------------------------
    # Accessors
    # ------------------------------------------------------------------
    def _get_entry(self, data_type: str):
        if data_type not in self._registry:
            raise ValueError(
                f"Data-type '{data_type}' not registered – available: {list(self._registry)}"
            )
        return self._registry[data_type]

    def get_components(self, data_type: str):
        f, n, c, _, _ = self._get_entry(data_type)
        return f, n, c

    def get_config_file(self, data_type: str) -> str:
        _, _, _, cfg, _ = self._get_entry(data_type)
        return cfg

    def get_data_source(self, data_type: str) -> str:
        _, _, _, _, src = self._get_entry(data_type)
        return src

    # ------------------------------------------------------------------
    # New helper – DataTypeConfig integration
    # ------------------------------------------------------------------
    # ------------------------------ config cache -------------------------
    _config_cache: dict[str, DataTypeConfig] = {}

    def get_data_type_config(self, data_type: str) -> DataTypeConfig:
        """Return **typed** configuration for *data_type* (load & cache lazily)."""

        if data_type in self._config_cache:
            return self._config_cache[data_type]

        cfg_file_path = Path(self.get_config_file(data_type)).resolve()

        candidate_paths: list[Path] = [cfg_file_path]

        # Fallback to conventional location if the registered path does not exist
        if not cfg_file_path.exists():
            fallback = (
                Path(__file__).resolve().parent.parent
                / "data_types"
                / "congressional"
                / data_type
                / "config.yaml"
            )
            candidate_paths.append(fallback)

        for path in candidate_paths:
            if not path.exists():
                continue
            try:
                cfg = self._load_config_from_yaml(path, data_type)
                self._config_cache[data_type] = cfg
                return cfg
            except Exception as exc:
                logger.debug("Failed to parse %s: %s", path, exc, exc_info=True)

        raise FileNotFoundError(
            f"Could not locate valid config for data-type '{data_type}' (looked in: {candidate_paths})"
        )

    # ------------------------------------------------------------------
    # YAML loader helpers (internal)
    # ------------------------------------------------------------------
    def _load_config_from_yaml(self, path: Path, target_name: str) -> DataTypeConfig:
        """Read *path* and return :class:`DataTypeConfig` matching *target_name*."""
        with open(path, encoding="utf-8") as fh:
            raw_yaml = yaml.safe_load(fh)

        if isinstance(raw_yaml, list):
            # Multi-config file – pick the one matching *target_name*
            for entry in raw_yaml:
                if not isinstance(entry, dict):
                    continue
                if (
                    entry.get("name") == target_name
                    or entry.get("table_name") == target_name
                ):
                    return self._parse_raw_config(entry, path)
            raise ValueError(
                f"No config named '{target_name}' found inside {path} (contains {[e.get('name') for e in raw_yaml if isinstance(e, dict)]})"
            )
        elif isinstance(raw_yaml, dict):
            return self._parse_raw_config(raw_yaml, path)
        else:
            raise ValueError(f"Unrecognised YAML structure in {path}")

    def _parse_raw_config(self, raw: dict, path: Path) -> DataTypeConfig:
        """Convert one *raw* mapping into :class:`DataTypeConfig`."""
        try:
            name = raw["name"]
            # --- API ---------------------------------------------------------
            api_raw = raw.get("api", {}) or {}
            api_fields = {f.name for f in dataclasses.fields(ApiConfig)}
            api_cfg = ApiConfig(**{k: v for k, v in api_raw.items() if k in api_fields})

            # --- SCHEMA ------------------------------------------------------
            schema_raw = raw["schema"]
            schema_fields = {f.name for f in dataclasses.fields(SchemaConfig)}
            schema_kwargs = {k: v for k, v in schema_raw.items() if k in schema_fields}
            schema_cfg = SchemaConfig(**schema_kwargs)

            # --- PROCESSING --------------------------------------------------
            proc_raw = raw.get("processing", {}) or {}
            proc_fields = {f.name for f in dataclasses.fields(ProcessingConfig)}
            proc_cfg = ProcessingConfig(
                **{k: v for k, v in proc_raw.items() if k in proc_fields}
            )
        except (KeyError, TypeError) as exc:
            raise ValueError(f"Malformed config in {path}: {exc}") from exc

        return DataTypeConfig(
            name=name,
            description=raw.get("description"),
            api=api_cfg,
            schema=schema_cfg,
            processing=proc_cfg,
        )

    # ------------------------------------------------------------------
    # misc helpers (schema names etc.) – copied from old router
    # ------------------------------------------------------------------
    def get_schema_names(self, data_type: str) -> dict[str, str]:
        src = self.get_data_source(data_type)
        return {
            "raw": f"bicam_raw_{src}",
            "staging": f"bicam_staging_{src}",
            "production": f"bicam_{src}",
        }

    def list_data_types(self):
        return list(self._registry)

    def list_data_types_by_source(self, data_source: str):
        return [
            dt for dt, (_, _, _, _, src) in self._registry.items() if src == data_source
        ]

    def list_data_sources(self):
        return sorted({src for _, (_, _, _, _, src) in self._registry.items()})

    def is_registered(self, data_type: str) -> bool:
        return data_type in self._registry

    # ------------------------------------------------------------------
    # Component helpers (fetcher / normalizer / cleaner)
    # ------------------------------------------------------------------

    def get_fetcher_class(self, data_type: str):  # noqa: ANN001 – runtime type
        f, _, _ = self.get_components(data_type)
        return f

    def get_normalizer_class(self, data_type: str):
        _, n, _ = self.get_components(data_type)
        return n

    def get_cleaner_class(self, data_type: str):
        _, _, c = self.get_components(data_type)
        return c


# ---------------------------------------------------------------------------
# Module-level singleton helpers (same API as before) ------------------------
# ---------------------------------------------------------------------------

a_global_registry: DataTypeRegistry | None = None


def get_global_registry() -> DataTypeRegistry:
    global a_global_registry
    if a_global_registry is None:
        a_global_registry = DataTypeRegistry()
    return a_global_registry


def register_data_type(
    data_type: str,
    *,
    fetcher_class: type[AbstractFetcher],
    normalizer_class: type[AbstractDatabaseNormalizer],
    cleaner_class: type[AbstractCleaner],
    config_file: str,
    data_source: str,
) -> None:
    get_global_registry().register_data_type(
        data_type,
        fetcher_class=fetcher_class,
        normalizer_class=normalizer_class,
        cleaner_class=cleaner_class,
        config_file=config_file,
        data_source=data_source,
    )
