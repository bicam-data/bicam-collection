"""
Consolidated Data Type and Plugin Registry

This module provides a single, unified registry that handles:
- Data type discovery and registration
- Configuration loading and caching
- Plugin creation and management
- Schema name resolution

This replaces the complex 3-layer system with a single, focused registry.
"""

import dataclasses
import logging
from pathlib import Path
from typing import Any, Literal

import yaml

from ..libs.data_type_config import (
    ApiConfig,
    DataTypeConfig,
    ProcessingConfig,
    SchemaConfig,
)
from .base import CleanerPlugin, FetcherPlugin, NormalizerPlugin
from .congressional import CongressionalFetcherPlugin
from .govinfo import GovInfoFetcherPlugin

logger = logging.getLogger(__name__)


class Registry:
    """
    Unified registry for data types, configurations, and plugins.

    This single registry handles everything:
    - Data type discovery and registration
    - Configuration loading and caching
    - Plugin creation and management
    - Schema name resolution
    """

    def __init__(self):
        # Core data type registry: data_type -> metadata
        self._data_types: dict[str, dict[str, Any]] = {}

        # Configuration cache
        self._config_cache: dict[str, DataTypeConfig] = {}

        # Plugin instance cache
        self._fetcher_plugins: dict[str, FetcherPlugin] = {}
        self._cleaner_plugins: dict[str, CleanerPlugin] = {}
        self._normalizer_plugins: dict[str, NormalizerPlugin] = {}
        self._custom_logic_plugins: dict[str, Any] = {}

        # Initialization state
        self._initialized = False

    def _ensure_initialized(self):
        """Ensure the registry is initialized."""
        if not self._initialized:
            self._discover_and_register_data_types()
            self._initialized = True

    def _discover_and_register_data_types(self):
        """Discover and register all data types from the filesystem."""
        try:
            # Get the plugins/custom_logic directory where data types are defined
            current_dir = Path(__file__).parent
            custom_logic_dir = current_dir / "custom_logic"

            if not custom_logic_dir.exists():
                logger.warning(f"Custom logic directory not found: {custom_logic_dir}")
                return

            logger.info(f"Discovering data types in: {custom_logic_dir}")

            # Scan for data source directories (congressional, govinfo, etc.)
            for data_source_item in custom_logic_dir.iterdir():
                if not data_source_item.is_dir() or data_source_item.name.startswith(
                    "_"
                ):
                    continue

                data_source = data_source_item.name
                logger.debug(f"Scanning data source: {data_source}")

                # Scan for data type directories within each data source
                for data_type_item in data_source_item.iterdir():
                    if not data_type_item.is_dir() or data_type_item.name.startswith(
                        "_"
                    ):
                        continue

                    data_type = data_type_item.name
                    config_file_path = data_type_item / "config.yaml"

                    if not config_file_path.exists():
                        logger.debug(f"No config file found for {data_type}, skipping")
                        continue

                    try:
                        # Register the data type
                        self._register_data_type(
                            data_type=data_type,
                            data_source=data_source,
                            config_file=str(config_file_path),
                            plugin_dir=str(data_type_item),
                        )

                        logger.info(
                            f"Registered data type: {data_type} (source: {data_source})"
                        )

                    except Exception as e:
                        logger.warning(f"Error registering {data_type}: {e}")

            logger.info(f"Discovered {len(self._data_types)} data types")

        except Exception as e:
            logger.error(f"Error during data type discovery: {e}")

    def _register_data_type(
        self,
        data_type: str,
        data_source: str,
        config_file: str,
        plugin_dir: str,
    ):
        """Register a single data type with its metadata."""
        self._data_types[data_type] = {
            "data_source": data_source,
            "config_file": config_file,
            "plugin_dir": plugin_dir,
            "plugin_type": data_source,  # congressional, govinfo, etc.
        }

    # =============================================================================
    # Configuration Management
    # =============================================================================

    def get_data_type_config(self, data_type: str) -> DataTypeConfig:
        """Get typed configuration for a data type."""
        self._ensure_initialized()

        if data_type in self._config_cache:
            return self._config_cache[data_type]

        if data_type not in self._data_types:
            raise ValueError(f"Data type '{data_type}' not registered")

        config_file = self._data_types[data_type]["config_file"]
        config_path = Path(config_file)

        try:
            config = self._load_config_from_yaml(config_path, data_type)
            self._config_cache[data_type] = config
            return config
        except Exception as e:
            raise FileNotFoundError(
                f"Could not load config for '{data_type}': {e}"
            ) from e

    def get_related_table_config(
        self, main_data_type: str, related_table: str
    ) -> DataTypeConfig | None:
        """Get configuration for a related table from the main data type's config file."""
        self._ensure_initialized()

        if main_data_type not in self._data_types:
            return None

        config_file = self._data_types[main_data_type]["config_file"]
        config_path = Path(config_file)

        try:
            # Load all configs from the main data type's config file
            with open(config_path, encoding="utf-8") as fh:
                raw_yaml = yaml.safe_load(fh)

            if isinstance(raw_yaml, list):
                # Multi-config file – look for the related table config
                related_config_name = f"{main_data_type}_{related_table}"
                for entry in raw_yaml:
                    if not isinstance(entry, dict):
                        continue
                    if (
                        entry.get("name") == related_config_name
                        or entry.get("name") == related_table
                    ):
                        return self._parse_raw_config(entry, config_path)
            elif isinstance(raw_yaml, dict):
                # Single config file - check if it's the related table
                if (
                    raw_yaml.get("name") == f"{main_data_type}_{related_table}"
                    or raw_yaml.get("name") == related_table
                ):
                    return self._parse_raw_config(raw_yaml, config_path)

            return None
        except Exception as e:
            logger.warning(
                f"Could not load related table config for {main_data_type}_{related_table}: {e}"
            )
            return None

    def _load_config_from_yaml(self, path: Path, target_name: str) -> DataTypeConfig:
        """Load configuration from YAML file."""
        with open(path, encoding="utf-8") as fh:
            raw_yaml = yaml.safe_load(fh)

        if isinstance(raw_yaml, list):
            # Multi-config file – pick the one matching target_name
            for entry in raw_yaml:
                if not isinstance(entry, dict):
                    continue
                if (
                    entry.get("name") == target_name
                    or entry.get("table_name") == target_name
                ):
                    return self._parse_raw_config(entry, path)
            raise ValueError(f"No config named '{target_name}' found in {path}")
        elif isinstance(raw_yaml, dict):
            return self._parse_raw_config(raw_yaml, path)
        else:
            raise ValueError(f"Invalid YAML structure in {path}")

    def _parse_raw_config(self, raw: dict, path: Path) -> DataTypeConfig:
        """Parse raw YAML config into DataTypeConfig."""
        try:
            name = raw["name"]

            # Parse API config
            api_raw = raw.get("api", {}) or {}
            api_fields = {f.name for f in dataclasses.fields(ApiConfig)}
            api_cfg = ApiConfig(**{k: v for k, v in api_raw.items() if k in api_fields})

            # Parse schema config
            schema_raw = raw["schema"]
            schema_fields = {f.name for f in dataclasses.fields(SchemaConfig)}
            schema_cfg = SchemaConfig(
                **{k: v for k, v in schema_raw.items() if k in schema_fields}
            )

            # Parse processing config
            proc_raw = raw.get("processing", {}) or {}
            proc_fields = {f.name for f in dataclasses.fields(ProcessingConfig)}
            proc_cfg = ProcessingConfig(
                **{k: v for k, v in proc_raw.items() if k in proc_fields}
            )

            return DataTypeConfig(
                name=name,
                description=raw.get("description"),
                api=api_cfg,
                schema=schema_cfg,
                processing=proc_cfg,
            )
        except (KeyError, TypeError) as e:
            raise ValueError(f"Malformed config in {path}: {e}") from e

    # =============================================================================
    # Plugin Management
    # =============================================================================

    def get_fetcher_plugin(self, data_type: str) -> FetcherPlugin | None:
        """Get or create fetcher plugin for data type."""
        self._ensure_initialized()

        if data_type in self._fetcher_plugins:
            return self._fetcher_plugins[data_type]

        plugin = self._create_fetcher_plugin(data_type)
        if plugin:
            self._fetcher_plugins[data_type] = plugin
        return plugin

    def get_cleaner_plugin(self, data_type: str) -> CleanerPlugin | None:
        """Get or create cleaner plugin for data type."""
        self._ensure_initialized()

        if data_type in self._cleaner_plugins:
            return self._cleaner_plugins[data_type]

        plugin = self._create_cleaner_plugin(data_type)
        if plugin:
            self._cleaner_plugins[data_type] = plugin
        return plugin

    def get_normalizer_plugin(self, data_type: str) -> NormalizerPlugin | None:
        """Get or create normalizer plugin for data type."""
        self._ensure_initialized()

        if data_type in self._normalizer_plugins:
            return self._normalizer_plugins[data_type]

        plugin = self._create_normalizer_plugin(data_type)
        if plugin:
            self._normalizer_plugins[data_type] = plugin
        return plugin

    def get_custom_logic_plugin(
        self, data_type: str, stage: Literal["fetching", "cleaning"]
    ) -> Any | None:
        """Get or create custom logic plugin for data type."""
        self._ensure_initialized()

        if data_type in self._custom_logic_plugins:
            return self._custom_logic_plugins[data_type]

        plugin = self._create_custom_logic_plugin(data_type, stage)
        if plugin:
            self._custom_logic_plugins[data_type] = plugin
        return plugin

    def _create_fetcher_plugin(self, data_type: str) -> FetcherPlugin | None:
        """Create fetcher plugin for data type."""
        if data_type not in self._data_types:
            return None

        data_source = self._data_types[data_type]["data_source"]

        try:
            if data_source == "congressional":
                return CongressionalFetcherPlugin(data_type)
            elif data_source == "govinfo":
                return GovInfoFetcherPlugin(data_type)
            else:
                logger.warning(f"Unknown data source for fetcher: {data_source}")
                return None
        except Exception as e:
            logger.warning(f"Error creating fetcher plugin for {data_type}: {e}")
            return None

    def _create_cleaner_plugin(self, data_type: str) -> CleanerPlugin | None:
        """Create cleaner plugin for data type."""
        if data_type not in self._data_types:
            return None

        data_source = self._data_types[data_type]["data_source"]

        try:
            if data_source == "congressional":
                return None
            elif data_source == "govinfo":
                # GovInfo cleaner plugin not implemented yet
                return None
            else:
                logger.warning(f"Unknown data source for cleaner: {data_source}")
                return None
        except Exception as e:
            logger.warning(f"Error creating cleaner plugin for {data_type}: {e}")
            return None

    def _create_normalizer_plugin(self, data_type: str) -> NormalizerPlugin | None:
        """Create normalizer plugin for data type."""
        if data_type not in self._data_types:
            return None

        data_source = self._data_types[data_type]["data_source"]

        try:
            if data_source == "congressional":
                return None
            elif data_source == "govinfo":
                # GovInfo normalizer plugin not implemented yet
                return None
            else:
                logger.warning(f"Unknown data source for normalizer: {data_source}")
                return None
        except Exception as e:
            logger.warning(f"Error creating normalizer plugin for {data_type}: {e}")
            return None

    def _create_custom_logic_plugin(
        self, data_type: str, stage: Literal["fetching", "cleaning"]
    ) -> Any | None:
        """Create custom logic plugin for data type."""
        if data_type not in self._data_types:
            logger.warning(f"Data type {data_type} not found in registry")
            return None

        data_source = self._data_types[data_type]["data_source"]
        if stage == "fetching":
            logic_class_name = f"{data_type.title()}FetcherLogic"
        elif stage == "cleaning":
            logic_class_name = f"{data_type.title()}CleanerLogic"
        else:
            raise ValueError(f"Unknown stage: {stage}")

        logger.info(
            f"Creating custom logic plugin for {data_type} ({data_source}, {stage})"
        )
        logger.info(f"Looking for class: {logic_class_name}")

        try:
            # Try to import the specific custom logic class for this data type
            module_path = f"streamlined.plugins.custom_logic.{data_source}.{data_type}.custom_plugins"

            try:
                import importlib

                module = importlib.import_module(module_path)
                logger.info(f"Successfully imported module: {module_path}")

                # Look for the custom logic class (e.g., BillsFetcherLogic)
                if hasattr(module, logic_class_name):
                    logic_class = getattr(module, logic_class_name)
                    logger.info(f"Found {logic_class_name} class: {logic_class}")
                    instance = logic_class()
                    logger.info(f"Created instance: {instance}")
                    return instance

                logger.warning(f"No {logic_class_name} found in {module_path}")
                logger.info(
                    f"Available classes in module: {[attr for attr in dir(module) if not attr.startswith('_')]}"
                )
                return None

            except ImportError as e:
                logger.error(f"Could not import custom logic from {module_path}: {e}")
                return None

        except Exception as e:
            logger.error(f"Error creating custom logic plugin for {data_type}: {e}")
            return None

    # =============================================================================
    # Schema and Metadata
    # =============================================================================

    def get_schema_names(self, data_type: str) -> dict[str, str]:
        """Get schema names for a data type."""
        self._ensure_initialized()

        if data_type not in self._data_types:
            raise ValueError(f"Data type '{data_type}' not registered")

        data_source = self._data_types[data_type]["data_source"]
        return {
            "raw": f"bicam_raw_{data_source}",
            "staging": f"bicam_staging_{data_source}",
            "production": f"bicam_{data_source}",
        }

    def get_data_source(self, data_type: str) -> str:
        """Get data source for a data type."""
        self._ensure_initialized()

        if data_type not in self._data_types:
            raise ValueError(f"Data type '{data_type}' not registered")

        return self._data_types[data_type]["data_source"]

    def get_plugin_type(self, data_type: str) -> str:
        """Get plugin type for a data type."""
        return self.get_data_source(data_type)

    def get_config_file(self, data_type: str) -> str:
        """Get config file path for a data type."""
        self._ensure_initialized()

        if data_type not in self._data_types:
            raise ValueError(f"Data type '{data_type}' not registered")

        return self._data_types[data_type]["config_file"]

    # =============================================================================
    # Registry Information
    # =============================================================================

    def list_data_types(self) -> list[str]:
        """List all registered data types."""
        self._ensure_initialized()
        return list(self._data_types.keys())

    def list_data_types_by_source(self, data_source: str) -> list[str]:
        """List data types for a specific data source."""
        self._ensure_initialized()
        return [
            dt
            for dt, metadata in self._data_types.items()
            if metadata["data_source"] == data_source
        ]

    def list_data_sources(self) -> list[str]:
        """List all available data sources."""
        self._ensure_initialized()
        return sorted(
            {metadata["data_source"] for metadata in self._data_types.values()}
        )

    def is_registered(self, data_type: str) -> bool:
        """Check if a data type is registered."""
        self._ensure_initialized()
        return data_type in self._data_types

    def get_registry_status(self) -> dict[str, Any]:
        """Get comprehensive registry status."""
        self._ensure_initialized()
        return {
            "total_data_types": len(self._data_types),
            "data_types_by_source": {
                source: self.list_data_types_by_source(source)
                for source in self.list_data_sources()
            },
            "config_cache_size": len(self._config_cache),
            "plugin_cache_sizes": {
                "fetcher": len(self._fetcher_plugins),
                "cleaner": len(self._cleaner_plugins),
                "normalizer": len(self._normalizer_plugins),
                "custom_logic": len(self._custom_logic_plugins),
            },
            "all_data_types": self.list_data_types(),
        }

    def get_plugin_config(self, data_type: str) -> dict[str, Any]:
        """Get complete plugin configuration for a data type."""
        self._ensure_initialized()

        if data_type not in self._data_types:
            raise ValueError(f"Data type '{data_type}' not registered")

        return {
            "data_type": data_type,
            "plugin_type": self.get_plugin_type(data_type),
            "config_file": self.get_config_file(data_type),
            "data_source": self.get_data_source(data_type),
            "schema_names": self.get_schema_names(data_type),
        }


# =============================================================================
# Global Registry Instance
# =============================================================================

_global_registry: Registry | None = None


def get_registry() -> Registry:
    """Get the global consolidated registry instance."""
    global _global_registry
    if _global_registry is None:
        _global_registry = Registry()
    return _global_registry


# =============================================================================
# Convenience Functions (for backward compatibility)
# =============================================================================


def get_plugin_registry():
    """Get plugin registry (backward compatibility)."""
    return get_registry()


def get_global_registry():
    """Get data type registry (backward compatibility)."""
    return get_registry()


def register_data_type(
    data_type: str,
    *,
    plugin_type: str,
    config_file: str,
    data_source: str,
    processing_config: dict[str, Any] | None = None,
) -> None:
    """Register a data type (backward compatibility)."""
    registry = get_registry()
    registry._register_data_type(
        data_type=data_type,
        data_source=data_source,
        config_file=config_file,
        plugin_dir=str(Path(config_file).parent),
    )


def list_data_types() -> list[str]:
    """List all data types (backward compatibility)."""
    return get_registry().list_data_types()


def create_processor_for_data_type(data_type: str, processor_type: str):
    """Create a processor instance for a data type."""
    registry = get_registry()

    if processor_type == "fetcher":
        return registry.get_fetcher_plugin(data_type)
    elif processor_type == "cleaner":
        return registry.get_cleaner_plugin(data_type)
    elif processor_type == "normalizer":
        return registry.get_normalizer_plugin(data_type)
    else:
        raise ValueError(f"Unknown processor type: {processor_type}")
