"""
Schema configuration loader for data types.

This module provides utilities to load and access schema configurations
for different data types, including their fields, related fields, and
other metadata.

All configs are cached per session to avoid repeated file loading.
"""

import logging
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)


class DataTypeConfig:
    """Configuration for a single data type from the schema."""

    def __init__(self, config_data: dict[str, Any]):
        self.name = config_data.get("name", "")
        self.table_name = config_data.get("table_name", "")
        self.description = config_data.get("description")
        self.is_main = config_data.get("is_main", False)
        self.create_raw = config_data.get("create_raw", False)
        self.expected_key = config_data.get("expected_key")
        self.outer_api_field = config_data.get("outer_api_field")
        self.id_fields = config_data.get("id_fields", [])
        self.fields = config_data.get("fields", [])
        self.nested_fields = config_data.get("nested_fields", [])
        self.related_fields = config_data.get("related_fields", [])
        self.api_field_mapping = config_data.get("api_field_mapping", {})
        self.batch_size = config_data.get("batch_size", 1000)
        self.checkpoint_frequency = config_data.get("checkpoint_frequency", 100)
        self.retry_attempts = config_data.get("retry_attempts", 3)

    def __repr__(self):
        return f"DataTypeConfig(name={self.name}, is_main={self.is_main}, related_fields={self.related_fields})"


class ConfigCache:
    """Global cache for configuration files to avoid repeated loading."""

    def __init__(self):
        # Cache configs by file path to avoid loading the same file multiple times
        self._file_cache: dict[str, list[DataTypeConfig]] = {}
        # Cache individual configs by (file_path, data_type_name) for quick lookup
        self._config_cache: dict[tuple[str, str], DataTypeConfig] = {}

    def clear_cache(self):
        """Clear all cached configurations (useful for testing)."""
        self._file_cache.clear()
        self._config_cache.clear()
        logger.info("Configuration cache cleared")

    def _load_config_file(self, file_path: Path) -> list[DataTypeConfig]:
        """Load and parse a config file, returning all configs in it."""
        file_path_str = str(file_path)

        if file_path_str in self._file_cache:
            logger.debug(f"Using cached config from {file_path}")
            return self._file_cache[file_path_str]

        try:
            with open(file_path) as f:
                config_data = yaml.safe_load(f)
            logger.info(f"Loaded data type config from {file_path}")

            # Handle new format: list of data type configurations
            if isinstance(config_data, list):
                configs = [
                    DataTypeConfig(data_type_config) for data_type_config in config_data
                ]
            # Handle legacy format: single data type configuration
            else:
                configs = [DataTypeConfig(config_data)]

            # Cache the results
            self._file_cache[file_path_str] = configs

            # Also cache individual configs for quick lookup
            for config in configs:
                cache_key = (file_path_str, config.name)
                self._config_cache[cache_key] = config

            return configs

        except Exception as e:
            logger.error(f"Failed to load data type config from {file_path}: {e}")
            return []

    def get_data_type_config(
        self, data_type_name: str, data_type_path: str | Path | None = None
    ) -> DataTypeConfig | None:
        """Get configuration for a specific data type with caching."""
        if data_type_path is None:
            # Default path pattern: data_types/{category}/{data_type}/config.yaml
            default_path = (
                Path(__file__).parent / "congressional" / data_type_name / "config.yaml"
            )
            data_type_path = default_path

        file_path = Path(data_type_path)
        file_path_str = str(file_path)
        cache_key = (file_path_str, data_type_name)

        # Check individual config cache first
        if cache_key in self._config_cache:
            logger.debug(f"Using cached config for {data_type_name}")
            return self._config_cache[cache_key]

        # Load the file (will be cached)
        configs = self._load_config_file(file_path)

        # Find the specific data type
        for config in configs:
            if config.name == data_type_name:
                return config

        logger.warning(f"Data type '{data_type_name}' not found in {file_path}")
        return None

    def get_all_data_type_configs(
        self, data_type_path: str | Path | None = None
    ) -> list[DataTypeConfig]:
        """Get all configurations from a config file with caching."""
        if data_type_path is None:
            # Default to bills config for now
            default_path = (
                Path(__file__).parent / "congressional" / "bills" / "config.yaml"
            )
            data_type_path = default_path

        file_path = Path(data_type_path)
        return self._load_config_file(file_path)

    def get_main_data_type_config(
        self, data_type_path: str | Path | None = None
    ) -> DataTypeConfig | None:
        """Get the main data type configuration from a config file."""
        configs = self.get_all_data_type_configs(data_type_path)

        # Find the main data type (the first one with is_main=True)
        for config in configs:
            if config.is_main:
                return config

        logger.warning(f"No main data type found in {data_type_path}")
        return None


# Global cache instance - singleton pattern
_config_cache = ConfigCache()


# =============================================================================
# PUBLIC API - These are the functions that should be used throughout the codebase
# =============================================================================


def get_data_type_config(
    data_type_name: str, data_type_path: str | Path | None = None
) -> DataTypeConfig | None:
    """
    Load configuration for a specific data type with caching.

    Args:
        data_type_name: Name of the data type (e.g., "bills")
        data_type_path: Optional path to the config file

    Returns:
        DataTypeConfig object or None if not found
    """
    return _config_cache.get_data_type_config(data_type_name, data_type_path)


def get_all_data_type_configs(
    data_type_path: str | Path | None = None,
) -> list[DataTypeConfig]:
    """
    Load all data type configurations from a config file with caching.

    Args:
        data_type_path: Optional path to the config file

    Returns:
        List of DataTypeConfig objects
    """
    return _config_cache.get_all_data_type_configs(data_type_path)


def get_main_data_type_config(
    data_type_path: str | Path | None = None,
) -> DataTypeConfig | None:
    """
    Load the main data type configuration from a config file with caching.

    Args:
        data_type_path: Optional path to the config file

    Returns:
        Main DataTypeConfig object or None if not found
    """
    return _config_cache.get_main_data_type_config(data_type_path)


def clear_config_cache():
    """Clear the global configuration cache. Useful for testing or hot-reloading."""
    _config_cache.clear_cache()


def get_related_configs_for_data_type(
    data_type_name: str, data_type_path: str | Path | None = None
) -> list[DataTypeConfig]:
    """
    Get related configurations for a specific data type with caching.

    Args:
        data_type_name: The main data type name
        data_type_path: Optional path to the config file

    Returns:
        List of related DataTypeConfig objects
    """
    all_configs = get_all_data_type_configs(data_type_path)

    # Get the main config to find its related fields
    main_config = None
    for config in all_configs:
        if config.name == data_type_name and config.is_main:
            main_config = config
            break

    if not main_config:
        logger.warning(f"Main data type '{data_type_name}' not found")
        return []

    # Find configs that match the related fields
    related_configs = []
    for config in all_configs:
        if not config.is_main and any(
            config.name.endswith(f"_{field}")
            or config.name == f"{data_type_name}_{field}"
            for field in main_config.related_fields
        ):
            related_configs.append(config)

    return related_configs


class SchemaLoader:
    """Legacy schema loader class - kept for backwards compatibility but uses the new cache."""

    def __init__(self, schema_path: str | Path):
        self.schema_path = Path(schema_path)

    def get_data_type_config(self, data_type_name: str) -> DataTypeConfig | None:
        """Get configuration for a specific data type."""
        return get_data_type_config(data_type_name, self.schema_path)

    def get_main_data_types(self) -> list[DataTypeConfig]:
        """Get all main data types from the schema."""
        all_configs = get_all_data_type_configs(self.schema_path)
        return [config for config in all_configs if config.is_main]

    def get_related_data_types(self, main_data_type: str) -> list[str]:
        """Get list of related data types for a main data type."""
        config = self.get_data_type_config(main_data_type)
        if config:
            return config.related_fields
        return []

    def get_id_fields(self, data_type_name: str) -> list[str]:
        """Get ID fields for a data type."""
        config = self.get_data_type_config(data_type_name)
        if config:
            return config.id_fields
        return []

    def should_create_raw(self, data_type_name: str) -> bool:
        """Check if raw data should be created for this data type."""
        config = self.get_data_type_config(data_type_name)
        if config:
            return config.create_raw
        return False
