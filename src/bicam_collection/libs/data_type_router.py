"""
Data Type Router and Registry

This module provides a centralized registry for mapping data types to their
corresponding fetcher, normalizer, and cleaner classes. This eliminates the
need for nested if statements and makes it easy to add new data types.

The system supports multiple data sources (congressional, govinfo, etc.) and
can automatically discover and register data types from any source.
"""

import logging
from pathlib import Path
from typing import Any

import yaml

from ..data_types.abstract import (
    AbstractCleaner,
    AbstractDatabaseNormalizer,
    AbstractFetcher,
)

logger = logging.getLogger(__name__)


class DataTypeRegistry:
    """
    Registry for data type components.

    This class maintains a mapping of data types to their corresponding
    fetcher, normalizer, and cleaner classes, along with their data source.
    """

    def __init__(self):
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

    def register_data_type(
        self,
        data_type: str,
        fetcher_class: type["AbstractFetcher"],
        normalizer_class: type["AbstractDatabaseNormalizer"],
        cleaner_class: type["AbstractCleaner"],
        config_file: str,
        data_source: str,
    ):
        """
        Register a data type with its component classes.

        Args:
            data_type: Name of the data type (e.g., "bills")
            fetcher_class: Fetcher class for this data type
            normalizer_class: Normalizer class for this data type
            cleaner_class: Cleaner class for this data type
            config_file: Path to the config file for this data type
            data_source: Data source category (e.g., "congressional", "govinfo")
        """
        self._registry[data_type] = (
            fetcher_class,
            normalizer_class,
            cleaner_class,
            config_file,
            data_source,
        )
        logger.info(
            f"Registered data type: {data_type} (source: {data_source}) with config: {config_file}"
        )

    def get_components(
        self, data_type: str
    ) -> tuple[
        type["AbstractFetcher"],
        type["AbstractDatabaseNormalizer"],
        type["AbstractCleaner"],
    ]:
        """
        Get component classes for a data type.

        Args:
            data_type: Name of the data type

        Returns:
            Tuple of (fetcher_class, normalizer_class, cleaner_class)

        Raises:
            ValueError: If data type is not registered
        """
        if data_type not in self._registry:
            available_types = list(self._registry.keys())
            raise ValueError(
                f"Data type '{data_type}' not registered. Available types: {available_types}"
            )

        fetcher_class, normalizer_class, cleaner_class, config_file, data_source = (
            self._registry[data_type]
        )
        return fetcher_class, normalizer_class, cleaner_class

    def get_fetcher_class(self, data_type: str) -> type["AbstractFetcher"]:
        """Get fetcher class for a data type."""
        fetcher_class, _, _ = self.get_components(data_type)
        return fetcher_class

    def get_normalizer_class(
        self, data_type: str
    ) -> type["AbstractDatabaseNormalizer"]:
        """Get normalizer class for a data type."""
        _, normalizer_class, _ = self.get_components(data_type)
        return normalizer_class

    def get_cleaner_class(self, data_type: str) -> type["AbstractCleaner"]:
        """Get cleaner class for a data type."""
        _, _, cleaner_class = self.get_components(data_type)
        return cleaner_class

    def get_config_file(self, data_type: str) -> str:
        """Get config file path for a data type."""
        if data_type not in self._registry:
            available_types = list(self._registry.keys())
            raise ValueError(
                f"Data type '{data_type}' not registered. Available types: {available_types}"
            )

        _, _, _, config_file, data_source = self._registry[data_type]
        return config_file

    def get_data_source(self, data_type: str) -> str:
        """Get data source for a data type."""
        if data_type not in self._registry:
            available_types = list(self._registry.keys())
            raise ValueError(
                f"Data type '{data_type}' not registered. Available types: {available_types}"
            )

        _, _, _, config_file, data_source = self._registry[data_type]
        return data_source

    def get_schema_names(self, data_type: str) -> dict[str, str]:
        """Get schema names for a data type based on its data source."""
        data_source = self.get_data_source(data_type)

        return {
            "raw": f"bicam_raw_{data_source}",
            "staging": f"bicam_staging_{data_source}",
            "production": f"bicam_{data_source}",
        }

    def get_table_names(self, data_type: str) -> list[str]:
        """
        Get table names for a data type from its config file.

        Args:
            data_type: Name of the data type

        Returns:
            List of table names for this data type

        Raises:
            ValueError: If data type is not registered or config file cannot be read
        """
        config_file = self.get_config_file(data_type)

        try:
            # Try to load the config file
            config_path = Path(config_file)
            if not config_path.is_absolute():
                project_root = _get_project_root()
                config_path = project_root / config_file

            if not config_path.exists():
                logger.warning(f"Config file not found: {config_file}")
                return [data_type]  # Fallback to data type name

            with open(config_path, encoding="utf-8") as f:
                config_data = yaml.safe_load(f)

            if not config_data:
                logger.warning(f"Empty config file: {config_file}")
                return [data_type]

            # Extract table names from the list of table configurations
            table_names = []

            if isinstance(config_data, list):
                # list of table configurations
                for table_config in config_data:
                    if isinstance(table_config, dict) and "name" in table_config:
                        table_names.append(table_config["name"])

            # Sort to ensure consistent ordering with main table first
            table_names.sort(key=lambda x: (x != data_type, x))

            if not table_names:
                logger.warning(f"No tables found for data type '{data_type}' in config")
                return [data_type]

            logger.debug(f"Found tables for {data_type}: {table_names}")
            return table_names

        except Exception as e:
            logger.error(f"Error reading config file {config_file}: {e}")
            return [data_type]  # Fallback to data type name

    def get_table_config(self, data_type: str, table_name: str) -> dict:
        """
        Get configuration for a specific table.

        Args:
            data_type: Name of the data type
            table_name: Name of the table

        Returns:
            Dictionary containing table configuration
        """
        config_file = self.get_config_file(data_type)

        try:
            config_path = Path(config_file)
            if not config_path.is_absolute():
                project_root = _get_project_root()
                config_path = project_root / config_file

            with open(config_path, encoding="utf-8") as f:
                config_data = yaml.safe_load(f)

            if not config_data:
                return {}

            if isinstance(config_data, list):
                # New format: list of table configurations
                for table_config in config_data:
                    if (
                        isinstance(table_config, dict)
                        and table_config.get("name") == table_name
                    ):
                        return table_config
                return {}

            else:
                logger.warning(f"Invalid config data format in {config_file}")
                return {}

        except Exception as e:
            logger.error(f"Error reading table config for {table_name}: {e}")
            return {}

    def get_table_configs(self, data_type: str) -> dict:
        """
        Get all table configurations for a data type.

        Args:
            data_type: Name of the data type

        Returns:
            Dictionary containing all table configurations
        """
        config_file = self.get_config_file(data_type)

        try:
            config_path = Path(config_file)
            if not config_path.is_absolute():
                project_root = _get_project_root()
                config_path = project_root / config_file

            with open(config_path, encoding="utf-8") as f:
                config_data = yaml.safe_load(f)

            if not config_data:
                return {}

            if isinstance(config_data, list):
                # New format: convert list to dictionary
                table_configs = {}
                for table_config in config_data:
                    if isinstance(table_config, dict) and "name" in table_config:
                        table_configs[table_config["name"]] = table_config
                return table_configs

            else:
                logger.warning(f"Invalid config data format in {config_file}")
                return {}

        except Exception as e:
            logger.error(f"Error reading config file {config_file}: {e}")
            return {}

    def list_data_types(self) -> list[str]:
        """Get list of registered data types."""
        return list(self._registry.keys())

    def list_data_types_by_source(self, data_source: str) -> list[str]:
        """Get list of registered data types for a specific data source."""
        return [
            data_type
            for data_type, (_, _, _, _, source) in self._registry.items()
            if source == data_source
        ]

    def list_data_sources(self) -> list[str]:
        """Get list of available data sources."""
        return sorted(
            {data_source for _, _, _, _, data_source in self._registry.values()}
        )

    def is_registered(self, data_type: str) -> bool:
        """Check if a data type is registered."""
        return data_type in self._registry


class DataTypeRouter:
    """
    Router for creating and managing data type component instances.

    This class uses the registry to create instances of fetcher, normalizer,
    and cleaner classes for specific data types.
    """

    def __init__(self, registry: DataTypeRegistry):
        self.registry = registry

    def create_fetcher(self, data_type: str, api_client) -> Any:
        """
        Create a fetcher instance for a data type.

        Args:
            data_type: Name of the data type
            api_client: API client instance

        Returns:
            Fetcher instance
        """
        fetcher_class = self.registry.get_fetcher_class(data_type)
        return fetcher_class(api_client)

    def create_normalizer(self, data_type: str) -> Any:
        """
        Create a normalizer instance for a data type.

        Args:
            data_type: Name of the data type

        Returns:
            Normalizer instance
        """
        normalizer_class = self.registry.get_normalizer_class(data_type)
        return normalizer_class()

    def create_cleaner(self, data_type: str) -> Any:
        """
        Create a cleaner instance for a data type.

        Args:
            data_type: Name of the data type

        Returns:
            Cleaner instance
        """
        cleaner_class = self.registry.get_cleaner_class(data_type)
        return cleaner_class()

    def create_all_components(self, data_type: str, api_client) -> tuple[Any, Any, Any]:
        """
        Create all component instances for a data type.

        Args:
            data_type: Name of the data type
            api_client: API client instance

        Returns:
            Tuple of (fetcher, normalizer, cleaner) instances
        """
        return (
            self.create_fetcher(data_type, api_client),
            self.create_normalizer(data_type),
            self.create_cleaner(data_type),
        )


# Global registry instance
_global_registry = DataTypeRegistry()
_registry_initialized = False


def get_global_registry() -> DataTypeRegistry:
    """Get the global data type registry."""
    global _registry_initialized
    if not _registry_initialized:
        _register_builtin_types()
        _registry_initialized = True
    return _global_registry


def register_data_type(
    data_type: str,
    fetcher_class: type["AbstractFetcher"],
    normalizer_class: type["AbstractDatabaseNormalizer"],
    cleaner_class: type["AbstractCleaner"],
    config_file: str,
    data_source: str,
):
    """
    Register a data type with the global registry.

    Args:
        data_type: Name of the data type
        fetcher_class: Fetcher class for this data type
        normalizer_class: Normalizer class for this data type
        cleaner_class: Cleaner class for this data type
        config_file: Path to the config file for this data type
        data_source: Data source category (e.g., "congressional", "govinfo")
    """
    _global_registry.register_data_type(
        data_type,
        fetcher_class,
        normalizer_class,
        cleaner_class,
        config_file,
        data_source,
    )


def create_router() -> DataTypeRouter:
    """Create a router using the global registry."""
    return DataTypeRouter(_global_registry)


def _register_builtin_types():
    """Register built-in data types automatically by scanning data source directories."""
    import importlib
    from pathlib import Path

    # Get the data types directory
    current_dir = Path(__file__).parent
    data_types_dir = current_dir / ".." / "data_types"

    # If the relative path doesn't exist, try absolute path construction
    if not data_types_dir.exists():
        project_root = _get_project_root()
        data_types_dir = project_root / "src" / "bicam_collection" / "data_types"

    if not data_types_dir.exists():
        logger.warning(f"Data types directory not found: {data_types_dir}")
        return

    # Scan for data source directories (congressional, govinfo, etc.)
    for data_source_item in data_types_dir.iterdir():
        if not data_source_item.is_dir() or data_source_item.name.startswith("_"):
            continue

        data_source = data_source_item.name
        logger.debug(f"Scanning data source: {data_source}")

        # Scan for data type directories within each data source
        for data_type_item in data_source_item.iterdir():
            if not data_type_item.is_dir() or data_type_item.name.startswith("_"):
                continue

            data_type = data_type_item.name
            logger.debug(
                f"Attempting to register data type: {data_type} (source: {data_source})"
            )

            try:
                # Import the data type module
                module_path = f"bicam_collection.data_types.{data_source}.{data_type}"
                data_type_module = importlib.import_module(module_path)

                # Get the component classes - they should follow the naming convention
                fetcher_class_name = f"{data_type.title()}Fetcher"
                normalizer_class_name = f"{data_type.title()}DatabaseNormalizer"
                cleaner_class_name = f"{data_type.title()}Cleaner"

                # Get classes from the module
                fetcher_class = getattr(data_type_module, fetcher_class_name, None)
                normalizer_class = getattr(
                    data_type_module, normalizer_class_name, None
                )
                cleaner_class = getattr(data_type_module, cleaner_class_name, None)

                if not all([fetcher_class, normalizer_class, cleaner_class]):
                    logger.warning(
                        f"Missing component classes for {data_type} ({data_source}): "
                        f"Fetcher: {fetcher_class is not None}, "
                        f"Normalizer: {normalizer_class is not None}, "
                        f"Cleaner: {cleaner_class is not None}"
                    )
                    continue

                # Skip validation to avoid import context issues
                # The inheritance is validated separately during testing
                logger.debug(
                    f"Skipping base class validation for {data_type} ({data_source})"
                )

                # Determine the config file path - prefer local config.yaml over configs folder
                project_root = _get_project_root()
                local_config_path = (
                    project_root
                    / "src"
                    / "bicam_collection"
                    / "data_types"
                    / data_source
                    / data_type
                    / "config.yaml"
                )

                if local_config_path.exists():
                    # Use the local config.yaml file in the data type's folder
                    config_file = f"src/bicam_collection/data_types/{data_source}/{data_type}/config.yaml"
                    logger.debug(f"Using local config for {data_type}: {config_file}")

                else:
                    logger.warning(
                        f"No config file found for {data_type} ({data_source}), skipping registration"
                    )
                    continue

                # Register the data type with its data source
                register_data_type(
                    data_type,
                    fetcher_class,
                    normalizer_class,
                    cleaner_class,
                    config_file,
                    data_source,
                )

                logger.info(
                    f"Successfully registered data type: {data_type} (source: {data_source})"
                )

            except ImportError as e:
                logger.debug(
                    f"Could not import {data_type} data type from {data_source}: {e}"
                )
            except AttributeError as e:
                logger.debug(
                    f"Missing required classes for {data_type} ({data_source}): {e}"
                )
            except Exception as e:
                logger.warning(
                    f"Error registering {data_type} data type from {data_source}: {e}"
                )


def get_specialized_assets(data_type: str) -> list:
    """
    Get specialized assets for a data type if they exist.

    Args:
        data_type: Name of the data type

    Returns:
        List of specialized assets, empty list if none found
    """
    import importlib

    try:
        # Get the data source for this data type
        registry = get_global_registry()
        data_source = registry.get_data_source(data_type)

        # Try to import the data type module
        module_path = f"bicam_collection.data_types.{data_source}.{data_type}"
        data_type_module = importlib.import_module(module_path)

        # Check if the module has a get_specialized_assets function
        if hasattr(data_type_module, "get_specialized_assets"):
            specialized_assets = data_type_module.get_specialized_assets()
            return specialized_assets or []
        else:
            logger.debug(f"No get_specialized_assets function found for {data_type}")
            return []

    except ImportError as e:
        logger.debug(f"Could not import specialized assets for {data_type}: {e}")
        return []
    except Exception as e:
        logger.warning(f"Error loading specialized assets for {data_type}: {e}")
        return []


def register_all_data_types():
    """
    Force re-registration of all data types.

    This can be useful for refreshing the registry after changes
    or for ensuring all data types are properly registered.
    """
    logger.info("Re-registering all data types...")
    _register_builtin_types()
    logger.info(f"Registration complete. Available data types: {list_data_types()}")


def list_data_types() -> list[str]:
    """Get list of all registered data types."""
    return _global_registry.list_data_types()


def get_data_type_info(data_type: str) -> dict:
    """
    Get comprehensive information about a registered data type.

    Args:
        data_type: Name of the data type

    Returns:
        Dictionary with data type information
    """
    if not _global_registry.is_registered(data_type):
        raise ValueError(f"Data type '{data_type}' is not registered")

    fetcher_class, normalizer_class, cleaner_class = _global_registry.get_components(
        data_type
    )
    config_file = _global_registry.get_config_file(data_type)
    data_source = _global_registry.get_data_source(data_type)
    table_names = _global_registry.get_table_names(data_type)
    schema_names = _global_registry.get_schema_names(data_type)

    return {
        "data_type": data_type,
        "data_source": data_source,
        "fetcher_class": fetcher_class.__name__,
        "normalizer_class": normalizer_class.__name__,
        "cleaner_class": cleaner_class.__name__,
        "config_file": config_file,
        "table_names": table_names,
        "table_count": len(table_names),
        "schema_names": schema_names,
        "has_specialized_assets": len(get_specialized_assets(data_type)) > 0,
        "specialized_assets_count": len(get_specialized_assets(data_type)),
    }


# Auto-register happens lazily when registry is first accessed


# Add helper to determine project root reliably
def _get_project_root() -> Path:
    """Return the absolute project root directory (four levels above this file)."""
    # __file__ = <repo_root>/src/bicam_collection/libs/data_type_router.py
    # Need to move up 4 levels: libs → bicam_collection → src → repo_root
    return Path(__file__).resolve().parent.parent.parent.parent
