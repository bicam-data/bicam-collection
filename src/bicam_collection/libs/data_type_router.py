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

from .data_type_registry import (
    get_global_registry,
    register_data_type,
)

logger = logging.getLogger(__name__)


class DataTypeRouter:
    """
    Router for creating and managing data type component instances.

    This class uses the registry to create instances of fetcher, normalizer,
    and cleaner classes for specific data types.
    """

    def __init__(self):
        self.registry = get_global_registry()

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
        data_source = self.registry.get_data_source(data_type)

        # Base classes need data_type_name and system_name parameters
        return normalizer_class(
            data_type_name=data_type,
            system_name=data_source,
        )

    def create_cleaner(self, data_type: str) -> Any:
        """
        Create a cleaner instance for a data type.

        Args:
            data_type: Name of the data type

        Returns:
            Cleaner instance
        """
        cleaner_class = self.registry.get_cleaner_class(data_type)
        data_source = self.registry.get_data_source(data_type)

        # Base classes need data_type_name and system_name parameters
        return cleaner_class(
            data_type_name=data_type,
            system_name=data_source,
        )

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


def create_router() -> DataTypeRouter:
    """Create a router using the global registry."""
    return DataTypeRouter()


def _register_related_tables(
    data_type: str,
    config_file: str,
    data_source: str,
    fetcher_class,
    normalizer_class,
    cleaner_class,
    processed_types: set[str] | None = None,
) -> None:
    """
    Recursively register related tables for a data type.

    Args:
        data_type: The parent data type name
        config_file: Path to the config file
        data_source: The data source name
        fetcher_class: The fetcher class to use
        normalizer_class: The normalizer class to use
        cleaner_class: The cleaner class to use
        processed_types: Set of already processed data types to avoid cycles
    """
    if processed_types is None:
        processed_types = set()

    # Skip if we've already processed this type
    if data_type in processed_types:
        return
    processed_types.add(data_type)

    try:
        from bicam_collection.libs.data_type_registry import get_global_registry

        # Get config for this data type
        try:
            type_config = get_global_registry().get_data_type_config(data_type)
        except Exception:
            # If we can't get the config directly (e.g. for a related table),
            # try to get it as a related table config
            parent_type = data_type.rsplit("_", 1)[0]
            suffix = data_type.rsplit("_", 1)[1]
            type_config = get_global_registry().get_related_table_config(
                parent_type, suffix
            )

        if (
            type_config
            and hasattr(type_config, "related_tables")
            and type_config.related_tables
        ):
            for suffix in type_config.related_tables:
                related_name = f"{data_type}_{suffix}"
                logger.debug(f"Registering related table {related_name}")

                # Register the related table
                register_data_type(
                    related_name,
                    fetcher_class=fetcher_class,
                    normalizer_class=normalizer_class,
                    cleaner_class=cleaner_class,
                    config_file=config_file,  # Use same config file
                    data_source=data_source,
                )

                # Recursively register its related tables
                _register_related_tables(
                    related_name,
                    config_file,
                    data_source,
                    fetcher_class,
                    normalizer_class,
                    cleaner_class,
                    processed_types,
                )
    except Exception as e:
        logger.warning(f"Error registering related tables for {data_type}: {e}")


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

            # Skip base directories - they are not data types
            if data_type_item.name == "base":
                continue

            data_type = data_type_item.name
            logger.debug(
                f"Attempting to register data type: {data_type} (source: {data_source})"
            )

            try:
                # Import the data type module
                module_path = f"bicam_collection.data_types.{data_source}.{data_type}"
                data_type_module = importlib.import_module(module_path)

                # Try to get the specialized classes
                fetcher_class = getattr(
                    data_type_module, f"{data_type.title()}Fetcher", None
                )
                normalizer_class = getattr(
                    data_type_module, f"{data_type.title()}DatabaseNormalizer", None
                )
                cleaner_class = getattr(
                    data_type_module, f"{data_type.title()}Cleaner", None
                )

                # Fallback to base fetcher if not found
                if fetcher_class is None:
                    fetcher_module_path = (
                        f"bicam_collection.data_types.{data_source}.base"
                    )
                    fetcher_module = importlib.import_module(fetcher_module_path)
                    fetcher_class = getattr(
                        fetcher_module, f"{data_source.title()}BaseFetcher", None
                    )

                # Fallback to shared base classes if not found
                if normalizer_class is None or cleaner_class is None:
                    abstract_module = importlib.import_module(
                        "bicam_collection.data_types.abstract"
                    )
                    if normalizer_class is None:
                        normalizer_class = getattr(
                            abstract_module, "BaseDatabaseNormalizer", None
                        )
                    if cleaner_class is None:
                        cleaner_class = getattr(abstract_module, "BaseCleaner", None)

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
                    fetcher_class=fetcher_class,
                    normalizer_class=normalizer_class,
                    cleaner_class=cleaner_class,
                    config_file=config_file,
                    data_source=data_source,
                )

                # Register related tables recursively
                _register_related_tables(
                    data_type,
                    config_file,
                    data_source,
                    fetcher_class,
                    normalizer_class,
                    cleaner_class,
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

        # Create specialized assets instance using the base class
        try:
            abstract_module = importlib.import_module(
                "bicam_collection.data_types.abstract"
            )
            specialized_assets_class = getattr(
                abstract_module, "BaseSpecializedAssets", None
            )

            if specialized_assets_class:
                specialized_assets_instance = specialized_assets_class(
                    data_type_name=data_type,
                    system_name=data_source,
                )

                # Check if the instance has a get_specialized_assets method
                if hasattr(specialized_assets_instance, "get_specialized_assets"):
                    assets = specialized_assets_instance.get_specialized_assets()
                    return assets or []
                else:
                    logger.debug(
                        f"No get_specialized_assets method found for {data_type}"
                    )
                    return []
            else:
                logger.debug("BaseSpecializedAssets class not found")
                return []

        except (ImportError, AttributeError) as e:
            logger.debug(
                f"Could not create specialized assets instance for {data_type}: {e}"
            )
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


# Global registry instance
_global_registry = get_global_registry()

# ---------------------------------------------------------------------------
# Trigger automatic registration of built-in data types at import time.
# This guarantees that *get_data_type_config* calls succeed even when the
# registry is accessed in a new interpreter (e.g. Dagster subprocesses) where
# register_all_data_types() hasn't been invoked explicitly.
# ---------------------------------------------------------------------------

_register_builtin_types()
