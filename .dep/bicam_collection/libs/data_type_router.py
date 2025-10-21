"""
Data Type Router - Simplified Plugin-Based Version

This module provides a simplified router for mapping data types to their
corresponding plugin implementations in the custom_logic folder.

The system now uses a plugin-based architecture where custom logic is
organized in plugins/custom_logic/{data_source}/{data_type}/
"""

import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class DataTypeRouter:
    """
    Simplified router for creating plugin-based data type component instances.
    """

    def __init__(self):
        self.custom_logic_plugins = {}
        self._load_custom_logic_plugins()

    def _load_custom_logic_plugins(self):
        """Load custom logic plugins from the custom_logic directory."""
        try:
            # Get the custom_logic directory
            custom_logic_dir = self._get_custom_logic_dir()

            if not custom_logic_dir.exists():
                logger.warning(f"Custom logic directory not found: {custom_logic_dir}")
                return

            # Scan for data source directories (congressional, govinfo, etc.)
            for data_source_item in custom_logic_dir.iterdir():
                if not data_source_item.is_dir() or data_source_item.name.startswith(
                    "_"
                ):
                    continue

                data_source = data_source_item.name
                logger.debug(f"Loading custom logic for data source: {data_source}")

                # Scan for data type directories within each data source
                for data_type_item in data_source_item.iterdir():
                    if not data_type_item.is_dir() or data_type_item.name.startswith(
                        "_"
                    ):
                        continue

                    data_type = data_type_item.name

                    # Check if custom_plugins.py exists
                    custom_plugins_file = data_type_item / "custom_plugins.py"
                    config_file = data_type_item / "config.yaml"

                    if custom_plugins_file.exists() and config_file.exists():
                        self.custom_logic_plugins[data_type] = {
                            "data_source": data_source,
                            "plugins_module": f"streamlined.plugins.custom_logic.{data_source}.{data_type}.custom_plugins",
                            "config_file": str(config_file),
                        }
                        logger.info(
                            f"Loaded custom logic for {data_type} ({data_source})"
                        )
                    else:
                        logger.debug(
                            f"Skipping {data_type} ({data_source}) - missing custom_plugins.py or config.yaml"
                        )

        except Exception as e:
            logger.error(f"Error loading custom logic plugins: {e}")

    def _get_custom_logic_dir(self) -> Path:
        """Get the custom_logic directory path."""
        # Current file: src/bicam_collection/libs/data_type_router.py
        # Target: src/streamlined/plugins/custom_logic/
        current_file = Path(__file__)
        project_root = current_file.parent.parent.parent.parent
        return project_root / "src" / "streamlined" / "plugins" / "custom_logic"

    def get_plugin_module(self, data_type: str) -> str | None:
        """Get the plugin module path for a data type."""
        if data_type in self.custom_logic_plugins:
            return self.custom_logic_plugins[data_type]["plugins_module"]
        return None

    def get_config_file(self, data_type: str) -> str | None:
        """Get the config file path for a data type."""
        if data_type in self.custom_logic_plugins:
            return self.custom_logic_plugins[data_type]["config_file"]
        return None

    def get_data_source(self, data_type: str) -> str | None:
        """Get the data source for a data type."""
        if data_type in self.custom_logic_plugins:
            return self.custom_logic_plugins[data_type]["data_source"]
        return None

    def create_fetcher_logic(self, data_type: str) -> Any:
        """Create a fetcher logic instance for a data type."""
        plugin_module = self.get_plugin_module(data_type)
        if not plugin_module:
            raise ValueError(f"No plugin module found for data type: {data_type}")

        try:
            import importlib

            module = importlib.import_module(plugin_module)

            # Get the fetcher logic class
            fetcher_class_name = f"{data_type.title()}FetcherLogic"
            fetcher_class = getattr(module, fetcher_class_name, None)

            if fetcher_class:
                return fetcher_class()
            else:
                logger.warning(f"No fetcher logic class found for {data_type}")
                return None

        except ImportError as e:
            logger.error(f"Error importing plugin module for {data_type}: {e}")
            return None

    def create_normalizer_logic(self, data_type: str) -> Any:
        """Create a normalizer logic instance for a data type."""
        plugin_module = self.get_plugin_module(data_type)
        if not plugin_module:
            raise ValueError(f"No plugin module found for data type: {data_type}")

        try:
            import importlib

            module = importlib.import_module(plugin_module)

            # Get the normalizer logic class
            normalizer_class_name = f"{data_type.title()}NormalizerLogic"
            normalizer_class = getattr(module, normalizer_class_name, None)

            if normalizer_class:
                data_source = self.get_data_source(data_type)
                return normalizer_class(
                    data_type_name=data_type, system_name=data_source
                )
            else:
                logger.warning(f"No normalizer logic class found for {data_type}")
                return None

        except ImportError as e:
            logger.error(f"Error importing plugin module for {data_type}: {e}")
            return None

    def create_cleaner_logic(self, data_type: str) -> Any:
        """Create a cleaner logic instance for a data type."""
        plugin_module = self.get_plugin_module(data_type)
        if not plugin_module:
            raise ValueError(f"No plugin module found for data type: {data_type}")

        try:
            import importlib

            module = importlib.import_module(plugin_module)

            # Get the cleaner logic class
            cleaner_class_name = f"{data_type.title()}CleanerLogic"
            cleaner_class = getattr(module, cleaner_class_name, None)

            if cleaner_class:
                data_source = self.get_data_source(data_type)
                return cleaner_class(data_type_name=data_type, system_name=data_source)
            else:
                logger.warning(f"No cleaner logic class found for {data_type}")
                return None

        except ImportError as e:
            logger.error(f"Error importing plugin module for {data_type}: {e}")
            return None

    def list_data_types(self) -> list[str]:
        """Get list of all registered data types."""
        return list(self.custom_logic_plugins.keys())

    def get_data_type_info(self, data_type: str) -> dict:
        """Get comprehensive information about a registered data type."""
        if data_type not in self.custom_logic_plugins:
            raise ValueError(f"Data type '{data_type}' is not registered")

        plugin_info = self.custom_logic_plugins[data_type]

        return {
            "data_type": data_type,
            "data_source": plugin_info["data_source"],
            "plugin_module": plugin_info["plugins_module"],
            "config_file": plugin_info["config_file"],
            "has_custom_logic": True,
        }


def create_router() -> DataTypeRouter:
    """Create a simplified router using custom logic plugins."""
    return DataTypeRouter()


def list_data_types() -> list[str]:
    """Get list of all registered data types."""
    router = create_router()
    return router.list_data_types()


def get_data_type_info(data_type: str) -> dict:
    """Get comprehensive information about a registered data type."""
    router = create_router()
    return router.get_data_type_info(data_type)


# Global router instance
_global_router: DataTypeRouter | None = None


def get_global_router() -> DataTypeRouter:
    """Get the global router instance."""
    global _global_router
    if _global_router is None:
        _global_router = DataTypeRouter()
    return _global_router
