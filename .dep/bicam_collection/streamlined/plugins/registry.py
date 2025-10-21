"""
Plugin Registry

This module provides a centralized registry for all plugins, with automatic
discovery and registration of existing implementations.
"""

import logging
from typing import Any

from .base import CleanerPlugin, FetcherPlugin, NormalizerPlugin

logger = logging.getLogger(__name__)


class PluginRegistry:
    """Centralized registry for all plugins with automatic discovery."""

    def __init__(self):
        self._fetcher_plugins: dict[str, FetcherPlugin] = {}
        self._cleaner_plugins: dict[str, CleanerPlugin] = {}
        self._normalizer_plugins: dict[str, NormalizerPlugin] = {}
        self._initialized = False

    def register_fetcher_plugin(self, data_type: str, plugin: FetcherPlugin):
        """Register a fetcher plugin for a data type."""
        self._fetcher_plugins[data_type] = plugin
        logger.debug(f"Registered fetcher plugin for {data_type}")

    def register_cleaner_plugin(self, data_type: str, plugin: CleanerPlugin):
        """Register a cleaner plugin for a data type."""
        self._cleaner_plugins[data_type] = plugin
        logger.debug(f"Registered cleaner plugin for {data_type}")

    def register_normalizer_plugin(self, data_type: str, plugin: NormalizerPlugin):
        """Register a normalizer plugin for a data type."""
        self._normalizer_plugins[data_type] = plugin
        logger.debug(f"Registered normalizer plugin for {data_type}")

    def get_fetcher_plugin(self, data_type: str) -> FetcherPlugin | None:
        """Get fetcher plugin for data type."""
        self._ensure_initialized()
        return self._fetcher_plugins.get(data_type)

    def get_cleaner_plugin(self, data_type: str) -> CleanerPlugin | None:
        """Get cleaner plugin for data type."""
        self._ensure_initialized()
        return self._cleaner_plugins.get(data_type)

    def get_normalizer_plugin(self, data_type: str) -> NormalizerPlugin | None:
        """Get normalizer plugin for data type."""
        self._ensure_initialized()
        return self._normalizer_plugins.get(data_type)

    def _ensure_initialized(self):
        """Ensure plugins are auto-registered."""
        if not self._initialized:
            self.auto_register_plugins()
            self._initialized = True

    def auto_register_plugins(self):
        """Auto-register plugins by discovering existing implementations."""
        try:
            # Register congressional data types
            self._register_congressional_plugins()

            # Register govinfo data types
            self._register_govinfo_plugins()

            logger.info("Auto-registered plugins successfully")
        except Exception as e:
            logger.warning(f"Error during plugin auto-registration: {e}")

    def _register_congressional_plugins(self):
        """Register plugins for congressional data types."""
        # Get congressional data types from the global registry
        try:
            from ...libs.data_type_router import get_global_registry

            registry = get_global_registry()
            congressional_types = [
                dt
                for dt in registry.list_data_types()
                if registry.get_data_source(dt) == "congressional"
            ]

            # Import and register congressional plugins
            from .congressional import (
                CongressionalCleanerPlugin,
                CongressionalFetcherPlugin,
                CongressionalNormalizerPlugin,
            )

            for data_type in congressional_types:
                # Register fetcher plugin
                fetcher_plugin = CongressionalFetcherPlugin(data_type)
                self.register_fetcher_plugin(data_type, fetcher_plugin)

                # Register cleaner plugin
                cleaner_plugin = CongressionalCleanerPlugin(data_type)
                self.register_cleaner_plugin(data_type, cleaner_plugin)

                # Register normalizer plugin
                normalizer_plugin = CongressionalNormalizerPlugin(data_type)
                self.register_normalizer_plugin(data_type, normalizer_plugin)

            logger.info(
                f"Registered congressional plugins for {len(congressional_types)} data types"
            )

        except Exception as e:
            logger.warning(f"Error registering congressional plugins: {e}")

    def _register_govinfo_plugins(self):
        """Register plugins for govinfo data types."""
        # TODO: Implement govinfo plugins when needed

    def get_stats(self) -> dict[str, Any]:
        """Get registry statistics."""
        self._ensure_initialized()
        return {
            "fetcher_plugins": len(self._fetcher_plugins),
            "cleaner_plugins": len(self._cleaner_plugins),
            "normalizer_plugins": len(self._normalizer_plugins),
            "registered_data_types": list(
                set(
                    list(self._fetcher_plugins.keys())
                    + list(self._cleaner_plugins.keys())
                    + list(self._normalizer_plugins.keys())
                )
            ),
        }


# Global registry instance
_global_registry: PluginRegistry | None = None


def get_plugin_registry() -> PluginRegistry:
    """Get the global plugin registry instance."""
    global _global_registry
    if _global_registry is None:
        _global_registry = PluginRegistry()
    return _global_registry


def initialize_plugins():
    """Initialize the plugin system."""
    registry = get_plugin_registry()
    registry.auto_register_plugins()
    logger.info("Plugin system initialized successfully")
