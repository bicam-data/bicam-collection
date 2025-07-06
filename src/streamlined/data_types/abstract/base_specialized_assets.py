"""
Consolidated base specialized assets.

This module provides a unified base specialized assets class that can handle both Congressional and GovInfo
data types with configurable schemas and processing strategies.

FEATURES:
- Configurable schema defaults (staging_schema, production_schema)
- Configurable system names for progress tracking
- Advanced batch processing with checkpoint support
- Multi-table processing support
- Flexible asset generation patterns
- Support for both simple and complex data types
"""

import logging
from typing import Any

from bicam_collection.libs.data_type_registry import get_global_registry

logger = logging.getLogger(__name__)


class BaseSpecializedAssets:
    """
    Consolidated base specialized assets for all data types.

    Provides configurable schema defaults, progress tracking, and advanced processing
    capabilities that work for both Congressional and GovInfo data types.
    """

    def __init__(
        self,
        data_type_name: str,
        system_name: str,
        staging_schema: str | None = None,
        production_schema: str | None = None,
        **kwargs,
    ):
        """
        Initialize the base specialized assets.

        Args:
            data_type_name: Name of the data type (e.g., "bills", "members")
            system_name: System name for progress tracking (e.g., "congressional", "govinfo")
            staging_schema: Staging schema for data (auto-detected if None)
            production_schema: Production schema for data (auto-detected if None)
            **kwargs: Additional arguments passed to AbstractSpecializedAssets
        """
        # Auto-detect schemas based on system name if not provided
        if staging_schema is None:
            staging_schema = f"bicam_staging_{system_name}"
        if production_schema is None:
            production_schema = f"bicam_{system_name}"

        # Set instance attributes directly since we're not inheriting
        self.data_type_name = data_type_name
        self.staging_schema = staging_schema
        self.production_schema = production_schema
        self.system_name = system_name

        # Set other attributes from kwargs
        self.db_pool = kwargs.get("db_pool")
        self.checkpoint_manager = kwargs.get("checkpoint_manager")
        self.run_manager = kwargs.get("run_manager")

    def get_fetcher_class(self):
        """Get the appropriate fetcher class for this data source."""
        raise NotImplementedError("Subclasses must implement get_fetcher_class")

    def get_normalizer_class(self):
        """Get the appropriate normalizer class for this data source."""
        raise NotImplementedError("Subclasses must implement get_normalizer_class")

    def get_cleaner_class(self):
        """Get the appropriate cleaner class for this data source."""
        raise NotImplementedError("Subclasses must implement get_cleaner_class")

    def get_data_type_config(self) -> Any:
        """Get the data type configuration."""
        return get_global_registry().get_data_type_config(self.data_type_name)

    def get_table_names(self) -> list[str]:
        """Get all table names for this data type."""
        config = self.get_data_type_config()
        tables = [config.table_name]

        # Add related tables
        for suffix in config.related_tables:
            tables.append(f"{self.data_type_name}_{suffix}")

        return tables

    def get_schema_names(self) -> dict[str, str]:
        """Get schema names for this data type."""
        return {
            "raw": f"bicam_raw_{self.system_name}",
            "staging": f"bicam_staging_{self.system_name}",
            "production": f"bicam_{self.system_name}",
        }

    def get_processing_config(self) -> dict[str, Any]:
        """Get processing configuration for this data type."""
        config = self.get_data_type_config()
        return {
            "checkpoint_frequency": config.processing.checkpoint_frequency,
            "batch_size": config.processing.batch_size,
            "page_size": config.api.page_size,
            "retry_attempts": config.api.retry_attempts,
        }

    def get_api_config(self) -> dict[str, Any]:
        """Get API configuration for this data type."""
        config = self.get_data_type_config()
        return {
            "api_endpoint": config.api.api_endpoint,
            "list_key": config.api.list_key,
            "full_key": config.api.full_key,
            "page_size": config.api.page_size,
            "retry_attempts": config.api.retry_attempts,
        }

    def get_schema_config(self) -> dict[str, Any]:
        """Get schema configuration for this data type."""
        config = self.get_data_type_config()
        return {
            "table_name": config.schema.table_name,
            "is_main": config.schema.is_main,
            "create_raw": config.schema.create_raw,
            "id_fields": config.schema.id_fields,
            "related_tables": config.schema.related_tables,
            "fields": config.schema.fields,
        }

    def validate_configuration(self) -> bool:
        """Validate that the configuration is complete and correct."""
        try:
            config = self.get_data_type_config()

            # Check required fields
            if not config.name:
                logger.error(f"Missing name in configuration for {self.data_type_name}")
                return False

            if not config.schema.table_name:
                logger.error(
                    f"Missing table_name in schema configuration for {self.data_type_name}"
                )
                return False

            if not config.schema.id_fields:
                logger.error(
                    f"Missing id_fields in schema configuration for {self.data_type_name}"
                )
                return False

            if not config.api.api_endpoint:
                logger.error(
                    f"Missing api_endpoint in API configuration for {self.data_type_name}"
                )
                return False

            return True

        except Exception as e:
            logger.error(
                f"Configuration validation failed for {self.data_type_name}: {e}"
            )
            return False

    def get_asset_dependencies(self) -> list[str]:
        """Get list of asset dependencies for this data type."""
        # Default implementation - subclasses can override
        return []

    def get_asset_description(self) -> str:
        """Get description of this specialized asset."""
        config = self.get_data_type_config()
        return (
            config.description
            or f"{self.system_name.title()} {self.data_type_name} specialized assets"
        )

    def get_asset_metadata(self) -> dict[str, Any]:
        """Get metadata for this specialized asset."""
        return {
            "data_type": self.data_type_name,
            "system_name": self.system_name,
            "staging_schema": self.staging_schema,
            "production_schema": self.production_schema,
            "description": self.get_asset_description(),
            "dependencies": self.get_asset_dependencies(),
            "table_names": self.get_table_names(),
            "schema_names": self.get_schema_names(),
        }
