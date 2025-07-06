"""
Plugin System for Streamlined Architecture

This module provides a plugin system that allows data type-specific customization
while maintaining the benefits of the simplified 4-layer architecture.

Key features:
- Data type plugins for custom logic
- Registry system for dynamic plugin loading
- Interface definitions for consistency
- Fallback to base implementations
"""

import logging
from typing import Any, Protocol

logger = logging.getLogger(__name__)


# =============================================================================
# PLUGIN INTERFACES
# =============================================================================


class FetcherPlugin(Protocol):
    """Interface for data type-specific fetcher plugins."""

    async def fetch_list_data(
        self,
        api_client,
        from_date: str | None = None,
        to_date: str | None = None,
        limit: int = 250,
        offset: int = 0,
        **kwargs,
    ) -> list[dict[str, Any]]:
        """Fetch Phase 1 list data."""
        ...

    async def fetch_detailed_data(
        self, api_client, url: str
    ) -> dict[str, Any] | None:
        """Fetch Phase 2 detailed data."""
        ...

    async def fetch_related_data(
        self, api_client, detailed_data: dict[str, Any]
    ) -> list[dict[str, Any]] | None:
        """Fetch Phase 3 related data."""
        ...

    def extract_item_id(self, data: dict[str, Any]) -> str:
        """Extract item ID from data."""
        ...


class CleanerPlugin(Protocol):
    """Interface for data type-specific cleaner plugins."""

    async def clean_record(
        self, record_data: dict[str, Any], data_type: str
    ) -> dict[str, Any]:
        """Clean a single record."""
        ...

    async def clean_table_data(
        self, table_data: list[dict[str, Any]], table_name: str
    ) -> list[dict[str, Any]]:
        """Clean table data with custom logic."""
        ...


class NormalizerPlugin(Protocol):
    """Interface for data type-specific normalizer plugins."""

    async def normalize_jsonb_data(
        self, db_pool, schema: str, table: str
    ) -> dict[str, Any]:
        """Normalize JSONB data with custom logic."""
        ...

    async def extract_list_data(
        self, db_pool, schema: str, source_table: str
    ) -> dict[str, Any]:
        """Extract list/array data with custom logic."""
        ...


# =============================================================================
# PLUGIN REGISTRY
# =============================================================================


class PluginRegistry:
    """Registry for data type plugins."""

    def __init__(self):
        self._fetcher_plugins: dict[str, FetcherPlugin] = {}
        self._cleaner_plugins: dict[str, CleanerPlugin] = {}
        self._normalizer_plugins: dict[str, NormalizerPlugin] = {}

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
        return self._fetcher_plugins.get(data_type)

    def get_cleaner_plugin(self, data_type: str) -> CleanerPlugin | None:
        """Get cleaner plugin for data type."""
        return self._cleaner_plugins.get(data_type)

    def get_normalizer_plugin(self, data_type: str) -> NormalizerPlugin | None:
        """Get normalizer plugin for data type."""
        return self._normalizer_plugins.get(data_type)

    def auto_register_plugins(self):
        """Auto-register plugins by discovering existing implementations."""
        try:
            # Discover and register congressional data types
            self._register_congressional_plugins()
            # Add other data source plugins here
        except Exception as e:
            logger.warning(f"Error auto-registering plugins: {e}")

    def _register_congressional_plugins(self):  # TODO: get from global registry
        """Register all congressional data type plugins."""
        congressional_types = [
            "bills",
            "amendments",
            "congresses",
            "committees",
            "committeemeetings",
            "committeeprints",
            "committeereports",
            "hearings",
            "members",
            "nominations",
            "treaties",
        ]

        for data_type in congressional_types:
            try:
                # Register fetcher plugin
                fetcher_plugin = CongressionalFetcherPlugin(data_type)
                self.register_fetcher_plugin(data_type, fetcher_plugin)

                # Register cleaner plugin
                cleaner_plugin = CongressionalCleanerPlugin(data_type)
                self.register_cleaner_plugin(data_type, cleaner_plugin)

                # Register normalizer plugin
                normalizer_plugin = CongressionalNormalizerPlugin(data_type)
                self.register_normalizer_plugin(data_type, normalizer_plugin)

            except Exception as e:
                logger.warning(f"Could not register plugins for {data_type}: {e}")


# Global plugin registry
_plugin_registry = PluginRegistry()


def get_plugin_registry() -> PluginRegistry:
    """Get the global plugin registry."""
    return _plugin_registry


# =============================================================================
# CONGRESSIONAL PLUGINS
# =============================================================================


class CongressionalFetcherPlugin:
    """Plugin that wraps existing congressional fetcher implementations."""

    def __init__(self, data_type: str):
        self.data_type = data_type
        self._fetcher_instance = None

    async def _get_fetcher_instance(self, api_client):
        """Get or create fetcher instance."""
        if self._fetcher_instance is None:
            # Dynamically import and create the appropriate fetcher
            self._fetcher_instance = await self._create_fetcher_instance(api_client)
        return self._fetcher_instance

    async def _create_fetcher_instance(self, api_client):
        """Create the appropriate fetcher instance."""
        try:
            # Map data type to fetcher class
            fetcher_mapping = {
                "bills": "bicam_collection.data_types.congressional.bills.fetcher.BillsFetcher",
                "amendments": "bicam_collection.data_types.congressional.amendments.fetcher.AmendmentsFetcher",
                "congresses": "bicam_collection.data_types.congressional.congresses.fetcher.CongressesFetcher",
                "committees": "bicam_collection.data_types.congressional.committees.fetcher.CommitteesFetcher",
                "committeemeetings": "bicam_collection.data_types.congressional.committeemeetings.fetcher.CommitteemeetingsFetcher",
                "committeeprints": "bicam_collection.data_types.congressional.committeeprints.fetcher.CommitteeprintsFetcher",
                "committeereports": "bicam_collection.data_types.congressional.committeereports.fetcher.CommitteereportsFetcher",
                "hearings": "bicam_collection.data_types.congressional.hearings.fetcher.HearingsFetcher",
                "members": "bicam_collection.data_types.congressional.members.fetcher.MembersFetcher",
                "nominations": "bicam_collection.data_types.congressional.nominations.fetcher.NominationsFetcher",
                "treaties": "bicam_collection.data_types.congressional.treaties.fetcher.TreatiesFetcher",
            }

            fetcher_class_path = fetcher_mapping.get(self.data_type)
            if not fetcher_class_path:
                raise ValueError(f"No fetcher mapping for {self.data_type}")

            # Import and instantiate the fetcher
            module_path, class_name = fetcher_class_path.rsplit(".", 1)
            module = __import__(module_path, fromlist=[class_name])
            fetcher_class = getattr(module, class_name)

            return fetcher_class(client=api_client)

        except Exception as e:
            logger.error(f"Failed to create fetcher for {self.data_type}: {e}")
            raise

    async def fetch_list_data(
        self,
        api_client,
        from_date: str | None = None,
        to_date: str | None = None,
        limit: int = 250,
        offset: int = 0,
        **kwargs,
    ) -> list[dict[str, Any]]:
        """Fetch Phase 1 list data using existing fetcher."""
        fetcher = await self._get_fetcher_instance(api_client)

        # Use the existing fetch_phase_1_data method
        items = []
        async for batch in fetcher.fetch_phase_1_data(
            from_date=from_date, to_date=to_date, limit=limit, **kwargs
        ):
            items.extend(batch)

        return items

    async def fetch_detailed_data(
        self, api_client, url: str
    ) -> dict[str, Any] | None:
        """Fetch Phase 2 detailed data using existing fetcher."""
        fetcher = await self._get_fetcher_instance(api_client)

        # Use the existing fetch_phase_2_data_with_client method
        return await fetcher.fetch_phase_2_data_with_client(url, api_client)

    async def fetch_related_data(
        self, api_client, detailed_data: dict[str, Any]
    ) -> list[dict[str, Any]] | None:
        """Fetch Phase 3 related data using existing fetcher."""
        fetcher = await self._get_fetcher_instance(api_client)

        # Use the existing fetch_phase_3_data_with_client method
        return await fetcher.fetch_phase_3_data_with_client(detailed_data, api_client)

    def extract_item_id(self, data: dict[str, Any]) -> str:
        """Extract item ID using existing fetcher logic."""
        if self._fetcher_instance:
            return self._fetcher_instance.extract_item_id(data)
        else:
            # Fallback ID extraction
            return data.get("number") or data.get("id") or str(hash(str(data)))[:8]


class CongressionalCleanerPlugin:
    """Plugin that wraps existing congressional cleaner implementations."""

    def __init__(self, data_type: str):
        self.data_type = data_type
        self._cleaner_instance = None

    async def _get_cleaner_instance(self):
        """Get or create cleaner instance."""
        if self._cleaner_instance is None:
            self._cleaner_instance = await self._create_cleaner_instance()
        return self._cleaner_instance

    async def _create_cleaner_instance(self):
        """Create the appropriate cleaner instance."""
        try:
            # Map data type to cleaner class
            cleaner_mapping = {
                "bills": "bicam_collection.data_types.congressional.bills.cleaner.BillsCleaner",
                "amendments": "bicam_collection.data_types.congressional.amendments.cleaner.AmendmentsCleaner",
                "congresses": "bicam_collection.data_types.congressional.congresses.cleaner.CongressesCleaner",
                "committees": "bicam_collection.data_types.congressional.committees.cleaner.CommitteesCleaner",
                "committeemeetings": "bicam_collection.data_types.congressional.committeemeetings.cleaner.CommitteemeetingsCleaner",
                "committeeprints": "bicam_collection.data_types.congressional.committeeprints.cleaner.CommitteeprintsCleaner",
                "committeereports": "bicam_collection.data_types.congressional.committeereports.cleaner.CommitteereportsCleaner",
                "hearings": "bicam_collection.data_types.congressional.hearings.cleaner.HearingsCleaner",
                "members": "bicam_collection.data_types.congressional.members.cleaner.MembersCleaner",
                "nominations": "bicam_collection.data_types.congressional.nominations.cleaner.NominationsCleaner",
                "treaties": "bicam_collection.data_types.congressional.treaties.cleaner.TreatiesCleaner",
            }

            cleaner_class_path = cleaner_mapping.get(self.data_type)
            if not cleaner_class_path:
                raise ValueError(f"No cleaner mapping for {self.data_type}")

            # Import and instantiate the cleaner
            module_path, class_name = cleaner_class_path.rsplit(".", 1)
            module = __import__(module_path, fromlist=[class_name])
            cleaner_class = getattr(module, class_name)

            return cleaner_class(data_type_name=self.data_type)

        except Exception as e:
            logger.error(f"Failed to create cleaner for {self.data_type}: {e}")
            raise

    async def clean_record(
        self, record_data: dict[str, Any], data_type: str
    ) -> dict[str, Any]:
        """Clean a single record using existing cleaner logic."""
        cleaner = await self._get_cleaner_instance()

        # Use the existing _clean_single_record method
        (
            cleaned_data,
            target_table,
            original_data_type,
        ) = await cleaner._clean_single_record(record_data, data_type)

        return cleaned_data

    async def clean_table_data(
        self, table_data: list[dict[str, Any]], table_name: str
    ) -> list[dict[str, Any]]:
        """Clean table data using existing cleaner logic."""
        cleaner = await self._get_cleaner_instance()

        # Clean each record in the table
        cleaned_data = []
        for record in table_data:
            try:
                cleaned_record = await self.clean_record(record, table_name)
                cleaned_data.append(cleaned_record)
            except Exception as e:
                logger.error(f"Error cleaning record in {table_name}: {e}")

        return cleaned_data


class CongressionalNormalizerPlugin:
    """Plugin that wraps existing congressional normalizer implementations."""

    def __init__(self, data_type: str):
        self.data_type = data_type
        self._normalizer_instance = None

    async def _get_normalizer_instance(self):
        """Get or create normalizer instance."""
        if self._normalizer_instance is None:
            self._normalizer_instance = await self._create_normalizer_instance()
        return self._normalizer_instance

    async def _create_normalizer_instance(self):
        """Create the appropriate normalizer instance."""
        try:
            # Map data type to normalizer class
            normalizer_mapping = {
                "bills": "bicam_collection.data_types.congressional.bills.database_normalizer.BillsDatabaseNormalizer",
                "amendments": "bicam_collection.data_types.congressional.amendments.database_normalizer.AmendmentsDatabaseNormalizer",
                "congresses": "bicam_collection.data_types.congressional.congresses.database_normalizer.CongressesDatabaseNormalizer",
                "committees": "bicam_collection.data_types.congressional.committees.database_normalizer.CommitteesDatabaseNormalizer",
                "committeemeetings": "bicam_collection.data_types.congressional.committeemeetings.database_normalizer.CommitteemeetingsDatabaseNormalizer",
                "committeeprints": "bicam_collection.data_types.congressional.committeeprints.database_normalizer.CommitteeprintsDatabaseNormalizer",
                "committeereports": "bicam_collection.data_types.congressional.committeereports.database_normalizer.CommitteereportsDatabaseNormalizer",
                "hearings": "bicam_collection.data_types.congressional.hearings.database_normalizer.HearingsDatabaseNormalizer",
                "members": "bicam_collection.data_types.congressional.members.database_normalizer.MembersDatabaseNormalizer",
                "nominations": "bicam_collection.data_types.congressional.nominations.database_normalizer.NominationsDatabaseNormalizer",
                "treaties": "bicam_collection.data_types.congressional.treaties.database_normalizer.TreatiesDatabaseNormalizer",
            }

            normalizer_class_path = normalizer_mapping.get(self.data_type)
            if not normalizer_class_path:
                raise ValueError(f"No normalizer mapping for {self.data_type}")

            # Import and instantiate the normalizer
            module_path, class_name = normalizer_class_path.rsplit(".", 1)
            module = __import__(module_path, fromlist=[class_name])
            normalizer_class = getattr(module, class_name)

            return normalizer_class(data_type_name=self.data_type)

        except Exception as e:
            logger.error(f"Failed to create normalizer for {self.data_type}: {e}")
            raise

    async def normalize_jsonb_data(
        self, db_pool, schema: str, table: str
    ) -> dict[str, Any]:
        """Normalize JSONB data using existing normalizer logic."""
        normalizer = await self._get_normalizer_instance()

        # Use existing normalization methods
        # This would call the appropriate methods on the normalizer
        return {"records_processed": 0, "tables_created": []}

    async def extract_list_data(
        self, db_pool, schema: str, source_table: str
    ) -> dict[str, Any]:
        """Extract list data using existing normalizer logic."""
        normalizer = await self._get_normalizer_instance()

        # Use existing list extraction methods
        # This would call the appropriate methods on the normalizer
        return {"records_processed": 0, "lists_extracted": []}


# =============================================================================
# AUTO-REGISTRATION
# =============================================================================


def initialize_plugins():
    """Initialize and auto-register all available plugins."""
    registry = get_plugin_registry()
    registry.auto_register_plugins()
    logger.info("Plugin system initialized and auto-registered")


# Initialize plugins on module import
initialize_plugins()
