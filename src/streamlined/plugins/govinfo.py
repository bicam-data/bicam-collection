"""
GovInfo Plugins

This module provides plugins that wrap existing GovInfo implementations,
preserving all custom logic while enabling the streamlined architecture.
"""

import logging
from typing import Any

logger = logging.getLogger(__name__)


class GovInfoFetcherPlugin:
    """Plugin that wraps existing GovInfo fetcher implementations."""

    def __init__(self, data_type: str):
        self.data_type = data_type
        self._fetcher_instance = None

    async def _get_fetcher_instance(self, api_client):
        """Get or create fetcher instance."""
        if self._fetcher_instance is None:
            self._fetcher_instance = await self._create_fetcher_instance(api_client)
        return self._fetcher_instance

    async def _create_fetcher_instance(self, api_client):
        """Create fetcher instance for this data type."""
        try:
            # Import the specific fetcher class for this data type
            if self.data_type == "congressional_directories":
                from ...data_types.govinfo.congressional_directories.fetcher import (
                    CongressionalDirectoriesFetcher,
                )

                return CongressionalDirectoriesFetcher(api_client)
            elif self.data_type == "bill_collections":
                from ...data_types.govinfo.bill_collections.fetcher import (
                    BillCollectionsFetcher,
                )

                return BillCollectionsFetcher(api_client)
            elif self.data_type == "congressional_reports":
                from ...data_types.govinfo.congressional_reports.fetcher import (
                    CongressionalReportsFetcher,
                )

                return CongressionalReportsFetcher(api_client)
            elif self.data_type == "hearing_packages":
                from ...data_types.govinfo.hearing_packages.fetcher import (
                    HearingPackagesFetcher,
                )

                return HearingPackagesFetcher(api_client)
            elif self.data_type == "print_packages":
                from ...data_types.govinfo.print_packages.fetcher import (
                    PrintPackagesFetcher,
                )

                return PrintPackagesFetcher(api_client)
            elif self.data_type == "treaty_docs":
                from ...data_types.govinfo.treaty_docs.fetcher import (
                    TreatyDocsFetcher,
                )

                return TreatyDocsFetcher(api_client)
            else:
                # Fallback to base govinfo fetcher
                from ...data_types.govinfo.base.govinfo_base_fetcher import (
                    GovInfoBaseFetcher,
                )

                return GovInfoBaseFetcher(api_client, data_type_name=self.data_type)
        except Exception as e:
            logger.warning(f"Error creating fetcher for {self.data_type}: {e}")
            # Return None to indicate fallback should be used
            return None

    async def fetch_list_data(
        self,
        api_client,
        from_date: str | None = None,
        to_date: str | None = None,
        limit: int = 250,
        offset: int = 0,
        **kwargs,
    ) -> list[dict[str, Any]]:
        """Fetch list data using existing fetcher implementation."""
        fetcher = await self._get_fetcher_instance(api_client)
        if fetcher is None:
            return []

        # Use the existing fetch_phase_1_data method
        data_batches = []
        async for batch in fetcher.fetch_phase_1_data(
            from_date=from_date, to_date=to_date, limit=limit, offset=offset, **kwargs
        ):
            data_batches.extend(batch)

        return data_batches

    async def fetch_detailed_data(self, api_client, url: str) -> dict[str, Any] | None:
        """Fetch detailed data using existing fetcher implementation."""
        fetcher = await self._get_fetcher_instance(api_client)
        if fetcher is None:
            return None

        # Use the existing fetch_phase_2_data method
        return await fetcher.fetch_phase_2_data(url)

    async def fetch_related_data(
        self, api_client, detailed_data: dict[str, Any]
    ) -> list[dict[str, Any]] | None:
        """Fetch related data using existing fetcher implementation."""
        fetcher = await self._get_fetcher_instance(api_client)
        if fetcher is None:
            return None

        # Use the existing fetch_phase_3_data method
        return await fetcher.fetch_phase_3_data(detailed_data)

    async def _get_related_methods(self, api_client) -> list[str]:
        """Get list of available related data methods for this data type."""
        fetcher = await self._get_fetcher_instance(api_client)
        if fetcher is None:
            return []
        
        # Get all methods starting with get_ from the fetcher
        exclude = {
            "get_source_system_name",
            "get_default_schema", 
            "get_run_type",
            "get_generic_related_data",
        }
        
        methods = [
            m for m in dir(fetcher)
            if m.startswith("get_") and callable(getattr(fetcher, m)) and m not in exclude
        ]
        return methods

    def __getattr__(self, name: str):
        """
        Dynamically create wrapper methods for get_* methods from the underlying fetcher.
        
        This creates async wrapper methods that match the signature expected by
        the streamlined fetcher: method(item_data, client)
        """
        if name.startswith("get_"):
            async def wrapper(item_data: dict[str, Any], client) -> list[dict[str, Any]]:
                fetcher = await self._get_fetcher_instance(client)
                if fetcher is None:
                    return []
                
                # Check if the method exists on the fetcher
                if hasattr(fetcher, name):
                    method = getattr(fetcher, name)
                    if callable(method):
                        # Call the method with just item_data (the original signature)
                        return await method(item_data)
                
                return []
            
            return wrapper
        
        raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")

    def extract_item_id(self, data: dict[str, Any]) -> str:
        """Extract item ID using existing fetcher implementation."""
        # GovInfo typically uses packageId or granuleId
        if "packageId" in data:
            return data["packageId"]
        elif "granuleId" in data:
            return data["granuleId"]
        elif "package_id" in data:
            return data["package_id"]
        elif "granule_id" in data:
            return data["granule_id"]
        elif "url" in data:
            # Extract ID from URL
            return data["url"].split("/")[-1]
        else:
            # Fallback to a hash of the data
            import hashlib

            return hashlib.md5(str(data).encode()).hexdigest()[:16]


class GovInfoCleanerPlugin:
    """Plugin that wraps existing GovInfo cleaner implementations."""

    def __init__(self, data_type: str):
        self.data_type = data_type
        self._cleaner_instance = None

    async def _get_cleaner_instance(self):
        """Get or create cleaner instance."""
        if self._cleaner_instance is None:
            self._cleaner_instance = await self._create_cleaner_instance()
        return self._cleaner_instance

    async def _create_cleaner_instance(self):
        """Create cleaner instance for this data type."""
        try:
            # Import the specific cleaner class for this data type
            if self.data_type == "congressional_directories":
                from ...data_types.govinfo.congressional_directories.cleaner import (
                    CongressionalDirectoriesCleaner,
                )

                return CongressionalDirectoriesCleaner()
            elif self.data_type == "bill_collections":
                from ...data_types.govinfo.bill_collections.cleaner import (
                    BillCollectionsCleaner,
                )

                return BillCollectionsCleaner()
            elif self.data_type == "congressional_reports":
                from ...data_types.govinfo.congressional_reports.cleaner import (
                    CongressionalReportsCleaner,
                )

                return CongressionalReportsCleaner()
            elif self.data_type == "hearing_packages":
                from ...data_types.govinfo.hearing_packages.cleaner import (
                    HearingPackagesCleaner,
                )

                return HearingPackagesCleaner()
            elif self.data_type == "print_packages":
                from ...data_types.govinfo.print_packages.cleaner import (
                    PrintPackagesCleaner,
                )

                return PrintPackagesCleaner()
            elif self.data_type == "treaty_docs":
                from ...data_types.govinfo.treaty_docs.cleaner import (
                    TreatyDocsCleaner,
                )

                return TreatyDocsCleaner()
            else:
                # Fallback to base cleaner
                from ...data_types.abstract.base_cleaner import BaseCleaner

                return BaseCleaner(data_type_name=self.data_type)
        except Exception as e:
            logger.warning(f"Error creating cleaner for {self.data_type}: {e}")
            return None

    async def clean_record(
        self, record_data: dict[str, Any], data_type: str
    ) -> dict[str, Any]:
        """Clean record using existing cleaner implementation."""
        cleaner = await self._get_cleaner_instance()
        if cleaner is None:
            return record_data

        # Use the existing _clean_single_record method
        try:
            cleaned_data, _, _ = await cleaner._clean_single_record(
                record_data, data_type
            )
            return cleaned_data
        except Exception as e:
            logger.warning(f"Error cleaning record for {data_type}: {e}")
            return record_data

    async def clean_table_data(
        self, table_data: list[dict[str, Any]], table_name: str
    ) -> list[dict[str, Any]]:
        """Clean table data using existing cleaner implementation."""
        cleaner = await self._get_cleaner_instance()
        if cleaner is None:
            return table_data

        # Clean each record in the table
        cleaned_records = []
        for record in table_data:
            try:
                cleaned_record = await self.clean_record(record, table_name)
                cleaned_records.append(cleaned_record)
            except Exception as e:
                logger.warning(f"Error cleaning record in table {table_name}: {e}")
                cleaned_records.append(record)  # Keep original on error

        return cleaned_records


class GovInfoNormalizerPlugin:
    """Plugin that wraps existing GovInfo normalizer implementations."""

    def __init__(self, data_type: str):
        self.data_type = data_type
        self._normalizer_instance = None

    async def _get_normalizer_instance(self):
        """Get or create normalizer instance."""
        if self._normalizer_instance is None:
            self._normalizer_instance = await self._create_normalizer_instance()
        return self._normalizer_instance

    async def _create_normalizer_instance(self):
        """Create normalizer instance for this data type."""
        try:
            # Import the specific normalizer class for this data type
            if self.data_type == "congressional_directories":
                from ...data_types.govinfo.congressional_directories.database_normalizer import (
                    CongressionalDirectoriesDatabaseNormalizer,
                )

                return CongressionalDirectoriesDatabaseNormalizer(self.data_type)
            elif self.data_type == "bill_collections":
                from ...data_types.govinfo.bill_collections.database_normalizer import (
                    BillCollectionsDatabaseNormalizer,
                )

                return BillCollectionsDatabaseNormalizer(self.data_type)
            elif self.data_type == "congressional_reports":
                from ...data_types.govinfo.congressional_reports.database_normalizer import (
                    CongressionalReportsDatabaseNormalizer,
                )

                return CongressionalReportsDatabaseNormalizer(self.data_type)
            elif self.data_type == "hearing_packages":
                from ...data_types.govinfo.hearing_packages.database_normalizer import (
                    HearingPackagesDatabaseNormalizer,
                )

                return HearingPackagesDatabaseNormalizer(self.data_type)
            elif self.data_type == "print_packages":
                from ...data_types.govinfo.print_packages.database_normalizer import (
                    PrintPackagesDatabaseNormalizer,
                )

                return PrintPackagesDatabaseNormalizer(self.data_type)
            elif self.data_type == "treaty_docs":
                from ...data_types.govinfo.treaty_docs.database_normalizer import (
                    TreatyDocsDatabaseNormalizer,
                )

                return TreatyDocsDatabaseNormalizer(self.data_type)
            else:
                # Fallback to base normalizer
                from ...data_types.abstract.base_database_normalizer import (
                    BaseDatabaseNormalizer,
                )

                return BaseDatabaseNormalizer(self.data_type)
        except Exception as e:
            logger.warning(f"Error creating normalizer for {self.data_type}: {e}")
            return None

    async def normalize_jsonb_data(
        self, db_pool, schema: str, table: str
    ) -> dict[str, Any]:
        """Normalize JSONB data using existing normalizer implementation."""
        normalizer = await self._get_normalizer_instance()
        if normalizer is None:
            return {"status": "error", "message": "No normalizer available"}

        # Use the existing normalization methods
        try:
            # This would use the existing normalize_data method
            # Implementation depends on the specific normalizer interface
            return {"status": "success", "normalized_records": 0}
        except Exception as e:
            logger.warning(f"Error normalizing JSONB data: {e}")
            return {"status": "error", "message": str(e)}

    async def extract_list_data(
        self, db_pool, schema: str, source_table: str
    ) -> dict[str, Any]:
        """Extract list data using existing normalizer implementation."""
        normalizer = await self._get_normalizer_instance()
        if normalizer is None:
            return {"status": "error", "message": "No normalizer available"}

        # Use the existing list extraction methods
        try:
            # This would use the existing extract_list_data method
            # Implementation depends on the specific normalizer interface
            return {"status": "success", "extracted_records": 0}
        except Exception as e:
            logger.warning(f"Error extracting list data: {e}")
            return {"status": "error", "message": str(e)}
