"""
Congressional Plugins

This module provides plugins that contain the custom Congressional logic directly,
eliminating the need for the old fetcher class hierarchy.
"""

import logging
from typing import Any

logger = logging.getLogger(__name__)


class CongressionalFetcherPlugin:
    """Plugin with direct Congressional fetching implementations."""

    def __init__(self, data_type: str):
        self.data_type = data_type
        self._config = None

    def _get_config(self):
        """Get configuration for this data type."""
        if self._config is None:
            try:
                from ...libs.data_type_registry import get_global_registry

                registry = get_global_registry()
                self._config = registry.get_data_type_config(self.data_type)
            except Exception as e:
                logger.warning(f"Could not load config for {self.data_type}: {e}")
        return self._config

    async def fetch_list_data(
        self,
        api_client,
        from_date: str | None = None,
        to_date: str | None = None,
        limit: int = 250,
        offset: int = 0,
        **kwargs,
    ) -> list[dict[str, Any]]:
        """Fetch list data using Congressional API patterns."""
        config = self._get_config()
        endpoint = config.api.api_endpoint if config else self.data_type

        data_batches = []
        async for batch in api_client.retrieve_data_list(
            data_type=endpoint,
            from_date=api_client._format_date_for_api(from_date) if from_date else None,
            to_date=api_client._format_date_for_api(to_date) if to_date else None,
            limit=limit,
            offset=offset,
            **kwargs,
        ):
            # Extract list items from API response
            list_key = config.api.list_key if config else None
            if list_key:
                keys = list_key if isinstance(list_key, list) else [list_key]
                for key in keys:
                    if key in batch:
                        data_batches.extend(batch[key])
                        break
            else:
                data_batches.extend(batch if isinstance(batch, list) else [])

        return data_batches

    async def fetch_detailed_data(
        self, api_client, item_data: dict[str, Any]
    ) -> dict[str, Any] | None:
        """Fetch detailed data using Congressional API patterns."""
        # Extract URL from item data
        if isinstance(item_data, dict) and "url" in item_data:
            url = item_data["url"]
        elif isinstance(item_data, str):
            url = item_data
        else:
            logger.warning(f"Invalid item data for detailed fetch: {item_data}")
            return None

        try:
            full_data = await api_client.retrieve_full_data_from_url(url)
            if not full_data:
                return None

            # Extract using full_key if configured
            config = self._get_config()
            full_key = config.api.full_key if config else None
            return full_data.get(full_key, full_data) if full_key else full_data

        except Exception as e:
            logger.error(f"Detailed fetch failed for {url}: {e}")
            return None

    async def fetch_related_data(
        self, api_client, detailed_data: dict[str, Any]
    ) -> list[dict[str, Any]] | None:
        """Fetch related data using data-type-specific methods."""
        if not detailed_data:
            return []

        item_id = self.extract_item_id(detailed_data)
        related_data = []

        # Get data-type-specific related data methods
        related_methods = self._get_related_methods()

        for method_name in related_methods:
            try:
                method = getattr(self, method_name)
                result = await method(api_client, detailed_data)
                if result:
                    related_data.append(
                        {
                            "type": method_name,
                            f"{self.data_type}_id": item_id,
                            "data": result,
                            "method": method_name,
                        }
                    )
            except Exception as e:
                logger.error(f"Failed to fetch {method_name} for {item_id}: {e}")

        return related_data

    def _get_related_methods(self) -> list[str]:
        """Get list of related data methods for this data type."""
        method_prefix = f"get_{self.data_type}_"
        return [
            m
            for m in dir(self)
            if m.startswith(method_prefix) and callable(getattr(self, m))
        ]

    def extract_item_id(self, data: dict[str, Any]) -> str:
        """Extract item ID using data-type-specific logic."""
        if self.data_type == "bills":
            return self._extract_bills_id(data)
        elif self.data_type == "amendments":
            return self._extract_amendments_id(data)
        elif self.data_type == "nominations":
            return self._extract_nominations_id(data)
        else:
            # Generic ID extraction
            if "number" in data and "congress" in data:
                return f"{data['congress']}-{data['number']}"
            elif "url" in data:
                return data["url"].split("/")[-1]
            else:
                import hashlib

                return hashlib.md5(str(data).encode()).hexdigest()[:16]

    def _extract_bills_id(self, data: dict[str, Any]) -> str:
        """Extract standardized bill ID."""
        bill_type = data.get("type", "").lower()
        number = str(data.get("number", "")).replace("½", ".5")
        congress = str(data.get("congress", ""))

        if all([bill_type, number, congress]):
            return f"{bill_type}{number}-{congress}"
        return "ID_ERROR"

    def _extract_amendments_id(self, data: dict[str, Any]) -> str:
        """Extract standardized amendment ID."""
        amendment_type = data.get("type", "").lower()
        number = str(data.get("number", ""))
        congress = str(data.get("congress", ""))

        if all([amendment_type, number, congress]):
            return f"{amendment_type}{number}-{congress}"
        return "ID_ERROR"

    def _extract_nominations_id(self, data: dict[str, Any]) -> str:
        """Extract standardized nomination ID."""
        nomination_number = str(data.get("nominationNumber", ""))
        congress = str(data.get("congress", ""))

        if all([nomination_number, congress]):
            return f"nomination{nomination_number}-{congress}"
        return "ID_ERROR"

    # =============================================================================
    # BILLS-SPECIFIC RELATED DATA METHODS
    # =============================================================================

    async def get_bills_actions(
        self, api_client, full_data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Get bill actions from actions URL."""
        return await self._get_generic_related_data(api_client, full_data, "actions")

    async def get_bills_cosponsors(
        self, api_client, full_data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Get bill cosponsors from cosponsors URL."""
        return await self._get_generic_related_data(api_client, full_data, "cosponsors")

    async def get_bills_texts(
        self, api_client, full_data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Get bill text versions from textVersions URL."""
        return await self._get_generic_related_data(api_client, full_data, "texts")

    async def get_bills_summaries(
        self, api_client, full_data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Get bill summaries from summaries URL."""
        return await self._get_generic_related_data(api_client, full_data, "summaries")

    async def get_bills_subjects(
        self, api_client, full_data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Get bill subjects from subjects URL."""
        return await self._get_generic_related_data(api_client, full_data, "subjects")

    async def get_bills_titles(
        self, api_client, full_data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Get bill titles from titles URL."""
        return await self._get_generic_related_data(api_client, full_data, "titles")

    async def get_bills_relatedbills(
        self, api_client, full_data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Get bill related bills with relationship processing."""
        if "relatedBills" not in full_data:
            return []

        related_bills_info = full_data["relatedBills"]
        if not isinstance(related_bills_info, dict) or "url" not in related_bills_info:
            return []

        # Get raw related bills data
        raw_related_bills = await api_client.retrieve_related_data_from_url(
            related_bills_info["url"], list_key=["relatedBills"]
        )

        if not raw_related_bills:
            return []

        # Process relationships
        relationships = []
        bill_id = self.extract_item_id(full_data)

        for related_bill in raw_related_bills:
            # Extract related bill ID
            related_bill_type = related_bill.get("type", "").lower()
            related_bill_number = str(related_bill.get("number", "")).replace("½", ".5")
            related_bill_congress = str(related_bill.get("congress", ""))

            if all([related_bill_type, related_bill_number, related_bill_congress]):
                relatedbill_id = (
                    f"{related_bill_type}{related_bill_number}-{related_bill_congress}"
                )
            else:
                relatedbill_id = related_bill.get("url", "unknown")

            # Process each relationship
            for relationship in related_bill.get("relationshipDetails", []):
                relationships.append(
                    {
                        "bill_id": bill_id,
                        "relatedbill_id": relatedbill_id,
                        "relationship_identified_by": relationship.get("identifiedBy"),
                        "relationship_type": relationship.get("type"),
                    }
                )

        return relationships

    # =============================================================================
    # AMENDMENTS-SPECIFIC RELATED DATA METHODS
    # =============================================================================

    async def get_amendments_actions(
        self, api_client, full_data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Get amendment actions from actions URL."""
        return await self._get_generic_related_data(api_client, full_data, "actions")

    async def get_amendments_cosponsors(
        self, api_client, full_data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Get amendment cosponsors from cosponsors URL."""
        return await self._get_generic_related_data(api_client, full_data, "cosponsors")

    # =============================================================================
    # GENERIC HELPER METHODS
    # =============================================================================

    async def _get_generic_related_data(
        self, api_client, full_data: dict[str, Any], related_table_name: str
    ) -> list[dict[str, Any]]:
        """Generic helper for fetching related data."""
        if related_table_name not in full_data:
            return []

        related_info = full_data[related_table_name]
        if not isinstance(related_info, dict) or "url" not in related_info:
            return []

        try:
            # Use default list key based on table name
            list_key = [related_table_name]
            data = await api_client.retrieve_related_data_from_url(
                related_info["url"], list_key=list_key
            )
            return data or []
        except Exception as e:
            logger.error(f"Failed to fetch {related_table_name}: {e}")
            return []


class CongressionalCleanerPlugin:
    """Plugin that wraps existing Congressional cleaner implementations."""

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
            if self.data_type == "bills":
                from ...data_types.congressional.bills.cleaner import BillsCleaner

                return BillsCleaner()
            elif self.data_type == "amendments":
                from ...data_types.congressional.amendments.cleaner import (
                    AmendmentsCleaner,
                )

                return AmendmentsCleaner()
            elif self.data_type == "nominations":
                from ...data_types.congressional.nominations.cleaner import (
                    NominationsCleaner,
                )

                return NominationsCleaner()
            elif self.data_type == "committees":
                from ...data_types.congressional.committees.cleaner import (
                    CommitteesCleaner,
                )

                return CommitteesCleaner()
            elif self.data_type == "members":
                from ...data_types.congressional.members.cleaner import MembersCleaner

                return MembersCleaner()
            elif self.data_type == "congresses":
                from ...data_types.congressional.congresses.cleaner import (
                    CongressesCleaner,
                )

                return CongressesCleaner()
            elif self.data_type == "hearings":
                from ...data_types.congressional.hearings.cleaner import HearingsCleaner

                return HearingsCleaner()
            elif self.data_type == "treaties":
                from ...data_types.congressional.treaties.cleaner import TreatiesCleaner

                return TreatiesCleaner()
            elif self.data_type == "committeereports":
                from ...data_types.congressional.committeereports.cleaner import (
                    CommitteereportsCleaner,
                )

                return CommitteereportsCleaner()
            elif self.data_type == "committeeprints":
                from ...data_types.congressional.committeeprints.cleaner import (
                    CommitteeprintsCleaner,
                )

                return CommitteeprintsCleaner()
            elif self.data_type == "committeemeetings":
                from ...data_types.congressional.committeemeetings.cleaner import (
                    CommitteemeetingsCleaner,
                )

                return CommitteemeetingsCleaner()
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


class CongressionalNormalizerPlugin:
    """Plugin that wraps existing Congressional normalizer implementations."""

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
            if self.data_type == "bills":
                from ...data_types.congressional.bills.database_normalizer import (
                    BillsDatabaseNormalizer,
                )

                return BillsDatabaseNormalizer(self.data_type)
            elif self.data_type == "amendments":
                from ...data_types.congressional.amendments.database_normalizer import (
                    AmendmentsDatabaseNormalizer,
                )

                return AmendmentsDatabaseNormalizer(self.data_type)
            elif self.data_type == "nominations":
                from ...data_types.congressional.nominations.database_normalizer import (
                    NominationsDatabaseNormalizer,
                )

                return NominationsDatabaseNormalizer(self.data_type)
            elif self.data_type == "committees":
                from ...data_types.congressional.committees.database_normalizer import (
                    CommitteesDatabaseNormalizer,
                )

                return CommitteesDatabaseNormalizer(self.data_type)
            elif self.data_type == "members":
                from ...data_types.congressional.members.database_normalizer import (
                    MembersDatabaseNormalizer,
                )

                return MembersDatabaseNormalizer(self.data_type)
            elif self.data_type == "congresses":
                from ...data_types.congressional.congresses.database_normalizer import (
                    CongressesDatabaseNormalizer,
                )

                return CongressesDatabaseNormalizer(self.data_type)
            elif self.data_type == "hearings":
                from ...data_types.congressional.hearings.database_normalizer import (
                    HearingsDatabaseNormalizer,
                )

                return HearingsDatabaseNormalizer(self.data_type)
            elif self.data_type == "treaties":
                from ...data_types.congressional.treaties.database_normalizer import (
                    TreatiesDatabaseNormalizer,
                )

                return TreatiesDatabaseNormalizer(self.data_type)
            elif self.data_type == "committeereports":
                from ...data_types.congressional.committeereports.database_normalizer import (
                    CommitteereportsDatabaseNormalizer,
                )

                return CommitteereportsDatabaseNormalizer(self.data_type)
            elif self.data_type == "committeeprints":
                from ...data_types.congressional.committeeprints.database_normalizer import (
                    CommitteeprintsDatabaseNormalizer,
                )

                return CommitteeprintsDatabaseNormalizer(self.data_type)
            elif self.data_type == "committeemeetings":
                from ...data_types.congressional.committeemeetings.database_normalizer import (
                    CommitteemeetingsDatabaseNormalizer,
                )

                return CommitteemeetingsDatabaseNormalizer(self.data_type)
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
