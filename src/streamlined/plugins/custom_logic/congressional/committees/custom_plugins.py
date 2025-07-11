import logging
from collections.abc import AsyncGenerator
from typing import Any

# Import the base class that provides shared functionality
from ....base import BaseCleanerLogic, CongressionalBaseFetcherLogic

logger = logging.getLogger(__name__)


class CommitteesFetcherLogic(CongressionalBaseFetcherLogic):
    """
    Committees-specific fetcher logic that extends the base Congressional fetcher logic.

    This class provides custom methods for fetching committees data, including:
    - Extracting standardized committee IDs
    - Handling related data from URLs
    - Implementing specialized fetching logic for committees-specific data

    Inherits shared functionality like get_generic_related_data from
    CongressionalBaseFetcherLogic.
    """

    def __init__(self, data_type: str = "committees"):
        super().__init__(data_type)

    def extract_item_id(self, item_data: dict[str, Any], **kwargs) -> str:
        """Extract standardized committee ID from committee data."""
        return item_data.get("systemCode", "ID_ERROR")

    def _extract_preliminary_item_id(self, list_item: dict[str, Any]) -> str:
        """
        Derive a stable ID for checkpointing directly from the list-response record.
        This guarantees a non-null value so the progress-tracker can reliably
        decide whether the item was processed in a previous run.
        """

        return self.extract_item_id(list_item)

    # =============================================================================
    # COMMITTEES-SPECIFIC RELATED DATA METHODS
    # =============================================================================

    async def get_committees_committeereports(
        self, full_committee_data: dict[str, Any], client
    ) -> list[dict[str, Any]]:
        """Get committee reports from reports URL in full committee data."""
        return await self.get_generic_related_data(
            full_committee_data, related_table_name="reports", client=client
        )

    async def get_committees_bills(
        self, full_committee_data: dict[str, Any], client
    ) -> list[dict[str, Any]]:
        """Get committee bills from bills URL in full committee data with pagination support."""
        return await self.get_paginated_related_data(
            full_committee_data, related_table_name="bills", client=client
        )

    async def get_paginated_related_data(
        self,
        full_data: dict[str, Any],
        related_table_name: str,
        client,
        list_key: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Fetch related data with pagination support for large datasets.

        This method handles pagination for related data endpoints that return
        large numbers of items (like committees_bills with 25,000+ bills).

        Args:
            full_data: The complete item payload returned by Phase-2.
            related_table_name: The field in *full_data* that contains the
                `{"url": "…"}` dict pointing to the related endpoint.
            client: API client instance for making requests.
            list_key: Optional explicit list key path to use when
                deserialising the API response.

        Returns:
            A list of dictionaries representing all related entities across all pages.
        """
        try:
            # Basic validation of expected structure
            if related_table_name not in full_data:
                logger.warning(
                    f"Field '{related_table_name}' not found in committee data"
                )
                return []

            table_info = full_data[related_table_name]
            if not isinstance(table_info, dict) or "url" not in table_info:
                logger.warning(
                    f"Invalid structure for '{related_table_name}' field: {table_info}"
                )
                return []

            url = table_info["url"]
            logger.info(f"Fetching paginated data from: {url}")

            # Derive list_key from config unless explicitly provided
            effective_list_key = None

            if list_key is not None:
                effective_list_key = list_key
            else:
                # Try to load related table config to determine correct list_key
                try:
                    from .consolidated_registry import get_consolidated_registry

                    registry = get_consolidated_registry()
                    related_config_name = f"{self.data_type}_{related_table_name}"
                    config_file = registry.get_config_file(self.data_type)

                    if config_file:
                        import yaml

                        with open(config_file) as f:
                            config_data = yaml.safe_load(f)

                        # config_data is a list of config entries, find the one for related_table
                        if isinstance(config_data, list):
                            for entry in config_data:
                                if entry.get("name") == related_config_name:
                                    api_config = entry.get("api", {})
                                    cfg_list_key = api_config.get("list_key")

                                    if cfg_list_key:
                                        # Ensure list type for downstream helper
                                        effective_list_key = (
                                            [cfg_list_key]
                                            if isinstance(cfg_list_key, str)
                                            else cfg_list_key
                                        )
                                        logger.info(
                                            f"Using list_key from config: {effective_list_key}"
                                        )
                                        break
                except Exception as e:
                    logger.debug(
                        f"Could not derive list_key from config for {related_table_name}: {e}"
                    )

            # Fallback to simple default if still None
            if not effective_list_key:
                effective_list_key = [related_table_name]
                logger.info(f"Using fallback list_key: {effective_list_key}")

            # Extract the endpoint from the full URL
            if not url.startswith(client.base_url):
                logger.error(f"URL does not match base URL: {url}")
                return []

            # Remove base URL and extract endpoint
            endpoint = url.replace(client.base_url, "")

            # Remove existing query parameters (we'll add our own for pagination)
            if "?" in endpoint:
                endpoint = endpoint.split("?")[0]

            # Fetch all pages with pagination
            all_data = []
            current_offset = 0
            page_size = 250  # Congressional API default
            page_number = 1

            while True:
                # Build parameters for this page
                params = {
                    "limit": page_size,
                    "offset": current_offset,
                }

                try:
                    logger.info(
                        f"Fetching page {page_number} (offset: {current_offset})"
                    )

                    # Make the request
                    response = await client._make_request(endpoint, params)

                    # Extract pagination metadata
                    pagination = response.get("pagination", {})
                    total_count = pagination.get("count", 0)
                    next_url = pagination.get("next")

                    # Extract data using the list_key
                    if effective_list_key:
                        if len(effective_list_key) > 1:
                            # access the key as many times as needed
                            data = response
                            for key in effective_list_key:
                                if isinstance(data, dict) and key in data:
                                    data = data.get(key)
                                else:
                                    data = None
                                    break
                        else:
                            data = response.get(effective_list_key[0])
                    else:
                        # Fallback: try to find any list in the response
                        data = None
                        for value in response.values():
                            if isinstance(value, list):
                                data = value
                                break

                    if data and isinstance(data, list):
                        all_data.extend(data)
                        logger.info(
                            f"Page {page_number}: Got {len(data)} items, total so far: {len(all_data)}"
                        )
                    else:
                        logger.warning(
                            f"Page {page_number}: No data found or invalid format"
                        )
                        break

                    # Check if we have more pages
                    if not next_url:
                        logger.info(
                            f"No next URL found. Completed fetching after {page_number} pages."
                        )
                        break

                    # Update offset for next page
                    current_offset += len(data) if data else 0
                    page_number += 1

                    # Safety check to prevent infinite loops
                    if page_number > 1000:  # Arbitrary limit
                        logger.warning(
                            f"Reached maximum page limit ({page_number}), stopping pagination"
                        )
                        break

                except Exception as e:
                    logger.error(f"Error fetching page {page_number}: {e}")
                    break

            logger.info(
                f"Retrieved {len(all_data)} total items for '{related_table_name}' across {page_number} pages"
            )
            return all_data

        except Exception as e:
            logger.error(
                f"Failed to fetch paginated related data '{related_table_name}' for {self.data_type}: {e}"
            )
            return []


class CommitteesCleanerLogic(BaseCleanerLogic):
    """
    Committees-specific cleaner logic extracted from CommitteesCleaner class.
    Contains all the custom cleaning methods for committees data.
    """

    def __init__(
        self,
        data_type_name: str = "committees",
        system_name: str = "congressional",
        staging_schema: str = "bicam_staging_congressional",
        production_schema: str = "bicam_congressional",
    ):
        super().__init__()
        self.data_type_name = data_type_name
        self.system_name = system_name
        self.staging_schema = staging_schema
        self.production_schema = production_schema
        self.db_pool = None

        # Map data types that span or alias multiple staging tables.
        # In staging, leadership-role data lives in `members_leadership`,
        # but the logical/production data-type name we expose is
        # `members_leadershiproles`.  This mapping lets the base cleaner
        # stream from the correct table without emitting missing-table warnings.
        self.multi_table_data_types = {
            # Add committees to enable custom joined streaming
            "committees": ["committees", "committees_list_raw"],
            # Add other multi-table or alias mappings here as needed
        }

    # =============================================================================
    # COMMITTEES-SPECIFIC DATA TYPE CLEANING METHODS
    # =============================================================================

    async def _clean_committees_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual committees records.

        FINAL COLUMNS:
        - committee_code TEXT PRIMARY KEY,
        - name TEXT,
        - chamber TEXT,
        - is_subcommittee BOOLEAN,
        - is_current BOOLEAN,
        - bills_count INTEGER,
        - reports_count INTEGER,
        - nominations_count INTEGER,
        - updated_at TIMESTAMP WITH TIME ZONE

        STAGING COLUMNS:
        Based on Congressional API committee structure, typically includes:
        - type                 text,
        - bills_url            text,
        - bills_count          text,
        - parent_url           text,
        - parent_name          text,
        - parent_systemcode    text,
        - batch_id             text,
        - iscurrent            text,
        - systemcode           text,
        - updatedate           text,
        - communications_url   text,
        - communications_count text,
        - reports_url          text,
        - reports_count        text,
        - nominations_url      text,
        - nominations_count    text

        Args:
            record_data: Raw committees record from staging
        Returns:
            Cleaned committees record
        """
        cleaned = record_data.copy()

        filtered_cleaned = {
            "committee_code": cleaned.get("systemcode", "ID_ERROR"),
            "name": cleaned.get("name"),
            "chamber": self.standardize_chamber(cleaned.get("chamber")),
            "committee_type": cleaned.get("type"),
            "is_current": cleaned.get("iscurrent") == "true",
            "bills_count": self.safe_int(cleaned.get("bills_count", 0), 0),
            "reports_count": self.safe_int(cleaned.get("reports_count", 0), 0),
            "nominations_count": self.safe_int(cleaned.get("nominations_count", 0), 0),
            "updated_at": self.standardize_date(cleaned.get("updatedate")),
        }
        return filtered_cleaned

    async def _clean_committees_committeereports_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual committees reports records.

        FINAL COLUMNS:
        - committee_code TEXT PRIMARY KEY,
        - report_id TEXT PRIMARY KEY,
        - report_type TEXT,
        - report_number FLOAT,
        - congress INTEGER,
        - chamber TEXT,
        - citation TEXT,
        - updated_at TIMESTAMP WITH TIME ZONE

        STAGING COLUMNS:
        - url            text,
        - part           text,
        - type           text,
        - number         text,
        - chamber        text,
        - batch_id       text,
        - citation       text,
        - congress       text,
        - updatedate     text,
        - committee_code text,
        - id             text not null
        """
        cleaned = record_data.copy()

        if all([cleaned.get("type"), cleaned.get("number"), cleaned.get("congress")]):
            report_id = f"{cleaned.get('type').lower()}{cleaned.get('number')}-{cleaned.get('part', '1')}-{cleaned.get('congress')}"
        else:
            report_id = "ID_ERROR"

        filtered_cleaned = {
            "committee_code": cleaned.get("committee_code", "ID_ERROR"),
            "report_id": report_id,
            "report_type": cleaned.get("type").lower(),
            "report_number": self.safe_int(cleaned.get("number", 0)),
            "congress": self.safe_int(cleaned.get("congress", 0)),
            "chamber": self.standardize_chamber(cleaned.get("chamber")),
            "citation": cleaned.get("citation"),
            "updated_at": self.standardize_date(cleaned.get("updatedate")),
        }
        return filtered_cleaned

    async def _clean_committees_subcommittees_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual committees subcommittees records.

        FINAL COLUMNS:
        - committee_code TEXT PRIMARY KEY,
        - subcommittee_code TEXT PRIMARY KEY,

        STAGING COLUMNS:
        - url            text,
        - name           text,
        - systemcode     text,
        - committee_code text,
        - id             text
        """
        cleaned = record_data.copy()

        filtered_cleaned = {
            "committee_code": cleaned.get("committee_code", "ID_ERROR"),
            "subcommittee_code": cleaned.get("systemcode", "ID_ERROR"),
        }
        return filtered_cleaned

    async def _clean_committees_history_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual committees history records.

        FINAL COLUMNS:
        - committee_code TEXT,
        - name TEXT,
        - loc_name TEXT,
        - started_at TIMESTAMP WITH TIME ZONE,
        - ended_at TIMESTAMP WITH TIME ZONE,
        - committee_type TEXT,
        - establishing_authority TEXT,
        - su_doc_class_number TEXT,
        - nara_id TEXT,
        - loc_linked_data_id TEXT,
        - updated_at TIMESTAMP WITH TIME ZONE

        STAGING COLUMNS:
        - enddate                      text,
        - startdate                    text,
        - updatedate                   text,
        - libraryofcongressname        text,
        - committee_code               text,
        - id                           text
        - officialname                 text,
        - naraid                       text,
        - loclinkeddataid              text,
        - committeetypecode            text,
        - establishingauthority        text,
        - superintendentdocumentnumber text
        """
        cleaned = record_data.copy()

        filtered_cleaned = {
            "committee_code": cleaned.get("committee_code", "ID_ERROR"),
            "name": cleaned.get("officialname"),
            "loc_name": cleaned.get("libraryofcongressname"),
            "started_at": self.standardize_date(cleaned.get("startdate")),
            "ended_at": self.standardize_date(cleaned.get("enddate")),
            "committee_type": cleaned.get("committeetypecode"),
            "establishing_authority": cleaned.get("establishingauthority"),
            "su_doc_class_number": cleaned.get("superintendentdocumentnumber"),
            "nara_id": cleaned.get("naraid"),
            "loc_linked_data_id": cleaned.get("loclinkeddataid"),
            "updated_at": self.standardize_date(cleaned.get("updatedate")),
        }
        return filtered_cleaned

    async def _clean_committees_bills_singular(
        self, record_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Custom cleaning logic for individual committees bills relationship records.

        FINAL COLUMNS:
        - committee_code TEXT,
        - bill_id TEXT,
        - relationship_type TEXT,
        - committee_action_date TIMESTAMP WITH TIME ZONE,
        - updated_at TIMESTAMP WITH TIME ZONE

        STAGING COLUMNS:
        - url            text,
        - type           text,
        - number         text,
        - congress       text,
        - committee_code text,
        - id             text,
        - updatedate     text
        """
        cleaned = record_data.copy()

        # Generate bill_id from type, number, and congress
        if all([cleaned.get("type"), cleaned.get("number"), cleaned.get("congress")]):
            bill_id = f"{cleaned.get('type').lower()}{cleaned.get('number')}-{cleaned.get('congress')}"
        else:
            bill_id = "ID_ERROR"

        filtered_cleaned = {
            "committee_code": cleaned.get("committee_code", "ID_ERROR"),
            "bill_id": bill_id,
            "relationship_type": "committee_referral",  # Default relationship type
            "committee_action_date": self.standardize_date(cleaned.get("updatedate")),
            "updated_at": self.standardize_date(cleaned.get("updatedate")),
        }
        return filtered_cleaned

    async def _stream_committees_joined_chunks(
        self, chunk_size: int
    ) -> AsyncGenerator[list[dict[str, Any]], None]:
        """
        Stream committees data with name field joined from committees_list_raw table.

        This method joins the main committees staging table with the committees_list_raw
        table to include the 'name' field that's only available in the list data.
        """
        if not self.db_pool:
            logger.error("No database pool available for streaming")
            return

        async with self.db_pool.acquire() as conn:
            # Check if both tables exist
            committees_exists = await conn.fetchval(
                """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = $1 AND table_name = $2
                )
                """,
                self.staging_schema,
                "committees",
            )

            list_raw_exists = await conn.fetchval(
                """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = $1 AND table_name = $2
                )
                """,
                "bicam_raw_congressional",
                "committees_list_raw",
            )

            if not committees_exists:
                logger.warning(f"Table {self.staging_schema}.committees does not exist")
                return

            if not list_raw_exists:
                logger.warning(
                    "Table bicam_raw_congressional.committees_list_raw does not exist"
                )
                # Fallback to single table streaming
                async for chunk in self._stream_single_table_chunks(
                    "committees", chunk_size
                ):
                    yield chunk
                return

            logger.debug("Streaming committees with joined name data from list_raw")

            offset = 0
            while True:
                # Join committees staging with committees_list_raw to get name field
                query = f"""
                    SELECT
                        c.*,
                        (lr.payload->>'name')::text as name,
                        (lr.payload->>'chamber')::text as chamber
                    FROM {self.staging_schema}.committees c
                    LEFT JOIN bicam_raw_congressional.committees_list_raw lr
                        ON c.systemcode = lr.source_doc_id
                    LIMIT $1 OFFSET $2
                """

                try:
                    rows = await conn.fetch(query, chunk_size, offset)
                    if not rows:
                        break

                    chunk = [dict(row) for row in rows]
                    yield chunk

                    offset += len(chunk)

                    # If we got fewer rows than requested, we're at the end
                    if len(chunk) < chunk_size:
                        break

                except Exception as e:
                    logger.error(
                        f"Error streaming committees with joined data at offset {offset}: {e}"
                    )
                    break

    async def _stream_single_table_chunks(
        self, table_name: str, chunk_size: int
    ) -> AsyncGenerator[list[dict[str, Any]], None]:
        """Fallback method to stream from a single table when join fails."""
        async with self.db_pool.acquire() as conn:
            offset = 0
            while True:
                query = f"""
                    SELECT * FROM {self.staging_schema}.{table_name}
                    LIMIT $1 OFFSET $2
                """

                try:
                    rows = await conn.fetch(query, chunk_size, offset)
                    if not rows:
                        break

                    chunk = [dict(row) for row in rows]
                    yield chunk

                    offset += len(chunk)

                    if len(chunk) < chunk_size:
                        break

                except Exception as e:
                    logger.error(
                        f"Error streaming {table_name} at offset {offset}: {e}"
                    )
                    break
