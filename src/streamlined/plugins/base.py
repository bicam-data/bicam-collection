"""
Base Plugin Protocols and Classes

This module defines the interfaces that all plugins must implement and provides
base classes with shared functionality. These protocols ensure consistency across
different data sources while allowing for custom implementations.
"""

import html
import logging
import re
from datetime import UTC, datetime
from typing import Any, Protocol, runtime_checkable

import asyncpg
from dateutil.parser import parse as parse_date

logger = logging.getLogger(__name__)


@runtime_checkable
class FetcherPlugin(Protocol):
    """Protocol for fetcher plugins that handle data fetching logic."""

    async def fetch_list_data(
        self,
        api_client,
        from_date: str | None = None,
        to_date: str | None = None,
        limit: int = 250,
        offset: int = 0,
        **kwargs,
    ) -> list[dict[str, Any]]:
        """Fetch list of items from the API."""
        ...

    async def fetch_detailed_data(self, api_client, url: str) -> dict[str, Any] | None:
        """Fetch detailed data for a specific item."""
        ...

    async def fetch_related_data(
        self, api_client, detailed_data: dict[str, Any]
    ) -> list[dict[str, Any]] | None:
        """Fetch related data for an item (e.g., actions, cosponsors)."""
        ...

    def extract_item_id(self, data: dict[str, Any]) -> str:
        """Extract the unique identifier from item data."""
        ...


@runtime_checkable
class CleanerPlugin(Protocol):
    """Protocol for cleaner plugins that handle data cleaning logic."""

    async def clean_record(
        self, record_data: dict[str, Any], data_type: str
    ) -> dict[str, Any]:
        """Clean a single record for production use."""
        ...

    async def clean_table_data(
        self, table_data: list[dict[str, Any]], table_name: str
    ) -> list[dict[str, Any]]:
        """Clean data for a specific table."""
        ...


@runtime_checkable
class NormalizerPlugin(Protocol):
    """Protocol for normalizer plugins that handle data normalization logic."""

    async def normalize_jsonb_data(
        self, db_pool, schema: str, table: str
    ) -> dict[str, Any]:
        """Normalize JSONB data into structured tables."""
        ...

    async def extract_list_data(
        self, db_pool, schema: str, source_table: str
    ) -> dict[str, Any]:
        """Extract list data from normalized tables."""
        ...


# =============================================================================
# BASE IMPLEMENTATION CLASSES
# =============================================================================


class CongressionalBaseFetcherLogic:
    """
    Base class for Congressional fetcher logic that provides shared functionality.

    This class provides common methods like get_generic_related_data that can be
    inherited by all Congressional data type custom logic classes.
    """

    def __init__(self, data_type: str):
        self.data_type = data_type

    async def get_generic_related_data(
        self,
        full_data: dict[str, Any],
        related_table_name: str,
        client,
        list_key: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Fetch related data for a given table name using the client helper.

        This is a shared method that can be used by all Congressional data types
        to fetch related data in a standardized way.

        Args:
            full_data: The complete item payload returned by Phase-2.
            related_table_name: The field in *full_data* that contains the
                `{"url": "…"}` dict pointing to the related endpoint.
            client: API client instance for making requests.
            list_key: Optional explicit list key path to use when
                deserialising the API response. If None, a sensible default
                is derived from config or falls back to [related_table_name].

        Returns:
            A list of dictionaries representing the related entities, or an
            empty list on error / missing data.
        """
        try:
            # Debug: Log available fields in full_data
            logger.info(
                f"Looking for '{related_table_name}' field in {self.data_type} data"
            )
            logger.info(f"Available fields in full_data: {list(full_data.keys())}")

            # Basic validation of expected structure
            if related_table_name not in full_data:
                logger.warning(
                    f"Field '{related_table_name}' not found in {self.data_type} data"
                )
                return []

            table_info = full_data[related_table_name]
            logger.info(f"Found '{related_table_name}' field: {table_info}")

            if not isinstance(table_info, dict) or "url" not in table_info:
                logger.warning(
                    f"Invalid structure for '{related_table_name}' field: {table_info}"
                )
                return []

            url = table_info["url"]
            logger.info(f"Found URL for '{related_table_name}': {url}")

            # Derive list_key from config unless explicitly provided
            effective_list_key = None

            if list_key is not None:
                effective_list_key = list_key
            else:
                # Try to load related table config to determine correct list_key
                try:
                    from .consolidated_registry import get_consolidated_registry

                    registry = get_consolidated_registry()

                    # Related table configs are in the main data type config file
                    # as separate entries, not separate data types
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

            data = await client.retrieve_related_data_from_url(
                url, list_key=effective_list_key
            )

            logger.info(
                f"Retrieved {len(data) if data else 0} items for '{related_table_name}'"
            )

            # Ensure we always return a list
            return data or []

        except Exception as e:
            logger.error(
                f"Failed to fetch related data '{related_table_name}' for {self.data_type}: {e}"
            )
            return []

    async def get_related_data_pagination_metadata(
        self,
        full_data: dict[str, Any],
        related_table_name: str,
        client,
        list_key: list[str],
        page_size: int = 250,
    ) -> dict[str, Any] | None:
        """
        Generic method to get pagination metadata for any related data type.

        This method can be used for any related data type that needs parallel pagination.

        Args:
            full_data: The complete item data
            related_table_name: The field name in item data (e.g., "bills", "communications")
            client: API client instance
            list_key: The list key path to extract data from response
            page_size: Page size for pagination

        Returns:
            Dict with pagination info or None if not applicable
        """
        try:
            # Basic validation of expected structure
            if related_table_name not in full_data:
                return None

            table_info = full_data[related_table_name]
            if not isinstance(table_info, dict) or "url" not in table_info:
                return None

            url = table_info["url"]

            # Extract the endpoint from the full URL
            if not url.startswith(client.base_url):
                return None

            # Remove base URL and extract endpoint
            endpoint = url.replace(client.base_url, "")

            # Remove existing query parameters
            if "?" in endpoint:
                endpoint = endpoint.split("?")[0]

            # Fetch just the first page to get pagination metadata
            params = {"limit": 1, "offset": 0}

            response = await client._make_request(endpoint, params)

            # Extract pagination metadata
            pagination = response.get("pagination", {})
            total_count = pagination.get("count", 0)

            if total_count > page_size:  # Only parallelize if there's significant data
                return {
                    "endpoint": endpoint,
                    "total_count": total_count,
                    "page_size": page_size,
                    "total_pages": (total_count + page_size - 1) // page_size,
                    "list_key": list_key,
                    "item_id": self.extract_item_id(full_data),
                    "related_table_name": related_table_name,
                }

            return None

        except Exception as e:
            logger.error(
                f"Error getting pagination metadata for {related_table_name}: {e}"
            )
            return None

    async def get_related_data_page(
        self, pagination_metadata: dict[str, Any], page_number: int, client
    ) -> list[dict[str, Any]]:
        """
        Generic method to fetch a specific page of related data.

        This method can be used for any related data type that needs parallel pagination.

        Args:
            pagination_metadata: Metadata from get_related_data_pagination_metadata
            page_number: Which page to fetch (0-based)
            client: API client to use

        Returns:
            List of records for this page
        """
        try:
            endpoint = pagination_metadata["endpoint"]
            page_size = pagination_metadata["page_size"]
            list_key = pagination_metadata["list_key"]
            related_table_name = pagination_metadata.get(
                "related_table_name", "unknown"
            )

            # Calculate offset for this page
            offset = page_number * page_size

            # Build parameters for this page
            params = {
                "limit": page_size,
                "offset": offset,
            }

            logger.info(
                f"Fetching {related_table_name} page {page_number + 1} "
                f"(offset: {offset}) for {self.data_type} {pagination_metadata.get('item_id')}"
            )

            # Make the request
            response = await client._make_request(endpoint, params)

            # Extract data using the list_key
            data = response
            for key in list_key:
                if isinstance(data, dict) and key in data:
                    data = data.get(key)
                else:
                    data = None
                    break

            if data and isinstance(data, list):
                logger.info(
                    f"Page {page_number + 1}: Got {len(data)} items for {self.data_type} "
                    f"{pagination_metadata.get('item_id')} ({related_table_name})"
                )
                return data
            else:
                logger.warning(
                    f"Page {page_number + 1}: No data found for {self.data_type} "
                    f"{pagination_metadata.get('item_id')} ({related_table_name})"
                )
                return []

        except Exception as e:
            logger.error(
                f"Error fetching {related_table_name} page {page_number} for {self.data_type} "
                f"{pagination_metadata.get('item_id')}: {e}"
            )
            return []

    async def get_paginated_related_data(
        self,
        full_data: dict[str, Any],
        related_table_name: str,
        client,
        list_key: list[str] | None = None,
        page_size: int = 250,
    ) -> list[dict[str, Any]]:
        """
        Generic method to fetch all related data using pagination.

        This method orchestrates the pagination process by:
        1. Getting pagination metadata to determine if parallelization is needed
        2. If needed, fetching all pages sequentially (fallback for non-parallel contexts)
        3. If not needed, falling back to the standard get_generic_related_data method

        Args:
            full_data: The complete item data
            related_table_name: The field name in item data (e.g., "bills", "communications")
            client: API client instance
            list_key: The list key path to extract data from response
            page_size: Page size for pagination

        Returns:
            List of all related records
        """
        try:
            # Derive list_key if not provided
            if list_key is None:
                list_key = [related_table_name]

            # Get pagination metadata to see if we need parallel processing
            pagination_metadata = await self.get_related_data_pagination_metadata(
                full_data, related_table_name, client, list_key, page_size
            )

            if pagination_metadata is None:
                # No pagination needed, use standard method
                logger.info(
                    f"No pagination needed for {related_table_name}, using standard method"
                )
                return await self.get_generic_related_data(
                    full_data, related_table_name, client, list_key
                )

            # Pagination is needed - fetch all pages sequentially as fallback
            # (The optimized processor will handle parallel fetching when available)
            logger.info(
                f"Pagination needed for {related_table_name}: "
                f"{pagination_metadata['total_count']} total items, "
                f"{pagination_metadata['total_pages']} pages - fetching sequentially"
            )

            all_data = []
            total_pages = pagination_metadata["total_pages"]

            # Fetch all pages sequentially
            for page_number in range(total_pages):
                try:
                    page_data = await self.get_related_data_page(
                        pagination_metadata, page_number, client
                    )
                    if page_data:
                        all_data.extend(page_data)
                        logger.debug(
                            f"Page {page_number + 1}/{total_pages}: Got {len(page_data)} items"
                        )
                except Exception as e:
                    logger.error(
                        f"Error fetching page {page_number} for {related_table_name}: {e}"
                    )

            logger.info(
                f"Completed sequential pagination for {related_table_name}: "
                f"{len(all_data)} total items from {total_pages} pages"
            )

            return all_data

        except Exception as e:
            logger.error(
                f"Error in get_paginated_related_data for {related_table_name}: {e}"
            )
            # Fall back to standard method on error
            return await self.get_generic_related_data(
                full_data, related_table_name, client, list_key
            )
class BaseCleanerLogic:
    """
    Base cleaner logic for congressional data with table override registration.

    This class provides utility methods for data cleaning operations and includes
    the ability to register target table overrides for use in the streamlined cleaner.
    """

    def __init__(self):
        """Initialize the cleaner logic with table override support."""
        self._target_table_override: str | None = None

    # =============================================================================
    # TARGET TABLE OVERRIDE METHODS
    # =============================================================================

    def _register_target_table_override(self, target_table: str) -> None:
        """Register a target table override for the next processing operation."""
        self._target_table_override = target_table

    def _clear_target_table_override(self) -> None:
        """Clear the target table override."""
        self._target_table_override = None

    @property
    def target_table_override(self) -> str | None:
        """Get the current target table override."""
        return self._target_table_override

    # =============================================================================
    # STATIC UTILITY METHODS
    # =============================================================================

    @staticmethod
    def standardize_date(date_value: Any) -> datetime | None:
        """
        Standardize date values to consistent timezone-aware datetime objects.

        Args:
            date_value: Raw date value in various formats

        Returns:
            Standardized timezone-aware datetime object (UTC) or None if invalid
        """
        if not date_value or date_value in ["", "null", "None"]:
            return None

        if isinstance(date_value, datetime):
            # Ensure existing datetime objects are timezone-aware
            if date_value.tzinfo is None:
                # Assume timezone-naive datetime is in UTC
                return date_value.replace(tzinfo=UTC)
            return date_value

        if isinstance(date_value, str):
            try:
                parsed_date = parse_date(date_value)
                # Ensure parsed datetime is timezone-aware
                if parsed_date.tzinfo is None:
                    # Assume timezone-naive datetime is in UTC
                    return parsed_date.replace(tzinfo=UTC)
                return parsed_date
            except Exception:
                logger.warning(f"Could not parse date: {date_value}")
                return None

        return None

    @staticmethod
    def clean_long_text(text: str | None) -> str | None:
        """
        Clean and normalize long text fields, including HTML content.

        Args:
            text: Raw text to clean (may contain HTML)

        Returns:
            Cleaned text or None
        """
        if not text:
            return None

        # Store original for fallback
        original_text = text

        try:
            # First, handle HTML content if present
            if "<" in text and ">" in text:
                # Remove DOCTYPE declarations and XML namespaces
                cleaned = re.sub(r"<!DOCTYPE[^>]*>", "", text, flags=re.IGNORECASE)
                cleaned = re.sub(r"<\?xml[^>]*\?>", "", cleaned, flags=re.IGNORECASE)

                # Convert HTML paragraph and line break tags to newlines for structure preservation
                cleaned = re.sub(r"</p>", "\n\n", cleaned, flags=re.IGNORECASE)
                cleaned = re.sub(r"<br\s*/?>", "\n", cleaned, flags=re.IGNORECASE)
                cleaned = re.sub(r"</div>", "\n", cleaned, flags=re.IGNORECASE)
                cleaned = re.sub(r"</section>", "\n\n", cleaned, flags=re.IGNORECASE)

                # Remove all remaining HTML tags
                cleaned = re.sub(r"<[^>]+>", "", cleaned)

                # Decode HTML entities (like &nbsp;, &lt;, &gt;, etc.)
                cleaned = html.unescape(cleaned)
            else:
                cleaned = text

            # Remove common artifacts
            cleaned = cleaned.replace("\x00", "")  # Null bytes
            cleaned = cleaned.replace("\ufffd", "")  # Unicode replacement character
            cleaned = cleaned.replace("\u00ad", "")  # Soft hyphens

            # Normalize line endings
            cleaned = cleaned.replace("\r\n", "\n")
            cleaned = cleaned.replace("\r", "\n")

            # Clean up excessive whitespace while preserving paragraph structure
            # First normalize multiple newlines (preserve double newlines for paragraphs)
            cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)

            # Remove spaces at the beginning/end of lines
            cleaned = re.sub(r"[ \t]+\n", "\n", cleaned)
            cleaned = re.sub(r"\n[ \t]+", "\n", cleaned)

            # Replace multiple spaces/tabs with single space
            cleaned = re.sub(r"[ \t]{2,}", " ", cleaned)

            # Clean up any remaining excessive whitespace
            cleaned = cleaned.strip()

            # Remove common legislative document artifacts
            cleaned = re.sub(r"\[\[Page\s+[^\]]+\]\]", "", cleaned)  # Page markers
            cleaned = re.sub(
                r"¿+", "", cleaned
            )  # Strange characters sometimes in congressional docs

            # Final whitespace cleanup
            cleaned = re.sub(
                r"\n\s*\n\s*\n", "\n\n", cleaned
            )  # Normalize paragraph breaks

            return cleaned if cleaned else None

        except Exception as e:
            # If cleaning fails for any reason, fall back to basic cleaning
            logger.warning(f"Advanced text cleaning failed, using basic cleaning: {e}")
            basic_cleaned = re.sub(r"\s+", " ", original_text.strip())
            basic_cleaned = basic_cleaned.replace("\x00", "")
            return basic_cleaned if basic_cleaned else None

    @staticmethod
    def standardize_chamber(chamber: str | None) -> str | None:
        """
        Standardize chamber values to consistent format.

        Args:
            chamber: Raw chamber value

        Returns:
            Standardized chamber value
        """
        if not chamber:
            return None

        chamber_lower = chamber.lower().strip()

        if chamber_lower in ["house", "h", "house of representatives"]:
            return "house"
        elif chamber_lower in ["senate", "s"]:
            return "senate"
        elif chamber_lower in ["joint", "both"]:
            return "joint"
        elif chamber_lower in ["nochamber", "no chamber"]:
            return "nochamber"
        else:
            return chamber  # Return original if not recognized

    @staticmethod
    def safe_int(value: Any, default: int | None = None) -> int | float | None:
        """
        Safely convert value to integer.

        Args:
            value: Value to convert
            default: Default value if conversion fails

        Returns:
            Integer value or default
        """
        if value is None or value == "":
            return default

        try:
            if isinstance(value, str):
                # Remove common non-numeric characters
                cleaned = re.sub(r"[^\d-]", "", value)
                if cleaned:
                    return int(cleaned)
            else:
                return int(value)
        except (ValueError, TypeError):
            pass

        return default

    @staticmethod
    def safe_float(value: Any, default: float | None = None) -> float | None:
        """
        Safely convert value to float.

        Args:
            value: Value to convert
            default: Default value if conversion fails

        Returns:
            Float value or default
        """
        if value is None or value == "":
            return default

        try:
            if isinstance(value, str):
                # Remove common non-numeric characters except decimal point
                cleaned = re.sub(r"[^\d.-]", "", value)
                if cleaned:
                    return float(cleaned)
            else:
                return float(value)
        except (ValueError, TypeError):
            pass

        return default

    # =============================================================================
    # TABLE-SPLITTING HELPERS
    # =============================================================================

    async def split_columns_to_new_table(
        self,
        source_table: str,
        dest_table: str,
        columns: list[str] | dict[str, str],
        *,
        data_type: str | None = None,
        src_schema: str | None = None,
        dst_schema: str | None = None,
        create_if_missing: bool = False,
        drop_from_source: bool = False,
        batch_size: int = 1000,
        storage_manager=None,
        cleaner_storage=None,
    ) -> int:
        """
        Move columns from *source_table* → *dest_table* using optimized storage managers.

        This is a modernized version of split_columns_to_new_table that works with:
        - OptimizedStorageManager for efficient data transfer
        - OptimizedCleanerStorage for staging table operations
        - New configuration structure
        - Batch processing for large datasets

        Args:
            source_table: Source table name
            dest_table: Destination table name
            columns: Either a list of column names or dict mapping {source_col: dest_col}
            data_type: Data type for checkpoint tracking
            src_schema: Source schema (defaults to production schema if None)
            dst_schema: Destination schema (defaults to production schema if None)
            create_if_missing: Whether to create destination table if missing
            drop_from_source: Whether to drop columns from source after copy
            batch_size: Batch size for processing
            storage_manager: OptimizedStorageManager instance
            cleaner_storage: OptimizedCleanerStorage instance

        Returns:
            Number of records inserted
        """
        # Auto-detect schemas if not provided
        if src_schema is None:
            src_schema = getattr(self, "production_schema", "bicam_congressional")
        if dst_schema is None:
            dst_schema = getattr(self, "production_schema", "bicam_congressional")

        logger.info(
            f"Splitting columns from {src_schema}.{source_table} to {dst_schema}.{dest_table}"
        )

        # Normalize columns parameter
        if isinstance(columns, dict):
            src_cols = list(columns.keys())
            dest_cols = list(columns.values())
            col_pairs = list(columns.items())  # preserve order
        else:
            src_cols = columns
            dest_cols = columns
            col_pairs = [(c, c) for c in src_cols]

        # Get database connection from storage manager
        if not storage_manager or not cleaner_storage:
            raise ValueError(
                "Both storage_manager and cleaner_storage must be provided"
            )

        async with storage_manager.pg_pool.acquire() as conn:
            # Discover primary key(s) of the source table
            pk_cols = await conn.fetch(
                """
                SELECT a.attname
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = $1::regclass AND i.indisprimary
                ORDER BY a.attnum
                """,
                f"{src_schema}.{source_table}",
            )
            primary_keys = [r["attname"] for r in pk_cols] or []

            # Determine which of the requested source columns actually exist
            existing_cols_rows = await conn.fetch(
                """
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = $1 AND table_name = $2
                ORDER BY ordinal_position
                """,
                src_schema,
                source_table,
            )
            existing_src_cols = {r["column_name"] for r in existing_cols_rows}

            # Abort early if none of the requested columns exist in the source
            if not any(col in existing_src_cols for col in src_cols):
                logger.error(
                    f"None of the requested columns {src_cols} exist in {src_schema}.{source_table}; skipping split into {dest_table}"
                )
                return 0

            # Build column lists for INSERT
            dest_insert_cols = primary_keys + dest_cols

            # Create destination table if requested
            if create_if_missing:
                await self._ensure_destination_table(conn, dst_schema, dest_table)

            # Process data in batches using optimized storage
            total_inserted = await self._process_column_split_batches(
                conn=conn,
                src_schema=src_schema,
                source_table=source_table,
                dest_table=dest_table,
                src_cols=src_cols,
                col_pairs=col_pairs,
                existing_src_cols=existing_src_cols,
                primary_keys=primary_keys,
                dest_insert_cols=dest_insert_cols,
                batch_size=batch_size,
                data_type=data_type,
                storage_manager=storage_manager,
                cleaner_storage=cleaner_storage,
            )

            # Optionally drop the columns from the source table
            if drop_from_source and src_cols:
                await self._drop_source_columns(
                    conn, src_schema, source_table, src_cols
                )

            return total_inserted

    async def _ensure_destination_table(
        self, conn: asyncpg.Connection, dst_schema: str, dest_table: str
    ):
        """Ensure destination table exists with proper schema."""
        existing = await conn.fetchval(
            """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = $1 AND table_name = $2
            )""",
            dst_schema,
            dest_table,
        )

        if not existing:
            raise ValueError(
                f"Destination table {dst_schema}.{dest_table} does not exist"
            )

    async def _process_column_split_batches(
        self,
        conn: asyncpg.Connection,
        src_schema: str,
        source_table: str,
        dest_table: str,
        src_cols: list[str],
        col_pairs: list[tuple[str, str]],
        existing_src_cols: set[str],
        primary_keys: list[str],
        dest_insert_cols: list[str],
        batch_size: int,
        data_type: str | None,
        storage_manager,
        cleaner_storage,
    ) -> int:
        """Process column split in batches using optimized storage."""
        total_inserted = 0
        offset = 0

        # Build WHERE clause for non-null conditions
        # Use column names without table alias to avoid the PostgreSQL issue
        non_null_conds = [
            f"{col} IS NOT NULL" for col in src_cols if col in existing_src_cols
        ]
        where_clause = f" WHERE {' OR '.join(non_null_conds)}" if non_null_conds else ""

        # Build SELECT expressions
        select_exprs: list[str] = [f"{pk}" for pk in primary_keys]

        for src_col, dest_col in col_pairs:
            if src_col in existing_src_cols:
                select_exprs.append(f"{src_col}")
            else:
                logger.warning(
                    f"Column '{src_col}' not found in {src_schema}.{source_table}; using NULL for '{dest_col}'"
                )
                select_exprs.append(f"NULL AS {dest_col}")

        select_cols_sql = ", ".join(select_exprs)

        while True:
            # Process batch
            batch_query = f"""
                SELECT {select_cols_sql}
                FROM {src_schema}.{source_table}{where_clause}
                LIMIT {batch_size} OFFSET {offset}
            """

            try:
                # Wrap in explicit transaction to isolate any transaction state issues
                async with conn.transaction():
                    rows = await conn.fetch(batch_query)
                if not rows:
                    break

                batch_data = []
                for row in rows:
                    record = {}
                    # Add primary keys as-is (convert to string)
                    for pk in primary_keys:
                        value = row[pk]
                        record[pk] = value

                    # Add mapped columns with destination names (convert to string)
                    for src_col, dest_col in col_pairs:
                        if src_col in row:
                            value = row[src_col]
                            record[dest_col] = value

                    batch_data.append(record)
                # Use optimized cleaner storage for bulk insert to production tables
                inserted_count = await cleaner_storage.bulk_insert_production(
                    table_name=dest_table,
                    records=batch_data,
                    upsert=False,  # Simple insert for column splits
                )

                total_inserted += inserted_count
                offset += len(batch_data)

                logger.debug(
                    f"Processed batch: {len(batch_data)} records, total: {total_inserted}"
                )

                # Update checkpoint if data type is provided
                if data_type and total_inserted % 10000 == 0:
                    staging_checkpoint = storage_manager.get_staging_checkpoint(
                        data_type
                    )
                    checkpoint = staging_checkpoint.cm.get_or_create_checkpoint(
                        "cleaning",  # Use string literal instead of enum
                        "column_split",
                        data_type,
                    )
                    checkpoint.processed_items = total_inserted
                    checkpoint.current_table = dest_table
                    staging_checkpoint.cm.save_checkpoint(checkpoint)

                if len(batch_data) < batch_size:
                    break

            except Exception as e:
                logger.error(
                    f"Error processing batch at offset {offset}: {e}", exc_info=True
                )
                raise

        return total_inserted

    async def _drop_source_columns(
        self,
        conn: asyncpg.Connection,
        src_schema: str,
        source_table: str,
        src_cols: list[str],
    ):
        """Drop columns from source table with error handling."""
        for col in src_cols:
            try:
                # Use CASCADE so that dependent indexes or constraints do not block the drop
                await conn.execute(
                    f"ALTER TABLE {src_schema}.{source_table} "
                    f"DROP COLUMN IF EXISTS {col} CASCADE"
                )
                logger.info(f"Dropped column {col} from {src_schema}.{source_table}")
            except Exception as exc:
                logger.warning(
                    f"Failed to drop column {col} from {src_schema}.{source_table}: {exc}"
                )
