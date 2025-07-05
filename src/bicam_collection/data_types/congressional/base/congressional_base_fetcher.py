"""
Congressional-specific base fetcher implementing the 3-phase pattern.

This module provides Congressional-specific implementations for:
- 3-phase processing pattern (list → full data → related data)
- Congressional API patterns and response structures
- Congressional schema defaults
- Source-specific configuration
"""

import asyncio
import json
import logging
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg

from ....libs.checkpoint import ProcessingPhase
from ....libs.run_tracking import RunType
from ...abstract.base_fetcher import BaseFetcher

logger = logging.getLogger(__name__)


class CongressionalBaseFetcher(BaseFetcher):
    """
    Congressional-specific base fetcher implementing the sophisticated 3-phase pattern.

    This class inherits all sophisticated processing logic from AbstractFetcher
    and implements Congressional-specific:
    - API response parsing
    - Schema configuration
    - Data extraction patterns
    - 3-phase processing workflow:
      * Phase 1: List data (initial collection)
      * Phase 2: Full data (complete item details)
      * Phase 3: Related data (using data-type-specific "get" methods)
    """

    # =============================================================================
    # REQUIRED ABSTRACT METHOD IMPLEMENTATIONS
    # =============================================================================

    def __init__(
        self,
        client,
        db_pool=None,
        data_type_name=None,
        checkpoint_manager=None,
        run_manager=None,
    ):
        super().__init__(
            client, db_pool, data_type_name, checkpoint_manager, run_manager
        )
        self.data_type_name = data_type_name
        # Load configuration to get proper id_field
        self.config = None
        if data_type_name:
            try:
                from ....libs.data_type_registry import get_global_registry

                registry = get_global_registry()
                self.config = registry.get_data_type_config(data_type_name)
                # Use the id_field from config, fallback to default pattern
                self.id_field = (
                    self.config.id_field if self.config else f"{data_type_name}_id"
                )
                logger.debug(
                    f"Loaded config for {data_type_name}, id_field: {self.id_field}"
                )
            except Exception as e:
                logger.warning(f"Could not load config for {data_type_name}: {e}")
                self.id_field = f"{data_type_name}_id" if data_type_name else "id"
        else:
            self.id_field = "id"

    def get_source_system_name(self) -> str:
        """Get the source system name for progress tracking."""
        return "congressional"

    def get_default_schema(self) -> str:
        """Get the default schema for raw data storage."""
        return "bicam_raw_congressional"

    def get_run_type(self) -> RunType:
        """Get the run type for tracking."""
        return RunType.SCRAPER_CONGRESSIONAL

    async def fetch_phase_1_data(
        self,
        from_date: str | None = None,
        to_date: str | None = None,
        limit: int | None = None,
        **kwargs,
    ) -> AsyncIterator[list[dict[str, Any]]]:
        """
        Phase 1: Fetch Congressional list data from API.

        Yields batches of items containing URLs for Phase 2 processing.
        """
        logger.info("Starting Congressional Phase 1 data fetch")

        try:
            # Get the proper API configuration from the registry
            if not self.config:
                logger.error("No configuration loaded for data type")
                return

            # Use the API configuration to get the correct endpoint and parameters
            api_config = self.config.api
            endpoint = api_config.api_endpoint or self.data_type_name

            # Format dates for API
            from_date_fmt = (
                self.client._format_date_for_api(from_date) if from_date else None
            )
            to_date_fmt = self.client._format_date_for_api(to_date) if to_date else None

            logger.debug(
                f"Using endpoint: {endpoint} for data type: {self.data_type_name}"
            )

            # Use the correct method name from CongressionalAPIClient
            async for batch in self.client.retrieve_data_list(
                data_type=endpoint,
                from_date=from_date_fmt,
                to_date=to_date_fmt,
                limit=limit,
                **kwargs,
            ):
                if batch:
                    # Extract the list of items using list_key from config
                    list_key = api_config.list_key
                    if list_key:
                        if isinstance(list_key, list):
                            # Try each key in the list until one works
                            extracted_items = []
                            for key in list_key:
                                if key in batch:
                                    extracted_items = batch[key]
                                    break
                            if not extracted_items:
                                logger.warning(
                                    f"None of the list keys {list_key} found in batch"
                                )
                                continue
                        else:
                            # Single key
                            extracted_items = batch.get(list_key, [])

                        if extracted_items:
                            yield extracted_items
                    else:
                        # Fallback: assume batch is already the list of items
                        yield batch

        except Exception as e:
            logger.error(f"Phase 1 fetch failed: {e}")
            raise

    async def fetch_phase_1_data_with_client(
        self,
        client,
        from_date: str | None = None,
        to_date: str | None = None,
        limit: int | None = None,
        **kwargs,
    ) -> AsyncIterator[list[dict[str, Any]]]:
        """
        Phase 1: Fetch Congressional list data using a specific client.
        """
        logger.debug(f"Phase 1 fetch with client: {client}")

        try:
            # Get the proper API configuration from the registry
            if not self.config:
                logger.error("No configuration loaded for data type")
                return

            # Use the API configuration to get the correct endpoint and parameters
            api_config = self.config.api
            endpoint = api_config.api_endpoint or self.data_type_name

            # Format dates for API
            from_date_fmt = (
                self.client._format_date_for_api(from_date) if from_date else None
            )
            to_date_fmt = self.client._format_date_for_api(to_date) if to_date else None

            logger.debug(
                f"Using endpoint: {endpoint} for data type: {self.data_type_name}"
            )

            # Use the correct method name from CongressionalAPIClient
            async for batch in client.retrieve_data_list(
                data_type=endpoint,
                from_date=from_date_fmt,
                to_date=to_date_fmt,
                limit=limit,
                **kwargs,
            ):
                if batch:
                    # Extract the list of items using list_key from config
                    list_key = api_config.list_key
                    if list_key:
                        if isinstance(list_key, list):
                            # Try each key in the list until one works
                            extracted_items = []
                            for key in list_key:
                                if key in batch:
                                    extracted_items = batch[key]
                                    break
                            if not extracted_items:
                                logger.warning(
                                    f"None of the list keys {list_key} found in batch"
                                )
                                continue
                        else:
                            # Single key
                            extracted_items = batch.get(list_key, [])

                        if extracted_items:
                            yield extracted_items
                    else:
                        # Fallback: assume batch is already the list of items
                        yield batch

        except Exception as e:
            logger.error(f"Phase 1 fetch with client failed: {e}")
            raise

    async def fetch_phase_2_data(self, item_url: str) -> dict[str, Any] | None:
        """
        Phase 2: Fetch complete Congressional item data from individual URL.
        """
        try:
            # Use the correct method name from CongressionalAPIClient
            full_data = await self.client.retrieve_full_data_from_url(item_url)

            if not full_data or not self.config:
                return full_data

            # Extract the full data item using full_key from config
            api_config = self.config.api
            full_key = api_config.full_key

            if full_key and full_key in full_data:
                return full_data[full_key]
            else:
                # Fallback: return the full response if no full_key or key not found
                return full_data

        except Exception as e:
            logger.error(f"Phase 2 fetch failed for {item_url}: {e}")
            return None

    async def fetch_phase_2_data_with_client(
        self, item_url: str, client
    ) -> dict[str, Any] | None:
        """
        Phase 2: Fetch complete Congressional item data using a specific client.
        """
        try:
            # Use the correct method name from CongressionalAPIClient
            full_data = await client.retrieve_full_data_from_url(item_url)

            if not full_data or not self.config:
                return full_data

            # Extract the full data item using full_key from config
            api_config = self.config.api
            full_key = api_config.full_key

            if full_key and full_key in full_data:
                return full_data[full_key]
            else:
                # Fallback: return the full response if no full_key or key not found
                return full_data

        except Exception as e:
            logger.error(f"Phase 2 fetch with client failed for {item_url}: {e}")
            return None

    async def fetch_phase_3_data(
        self, item_data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """
        Phase 3: Fetch Congressional related data using data-type-specific "get" methods.

        This uses the fetcher's specific "get" methods (like get_actions, get_amendments, etc.)
        to fetch related data for the Congressional item.
        """
        try:
            related_data = []

            # Get the item ID for related data fetching
            item_id = self.extract_item_id(item_data)

            # Check if this fetcher has specific "get" methods for related data
            # Common Congressional related data methods include:
            # - get_actions, get_amendments, get_committees, get_cosponsors, get_related_bills, etc.

            related_methods = [
                method
                for method in dir(self)
                if method.startswith("get_")
                and callable(getattr(self, method))
                and method
                not in ["get_source_system_name", "get_default_schema", "get_run_type"]
            ]

            if related_methods:
                logger.debug(
                    f"Found {len(related_methods)} related data methods: {related_methods}"
                )

                for method_name in related_methods:
                    try:
                        method = getattr(self, method_name)
                        # Call the method with the item data or ID
                        related_result = await method(item_data)

                        if related_result:
                            # Add metadata about the related data type
                            related_entry = {
                                "type": method_name,
                                "parent_id": item_id,
                                "data": related_result,
                                "method": method_name,
                            }
                            related_data.append(related_entry)

                    except Exception as e:
                        logger.error(
                            f"Failed to fetch related data using {method_name}: {e}"
                        )
                        continue

            return related_data

        except Exception as e:
            logger.error(f"Phase 3 fetch failed: {e}")
            return []

    async def fetch_phase_3_data_with_client(
        self, item_data: dict[str, Any], client
    ) -> list[dict[str, Any]]:
        """
        Phase 3: Fetch Congressional related data using a specific client.

        Note: The signature is different from the abstract method to match Congressional patterns.
        """
        try:
            related_data = []

            # Get the item ID for related data fetching
            item_id = self.extract_item_id(item_data)

            # Check if this fetcher has specific "get" methods for related data
            related_methods = [
                method
                for method in dir(self)
                if method.startswith("get_")
                and callable(getattr(self, method))
                and method
                not in ["get_source_system_name", "get_default_schema", "get_run_type"]
            ]

            if related_methods:
                logger.debug(
                    f"Found {len(related_methods)} related data methods: {related_methods}"
                )

                for method_name in related_methods:
                    try:
                        method = getattr(self, method_name)
                        # Call the method with the item data, using the specific client
                        # Some methods might accept a client parameter
                        try:
                            related_result = await method(item_data, client=client)
                        except TypeError:
                            # If method doesn't accept client parameter, call without it
                            related_result = await method(item_data)

                        if related_result:
                            # Add metadata about the related data type
                            related_entry = {
                                "type": method_name,
                                "parent_id": item_id,
                                "data": related_result,
                                "method": method_name,
                            }
                            related_data.append(related_entry)

                    except Exception as e:
                        logger.error(
                            f"Failed to fetch related data using {method_name}: {e}"
                        )
                        continue

            return related_data

        except Exception as e:
            logger.error(f"Phase 3 fetch with client failed: {e}")
            return []

    def extract_item_id(self, item_data: dict[str, Any]) -> str:
        """Extract standardized ID from Congressional item data."""
        return (
            item_data.get("number")
            or item_data.get("id")
            or item_data.get("congress", {}).get("number", "unknown")
        )

    def _extract_preliminary_item_id(self, list_item: dict[str, Any]) -> str:
        """Extract preliminary ID from Congressional list item data."""
        return (
            list_item.get("number")
            or list_item.get("id")
            or f"item_{hash(str(list_item))}"
        )

    async def store_phase_1_data(
        self, data: dict[str, Any], batch_id: str | None = None
    ) -> None:
        """Store Phase 1 Congressional list data with checkpointing."""
        if not self.db_pool:
            return

        # Extract preliminary item ID for checkpoint tracking
        preliminary_item_id = self._extract_preliminary_item_id(data)

        # Check if this item is already processed
        if (
            hasattr(self, "progress_tracker")
            and self.progress_tracker
            and self.progress_tracker.should_skip_item(
                preliminary_item_id, ProcessingPhase.PHASE_1, field_name="list_data"
            )
        ):
            logger.debug(
                f"Skipping already processed list data for {preliminary_item_id}"
            )
            return

        try:
            await self.store_raw_data(
                schema=self.get_default_schema(),
                table=f"{self.data_type_name}_list_raw",
                items=[data],
                batch_id=batch_id,
            )

            # Mark as processed in checkpoint
            if hasattr(self, "progress_tracker") and self.progress_tracker:
                self.progress_tracker.increment_processed(
                    preliminary_item_id,
                    ProcessingPhase.PHASE_1,
                    field_name="list_data",
                )

        except Exception as e:
            logger.error(f"Failed to store Phase 1 data: {e}")

    async def store_phase_2_data(
        self, data: dict[str, Any], batch_id: str | None = None
    ) -> None:
        """Store Phase 2 Congressional full data with checkpointing."""
        if not self.db_pool:
            return

        # Extract item ID for checkpoint tracking
        item_id = self.extract_item_id(data)

        # Check if this item is already processed
        if hasattr(self, "progress_tracker") and self.progress_tracker and self.progress_tracker.should_skip_item(
                item_id, ProcessingPhase.PHASE_2, field_name="full_data"
            ):
                logger.debug(f"Skipping already processed full data for {item_id}")
                return

        try:
            await self.store_raw_data(
                schema=self.get_default_schema(),
                table=f"{self.data_type_name}_raw",
                items=[data],
                batch_id=batch_id,
            )

            # Mark as processed in checkpoint
            if hasattr(self, "progress_tracker") and self.progress_tracker:
                self.progress_tracker.increment_processed(
                    item_id,
                    ProcessingPhase.PHASE_2,
                    field_name="full_data",
                )

        except Exception as e:
            logger.error(f"Failed to store Phase 2 data: {e}")

    async def store_phase_3_data(
        self, data: list[dict[str, Any]], parent_id: str, batch_id: str | None = None
    ) -> None:
        """Store Phase 3 Congressional related data in separate tables by data type with checkpointing."""
        if not self.db_pool or not data:
            return

        # Check if this item's related data is already processed
        if (
            hasattr(self, "progress_tracker")
            and self.progress_tracker
            and self.progress_tracker.should_skip_item(
                parent_id, ProcessingPhase.PHASE_3
            )
        ):
            logger.debug(f"Skipping already processed related data for {parent_id}")
            return

        try:
            # Group related data by type and store in separate tables
            for related_item in data:
                method_name = related_item.get("type", "unknown")
                related_data = related_item.get("data", [])

                if related_data:
                    # Extract the related table name from the method name
                    # Method names are like "get_bills_actions" -> we want "actions"
                    # or "get_actions" -> we want "actions"
                    if method_name.startswith("get_"):
                        # Remove "get_" prefix
                        related_table_name = method_name[4:]
                        # If it still has the data_type prefix (e.g., "bills_actions"), remove it
                        if related_table_name.startswith(f"{self.data_type_name}_"):
                            related_table_name = related_table_name[
                                len(f"{self.data_type_name}_") :
                            ]
                    else:
                        related_table_name = method_name

                    # Create table name: {main_data_type}_{related_table_name}_raw
                    # e.g., bills_actions_raw, bills_amendments_raw, etc.
                    table_name = f"{self.data_type_name}_{related_table_name}_raw"

                    # Add parent reference to each data item
                    for item in related_data:
                        if isinstance(item, dict):
                            item["parent_id"] = parent_id
                            item["data_type"] = related_table_name

                    # Store with parent's source_doc_id for related data
                    await self.store_raw_data(
                        schema=self.get_default_schema(),
                        table=table_name,
                        items=related_data,
                        batch_id=batch_id,
                        parent_source_doc_id=parent_id,  # Pass parent's source_doc_id
                    )

                    logger.debug(f"Stored {len(related_data)} items in {table_name}")

            # Mark as processed in checkpoint
            if hasattr(self, "progress_tracker") and self.progress_tracker:
                self.progress_tracker.increment_processed(
                    parent_id,
                    ProcessingPhase.PHASE_3,
                )

        except Exception as e:
            logger.error(f"Failed to store Phase 3 data: {e}")

    async def store_raw_data(
        self,
        schema: str = "bicam_raw_congressional",
        table: str = None,
        items: list[dict[str, Any]] = None,
        url_field: str = "url",
        batch_id: str | None = None,
        conn: asyncpg.Connection | None = None,
        parent_source_doc_id: str | None = None,
        **kwargs,
    ) -> None:
        """Store raw data with Congressional schema defaults."""
        if not all([table, items]):
            raise ValueError("table, items, and id_field are required")

        if conn:
            await self._execute_store_raw_data(
                conn, schema, table, items, url_field, batch_id, parent_source_doc_id
            )
        else:
            async with self.db_pool.acquire() as conn:
                await self._execute_store_raw_data(
                    conn,
                    schema,
                    table,
                    items,
                    url_field,
                    batch_id,
                    parent_source_doc_id,
                )

    async def _execute_store_raw_data(
        self,
        conn: asyncpg.Connection,
        schema: str,
        table: str,
        items: list[dict[str, Any]],
        url_field: str,
        batch_id: str | None,
        parent_source_doc_id: str | None = None,
    ) -> None:
        """Execute raw data storage with proper error handling."""
        if not items:
            return

        # Determine if we should attach run_id (only for _list_raw tables)
        include_run_id = (
            table.endswith("_list_raw")
            and hasattr(self, "current_run_id")
            and self.current_run_id is not None
        )

        # Prepare data for storage
        prepared_items = []
        for item in items:
            # Extract source document ID using the appropriate method based on table type
            if table.endswith("_list_raw"):
                # Phase 1 data: use preliminary ID extraction
                source_doc_id = self._extract_preliminary_item_id(item)
            elif parent_source_doc_id is not None:
                # Phase 3 related data: use parent's source_doc_id
                source_doc_id = parent_source_doc_id
            else:
                # Phase 2 data: use proper item ID extraction
                source_doc_id = self.extract_item_id(item)

            # Extract URL properly
            item_url = item.get(url_field)
            if not item_url and "url" in item:
                item_url = item["url"]

            # Generate ETL batch ID (use batch_id if provided, otherwise generate new UUID)
            etl_batch_id = batch_id if batch_id else str(uuid.uuid4())

            # Add metadata
            item_with_metadata = {
                "id_uuid": str(uuid.uuid4()),
                "url": item_url,
                "batch_id": batch_id or str(uuid.uuid4()),
                "scraped_at": datetime.now(UTC),
                "payload": self._safe_json_dumps(item),
                "source_doc_id": source_doc_id,
                "etl_batch_id": etl_batch_id,
            }
            if include_run_id:
                item_with_metadata["run_id"] = self.current_run_id
            prepared_items.append(item_with_metadata)

        # Insert data
        try:
            # Check if table exists first to avoid constraint violations
            table_exists = await conn.fetchval(
                """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = $1 AND table_name = $2
                );
            """,
                schema,
                table,
            )

            if not table_exists:
                # Create table if needed
                columns_ddl = """
                        id_uuid TEXT PRIMARY KEY,
                        url TEXT,
                        batch_id TEXT,
                        scraped_at TIMESTAMPTZ,
                        payload JSONB,
                        endpoint TEXT,
                        source_doc_id TEXT,
                        etl_batch_id TEXT
                """
                if include_run_id:
                    columns_ddl += ",\n                        run_id TEXT"

                await conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {schema}.{table} (
                        {columns_ddl}
                    );
                """)
                logger.debug(f"Created table {schema}.{table}")
            else:
                logger.debug(f"Table {schema}.{table} already exists")

            # Build dynamic insert statement
            base_cols = [
                "id_uuid",
                "url",
                "batch_id",
                "scraped_at",
                "payload",
                "source_doc_id",
                "etl_batch_id",
            ]
            if include_run_id:
                base_cols.append("run_id")
            cols_str = ", ".join(base_cols)
            placeholders = ", ".join([f"${i}" for i in range(1, len(base_cols) + 1)])

            on_conflict_set = ",\n                        ".join(
                f"{col} = EXCLUDED.{col}"
                for col in [
                    "scraped_at",
                    "payload",
                    "source_doc_id",
                    "etl_batch_id",
                ]
            )

            if include_run_id:
                on_conflict_set += ",\n                        run_id = EXCLUDED.run_id"

            for item in prepared_items:
                values = [item.get(col) for col in base_cols]
                await conn.execute(
                    f"""
                    INSERT INTO {schema}.{table} ({cols_str})
                    VALUES ({placeholders})
                    ON CONFLICT (id_uuid) DO UPDATE SET
                        {on_conflict_set}
                """,
                    *values,
                )

            logger.debug(f"Stored {len(prepared_items)} items in {schema}.{table}")

        except Exception as e:
            logger.error(f"Error storing data in {schema}.{table}: {e}")
            raise

    def _string_to_uuid(self, s: str) -> uuid.UUID:
        """Convert string to deterministic UUID."""
        # Use MD5 hash for deterministic UUID generation
        import hashlib

        hash_object = hashlib.md5(s.encode())
        return uuid.UUID(hash_object.hexdigest())

    def _safe_json_dumps(self, obj, **kwargs):
        """Safely dumps JSON with datetime and UUID handling"""

        class LocalDateTimeEncoder(json.JSONEncoder):
            def default(self, obj):
                if isinstance(obj, datetime):
                    return obj.isoformat()
                elif isinstance(obj, timedelta):
                    return obj.total_seconds()
                elif isinstance(obj, uuid.UUID):
                    return str(obj)
                return super().default(obj)

        kwargs.setdefault("cls", LocalDateTimeEncoder)
        kwargs.setdefault("ensure_ascii", False)
        return json.dumps(obj, **kwargs)

    # =============================================================================
    # CONGRESSIONAL-SPECIFIC OVERRIDES
    # =============================================================================

    def _extract_pagination_info(
        self, page_data: list[dict[str, Any]]
    ) -> dict[str, Any]:
        """
        Extract pagination info from Congressional API response.

        Congressional API typically includes pagination metadata in the response.
        """
        if not page_data or not isinstance(page_data, list):
            return {"total_count": 0, "count_per_page": 0}

        # Check if first item contains pagination metadata
        first_item = page_data[0] if page_data else {}

        # Congressional API pattern: check for pagination in metadata
        if "pagination" in first_item:
            pagination = first_item["pagination"]
            return {
                "total_count": pagination.get("count", len(page_data)),
                "count_per_page": pagination.get("per_page", len(page_data)),
            }

        # Fallback to batch size
        return {
            "total_count": len(page_data),
            "count_per_page": len(page_data),
        }

    def _extract_latest_date_from_batch(
        self, batch: list[dict[str, Any]]
    ) -> str | None:
        """
        Extract the latest date from a batch of Congressional items.

        Congressional data typically uses 'updateDate' or 'lastModifiedDate' fields.
        """
        dates = []
        for item in batch:
            date_str = (
                item.get("updateDate")
                or item.get("lastModifiedDate")
                or item.get("date")
            )
            if date_str:
                dates.append(date_str)

        if dates:
            return max(dates)
        return None

    async def _get_pagination_metadata(
        self,
        from_date: str | None,
        to_date: str | None,
        limit: int | None,
        **kwargs,
    ) -> tuple[int | None, dict[str, Any]]:
        """
        Get pagination metadata for optimal parallel distribution.

        Returns:
            Tuple of (pages_to_process, metadata_dict)
        """
        try:
            # Fetch first page to get pagination info
            async for _first_batch in self.fetch_phase_1_data(
                from_date=from_date,
                to_date=to_date,
                limit=limit or 250,
                **kwargs,
            ):
                if hasattr(self.client, "last_response_metadata"):
                    metadata = self.client.last_response_metadata
                    if (
                        metadata
                        and isinstance(metadata, dict)
                        and "pagination" in metadata
                    ):
                        pagination = metadata.get("pagination")
                        if (
                            pagination
                            and isinstance(pagination, dict)
                            and "count" in pagination
                        ):
                            total_count = pagination.get("count")
                            if total_count is not None and isinstance(total_count, int):
                                page_size = limit or 250
                                total_pages = (total_count + page_size - 1) // page_size

                                metadata_dict = {
                                    "total_count": total_count,
                                    "page_size": page_size,
                                    "total_pages": total_pages,
                                }

                                return total_pages, metadata_dict
                break

        except Exception as e:
            logger.warning(f"Could not get pagination metadata: {e}")

        return None, {}

    def _calculate_interleaved_offsets(
        self, num_sessions: int, total_pages: int, page_size: int, start_offset: int = 0
    ) -> list[list[int]]:
        """
        Calculate interleaved offset assignments for load balancing.

        Args:
            num_sessions: Number of parallel sessions
            total_pages: Number of pages to process
            page_size: Items per page
            start_offset: Starting offset
        """
        assignments = [[] for _ in range(num_sessions)]

        for page_num in range(total_pages):
            session_id = page_num % num_sessions
            # Calculate offset starting from the start position
            offset = start_offset + (page_num * page_size)
            assignments[session_id].append(offset)

        logger.debug(
            f"Calculated offsets: start_offset={start_offset}, "
            f"pages={total_pages}, assignments={[(i, len(a)) for i, a in enumerate(assignments)]}"
        )

        return assignments

    # =============================================================================
    # CONGRESSIONAL-SPECIFIC PARALLEL PROCESSING
    # =============================================================================

    async def _execute_redistribution_strategy(
        self,
        api_clients: list,
        distribution_strategy: dict,
        from_date: str | None,
        to_date: str | None,
        limit: int | None,
        batch_size: int,
        **kwargs,
    ) -> dict[str, Any]:
        """
        Execute redistribution strategy for Congressional 3-phase processing.

        Congressional redistribution strategy:
        1. Use all sessions for concurrent page processing (Phase 1 & 2)
        2. Each session processes its assigned pages completely (list + full + related)
        3. This maximizes API utilization and ensures all sessions are active

        This approach is more efficient than the abstract fetcher's redistribution
        because it keeps all sessions busy throughout the entire process.
        """
        logger.info("Executing Congressional redistribution strategy")

        # Setup progress tracker for checkpointing
        self.setup_progress_tracker()

        stats = {
            "total_processed": 0,
            "successful": 0,
            "errors": 0,
            "redistribution_used": True,
            "phase1_clients": len(api_clients),
            "phase2_clients": len(api_clients),
            "phase3_clients": len(api_clients),
            "phase2_processed": 0,
            "phase3_processed": 0,
        }

        try:
            # Get pagination metadata for optimal distribution
            pages_to_process, pagination_metadata = await self._get_pagination_metadata(
                from_date, to_date, limit, **kwargs
            )

            if pages_to_process is None or pages_to_process == 0:
                logger.info("No pages to process")
                return stats

            # Calculate offset assignments for all sessions
            page_size = pagination_metadata.get("page_size", limit or 250)
            start_offset = pagination_metadata.get("start_offset", 0)
            offset_assignments = self._calculate_interleaved_offsets(
                len(api_clients), pages_to_process, page_size, start_offset
            )

            logger.info(
                f"Using all {len(api_clients)} sessions for concurrent page processing"
            )

            # Each session processes its assigned pages completely (list + full + related)
            async def process_session_pages(client, assigned_offsets, session_id):
                session_stats = {
                    "total_processed": 0,
                    "successful": 0,
                    "errors": 0,
                    "phase2_processed": 0,
                    "phase3_processed": 0,
                    "session_id": session_id,
                }

                logger.info(
                    f"Session {session_id}: Processing {len(assigned_offsets)} pages"
                )

                for offset in assigned_offsets:
                    try:
                        # Get list data for this page
                        async for batch in self.fetch_phase_1_data_with_client(
                            client, from_date, to_date, limit, offset=offset, **kwargs
                        ):
                            if not batch:
                                continue

                            # Process each item in this page completely (list + full + related)
                            for list_item in batch:
                                try:
                                    # Phase 1: Store list data
                                    await self.store_phase_1_data(list_item)
                                    session_stats["total_processed"] += 1

                                    # Phase 2: Get and store full data
                                    url = list_item.get("url")
                                    if url:
                                        detailed_data = (
                                            await self.fetch_phase_2_data_with_client(
                                                url, client
                                            )
                                        )
                                        if detailed_data:
                                            await self.store_phase_2_data(detailed_data)
                                            session_stats["phase2_processed"] += 1

                                            # Phase 3: Get and store related data immediately
                                            related_data = await self.fetch_phase_3_data_with_client(
                                                detailed_data, client
                                            )
                                            if related_data:
                                                item_id = self.extract_item_id(
                                                    detailed_data
                                                )
                                                await self.store_phase_3_data(
                                                    related_data, item_id
                                                )
                                                session_stats["phase3_processed"] += 1

                                            session_stats["successful"] += 1

                                except Exception as e:
                                    logger.error(
                                        f"Session {session_id} error processing item: {e}"
                                    )
                                    session_stats["errors"] += 1

                    except Exception as e:
                        logger.error(
                            f"Session {session_id} error processing offset {offset}: {e}"
                        )
                        session_stats["errors"] += 1

                logger.info(f"Session {session_id} completed: {session_stats}")
                return session_stats

            # Process all sessions concurrently
            tasks = []
            for session_id, (client, assigned_offsets) in enumerate(
                zip(api_clients, offset_assignments, strict=False)
            ):
                task = process_session_pages(client, assigned_offsets, session_id)
                tasks.append(task)

            # Execute all sessions concurrently
            session_results = await asyncio.gather(*tasks, return_exceptions=True)

            # Aggregate results from all sessions
            for result in session_results:
                if isinstance(result, Exception):
                    logger.error(f"Session failed: {result}")
                    stats["errors"] += 1
                elif isinstance(result, dict):
                    stats["total_processed"] += result.get("total_processed", 0)
                    stats["successful"] += result.get("successful", 0)
                    stats["errors"] += result.get("errors", 0)
                    stats["phase2_processed"] += result.get("phase2_processed", 0)
                    stats["phase3_processed"] += result.get("phase3_processed", 0)

            logger.info(f"Congressional redistribution completed: {stats}")
            return stats

        except Exception as e:
            logger.error(f"Congressional redistribution failed: {e}")
            stats["error"] = str(e)
            return stats

    async def _execute_standard_parallel_processing(
        self,
        api_clients: list,
        from_date: str | None,
        to_date: str | None,
        limit: int | None,
        batch_size: int,
        max_concurrent: int,
        **kwargs,
    ) -> dict[str, Any]:
        """
        Execute standard parallel processing for Congressional 3-phase data.

        Process all phases for each batch to avoid memory issues with large datasets.
        """
        logger.info("Executing Congressional standard parallel processing")

        # Setup progress tracker for checkpointing
        self.setup_progress_tracker()

        stats = {
            "total_processed": 0,
            "successful": 0,
            "errors": 0,
            "clients_used": len(api_clients),
            "phase2_processed": 0,
            "phase3_processed": 0,
        }

        try:
            semaphore = asyncio.Semaphore(max_concurrent)

            async def process_detailed_item(item, client):
                async with semaphore:
                    try:
                        # Phase 2: Get detailed data
                        url = item.get("url")
                        if not url:
                            return {"phase2": 0, "phase3": 0}

                        detailed_data = await self.fetch_phase_2_data_with_client(
                            url, client
                        )
                        if not detailed_data:
                            return {"phase2": 0, "phase3": 0}

                        await self.store_phase_2_data(detailed_data)
                        phase2_success = 1

                        # Phase 3: Get related data using "get" methods
                        related_data = await self.fetch_phase_3_data_with_client(
                            detailed_data, client
                        )
                        if related_data:
                            item_id = self.extract_item_id(detailed_data)
                            await self.store_phase_3_data(related_data, item_id)
                            phase3_success = 1
                        else:
                            phase3_success = 0

                        return {"phase2": phase2_success, "phase3": phase3_success}

                    except Exception as e:
                        logger.error(f"Error processing detailed item: {e}")
                        return {"phase2": 0, "phase3": 0}

            # Process each client's batches immediately with all phases
            async def process_client_batches(client):
                client_stats = {"total": 0, "phase2": 0, "phase3": 0, "errors": 0}

                async for batch in self.fetch_phase_1_data_with_client(
                    client, from_date, to_date, limit, **kwargs
                ):
                    if not batch:
                        continue

                    # Store Phase 1 data for this batch
                    for item in batch:
                        await self.store_phase_1_data(item)
                        client_stats["total"] += 1

                    # Immediately process Phase 2 & 3 for this batch
                    logger.info(
                        f"Processing Phase 2 & 3 for batch of {len(batch)} items"
                    )

                    # Process items in this batch with controlled concurrency
                    batch_tasks = []
                    for item in batch:
                        batch_tasks.append(process_detailed_item(item, client))

                    batch_results = await asyncio.gather(
                        *batch_tasks, return_exceptions=True
                    )

                    # Count results for this batch
                    for result in batch_results:
                        if isinstance(result, dict):
                            client_stats["phase2"] += result.get("phase2", 0)
                            client_stats["phase3"] += result.get("phase3", 0)
                        else:
                            client_stats["errors"] += 1

                return client_stats

            # Process all clients concurrently
            client_tasks = [process_client_batches(client) for client in api_clients]
            client_results = await asyncio.gather(*client_tasks, return_exceptions=True)

            # Aggregate results from all clients
            for result in client_results:
                if isinstance(result, dict):
                    stats["total_processed"] += result.get("total", 0)
                    stats["phase2_processed"] += result.get("phase2", 0)
                    stats["phase3_processed"] += result.get("phase3", 0)
                    stats["errors"] += result.get("errors", 0)
                    stats["successful"] += result.get("phase2", 0) + result.get(
                        "phase3", 0
                    )
                else:
                    logger.error(f"Client processing failed: {result}")
                    stats["errors"] += 1

            logger.info(
                f"Congressional standard parallel processing completed: {stats}"
            )
            return stats

        except Exception as e:
            logger.error(f"Congressional standard parallel processing failed: {e}")
            stats["error"] = str(e)
            return stats

    def _calculate_distribution_strategy(
        self,
        api_clients: list,
        pages_to_process: int,
        redistribute_idle_sessions: bool,
    ) -> dict[str, Any]:
        """
        Calculate optimal distribution strategy for Congressional 3-phase processing.

        Congressional redistribution strategy:
        - Always use redistribution when enabled, regardless of page-to-client ratio
        - Use all clients for concurrent page processing (Phase 1 & 2)
        - Redistribute all clients for related data processing (Phase 3)
        - This maximizes API utilization across all phases
        """
        num_clients = len(api_clients)

        if redistribute_idle_sessions:
            # Congressional redistribution: Use all clients for all phases
            # This is more efficient than the abstract fetcher's approach
            strategy = {
                "type": "redistribution",
                "use_redistribution": True,
                "clients_for_phase1": num_clients,  # All clients for page processing
                "clients_for_redistribution": num_clients,  # All clients for related data
                "pages_per_client": max(1, pages_to_process // num_clients),
            }
        else:
            # Standard distribution (no redistribution)
            pages_per_client = max(1, pages_to_process // num_clients)
            strategy = {
                "type": "standard",
                "use_redistribution": False,
                "clients_for_phase1": num_clients,
                "clients_for_redistribution": 0,
                "pages_per_client": pages_per_client,
            }

        return strategy

    async def get_generic_related_data(
        self,
        full_data: dict[str, Any],
        related_table_name: str = None,
        field_name: str = None,
        list_key: str | list[str] = None,
    ) -> list[dict[str, Any]]:
        """
        Generic method to fetch related data from URLs in full_data.

        Args:
            full_data: Complete item data containing URL fields
            related_table_name: Name of the related table (e.g., "actions") - will be combined with data_type_name
            field_name: Field name containing the URL (e.g., "actions", "amendments") - overrides config
            list_key: Expected response key or list of keys - overrides config

        Returns:
            List of related data items
        """
        # Try to get configuration from the related table if specified
        related_config = None
        if related_table_name:
            try:
                from ....libs.data_type_registry import get_global_registry

                registry = get_global_registry()
                # Construct the full related table name: {data_type_name}_{related_table_name}
                full_related_table_name = f"{self.data_type_name}_{related_table_name}"
                related_config = registry.get_related_table_config(
                    self.data_type_name, full_related_table_name
                )
            except Exception as e:
                logger.debug(
                    f"Could not load config for related table {full_related_table_name}: {e}"
                )

        # Use provided parameters or fall back to config values
        if not field_name:
            if related_config and related_config.api.api_endpoint:
                field_name = related_config.api.api_endpoint
            else:
                field_name = (
                    self.config.api.api_endpoint if self.config else self.data_type_name
                )

        if not list_key:
            if related_config and related_config.api.list_key:
                list_key = related_config.api.list_key
            else:
                list_key = (
                    self.config.api.list_key if self.config else self.data_type_name
                )

        if field_name not in full_data:
            return []

        field_data = full_data[field_name]
        if not field_data or "url" not in field_data:
            return []

        url = field_data["url"]
        try:
            # Ensure list_key is passed as a list
            list_key_list = [list_key] if isinstance(list_key, str) else list_key
            return await self.client.retrieve_related_data_from_url(url, list_key_list)
        except Exception as e:
            logger.error(f"Error fetching {field_name} from {url}: {e}")
            return []
