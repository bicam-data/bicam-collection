"""
Bills data fetcher using the 3-phase Congressional API approach.

This module handles:
1. Phase 1: Get bills list from bulk endpoints (/bill, /bill/119, etc.)
2. Phase 2: Get full bill data from individual URLs
3. Phase 3: Get related data (actions, cosponsors, texts, summaries) from URLs
"""

import asyncio
import logging
from collections.abc import AsyncIterator
from typing import Any

import asyncpg

from ....api_clients import CongressionalAPIClient
from ....libs.checkpoint import (
    HierarchicalProgressTracker,
    ProcessingPhase,
    ProcessingStage,
)
from ...base import BaseFetcher

logger = logging.getLogger(__name__)


class BillsFetcher(BaseFetcher):
    """
    Fetcher for Congressional bills data using the 3-phase API approach.
    """

    def __init__(
        self, client: CongressionalAPIClient, db_pool: asyncpg.Pool | None = None
    ):
        super().__init__(client, db_pool, data_type_name="bills")
        self.progress_tracker = None
        self._latest_update_date = (
            None  # Track latest update date for incremental fetching
        )

    def setup_progress_tracker(self, checkpoint_manager) -> HierarchicalProgressTracker:
        """Setup progress tracker for checkpointing."""
        # Use the proper stage-based checkpoint system for fetching (scraping stage)
        self.progress_tracker = HierarchicalProgressTracker.create_for_stage(
            checkpoint_manager, "congressional", "bills", ProcessingStage.SCRAPING
        )
        return self.progress_tracker

    def extract_item_id(self, item_data: dict[str, Any]) -> str:
        """Extract standardized bill ID from bill data."""
        bill_type = item_data.get("type", "").lower()
        number = item_data.get("number", "").replace("½", ".5")
        congress = item_data.get("congress", "")

        if all([bill_type, number, congress]):
            return f"{bill_type}{number}-{congress}"
        else:
            return "ID_ERROR"

    # Implementation of abstract methods
    async def fetch_list_data(
        self,
        from_date: str | None = None,
        to_date: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
        **kwargs,
    ) -> AsyncIterator[list[dict[str, Any]]]:
        """
        Phase 1: Get bills list data from bulk endpoints.

        Args:
            congress: Congress number (e.g., 119)
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            limit: Ignored - API limit is always set to 250 for maximum efficiency
            offset: Starting offset
            bill_type: Bill type ('hr', 's', 'hjres', 'sjres')

        Yields:
            Lists of bill list dictionaries with URLs for full data
        """
        # Always use maximum API limit (250) for efficiency
        async for batch in self.client.retrieve_data_list(
            data_type="bill",
            from_date=from_date,
            to_date=to_date,
            limit=250,  # Always maximize API efficiency
            offset=offset,
        ):
            yield batch

    async def fetch_full_data(self, item_url: str) -> dict[str, Any] | None:
        """
        Phase 2: Get full bill data from individual bill URL.

        Args:
            item_url: URL from bills list data

        Returns:
            Full bill data dictionary or None if error
        """
        full_data = await self.client.retrieve_full_data_from_url(
            item_url, expected_key="bill"
        )
        if full_data:
            bill_id = self.extract_item_id(full_data)
            full_data["bill_id"] = bill_id
        return full_data

    async def fetch_all_related_data(
        self, full_bill_data: dict[str, Any]
    ) -> dict[str, list[dict[str, Any]]]:
        """
        Get all related data for a bill based on schema configuration.
        Uses dynamic method calls with naming convention: get_{data_type}_{related_field}

        Args:
            full_bill_data: Full bill data from Phase 2

        Returns:
            Dictionary with all related data lists
        """
        related_data = {}

        # Debug logging
        logger.info(f"Data type name: {self.data_type_name}")

        # Check schema config
        schema_config = self.get_schema_config()
        logger.info(f"Schema config: {schema_config}")

        related_fields = self.get_related_field_names()
        logger.info(f"Related fields found: {related_fields}")

        if not related_fields:
            logger.warning(
                f"No related fields found in schema for {self.data_type_name}"
            )
            return related_data

        # Get all related data concurrently using dynamic method calls
        import asyncio

        tasks = {}
        for field in related_fields:
            # Dynamic method name: get_{data_type}_{related_field}
            method_name = f"get_{self.data_type_name}_{field}"
            logger.debug(f"Looking for method: {method_name}")

            if hasattr(self, method_name):
                method = getattr(self, method_name)
                if callable(method):
                    tasks[field] = method(full_bill_data)
                    logger.debug(f"Using method {method_name} for field {field}")
                else:
                    logger.warning(f"Method {method_name} is not callable")
            else:
                logger.warning(
                    f"Method {method_name} not found on {self.__class__.__name__}"
                )

        if not tasks:
            logger.warning("No related data tasks created")
            return related_data

        results = await asyncio.gather(*tasks.values(), return_exceptions=True)

        for _, (key, result) in enumerate(zip(tasks.keys(), results, strict=False)):
            if isinstance(result, Exception):
                logger.error(f"Error fetching {key}: {result}")
                related_data[key] = []
            else:
                related_data[key] = result or []

        return related_data

    # Phase 3: Related data from URLs
    async def get_generic_related_data(
        self,
        full_bill_data: dict[str, Any],
        field_name: str,
        expected_key: str | list[str],
    ) -> dict[str, list[dict[str, Any]]]:
        """
        Get all related data for a bill based on schema configuration.
        """
        if field_name not in full_bill_data:
            logger.warning(f"No {field_name} field in full bill data")
            return []

        field_info = full_bill_data[field_name]
        if not isinstance(field_info, dict) or "url" not in field_info:
            logger.warning(f"Invalid {field_name} info in full bill data")
            return []

        field_url = field_info["url"]
        return await self.client.retrieve_related_data_from_url(
            field_url,
            expected_key=[expected_key]
            if not isinstance(expected_key, list)
            else expected_key,
        )

    async def get_bills_actions(
        self, full_bill_data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """
        Phase 3: Get bill actions from actions URL in full bill data.

        Args:
            full_bill_data: Full bill data from Phase 2

        Returns:
            List of action dictionaries
        """
        return await self.get_generic_related_data(full_bill_data, "actions", "actions")

    async def get_bills_cosponsors(
        self, full_bill_data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """
        Phase 3: Get bill cosponsors from cosponsors URL in full bill data.

        Args:
            full_bill_data: Full bill data from Phase 2

        Returns:
            List of cosponsor dictionaries
        """
        return await self.get_generic_related_data(
            full_bill_data, "cosponsors", "cosponsors"
        )

    async def get_bills_texts(
        self, full_bill_data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """
        Phase 3: Get bill text versions from textVersions URL in full bill data.

        Args:
            full_bill_data: Full bill data from Phase 2

        Returns:
            List of text version dictionaries
        """
        return await self.get_generic_related_data(
            full_bill_data, "textVersions", "textVersions"
        )

    async def get_bills_summaries(
        self, full_bill_data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """
        Phase 3: Get bill summaries from summaries URL in full bill data.

        Args:
            full_bill_data: Full bill data from Phase 2

        Returns:
            List of summary dictionaries
        """
        return await self.get_generic_related_data(
            full_bill_data, "summaries", "summaries"
        )

    async def get_bills_subjects(
        self, full_bill_data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """
        Phase 3: Get bill subjects from subjects URL in full bill data.
        Note: Congressional API returns subjects in nested structure:
        {"subjects": {"legislativeSubjects": [...], "policyArea": {...}}}
        """
        return await self.get_generic_related_data(
            full_bill_data, "subjects", ["subjects", "legislativeSubjects"]
        )

    async def get_bills_titles(
        self, full_bill_data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """
        Phase 3: Get bill titles from titles URL in full bill data.
        """
        return await self.get_generic_related_data(full_bill_data, "titles", "titles")

    async def get_bills_relatedbills(
        self, full_bill_data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """
        Phase 3: Get bill related bills from relatedBills URL in full bill data.

        Related bills require special handling since each related bill can have
        multiple relationships, which need to be flattened into separate records.

        Args:
            full_bill_data: Full bill data from Phase 2

        Returns:
            List of related bill relationship dictionaries
        """
        if "relatedBills" not in full_bill_data:
            logger.warning("No relatedBills field in full bill data")
            return []

        related_bills_info = full_bill_data["relatedBills"]
        if not isinstance(related_bills_info, dict) or "url" not in related_bills_info:
            logger.warning("Invalid relatedBills info in full bill data")
            return []

        related_bills_url = related_bills_info["url"]

        # Get raw related bills data using the new client method
        raw_related_bills = await self.client.retrieve_related_data_from_url(
            related_bills_url, expected_key=["relatedBills"]
        )

        if not raw_related_bills:
            return []

        # Process the related bills data to extract relationships
        related_bills_relationships = []

        # Extract the bill_id from the full_bill_data
        bill_id = self.extract_item_id(full_bill_data)

        for related_bill in raw_related_bills:
            # Extract the related bill ID
            related_bill_type = related_bill.get("type", "").lower()
            related_bill_number = str(related_bill.get("number", "")).replace("½", ".5")
            related_bill_congress = str(related_bill.get("congress", ""))

            if all([related_bill_type, related_bill_number, related_bill_congress]):
                relatedbill_id = (
                    f"{related_bill_type}{related_bill_number}-{related_bill_congress}"
                )
            else:
                # Fallback to URL if we can't construct ID
                relatedbill_id = related_bill.get("url", "unknown")

            # Process each relationship for this related bill
            for relationship in related_bill.get("relationshipDetails", []):
                related_bills_relationships.append(
                    {
                        "bill_id": bill_id,
                        "relatedbill_id": relatedbill_id,
                        "relationship_identified_by": relationship.get("identifiedBy"),
                        "relationship_type": relationship.get("type"),
                    }
                )

        return related_bills_relationships

    async def get_bills_committeeactivities(
        self, full_bill_data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """
        Phase 3: Get bill committee activities from committees URL in full bill data.

        Args:
            full_bill_data: Full bill data from Phase 2

        Returns:
            List of committee activity dictionaries
        """
        if "committees" not in full_bill_data:
            logger.warning("No committees field in full bill data")
            return []

        committee_info = full_bill_data["committees"]
        if not isinstance(committee_info, dict) or "url" not in committee_info:
            logger.warning("Invalid committees info in full bill data")
            return []

        committee_url = committee_info["url"]

        # Get raw committee data using the new client method
        raw_committees = await self.client.retrieve_related_data_from_url(
            committee_url, expected_key=["committees"]
        )

        if not raw_committees:
            return []

        # Process the committee data to extract activities
        committee_activities = []

        for committee in raw_committees:
            # Process main committee activities
            for activity in committee.get("activities", []):
                committee_activities.append(
                    {
                        "name": committee.get("name"),
                        "committee_code": committee.get("systemCode"),
                        "chamber": committee.get("chamber", "").lower()
                        if committee.get("chamber")
                        else None,
                        "type": committee.get("type"),
                        "activity_name": activity.get("name"),
                        "activity_date": activity.get("date"),
                    }
                )

            # Process subcommittee activities
            for subcommittee in committee.get("subcommittees", []):
                for activity in subcommittee.get("activities", []):
                    committee_activities.append(
                        {
                            "name": subcommittee.get("name"),
                            "committee_code": subcommittee.get("systemCode"),
                            "chamber": subcommittee.get("chamber", "").lower()
                            if subcommittee.get("chamber")
                            else None,
                            "type": "Subcommittee",
                            "activity_name": activity.get("name"),
                            "activity_date": activity.get("date"),
                        }
                    )

        return committee_activities

    ### Raw data storage methods ###
    async def store_list_data(
        self, list_data: dict[str, Any], batch_id: str | None = None
    ) -> None:
        """
        if not list_data:
            return

        # Add batch metadata to each item
        enhanced_list = []
        for item in list_data:
            enhanced_item = item.copy()
        Args:
            list_data: List of bill dictionaries from Phase 1
            batch_id: Optional batch ID for grouping
        """
        await self.store_raw_data(
            schema="bicam_raw_congressional",
            table="bills_list_raw",
            items=[list_data],
            id_field="url",  # Use URL as the ID for list data
            url_field="url",
            batch_id=batch_id,
        )

    async def store_full_data(
        self, full_data: dict[str, Any], batch_id: str | None = None
    ) -> None:
        """
        Store Phase 2 raw full data to database.

        Args:
            full_data: Full bill data from Phase 2
            batch_id: Optional batch ID for grouping
        """
        if full_data:
            await self.store_raw_data(
                schema="bicam_raw_congressional",
                table="bills_raw",
                items=[full_data],
                id_field="bill_id",
                url_field="url",
                batch_id=batch_id,
            )

    async def store_related_data(
        self,
        related_data: dict[str, list[dict[str, Any]]],
        bill_id: str,
        batch_id: str | None = None,
    ) -> None:
        """
        Store Phase 3 raw related data to database.

        Args:
            related_data: Dictionary of related data lists
            bill_id: Bill ID to associate with the related data
            batch_id: Optional batch ID for grouping
        """
        for relation_type, data_list in related_data.items():
            if data_list:
                # Add bill_id to each item for linking
                for item in data_list:
                    if isinstance(item, dict):
                        item["bill_id"] = bill_id

                await self.store_raw_data(
                    schema="bicam_raw_congressional",
                    table=f"bills_{relation_type}_raw",
                    items=data_list,
                    id_field="bill_id",
                    url_field="url",
                    batch_id=batch_id,
                )

    async def store_complete_item_data(
        self,
        list_item: dict[str, Any],
        full_data: dict[str, Any],
        related_data: dict[str, list[dict[str, Any]]],
        batch_id: str | None = None,
    ) -> None:
        """
        Store all data for a single item in a single transaction.

        This method stores the full data and all related data for one item
        in the database as an atomic operation for better consistency.

        Args:
            list_item: Original list item data
            full_data: Full item data from Phase 2
            related_data: Related data from Phase 3
            batch_id: Optional batch ID for grouping
        """
        if not self.db_pool:
            return

        item_id = self.extract_item_id(full_data)
        logger.debug(f"Storing complete data for item {item_id}")

        # FIX 1: Add batch_id to all data payloads for indexing
        if batch_id:
            full_data["_batch_id"] = batch_id
            list_item["_batch_id"] = batch_id
            for _, data_list in related_data.items():
                for item in data_list:
                    if isinstance(item, dict):
                        item["_batch_id"] = batch_id

        await self.store_list_data(list_item, batch_id)

        # Store full data (main item) - list data already stored in batch
        await self.store_full_data(full_data, batch_id)

        # Store all related data
        await self.store_related_data(related_data, item_id, batch_id)

        logger.debug(f"Successfully stored complete data for item {item_id}")

    async def process_item(
        self,
        list_item: dict[str, Any],
        batch_id: str | None = None,
    ) -> bool | str:
        """
        Process a single item through all 3 phases with checkpointing.

        This method follows the proper 3-phase Congressional API approach:
        1. Phase 1: List item is already provided (contains URL for full data)
        2. Phase 2: Get full data using the URL from the list item
        3. Phase 3: Get all related data using the full data
        4. Store everything for this one item in database at once

        Args:
            list_item: Item from Phase 1 list data (contains URL for Phase 2)
            batch_id: Optional batch ID for grouping

        Returns:
            True if successfully processed, False if failed, "skipped" if already processed
        """
        if not self.progress_tracker:
            logger.error(
                "Progress tracker not initialized. Call setup_progress_tracker() first."
            )
            return False

        item_url = list_item.get("url")
        if not item_url:
            logger.error("No URL found in list item")
            return False

        # Extract item ID from URL segment between "/v3/" and "?"
        if "/v3/" in item_url:
            v3_part = item_url.split("/v3/")[1]
            item_id = v3_part.split("?")[0] if "?" in v3_part else v3_part
        else:
            item_id = item_url.split("/")[-1]  # Fallback to URL segment

        try:
            # Check if already processed
            if self.progress_tracker.should_skip_item(
                item_id, ProcessingPhase.MAIN_ITEMS
            ):
                logger.info(f"Skipping already processed item: {item_id}")
                return "skipped"

            logger.info(f"Processing item: {item_id}")

            # Phase 2: Get full data using URL from list item
            self.progress_tracker.set_processing_phase(
                ProcessingPhase.MAIN_ITEMS, current_field="full_data"
            )

            full_data = await self.fetch_full_data(item_url)
            if not full_data:
                error_msg = f"Failed to fetch full data for {item_id}"
                self.progress_tracker.increment_failed(item_id, error_msg)
                return False

            # Phase 3: Get all related data using the full data
            self.progress_tracker.set_processing_phase(
                ProcessingPhase.RELATED_ENTITIES, current_field="related_data"
            )

            related_data = await self.fetch_all_related_data(full_data)

            # Store everything for this one item in database at once
            logger.debug(f"Storing complete data for item {item_id}")
            await self.store_complete_item_data(
                list_item, full_data, related_data, batch_id
            )

            # Mark as processed
            self.progress_tracker.increment_processed(
                item_id, ProcessingPhase.MAIN_ITEMS
            )

            logger.info(f"Successfully processed item: {item_id}")
            return True

        except Exception as e:
            error_msg = f"Error processing item {item_id}: {str(e)}"
            logger.error(error_msg, exc_info=True)
            self.progress_tracker.increment_failed(item_id, error_msg, str(e))
            return False

    async def process_items(
        self,
        from_date: str | None = None,
        to_date: str | None = None,
        offset: int | None = None,
        batch_id: str | None = None,
        run_limit: int | None = None,  # Total processing limit (not API per-page limit)
        enable_parallelization: bool = False,  # Enable parallel session processing
        **kwargs,
    ) -> dict[str, int]:
        """
        Process entire list with item-level checkpointing following 3-phase approach.
        Now supports parallel processing with multiple API client sessions.

        This method follows the complete 3-phase Congressional API workflow:
        1. Phase 1: Get list data from bulk endpoints (API limit = 250 per page for efficiency)
        2. For each list item:
            - Phase 2: Get full data using URL from list item
            - Phase 3: Get all related data using full data
            - Store everything for that one item in database at once
        3. Continue with next item with granular checkpointing

        Args:
            from_date: Start date for incremental processing
            to_date: End date filter
            offset: Starting offset (for resuming)
            batch_id: Unique batch identifier
            run_limit: Total processing limit (None = process all until date cutoff)
            enable_parallelization: If True, use multiple parallel sessions with API key pairs

        Note:
            - API requests always use limit=250 per page for maximum efficiency
            - The 'run_limit' parameter controls total processing, not per-page API limit
            - For incremental processing, leave limit=None to process until last_processed_date
            - Parallelization divides work across multiple sessions with backup API keys

        Returns:
            Dictionary with processing statistics
        """
        if not self.progress_tracker:
            logger.error(
                "Progress tracker not initialized. Call setup_progress_tracker() first."
            )
            return {"processed": 0, "failed": 0, "skipped": 0}

        stats = {"processed": 0, "failed": 0, "skipped": 0}
        latest_update_date = None

        # Generate batch ID if not provided
        if not batch_id:
            from datetime import UTC, datetime

            batch_id = (
                f"{self.data_type_name}_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}"
            )

        try:
            self.progress_tracker.start_processing()

            pagination_params = {
                "from_date": from_date,
                "to_date": to_date,
                "offset": offset,
                "limit": run_limit,
                **kwargs,
            }

            # Only set limit if explicitly provided, otherwise get ALL data
            if run_limit is not None:
                pagination_params["limit"] = run_limit

            logger.info(f"Starting pagination with params: {pagination_params}")

            if enable_parallelization:
                # Use parallel processing
                stats = await self._process_with_parallel_sessions(
                    pagination_params, batch_id, from_date
                )
            else:
                # Use traditional single-client processing
                stats = await self._process_with_single_client(
                    pagination_params, batch_id, from_date
                )

            # Update the last_processed_date in the checkpoint system if we processed items
            if stats["processed"] > 0 and latest_update_date:
                self.progress_tracker.update_progress(
                    last_processed_date=latest_update_date
                )
                logger.info(
                    f"Updated checkpoint last_processed_date to: {latest_update_date}"
                )

            # If we have access to the client pool, also update the Congressional API tracking table
            if (
                hasattr(self, "client")
                and hasattr(self.client, "db_pool")
                and self.client.db_pool
                and stats["processed"] > 0
            ):
                # Use the latest update date from processed items, or fallback to current time
                if latest_update_date:
                    update_date_to_use = latest_update_date
                else:
                    from datetime import UTC, datetime

                    update_date_to_use = datetime.now(UTC).strftime(
                        "%Y-%m-%dT%H:%M:%SZ"
                    )

                success = await self.client.update_last_processed_date(
                    self.data_type_name, update_date_to_use, stats.get("total_count")
                )
                if success:
                    logger.info(
                        f"Updated Congressional API last_processed_date for {self.data_type_name} to: {update_date_to_use}"
                    )
                else:
                    logger.warning(
                        f"Failed to update Congressional API last_processed_date for {self.data_type_name}"
                    )

            self.progress_tracker.complete_processing()
            logger.info(f"Processing completed. Final stats: {stats}")

        except Exception as e:
            error_msg = f"Fatal error in processing: {str(e)}"
            logger.error(error_msg, exc_info=True)
            self.progress_tracker.fail_processing(error_msg)

        return stats

    async def _process_with_parallel_sessions(
        self, pagination_params: dict, batch_id: str, from_date: str | None
    ) -> dict[str, int]:
        """Process data using true parallel sessions with concurrent page fetching."""
        logger.info(
            "Starting parallel session processing with concurrent page fetching"
        )

        # We need access to the processing resource to get parallel sessions
        if not hasattr(self, "processing_resource"):
            logger.warning(
                "No processing resource available for parallel sessions - falling back to single client"
            )
            return await self._process_with_single_client(
                pagination_params, batch_id, from_date
            )

        try:
            # Get parallel API clients for this data type
            api_clients = (
                await self.processing_resource.get_api_clients_for_parallel_sessions(
                    self.data_type_name
                )
            )

            if len(api_clients) <= 1:
                logger.info(
                    f"Only {len(api_clients)} API client(s) available - using single client processing"
                )
                return await self._process_with_single_client(
                    pagination_params, batch_id, from_date
                )

            logger.info(
                f"Using {len(api_clients)} parallel sessions for concurrent processing"
            )

            # Strategy: Concurrent page fetching with parallel item processing
            return await self._process_with_concurrent_page_fetching(
                api_clients, pagination_params, batch_id, from_date
            )

        except Exception as e:
            logger.error(f"Error in parallel session processing: {e}", exc_info=True)
            logger.info("Falling back to single client processing")
            return await self._process_with_single_client(
                pagination_params, batch_id, from_date
            )

    async def _process_with_concurrent_page_fetching(
        self,
        api_clients: list,
        pagination_params: dict,
        batch_id: str,
        from_date: str | None,
    ) -> dict[str, int]:
        """
        Process data with intelligent concurrent page fetching using real pagination metadata.

        Strategy:
        1. Fetch first page to get actual total count from pagination metadata
        2. Calculate optimal offset distribution based on real count
        3. Use interleaved offset assignment to distribute work evenly
        4. Handle count changes during processing
        5. Store metadata for future runs
        """
        num_sessions = len(api_clients)
        page_size = 250  # API limit per page

        logger.info(
            f"Starting intelligent concurrent page fetching with {num_sessions} sessions"
        )

        # Step 1: Get actual count from first page using primary session
        primary_client = api_clients[0]
        original_client = self.client
        self.client = primary_client

        try:
            total_count, last_processed_count = await self._get_pagination_metadata(
                pagination_params
            )

            if total_count is None:
                logger.warning(
                    "Could not determine total count, falling back to sequential processing"
                )
                return await self._process_with_single_client(
                    pagination_params, batch_id, from_date
                )

            logger.info(f"Total items available: {total_count}")
            if last_processed_count is not None:
                logger.info(
                    f"Estimated new items since last run: {total_count - last_processed_count}"
                )

            # Step 2: Calculate optimal distribution
            total_pages = (total_count + page_size - 1) // page_size  # Ceiling division
            logger.info(f"Total pages to process: {total_pages}")

            if total_pages <= num_sessions:
                # Few pages - each session gets one page
                logger.info("Few pages available, using simple distribution")
                return await self._process_with_simple_distribution(
                    api_clients, pagination_params, batch_id, from_date, total_pages
                )

            # Step 3: Create interleaved offset assignments
            offset_assignments = self._calculate_interleaved_offsets(
                num_sessions, total_pages, page_size
            )

            logger.info(
                f"Offset assignments: {[(i, assignments[:3]) for i, assignments in enumerate(offset_assignments)]}"
            )

        finally:
            self.client = original_client

        # Step 4: Start concurrent processing with calculated offsets
        session_tasks = []
        for i, client in enumerate(api_clients):
            assigned_offsets = offset_assignments[i]
            if assigned_offsets:
                task = asyncio.create_task(
                    self._process_assigned_offsets(
                        client,
                        pagination_params,
                        assigned_offsets,
                        batch_id,
                        from_date,
                        i,
                        total_count,
                    )
                )
                session_tasks.append(task)

        # Wait for all sessions to complete
        logger.info(
            f"Starting {len(session_tasks)} concurrent sessions with interleaved offsets"
        )
        session_results = await asyncio.gather(*session_tasks, return_exceptions=True)

        # Step 5: Aggregate results and handle count changes
        total_stats = {"processed": 0, "failed": 0, "skipped": 0}
        max_observed_count = total_count

        for i, result in enumerate(session_results):
            if isinstance(result, Exception):
                logger.error(f"Session {i} failed with exception: {result}")
                total_stats["failed"] += 1
            elif isinstance(result, dict):
                logger.info(f"Session {i} completed: {result}")
                for key in ["processed", "failed", "skipped"]:
                    total_stats[key] += result.get(key, 0)

                # Track count changes
                if (
                    "observed_count" in result
                    and result["observed_count"] > max_observed_count
                ):
                    max_observed_count = result["observed_count"]
            else:
                logger.warning(f"Session {i} returned unexpected result: {result}")

        # Step 6: Handle any new items that appeared during processing
        if max_observed_count > total_count:
            logger.info(
                f"Count increased during processing: {total_count} → {max_observed_count}"
            )

            # Calculate additional items that need processing
            additional_items = max_observed_count - total_count
            logger.info(
                f"Processing {additional_items} additional items that appeared during execution"
            )

            # Calculate which pages contain the new items
            original_pages = (total_count + page_size - 1) // page_size
            new_total_pages = (max_observed_count + page_size - 1) // page_size
            additional_pages = new_total_pages - original_pages

            if additional_pages > 0:
                logger.info(
                    f"Processing {additional_pages} additional pages (pages {original_pages} to {new_total_pages - 1})"
                )

                # Create offset assignments for the additional pages
                additional_offsets = []
                for page_num in range(original_pages, new_total_pages):
                    additional_offsets.append(page_num * page_size)

                # Distribute additional pages across available sessions
                if len(api_clients) > 1 and len(additional_offsets) > 1:
                    # Use parallel processing for additional pages
                    additional_offset_assignments = [
                        [] for _ in range(len(api_clients))
                    ]
                    for i, offset in enumerate(additional_offsets):
                        session_id = i % len(api_clients)
                        additional_offset_assignments[session_id].append(offset)

                    # Process additional pages in parallel
                    additional_tasks = []
                    for i, client in enumerate(api_clients):
                        if additional_offset_assignments[i]:
                            task = asyncio.create_task(
                                self._process_assigned_offsets(
                                    client,
                                    pagination_params,
                                    additional_offset_assignments[i],
                                    f"{batch_id}_additional",
                                    from_date,
                                    i,
                                    max_observed_count,
                                )
                            )
                            additional_tasks.append(task)

                    if additional_tasks:
                        logger.info(
                            f"Starting {len(additional_tasks)} sessions to process additional items"
                        )
                        additional_results = await asyncio.gather(
                            *additional_tasks, return_exceptions=True
                        )

                        # Aggregate additional results
                        for i, result in enumerate(additional_results):
                            if isinstance(result, Exception):
                                logger.error(
                                    f"Additional processing session {i} failed: {result}"
                                )
                                total_stats["failed"] += 1
                            elif isinstance(result, dict):
                                logger.info(
                                    f"Additional session {i} completed: {result}"
                                )
                                for key in ["processed", "failed", "skipped"]:
                                    total_stats[key] += result.get(key, 0)

                                # Update max observed count if it increased further
                                if (
                                    "observed_count" in result
                                    and result["observed_count"] > max_observed_count
                                ):
                                    max_observed_count = result["observed_count"]
                else:
                    # Use single session for few additional pages
                    logger.info("Processing additional pages with single session")
                    primary_client = api_clients[0]
                    original_client = self.client
                    self.client = primary_client

                    try:
                        additional_result = await self._process_assigned_offsets(
                            primary_client,
                            pagination_params,
                            additional_offsets,
                            f"{batch_id}_additional",
                            from_date,
                            0,
                            max_observed_count,
                        )

                        if isinstance(additional_result, dict):
                            logger.info(
                                f"Additional processing completed: {additional_result}"
                            )
                            for key in ["processed", "failed", "skipped"]:
                                total_stats[key] += additional_result.get(key, 0)

                            # Update max observed count if it increased further
                            if (
                                "observed_count" in additional_result
                                and additional_result["observed_count"]
                                > max_observed_count
                            ):
                                max_observed_count = additional_result["observed_count"]
                    except Exception as e:
                        logger.error(f"Error processing additional items: {e}")
                        total_stats["failed"] += additional_items
                    finally:
                        self.client = original_client

                logger.info(
                    f"Completed processing additional items. Updated stats: {total_stats}"
                )
            else:
                logger.info(
                    "New items are within existing pages, likely already processed"
                )

            total_stats["count_changed"] = True
            total_stats["final_count"] = max_observed_count

        # Step 7: Include total count for metadata update
        total_stats["total_count"] = max_observed_count

        logger.info(
            f"Intelligent concurrent processing completed. Final stats: {total_stats}"
        )
        return total_stats

    async def _process_item_with_semaphore(
        self, semaphore: asyncio.Semaphore, item: dict, session_id: int
    ):
        """Process a single item with semaphore control and concurrent related data fetching."""
        async with semaphore:
            return await self._process_item_with_concurrent_related_data(
                item, session_id
            )

    async def _process_item_with_concurrent_related_data(
        self, item: dict, session_id: int
    ):
        """
        Process a single item with concurrent related data fetching.

        Strategy:
        1. Fetch full data
        2. Fetch all related data types concurrently using asyncio.gather
        3. Store everything atomically
        """
        try:
            item_url = item.get("url")
            if not item_url:
                logger.error(f"Session {session_id}: No URL found in item")
                return False

            # Extract item ID for checkpointing
            if "/v3/" in item_url:
                v3_part = item_url.split("/v3/")[1]
                item_id = v3_part.split("?")[0] if "?" in v3_part else v3_part
            else:
                item_id = item_url.split("/")[-1]

            # Check if already processed
            if self.progress_tracker and self.progress_tracker.should_skip_item(
                item_id, ProcessingPhase.MAIN_ITEMS
            ):
                return "skipped"

            # Phase 2: Get full data
            full_data = await self.fetch_full_data(item_url)
            if not full_data:
                if self.progress_tracker:
                    self.progress_tracker.increment_failed(
                        item_id, "Failed to fetch full data"
                    )
                return False

            # Phase 3: Get all related data concurrently
            related_data = await self._fetch_related_data_concurrently(full_data)

            # Store everything atomically
            await self.store_complete_item_data(
                item, full_data, related_data, item.get("_batch_id")
            )

            # Mark as processed
            if self.progress_tracker:
                self.progress_tracker.increment_processed(
                    item_id, ProcessingPhase.MAIN_ITEMS
                )

            return True

        except Exception as e:
            logger.error(f"Session {session_id}: Error processing item: {e}")
            return False

    async def _fetch_related_data_concurrently(
        self, full_data: dict
    ) -> dict[str, list[dict[str, Any]]]:
        """
        Fetch all related data types concurrently instead of sequentially.

        This is where we get the real speed improvement - instead of fetching
        actions, then cosponsors, then texts, etc. sequentially, we fetch them all at once.
        """
        related_fields = self.get_related_field_names()
        if not related_fields:
            return {}

        # Create concurrent tasks for all related data types
        tasks = {}
        for field in related_fields:
            method_name = f"get_{self.data_type_name}_{field}"
            if hasattr(self, method_name):
                method = getattr(self, method_name)
                if callable(method):
                    tasks[field] = method(full_data)

        if not tasks:
            return {}

        # Fetch all related data concurrently
        logger.debug(
            f"Fetching {len(tasks)} related data types concurrently: {list(tasks.keys())}"
        )
        results = await asyncio.gather(*tasks.values(), return_exceptions=True)

        # Process results
        related_data = {}
        for field, result in zip(tasks.keys(), results, strict=False):
            if isinstance(result, Exception):
                logger.error(f"Error fetching {field}: {result}")
                related_data[field] = []
            else:
                related_data[field] = result or []

        return related_data

    async def _get_pagination_metadata(
        self, pagination_params: dict
    ) -> tuple[int | None, int | None]:
        """
        Get total count from first page and last processed count from metadata table.

        Returns:
            (total_count, last_processed_count) tuple
        """
        try:
            # Fetch first page to get pagination metadata
            first_page_params = {**pagination_params, "offset": 0, "limit": 250}

            async for batch in self.fetch_list_data(**first_page_params):
                if batch:
                    # Look for pagination metadata in the response
                    # Congressional API typically includes pagination info
                    first_item = batch[0] if batch else None
                    if first_item and hasattr(self.client, "last_response_metadata"):
                        pagination_info = getattr(
                            self.client, "last_response_metadata", {}
                        ).get("pagination", {})
                        total_count = pagination_info.get("count")

                        if total_count is not None:
                            logger.info(
                                f"Found total count in pagination metadata: {total_count}"
                            )
                            # Get last processed count from metadata table
                            last_processed_count = (
                                await self._get_last_processed_count()
                            )
                            return total_count, last_processed_count

                    # Fallback: if no pagination metadata, estimate from batch size
                    logger.warning(
                        "No pagination metadata found, cannot determine total count"
                    )
                    return None, None
                break

            return None, None

        except Exception as e:
            logger.error(f"Error getting pagination metadata: {e}")
            return None, None

    async def _get_last_processed_count(self) -> int | None:
        """Get the last processed count from the existing Congressional API infrastructure."""
        if not self.client:
            return None

        try:
            return await self.client.access_last_processed_count(self.data_type_name)
        except Exception as e:
            logger.warning(f"Could not get last processed count: {e}")
            return None

    def _calculate_interleaved_offsets(
        self, num_sessions: int, total_pages: int, page_size: int
    ) -> list[list[int]]:
        """
        Calculate interleaved offset assignments for optimal distribution.

        Example with 4 sessions and 20 pages:
        Session 0: [0, 1000, 2000, 3000, 4000]     (pages 0, 4, 8, 12, 16)
        Session 1: [250, 1250, 2250, 3250]        (pages 1, 5, 9, 13, 17)
        Session 2: [500, 1500, 2500, 3500]        (pages 2, 6, 10, 14, 18)
        Session 3: [750, 1750, 2750, 3750]        (pages 3, 7, 11, 15, 19)
        """
        assignments = [[] for _ in range(num_sessions)]

        for page_num in range(total_pages):
            session_id = page_num % num_sessions
            offset = page_num * page_size
            assignments[session_id].append(offset)

        # Log the distribution
        for i, offsets in enumerate(assignments):
            logger.debug(
                f"Session {i}: {len(offsets)} pages, offsets {offsets[:5]}{'...' if len(offsets) > 5 else ''}"
            )

        return assignments

    async def _process_with_simple_distribution(
        self,
        api_clients: list,
        pagination_params: dict,
        batch_id: str,
        from_date: str | None,
        total_pages: int,
    ) -> dict[str, int]:
        """Handle simple case where we have fewer pages than sessions."""
        import asyncio

        logger.info(f"Processing {total_pages} pages with {len(api_clients)} sessions")

        session_tasks = []
        for i in range(min(total_pages, len(api_clients))):
            client = api_clients[i]
            offset = i * 250  # One page per session

            task = asyncio.create_task(
                self._process_assigned_offsets(
                    client, pagination_params, [offset], batch_id, from_date, i, 0
                )
            )
            session_tasks.append(task)

        session_results = await asyncio.gather(*session_tasks, return_exceptions=True)

        # Aggregate results
        total_stats = {"processed": 0, "failed": 0, "skipped": 0}
        max_observed_count = 0

        for i, result in enumerate(session_results):
            if isinstance(result, Exception):
                logger.error(f"Session {i} failed: {result}")
                total_stats["failed"] += 1
            elif isinstance(result, dict):
                for key in ["processed", "failed", "skipped"]:
                    total_stats[key] += result.get(key, 0)
                # Track the highest observed count
                if (
                    "observed_count" in result
                    and result["observed_count"] > max_observed_count
                ):
                    max_observed_count = result["observed_count"]

        # Include total count if we observed one
        if max_observed_count > 0:
            total_stats["total_count"] = max_observed_count

        return total_stats

    async def _process_assigned_offsets(
        self,
        client,
        pagination_params: dict,
        assigned_offsets: list[int],
        batch_id: str,
        from_date: str | None,
        session_id: int,
        initial_total_count: int,
    ) -> dict[str, int]:
        """Process a list of assigned offsets for a session."""
        import asyncio

        stats = {"processed": 0, "failed": 0, "skipped": 0}
        original_client = self.client
        max_observed_count = initial_total_count

        try:
            self.client = client
            semaphore = asyncio.Semaphore(5)  # Process up to 5 items concurrently

            logger.info(
                f"Session {session_id}: Processing {len(assigned_offsets)} pages"
            )

            for page_idx, offset in enumerate(assigned_offsets):
                # Check if we should stop due to date cutoff
                cutoff_reached = False

                # Fetch this specific page
                page_params = {**pagination_params, "offset": offset, "limit": 250}

                page_items = []
                async for batch in self.fetch_list_data(**page_params):
                    if batch:
                        page_items.extend(batch)
                        break

                if not page_items:
                    logger.debug(f"Session {session_id}: No items at offset {offset}")
                    continue

                logger.info(
                    f"Session {session_id}: Page {page_idx + 1}/{len(assigned_offsets)} - {len(page_items)} items (offset {offset})"
                )

                # Check for date cutoff and count changes
                for item in page_items:
                    update_date = item.get("updateDate")
                    if from_date and update_date and update_date <= from_date:
                        cutoff_reached = True
                        break

                # Check if total count changed (new items appeared)
                if hasattr(self.client, "last_response_metadata"):
                    pagination_info = getattr(
                        self.client, "last_response_metadata", {}
                    ).get("pagination", {})
                    current_count = pagination_info.get("count")
                    if current_count and current_count > max_observed_count:
                        max_observed_count = current_count
                        logger.info(
                            f"Session {session_id}: Observed count increase: {max_observed_count}"
                        )

                # Process items in this page concurrently
                if not cutoff_reached and page_items:
                    item_tasks = []
                    for item in page_items:
                        item["_batch_id"] = f"{batch_id}_session_{session_id}"
                        item["_page_offset"] = offset

                        task = asyncio.create_task(
                            self._process_item_with_semaphore(
                                semaphore, item, session_id
                            )
                        )
                        item_tasks.append(task)

                    # Wait for all items in this page
                    if item_tasks:
                        item_results = await asyncio.gather(
                            *item_tasks, return_exceptions=True
                        )

                        for result in item_results:
                            if isinstance(result, Exception):
                                stats["failed"] += 1
                            elif result == "skipped":
                                stats["skipped"] += 1
                            elif result:
                                stats["processed"] += 1
                            else:
                                stats["failed"] += 1

                if cutoff_reached:
                    logger.info(f"Session {session_id}: Stopping due to date cutoff")
                    break

                # Log progress periodically
                if (page_idx + 1) % 10 == 0:
                    logger.info(
                        f"Session {session_id}: Completed {page_idx + 1}/{len(assigned_offsets)} pages. Stats: {stats}"
                    )

            stats["observed_count"] = max_observed_count
            logger.info(
                f"Session {session_id}: Completed all assigned pages. Final stats: {stats}"
            )
            return stats

        except Exception as e:
            logger.error(f"Session {session_id}: Fatal error: {e}", exc_info=True)
            return stats
        finally:
            self.client = original_client

    def set_processing_resource(self, processing_resource):
        """Set the processing resource to enable parallel session processing."""
        self.processing_resource = processing_resource
        logger.info("Processing resource set for parallel session support")

    async def _process_with_single_client(
        self, pagination_params: dict, batch_id: str, from_date: str | None
    ) -> dict[str, int]:
        """Process data using traditional single client approach."""
        stats = {"processed": 0, "failed": 0, "skipped": 0}
        latest_update_date = None

        batch_counter = 0
        total_items_processed = 0
        cutoff_reached = False
        total_count = None  # Track total count from pagination metadata

        async for batch in self.fetch_list_data(**pagination_params):
            if not batch:
                logger.info("Received empty batch, ending pagination")
                break

            batch_counter += 1
            total_items_processed += len(batch)
            logger.info(
                f"Processing batch {batch_counter} with {len(batch)} items (total processed: {total_items_processed})"
            )

            # Capture total count from first batch if available
            if batch_counter == 1 and hasattr(self.client, "last_response_metadata"):
                pagination_info = getattr(
                    self.client, "last_response_metadata", {}
                ).get("pagination", {})
                total_count = pagination_info.get("count")
                if total_count:
                    logger.info(
                        f"Found total count in pagination metadata: {total_count}"
                    )

            # Track the latest updateDate from this batch for incremental fetching
            for item in batch:
                # Add batch_id to each item's payload for indexing
                item["_batch_id"] = batch_id
                item["_batch_number"] = batch_counter

                # Extract updateDate from the item if available
                update_date = item.get("updateDate")

                # Check if we've reached the cutoff date for incremental processing
                if from_date and update_date and update_date <= from_date:
                    logger.info(
                        f"Reached cutoff date. Item updateDate: {update_date} <= from_date: {from_date}"
                    )
                    logger.info(
                        "Stopping processing as we've reached data older than last processed date"
                    )
                    cutoff_reached = True
                    break

                if update_date and (
                    not latest_update_date or update_date > latest_update_date
                ):
                    latest_update_date = update_date

                result = await self.process_item(item, batch_id)
                if result == "skipped":
                    stats["skipped"] += 1
                elif result:
                    stats["processed"] += 1
                else:
                    stats["failed"] += 1

            logger.info(f"Batch {batch_counter} completed. Running stats: {stats}")

            # Break out of the batch loop if we hit the cutoff
            if cutoff_reached:
                logger.info("Exiting pagination due to date cutoff")
                break

        logger.info(
            f"Processed {batch_counter} batches with {total_items_processed} total items"
        )

        # Include total count if we captured it
        if total_count:
            stats["total_count"] = total_count

        return stats

    async def sync_checkpoint_with_api_tracking(
        self, force_current_time: bool = False
    ) -> bool:
        """
        Sync the checkpoint system with the Congressional API last_processed_date tracking.

        This bridges the gap between the granular SQLite-based checkpointing system
        and the PostgreSQL-based incremental fetching system.

        Args:
            force_current_time: If True, use current time instead of checkpoint date

        Returns:
            True if sync was successful, False otherwise
        """
        if not self.progress_tracker:
            logger.warning("No progress tracker available for syncing")
            return False

        if (
            not hasattr(self, "client")
            or not hasattr(self.client, "db_pool")
            or not self.client.db_pool
        ):
            logger.warning(
                "No Congressional API client with database pool available for syncing"
            )
            return False

        try:
            # Get the last processed date from the checkpoint system
            checkpoint_date = self.progress_tracker.checkpoint.last_processed_date

            if force_current_time or not checkpoint_date:
                from datetime import UTC, datetime

                date_to_use = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
                logger.info(f"Using current time for sync: {date_to_use}")
            else:
                date_to_use = checkpoint_date
                logger.info(f"Using checkpoint date for sync: {date_to_use}")

            # Update the Congressional API tracking table
            success = await self.client.update_last_processed_date(
                self.data_type_name, date_to_use
            )

            if success:
                logger.info(
                    f"Successfully synced {self.data_type_name} last_processed_date to: {date_to_use}"
                )
                return True
            else:
                logger.error(
                    f"Failed to sync {self.data_type_name} last_processed_date"
                )
                return False

        except Exception as e:
            logger.error(
                f"Error syncing checkpoint with API tracking: {str(e)}", exc_info=True
            )
            return False

    async def sync_checkpoint_with_normalizer(
        self, normalizer_checkpoint_manager
    ) -> bool:
        """
        Sync the fetcher checkpoint state with the normalizer checkpoint system.

        This helps coordinate between fetching and normalization phases to prevent
        checkpoint conflicts and ensure proper processing state management.

        Args:
            normalizer_checkpoint_manager: The checkpoint manager used by the normalizer

        Returns:
            True if sync was successful, False otherwise
        """
        if not self.progress_tracker:
            logger.warning("No fetcher progress tracker available for syncing")
            return False

        try:
            # Create a normalizer progress tracker to check its state using the proper stage naming
            normalizer_tracker = HierarchicalProgressTracker.create_for_stage(
                normalizer_checkpoint_manager,
                "congressional",
                "bills",
                ProcessingStage.NORMALIZATION,
            )

            # Get processing stats from both trackers
            fetcher_stats = self.progress_tracker.get_processing_summary()
            normalizer_stats = normalizer_tracker.get_processing_summary()

            logger.info(
                f"Fetcher checkpoint state: {fetcher_stats['status']}, processed: {fetcher_stats['processed_items']}"
            )
            logger.info(
                f"Normalizer checkpoint state: {normalizer_stats['status']}, processed: {normalizer_stats['processed_items']}"
            )

            # If fetcher has processed items but normalizer hasn't started,
            # this is normal - return success
            if (
                fetcher_stats["processed_items"] > 0
                and normalizer_stats["processed_items"] == 0
                and normalizer_stats["status"] == "pending"
            ):
                logger.info(
                    "Fetcher has data ready for normalization - checkpoint sync OK"
                )
                return True

            # If both have processed items, check for consistency
            if (
                fetcher_stats["processed_items"] > 0
                and normalizer_stats["processed_items"] > 0
            ):
                logger.info(
                    "Both fetcher and normalizer have processed items - systems are active"
                )
                return True

            # If normalizer has processed more items than fetcher, something is wrong
            if normalizer_stats["processed_items"] > fetcher_stats["processed_items"]:
                logger.warning(
                    f"Normalizer processed {normalizer_stats['processed_items']} items "
                    f"but fetcher only processed {fetcher_stats['processed_items']} - possible sync issue"
                )
                return False

            return True

        except Exception as e:
            logger.error(
                f"Error syncing checkpoint with normalizer: {str(e)}", exc_info=True
            )
            return False
