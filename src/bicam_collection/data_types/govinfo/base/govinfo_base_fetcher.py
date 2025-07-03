"""
GovInfo-specific base fetcher implementing the 4-phase pattern.

This module provides GovInfo-specific implementations including:
- 4-phase processing pattern (collection → package → granules → granule data)
- GovInfo schema defaults
- GovInfo progress tracking configuration
- GovInfo API patterns
"""

import asyncio
import hashlib
import json
import logging
import uuid
from abc import abstractmethod
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg

from ....libs.checkpoint import HierarchicalProgressTracker, ProcessingStage
from ....libs.run_tracking import RunMetadata, RunType
from ...abstract import AbstractFetcher

logger = logging.getLogger(__name__)


class GovInfoBaseFetcher(AbstractFetcher):
    """
    GovInfo-specific base fetcher.

    Implements the GovInfo API 4-phase pattern:
    1. fetch_collection_data() → collections/packages
    2. fetch_package_data() → package metadata
    3. fetch_granules_list() → granules for package
    4. fetch_granule_data() → full granule data

    Provides GovInfo-specific defaults for schemas and progress tracking.
    """

    def setup_progress_tracker(self) -> HierarchicalProgressTracker | None:
        """GovInfo-specific progress tracker setup."""
        if not self.checkpoint_manager or not self.data_type_name:
            logger.warning("Cannot setup progress tracker - missing dependencies")
            return None

        self.progress_tracker = HierarchicalProgressTracker.create_for_stage(
            self.checkpoint_manager,
            "govinfo",  # GovInfo-specific system name
            self.data_type_name,
            ProcessingStage.SCRAPING,
        )
        return self.progress_tracker

    def set_processing_resource(self, processing_resource) -> None:
        """
        Set the processing resource for accessing parallel API clients and other resources.

        This method is called by the Dagster pipeline to provide access to:
        - Parallel API client sessions
        - Shared database pools
        - Other shared resources

        Args:
            processing_resource: Resource providing parallel clients
        """
        self.processing_resource = processing_resource
        if hasattr(processing_resource, "get_api_clients_for_parallel_sessions"):
            logger.debug("Processing resource has parallel clients capability")
        else:
            logger.debug("Processing resource does not support parallel clients")

    async def setup_run_tracking(
        self, run_metadata: RunMetadata | None = None
    ) -> str | None:
        """Setup run tracking for this processing session."""
        if not self.run_manager:
            logger.warning("No run manager provided - run tracking disabled")
            return None

        try:
            if not run_metadata:
                run_metadata = RunMetadata(
                    data_type_names=[self.data_type_name]
                    if self.data_type_name
                    else [],
                    run_type=RunType.FETCH,
                    description=f"Fetch GovInfo {self.data_type_name} data",
                    configuration={},
                )

            self.current_run_id = await self.run_manager.start_run(run_metadata)
            logger.info(f"Started run tracking with ID: {self.current_run_id}")
            return self.current_run_id

        except Exception as e:
            logger.error(f"Failed to setup run tracking: {e}")
            return None

    # =============================================================================
    # GOVINFO-SPECIFIC 4-PHASE PATTERN
    # =============================================================================

    @abstractmethod
    async def fetch_collection_data(
        self,
        from_date: str | None = None,
        to_date: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
        **kwargs,
    ) -> AsyncIterator[list[dict[str, Any]]]:
        """
        Phase 1: Fetch collections/packages from GovInfo.

        Must yield batches of packages containing URLs for Phase 2.

        Args:
            from_date: Start date filter (YYYY-MM-DD)
            to_date: End date filter (YYYY-MM-DD)
            limit: Items per page
            offset: Starting offset
            **kwargs: Additional parameters

        Yields:
            Batches of package dictionaries with 'packageLink' fields
        """

    @abstractmethod
    async def fetch_package_data(self, package_url: str) -> dict[str, Any] | None:
        """
        Phase 2: Fetch package metadata from GovInfo URL.

        Args:
            package_url: Individual package URL from Phase 1

        Returns:
            Complete package data or None on error
        """

    @abstractmethod
    async def fetch_granules_list(
        self, package_data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """
        Phase 3: Fetch granules list for package.

        Args:
            package_data: Complete package data from Phase 2

        Returns:
            List of granule metadata
        """

    @abstractmethod
    async def fetch_granule_data(self, granule_url: str) -> dict[str, Any] | None:
        """
        Phase 4: Fetch full granule data.

        Args:
            granule_url: Individual granule URL from Phase 3

        Returns:
            Complete granule data or None on error
        """

    # =============================================================================
    # MAIN PROCESSING METHOD - GOVINFO 4-PHASE IMPLEMENTATION
    # =============================================================================

    async def process_items(
        self,
        from_date: str | None = None,
        to_date: str | None = None,
        limit: int | None = None,
        max_items: int | None = None,
        batch_size: int = 50,
        enable_parallelization: bool = False,
        max_concurrent: int = 10,
        **kwargs,
    ) -> dict[str, Any]:
        """
        GovInfo-specific 4-phase processing implementation.

        Args:
            from_date: Start date filter (YYYY-MM-DD)
            to_date: End date filter (YYYY-MM-DD)
            limit: Items per page
            max_items: Maximum total items to process
            batch_size: Items per storage batch
            enable_parallelization: Enable parallel processing
            max_concurrent: Maximum concurrent operations
            **kwargs: Additional parameters

        Returns:
            Processing results with statistics
        """
        logger.info(f"Starting GovInfo {self.data_type_name} processing")
        logger.info(f"Date range: {from_date} to {to_date}")
        logger.info(f"Parallelization: {enable_parallelization}")

        # Setup progress tracking
        self.setup_progress_tracker()

        # Setup run tracking
        await self.setup_run_tracking()

        stats = {
            "total_processed": 0,
            "successful": 0,
            "errors": 0,
            "batches_stored": 0,
            "start_time": datetime.now(UTC),
            "phases": {
                "collection": {"processed": 0, "errors": 0},
                "packages": {"processed": 0, "errors": 0},
                "granules": {"processed": 0, "errors": 0},
                "granule_data": {"processed": 0, "errors": 0},
            },
        }

        try:
            if enable_parallelization:
                return await self._process_with_parallelization(
                    from_date,
                    to_date,
                    limit,
                    max_items,
                    batch_size,
                    max_concurrent,
                    **kwargs,
                )
            else:
                return await self._process_sequentially(
                    from_date, to_date, limit, max_items, batch_size, **kwargs
                )

        except Exception as e:
            logger.error(f"GovInfo processing failed: {e}")
            if self.run_manager and self.current_run_id:
                await self.run_manager.fail_run(self.current_run_id, str(e))
            raise

        finally:
            if self.run_manager and self.current_run_id:
                await self.run_manager.complete_run(self.current_run_id)

    async def _process_sequentially(
        self,
        from_date: str | None,
        to_date: str | None,
        limit: int | None,
        max_items: int | None,
        batch_size: int,
        **kwargs,
    ) -> dict[str, Any]:
        """Process items sequentially using GovInfo 4-phase pattern."""
        stats = {
            "total_processed": 0,
            "successful": 0,
            "errors": 0,
            "batches_stored": 0,
            "start_time": datetime.now(UTC),
            "phases": {
                "collection": {"processed": 0, "errors": 0},
                "packages": {"processed": 0, "errors": 0},
                "granules": {"processed": 0, "errors": 0},
                "granule_data": {"processed": 0, "errors": 0},
            },
        }

        batch_items = []
        processed_count = 0

        # Phase 1: Collection/Package iteration
        async for collection_batch in self.fetch_collection_data(
            from_date=from_date, to_date=to_date, limit=limit, **kwargs
        ):
            stats["phases"]["collection"]["processed"] += len(collection_batch)

            for package_item in collection_batch:
                if max_items and processed_count >= max_items:
                    break

                try:
                    processed_item = await self._process_single_package(package_item)
                    batch_items.append(processed_item)
                    stats["successful"] += 1

                except Exception as e:
                    logger.error(
                        f"Error processing package {package_item.get('packageLink', 'UNKNOWN')}: {e}"
                    )
                    stats["errors"] += 1
                    stats["phases"]["packages"]["errors"] += 1

                processed_count += 1
                stats["total_processed"] += 1

                # Store batch when full
                if len(batch_items) >= batch_size:
                    await self._store_batch(batch_items)
                    stats["batches_stored"] += 1
                    batch_items.clear()

                if max_items and processed_count >= max_items:
                    break

            if max_items and processed_count >= max_items:
                break

        # Store remaining items
        if batch_items:
            await self._store_batch(batch_items)
            stats["batches_stored"] += 1

        stats["end_time"] = datetime.now(UTC)
        stats["duration"] = (stats["end_time"] - stats["start_time"]).total_seconds()

        logger.info(f"GovInfo sequential processing completed: {stats}")
        return stats

    async def _process_with_parallelization(
        self,
        from_date: str | None,
        to_date: str | None,
        limit: int | None,
        max_items: int | None,
        batch_size: int,
        max_concurrent: int,
        **kwargs,
    ) -> dict[str, Any]:
        """Process items with parallelization using multiple API clients."""
        logger.info("Using parallelization for GovInfo processing")

        # Get parallel API clients if available
        processing_resource = getattr(self, "processing_resource", None)
        if not processing_resource or not hasattr(
            processing_resource, "get_api_clients_for_parallel_sessions"
        ):
            logger.warning(
                "No parallel clients available, falling back to sequential processing"
            )
            return await self._process_sequentially(
                from_date, to_date, limit, max_items, batch_size, **kwargs
            )

        api_clients = await processing_resource.get_api_clients_for_parallel_sessions(
            self.data_type_name
        )
        if len(api_clients) <= 1:
            logger.warning(
                "Insufficient parallel clients, falling back to sequential processing"
            )
            return await self._process_sequentially(
                from_date, to_date, limit, max_items, batch_size, **kwargs
            )

        logger.info(f"Using {len(api_clients)} parallel API clients for GovInfo")

        # For GovInfo, we'll distribute package processing across clients
        return await self._process_with_concurrent_package_processing(
            api_clients,
            from_date,
            to_date,
            limit,
            max_items,
            batch_size,
            max_concurrent,
            **kwargs,
        )

    async def _process_with_concurrent_package_processing(
        self,
        api_clients: list,
        from_date: str | None,
        to_date: str | None,
        limit: int | None,
        max_items: int | None,
        batch_size: int,
        max_concurrent: int,
        **kwargs,
    ) -> dict[str, Any]:
        """Process packages with concurrent processing across multiple clients."""
        stats = {
            "total_processed": 0,
            "successful": 0,
            "errors": 0,
            "batches_stored": 0,
            "start_time": datetime.now(UTC),
            "phases": {
                "collection": {"processed": 0, "errors": 0},
                "packages": {"processed": 0, "errors": 0},
                "granules": {"processed": 0, "errors": 0},
                "granule_data": {"processed": 0, "errors": 0},
            },
        }

        # Collect all packages first (Phase 1)
        all_packages = []
        async for collection_batch in self.fetch_collection_data(
            from_date=from_date, to_date=to_date, limit=limit, **kwargs
        ):
            all_packages.extend(collection_batch)
            stats["phases"]["collection"]["processed"] += len(collection_batch)

            if max_items and len(all_packages) >= max_items:
                all_packages = all_packages[:max_items]
                break

        logger.info(f"Collected {len(all_packages)} packages for parallel processing")

        # Distribute packages across clients for parallel processing
        semaphore = asyncio.Semaphore(max_concurrent)

        async def process_package_with_semaphore(package_item, client):
            async with semaphore:
                return await self._process_single_package_with_client(
                    package_item, client
                )

        # Round-robin distribution of packages to clients
        tasks = []
        for i, package_item in enumerate(all_packages):
            client = api_clients[i % len(api_clients)]
            task = process_package_with_semaphore(package_item, client)
            tasks.append(task)

        # Execute all packages concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Process results and store in batches
        batch_items = []
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Package processing failed: {result}")
                stats["errors"] += 1
                stats["phases"]["packages"]["errors"] += 1
            elif result:
                batch_items.append(result)
                stats["successful"] += 1
                stats["total_processed"] += 1

                # Store batch when full
                if len(batch_items) >= batch_size:
                    await self._store_batch(batch_items)
                    stats["batches_stored"] += 1
                    batch_items.clear()

        # Store remaining items
        if batch_items:
            await self._store_batch(batch_items)
            stats["batches_stored"] += 1

        stats["end_time"] = datetime.now(UTC)
        stats["duration"] = (stats["end_time"] - stats["start_time"]).total_seconds()

        logger.info(f"GovInfo parallel processing completed: {stats}")
        return stats

    async def _process_single_package(
        self, package_item: dict[str, Any]
    ) -> dict[str, Any]:
        """Process a single package through all GovInfo phases using the default client."""
        return await self._process_single_package_with_client(package_item, self.client)

    async def _process_single_package_with_client(
        self, package_item: dict[str, Any], client
    ) -> dict[str, Any]:
        """Process a single package through all GovInfo phases using a specific client."""
        package_url = package_item.get("packageLink")
        if not package_url:
            raise ValueError("Package item missing packageLink field")

        # Phase 2: Fetch package data
        package_data = await client.retrieve_package_data_from_url(
            package_url, expected_key="package"
        )
        if not package_data:
            raise ValueError(f"Failed to fetch package data from {package_url}")

        # Phase 3: Fetch granules list
        granules_list = await self.fetch_granules_list(package_data)

        # Phase 4: Fetch granule data for each granule
        granule_data_list = []
        for granule in granules_list:
            granule_url = granule.get("granuleLink")
            if granule_url:
                try:
                    granule_data = await self.fetch_granule_data(granule_url)
                    if granule_data:
                        granule_data_list.append(granule_data)
                except Exception as e:
                    logger.warning(
                        f"Failed to fetch granule data from {granule_url}: {e}"
                    )

        # Combine all data
        return {
            "package_id": self.extract_item_id(package_data),
            "collection_data": package_item,
            "package_data": package_data,
            "granules_list": granules_list,
            "granule_data": granule_data_list,
            "processed_at": datetime.now(UTC).isoformat(),
        }

    async def _store_batch(self, batch_items: list[dict[str, Any]]) -> int:
        """Store a batch of processed GovInfo items."""
        if not batch_items or not self.db_pool:
            return 0

        batch_id = str(uuid.uuid4())
        stored_count = 0

        try:
            for item in batch_items:
                await self.store_complete_package_data(
                    collection_item=item["collection_data"],
                    package_data=item["package_data"],
                    granules_list=item["granules_list"],
                    granule_data=item["granule_data"],
                    batch_id=batch_id,
                )
                stored_count += 1

            logger.debug(f"Stored GovInfo batch {batch_id}: {stored_count} items")

        except Exception as e:
            logger.error(f"Error storing GovInfo batch {batch_id}: {e}")
            raise

        return stored_count

    # =============================================================================
    # STORAGE METHODS - GOVINFO DEFAULTS
    # =============================================================================

    async def store_collection_data(
        self,
        collection_data: dict[str, Any],
        batch_id: str | None = None,
        schema: str = "bicam_raw_govinfo",
        table_suffix: str = "collection_raw",
    ) -> None:
        """Store collection data with GovInfo defaults."""
        table_name = f"{self.data_type_name}_{table_suffix}"
        await self.store_raw_data(
            schema=schema,
            table=table_name,
            items=[collection_data],
            id_field="packageLink",
            batch_id=batch_id,
        )

    async def store_package_data(
        self,
        package_data: dict[str, Any],
        batch_id: str | None = None,
        schema: str = "bicam_raw_govinfo",
        table_suffix: str = "package_raw",
    ) -> None:
        """Store package data with GovInfo defaults."""
        table_name = f"{self.data_type_name}_{table_suffix}"

        # Extract package ID for the record
        package_id = self.extract_item_id(package_data)
        package_data_with_id = {
            **package_data,
            "extracted_package_id": package_id,
            "batch_id": batch_id,
        }

        await self.store_raw_data(
            schema=schema,
            table=table_name,
            items=[package_data_with_id],
            id_field="extracted_package_id",
            url_field="packageLink",
            batch_id=batch_id,
        )

    async def store_granules_data(
        self,
        granules_list: list[dict[str, Any]],
        package_id: str,
        batch_id: str | None = None,
        schema: str = "bicam_raw_govinfo",
        table_suffix: str = "granules_raw",
    ) -> None:
        """Store granules list with GovInfo defaults."""
        if not granules_list:
            return

        table_name = f"{self.data_type_name}_{table_suffix}"

        # Add parent package ID to each granule
        granules_with_parent = []
        for granule in granules_list:
            granule_with_parent = {
                **granule,
                "parent_package_id": package_id,
                "batch_id": batch_id,
            }
            granules_with_parent.append(granule_with_parent)

        await self.store_raw_data(
            schema=schema,
            table=table_name,
            items=granules_with_parent,
            id_field="granuleLink",
            batch_id=batch_id,
        )

    async def store_granule_data(
        self,
        granule_data_list: list[dict[str, Any]],
        package_id: str,
        batch_id: str | None = None,
        schema: str = "bicam_raw_govinfo",
        table_suffix: str = "granule_data_raw",
    ) -> None:
        """Store granule data with GovInfo defaults."""
        if not granule_data_list:
            return

        table_name = f"{self.data_type_name}_{table_suffix}"

        # Add parent package ID to each granule data
        granule_data_with_parent = []
        for granule_data in granule_data_list:
            granule_data_with_parent_item = {
                **granule_data,
                "parent_package_id": package_id,
                "batch_id": batch_id,
            }
            granule_data_with_parent.append(granule_data_with_parent_item)

        await self.store_raw_data(
            schema=schema,
            table=table_name,
            items=granule_data_with_parent,
            id_field="granuleLink",
            batch_id=batch_id,
        )

    async def store_complete_package_data(
        self,
        collection_item: dict[str, Any],
        package_data: dict[str, Any],
        granules_list: list[dict[str, Any]],
        granule_data: list[dict[str, Any]],
        batch_id: str | None = None,
        schema: str = "bicam_raw_govinfo",
    ) -> None:
        """Store complete package data (all phases) with GovInfo defaults."""
        package_id = self.extract_item_id(package_data)

        # Store each component
        await self.store_collection_data(collection_item, batch_id, schema)
        await self.store_package_data(package_data, batch_id, schema)
        await self.store_granules_data(granules_list, package_id, batch_id, schema)
        await self.store_granule_data(granule_data, package_id, batch_id, schema)

        # Update progress tracker if available
        if self.progress_tracker:
            self.progress_tracker.increment_processed(package_id)
            # Also update with detailed progress info
            self.progress_tracker.update_progress(
                last_processed_id=package_id,
                last_exported_item={"package_id": package_id, "status": "completed"},
            )

    async def store_raw_data(
        self,
        schema: str = "bicam_raw_govinfo",
        table: str = None,
        items: list[dict[str, Any]] = None,
        id_field: str = None,
        url_field: str = "packageLink",
        batch_id: str | None = None,
        conn: asyncpg.Connection | None = None,
        **kwargs,
    ) -> None:
        """Store raw data with GovInfo schema defaults."""
        if not all([table, items, id_field]):
            raise ValueError("table, items, and id_field are required")

        if conn:
            await self._execute_store_raw_data(
                conn, schema, table, items, id_field, url_field, batch_id
            )
        else:
            async with self.db_pool.acquire() as conn:
                await self._execute_store_raw_data(
                    conn, schema, table, items, id_field, url_field, batch_id
                )

    async def _execute_store_raw_data(
        self,
        conn: asyncpg.Connection,
        schema: str,
        table: str,
        items: list[dict[str, Any]],
        id_field: str,
        url_field: str,
        batch_id: str | None,
    ) -> None:
        """Execute raw data storage with proper error handling."""
        if not items:
            return

        # Prepare data for storage
        prepared_items = []
        for item in items:
            # Extract source document ID
            source_doc_id = (
                self.extract_item_id(item)
                if hasattr(self, "extract_item_id")
                else item.get(id_field, "")
            )

            # Extract URLs properly
            package_link = item.get(url_field, "")
            if not package_link and "packageLink" in item:
                package_link = item["packageLink"]

            granule_link = item.get("granuleLink", "")

            # Generate ETL batch ID (use batch_id if provided, otherwise generate new UUID)
            etl_batch_id = batch_id if batch_id else str(uuid.uuid4())

            # Add metadata
            item_with_metadata = {
                "id_uuid": str(uuid.uuid4()),
                "package_link": package_link,
                "granule_link": granule_link,
                "batch_id": batch_id or str(uuid.uuid4()),
                "scraped_at": datetime.now(UTC),
                "payload": self._safe_json_dumps(item),
                "source_doc_id": source_doc_id,
                "etl_batch_id": etl_batch_id,
            }
            prepared_items.append(item_with_metadata)

        # Insert data
        try:
            # Create table if needed
            await conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table} (
                    id_uuid TEXT PRIMARY KEY,
                    package_link TEXT,
                    granule_link TEXT,
                    batch_id TEXT,
                    scraped_at TIMESTAMPTZ,
                    payload JSONB,
                    endpoint TEXT,
                    source_doc_id TEXT,
                    etl_batch_id TEXT
                );
            """)

            # Use upsert with ON CONFLICT
            insert_sql = f"""
            INSERT INTO {schema}.{table} (id_uuid, package_link, granule_link, batch_id, scraped_at, payload, source_doc_id, etl_batch_id)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (id_uuid) DO UPDATE SET
                scraped_at = EXCLUDED.scraped_at,
                payload = EXCLUDED.payload,
                source_doc_id = EXCLUDED.source_doc_id,
                etl_batch_id = EXCLUDED.etl_batch_id
            """

            # Extract values efficiently
            values = [
                (
                    item.get("id_uuid"),
                    item.get("package_link"),
                    item.get("granule_link"),
                    item.get("batch_id"),
                    item.get("scraped_at"),
                    item.get("payload"),
                    item.get("source_doc_id"),
                    item.get("etl_batch_id"),
                )
                for item in prepared_items
            ]

            # Insert records with conflict handling
            await conn.executemany(insert_sql, values)

            logger.debug(f"Stored {len(prepared_items)} items in {schema}.{table}")

        except Exception as e:
            logger.error(f"Error storing data in {schema}.{table}: {e}")
            raise

    def _string_to_uuid(self, s: str) -> uuid.UUID:
        """Convert string to deterministic UUID."""
        # Use MD5 hash for deterministic UUID generation

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
