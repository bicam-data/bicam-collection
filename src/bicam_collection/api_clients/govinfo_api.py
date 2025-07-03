"""
GovInfo API Client

This module provides a GovInfo API client that follows the same patterns as
CongressionalAPIClient but adapts to GovInfo's specific implementation including:
- Collections and packages instead of direct data types
- Granules as sub-components of packages
- Different URL patterns and date formatting
- Bulk data retrieval methods
"""

import asyncio
import logging
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any
from urllib.parse import quote

import aiohttp
import asyncpg

logger = logging.getLogger(__name__)


class GovInfoAPIError(Exception):
    """Exception raised for GovInfo API errors."""


class GovInfoAPIClient:
    """
    GovInfo API client that mirrors CongressionalAPIClient patterns.

    This client provides the core functionality needed for GovInfo data retrieval
    following the same 3-phase pattern as Congressional API but adapted for GovInfo's
    collections/packages/granules structure.

    Features:
    - Automatic API key rotation
    - Rate limiting
    - Error logging to database (optional)
    - Collections and packages support
    - Granule data retrieval
    """

    def __init__(
        self,
        api_keys: list[str],
        session: aiohttp.ClientSession | None = None,
        base_url: str = "https://api.govinfo.gov",
        rate_limit_per_second: float = 2.0,
        db_pool: asyncpg.Pool | None = None,
    ):
        self.api_keys = api_keys
        self.current_key_index = 0
        self.session = session
        self.base_url = base_url.rstrip("/")
        self.rate_limit_per_second = rate_limit_per_second
        self._last_request_time = 0.0
        self.db_pool = db_pool

        # Session management
        self._own_session = session is None

        # Rate limiting tracking for adaptive behavior
        self._recent_rate_limits = 0
        self._last_rate_limit_time = 0

        # GovInfo specific tracking
        self.items_remaining = 0
        self.last_response_metadata = {}

    async def __aenter__(self):
        if self._own_session:
            self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._own_session and self.session:
            await self.session.close()

    def _get_current_api_key(self) -> str:
        """Get the current API key, rotating if needed."""
        if not self.api_keys:
            raise GovInfoAPIError("No API keys available")
        return self.api_keys[self.current_key_index % len(self.api_keys)]

    def _rotate_api_key(self):
        """Rotate to the next API key."""
        self.current_key_index = (self.current_key_index + 1) % len(self.api_keys)

    async def _log_error_to_db(
        self, url: str, error_message: str, error_type: str = "api_error"
    ):
        """Log an error to the database if db_pool is available."""
        if not self.db_pool:
            return

        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO bicam_metadata.govinfo_errors
                    (url, error, data_type, timestamp)
                    VALUES ($1, $2, $3, $4)
                """,
                    url,
                    error_message,
                    "govinfo_api",
                    datetime.now(UTC),
                )
        except Exception as e:
            # Don't let database errors break the API client
            logger.warning(f"Failed to log error to database: {e}")

    async def _rate_limit(self):
        """Apply adaptive rate limiting."""
        if self.rate_limit_per_second <= 0:
            return

        current_time = asyncio.get_event_loop().time()
        time_since_last = current_time - self._last_request_time
        min_interval = 1.0 / self.rate_limit_per_second

        # Adaptive rate limiting similar to Congressional API
        if hasattr(self, "_recent_rate_limits") and self._recent_rate_limits > 0:
            time_since_last_rate_limit = current_time - self._last_rate_limit_time
            decay_intervals = int(
                time_since_last_rate_limit / 300
            )  # 5-minute intervals
            if decay_intervals > 0:
                self._recent_rate_limits = max(
                    0, self._recent_rate_limits - decay_intervals
                )
                self._last_rate_limit_time = current_time

            # Slow down by 2x for each recent rate limit, up to 10x slower
            slowdown_factor = min(2**self._recent_rate_limits, 10)
            min_interval *= slowdown_factor
            logger.debug(
                f"Adaptive rate limiting: slowing down by {slowdown_factor}x due to recent rate limits"
            )

        if time_since_last < min_interval:
            sleep_time = min_interval - time_since_last
            logger.debug(f"Rate limiting: sleeping for {sleep_time:.2f}s")
            await asyncio.sleep(sleep_time)

        self._last_request_time = asyncio.get_event_loop().time()

    def _format_date_for_api(self, date_str: str) -> str:
        """Convert date string to GovInfo API format (YYYY-MM-DDTHH:MM:SSZ)."""
        if "T" in date_str and "Z" in date_str:
            return date_str

        # If in YYYY-MM-DD format, convert to YYYY-MM-DDTHH:MM:SSZ
        if len(date_str) == 10 and date_str.count("-") == 2:
            return f"{date_str}T00:00:00Z"

        try:
            dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            logger.warning(f"Unexpected date format: {date_str}, passing through as-is")
            return date_str

    def _construct_collection_url(
        self,
        collection_code: str,
        start_date: str,
        end_date: str,
        doc_class: str = None,
    ) -> str:
        """Construct URL for GovInfo collections with proper date formatting."""
        formatted_start = quote(self._format_date_for_api(start_date))
        formatted_end = quote(self._format_date_for_api(end_date))

        path = f"/collections/{collection_code}/{formatted_start}/{formatted_end}"

        # Construct URL with parameters
        api_key = self._get_current_api_key()
        if doc_class:
            url = f"{self.base_url}{path}?pageSize=1000&docClass={doc_class}&offsetMark=%2A&api_key={api_key}"
        else:
            url = (
                f"{self.base_url}{path}?pageSize=1000&offsetMark=%2A&api_key={api_key}"
            )

        return url

    async def _make_request(self, url: str, max_retries: int = 3) -> dict[str, Any]:
        """Make an API request and return the JSON response."""
        await self._rate_limit()

        retries = 0
        consecutive_rate_limits = 0

        while retries < max_retries:
            logger.debug(f"Making request to: {url} (attempt {retries + 1})")

            try:
                async with self.session.get(url) as response:
                    if response.status == 200:
                        consecutive_rate_limits = 0
                        data = await response.json()
                        return data
                    elif response.status == 403:
                        logger.warning(
                            f"API key failed (403), rotating to next key (attempt {retries + 1})"
                        )
                        self._rotate_api_key()
                        retries += 1
                        if retries < max_retries:
                            continue
                        else:
                            raise GovInfoAPIError(
                                f"All API keys failed authentication after {max_retries} attempts"
                            )
                    elif response.status == 429:
                        consecutive_rate_limits += 1
                        current_time = asyncio.get_event_loop().time()
                        self._recent_rate_limits += 1
                        self._last_rate_limit_time = current_time

                        logger.warning(
                            f"Rate limited (429), rotating to next API key (attempt {retries + 1})"
                        )
                        self._rotate_api_key()

                        if consecutive_rate_limits >= len(self.api_keys):
                            retry_after = int(response.headers.get("Retry-After", 900))
                            backoff_multiplier = min(
                                2 ** (consecutive_rate_limits - len(self.api_keys)), 8
                            )
                            total_wait_time = min(
                                retry_after * backoff_multiplier, 1800
                            )

                            logger.warning(
                                f"All {len(self.api_keys)} API keys rate limited. Waiting {total_wait_time}s"
                            )
                            await asyncio.sleep(total_wait_time)
                            consecutive_rate_limits = 0
                            await asyncio.sleep(5)
                            continue
                        else:
                            await asyncio.sleep(2)
                            continue
                    else:
                        error_text = await response.text()
                        retries += 1
                        if retries < max_retries:
                            logger.warning(
                                f"API error {response.status}, retrying (attempt {retries + 1}): {error_text}"
                            )
                            await asyncio.sleep(2**retries)
                            continue
                        else:
                            raise GovInfoAPIError(
                                f"API request failed: {response.status} - {error_text}"
                            )

            except aiohttp.ClientError as e:
                retries += 1
                if retries < max_retries:
                    logger.warning(
                        f"Network error, retrying (attempt {retries + 1}): {e}"
                    )
                    await asyncio.sleep(2**retries)
                    continue
                else:
                    raise GovInfoAPIError(
                        f"Network error after {max_retries} attempts: {e}"
                    ) from e

        raise GovInfoAPIError(f"Max retries ({max_retries}) exceeded")

    # =============================================================================
    # PHASE 1: COLLECTION DATA RETRIEVAL (equivalent to retrieve_data_list)
    # =============================================================================

    async def retrieve_collection_data(
        self,
        collection_code: str,
        start_date: str,
        end_date: str,
        doc_class: str = None,
        limit: int = None,
        **kwargs,
    ) -> AsyncIterator[list[dict[str, Any]]]:
        """
        PHASE 1: Get collection data for any GovInfo collection type.

        This is equivalent to retrieve_data_list() in CongressionalAPIClient but
        adapted for GovInfo's collections/packages structure.

        Args:
            collection_code: GovInfo collection code (BILLS, CRPT, CHRG, etc.)
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            doc_class: Document class filter (optional)
            limit: Maximum number of results total
            **kwargs: Additional parameters

        Yields:
            Lists of package dictionaries (with URLs for full data)
        """
        if not kwargs.get("next_url"):
            url = self._construct_collection_url(
                collection_code, start_date, end_date, doc_class
            )
        else:
            url = kwargs.get("next_url")

        total_processed = 0
        page_number = 1

        while True:
            try:
                response = await self._make_request(url)

                # Extract pagination metadata
                count = response.get("count", 0)
                next_page = response.get("nextPage")

                # Store pagination metadata for access by fetcher
                self.last_response_metadata = {
                    "pagination": {"count": count, "next": next_page}
                }

                # Extract packages from response
                packages = response.get("packages", [])

                if not packages:
                    logger.info(f"No more packages available for {collection_code}")
                    break

                yield packages

                total_processed += len(packages)
                logger.info(
                    f"Page {page_number} - Packages: {len(packages)}, Total processed: {total_processed}/{count}"
                )

                if not next_page:
                    logger.info(
                        f"No next page found. Completed fetching {collection_code} data after {page_number} pages."
                    )
                    break

                if limit and total_processed >= limit:
                    logger.info(f"Reached limit of {limit} items")
                    break

                # Update URL for next page
                url = next_page
                if "api_key" not in url:
                    api_key = self._get_current_api_key()
                    separator = "&" if "?" in url else "?"
                    url = f"{url}{separator}api_key={api_key}"

                page_number += 1

            except GovInfoAPIError as e:
                logger.error(f"Error fetching {collection_code} collection: {e}")
                await self._log_error_to_db(url, str(e), "collection_data_error")
                break

    # =============================================================================
    # PHASE 2: PACKAGE DATA RETRIEVAL (equivalent to retrieve_full_data_from_url)
    # =============================================================================

    async def retrieve_package_data_from_url(
        self, package_url: str, max_retries: int = 5
    ) -> dict[str, Any] | None:
        """
        PHASE 2: Get full package data from a package URL.

        Equivalent to retrieve_full_data_from_url() but for GovInfo packages.

        Args:
            package_url: Full URL to the package
            max_retries: Maximum number of retries

        Returns:
            Full package data dictionary, or None if error
        """
        for attempt in range(max_retries):
            try:
                # Ensure API key is in URL
                if "api_key" not in package_url:
                    api_key = self._get_current_api_key()
                    separator = "&" if "?" in package_url else "?"
                    package_url = f"{package_url}{separator}api_key={api_key}"

                response = await self._make_request(package_url)
                return response

            except GovInfoAPIError as e:
                if "rate limited" in str(e).lower() and attempt < max_retries - 1:
                    wait_time = 30 * (2**attempt)
                    logger.warning(
                        f"Rate limiting error for {package_url}, waiting {wait_time}s before retry: {e}"
                    )
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    logger.error(
                        f"Error fetching package data from URL {package_url} (attempt {attempt + 1}/{max_retries}): {e}"
                    )
                    if attempt == max_retries - 1:
                        await self._log_error_to_db(
                            package_url, str(e), "package_request_error"
                        )
                        return None

        return None

    # =============================================================================
    # PHASE 3: GRANULE DATA RETRIEVAL (equivalent to retrieve_related_data_from_url)
    # =============================================================================

    async def retrieve_granules_from_url(
        self, granules_url: str, granule_class: str = None, max_retries: int = 5
    ) -> list[dict[str, Any]]:
        """
        PHASE 3: Get granule data from granules URL.

        Equivalent to retrieve_related_data_from_url() but for GovInfo granules.

        Args:
            granules_url: URL to granules data
            granule_class: Granule class filter (optional)
            max_retries: Maximum number of retries

        Returns:
            List of granule data dictionaries
        """
        for attempt in range(max_retries):
            try:
                # Ensure API key is in URL
                if "api_key" not in granules_url:
                    api_key = self._get_current_api_key()
                    separator = "&" if "?" in granules_url else "?"
                    granules_url = f"{granules_url}{separator}api_key={api_key}"

                # Add granule class filter if specified
                if granule_class and "granuleClass" not in granules_url:
                    separator = "&" if "?" in granules_url else "?"
                    granules_url = (
                        f"{granules_url}{separator}granuleClass={granule_class}"
                    )

                response = await self._make_request(granules_url)

                # Extract granules from response
                granules = response.get("granules", [])
                return granules

            except GovInfoAPIError as e:
                if "rate limited" in str(e).lower() and attempt < max_retries - 1:
                    wait_time = 30 * (2**attempt)
                    logger.warning(
                        f"Rate limiting error for granules {granules_url}, waiting {wait_time}s before retry: {e}"
                    )
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    logger.error(
                        f"Error fetching granules from URL {granules_url} (attempt {attempt + 1}/{max_retries}): {e}"
                    )
                    if attempt == max_retries - 1:
                        await self._log_error_to_db(
                            granules_url, str(e), "granules_data_error"
                        )
                        return []

        return []

    # =============================================================================
    # INCREMENTAL FETCHING METHODS (mirroring Congressional API)
    # =============================================================================

    async def access_last_processed_date(self, data_type: str) -> str | None:
        """
        Retrieve the last processed date for a data type from the database.

        Same interface as CongressionalAPIClient but for GovInfo data types.
        """
        if not self.db_pool:
            logger.warning("No database pool available for last processed date lookup")
            return None

        try:
            async with self.db_pool.acquire() as conn:
                result = await conn.fetchrow(
                    """
                    SELECT last_processed_date
                    FROM bicam_metadata.govinfo_last_processed_dates
                    WHERE data_type = $1
                    """,
                    data_type,
                )

                if result:
                    last_date = result["last_processed_date"]
                    logger.info(
                        f"Found last processed date for {data_type}: {last_date}"
                    )
                    return last_date
                else:
                    logger.info(f"No last processed date found for {data_type}")
                    return None

        except Exception as e:
            logger.error(f"Error retrieving last processed date for {data_type}: {e}")
            return None

    async def access_last_processed_count(self, data_type: str) -> int | None:
        """
        Retrieve the last processed count for a data type from the database.

        Same interface as CongressionalAPIClient but for GovInfo data types.
        """
        if not self.db_pool:
            logger.warning("No database pool available for last processed count lookup")
            return None

        try:
            async with self.db_pool.acquire() as conn:
                result = await conn.fetchrow(
                    """
                    SELECT last_total_count
                    FROM bicam_metadata.govinfo_last_processed_dates
                    WHERE data_type = $1 AND last_total_count IS NOT NULL
                    """,
                    data_type,
                )

                if result:
                    last_count = result["last_total_count"]
                    logger.info(
                        f"Found last processed count for {data_type}: {last_count}"
                    )
                    return last_count
                else:
                    logger.info(f"No last processed count found for {data_type}")
                    return None

        except Exception as e:
            logger.error(f"Error retrieving last processed count for {data_type}: {e}")
            return None

    async def update_last_processed_date(
        self, data_type: str, date: str, total_count: int | None = None
    ) -> bool:
        """
        Update the last processed date for a data type in the database.

        Same interface as CongressionalAPIClient but for GovInfo data types.
        """
        if not self.db_pool:
            logger.warning("No database pool available for last processed date update")
            return False

        try:
            async with self.db_pool.acquire() as conn:
                if total_count is not None:
                    # Update both date and count
                    await conn.execute(
                        """
                        INSERT INTO bicam_metadata.govinfo_last_processed_dates
                        (data_type, last_processed_date, last_total_count)
                        VALUES ($1, $2, $3)
                        ON CONFLICT (data_type)
                        DO UPDATE SET
                            last_processed_date = EXCLUDED.last_processed_date,
                            last_total_count = EXCLUDED.last_total_count,
                            updated_at = CURRENT_TIMESTAMP
                        """,
                        data_type,
                        date,
                        total_count,
                    )
                    logger.info(
                        f"Updated last processed date for {data_type} to {date} with count {total_count}"
                    )
                else:
                    # Update only the date, preserve existing count
                    await conn.execute(
                        """
                        INSERT INTO bicam_metadata.govinfo_last_processed_dates
                        (data_type, last_processed_date)
                        VALUES ($1, $2)
                        ON CONFLICT (data_type)
                        DO UPDATE SET
                            last_processed_date = EXCLUDED.last_processed_date,
                            updated_at = CURRENT_TIMESTAMP
                        """,
                        data_type,
                        date,
                    )
                    logger.info(
                        f"Updated last processed date for {data_type} to {date} (count unchanged)"
                    )
                return True

        except Exception as e:
            logger.error(f"Error updating last processed date for {data_type}: {e}")
            return False

    async def retrieve_incremental_collection_data(
        self,
        collection_code: str,
        data_type: str,
        fallback_days: int = 30,
        doc_class: str = None,
        limit: int = None,
        **kwargs,
    ) -> AsyncIterator[tuple[list[dict[str, Any]], str | None]]:
        """
        Get collection data incrementally using the last processed date from database.

        Similar to retrieve_incremental_data_list() but for GovInfo collections.
        """
        from datetime import UTC, datetime, timedelta

        # Get the last processed date
        last_processed = await self.access_last_processed_date(data_type)

        if last_processed:
            start_date = last_processed
            logger.info(f"Fetching {data_type} data updated since: {start_date}")
        else:
            if fallback_days is None or fallback_days <= 0:
                # Fetch entire history
                start_date = None
                logger.info(
                    f"No last processed date found for {data_type}, fetching ALL available data"
                )
            else:
                # Fallback: get data from the last N days
                fallback_date = datetime.now(UTC) - timedelta(days=fallback_days)
                start_date = fallback_date.strftime("%Y-%m-%d")
                logger.info(
                    f"No last processed date found for {data_type}, fetching from {fallback_days} days ago: {start_date}"
                )

        # End date is today
        end_date = datetime.now(UTC).strftime("%Y-%m-%d")

        latest_date = None

        async for batch in self.retrieve_collection_data(
            collection_code=collection_code,
            start_date=start_date,
            end_date=end_date,
            doc_class=doc_class,
            limit=limit,
            **kwargs,
        ):
            if batch:
                # Extract the latest date from this batch
                batch_latest = self._extract_latest_date(batch)
                if batch_latest and (not latest_date or batch_latest > latest_date):
                    latest_date = batch_latest

                yield batch, latest_date
            else:
                yield batch, latest_date

    def _extract_latest_date(self, data_batch: list[dict[str, Any]]) -> str | None:
        """
        Extract the latest date from a batch of GovInfo data.

        Args:
            data_batch: List of package data items

        Returns:
            Latest date string or None
        """
        latest_date = None

        for item in data_batch:
            # GovInfo typically uses 'lastModified' or 'dateIssued'
            date_field = item.get("lastModified") or item.get("dateIssued")

            if date_field and (not latest_date or date_field > latest_date):
                latest_date = date_field

        return latest_date

    async def finalize_incremental_fetch(
        self, data_type: str, latest_date: str | None
    ) -> bool:
        """
        Finalize an incremental fetch by updating the last processed date.

        Same interface as CongressionalAPIClient.
        """
        if latest_date:
            return await self.update_last_processed_date(data_type, latest_date)
        else:
            logger.warning(
                f"No latest date provided for {data_type}, not updating last processed date"
            )
            return False

    # =============================================================================
    # CONVENIENCE METHODS FOR SPECIFIC COLLECTIONS
    # =============================================================================

    async def get_bills_collections(
        self, start_date: str, end_date: str, **kwargs
    ) -> AsyncIterator[list[dict[str, Any]]]:
        """Get Bills collection data."""
        async for batch in self.retrieve_collection_data(
            "BILLS", start_date, end_date, **kwargs
        ):
            yield batch

    async def get_congressional_reports(
        self, start_date: str, end_date: str, **kwargs
    ) -> AsyncIterator[list[dict[str, Any]]]:
        """Get Congressional Reports collection data."""
        async for batch in self.retrieve_collection_data(
            "CRPT", start_date, end_date, **kwargs
        ):
            yield batch

    async def get_hearings(
        self, start_date: str, end_date: str, **kwargs
    ) -> AsyncIterator[list[dict[str, Any]]]:
        """Get Hearings collection data."""
        async for batch in self.retrieve_collection_data(
            "CHRG", start_date, end_date, **kwargs
        ):
            yield batch

    async def get_committee_prints(
        self, start_date: str, end_date: str, **kwargs
    ) -> AsyncIterator[list[dict[str, Any]]]:
        """Get Committee Prints collection data."""
        async for batch in self.retrieve_collection_data(
            "CPRT", start_date, end_date, **kwargs
        ):
            yield batch

    async def get_congressional_directories(
        self, start_date: str, end_date: str, **kwargs
    ) -> AsyncIterator[list[dict[str, Any]]]:
        """Get Congressional Directories collection data."""
        async for batch in self.retrieve_collection_data(
            "CDIR", start_date, end_date, **kwargs
        ):
            yield batch

    async def get_treaties(
        self, start_date: str, end_date: str, **kwargs
    ) -> AsyncIterator[list[dict[str, Any]]]:
        """Get Treaties collection data (CDOC with TDOC doc class)."""
        async for batch in self.retrieve_collection_data(
            "CDOC", start_date, end_date, doc_class="TDOC", **kwargs
        ):
            yield batch
