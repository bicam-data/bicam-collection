"""
Lightweight Congressional API Client

This module provides a simplified API client for the Congressional API that returns
plain dictionaries instead of complex objects. This reduces dependencies and makes
data handling more straightforward.
"""

import asyncio
import logging
import re
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Any

import aiohttp
import asyncpg

logger = logging.getLogger(__name__)


class CongressionalAPIError(Exception):
    """Exception raised for Congressional API errors."""


class CongressionalAPIClient:
    """
    Lightweight Congressional API client that returns dictionaries.

    This client provides the core functionality needed for data retrieval
    without the complexity of object models.

    Features:
    - Automatic API key rotation
    - Rate limiting
    - Error logging to database (optional)
    - Comprehensive pagination support
    """

    def __init__(
        self,
        api_keys: list[str],
        session: aiohttp.ClientSession | None = None,
        base_url: str = "https://api.congress.gov/",
        version: str = "v3",
        rate_limit_per_second: float = 2.0,
        db_pool: asyncpg.Pool | None = None,
    ):
        self.api_keys = api_keys
        self.current_key_index = 0
        self.session = session
        self.version = version
        self.base_url = base_url.rstrip("/") + f"/{self.version}"
        self.rate_limit_per_second = rate_limit_per_second
        self._last_request_time = 0.0
        self.db_pool = db_pool

        # Session management
        self._own_session = session is None

        # Rate limiting tracking for adaptive behavior
        self._recent_rate_limits = 0
        self._last_rate_limit_time = 0

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
            raise CongressionalAPIError("No API keys available")
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
                    INSERT INTO bicam_metadata.congressional_errors
                    (url, error, data_type, timestamp)
                    VALUES ($1, $2, $3, $4)
                """,
                    url,
                    error_message,
                    "congressional_api",
                    datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S"),
                )
        except Exception as e:
            # Don't let database errors break the API client
            logger.warning(f"Failed to log error to database: {e}")

    async def _log_endpoint_error(
        self, endpoint: str, error_message: str, error_type: str = "missing_key"
    ):
        """Log an endpoint-specific error to the database."""
        full_url = f"{self.base_url}{endpoint}"
        await self._log_error_to_db(full_url, error_message, error_type)

    async def _rate_limit(self):
        """Apply adaptive rate limiting."""
        if self.rate_limit_per_second <= 0:
            return

        current_time = asyncio.get_event_loop().time()
        time_since_last = current_time - self._last_request_time
        min_interval = 1.0 / self.rate_limit_per_second

        # Decay recent rate limits over time (every 5 minutes without rate limits, reduce by 1)
        if hasattr(self, "_last_rate_limit_time") and self._recent_rate_limits > 0:
            time_since_last_rate_limit = current_time - self._last_rate_limit_time
            decay_intervals = int(
                time_since_last_rate_limit / 300
            )  # 5-minute intervals
            if decay_intervals > 0:
                self._recent_rate_limits = max(
                    0, self._recent_rate_limits - decay_intervals
                )
                self._last_rate_limit_time = current_time
                if decay_intervals > 0:
                    logger.debug(
                        f"Rate limit tracking: decayed recent rate limits to {self._recent_rate_limits}"
                    )

        # Adaptive rate limiting: if we've recently hit rate limits, be more conservative
        if hasattr(self, "_recent_rate_limits") and self._recent_rate_limits > 0:
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

    async def _ensure_session(self):
        """Ensure session is created if not already available."""
        if self.session is None and self._own_session:
            import aiohttp

            self.session = aiohttp.ClientSession()

    async def _make_request(
        self, endpoint: str, params: dict | None = None, max_retries: int = 3
    ) -> dict[str, Any]:
        """Make an API request and return the JSON response."""
        await self._ensure_session()
        await self._rate_limit()

        # Ensure endpoint starts with /
        if not endpoint.startswith("/"):
            endpoint = f"/{endpoint}"

        # Build complete URL with parameters manually
        request_params = params.copy() if params else {}
        request_params.update(
            {"format": "json", "api_key": self._get_current_api_key()}
        )

        # Manually construct URL with query parameters
        from urllib.parse import urlencode

        query_string = urlencode(request_params)
        url = f"{self.base_url}{endpoint}?{query_string}"

        retries = 0
        consecutive_rate_limits = 0

        while retries < max_retries:
            logger.debug(f"Making request to: {url} (attempt {retries + 1})")

            try:
                async with self.session.get(url) as response:
                    logger.debug(f"Response value: {await response.text()}")

                    if response.status == 200:
                        # Reset consecutive rate limit counter on success
                        consecutive_rate_limits = 0
                        data = await response.json()
                        return data
                    elif response.status == 403:
                        # API key might be invalid, try rotating
                        logger.warning(
                            f"API key failed (403), rotating to next key (attempt {retries + 1})"
                        )
                        self._rotate_api_key()
                        retries += 1
                        if retries < max_retries:
                            continue  # Try again with new key
                        else:
                            await self._log_error_to_db(
                                url,
                                f"All API keys failed authentication after {max_retries} attempts",
                                "api_key_error",
                            )
                            raise CongressionalAPIError(
                                f"All API keys failed authentication after {max_retries} attempts"
                            )
                    elif response.status == 429:
                        # Rate limited - handle more intelligently
                        consecutive_rate_limits += 1

                        # Track recent rate limits for adaptive behavior
                        current_time = asyncio.get_event_loop().time()
                        self._recent_rate_limits += 1
                        self._last_rate_limit_time = current_time

                        logger.warning(
                            f"Rate limited (429), rotating to next API key (attempt {retries + 1}, consecutive rate limits: {consecutive_rate_limits})"
                        )
                        self._rotate_api_key()

                        # If we've tried all keys and they're all rate limited
                        if consecutive_rate_limits >= len(self.api_keys):
                            # Calculate smart wait time based on response headers and consecutive failures
                            retry_after = int(
                                response.headers.get("Retry-After", 900)
                            )  # Default to 15 minutes

                            # Add exponential backoff based on consecutive failures
                            backoff_multiplier = min(
                                2 ** (consecutive_rate_limits - len(self.api_keys)), 8
                            )  # Cap at 8x
                            total_wait_time = retry_after * backoff_multiplier

                            # But cap the maximum wait at 30 minutes to avoid extremely long waits
                            total_wait_time = min(total_wait_time, 1800)

                            logger.warning(
                                f"All {len(self.api_keys)} API keys rate limited. "
                                f"Retry-After: {retry_after}s, Backoff multiplier: {backoff_multiplier}x, "
                                f"Total wait time: {total_wait_time}s ({total_wait_time / 60:.1f} minutes)"
                            )

                            await asyncio.sleep(total_wait_time)

                            # Reset the consecutive rate limit counter after waiting
                            consecutive_rate_limits = 0

                            # Don't increment retries for rate limit waits - we want to keep trying
                            # But add a small additional delay to avoid immediate re-rate-limiting
                            await asyncio.sleep(5)
                            continue  # Try again after waiting
                        else:
                            # Still have keys to try, short delay and continue
                            await asyncio.sleep(2)
                            continue

                    elif response.status == 520:
                        # Cloudflare error - retry after 10 seconds
                        await asyncio.sleep(10)
                        continue
                    elif response.status == 503:
                        # Service Unavailable - retry after 10 seconds
                        await asyncio.sleep(10)
                        continue
                    else:
                        error_text = await response.text()
                        retries += 1
                        if retries < max_retries:
                            logger.warning(
                                f"API error {response.status}, retrying (attempt {retries + 1}): {error_text}"
                            )
                            await asyncio.sleep(2**retries)  # Exponential backoff
                            continue
                        else:
                            await self._log_error_to_db(
                                url,
                                f"API request failed: {response.status} - {error_text}",
                                "api_request_error",
                            )
                            raise CongressionalAPIError(
                                f"API request failed: {response.status} - {error_text}"
                            )

            except aiohttp.ClientError as e:
                retries += 1
                if retries < max_retries:
                    logger.warning(
                        f"Network error, retrying (attempt {retries + 1}): {e}"
                    )
                    await asyncio.sleep(2**retries)  # Exponential backoff
                    continue
                else:
                    await self._log_error_to_db(
                        url,
                        f"Network error after {max_retries} attempts: {e}",
                        "network_error",
                    )
                    raise CongressionalAPIError(
                        f"Network error after {max_retries} attempts: {e}"
                    ) from e

        await self._log_error_to_db(
            url, f"Max retries ({max_retries}) exceeded", "max_retries_exceeded"
        )
        raise CongressionalAPIError(f"Max retries ({max_retries}) exceeded")

    async def retrieve_full_data_from_url(
        self, url: str, expected_key: str | None = None, max_retries: int = 5
    ) -> dict[str, Any] | None:
        """
        PHASE 2: Get full data from an individual item URL.

        Takes a URL from the list data (e.g., from get_bills_list) and fetches
        the complete data for that item. This is the second phase of the 3-phase
        Congressional API pattern.

        Args:
            url: Full URL to the individual item (e.g., bill, member, committee)
            expected_key: Expected top-level key in response (e.g., 'bill', 'member')
                        If None, will try common keys or return whole response
            max_retries: Maximum number of retries for this specific URL

        Returns:
            Full data dictionary for the item, or None if error
        """
        for attempt in range(max_retries):
            try:
                # Extract the endpoint from the full URL
                # URL format: https://api.congress.gov/v3/bill/119/hr/2808?format=json
                if not url.startswith(self.base_url):
                    logger.error(f"URL does not match base URL: {url}")
                    return None

                # Remove base URL and extract endpoint
                endpoint = url.replace(self.base_url, "")

                # Remove existing query parameters (we'll add our own)
                if "?" in endpoint:
                    endpoint = endpoint.split("?")[0]

                response = await self._make_request(endpoint)

                # If expected_key is provided, use it
                if expected_key and expected_key in response:
                    return response[expected_key]
                elif expected_key:
                    # Expected key was provided but not found - log error
                    available_keys = list(response.keys())
                    error_msg = f"Expected key '{expected_key}' not found in response. Available keys: {available_keys}"
                    logger.error(f"Missing expected key in {endpoint}: {error_msg}")

                    # Log to database if available
                    await self._log_endpoint_error(
                        endpoint, error_msg, "missing_expected_key"
                    )
                    return None

                # Return whole response if no nested structure found
                return response

            except CongressionalAPIError as e:
                if "rate limited" in str(e).lower() and attempt < max_retries - 1:
                    # For rate limiting errors, wait with exponential backoff before retrying
                    wait_time = 30 * (2**attempt)  # 30s, 60s, 120s, etc.
                    logger.warning(
                        f"Rate limiting error for {url}, attempt {attempt + 1}/{max_retries}. "
                        f"Waiting {wait_time}s before retry: {e}"
                    )
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    logger.error(
                        f"Error fetching full data from URL {url} (attempt {attempt + 1}/{max_retries}): {e}"
                    )
                    if attempt == max_retries - 1:
                        # Log final failure to database
                        await self._log_error_to_db(url, str(e), "api_request_error")
                        return None
            except Exception as e:
                logger.error(
                    f"Unexpected error fetching full data from URL {url} (attempt {attempt + 1}/{max_retries}): {e}"
                )
                if attempt == max_retries - 1:
                    await self._log_error_to_db(url, str(e), "unexpected_error")
                    return None

        return None

    async def retrieve_related_data_from_url(
        self, url: str, expected_key: list[str] | None = None, max_retries: int = 5
    ) -> list[dict[str, Any]]:
        """
        PHASE 3: Get related data from URLs within full data.

        Takes a URL for related data (e.g., actions, cosponsors, texts) and fetches
        the related items. This is the third phase of the 3-phase Congressional API pattern.

        Args:
            url: URL to related data (e.g., bill actions, cosponsors, etc.)
            expected_key: Expected key(s) in response. Can be:
                        - str: Single top-level key (e.g., 'actions', 'cosponsors')
                        - list[str]: Nested path (e.g., ['subjects', 'legislativeSubjects'])
                        - None: Will try common keys
            max_retries: Maximum number of retries for this specific URL

        Returns:
            List of related data dictionaries
        """
        for attempt in range(max_retries):
            try:
                # Extract the endpoint from the full URL
                if not url.startswith(self.base_url):
                    logger.error(f"URL does not match base URL: {url}")
                    return []

                # Remove base URL and extract endpoint
                endpoint = url.replace(self.base_url, "")

                # Remove existing query parameters (we'll add our own)
                if "?" in endpoint:
                    endpoint = endpoint.split("?")[0]

                response = await self._make_request(endpoint)

                # If expected_key is provided, use it
                if expected_key:
                    if len(expected_key) > 1:
                        # access the key as many times as needed
                        data = response
                        for key in expected_key:
                            if isinstance(data, dict) and key in data:
                                data = data.get(key)
                            else:
                                data = None
                                break
                    else:
                        data = response.get(expected_key[0])
                else:
                    # If no expected_key, try common keys for Congressional API
                    common_keys = [
                        "actions",
                        "cosponsors",
                        "textVersions",
                        "summaries",
                        "titles",
                        "subjects",
                        "relatedBills",
                        "amendments",
                        "committees",
                        "sponsors",
                        "nominees",
                        "hearings",
                        "meetings",
                        "prints",
                        "reports",
                        "laws",
                    ]
                    data = None
                    for key in common_keys:
                        if key in response:
                            data = response.get(key)
                            logger.debug(f"Found data under key '{key}' for URL {url}")
                            break

                if data is not None:
                    if isinstance(data, list):
                        return data
                    else:
                        logger.warning(
                            f"Expected list for key path '{expected_key}' but got {type(data)} from {url}"
                        )
                        return []
                else:
                    # Log available keys for debugging
                    available_keys = (
                        list(response.keys()) if isinstance(response, dict) else []
                    )
                    logger.warning(
                        f"No list data found in response from {url}. "
                        f"Expected key: {expected_key}, Available keys: {available_keys}"
                    )
                    return []

            except CongressionalAPIError as e:
                if "rate limited" in str(e).lower() and attempt < max_retries - 1:
                    # For rate limiting errors, wait with exponential backoff before retrying
                    wait_time = 30 * (2**attempt)  # 30s, 60s, 120s, etc.
                    logger.warning(
                        f"Rate limiting error for related data {url}, attempt {attempt + 1}/{max_retries}. "
                        f"Waiting {wait_time}s before retry: {e}"
                    )
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    logger.error(
                        f"Error fetching related data from URL {url} (attempt {attempt + 1}/{max_retries}): {e}"
                    )
                    if attempt == max_retries - 1:
                        # Log final failure to database
                        await self._log_error_to_db(url, str(e), "related_data_error")
                        return []
            except Exception as e:
                logger.error(
                    f"Unexpected error fetching related data from URL {url} (attempt {attempt + 1}/{max_retries}): {e}"
                )
                if attempt == max_retries - 1:
                    await self._log_error_to_db(url, str(e), "unexpected_error")
                    return []

        return []

    async def retrieve_data_list(
        self,
        data_type: str,
        from_date: str | None = None,
        to_date: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
        sort: str | None = None,
        single_page_only: bool = False,
        **kwargs,
    ) -> AsyncIterator[list[dict[str, Any]]]:
        """
        PHASE 1: Generic method to get list data for any Congressional data type.

        Args:
            data_type: Type of data ('bill', 'member', 'committee', 'nomination', 'treaty', 'amendment')
            congress: Congress number
            chamber: Chamber ('house', 'senate')
            bill_type: Bill type (for bills only)
            from_date: Start date (YYYY-MM-DD) or datetime
            to_date: End date (YYYY-MM-DD) or datetime
            limit: Maximum number of results per page
            offset: Starting offset
            sort: Sort parameter (e.g., 'updateDate+desc')
            single_page_only: If True, only fetch the specified page/offset and stop (for parallel processing)
            **kwargs: Additional parameters specific to data type

        Yields:
            Lists of data dictionaries (with URLs for full data)
        """
        # Validate data_type parameter
        if not data_type:
            raise CongressionalAPIError(
                f"data_type cannot be None or empty. Got: {data_type}"
            )

        # Build endpoint based on data type
        endpoint = f"/{data_type}"
        abnormal_key = kwargs.get("abnormal_key")

        # Build params
        params = {}
        if from_date:
            # Convert YYYY-MM-DD format to YYYY-MM-DDTHH:MM:SSZ format if needed
            params["fromDateTime"] = self._format_date_for_api(from_date)
        if to_date:
            # Convert YYYY-MM-DD format to YYYY-MM-DDTHH:MM:SSZ format if needed
            params["toDateTime"] = self._format_date_for_api(to_date)
        if limit:
            params["limit"] = limit or 250
        if offset is not None:
            params["offset"] = offset
        if sort:
            params["sort"] = sort

        # Add any additional parameters
        params.update(kwargs)

        # Handle pagination - continue until we exhaust all data or reach date cutoff
        total_processed = 0
        current_offset = offset or 0
        page_number = 1

        while True:
            # Update offset for pagination (only if not single page mode and not first iteration)
            if not single_page_only and page_number > 1:
                params["offset"] = current_offset

            try:
                response = await self._make_request(endpoint, params)

                # Extract pagination metadata and store separately
                pagination = response.get("pagination", {})
                logger.debug(f"Pagination: {pagination}")
                total_count = pagination.get("count", "unknown")
                next_url = pagination.get("next")

                # Convert total_count to integer if it's not "unknown"
                parsed_total_count = None
                if total_count != "unknown" and total_count is not None:
                    try:
                        parsed_total_count = int(total_count)
                    except (ValueError, TypeError):
                        parsed_total_count = None

                # Store pagination metadata for access by fetcher
                self.last_response_metadata = {
                    "pagination": {"count": parsed_total_count, "next": next_url}
                }

                # Extract data from response using plural form
                # Handle data types that use camelCase in response by converting kebab-case to camelCase
                if abnormal_key:
                    data_key = abnormal_key
                elif "-" in data_type:
                    # Convert kebab-case to camelCase: committee-meeting -> committeeMeetings
                    parts = data_type.split("-")
                    data_key = (
                        parts[0]
                        + "".join(part.capitalize() for part in parts[1:])
                        + "s"
                    )
                else:
                    data_key = f"{data_type}s"  # bills, members, committees, etc.
                data = response.get(data_key, [])

                # Handle limit=0 case - just get metadata and return empty data
                if limit == 0:
                    logger.info(
                        "Limit=0 mode: returning empty data with pagination metadata only"
                    )
                    yield []
                    break

                # Check pagination - if there's no data and we're not in limit=0 mode, we're done
                if not data:
                    logger.info(f"No more data available for {data_type}")
                    break

                yield data

                total_processed += len(data)

                logger.info(
                    f"Page {page_number} - Items: {len(data)}, Total processed: {total_processed}/{total_count}, Has next: {next_url is not None}"
                )

                # If single_page_only mode, stop after processing one page
                if single_page_only:
                    logger.debug(
                        f"Single page mode: stopping after processing offset {offset}"
                    )
                    break

                if not next_url:
                    logger.info(
                        f"No next URL found in pagination. Completed fetching {data_type} data after {page_number} pages."
                    )
                    break

                current_offset += len(data)
                page_number += 1

            except CongressionalAPIError as e:
                logger.error(f"Error fetching {data_type} list: {e}")
                # Log to database if available
                full_url = f"{self.base_url}{endpoint}"
                await self._log_error_to_db(full_url, str(e), "list_data_error")
                break

    # =============================================================================
    # INCREMENTAL FETCHING METHODS
    # =============================================================================

    async def access_last_processed_date(self, data_type: str) -> str | None:
        """
        Retrieve the last processed date for a data type from the database.

        Args:
            data_type: The data type to check (e.g., 'bills', 'amendments')

        Returns:
            Last processed date as string (YYYY-MM-DD format) or None if not found
        """
        if not self.db_pool:
            logger.warning("No database pool available for last processed date lookup")
            return None

        try:
            async with self.db_pool.acquire() as conn:
                result = await conn.fetchrow(
                    """
                    SELECT last_processed_date
                    FROM bicam_metadata.congressional_last_processed_dates
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

        Args:
            data_type: The data type to check (e.g., 'bills', 'amendments')

        Returns:
            Last processed count as integer or None if not found
        """
        if not self.db_pool:
            logger.warning("No database pool available for last processed count lookup")
            return None

        try:
            async with self.db_pool.acquire() as conn:
                result = await conn.fetchrow(
                    """
                    SELECT last_total_count
                    FROM bicam_metadata.congressional_last_processed_dates
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
        Update the last processed date and optionally total count for a data type in the database.

        Args:
            data_type: The data type to update (e.g., 'bills', 'amendments')
            date: The date to set as last processed (YYYY-MM-DD format)
            total_count: Optional total count of items observed during processing

        Returns:
            True if successful, False otherwise
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
                        INSERT INTO bicam_metadata.congressional_last_processed_dates
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

                return True

        except Exception as e:
            logger.error(f"Error updating last processed date for {data_type}: {e}")
            return False

    async def retrieve_incremental_data_list(
        self,
        data_type: str,
        fallback_days: int = 30,
        limit: int | None = None,
        **kwargs,
    ) -> AsyncIterator[tuple[list[dict[str, Any]], str | None]]:
        """
        Get list data incrementally using the last processed date from database.

        This method automatically:
        1. Retrieves the last processed date from the database
        2. Fetches data updated since that date (sorted by updateDate descending)
        3. Tracks the newest updateDate for the next run

        Args:
            data_type: Type of data ('bill', 'member', etc.)
            fallback_days: If no last processed date, fetch data from this many days ago
            limit: Maximum number of results total
            **kwargs: Additional parameters (congress, bill_type, etc.)

        Yields:
            Tuples of (data_batch, latest_update_date_in_batch)
        """
        # Get the last processed date
        last_processed = await self.access_last_processed_date(data_type)

        if last_processed:
            from_date = last_processed
            logger.info(f"Fetching {data_type} data updated since: {from_date}")
        else:
            if fallback_days is None or fallback_days <= 0:
                # No fallback limit – fetch the entire dataset
                from_date = None
                logger.info(
                    f"No last processed date found for {data_type}, fetching ALL available data"
                )
            else:
                # Fallback: get data from the last N days
                fallback_date = datetime.now(UTC) - timedelta(days=fallback_days)
                from_date = fallback_date.strftime("%Y-%m-%d")
                logger.info(
                    f"No last processed date found for {data_type}, fetching from {fallback_days} days ago: {from_date}"
                )

        # Always sort by updateDate descending to get newest first
        # This ensures we process the most recent data first
        sort_param = "updateDate+desc"

        latest_update_date = None

        async for batch in self.retrieve_data_list(
            data_type=data_type,
            from_date=from_date,
            limit=limit,
            sort=sort_param,
            **kwargs,
        ):
            if batch:
                # Extract the latest updateDate from this batch
                batch_latest = self._extract_latest_update_date(batch)
                if batch_latest and (
                    not latest_update_date or batch_latest > latest_update_date
                ):
                    latest_update_date = batch_latest

                yield batch, latest_update_date
            else:
                yield batch, latest_update_date

    def _extract_latest_update_date(
        self, data_batch: list[dict[str, Any]]
    ) -> str | None:
        """
        Extract the latest updateDate from a batch of data.

        Args:
            data_batch: List of data items

        Returns:
            Latest update date string or None
        """
        latest_date = None

        for item in data_batch:
            # Congressional API typically uses 'updateDate' or 'updateDateIncludingText'
            update_date = item.get("updateDate") or item.get("updateDateIncludingText")

            if update_date and (not latest_date or update_date > latest_date):
                latest_date = update_date

        return latest_date

    async def finalize_incremental_fetch(
        self, data_type: str, latest_date: str | None
    ) -> bool:
        """
        Finalize an incremental fetch by updating the last processed date.

        Args:
            data_type: The data type that was processed
            latest_date: The latest update date encountered during processing

        Returns:
            True if successful, False otherwise
        """
        if latest_date:
            return await self.update_last_processed_date(data_type, latest_date)
        else:
            logger.warning(
                f"No latest date provided for {data_type}, not updating last processed date"
            )
            return False

    def _format_date_for_api(self, date: str | None) -> str | None:
        """
        Convert a date string to YYYY-MM-DDTHH:MM:SSZ format as required by the Congressional API.

        Args:
            date: Date string in various formats, or None

        Returns:
            Date string in YYYY-MM-DDTHH:MM:SSZ format or None
        """
        if not date:
            return None

        # If already in correct format, return as-is
        if "T" in date and "Z" in date and "+00:00" not in date:
            return date

        # If in YYYY-MM-DD format, convert to YYYY-MM-DDTHH:MM:SSZ
        if len(date) == 10 and date.count("-") == 2:
            return f"{date}T00:00:00Z"

            # Handle various datetime formats including timezone info
        try:

            # Handle format like "2025-07-01T22:58:15+00:00" (ISO with timezone)
            if "T" in date and "+00:00" in date:
                clean_date = date.replace("+00:00", "")
                dt = datetime.strptime(clean_date, "%Y-%m-%dT%H:%M:%S")
                return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

            # Handle format like "2025-07-01 22:58:15+00:00" (space-separated with timezone)
            elif "+00:00" in date:
                # Remove timezone and parse
                clean_date = date.replace("+00:00", "")
                dt = datetime.strptime(clean_date, "%Y-%m-%d %H:%M:%S")
                return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

            # Handle other timezone formats (like -05:00, +02:00, etc.)
            tz_pattern = r"[+-]\d{2}:\d{2}$"
            if re.search(tz_pattern, date):
                # Remove timezone suffix and parse
                clean_date = re.sub(tz_pattern, "", date)
                if "T" in clean_date:
                    dt = datetime.strptime(clean_date, "%Y-%m-%dT%H:%M:%S")
                else:
                    dt = datetime.strptime(clean_date, "%Y-%m-%d %H:%M:%S")
                return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

            # Try parsing as simple YYYY-MM-DD
            dt = datetime.strptime(date, "%Y-%m-%d")
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

        except ValueError as e:
            logger.warning(f"Could not parse date format '{date}': {e}")
            # Last resort: if it looks like a date, try to extract just the date part
            if len(date) >= 10 and date[:10].count("-") == 2:
                try:
                    date_part = date[:10]  # Just take YYYY-MM-DD part
                    dt = datetime.strptime(date_part, "%Y-%m-%d")
                    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
                except ValueError:
                    pass

            logger.error(
                f"Unable to convert date '{date}' to API format, returning None"
            )
            return None
