"""
Lightweight Congressional API Client

This module provides a simplified API client for the Congressional API that returns
plain dictionaries instead of complex objects. This reduces dependencies and makes
data handling more straightforward.
"""

import asyncio
import logging
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import urlencode

import aiohttp
import asyncpg

from .base_api_client import BaseAPIClient, BaseAPIError

logger = logging.getLogger(__name__)


class CongressionalAPIError(BaseAPIError):
    """Exception raised for Congressional API errors."""


class CongressionalAPIClient(BaseAPIClient):
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
        # Initialize the base class with the full URL
        full_base_url = base_url.rstrip("/") + f"/{version}"
        super().__init__(
            api_keys=api_keys,
            session=session,
            base_url=full_base_url,
            rate_limit_per_second=rate_limit_per_second,
            db_pool=db_pool,
        )
        self.version = version

    def _get_error_table_name(self) -> str:
        """Get the name of the error table for Congressional API."""
        return "congressional_errors"

    def _get_data_type_name(self) -> str:
        """Get the data type name for Congressional API."""
        return "congressional_api"

    def _get_last_processed_dates_table_name(self) -> str:
        """Get the appropriate last processed dates table name for Congressional API."""
        return "congressional_last_processed_dates"

    def _extract_latest_date(self, data_batch: list[dict[str, Any]]) -> str | None:
        """Extract the latest update date from Congressional API data."""
        if not data_batch:
            return None

        latest_date = None
        for item in data_batch:
            # Congressional API specific date fields
            for date_field in ["updateDate", "lastModified", "actionDate", "date"]:
                if date_field in item and item[date_field]:
                    item_date = item[date_field]
                    if not latest_date or item_date > latest_date:
                        latest_date = item_date

        return latest_date

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
        query_string = urlencode(request_params)
        url = f"{self.base_url}{endpoint}?{query_string}"

        retries = 0

        while retries < max_retries:
            logger.debug(f"Making request to: {url} (attempt {retries + 1})")

            try:
                async with self.session.get(url) as response:
                    logger.debug(f"Response status: {response.status}")

                    if response.status == 200:
                        data = await response.json()
                        return data

                    # Use base class methods for common error handling
                    if await self._handle_rate_limit_response(response):
                        continue  # Retry with new key

                    if await self._handle_auth_error(response):
                        retries += 1
                        continue  # Retry with new key

                    # Handle Congressional API specific errors
                    if response.status == 403:
                        # API key might be invalid, try rotating
                        logger.warning(
                            f"API key failed (403), rotating to next key (attempt {retries + 1})"
                        )
                        self._rotate_api_key()
                        retries += 1
                        if retries >= max_retries:
                            await self._log_error_to_db(
                                url,
                                f"All API keys failed authentication after {max_retries} attempts",
                                "api_key_error",
                            )
                            raise CongressionalAPIError(
                                f"All API keys failed authentication after {max_retries} attempts"
                            )
                        continue
                    elif response.status in [520, 503]:
                        # Cloudflare error or Service Unavailable - retry after 10 seconds
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
        self, url: str, full_key: str | None = None, max_retries: int = 5
    ) -> dict[str, Any] | None:
        """
        PHASE 2: Get full data from an individual item URL.

        Takes a URL from the list data (e.g., from get_bills_list) and fetches
        the complete data for that item. This is the second phase of the 3-phase
        Congressional API pattern.

        Args:
            url: Full URL to the individual item (e.g., bill, member, committee)
            full_key: Expected top-level key in response (e.g., 'bill', 'member')
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

                # If full_key is provided, use it
                if full_key and full_key in response:
                    return response[full_key]
                elif full_key:
                    # Expected key was provided but not found - log error
                    available_keys = list(response.keys())
                    error_msg = f"Expected key '{full_key}' not found in response. Available keys: {available_keys}"
                    logger.error(f"Missing full key in {endpoint}: {error_msg}")

                    # Log to database if available
                    await self._log_endpoint_error(
                        endpoint, error_msg, "missing_full_key"
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
        self, url: str, list_key: list[str] | None = None, max_retries: int = 5
    ) -> list[dict[str, Any]]:
        """
        PHASE 3: Get related data from URLs within full data.

        Takes a URL for related data (e.g., actions, cosponsors, texts) and fetches
        the related items. This is the third phase of the 3-phase Congressional API pattern.

        Args:
            url: URL to related data (e.g., bill actions, cosponsors, etc.)
            list_key: Expected key(s) in response. Can be:
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

                # If list_key is provided, use it
                if list_key:
                    if len(list_key) > 1:
                        # access the key as many times as needed
                        data = response
                        for key in list_key:
                            if isinstance(data, dict) and key in data:
                                data = data.get(key)
                            else:
                                data = None
                                break
                    else:
                        data = response.get(list_key[0])
                else:
                    # If no list_key, try common keys for Congressional API
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
                            f"Expected list for key path '{list_key}' but got {type(data)} from {url}"
                        )
                        return []
                else:
                    # Log available keys for debugging
                    available_keys = (
                        list(response.keys()) if isinstance(response, dict) else []
                    )
                    logger.warning(
                        f"No list data found in response from {url}. "
                        f"Expected key: {list_key}, Available keys: {available_keys}"
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

                # Return the full response - let the fetcher handle data extraction using config
                # Handle limit=0 case - just get metadata and return empty data
                # if limit == 0:
                #     logger.info(
                #         "Limit=0 mode: returning empty data with pagination metadata only"
                #     )
                #     yield []
                #     break

                # Check pagination - if there's no data and we're not in limit=0 mode, we're done
                if not response:
                    logger.info(f"No more data available for {data_type}")
                    break

                yield response

                # For logging purposes, try to get the data length from the response
                # This is just for logging - the actual data extraction is handled by the fetcher
                data_length = 0
                if isinstance(response, dict):
                    # Try to find any list in the response for logging
                    for value in response.values():
                        if isinstance(value, list):
                            data_length = len(value)
                            break

                total_processed += data_length

                logger.info(
                    f"Page {page_number} - Items: {data_length}, Total processed: {total_processed}/{total_count}, Has next: {next_url is not None}"
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

                current_offset += data_length
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
                batch_latest = self._extract_latest_date(batch)
                if batch_latest and (
                    not latest_update_date or batch_latest > latest_update_date
                ):
                    latest_update_date = batch_latest

                yield batch, latest_update_date
            else:
                yield batch, latest_update_date
