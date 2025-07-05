"""
GovInfo API Client

This module provides a sophisticated GovInfo API client that follows the same patterns as
CongressionalAPIClient but adapts to GovInfo's specific implementation including:
- Collections and packages instead of direct data types
- Granules as sub-components of packages
- Different URL patterns and date formatting
- 4-phase processing pattern (collection → package → granules → granule data)
- Advanced error handling and parallel processing support
"""

import asyncio
import logging
import re
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import quote, urlencode

import aiohttp
import asyncpg

from .base_api_client import BaseAPIClient, BaseAPIError

logger = logging.getLogger(__name__)


class GovInfoAPIError(BaseAPIError):
    """Exception raised for GovInfo API errors."""


class GovInfoAPIClient(BaseAPIClient):
    """
    Sophisticated GovInfo API client that mirrors CongressionalAPIClient patterns.

    This client provides the core functionality needed for GovInfo data retrieval
    following a 4-phase pattern adapted for GovInfo's collections/packages/granules structure:
    1. retrieve_collection_data() → bulk collections endpoints
    2. retrieve_package_data() → individual package URLs
    3. retrieve_granules_data() → granule list URLs from packages
    4. retrieve_granule_data() → individual granule URLs

    Features:
    - Automatic API key rotation
    - Advanced rate limiting with adaptive behavior
    - Comprehensive error logging to database
    - Collections, packages, and granules support
    - Parallel processing support
    - Incremental fetching capabilities
    """

    def __init__(
        self,
        api_keys: list[str],
        session: aiohttp.ClientSession | None = None,
        base_url: str = "https://api.govinfo.gov",
        rate_limit_per_second: float = 2.0,
        db_pool: asyncpg.Pool | None = None,
    ):
        # Initialize the base class
        super().__init__(
            api_keys=api_keys,
            session=session,
            base_url=base_url,
            rate_limit_per_second=rate_limit_per_second,
            db_pool=db_pool,
        )

        # GovInfo specific tracking
        self.last_response_metadata = {}

    def _get_error_table_name(self) -> str:
        """Get the name of the error table for GovInfo API."""
        return "govinfo_errors"

    def _get_data_type_name(self) -> str:
        """Get the data type name for GovInfo API."""
        return "govinfo_api"

    def _format_date_for_api(self, date_str: str) -> str:
        """Convert date string to GovInfo API format (YYYY-MM-DDTHH:MM:SSZ)."""
        if not date_str:
            return None

        # If already in correct format, return as-is
        if "T" in date_str and "Z" in date_str and "+00:00" not in date_str:
            return date_str

        # If in YYYY-MM-DD format, convert to YYYY-MM-DDTHH:MM:SSZ
        if len(date_str) == 10 and date_str.count("-") == 2:
            return f"{date_str}T00:00:00Z"

        # Handle various datetime formats including timezone info
        try:
            # Handle format like "2025-07-01T22:58:15+00:00" (ISO with timezone)
            if "T" in date_str and "+00:00" in date_str:
                clean_date = date_str.replace("+00:00", "")
                dt = datetime.strptime(clean_date, "%Y-%m-%dT%H:%M:%S")
                return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

            # Handle format like "2025-07-01 22:58:15+00:00" (space-separated with timezone)
            elif "+00:00" in date_str:
                clean_date = date_str.replace("+00:00", "")
                dt = datetime.strptime(clean_date, "%Y-%m-%d %H:%M:%S")
                return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

            # Handle other timezone formats (like -05:00, +02:00, etc.)
            tz_pattern = r"[+-]\d{2}:\d{2}$"
            if re.search(tz_pattern, date_str):
                clean_date = re.sub(tz_pattern, "", date_str)
                if "T" in clean_date:
                    dt = datetime.strptime(clean_date, "%Y-%m-%dT%H:%M:%S")
                else:
                    dt = datetime.strptime(clean_date, "%Y-%m-%d %H:%M:%S")
                return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

            # Try parsing as simple YYYY-MM-DD
            dt = datetime.strptime(date_str, "%Y-%m-%d")
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

        except ValueError as e:
            logger.warning(f"Could not parse date format '{date_str}': {e}")
            # Last resort: if it looks like a date, try to extract just the date part
            if len(date_str) >= 10 and date_str[:10].count("-") == 2:
                try:
                    date_part = date_str[:10]  # Just take YYYY-MM-DD part
                    dt = datetime.strptime(date_part, "%Y-%m-%d")
                    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
                except ValueError:
                    pass

            logger.error(
                f"Unable to convert date '{date_str}' to GovInfo API format, returning None"
            )
            return None

    def _construct_collection_url(
        self,
        collection_code: str,
        start_date: str,
        end_date: str,
        doc_class: str = None,
        offset_mark: str = "*",
        page_size: int = 1000,
    ) -> str:
        """Construct URL for GovInfo collections with proper date formatting."""
        formatted_start = quote(self._format_date_for_api(start_date))
        formatted_end = quote(self._format_date_for_api(end_date))

        path = f"/collections/{collection_code}/{formatted_start}/{formatted_end}"

        # Build query parameters
        params = {
            "pageSize": page_size,
            "offsetMark": offset_mark,
            "api_key": self._get_current_api_key(),
        }

        if doc_class:
            params["docClass"] = doc_class

        query_string = urlencode(params)
        return f"{self.base_url}{path}?{query_string}"

    async def _make_request(self, url: str, max_retries: int = 3) -> dict[str, Any]:
        """Make an API request and return the JSON response with comprehensive error handling."""
        await self._ensure_session()
        await self._rate_limit()

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
                        # Update URL with new API key for GovInfo-specific handling
                        if "api_key=" in url:
                            new_api_key = self._get_current_api_key()
                            url = re.sub(
                                r"api_key=[^&]*", f"api_key={new_api_key}", url
                            )
                        continue

                    if await self._handle_auth_error(response):
                        # Update URL with new API key for GovInfo-specific handling
                        if "api_key=" in url:
                            new_api_key = self._get_current_api_key()
                            url = re.sub(
                                r"api_key=[^&]*", f"api_key={new_api_key}", url
                            )
                        retries += 1
                        continue

                    # Handle GovInfo-specific errors
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
                            raise GovInfoAPIError(
                                f"All API keys failed authentication after {max_retries} attempts"
                            )
                        # Update URL with new API key
                        if "api_key=" in url:
                            new_api_key = self._get_current_api_key()
                            url = re.sub(
                                r"api_key=[^&]*", f"api_key={new_api_key}", url
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
                            raise GovInfoAPIError(
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
                    raise GovInfoAPIError(
                        f"Network error after {max_retries} attempts: {e}"
                    ) from e

        await self._log_error_to_db(
            url, f"Max retries ({max_retries}) exceeded", "max_retries_exceeded"
        )
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
        offset_mark: str = "*",
        single_page_only: bool = False,
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
            offset_mark: Pagination offset mark
            single_page_only: If True, only fetch the specified page and stop (for parallel processing)
            **kwargs: Additional parameters

        Yields:
            Lists of package dictionaries (with URLs for full data)
        """
        # Validate collection_code parameter
        if not collection_code:
            raise GovInfoAPIError(
                f"collection_code cannot be None or empty. Got: {collection_code}"
            )

        url = self._construct_collection_url(
            collection_code, start_date, end_date, doc_class, offset_mark
        )

        total_processed = 0
        page_number = 1

        while True:
            try:
                response = await self._make_request(url)

                # Extract pagination metadata
                count = response.get("count", 0)
                next_page = response.get("nextPage")

                # Convert count to integer if possible
                parsed_count = None
                if count != "unknown" and count is not None:
                    try:
                        parsed_count = int(count)
                    except (ValueError, TypeError):
                        parsed_count = None

                # Store pagination metadata for access by fetcher
                self.last_response_metadata = {
                    "pagination": {"count": parsed_count, "next": next_page}
                }

                # Extract packages from response
                packages = response.get("packages", [])

                # Handle limit=0 case - just get metadata and return empty data
                if limit == 0:
                    logger.info(
                        "Limit=0 mode: returning empty data with pagination metadata only"
                    )
                    yield []
                    break

                # Check pagination - if there's no data and we're not in limit=0 mode, we're done
                if not packages:
                    logger.info(f"No more packages available for {collection_code}")
                    break

                yield packages

                total_processed += len(packages)
                logger.info(
                    f"Page {page_number} - Packages: {len(packages)}, Total processed: {total_processed}/{count}, Has next: {next_page is not None}"
                )

                # If single_page_only mode, stop after processing one page
                if single_page_only:
                    logger.debug(
                        "Single page mode: stopping after processing one page"
                    )
                    break

                if not next_page:
                    logger.info(
                        f"No next page found. Completed fetching {collection_code} data after {page_number} pages."
                    )
                    break

                if limit and total_processed >= limit:
                    logger.info(f"Reached limit of {limit} items")
                    break

                # Update URL for next page with proper API key
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
        PHASE 2: Get full package data from an individual package URL.

        Takes a URL from the collection data (e.g., from retrieve_collection_data) and fetches
        the complete package data. This is the second phase of the 4-phase GovInfo pattern.

        Args:
            package_url: Full URL to the individual package
            max_retries: Maximum number of retries for this specific URL

        Returns:
            Full package data dictionary, or None if error
        """
        for attempt in range(max_retries):
            try:
                # Add API key to package URL if not present
                if "api_key" not in package_url:
                    api_key = self._get_current_api_key()
                    separator = "&" if "?" in package_url else "?"
                    package_url = f"{package_url}{separator}api_key={api_key}"

                response = await self._make_request(package_url)

                # GovInfo package data is typically returned directly
                return response

            except GovInfoAPIError as e:
                if "rate limited" in str(e).lower() and attempt < max_retries - 1:
                    # For rate limiting errors, wait with exponential backoff before retrying
                    wait_time = 30 * (2**attempt)  # 30s, 60s, 120s, etc.
                    logger.warning(
                        f"Rate limiting error for {package_url}, attempt {attempt + 1}/{max_retries}. "
                        f"Waiting {wait_time}s before retry: {e}"
                    )
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    logger.error(
                        f"Error fetching package data from URL {package_url} (attempt {attempt + 1}/{max_retries}): {e}"
                    )
                    if attempt == max_retries - 1:
                        # Log final failure to database
                        await self._log_error_to_db(
                            package_url, str(e), "package_request_error"
                        )
                        return None
            except Exception as e:
                logger.error(
                    f"Unexpected error fetching package data from URL {package_url} (attempt {attempt + 1}/{max_retries}): {e}"
                )
                if attempt == max_retries - 1:
                    await self._log_error_to_db(package_url, str(e), "unexpected_error")
                    return None

        return None

    # =============================================================================
    # PHASE 3: GRANULES LIST RETRIEVAL (equivalent to retrieve_related_data_from_url for lists)
    # =============================================================================

    async def retrieve_granules_from_url(
        self, granules_url: str, granule_class: str = None, max_retries: int = 5
    ) -> list[dict[str, Any]]:
        """
        PHASE 3: Get granules list data from a granules URL.

        Takes a granules URL from the package data and fetches the list of granules.
        This is the third phase of the 4-phase GovInfo pattern.

        Args:
            granules_url: URL to granules list (e.g., from package data 'granulesLink')
            granule_class: Optional granule class filter
            max_retries: Maximum number of retries for this specific URL

        Returns:
            List of granule data dictionaries
        """
        all_granules = []

        for attempt in range(max_retries):
            try:
                # Add API key and parameters to granules URL if not present
                if "api_key" not in granules_url:
                    api_key = self._get_current_api_key()
                    separator = "&" if "?" in granules_url else "?"
                    granules_url = f"{granules_url}{separator}api_key={api_key}"

                # Add pageSize for efficient fetching
                if "pageSize" not in granules_url:
                    separator = "&" if "?" in granules_url else "?"
                    granules_url = f"{granules_url}{separator}pageSize=1000"

                # Add granule class filter if specified
                if granule_class and "granuleClass" not in granules_url:
                    separator = "&" if "?" in granules_url else "?"
                    granules_url = (
                        f"{granules_url}{separator}granuleClass={granule_class}"
                    )

                current_url = granules_url

                # Handle pagination for granules
                while current_url:
                    response = await self._make_request(current_url)

                    # Extract granules from response
                    granules = response.get("granules", [])
                    if not granules:
                        logger.info(f"No granules found in response from {current_url}")
                        break

                    all_granules.extend(granules)

                    # Check for next page
                    next_page = response.get("nextPage")
                    if not next_page:
                        break

                    # Prepare next URL with API key
                    current_url = next_page
                    if "api_key" not in current_url:
                        api_key = self._get_current_api_key()
                        separator = "&" if "?" in current_url else "?"
                        current_url = f"{current_url}{separator}api_key={api_key}"

                return all_granules

            except GovInfoAPIError as e:
                if "rate limited" in str(e).lower() and attempt < max_retries - 1:
                    # For rate limiting errors, wait with exponential backoff before retrying
                    wait_time = 30 * (2**attempt)  # 30s, 60s, 120s, etc.
                    logger.warning(
                        f"Rate limiting error for granules {granules_url}, attempt {attempt + 1}/{max_retries}. "
                        f"Waiting {wait_time}s before retry: {e}"
                    )
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    logger.error(
                        f"Error fetching granules from URL {granules_url} (attempt {attempt + 1}/{max_retries}): {e}"
                    )
                    if attempt == max_retries - 1:
                        # Log final failure to database
                        await self._log_error_to_db(
                            granules_url, str(e), "granules_request_error"
                        )
                        return []
            except Exception as e:
                logger.error(
                    f"Unexpected error fetching granules from URL {granules_url} (attempt {attempt + 1}/{max_retries}): {e}"
                )
                if attempt == max_retries - 1:
                    await self._log_error_to_db(
                        granules_url, str(e), "unexpected_error"
                    )
                    return []

        return []

    # =============================================================================
    # PHASE 4: GRANULE DATA RETRIEVAL (equivalent to retrieve_full_data_from_url for granules)
    # =============================================================================

    async def retrieve_granule_data_from_url(
        self, granule_url: str, max_retries: int = 5
    ) -> dict[str, Any] | None:
        """
        PHASE 4: Get full granule data from an individual granule URL.

        Takes a URL from the granules list (e.g., from retrieve_granules_from_url) and fetches
        the complete granule data. This is the fourth phase of the 4-phase GovInfo pattern.

        Args:
            granule_url: Full URL to the individual granule
            max_retries: Maximum number of retries for this specific URL

        Returns:
            Full granule data dictionary, or None if error
        """
        for attempt in range(max_retries):
            try:
                # Add API key to granule URL if not present
                if "api_key" not in granule_url:
                    api_key = self._get_current_api_key()
                    separator = "&" if "?" in granule_url else "?"
                    granule_url = f"{granule_url}{separator}api_key={api_key}"

                response = await self._make_request(granule_url)

                # GovInfo granule data is typically returned directly
                return response

            except GovInfoAPIError as e:
                if "rate limited" in str(e).lower() and attempt < max_retries - 1:
                    # For rate limiting errors, wait with exponential backoff before retrying
                    wait_time = 30 * (2**attempt)  # 30s, 60s, 120s, etc.
                    logger.warning(
                        f"Rate limiting error for {granule_url}, attempt {attempt + 1}/{max_retries}. "
                        f"Waiting {wait_time}s before retry: {e}"
                    )
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    logger.error(
                        f"Error fetching granule data from URL {granule_url} (attempt {attempt + 1}/{max_retries}): {e}"
                    )
                    if attempt == max_retries - 1:
                        # Log final failure to database
                        await self._log_error_to_db(
                            granule_url, str(e), "granule_request_error"
                        )
                        return None
            except Exception as e:
                logger.error(
                    f"Unexpected error fetching granule data from URL {granule_url} (attempt {attempt + 1}/{max_retries}): {e}"
                )
                if attempt == max_retries - 1:
                    await self._log_error_to_db(granule_url, str(e), "unexpected_error")
                    return None

        return None

    # =============================================================================
    # INCREMENTAL FETCHING METHODS
    # =============================================================================

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

        This method automatically:
        1. Retrieves the last processed date from the database
        2. Fetches data updated since that date
        3. Tracks the newest date for the next run

        Args:
            collection_code: GovInfo collection code (BILLS, CRPT, CHRG, etc.)
            data_type: Type of data for tracking ('congressional_directories', 'bills_collection', etc.)
            fallback_days: If no last processed date, fetch data from this many days ago
            doc_class: Document class filter (optional)
            limit: Maximum number of results total
            **kwargs: Additional parameters

        Yields:
            Tuples of (data_batch, latest_date_in_batch)
        """
        # Get the last processed date
        last_processed = await self.access_last_processed_date(data_type)

        if last_processed:
            start_date = last_processed
            logger.info(f"Fetching {data_type} data updated since: {start_date}")
        else:
            if fallback_days is None or fallback_days <= 0:
                # No fallback limit – fetch a reasonable default range
                fallback_date = datetime.now(UTC) - timedelta(
                    days=365
                )  # 1 year fallback
                start_date = fallback_date.strftime("%Y-%m-%d")
                logger.info(
                    f"No last processed date found for {data_type}, fetching from 1 year ago: {start_date}"
                )
            else:
                # Fallback: get data from the last N days
                fallback_date = datetime.now(UTC) - timedelta(days=fallback_days)
                start_date = fallback_date.strftime("%Y-%m-%d")
                logger.info(
                    f"No last processed date found for {data_type}, fetching from {fallback_days} days ago: {start_date}"
                )

        # Use current date as end date
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
            data_batch: List of data items

        Returns:
            Latest date string or None
        """
        latest_date = None

        for item in data_batch:
            # GovInfo typically uses 'lastModified' or 'dateIssued'
            date_value = item.get("lastModified") or item.get("dateIssued")

            if date_value and (not latest_date or date_value > latest_date):
                latest_date = date_value

        return latest_date
