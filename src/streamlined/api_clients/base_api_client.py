"""
Base API Client

This module provides a base API client class that contains common functionality
for both Congressional and GovInfo API clients, reducing code duplication.
"""

import asyncio
import logging
from abc import ABC, abstractmethod
from datetime import UTC, datetime
from typing import Any

import aiohttp
import asyncpg

logger = logging.getLogger(__name__)


class BaseAPIError(Exception):
    """Base exception class for API errors."""


class BaseAPIClient(ABC):
    """
    Base API client class containing common functionality.

    Provides shared functionality for:
    - API key rotation
    - Rate limiting with adaptive behavior
    - Error handling and database logging
    - Session management
    - Database operations for tracking
    """

    def __init__(
        self,
        api_keys: list[str],
        session: aiohttp.ClientSession | None = None,
        base_url: str = "",
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
            raise BaseAPIError("No API keys available")
        return self.api_keys[self.current_key_index % len(self.api_keys)]

    def _rotate_api_key(self):
        """Rotate to the next API key."""
        self.current_key_index = (self.current_key_index + 1) % len(self.api_keys)

    @abstractmethod
    def _get_error_table_name(self) -> str:
        """Get the name of the error table for this API client."""

    @abstractmethod
    def _get_data_type_name(self) -> str:
        """Get the data type name for this API client."""

    async def _log_error_to_db(
        self, url: str, error_message: str, error_type: str = "api_error"
    ):
        """Log an error to the database if db_pool is available."""
        if not self.db_pool:
            return

        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute(
                    f"""
                    INSERT INTO bicam_metadata.{self._get_error_table_name()}
                    (url, error, data_type, timestamp)
                    VALUES ($1, $2, $3, $4)
                """,
                    url,
                    error_message,
                    self._get_data_type_name(),
                    datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S"),
                )
        except Exception as e:
            # Don't let database errors break the API client
            logger.warning(f"Failed to log error to database: {e}")

    async def _log_endpoint_error(
        self, endpoint: str, error_message: str, error_type: str = "missing_key"
    ):
        """Log an endpoint-specific error to the database."""
        await self._log_error_to_db(endpoint, error_message, error_type)

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

    async def _handle_rate_limit_response(self, response: aiohttp.ClientResponse):
        """Handle rate limit response and update tracking."""
        if response.status == 429:
            # Track rate limit occurrence
            self._recent_rate_limits += 1
            self._last_rate_limit_time = asyncio.get_event_loop().time()
            logger.warning(f"Rate limit hit (recent count: {self._recent_rate_limits})")

            # Rotate API key on rate limit
            self._rotate_api_key()

            # Wait before retrying
            await asyncio.sleep(min(2**self._recent_rate_limits, 60))
            return True
        return False

    async def _handle_auth_error(self, response: aiohttp.ClientResponse):
        """Handle authentication errors."""
        if response.status == 401:
            logger.warning("Authentication failed, rotating API key")
            self._rotate_api_key()
            return True
        return False

    # =============================================================================
    # DATABASE OPERATIONS FOR TRACKING
    # =============================================================================

    async def access_last_processed_date(self, data_type: str) -> str | None:
        """Get the last processed date for a specific data type."""
        if not self.db_pool:
            return None

        # Determine which table to query based on the client type
        table_name = self._get_last_processed_dates_table_name()

        try:
            async with self.db_pool.acquire() as conn:
                result = await conn.fetchrow(
                    f"""
                    SELECT last_processed_date
                    FROM bicam_metadata.{table_name}
                    WHERE data_type = $1
                    """,
                    data_type,
                )
                return result["last_processed_date"] if result else None
        except Exception as e:
            logger.warning(f"Failed to access last processed date: {e}")
            return None

    def _get_last_processed_dates_table_name(self) -> str:  # noqa: B027
        """Get the appropriate last processed dates table name based on client type."""


    async def access_last_processed_count(self, data_type: str) -> int | None:
        """Get the last processed count for a specific data type."""
        if not self.db_pool:
            return None

        # Determine which table to query based on the client type
        table_name = self._get_last_processed_dates_table_name()

        try:
            async with self.db_pool.acquire() as conn:
                result = await conn.fetchrow(
                    f"""
                    SELECT last_total_count
                    FROM bicam_metadata.{table_name}
                    WHERE data_type = $1
                    """,
                    data_type,
                )
                return result["last_total_count"] if result else None
        except Exception as e:
            logger.warning(f"Failed to access last processed count: {e}")
            return None

    async def update_last_processed_date(
        self, data_type: str, date: str, total_count: int | None = None
    ) -> bool:
        """Update the last processed date for a specific data type."""
        if not self.db_pool:
            return False

        # Determine which table to query based on the client type
        table_name = self._get_last_processed_dates_table_name()

        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute(
                    f"""
                    INSERT INTO bicam_metadata.{table_name}
                    (data_type, last_processed_date, last_total_count, updated_at)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (data_type) DO UPDATE SET
                        last_processed_date = $2,
                        last_total_count = $3,
                        updated_at = $4
                    """,
                    data_type,
                    date,
                    total_count,
                    datetime.now(UTC),
                )
                return True
        except Exception as e:
            logger.warning(f"Failed to update last processed date: {e}")
            return False

    async def finalize_incremental_fetch(
        self, data_type: str, latest_date: str | None
    ) -> bool:
        """Finalize incremental fetch by updating the last processed date."""
        if not latest_date:
            logger.warning("No latest date provided for finalization")
            return False

        return await self.update_last_processed_date(data_type, latest_date)

    def _format_date_for_api(self, date: str | None) -> str | None:
        """Format date for Congressional API (ISO8601: YYYY-MM-DDTHH:MM:SSZ)."""
        if not date:
            return None
        # If already in correct format, return as-is
        if "T" in date and date.endswith("Z"):
            return date
        # If in YYYY-MM-DD format, convert to YYYY-MM-DDT00:00:00Z
        if len(date) == 10 and date.count("-") == 2:
            return f"{date}T00:00:00Z"
        # Handle various datetime formats
        try:
            # Try parsing as ISO8601 with or without timezone
            dt = None
            if "T" in date:
                # Remove Z or timezone info for parsing
                clean = date.replace("Z", "").replace("+00:00", "")
                try:
                    dt = datetime.fromisoformat(clean)
                except Exception:
                    dt = None
            if not dt:
                dt = datetime.strptime(date, "%Y-%m-%d")
            # Always output as UTC Zulu
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        except Exception:
            return date

    def _extract_latest_date(self, data_batch: list[dict[str, Any]]) -> str | None:
        """Extract the latest date from a batch of data. Can be overridden by subclasses."""
        if not data_batch:
            return None

        # Default implementation - subclasses can override
        latest_date = None
        for item in data_batch:
            # Try common date field names
            for date_field in ["lastModified", "publishedDate", "updated", "date"]:
                if date_field in item and item[date_field]:
                    item_date = item[date_field]
                    if not latest_date or item_date > latest_date:
                        latest_date = item_date

        return latest_date
