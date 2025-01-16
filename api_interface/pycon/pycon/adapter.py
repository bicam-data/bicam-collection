import asyncio
import random
from datetime import datetime, timedelta, timezone
import aiohttp
import logging
import asyncpg
import itertools
from collections import deque
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse
from pycon.api_models import Result, ErrorResult

class RestAdapter:
    def __init__(
        self,
        hostname: str = "api.congress.gov",
        api_key: str | list | None = None,
        ver: str = "v3",
        ssl_verify: bool = True,
        logger: logging.Logger = logging.Logger(""),
        session: aiohttp.ClientSession | None = None,
        db_pool: asyncpg.Pool | None = None,
        max_concurrent_requests: int = 22
    ):
        self._logger = logger or logging.getLogger(__name__)
        self.url = f"https://{hostname}/{ver}" if ver else f"https://{hostname}"
        self.api_keys = api_key if isinstance(api_key, list) else [api_key]
        self.api_key_cycle = itertools.cycle(self.api_keys)
        self.current_api_key = next(self.api_key_cycle)
        self.semaphore = asyncio.Semaphore(max_concurrent_requests)
        self.db_pool = db_pool

        self.remaining_requests = {key: 5000 for key in self.api_keys}
        self.request_times = {key: deque(maxlen=5000) for key in self.api_keys}
        self.retry_after = {key: datetime.min.replace(tzinfo=timezone.utc) for key in self.api_keys}
        self._ssl_verify = ssl_verify
        self.session = session
        self.error_urls = []

        self._logger.setLevel(logging.DEBUG)

    async def _do(self, endpoint: str, http_method: str, override=False, **kwargs):
        if not override:
            full_url = self.url + endpoint
        else:
            full_url = endpoint

        parsed_url = urlparse(full_url)
        query_params = parse_qs(parsed_url.query)
        query_params['format'] = ['json']

        max_retries = 10
        retry_count = 0

        async def request_with_retry():
            nonlocal retry_count
            while retry_count < max_retries:
                try:
                    self.current_api_key = await self._get_best_api_key()

                    query_params['api_key'] = [self.current_api_key]
                    updated_url = urlunparse(
                        parsed_url._replace(query=urlencode(query_params, doseq=True))
                    )

                    async with self.session.request(
                        method=http_method,
                        url=updated_url,
                        ssl=self._ssl_verify,
                        timeout=80,
                        **kwargs,
                    ) as response:
                        await self._update_rate_limit_info(self.current_api_key, response.headers)

                        response.raise_for_status()

                        if response.content_type == "application/json":
                            data_out = await response.json()
                        else:
                            self._logger.error(f"Unexpected response type. Status code: {response.status}, Response: {await response.text()}")
                            retry_count += 1
                            continue

                        return Result(response.status, data=data_out, message=response.reason, headers=response.headers)

                except aiohttp.ClientResponseError as e:
                    if e.status == 429:
                        retry_after = int(e.headers.get("Retry-After", 300))
                        self._logger.warning(f"Rate limit hit for key {self.current_api_key}. Retry-After: {retry_after} seconds.")
                        self.retry_after[self.current_api_key] = datetime.now(timezone.utc) + timedelta(seconds=retry_after)
                        self.remaining_requests[self.current_api_key] = 0
                        continue  # This will immediately try the next best key
                    
                    if e.status in [500, 502, 503, 504]:
                        self._logger.warning(f"Server error {e.status}. Retrying... {full_url}")
                        await asyncio.sleep(15)
                    else:
                        self._logger.error(f"ClientResponseError: {e}")
                        await self._write_error_to_database(updated_url, str(e))
                        return ErrorResult(updated_url, str(e))

                except Exception as e:
                    self._logger.error(f"Unexpected error: {str(e)}", exc_info=True)
                    await self._write_error_to_database(updated_url, str(e))
                    return ErrorResult(updated_url, str(e))

                retry_count += 1

            error_message = "Max retries reached"
            self._logger.error(f"Exhausted all retries for URL: {updated_url}")
            await self._write_error_to_database(updated_url, error_message)
            return ErrorResult(updated_url, error_message)

        async with self.semaphore:
            return await self._retry_with_backoff(request_with_retry)
    async def _get_best_api_key(self):
        current_time = datetime.now(timezone.utc)
        available_keys = [key for key, requests in self.remaining_requests.items() if requests > 0 and current_time >= self.retry_after[key]]
        
        if available_keys:
            return max(available_keys, key=lambda k: self.remaining_requests[k])
        else:
            # Find the key with the earliest retry_after time
            next_key = min(self.api_keys, key=lambda k: self.retry_after[k])
            sleep_time = (self.retry_after[next_key] - current_time).total_seconds()
            if sleep_time > 0:
                self._logger.debug(f"All keys exhausted. Sleeping for {sleep_time} seconds...")
                await asyncio.sleep(sleep_time)
            return next_key

    async def _update_rate_limit_info(self, key, headers):
        if "x-ratelimit-remaining" in headers:
            self.remaining_requests[key] = int(headers["x-ratelimit-remaining"])
        self.request_times[key].append(datetime.now(timezone.utc))

    def check_remaining_requests(self, api_key):
        return self.remaining_requests.get(api_key, 0)

    async def _retry_with_backoff(self, coroutine, max_retries=5, base_delay=1, max_delay=60):
        for attempt in range(max_retries):
            try:
                return await coroutine()
            except aiohttp.ClientConnectorError as e:
                if "Temporary failure in name resolution" in str(e):
                    if attempt == max_retries - 1:
                        raise
                    delay = min(base_delay * (2 ** attempt) + random.uniform(0, 1), max_delay)
                    self._logger.warning(f"Temporary DNS resolution failure. Retrying in {delay:.2f} seconds...")
                    await asyncio.sleep(delay)
                else:
                    raise

    async def _write_error_to_database(self, url: str, error: str):
        if not self.db_pool:
            self._logger.error("Database pool not provided. Cannot write error to database.")
            return

        try:
            timestamp = datetime.now(timezone.utc).isoformat()
            async with self.db_pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO __metadata.congressional_errors (url, error, timestamp)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (error, url, timestamp) DO NOTHING
                    """,
                    url, error, str(timestamp)
                )
        except Exception as e:
            self._logger.error(f"Error writing to database: {str(e)}")

    async def retrieve(self, endpoint: str, override=False, **kwargs):
        response = await self._do(endpoint, "GET", override, **kwargs)
        if isinstance(response, Result) and 'x-ratelimit-remaining' in response.headers:
            self.remaining_requests[self.current_api_key] = int(response.headers['x-ratelimit-remaining'])
        else:
            pass
        return response

    async def delete(self, endpoint: str, override=False) -> Result:
        return await self._do(endpoint, "DELETE", override)

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()