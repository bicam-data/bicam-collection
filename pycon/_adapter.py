import asyncio
import itertools
import json
import logging
import os
from collections import deque
from datetime import datetime, timezone
import time
import asyncpg
import dateutil.parser

import aiohttp
import urllib3
from pycon.exceptions import PyCongressException

from pycon.models import Result, ErrorResult



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
        self.url = f"https://{hostname}/{ver}"
        self.api_keys = api_key if isinstance(api_key, list) else [api_key]
        self.api_key_cycle = itertools.cycle(self.api_keys)
        self.current_api_key = next(self.api_key_cycle)
        self.semaphore = asyncio.Semaphore(max_concurrent_requests)
        self.db_pool = db_pool

        self.remaining_requests = {key: 5000 for key in self.api_keys}
        self.request_times = {key: deque(maxlen=5000) for key in self.api_keys}
        self._ssl_verify = ssl_verify
        if not ssl_verify:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        self.session = session
        self.error_urls = []

        self._logger.setLevel(logging.DEBUG)

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()

    async def _do(self, endpoint: str, http_method: str, override=False, **kwargs):
        if not override:
            full_url = self.url + endpoint
        else:
            full_url = endpoint

        if "params" in kwargs:
            kwargs["params"]["format"] = "json"
        else:
            kwargs["params"] = {"format": "json"}

        log_line_pre = f"method={http_method}, url={full_url}"
        log_line_post = ", ".join((log_line_pre, "success={}, status_code={}, message={}"))

        max_retries = 10
        retry_count = 0
        all_keys_exhausted = False

        async with self.semaphore:
            while True:  # Keep trying until we get a successful response or hit max retries
                try:
                    self.current_api_key = await self._get_best_api_key()
                    if all_keys_exhausted:
                        self._logger.warning("All keys were exhausted. Resuming with available key.")
                        all_keys_exhausted = False

                    headers = {"x-api-key": self.current_api_key}

                    self._logger.debug(msg=log_line_pre)

                    async with self.session.request(
                        method=http_method,
                        url=full_url,
                        ssl=self._ssl_verify,
                        headers=headers,
                        timeout=80,
                        **kwargs,
                    ) as response:
                        await self._update_rate_limit_info(self.current_api_key, response.headers)

                        if response.status == 429:
                            retry_after = int(response.headers.get("Retry-After", 300))
                            if not any(self.remaining_requests.values()):
                                all_keys_exhausted = True
                                self._logger.warning(f"All API keys exhausted. Waiting for {retry_after} seconds before retrying.")
                            await asyncio.sleep(retry_after)
                            continue  # Try again with the next key without incrementing retry_count

                        response.raise_for_status()

                        if response.content_type == "application/json":
                            data_out = await response.json()
                        else:
                            self._logger.error(f"Unexpected response type. Status code: {response.status}, Response: {await response.text()}")
                            retry_count += 1
                            if retry_count >= max_retries:
                                break
                            continue

                        is_success = 200 <= response.status <= 299
                        if is_success:
                            self._logger.debug(msg=log_line_post.format(is_success, response.status, response.reason))
                            return Result(response.status, data=data_out, message=response.reason, headers=response.headers)

                except aiohttp.ClientResponseError as e:
                    if e.status == 429:
                        if not any(self.remaining_requests.values()):
                            all_keys_exhausted = True
                            self._logger.warning("All API keys exhausted. Waiting before retrying.")
                        continue  # Try again with the next key without incrementing retry_count
                    elif e.status in [500, 502, 503, 504]:
                        self._logger.warning(f"Server error {e.status}. Retrying...")
                        await asyncio.sleep(15)
                        retry_count += 1
                        if retry_count >= max_retries:
                            break
                        continue
                    else:
                        self._logger.error(f"ClientResponseError: {e}")
                        return ErrorResult(full_url, str(e))

                except aiohttp.ClientError as e:
                    self._logger.error(f"ClientError with key {self.current_api_key}: {str(e)}", exc_info=True)
                    retry_count += 1
                    if retry_count >= max_retries:
                        break
                    continue

                except Exception as e:
                    self._logger.error(f"Unexpected error: {str(e)}", exc_info=True)
                    return ErrorResult(full_url, str(e))

            # If we've exhausted all retries without success
            error_message = "Max retries reached"
            self._logger.error(f"Exhausted all retries for URL: {full_url}")
            await self._write_error_to_database(full_url, error_message)
            return ErrorResult(full_url, error_message)

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
                    url, error, timestamp
                )
        except Exception as e:
            self._logger.error(f"Error writing to database: {str(e)}")


    async def _get_best_api_key(self):
        while True:
            current_time = datetime.now(timezone.utc)
            available_keys = [key for key, requests in self.remaining_requests.items() if requests > 0]
            
            if available_keys:
                return max(available_keys, key=lambda k: self.remaining_requests[k])
            else:
                next_available_times = [time.replace(tzinfo=timezone.utc) for key in self.api_keys for time in self.request_times[key] if self.request_times[key]]
                if not next_available_times:
                    sleep_time = 60
                else:
                    next_available_time = min(next_available_times)
                    sleep_time = max((next_available_time - current_time).total_seconds(), 0) + 1

                self._logger.debug(f"All keys exhausted. Sleeping for {sleep_time} seconds...")
                await asyncio.sleep(sleep_time)
                
    def add_api_key(self, new_key):
            if new_key not in self.api_keys:
                self.api_keys.append(new_key)
                self.api_key_cycle = itertools.cycle(self.api_keys)
                self.remaining_requests[new_key] = 5000
                self.request_times[new_key] = deque(maxlen=5000)
                self._logger.warning(f"Added new API key: {new_key}")
            else:
                self._logger.warning(f"API key {new_key} already exists")

    async def _update_rate_limit_info(self, key, headers):
        if "x-ratelimit-remaining" in headers:
            self.remaining_requests[key] = int(headers["x-ratelimit-remaining"])
        self.request_times[key].append(datetime.now(timezone.utc))

    def check_remaining_requests(self, key):
        return self.remaining_requests.get(key, "unknown")

    async def retrieve(self, endpoint: str, override=False, **kwargs):
        self._logger.info(f"Accessing URL: {endpoint}")
        return await self._do(endpoint, "GET", override, **kwargs)

    async def delete(self, endpoint: str, override=False) -> Result:
        return await self._do(endpoint, "DELETE", override)