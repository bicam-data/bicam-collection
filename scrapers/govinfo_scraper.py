import argparse
import asyncio
import logging
from datetime import datetime, timedelta, timezone
import time
from typing import Any, AsyncIterator, Dict, List, Optional
from dotenv import load_dotenv
import aiohttp
import yaml
import json
from collections import defaultdict, deque
from http.client import HTTPException
import os
import csv
import aiofiles
import asyncpg
import random
import sqlite3

from pycon.govinfo.govinfo_abstractions import GovInfoAPI
from pycon.govinfo.govinfo_internal_models import *
from pycon.models import ErrorResult
from pycon.retriever_class import Retriever

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class RetryManager:
    def __init__(self, max_retries=5, base_delay=1, max_delay=60):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay

    async def retry_with_backoff(self, coroutine, *args, **kwargs):
        for attempt in range(self.max_retries):
            try:
                if 'endpoint' in kwargs:
                    kwargs['endpoint'] = str(kwargs['endpoint'])
                if args and isinstance(args[0], (str, bytes)):
                    args = (str(args[0]),) + args[1:]
                return await coroutine(*args, **kwargs)
            except Exception as e:
                if attempt == self.max_retries - 1:
                    raise
                delay = min(self.base_delay * 2 ** attempt, self.max_delay)
                logger.warning(f"Attempt {attempt + 1} failed. Retrying in {delay} seconds: {str(e)}")
                await asyncio.sleep(delay)

    @staticmethod
    async def wait_for_rate_limit(rate_limit_reset):
        now = datetime.now(timezone.utc)
        wait_time = (rate_limit_reset - now).total_seconds()
        if wait_time > 0:
            await asyncio.sleep(wait_time)

    @staticmethod
    def is_rate_limit_error(error):
        return isinstance(error, HTTPException) and error.status_code == 429

class DatabaseManager:
    def __init__(self, env_path: str = ".env"):
        load_dotenv(dotenv_path=env_path)
        self.db_config = {
            "database": os.getenv("POSTGRESQL_DB"),
            "user": os.getenv("POSTGRESQL_USER"),
            "password": os.getenv("POSTGRESQL_PASSWORD"),
            "host": os.getenv("POSTGRESQL_HOST"),
            "port": os.getenv("POSTGRESQL_PORT"),
        }
        self.pool = None

    async def create_pool(self):
        self.pool = await asyncpg.create_pool(**self.db_config)

    async def close_pool(self):
        if self.pool:
            await self.pool.close()

    async def get_last_processed_date(self, data_type: str) -> Optional[str]:
        async with self.pool.acquire() as conn:
            result = await conn.fetchval("""
                SELECT last_processed_date
                FROM __metadata.govinfo_last_processed_dates
                WHERE data_type = $1
            """, data_type)
        return result

    async def update_last_processed_date(self, data_type: str, last_date: str):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO __metadata.govinfo_last_processed_dates (data_type, last_processed_date)
                VALUES ($1, $2)
                ON CONFLICT (data_type) DO UPDATE
                SET last_processed_date = EXCLUDED.last_processed_date
            """, data_type, last_date)
        logger.info(f"Updated last processed date for {data_type}: {last_date}")

    async def write_error_to_database(self, url: str, error_message: str, data_type: str):
        timestamp = datetime.now(timezone.utc).isoformat()
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO __metadata.govinfo_errors (url, error, data_type, timestamp)
                VALUES ($1, $2, $3, $4)
            """, url, error_message, data_type, timestamp)

class CSVExporter:
    def __init__(self, base_directory: str, max_concurrent_files=100):
        self.base_directory = base_directory
        os.makedirs(base_directory, exist_ok=True)
        self.semaphore = asyncio.Semaphore(max_concurrent_files)
        self.field_cache = {}  # Cache to store fieldnames for each data type

    async def export_to_csv(self, data_type: str, items: List[Dict]):
        if not items:
            logger.info(f"No items to export for {data_type}")
            return

        filepath = os.path.join(self.base_directory, f"{data_type}.csv")
        file_exists = os.path.exists(filepath)

        # Get or create fieldnames for this data type
        if data_type not in self.field_cache:
            self.field_cache[data_type] = self._get_all_possible_fields(items)
        fieldnames = self.field_cache[data_type]

        # Process items to ensure consistent structure
        processed_items = []
        for item in items:
            processed_item = {}
            for field in fieldnames:
                # Get the value, defaulting to None if not present
                value = item.get(field)
                
                # Handle special cases
                if isinstance(value, (list, dict)):
                    processed_item[field] = json.dumps(value) if value else None
                elif value == "":
                    processed_item[field] = None
                else:
                    processed_item[field] = value
            processed_items.append(processed_item)

        async with self.semaphore:
            try:
                async with aiofiles.open(filepath, 'a' if file_exists else 'w', newline='') as f:
                    # Create a temporary buffer for writing
                    output = []
                    if not file_exists:
                        output.append(','.join(f'"{field}"' for field in fieldnames) + '\n')
                    
                    for item in processed_items:
                        row = []
                        for field in fieldnames:
                            value = item.get(field)
                            if value is None:
                                row.append('')
                            else:
                                # Properly escape and quote values
                                value = str(value).replace('"', '""')
                                row.append(f'"{value}"')
                        output.append(','.join(row) + '\n')
                    
                    # Write everything at once
                    await f.write(''.join(output))

                logger.info(f"Exported {len(processed_items)} items to CSV: {filepath}")
            except Exception as e:
                logger.error(f"Error writing to CSV {filepath}: {str(e)}")
                raise

    def _get_all_possible_fields(self, items: List[Dict]) -> List[str]:
        """Get a complete, sorted list of all possible fields from the items."""
        fields = set()
        for item in items:
            fields.update(item.keys())
        return sorted(list(fields))

class APIManager:
    def __init__(self, db_manager, env_path=".env.info"):
        self.session_pool = {}
        self.all_api_keys = self._load_api_keys(env_path)
        if not self.all_api_keys:
            raise ValueError("No API keys found. Please check your environment file.")
        self.govinfo_instances = {}
        self.db_manager = db_manager
        self.semaphore = asyncio.Semaphore(10)  # Limit concurrent requests
        self.data_type_stats = {}
        self.api_key_index = 0
        self.logger = logging.getLogger(__name__)
        self.api_key_status = {key: {"sleeping": False, "sleep_until": None} for key in self.all_api_keys}

    def _load_api_keys(self, env_path):
        load_dotenv(env_path)
        api_keys = []
        for key, value in os.environ.items():
            if key.startswith("GOVINFO_API_KEY"):
                api_keys.append(value)
        return api_keys

    async def set_api_key_sleeping(self, api_key, sleep_duration):
        self.api_key_status[api_key]["sleeping"] = True
        self.api_key_status[api_key]["sleep_until"] = datetime.now(timezone.utc) + timedelta(seconds=sleep_duration)

    async def wake_up_api_key(self, api_key):
        self.api_key_status[api_key]["sleeping"] = False
        self.api_key_status[api_key]["sleep_until"] = None

    async def get_active_api_keys(self):
        now = datetime.now(timezone.utc)
        active_keys = []
        for key, status in self.api_key_status.items():
            if not status["sleeping"] or (status["sleep_until"] and status["sleep_until"] <= now):
                active_keys.append(key)
                if status["sleeping"]:
                    await self.wake_up_api_key(key)
        return active_keys


    async def fetch_full_item(self, item):
        max_retries = 1
        for attempt in range(max_retries):
            async with self.semaphore:
                try:
                    if hasattr(item, 'package_url'):
                        full_item_result = await item._adapter.retrieve(item.package_url, override=True)
                        if isinstance(full_item_result, ErrorResult):
                            logger.error(f"Error fetching full item details: {full_item_result.error_message}")
                            if attempt == max_retries - 1:
                                await self.db_manager.write_error_to_database(
                                    url=item.package_url,
                                    error_message=full_item_result.error_message,
                                    data_type=type(item).__name__
                                )
                            else:
                                await self.exponential_backoff(attempt)
                                continue

                        # Instantiate the appropriate object based on the item type
                        item_class = type(item)
                        full_item = item_class(data=full_item_result.data, _pagination=item._pagination, _adapter=item._adapter)

                        # Check for 'MISSING' fields in full_item and fill from original item if available
                        for attr in dir(full_item):
                            if not attr.startswith('_') and getattr(full_item, attr, None) == 'MISSING':
                                original_value = getattr(item, attr, None)
                                if original_value is not None and original_value != 'MISSING':
                                    setattr(full_item, attr, original_value)

                        return full_item
                    else:
                        logger.warning(f"Item of type {type(item).__name__} does not have package_url attribute")
                        return item
                except Exception as e:
                    logger.error(f"Error fetching full item details (attempt {attempt + 1}): {str(e)}", exc_info=True)
                    if attempt == max_retries - 1:
                        await self.db_manager.write_error_to_database(
                            url=getattr(item, 'package_url', 'unknown'),
                            error_message=str(e),
                            data_type=type(item).__name__
                        )
                    else:
                        await self.exponential_backoff(attempt)
        return None

    @staticmethod
    async def exponential_backoff(attempt, max_delay=60):
        delay = min(2**attempt + random.uniform(0, 1), max_delay)
        await asyncio.sleep(delay)

    async def get_data(self, data_type: str, **kwargs) -> AsyncIterator[List[Any]]:
        govinfo = self.govinfo_instances[data_type]
        method_name = f"get_bulk_{data_type}"
        method = getattr(govinfo, method_name)
        total_count = None
        processed_count = 0
        retry_manager = RetryManager()  # Create an instance of RetryManager

        async def fetch_data():
            return await method(**kwargs)

        while True:
            try:
                items = await retry_manager.retry_with_backoff(fetch_data)
                logger.info(f"Fetched {len(items)} items for {data_type}")


                if isinstance(items, ErrorResult):
                    yield items
                    break

                if not items:
                    break

                if total_count is None and items[0]._pagination.get('count'):
                    total_count = items[0]._pagination['count']
                    logger.info(f"Total items for {data_type}: {total_count}")

                full_items = []
                if len(items) > 1000:
                    logger.warning(f"More than 1000 items for {data_type}: {len(items)}")
                    items = items[:1000]
                for item in items:  # Limit to 1000 items
                    logger.debug(f"Fetching full item: {item}")
                    try:
                        full_item = await self.fetch_full_item(item)
                        if full_item:
                            full_items.append(full_item)
                    except Exception as e:
                        logger.error(f"Error fetching full item details for {data_type}: {str(e)}", exc_info=True)

                if full_items:
                    yield full_items
                    processed_count += len(full_items)
                    if total_count:
                        logger.info(f"Progress for {data_type}: {processed_count}/{total_count} ({processed_count/total_count*100:.2f}%)")

                if not items[0]._pagination.get('next') or len(items) > 1000:
                    break

                kwargs['next_url'] = items[0]._pagination['next']

            except Exception as e:
                logger.error(f"Error in get_data for {data_type}: {str(e)}", exc_info=True)
                yield ErrorResult(url=kwargs.get('next_url', f"bulk_{data_type}"), error_message=str(e))
                await asyncio.sleep(60)  # Wait for a minute before retrying

            if 'next_url' not in kwargs:
                break


    async def create_govinfo_instance(self, data_type, api_keys):
        session = await self.create_session(data_type)
        self.govinfo_instances[data_type] = GovInfoAPI(api_keys, session=session, db_pool=self.db_manager.pool)
        self.data_type_stats[data_type] = {
            "items_processed": 0,
            "items_remaining": 0,
            "start_time": datetime.now(timezone.utc)
        }
        logger.info(f"Created GovInfo instance for {data_type}")
        return self.govinfo_instances[data_type]

    async def create_session(self, data_type):
        if data_type not in self.session_pool:
            self.session_pool[data_type] = aiohttp.ClientSession(
                raise_for_status=True,
                timeout=aiohttp.ClientTimeout(total=300)
            )
        return self.session_pool[data_type]

    async def switch_api_key(self, data_type):
        self.api_key_index = (self.api_key_index + 1) % len(self.all_api_keys)
        new_key = self.all_api_keys[self.api_key_index]
        self.govinfo_instances[data_type]._adapter.api_key = new_key
        logger.info(f"Switched to API key: {new_key[:5]}... for {data_type}")

    async def check_remaining_requests(self):
        remaining = getattr(self.govinfo_instances[next(iter(self.govinfo_instances))]._adapter, 'remaining_requests', None)
        if remaining is not None:
            return remaining

    async def close_all_sessions(self):
        for session in self.session_pool.values():
            if not session.closed:
                await session.close()
        self.session_pool.clear()

    async def is_rate_limit_exceeded(self):
        current_time = datetime.now()
        while self.request_times and (current_time - self.request_times[0]) > timedelta(hours=1):
            print(f"{len(self.request_times)} requests in the last hour. Removing {self.request_times[0]}")
            self.request_times.popleft()

        return len(self.request_times) >= 4500

    async def wait_for_rate_limit(self):
        while await self.is_rate_limit_exceeded():
            time_to_wait = (datetime.now() - self.request_times[0]).total_seconds()
            sleep_time = max(1, 3600 - time_to_wait + 1)
            print(f"Rate limit exceeded. Sleeping for {sleep_time} seconds.")
            await asyncio.sleep(sleep_time)

class ETLCoordinator:
    def __init__(
        self,
        api_manager,
        db_manager,
        csv_exporter,
        config_file: str,
        num_workers: int = 10,
        retry_manager: RetryManager = None
    ):
        self.api_manager = api_manager
        self.db_manager = db_manager
        self.csv_exporter = csv_exporter
        self.config = self.load_config(config_file)
        self.num_workers = num_workers
        self.retry_manager = retry_manager or RetryManager()
        self.data_type_queue = asyncio.Queue()

    def load_config(self, config_file: str) -> Dict:
        with open(config_file, 'r') as f:
            return yaml.safe_load(f)

    def get_main_data_types(self) -> List[str]:
        return [
            data_type
            for data_type, config in self.config.items()
            if config.get('main', False)
        ]

    async def initialize(self):
        await self.db_manager.create_pool()
        main_data_types = self.get_main_data_types()
        all_api_keys = self.api_manager.all_api_keys
        keys_per_type = max(1, len(all_api_keys) // len(main_data_types))

        for i, data_type in enumerate(main_data_types):
            start_index = (i * keys_per_type) % len(all_api_keys)
            assigned_keys = all_api_keys[start_index : start_index + keys_per_type]
            if len(assigned_keys) < keys_per_type:
                assigned_keys += all_api_keys[: keys_per_type - len(assigned_keys)]

            await self.api_manager.create_govinfo_instance(data_type, assigned_keys)

        logger.info(f"Initialized govinfo instances for data types: {', '.join(main_data_types)}")

    async def run(self):
        try:
            await self.initialize()
            await self.process_all_data()
        except Exception as e:
            logger.error(f"An error occurred during the ETL process: {str(e)}", exc_info=True)
        finally:
            await self.cleanup()

    async def process_all_data(self):
        main_data_types = self.get_main_data_types()
        
        for data_type in main_data_types:
            await self.data_type_queue.put(data_type)

        workers = [asyncio.create_task(self.worker()) for _ in range(self.num_workers)]
        await self.data_type_queue.join()
        
        for worker in workers:
            worker.cancel()
        await asyncio.gather(*workers, return_exceptions=True)

    async def worker(self):
        while True:
            try:
                data_type = await self.data_type_queue.get()
                logger.info(f"Worker starting to process {data_type}")
                await self.process_data_type(data_type)
                logger.info(f"Worker finished processing {data_type}")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error processing {data_type}: {str(e)}", exc_info=True)
            finally:
                self.data_type_queue.task_done()

    async def process_data_type(self, data_type: str):
        """Process a single data type"""
        try:
            last_processed_date = await self.db_manager.get_last_processed_date(data_type)
            kwargs, start_date = await self._get_date_range(data_type, last_processed_date)
            
            data_generator = self.api_manager.get_data(data_type, **kwargs)
            
            while True:
                try:
                    items_batch = await self._fetch_batch_with_dynamic_timeout(data_generator)
                    if not items_batch:
                        logger.info(f"No more items to process for {data_type}")
                        break

                    await self.process_batch(data_type, items_batch)
                    
                    # Update the last processed date in the database
                    batch_latest_date = self._get_latest_date_from_batch(items_batch)
                    if batch_latest_date:
                        await self.db_manager.update_last_processed_date(data_type, batch_latest_date.isoformat())

                except StopAsyncIteration:
                    logger.info(f"Completed processing all items for {data_type}")
                    break
                except Exception as e:
                    logger.error(f"Error processing batch for {data_type}: {str(e)}")
                    await asyncio.sleep(60)  # Wait before retrying
                    continue

        except Exception as e:
            logger.error(f"Error processing data type {data_type}: {str(e)}")
            raise

    async def process_batch(self, data_type: str, items_batch: List[Any]):
        logger.info(f"Processing batch of {len(items_batch)} items for {data_type}")
        processed_items = []
        nested_data = defaultdict(list)

        for item in items_batch:
            try:
                processed_item = await self.process_item(item, data_type)
                if processed_item:
                    processed_items.append(processed_item)

                # Process nested fields
                nested_fields = self.config.get(data_type, {}).get('nested_fields', [])
                if nested_fields:
                    if isinstance(nested_fields, list):
                        for field in nested_fields:
                            field_name = field if isinstance(field, str) else next(iter(field))
                            if hasattr(item, field_name):
                                nested_value = getattr(item, field_name)
                                if nested_value is not None:
                                    prepared_data = self._prepare_nested_data(data_type, field_name, nested_value, processed_item)
                                    if prepared_data:
                                        nested_data[field_name].extend(prepared_data)

                # Process granules
                if hasattr(item, 'get_granules'):
                    await self.process_granules(data_type, item)

            except Exception as e:
                logger.error(f"Error processing item for {data_type}: {str(e)}")
                continue

        # Export processed items to CSV
        if processed_items:
            await self.csv_exporter.export_to_csv(data_type, processed_items)

        # Export nested data
        for nested_field, nested_items in nested_data.items():
            if nested_items:
                await self.csv_exporter.export_to_csv(f"{data_type}_{nested_field}", nested_items)

    def _get_latest_date_from_batch(self, items_batch: List[Any]) -> Optional[datetime]:
        date_fields = ["last_modified", "dateIssued", "date_issued"]
        for item in items_batch:
            for field in date_fields:
                date_str = getattr(item, field, None)
                if date_str:
                    try:
                        parsed_date = datetime.fromisoformat(date_str.rstrip('Z'))
                        if parsed_date.tzinfo is None:
                            parsed_date = parsed_date.replace(tzinfo=timezone.utc)
                        return parsed_date
                    except (ValueError, AttributeError) as e:
                        logger.warning(f"Error parsing date '{date_str}' for field '{field}': {str(e)}")
        return None

    async def process_granules(self, data_type: str, item: Any):
        """Process granules without state management"""
        granules_data_type = f"{data_type}_granules"
        granules_url = getattr(item, 'granules_url', None)

        if granules_url:
            try:
                async for granule in item.get_granules():
                    processed_granule = await self.process_item(granule, granules_data_type)

                    # Add parent item's ID to the granule
                    parent_id_fields = self.config.get(data_type, {}).get('id_fields', ['id'])
                    for id_field in parent_id_fields:
                        parent_id = getattr(item, id_field, None)
                        if parent_id:
                            processed_granule[f"parent_{id_field}"] = parent_id

                    # Process nested fields for the granule
                    await self.process_nested_fields(granules_data_type, granule, processed_granule)

                    # Export single granule to CSV
                    await self.csv_exporter.export_to_csv(granules_data_type, [processed_granule])

            except Exception as e:
                logger.error(f"Error processing granules for {data_type}: {str(e)}", exc_info=True)
                await self.db_manager.write_error_to_database(
                    url=granules_url,
                    error_message=str(e),
                    data_type=granules_data_type
                )
        else:
            logger.debug(f"No granules URL available for item of {data_type}")

    async def process_nested_fields(self, data_type: str, item: Any, processed_item: Dict[str, Any]):
        """Process nested fields without state management"""
        nested_fields = self.config.get(data_type, {}).get('nested_fields', [])
        parent_ids = self._get_parent_ids(item, data_type)
        
        if isinstance(nested_fields, list):
            for nested_field in nested_fields:
                if isinstance(nested_field, dict):
                    field_name = next(iter(nested_field))
                    nested_config = nested_field[field_name]
                else:
                    field_name = nested_field
                    nested_config = {}
                
                if hasattr(item, field_name):
                    nested_value = getattr(item, field_name)
                    if nested_value is not None:
                        nested_data_type = f"{data_type}_{field_name}"
                        processed_nested_data = self._prepare_nested_data_for_csv(
                            nested_value, 
                            parent_ids, 
                            nested_config
                        )
                        
                        if processed_nested_data:
                            await self.csv_exporter.export_to_csv(nested_data_type, processed_nested_data)

    async def cleanup(self):
        logger.info("Performing cleanup...")
        await self.api_manager.close_all_sessions()
        await self.db_manager.close_pool()
        logger.info("Cleanup complete.")

    def _prepare_nested_data(self, data_type: str, nested_field: str, nested_value: Any, parent_item: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Improved nested data preparation with better string list handling"""
        id_fields = self.config.get(data_type, {}).get('id_fields', ['id'])
        parent_ids = {field: parent_item.get(field) for field in id_fields if field in parent_item}

        if isinstance(nested_value, list):
            result = []
            for item in nested_value:
                if isinstance(item, (str, int, float, bool)):
                    # Handle primitive values in list
                    result.append({
                        **parent_ids,
                        nested_field: str(item)
                    })
                else:
                    # Handle objects in list
                    processed = self._process_nested_object(nested_field, item, [])
                    if processed:
                        result.append({**parent_ids, **processed})
            return result
        elif isinstance(nested_value, (str, int, float, bool)):
            # Handle single primitive value
            return [{**parent_ids, nested_field: str(nested_value)}]
        elif isinstance(nested_value, dict):
            # Handle single object
            processed = self._process_nested_object(nested_field, nested_value, [])
            return [{**parent_ids, **processed}] if processed else []

    def _process_nested_object(self, field_name: str, nested_obj: Dict, fields: List[str]) -> Dict[str, Any]:
        if fields:
            return {
                f"{field_name}_{k}": self._serialize_value(v)
                for k, v in vars(nested_obj).items() if k in fields
            }
        else:
            return {
                f"{field_name}_{k}": self._serialize_value(v)
                for k, v in vars(nested_obj).items()
            }

    def _prepare_nested_data_for_csv(self, nested_value: Any, parent_ids: Dict[str, Any], nested_config: Dict) -> List[Dict[str, Any]]:
        """
        Universal handler for nested data that creates appropriate CSV rows only if data exists.
        """
        processed_data = []
        
        try:
            if nested_value is None:
                return []
                
            # Handle lists (both primitive values and objects)
            if isinstance(nested_value, (list, tuple)):
                if not nested_value:  # Empty list
                    return []
                    
                for item in nested_value:
                    if isinstance(item, (str, int, float, bool)):
                        # Primitive value in list
                        processed_data.append({
                            **parent_ids,
                            'value': item
                        })
                    else:
                        # Object in list - only add if it has data
                        item_data = self._extract_fields(item)
                        if item_data:  # Only add if we got data
                            processed_data.append({
                                **parent_ids,
                                **item_data
                            })
                        
            # Handle single items (both primitive values and objects)
            else:
                if isinstance(nested_value, (str, int, float, bool)):
                    # Single primitive value
                    processed_data.append({
                        **parent_ids,
                        'value': nested_value
                    })
                else:
                    # Single object - only add if it has data
                    item_data = self._extract_fields(nested_value)
                    if item_data:  # Only add if we got data
                        processed_data.append({
                            **parent_ids,
                            **item_data
                        })
            
            return processed_data
            
        except Exception as e:
            logger.error(f"Error processing nested data: {str(e)}", exc_info=True)
            return []

    def _extract_fields(self, item: Any) -> Dict[str, Any]:
        """
        Extracts fields from any object type, returning empty dict if no valid data.
        """
        if hasattr(item, '__dict__'):
            data = {k: v for k, v in vars(item).items() 
                if v is not None and v != 'MISSING' and v != ''}
            return data if data else {}  # Return empty dict if no valid data
        elif isinstance(item, dict):
            data = {k: v for k, v in item.items() 
                if v is not None and v != 'MISSING' and v != ''}
            return data if data else {}  # Return empty dict if no valid data
        else:
            value = str(item)
            return {'value': value} if value.strip() else {}

    def _get_parent_ids(self, parent_item: Any, data_type: str) -> Dict[str, Any]:
        parent_id_fields = self.config.get(data_type, {}).get('id_fields', ['id'])
        return {
            field: getattr(parent_item, field)
            for field in parent_id_fields
            if hasattr(parent_item, field) and getattr(parent_item, field) is not None
        }

    def _serialize_value(self, value):
        if isinstance(value, (str, int, float, bool)) or value is None:
            return value
        elif isinstance(value, datetime):
            return value.isoformat()
        elif isinstance(value, list):
            return value  # Return the list as-is
        elif isinstance(value, dict):
            return json.dumps(value)  # Keep dictionaries as JSON strings
        else:
            return str(value)

    async def process_item(self, item: Any, data_type: str) -> Dict[str, Any]:
        result = {}
        fields = self.config.get(data_type, {}).get('fields', [])
        nested_fields = self.config.get(data_type, {}).get('nested_fields', [])

        for attr in fields:
            if hasattr(item, attr):
                value = getattr(item, attr)
                result[attr] = self._serialize_value(value)

        if isinstance(nested_fields, list):
            for nested_field in nested_fields:
                if isinstance(nested_field, dict):
                    field_name = next(iter(nested_field))
                    nested_config = nested_field[field_name]
                else:
                    field_name = nested_field
                    nested_config = {}
                
                if hasattr(item, field_name):
                    nested_value = getattr(item, field_name)
                    result.update(self._process_nested_field(field_name, nested_value, data_type, nested_config))
        elif isinstance(nested_fields, dict):
            for nested_field, nested_config in nested_fields.items():
                if hasattr(item, nested_field):
                    nested_value = getattr(item, nested_field)
                    result.update(self._process_nested_field(nested_field, nested_value, data_type, nested_config))

        return result

    def _process_nested_field(self, field_name: str, nested_value: Any, parent_data_type: str, nested_config: Optional[Dict] = None) -> Dict[str, Any]:
        nested_config = nested_config or {}
        fields = nested_config.get('fields', [])
        
        if isinstance(nested_value, (list, tuple)):
            if all(isinstance(item, (str, int, float, bool)) for item in nested_value):
                return {field_name: json.dumps(nested_value)}
            else:
                return self._process_nested_object_list(field_name, nested_value, fields)
        elif isinstance(nested_value, dict):
            return self._process_nested_object(field_name, nested_value, fields)
        else:
            return {field_name: self._serialize_value(nested_value)}

    def _process_nested_object(self, field_name: str, nested_obj: Dict, fields: List[str]) -> Dict[str, Any]:
        if fields:
            return {
                f"{field_name}_{k}": self._serialize_value(v)
                for k, v in vars(nested_obj).items() if k in fields
            }
        else:
            return {
                f"{field_name}_{k}": self._serialize_value(v)
                for k, v in vars(nested_obj).items()
            }

    def _process_nested_object_list(self, field_name: str, nested_list: List, fields: List[str]) -> Dict[str, Any]:
        result = {}
        for i, obj in enumerate(nested_list):
            if isinstance(obj, dict):
                if fields:
                    for k in fields:
                        if k in obj:
                            result[f"{field_name}_{i+1}_{k}"] = self._serialize_value(obj[k])
                else:
                    for k, v in obj.items():
                        result[f"{field_name}_{i+1}_{k}"] = self._serialize_value(v)
            else:
                result[f"{field_name}_{i+1}"] = self._serialize_value(obj)
        return result
    
    def _granule_matches(self, granule: Any, last_processed_granule: Dict[str, Any]) -> bool:
        if not last_processed_granule:
            return False
        return all(getattr(granule, k, None) == v for k, v in last_processed_granule.items())

    def _save_progress(self, data_type: str, kwargs: Dict, start_date: Optional[datetime],
                      overall_latest_date: Optional[datetime], resume_state: Dict):
        """Synchronous version of save progress"""
        message = {
            'kwargs': kwargs,
            'start_date': start_date.isoformat() if start_date else None,
            'overall_latest_date': overall_latest_date.isoformat() if overall_latest_date else None,
            'resume_state': resume_state
        }
        self.message_queue.push(data_type, message)

    def _save_item_progress(self, data_type: str, index: int, total: int):
        message = {
            'type': 'item',
            'index': index,
            'total': total
        }
        self.message_queue.push(data_type, message)
        logging.debug(f"Saved progress for {data_type}: item {index + 1}/{total}")

    def _save_granule_progress(self, data_type: str, item_index: int, item_total: int, granule_count: int):
        message = {
            'type': 'granule',
            'item_index': item_index,
            'item_total': item_total,
            'granule_count': granule_count
        }
        self.message_queue.push(f"{data_type}_granules", message)
        logging.debug(f"Saved progress for {data_type}: item {item_index + 1}/{item_total}, granules processed: {granule_count}")

    async def check_csv_consistency(self, data_type: str) -> Optional[Dict[str, Any]]:
        last_exported_item = self.message_queue.get_last_exported_item(data_type)
        if not last_exported_item:
            return None

        csv_path = os.path.join(self.csv_exporter.base_directory, f"{data_type}.csv")
        if not os.path.exists(csv_path):
            return None

        with open(csv_path, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reversed(list(reader)):
                if all(row[k] == str(v) for k, v in last_exported_item.items() if k in row):
                    return last_exported_item

        return None

    def _item_matches(self, item: Any, last_exported_item: Dict[str, Any]) -> bool:
        if not last_exported_item:
            return False
        return all(getattr(item, k, None) == v for k, v in last_exported_item.items())

    @staticmethod
    def format_date(date: datetime) -> str:
        return date.strftime("%Y-%m-%dT%H:%M:%SZ")

    def _get_item_date(self, item: Any) -> datetime:
        date_fields = ["last_modified", "dateIssued", "date_issued"]
        for field in date_fields:
            date_str = getattr(item, field, None)
            if date_str:
                try:
                    parsed_date = datetime.fromisoformat(date_str.rstrip('Z'))
                    if parsed_date.tzinfo is None:
                        parsed_date = parsed_date.replace(tzinfo=timezone.utc)
                    return parsed_date
                except (ValueError, AttributeError) as e:
                    logger.warning(f"Error parsing date '{date_str}' for field '{field}': {str(e)}")
        return datetime.now(timezone.utc)

    async def _get_date_range(self, data_type: str, last_processed_date: Optional[str]):
        end_date = datetime.now(tz=timezone.utc)
        overlap_window = timedelta(hours=1)

        if last_processed_date:
            # Remove the 'Z' and add UTC timezone
            start_date = datetime.fromisoformat(last_processed_date.rstrip('Z')).replace(tzinfo=timezone.utc) - overlap_window
        else:
            start_date = datetime(1789, 1, 1, tzinfo=timezone.utc)

        latest_date_in_batch = start_date

        kwargs = {
            "start_date": self.format_date(start_date),
            "end_date": self.format_date(end_date)
        }

        return kwargs, latest_date_in_batch

    async def _fetch_batch_with_dynamic_timeout(self, data_generator, base_timeout=3600):
        while True:
            active_keys = await self.api_manager.get_active_api_keys()
            if not active_keys:
                sleep_time = min(status["sleep_until"] for status in self.api_manager.api_key_status.values() if status["sleeping"])
                wait_time = (sleep_time - datetime.now(timezone.utc)).total_seconds()
                logger.info(f"All API keys are sleeping. Waiting for {wait_time} seconds.")
                await asyncio.sleep(wait_time)
            else:
                try:
                    return await asyncio.wait_for(data_generator.__anext__(), timeout=base_timeout)
                except asyncio.TimeoutError:
                    logger.warning("Timeout occurred, but some API keys might still be active. Retrying...")

async def main():
    parser = argparse.ArgumentParser(description="GovInfo ETL Script")
    args = parser.parse_args()

    load_dotenv(".env.info")
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)

    db_manager = DatabaseManager()
    api_manager = APIManager(db_manager)
    csv_exporter = CSVExporter("/mnt/big_data/database-congress/api-govinfo")
    config_file = "govinfo_config.yaml"
    retry_manager = RetryManager()

    etl_coordinator = ETLCoordinator(
        api_manager, 
        db_manager, 
        csv_exporter, 
        config_file, 
        retry_manager=retry_manager
    )

    try:
        await etl_coordinator.run()
    except Exception as e:
        logger.error(f"An error occurred during the ETL process: {str(e)}", exc_info=True)
    finally:
        await etl_coordinator.cleanup()

if __name__ == "__main__":
    asyncio.run(main())