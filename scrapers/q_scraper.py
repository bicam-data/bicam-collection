import argparse
from http.client import HTTPException
import random
import sqlite3
import json
import asyncio
import csv
import os
import time
import aiohttp
import aiofiles
import asyncpg
from dotenv import load_dotenv
import yaml
import logging
from typing import AsyncIterator, Dict, Any, List, Optional
from datetime import datetime, timezone, timedelta
from dateutil import parser
from pycon.api_models import ErrorResult
from pycon.congress.abstractions import PyCongress

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class APIManager:
    """
    Manages API connections, rate limiting, and data retrieval from the Congressional API.
    
    This class handles:
    - API key rotation and management
    - Session pooling
    - Rate limit handling
    - Concurrent request management
    
    Attributes:
        session_pool (Dict): Pool of aiohttp sessions for different data types
        all_api_keys (List[str]): List of available API keys
        congress_instances (Dict): Dictionary of PyCongress instances for different data types
        db_manager (DatabaseManager): Database connection manager
        semaphore (asyncio.Semaphore): Limits concurrent API requests
        data_type_stats (Dict): Statistics for different data types
        api_key_index (int): Current index in the API key rotation
        api_key_status (Dict): Tracks the status of each API key
    """

    def __init__(self, db_manager, env_path=".env.gov"):
        """
        Initialize the API Manager.

        Args:
            db_manager: Database manager instance for handling database connections
            env_path (str): Path to the environment file containing API keys
        """
        self.session_pool = {}
        self.all_api_keys = self._load_api_keys(env_path)
        self.congress_instances = {}
        self.db_manager = db_manager
        self.semaphore = asyncio.Semaphore(10)  # Limit concurrent requests
        self.data_type_stats = {}
        self.api_key_index = 0
        self.logger = logging.getLogger(__name__)
        self.api_key_status = {key: {"sleeping": False, "sleep_until": None} for key in self.all_api_keys}

    async def set_api_key_sleeping(self, api_key: str, sleep_duration: int) -> None:
        """
        Mark an API key as sleeping for a specified duration.

        Args:
            api_key (str): The API key to set as sleeping
            sleep_duration (int): Duration in seconds for the API key to sleep
        """
        self.api_key_status[api_key]["sleeping"] = True
        self.api_key_status[api_key]["sleep_until"] = datetime.now(timezone.utc) + timedelta(seconds=sleep_duration)

    async def wake_up_api_key(self, api_key: str) -> None:
        """
        Mark an API key as active (no longer sleeping).

        Args:
            api_key (str): The API key to wake up
        """
        self.api_key_status[api_key]["sleeping"] = False
        self.api_key_status[api_key]["sleep_until"] = None

    async def get_active_api_keys(self) -> List[str]:
        """
        Get a list of currently active (non-sleeping) API keys.

        Returns:
            List[str]: List of active API keys
        """
        now = datetime.now(timezone.utc)
        active_keys = []
        for key, status in self.api_key_status.items():
            if not status["sleeping"] or (status["sleep_until"] and status["sleep_until"] <= now):
                active_keys.append(key)
                if status["sleeping"]:
                    await self.wake_up_api_key(key)
        return active_keys

    async def get_data(self, data_type: str, **kwargs) -> AsyncIterator[List[Any]]:
        """
        Retrieve data from the Congressional API for a specific data type.

        This method handles pagination and error handling for API requests.

        Args:
            data_type (str): Type of data to retrieve (e.g., 'bills', 'committees')
            **kwargs: Additional parameters for the API request, such as 'next_url' for pagination

        Yields:
            List[Any]: Batches of retrieved items
            ErrorResult: In case of API errors

        Raises:
            Exception: For unexpected errors during data retrieval
        """
        congress = self.congress_instances[data_type]
        method_name = f"get_bulk_{data_type}"
        method = getattr(congress, method_name)
        total_count = None
        truncated = False
        processed_count = 0

        while True:
            try:
                items = await method(**kwargs)
                logger.info(f"Fetched {len(items)} items for {data_type}")

                if isinstance(items, ErrorResult): # if there is an error, yield it to be handled later and continue
                    yield items
                    continue

                if not items:
                    logger.info(f"No more {data_type} items to process")
                    break

                if total_count is None and items and items[0]._pagination.get('count'): # if total count is not set, set it to the count of the first item
                    total_count = items[0]._pagination['count']
                    logger.info(f"Total items for {data_type}: {total_count}")


                full_items = []

                if len(items) > 250: # if the number of items is greater than 250, truncate it to 250; this sometimes happens when the API returns more than 250 items
                    # truncate items to 250
                    items = items[:250]
                    truncated = True
                time.sleep(10)
                for item in items:
                    try:
                        full_item = await self.fetch_full_item(item)
                        if full_item:
                            full_items.append(full_item)
                    except Exception as e:
                        logger.error(f"Error fetching full item details for {data_type}: {str(e)}", exc_info=True)
                        await self.db_manager.write_error_to_database( # write the error to the database for permanent record/debugging/backfilling
                            url=item.url,
                            error_message=str(e),
                            data_type=type(item).__name__
                        )
                        continue
                yield full_items

                processed_count += len(items)
                
                # Check for end conditions:
                # 1. No next page in pagination
                # 2. Reached or exceeded total count
                pagination_url = items[0]._pagination.get('next') if items else None
                if not pagination_url:
                    logger.info(f"No next page for {data_type}, ending pagination")
                    break
                if total_count and processed_count >= total_count:
                    logger.info(f"Reached total count ({processed_count}/{total_count}) for {data_type}")
                    break
                if truncated:
                    logger.info(f"Batch was truncated for {data_type}, ending pagination")
                    break

                kwargs['next_url'] = pagination_url

            except Exception as e:
                logger.error(f"Error in get_data for {data_type}: {str(e)}", exc_info=True)
                yield ErrorResult(url=kwargs.get('next_url', f"bulk_{data_type}"), error_message=str(e))
                await asyncio.sleep(60)  # Wait for a minute before retrying

        logger.info(f"Completed fetching all data for {data_type}")

    async def fetch_full_item(self, item):
        """
        Fetch the full item details from the API.

        Args:
            item (Item): The item to fetch the full details for (e.g. a 'Bill' object that only has attributes set from the bulk API call)

        Returns:
            Item: The full item details (e.g. a 'Bill' object with all attributes set)
        """
        max_retries = 3
        for attempt in range(max_retries):
            async with self.semaphore:
                try:
                    full_item_result = await item._adapter.retrieve(item.url, override=True)
                    if isinstance(full_item_result, ErrorResult):
                        logger.error(f"Error fetching full item details: {full_item_result.error_message}")
                        if attempt == max_retries - 1:
                            await self.db_manager.write_error_to_database(
                                url=item.url,
                                error_message=full_item_result.error_message,
                                data_type=type(item).__name__
                            )
                        else:
                            await self.exponential_backoff(attempt)
                            continue

                    # Instantiate the appropriate object based on the item type
                    item_class = type(item)
                    full_item = item_class(data=full_item_result.data, _pagination=item._pagination, _adapter=item._adapter)

                    # Check for 'MISSING' fields in full_item and fill from the bulk API call version of the item if available
                    for attr in dir(full_item):
                        if not attr.startswith('_') and getattr(full_item, attr) == 'MISSING':
                            original_value = getattr(item, attr, None)
                            if original_value is not None and original_value != 'MISSING':
                                setattr(full_item, f"_{attr}", original_value)
                            else:
                                setattr(full_item, f"_{attr}", '')

                    return full_item
                except Exception as e:
                    logger.error(f"Error fetching full item details (attempt {attempt + 1}): {str(e)}", exc_info=True)
                    if attempt == max_retries - 1:
                        await self.db_manager.write_error_to_database(
                            url=getattr(item, 'url', 'unknown'),
                            error_message=str(e),
                            data_type=type(item).__name__
                        )
                    else:
                        await self.exponential_backoff(attempt)
        return None


    @staticmethod
    async def exponential_backoff(attempt, max_delay=60):
        """
        Exponential backoff for API requests.

        Args:
            attempt (int): The current retry attempt number
            max_delay (int): The maximum delay in seconds
        """
        delay = min(2**attempt + random.uniform(0, 1), max_delay)
        await asyncio.sleep(delay)

    def _load_api_keys(self, env_path): 
        """
        Load API keys from the environment file.

        Args:
            env_path (str): The path to the environment file
        """
        load_dotenv(env_path)
        api_keys = []
        for key, value in os.environ.items():
            if key.startswith("CONGRESS_API_KEY"):
                api_keys.append(value)
        return api_keys

    async def create_session(self, data_type):
        """
        Create a new session for a given data type.

        Args:
            data_type (str): The type of data to create a session for, e.g. 'bills' or 'committees'
        """
        if data_type not in self.session_pool:
            self.session_pool[data_type] = aiohttp.ClientSession(
                raise_for_status=True,
                timeout=aiohttp.ClientTimeout(total=300)
            )
        return self.session_pool[data_type]

    async def close_all_sessions(self):
        """
        Close all sessions.
        """
        await asyncio.gather(*[session.close() for session in self.session_pool.values()])

    async def create_congress_instance(self, data_type, api_keys):
        """
        Create a new PyCongress instance for a given data type.

        Args:
            data_type (str): The type of data to create a PyCongress instance for, e.g. 'bills' or 'committees'
            api_keys (List[str]): The API key(s) to use for the PyCongress instance
        """
        session = await self.create_session(data_type)
        self.congress_instances[data_type] = PyCongress(api_keys, session=session, db_pool=self.db_manager.pool)
        self.data_type_stats[data_type] = {
            "items_processed": 0,
            "items_remaining": 0,
            "start_time": datetime.now(timezone.utc)
        }
        return self.congress_instances[data_type]

    async def close_session(self, data_type):
        """
        Close a session for a given data type.

        Args:
            data_type (str): The type of data to close the session for, e.g. 'bills' or 'committees'
        """
        if data_type in self.session_pool and not self.session_pool[data_type].closed:
            await self.session_pool[data_type].close()
        if data_type in self.congress_instances:
            del self.congress_instances[data_type]

class DatabaseManager:
    """
    Manages database connections and operations for the ETL process.
    
    This class handles PostgreSQL database connections, connection pooling,
    and database operations including metadata tracking and error logging.
    
    Attributes:
        db_config (Dict): PostgreSQL database configuration
        pool (asyncpg.Pool): Connection pool for database operations
    """

    def __init__(self, env_path: str = ".env"):
        """
        Initialize the DatabaseManager with configuration from environment variables.

        Args:
            env_path (str): Path to the environment file containing database credentials
        """
        load_dotenv(dotenv_path=env_path)
        self.db_config = {
            "database": os.getenv("POSTGRESQL_DB"),
            "user": os.getenv("POSTGRESQL_USER"),
            "password": os.getenv("POSTGRESQL_PASSWORD"),
            "host": os.getenv("POSTGRESQL_HOST"),
            "port": os.getenv("POSTGRESQL_PORT"),
        }
        self.pool = None

    async def create_pool(self) -> None:
        """
        Create a connection pool for database operations.
        
        This method initializes the asyncpg connection pool using the configured
        database credentials.
        """
        self.pool = await asyncpg.create_pool(**self.db_config)

    async def close_pool(self) -> None:
        """
        Close the database connection pool.
        
        This method should be called during cleanup to properly close all database
        connections.
        """
        if self.pool:
            await self.pool.close()

    async def get_last_processed_date(self, data_type: str) -> Optional[str]:
        """
        Retrieve the last processed date for a specific data type.

        Args:
            data_type (str): The type of data to check (e.g., 'bills', 'committees')

        Returns:
            Optional[str]: ISO formatted date string of last processed date, or None if not found
        """
        async with self.pool.acquire() as conn:
            result = await conn.fetchval("""
                SELECT last_processed_date
                FROM __metadata.last_processed_dates
                WHERE data_type = $1
            """, data_type)
        return result

    async def update_last_processed_date(self, data_type: str, last_date: str) -> None:
        """
        Update the last processed date for a specific data type.

        Args:
            data_type (str): The type of data being processed
            last_date (str): ISO formatted date string of the last processed date
        """
        async with self.pool.acquire() as conn:
            await conn.execute("""
                UPDATE __metadata.last_processed_dates
                SET last_processed_date = $2
                WHERE data_type = $1
            """, data_type, last_date)
        logger.info(f"Updated last processed date for {data_type}: {last_date}")

    async def write_error_to_database(self, url: str, error_message: str, data_type: str) -> None:
        """
        Log an error to the database for tracking and debugging purposes.

        Args:
            url (str): The URL that caused the error
            error_message (str): Description of the error
            data_type (str): The type of data being processed when the error occurred
        """
        timestamp = datetime.now(timezone.utc).isoformat()
        url = url if url != 'unknown' else 'Unknown URL'
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO __metadata.congressional_errors (url, error, data_type, timestamp) 
                VALUES ($1, $2, $3, $4)
            """, url, error_message, data_type, timestamp)


class CSVExporter:
    """
    Manages the export of processed data to CSV files.

    This class handles the creation and updating of CSV files for each data type,
    including proper field ordering and concurrent file access. While we could go directly into the database,
    for our purposes, it's easier to just export to CSV for debugging before sending to staging, 
    especially with large text fields and small datasets.

    Attributes:
        base_directory (str): Directory where CSV files will be stored
        semaphore (asyncio.Semaphore): Controls concurrent file access
        config (Dict): Configuration for field ordering and data type settings
    """

    def __init__(self, base_directory: str, config: Dict, max_concurrent_files=100):
        """
        Initialize the CSV Exporter.

        Args:
            base_directory (str): Directory path for storing CSV files
            config (Dict): Configuration dictionary containing field mappings
            max_concurrent_files (int): Maximum number of files to write to concurrently
        """
        self.base_directory = base_directory
        os.makedirs(base_directory, exist_ok=True)
        self.semaphore = asyncio.Semaphore(max_concurrent_files)
        self.config = config

    def get_ordered_fields(self, data_type: str, items: List[Dict]) -> List[str]:
        """
        Determine the correct order of fields for a CSV file.

        Prioritizes ID fields first, followed by configured fields, and finally
        any remaining fields found in the data. Purely for readability.

        Args:
            data_type (str): Type of data being processed
            items (List[Dict]): Sample items to extract fields from

        Returns:
            List[str]: Ordered list of field names
        """
        config = self.config.get(data_type, {})
        id_fields = config.get('id_fields', [])
        fields = config.get('fields', [])
        
        # Ensure id_fields are at the beginning of the list
        ordered_fields = id_fields.copy()
        ordered_fields.extend([f for f in fields if f not in id_fields])
        
        if not ordered_fields:
            # Fallback: use all keys from items, with id_fields first. This is when we're
            # dealing with a data type that doesn't have any fields configured (also known
            # as a data type that we want to get everything available for, whether that's all attributes
            # of the item, the full list, etc)
            all_keys = set()
            for item in items:
                all_keys.update(item.keys())
            ordered_fields = id_fields + [f for f in sorted(all_keys) if f not in id_fields]

        return ordered_fields

    async def export_to_csv(self, data_type: str, items: List[Dict]) -> None:
        """
        Export a batch of items to a CSV file.

        Handles creation of new files and appending to existing ones, with proper
        field ordering and concurrent access control.

        Args:
            data_type (str): Type of data being exported
            items (List[Dict]): Items to write to the CSV file
        """
        if not items:
            logger.debug(f"No items to export for {data_type}")
            return

        filepath = os.path.join(self.base_directory, f"{data_type}.csv")
        file_exists = os.path.exists(filepath)

        ordered_fields = self.get_ordered_fields(data_type, items)

        async with self.semaphore:
            async with aiofiles.open(filepath, 'a' if file_exists else 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=ordered_fields, extrasaction='ignore')
                if not file_exists:
                    await writer.writeheader()
                for item in items:
                    await writer.writerow({field: item.get(field, '') for field in ordered_fields})

        logger.debug(f"Exported {len(items)} items to CSV: {filepath}")

    def id_exists_in_csv(self, data_type: str, id_value: str, id_field: str) -> bool:
        """
        Check if a specific ID already exists in a CSV file.

        Args:
            data_type (str): Type of data to check
            id_value (str): Value of the ID to look for
            id_field (str): Name of the ID field in the CSV

        Returns:
            bool: True if the ID exists, False otherwise
        """
        filepath = os.path.join(self.base_directory, f"{data_type}.csv")
        if not os.path.exists(filepath):
            return False

        with open(filepath, 'r', newline='') as f:
            reader = csv.DictReader(f)
            if id_field not in reader.fieldnames:
                return False
            return any(row[id_field] == id_value for row in reader)

class RetryManager:
    """
    Manages retry logic for failed operations with exponential backoff.

    This class provides a robust retry mechanism for handling transient failures
    in API calls and other operations, implementing exponential backoff to
    reduce server load during retries.

    Attributes:
        max_retries (int): Maximum number of retry attempts
        base_delay (int): Initial delay between retries in seconds
        max_delay (int): Maximum delay between retries in seconds
    """

    def __init__(self, max_retries=5, base_delay=1, max_delay=60):
        """
        Initialize the RetryManager.

        Args:
            max_retries (int): Maximum number of retry attempts
            base_delay (int): Initial delay between retries in seconds
            max_delay (int): Maximum delay between retries in seconds
        """
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay

    async def retry_with_backoff(self, coroutine, *args, **kwargs):
        """
        Execute a coroutine with retry logic and exponential backoff.

        Args:
            coroutine: The async function to execute
            *args: Positional arguments for the coroutine
            **kwargs: Keyword arguments for the coroutine

        Returns:
            Any: Result from the successful execution of the coroutine

        Raises:
            Exception: The last exception encountered after all retries are exhausted
        """
        for attempt in range(self.max_retries):
            try:
                return await coroutine(*args, **kwargs)
            except Exception as e:
                if attempt == self.max_retries - 1:
                    raise
                delay = min(self.base_delay * 2 ** attempt, self.max_delay)
                await asyncio.sleep(delay)

    @staticmethod
    async def wait_for_rate_limit(rate_limit_reset):
        """
        Wait until a rate limit reset time has passed.
        The rate limit is found in the "Retry-After" header of the response,
        and normally spans around 30 minutes.

        Args:
            rate_limit_reset (datetime): Time when the rate limit will reset
        """
        now = datetime.now(timezone.utc)
        wait_time = (rate_limit_reset - now).total_seconds()
        if wait_time > 0:
            await asyncio.sleep(wait_time)

    @staticmethod
    def is_rate_limit_error(error):
        """
        Check if an error is due to rate limiting.

        Args:
            error (Exception): The error to check

        Returns:
            bool: True if the error is a rate limit error, False otherwise
        """
        return isinstance(error, HTTPException) and error.status_code == 429


class ActivityMonitor:
    """
    Monitors and tracks activity in the ETL process.
    
    This class helps detect inactivity and manage sleep states during the ETL process,
    which is useful for handling rate limits, preventing unnecessary API calls,
    and checking if it is the ETL process that is hanging or if it is the API.
    
    Attributes:
        last_activity_time (datetime): Timestamp of the last recorded activity
        timeout_duration (timedelta): Duration after which inactivity is considered a timeout
        sleep_start_time (datetime): Time when the system entered sleep state
        total_sleep_time (timedelta): Cumulative time spent in sleep state
    """

    def __init__(self, timeout_duration):
        """
        Initialize the ActivityMonitor.

        Args:
            timeout_duration (timedelta): Duration after which to consider the system inactive
        """
        self.last_activity_time = datetime.now(timezone.utc)
        self.timeout_duration = timeout_duration
        self.sleep_start_time = None
        self.total_sleep_time = timedelta()

    def update_activity(self):
        """Update the last activity timestamp."""
        self.last_activity_time = datetime.now(timezone.utc)

    def start_sleep(self):
        """Mark the beginning of a sleep period."""
        if self.sleep_start_time is None:
            self.sleep_start_time = datetime.now(timezone.utc)

    def end_sleep(self):
        """
        Mark the end of a sleep period and update total sleep time.
        """
        if self.sleep_start_time is not None:
            sleep_duration = datetime.now(timezone.utc) - self.sleep_start_time
            self.total_sleep_time += sleep_duration
            self.sleep_start_time = None

    def is_timed_out(self) -> bool:
        """
        Check if the system has timed out due to inactivity.

        Returns:
            bool: True if the system has timed out, False otherwise
        """
        current_time = datetime.now(timezone.utc)
        active_duration = current_time - self.last_activity_time
        if self.sleep_start_time:
            active_duration -= (current_time - self.sleep_start_time)
        active_duration -= self.total_sleep_time
        return active_duration > self.timeout_duration

    async def wait_for_timeout(self) -> bool:
        """
        Wait until a timeout occurs.

        Returns:
            bool: True when timeout occurs
        """
        while not self.is_timed_out():
            await asyncio.sleep(60)  # Check every minute
        return True


class MessageQueue:
    """
    Manages persistent message queuing for the ETL process using SQLite.
    
    This class provides a simple message queue implementation that persists messages
    to disk, allowing for process resumption and state tracking across runs.
    
    Attributes:
        conn (sqlite3.Connection): Connection to the SQLite database
    """

    def __init__(self, db_path: str = 'etl_message_queue.db'):
        """
        Initialize the MessageQueue.

        Args:
            db_path (str): Path to the SQLite database file
        """
        self.conn = sqlite3.connect(db_path)
        self.create_table()

    def create_table(self):
        """Create the messages table if it doesn't exist."""
        cursor = self.conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS messages (
                data_type TEXT PRIMARY KEY,
                message TEXT NOT NULL
            )
        ''')
        self.conn.commit()

    def push(self, data_type: str, message: Dict[str, Any]):
        """
        Add or update a message in the queue.

        Args:
            data_type (str): Type of data the message relates to
            message (Dict[str, Any]): Message content to store
        """
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO messages (data_type, message)
            VALUES (?, ?)
        ''', (data_type, json.dumps(message)))
        self.conn.commit()

    def pop(self, data_type: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve a message from the queue without removing it.

        Args:
            data_type (str): Type of data to retrieve message for

        Returns:
            Optional[Dict[str, Any]]: The message if found, None otherwise
        """
        cursor = self.conn.cursor()
        cursor.execute('SELECT message FROM messages WHERE data_type = ?', (data_type,))
        result = cursor.fetchone()
        if result:
            return json.loads(result[0])
        return None

    def get_last_exported_item(self, data_type: str) -> Optional[Dict[str, Any]]:
        """
        Get the last exported item for a specific data type.

        Args:
            data_type (str): Type of data to check

        Returns:
            Optional[Dict[str, Any]]: Last exported item if found, None otherwise
        """
        message = self.pop(data_type)
        if message and 'last_exported_item' in message:
            return message['last_exported_item']
        return None

    def update_last_exported_item(self, data_type: str, item: Dict[str, Any]):
        """
        Update the last exported item for a specific data type.

        Args:
            data_type (str): Type of data being exported
            item (Dict[str, Any]): The item that was last exported
        """
        message = self.pop(data_type) or {}
        message['last_exported_item'] = item
        self.push(data_type, message)

    def clear_type(self, data_type: str):
        """
        Clear messages for a specific data type.

        Args:
            data_type (str): Type of data to clear messages for
        """
        cursor = self.conn.cursor()
        cursor.execute('DELETE FROM messages WHERE data_type = ?', (data_type,))
        self.conn.commit()
        logging.info(f"Cleared messages for data type: {data_type}")

    def clear_types(self, data_types: List[str]):
        """
        Clear messages for multiple data types.

        Args:
            data_types (List[str]): List of data types to clear messages for
        """
        cursor = self.conn.cursor()
        for data_type in data_types:
            cursor.execute('DELETE FROM messages WHERE data_type = ?', (data_type,))
        self.conn.commit()
        logging.info(f"Cleared messages for data types: {', '.join(data_types)}")

    def clear_except_bills(self):
        """Clear all messages except those related to bills."""
        cursor = self.conn.cursor()
        cursor.execute('''
            DELETE FROM messages 
            WHERE data_type NOT LIKE '%bill%' AND data_type NOT LIKE '%Bill%'
        ''')
        self.conn.commit()
        logging.info("Cleared all messages from the queue except for bills.")

    def clear_all(self):
        """Clear all messages from the queue."""
        cursor = self.conn.cursor()
        cursor.execute('DELETE FROM messages')
        self.conn.commit()
        logging.info("Cleared all messages from the queue.")

    def close(self):
        """Close the database connection."""
        self.conn.close()

class ETLCoordinator:
    """
    Main orchestrator for the ETL (Extract, Transform, Load) process.
    
    This class coordinates the entire ETL pipeline, managing:
    - Concurrent processing of different data types
    - API key distribution and management
    - Data extraction from the Congressional API
    - Data transformation and validation
    - Loading data into both database and CSV files
    - Error handling and recovery
    - Progress tracking and resumption
    
    Attributes:
        api_manager (APIManager): Manages API connections and requests
        db_manager (DatabaseManager): Handles database operations
        csv_exporter (CSVExporter): Manages CSV file exports
        config (Dict): Configuration settings for different data types
        num_workers (int): Number of concurrent worker tasks
        max_queue_size (int): Maximum size of the work queue
        data_type_queue (asyncio.Queue): Queue for data types to process
        work_queue (asyncio.Queue): Queue for individual work items
        semaphore (asyncio.Semaphore): Controls concurrent operations
        retry_manager (RetryManager): Handles operation retries
        message_queue (MessageQueue): Manages persistent message storage
        activity_monitor (ActivityMonitor): Tracks process activity
        finished_data_types (set): Tracks completed data types
    """

    def __init__(
        self,
        api_manager,
        db_manager,
        csv_exporter,
        config_file: str,
        num_workers: int = 10,
        max_queue_size: int = 1000,
        retry_manager: RetryManager = None,
        message_queue: MessageQueue = None
    ):
        """
        Initialize the ETL Coordinator.

        Args:
            api_manager (APIManager): API management instance
            db_manager (DatabaseManager): Database management instance
            csv_exporter (CSVExporter): CSV export management instance
            config_file (str): Path to configuration file
            num_workers (int): Number of concurrent workers
            max_queue_size (int): Maximum size of work queue
            retry_manager (RetryManager, optional): Custom retry manager
            message_queue (MessageQueue, optional): Custom message queue
        """
        self.api_manager = api_manager
        self.db_manager = db_manager
        self.csv_exporter = csv_exporter
        self.config = config_file
        self.num_workers = num_workers
        self.max_queue_size = max_queue_size
        self.data_type_queue = asyncio.Queue()
        self.work_queue = asyncio.Queue()
        self.semaphore = asyncio.Semaphore(20)
        self.retry_manager = retry_manager or RetryManager()
        self.message_queue = message_queue or MessageQueue()
        self.activity_monitor = ActivityMonitor(timeout_duration=timedelta(hours=1))
        self.finished_data_types = set()

    def get_main_data_types(self) -> List[str]:
        """
        Get the list of primary data types to process.

        Returns:
            List[str]: List of main data types from configuration
        """
        return [
            data_type
            for data_type, config in self.config.items()
            if config.get('main', False)
        ]

    async def initialize(self):
        """
        Initialize the ETL process.
        
        Sets up database connections, distributes API keys, and prepares
        processing instances for each data type.
        """
        await self.db_manager.create_pool()
        main_data_types = self.get_main_data_types()
        all_api_keys = self.api_manager.all_api_keys

        # Start by assigning 2 keys to each data type
        initial_keys_per_type = 2
        remaining_keys = all_api_keys.copy()

        for data_type in main_data_types:
            assigned_keys = remaining_keys[:initial_keys_per_type]
            remaining_keys = remaining_keys[initial_keys_per_type:]
            await self.api_manager.create_congress_instance(data_type, assigned_keys)

        self.dynamic_key_pool = remaining_keys

    async def redistribute_keys(self, finished_data_type: str):
        """
        Redistribute API keys from finished data types to active ones.

        This method optimizes API key usage by reallocating keys from completed
        data types to those still being processed.

        Args:
            finished_data_type (str): Data type that has completed processing
        """
        # Add keys from the finished data type back to the dynamic pool
        finished_keys = self.api_manager.congress_instances[finished_data_type]._adapter.api_keys
        self.dynamic_key_pool.extend(finished_keys)

        # Redistribute keys to unfinished data types
        unfinished_data_types = [
            dt for dt in self.get_main_data_types()
            if dt != finished_data_type and dt not in self.finished_data_types
        ]

        for data_type in unfinished_data_types:
            if self.dynamic_key_pool:
                additional_key = self.dynamic_key_pool.pop(0)
                self.api_manager.congress_instances[data_type]._adapter.api_keys.append(additional_key)

    async def run(self):
        """
        Run the complete ETL process.
        
        This is the main entry point for the ETL process, handling initialization,
        execution, and cleanup, including error handling and graceful shutdown.
        """
        try:
            await self.initialize()
            try:
                await self.process_all_data()
            except asyncio.CancelledError:
                logger.info("Received keyboard interrupt. Saving progress and stopping the ETL process.")
                await self.save_global_progress()
        except Exception as e:
            logger.error(f"An error occurred during the ETL process: {str(e)}", exc_info=True)
        finally:
            await self.cleanup()

    async def save_global_progress(self):
        """
        Save the current progress state for all data types.
        
        This method ensures that progress is not lost in case of interruption
        and enables process resumption.
        """
        for data_type in self.get_main_data_types():
            message = self.message_queue.pop(data_type)
            if message:
                self.message_queue.push(data_type, message)
        logger.info("Global progress saved")

    async def process_all_data(self):
        logger.info("Starting process_all_data method")
        main_data_types = self.get_main_data_types()

        for data_type in main_data_types:
            message = self.message_queue.pop(data_type)
            if message and message.get("status") == "FINISHED":
                logger.info(f"Skipping {data_type} as it has already been processed and marked as FINISHED.")
                continue

            if message:
                self.message_queue.push(data_type, message)
            await self.data_type_queue.put(data_type)

        workers = [asyncio.create_task(self.worker()) for _ in range(self.num_workers)]

        try:
            while not self.data_type_queue.empty() or any(not w.done() for w in workers):
                active_keys = await self.api_manager.get_active_api_keys()
                if not active_keys:
                    sleep_time = min(status["sleep_until"] for status in self.api_manager.api_key_status.values() if status["sleeping"])
                    wait_time = (sleep_time - datetime.now(timezone.utc)).total_seconds()
                    logger.info(f"All API keys are sleeping. Waiting for {wait_time} seconds.")
                    self.activity_monitor.start_sleep()
                    await asyncio.sleep(wait_time)
                    self.activity_monitor.end_sleep()
                else:
                    self.activity_monitor.update_activity()
                    try:
                        await asyncio.wait_for(self.data_type_queue.join(), timeout=3600)  # 1 hour timeout
                        break
                    except asyncio.TimeoutError:
                        logger.warning("Timeout occurred, but some API keys might still be active. Continuing...")
        except asyncio.CancelledError:
            logger.info("Received keyboard interrupt. Saving progress...")
            await self.save_global_progress()
            raise

        for worker in workers:
            worker.cancel()
        await asyncio.gather(*workers, return_exceptions=True)

        logger.info("Completed processing all data types")

    async def worker(self):
        while True:
            try:
                data_type = await self.data_type_queue.get()
                logger.info(f"Worker starting to process {data_type}")
                await self.process_data_type_concurrent(data_type)
                logger.info(f"Worker finished processing {data_type}")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error processing {data_type}: {str(e)}", exc_info=True)
            finally:
                self.data_type_queue.task_done()

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

    async def process_data_type_concurrent(self, data_type: str):
        logger.info(f"Starting to process data type: {data_type}")

        last_exported_item = await self.check_csv_consistency(data_type)
        if last_exported_item:
            logger.info(f"Resuming {data_type} from last exported item: {last_exported_item}")
        else:
            logger.info(f"Starting {data_type} from the beginning")

        while True:
            try:
                await self._process_data_type(data_type, last_exported_item)
                self.activity_monitor.update_activity()
                # If processing completes successfully, mark as finished and break the loop
                self.message_queue.push(data_type, {"status": "FINISHED"})
                logger.info(f"Finished processing {data_type}. Marked as FINISHED in the message queue.")
                
                # Add to finished set before redistributing keys
                self.finished_data_types.add(data_type)
                
                # Redistribute keys when a data type is finished
                await self.redistribute_keys(data_type)
                
                break
            except asyncio.TimeoutError:
                logger.error(f"Timeout occurred while processing {data_type}. Saving progress and retrying.")
                await self.save_global_progress()
                await asyncio.sleep(60)  # Wait for a minute before retrying
            except Exception as e:
                logger.error(f"Error processing {data_type}: {str(e)}", exc_info=True)
                await self.db_manager.write_error_to_database(
                    url=f"process_data_type_concurrent_{data_type}",
                    error_message=str(e),
                    data_type=data_type
                )
                await asyncio.sleep(60)  # Wait for a minute before retrying

        logger.info(f"Exiting process_data_type_concurrent for {data_type}")


    async def _process_data_type(self, data_type: str, last_exported_item: Optional[Dict[str, Any]]):
        message = self.message_queue.pop(data_type)
        if message:
            logging.info(f"Resuming processing for {data_type} from saved state")
            kwargs = message.get('kwargs', {})
            start_date = datetime.fromisoformat(message.get('start_date', datetime.min.replace(tzinfo=timezone.utc).isoformat()))
            overall_latest_date = datetime.fromisoformat(message.get('overall_latest_date', start_date.isoformat()))
            resume_state = message.get('resume_state', {})
        else:
            last_processed_date = await self.db_manager.get_last_processed_date(data_type)
            kwargs, start_date = await self._get_date_range(data_type, last_processed_date)
            overall_latest_date = start_date if start_date else datetime.min.replace(tzinfo=timezone.utc)
            resume_state = {}

        data_generator = self.api_manager.get_data(data_type, **kwargs)

        found_last_item = last_exported_item is None
        new_items_retrieved = False
        total_processed = 0
        target_count = None  # Will store our total count

        # Get initial count of items to process
        try:
            first_batch = await self._fetch_batch_with_dynamic_timeout(data_generator)
            if isinstance(first_batch, ErrorResult):
                logging.error(f"Error retrieving initial batch for {data_type}: {first_batch.error_message}")
                return
            
            # Process the first batch and get total count from pagination
            if first_batch and first_batch[0]._pagination.get('count'):
                target_count = first_batch[0]._pagination['count']
                total_processed = len(first_batch)
                new_items_retrieved = True
                
                should_process = True
                if not found_last_item:
                    should_process = False
                    for item in first_batch:
                        if self._item_matches(item, last_exported_item):
                            found_last_item = True
                            should_process = True
                            break

                if should_process:
                    try:
                        await self.process_batch(data_type, first_batch, resume_state)
                        resume_state = {}
                    except asyncio.TimeoutError:
                        logging.error(f"Timeout occurred while processing batch for {data_type}")
                        self._save_progress(data_type, kwargs, start_date, overall_latest_date, resume_state)
                        raise

        except asyncio.TimeoutError:
            logging.error(f"Timeout occurred while fetching initial batch for {data_type}")
            self._save_progress(data_type, kwargs, start_date, overall_latest_date, resume_state)
            raise
        except StopAsyncIteration:
            logging.info(f"No data to retrieve for {data_type}")
            return

        logging.info(f"Processing {data_type}: Target count is {target_count}, processed so far: {total_processed}")

        while total_processed < target_count:
            try:
                items_batch = await self._fetch_batch_with_dynamic_timeout(data_generator)
            except asyncio.TimeoutError:
                logging.error(f"Timeout occurred while fetching batch for {data_type}")
                self._save_progress(data_type, kwargs, start_date, overall_latest_date, resume_state)
                raise
            except StopAsyncIteration:
                logging.info(f"No more data to retrieve for {data_type}")
                break

            if isinstance(items_batch, ErrorResult):
                logging.error(f"Error retrieving data for {data_type}: {items_batch.error_message}")
                self._save_progress(data_type, kwargs, start_date, overall_latest_date, resume_state)
                await self.db_manager.write_error_to_database(
                    url=items_batch.url,
                    error_message=items_batch.error_message,
                    data_type=data_type
                )
                continue

            if items_batch:
                new_items_retrieved = True
                total_processed += len(items_batch)
                logging.info(f"Processing {data_type}: {total_processed}/{target_count} items processed")
                
                if not found_last_item:
                    for item in items_batch:
                        if self._item_matches(item, last_exported_item):
                            found_last_item = True
                            break
                    if not found_last_item:
                        continue

                try:
                    await self.process_batch(data_type, items_batch, resume_state)
                    resume_state = {}
                except asyncio.TimeoutError:
                    logging.error(f"Timeout occurred while processing batch for {data_type}")
                    self._save_progress(data_type, kwargs, start_date, overall_latest_date, resume_state)
                    raise
                except Exception as e:
                    logging.error(f"Error processing batch for {data_type}: {str(e)}", exc_info=True)
                    self._save_progress(data_type, kwargs, start_date, overall_latest_date, resume_state)
                    continue  # Continue with the next batch instead of raising the exception

                batch_latest_date = max((self._get_item_date(item) for item in items_batch), default=datetime.min.replace(tzinfo=timezone.utc))
                overall_latest_date = max(overall_latest_date, batch_latest_date)

            self._save_progress(data_type, kwargs, start_date, overall_latest_date, resume_state)

        logging.info(f"Finished processing all {total_processed} items for {data_type}")
        self.message_queue.pop(data_type)

    def _item_matches(self, item: Any, last_exported_item: Dict[str, Any]) -> bool:
        if not last_exported_item:
            return False
        return all(getattr(item, k, None) == v for k, v in last_exported_item.items())

    async def process_batch(self, data_type: str, items_batch: List[Any], resume_state: Optional[Dict[str, Any]] = None):
        async def process():
            nonlocal resume_state
            logger.info(f"Processing batch of {len(items_batch)} items for {data_type}")
            fields = self.config.get(data_type, {}).get('fields', [])
            processed_items = []

            resume_state = resume_state or {}
            start_index = resume_state.get('item_index', 0) if resume_state.get('type') == 'item' else 0

            for index in range(start_index, len(items_batch)):
                try:
                    item = items_batch[index]
                    logger.debug(f"Processing item {index + 1}/{len(items_batch)} for {data_type}")
                    logger.debug(f"Type of item in process_batch: {type(item)}")
                    processed_item = await self.process_item(item, fields)
                    processed_items.append(processed_item)
                    logger.debug(f"Processed item for {data_type}:")
                    logger.debug(processed_item)

                    # Save progress after each item
                    self._save_item_progress(data_type, index, len(items_batch))
                    self.message_queue.update_last_exported_item(data_type, processed_item)

                    # Process nested fields and related data types for each item
                    await self.process_nested_fields(data_type, item, index, len(items_batch), resume_state)
                    await self.process_related_data_types(data_type, item, index, len(items_batch), resume_state)

                    # Clear resume state after successful processing of an item
                    resume_state = {}
                except Exception as e:
                    logger.error(f"Error processing item {index + 1} for {data_type}: {str(e)}", exc_info=True)
                    resume_state = {
                        'type': 'item',
                        'item_index': index
                    }
                    raise  # Re-raise the exception to be caught by retry mechanism

            await self.csv_exporter.export_to_csv(data_type, processed_items)

            logger.info(f"Finished processing batch for {data_type}")

        try:
            await self.retry_manager.retry_with_backoff(process)
        except Exception as e:
            logger.error(f"Failed to process batch for {data_type} after retries: {str(e)}", exc_info=True)
            # Save the current state before raising the exception
            self._save_progress(data_type, {}, None, None, resume_state)
            raise

    async def process_item(self, item: Any, fields: List[str] = []) -> Dict[str, Any]:
        try:
            if isinstance(item, (str, int, float, bool)):
                return {"value": item}
            elif isinstance(item, dict):
                return item
            else:
                result = {}
                for attr in fields if fields else dir(item):
                    if not attr.startswith('_') and attr != 'data':
                        prop = getattr(type(item), attr, None)
                        if isinstance(prop, property):
                            value = getattr(item, attr)
                            if isinstance(value, (str, int, float, bool)) or value is None:
                                result[attr] = value
                            elif isinstance(value, datetime):
                                result[attr] = value.isoformat()
                            elif isinstance(value, list):
                                # Keep lists as they are
                                result[attr] = value
                            else:
                                # For other types, convert to string
                                result[attr] = str(value)
                if not result:
                    logger.warning(f"No processable attributes found for item")
                    # logger.debug(f"Item __dict__ for unprocessable item:")
                    # logger.debug(item.__dict__)
                return result
        except Exception as e:
            logger.error(f"Error processing item: {str(e)}", exc_info=True)
            # logger.debug(f"Item __dict__ for error-causing item:")
            # logger.debug(item.__dict__)
            return {}

    async def process_nested_fields(self, data_type: str, item: Any, index: int, total: int, resume_state: Dict[str, Any]):
        logger.info(f"Processing nested fields for {data_type} (item {index + 1}/{total})")
        nested_fields = self.config.get(data_type, {}).get('nested_fields', [])
        logger.debug(f"Nested fields for {data_type}: {nested_fields}")
        
        start_field_index = resume_state.get('field_index', 0) if resume_state.get('type') == 'nested_field' and resume_state.get('item_index') == index else 0
        for field_index in range(start_field_index, len(nested_fields)):
            self.activity_monitor.update_activity()  # Add this line
            nested_field = nested_fields[field_index]
            if isinstance(nested_field, dict):
                field_name = next(iter(nested_field))
                field_config = nested_field[field_name]
            else:
                field_name = nested_field
                field_config = {}

            logger.debug(f"Processing nested field: {field_name}")
            nested_data = getattr(item, field_name, None)

            if nested_data:
                logger.info(f"Found nested data for {field_name}")
                await self.process_single_nested_field(data_type, field_name, nested_data, item, field_config)
            else:
                logger.debug(f"No nested data found for {field_name}")
                # logger.debug(f"Item __dict__ for {data_type} without nested field {field_name}:")
                # logger.debug(item.__dict__)

            # Save progress after each nested field
            self._save_nested_field_progress(data_type, index, total, field_index, len(nested_fields))

    async def process_single_nested_field(self, data_type: str, nested_field: str, nested_data: Any, parent_item: Any = None, nested_config: Dict = None):
        nested_data_type = f"{data_type}_{nested_field}"
        id_fields = self.config[data_type].get('id_fields', ['id'])

        if nested_data is None:
            logger.warning(f"Nested data is None for {nested_data_type}")
            return

        def add_missing_id_fields(item_dict):
            for id_field in id_fields:
                if id_field not in item_dict:
                    parent_id_value = getattr(parent_item, id_field, None)
                    if parent_id_value is not None:
                        item_dict[id_field] = parent_id_value
            return item_dict

        if isinstance(nested_data, list):
            if all(isinstance(item, (str, int, float, bool)) for item in nested_data):
                # For list of flat data types
                formatted_data = [add_missing_id_fields({nested_field: item}) for item in nested_data]
            else:
                # For list of objects
                formatted_data = []
                for item in nested_data:
                    if item is not None:
                        if nested_config and 'fields' in nested_config:
                            # Use specified fields
                            processed_item = {field: getattr(item, field, None) for field in nested_config['fields']}
                        else:
                            # Use all attributes of the object
                            processed_item = {attr: getattr(item, attr) for attr in dir(item) 
                                            if not attr.startswith('_') and not callable(getattr(item, attr)) and attr != 'data'}
                        formatted_data.append(add_missing_id_fields(processed_item))
        elif isinstance(nested_data, dict):
            if nested_config and 'fields' in nested_config:
                # Use specified fields
                formatted_data = [add_missing_id_fields({field: nested_data.get(field) for field in nested_config['fields']})]
            else:
                # Use all keys of the dict
                formatted_data = [add_missing_id_fields(nested_data)]
        elif isinstance(nested_data, (str, int, float, bool)):
            # Handle simple data types
            formatted_data = [add_missing_id_fields({nested_field: nested_data})]
        else:
            logger.warning(f"Unexpected nested data type for {nested_data_type}: {type(nested_data)}")
            return

        if formatted_data:
            await self.csv_exporter.export_to_csv(nested_data_type, formatted_data)
            logger.info(f"Processed and exported nested data for {nested_data_type}")
        else:
            logger.debug(f"No data to export for {nested_data_type}")

    async def process_related_data_types(self, data_type: str, item: Any, index: int, total: int, resume_state: Dict[str, Any]):
        logger.info(f"Processing related data types for {data_type} (item {index + 1}/{total})")
        related_types = self.config.get(data_type, {}).get('related_fields', [])
        logger.debug(f"Related types for {data_type}: {related_types}")
        id_fields = self.config[data_type].get('id_fields', ['id'])

        logger.info(f"Item type in process_related_data_types: {type(item)}")

        start_related_index = resume_state.get('related_index', 0) if resume_state.get('type') == 'related_item' and resume_state.get('item_index') == index else 0
        for related_index in range(start_related_index, len(related_types)):
            self.activity_monitor.update_activity()  # Add this line
            related_type = related_types[related_index]
            method_name = f"get_{related_type}"
            logger.debug(f"Checking for method: {method_name}")

            if hasattr(item, method_name):
                logger.debug(f"Found method {method_name} for {data_type}")
                try:
                    method = getattr(item, method_name)
                    async_generator = method()
                    related_items = []
                    try:
                        async for related_item in async_generator:
                            self.activity_monitor.update_activity()  # Add this line

                            if isinstance(related_item, ErrorResult):
                                error_message = related_item.error_message or related_item.data.get("error", "Unknown error")
                                logger.error(f"Error retrieving related data for {data_type}_{related_type}: {error_message}")
                                url = getattr(related_item, 'url', 'unknown')
                                await self.db_manager.write_error_to_database(url, error_message, data_type=f"{data_type}_{related_type}")
                                break
                            elif related_item:
                                processed_item = await self.process_item(related_item)
                                if processed_item:
                                    for id_field in id_fields:
                                        if id_field not in processed_item:
                                            parent_id_value = getattr(item, id_field, None)
                                            if parent_id_value is not None:
                                                processed_item[id_field] = parent_id_value
                                    related_items.append(processed_item)

                                # Process second level related data
                                await self.process_second_level_related_data(data_type, related_type, related_item)

                            # Save progress after each related item
                            self._save_related_item_progress(data_type, index, total, related_index, len(related_types), len(related_items))
                    except Exception as e:
                        logger.error(f"Error processing related data {related_type} for {data_type}: {str(e)}", exc_info=True)
                        url = getattr(item, 'url', 'unknown')
                        await self.db_manager.write_error_to_database(url, str(e), data_type=f"{data_type}_{related_type}")
                    
                    if related_items:
                        logger.debug(f"Exporting {len(related_items)} items for {data_type}_{related_type}")
                        await self.csv_exporter.export_to_csv(f"{data_type}_{related_type}", related_items)
                    else:
                        logger.debug(f"No related items found for {data_type}_{related_type}")
                        # logger.debug(f"Item attributes for {data_type} with no {related_type}:")
                        # logger.debug(", ".join(dir(item)))
                except Exception as e:
                    logger.error(f"Error processing related data {related_type} for {data_type}: {str(e)}", exc_info=True)
                    url = getattr(item, 'url', 'unknown')
                    await self.db_manager.write_error_to_database(url, str(e), data_type=f"{data_type}_{related_type}")
            else:
                logger.warning(f"Method {method_name} not found for {data_type}")
                # logger.debug(f"Item attributes for {data_type} without {method_name} method:")
                # logger.debug(", ".join(dir(item)))

    async def process_second_level_related_data(self, main_data_type: str, related_type: str, related_item: Any):
        logger.debug(f"Processing second level related data for {main_data_type}_{related_type}")

        related_config = self.config.get(f"{main_data_type}_{related_type}", {})
        second_level_related_types = related_config.get('related_fields', [])
        second_level_nested_fields = related_config.get('nested_fields', [])
        id_fields = related_config.get('id_fields', [])

        for second_level_related_type in second_level_related_types:
            self.activity_monitor.update_activity()  # Add this line
            method_name = f"get_{second_level_related_type}"
            if hasattr(related_item, method_name):
                try:
                    method = getattr(related_item, method_name)
                    async_generator = method()
                    second_level_related_items = []
                    async for second_level_item in async_generator:
                        self.activity_monitor.update_activity()  # Add this line
                        if isinstance(second_level_item, ErrorResult):
                            error_message = second_level_item.error_message or second_level_item.data.get("error", "Unknown error")
                            logger.error(f"Error retrieving second level related data for {main_data_type}_{related_type}_{second_level_related_type}: {error_message}")
                            url = getattr(second_level_item, 'url', 'unknown')
                            await self.db_manager.write_error_to_database(url, error_message, data_type=f"{main_data_type}_{related_type}_{second_level_related_type}")
                            break
                        elif second_level_item:
                            processed_item = await self.process_item(second_level_item)
                            if processed_item:
                                for id_field in id_fields:
                                    if id_field not in processed_item:
                                        id_value = getattr(related_item, id_field, None)
                                        if id_value is not None:
                                            processed_item[id_field] = id_value
                                second_level_related_items.append(processed_item)

                    if second_level_related_items:
                        logger.debug(f"Exporting {len(second_level_related_items)} items for {main_data_type}_{related_type}_{second_level_related_type}")
                        await self.csv_exporter.export_to_csv(f"{main_data_type}_{related_type}_{second_level_related_type}", second_level_related_items)
                    else:
                        logger.debug(f"No second level related items found for {main_data_type}_{related_type}_{second_level_related_type}")
                except Exception as e:
                    logger.error(f"Error processing second level related data {second_level_related_type} for {main_data_type}_{related_type}: {str(e)}", exc_info=True)
                    url = getattr(related_item, 'url', 'unknown')
                    await self.db_manager.write_error_to_database(url, str(e), data_type=f"{main_data_type}_{related_type}_{second_level_related_type}")

        for nested_field in second_level_nested_fields:
            self.activity_monitor.update_activity()  # Add this line
            if isinstance(nested_field, dict):
                field_name = next(iter(nested_field))
                nested_config = nested_field[field_name]
            else:
                field_name = nested_field
                nested_config = {}
            await self.process_single_nested_field(f"{main_data_type}_{related_type}", field_name, getattr(related_item, field_name, None), related_item, nested_config)

    async def _fetch_batch_with_dynamic_timeout(self, data_generator, base_timeout=3600):
        while True:
            active_keys = await self.api_manager.get_active_api_keys()
            if not active_keys:
                sleep_time = min(status["sleep_until"] for status in self.api_manager.api_key_status.values() if status["sleeping"])
                wait_time = (sleep_time - datetime.now(timezone.utc)).total_seconds()
                logger.info(f"All API keys are sleeping. Waiting for {wait_time} seconds.")
                self.activity_monitor.start_sleep()
                await asyncio.sleep(wait_time)
                self.activity_monitor.end_sleep()
            else:
                self.activity_monitor.update_activity()
                try:
                    return await asyncio.wait_for(data_generator.__anext__(), timeout=base_timeout)
                except asyncio.TimeoutError:
                    logger.warning("Timeout occurred, but some API keys might still be active. Retrying...")

    def _save_progress(self, data_type: str, kwargs: Dict[str, Any], start_date: datetime, overall_latest_date: datetime, resume_state: Dict[str, Any]):
        message = {
            'kwargs': kwargs,
            'start_date': start_date.isoformat() if start_date else None,
            'overall_latest_date': overall_latest_date.isoformat(),
            'resume_state': resume_state
        }
        self.message_queue.push(data_type, message)
        logging.info(f"Saved progress for {data_type}")

    def _save_item_progress(self, data_type: str, index: int, total: int):
        message = {
            'type': 'item',
            'index': index,
            'total': total
        }
        self.message_queue.push(data_type, message)
        logging.debug(f"Saved progress for {data_type}: item {index + 1}/{total}")

    def _save_nested_field_progress(self, data_type: str, item_index: int, item_total: int, field_index: int, field_total: int):
        message = {
            'type': 'nested_field',
            'item_index': item_index,
            'item_total': item_total,
            'field_index': field_index,
            'field_total': field_total
        }
        self.message_queue.push(data_type, message)
        logging.debug(f"Saved progress for {data_type}: item {item_index + 1}/{item_total}, nested field {field_index + 1}/{field_total}")

    def _save_related_item_progress(self, data_type: str, item_index: int, item_total: int, related_index: int, related_total: int, related_items_count: int):
        message = {
            'type': 'related_item',
            'item_index': item_index,
            'item_total': item_total,
            'related_index': related_index,
            'related_total': related_total,
            'related_items_count': related_items_count
        }
        self.message_queue.push(data_type, message)
        logging.debug(f"Saved progress for {data_type}: item {item_index + 1}/{item_total}, related type {related_index + 1}/{related_total}, related items processed: {related_items_count}")

    @staticmethod
    def format_date(date: datetime) -> str:
        return date.strftime("%Y-%m-%dT%H:%M:%SZ")

    def _get_item_date(self, item: Any) -> datetime:
        date_fields = ["updated_at", "updateDate", "update_date", "latest_action_date"]
        for field in date_fields:
            date_str = getattr(item, field, None)
            if date_str:
                try:
                    # Parse the date string and ensure it's timezone-aware
                    parsed_date = datetime.fromisoformat(date_str.rstrip('Z'))
                    if parsed_date.tzinfo is None:
                        parsed_date = parsed_date.replace(tzinfo=timezone.utc)
                    return parsed_date
                except (ValueError, AttributeError) as e:
                    logger.warning(f"Error parsing date '{date_str}' for field '{field}': {str(e)}")

        return datetime.now(timezone.utc)


    async def _get_date_range(self, data_type: str, last_processed_date: str):
        end_date = datetime.now(tz=timezone.utc)
        overlap_window = timedelta(hours=1)

        if last_processed_date:
            try:
                # Use dateutil.parser to parse the ISO 8601 format with 'Z' timezone
                start_date = parser.isoparse(last_processed_date)
                if start_date.tzinfo is None:
                    start_date = start_date.replace(tzinfo=timezone.utc)
                start_date -= overlap_window
            except ValueError as e:
                logger.error(f"Error parsing last_processed_date for {data_type}: {e}")
                start_date = None
        else:
            start_date = None

        latest_date_in_batch = start_date if start_date else datetime.min.replace(tzinfo=timezone.utc)

        kwargs = {}
        if start_date:
            kwargs["from_date"] = self.format_date(start_date)
        kwargs["to_date"] = self.format_date(end_date)

        return kwargs, latest_date_in_batch

    async def cleanup(self):
        logger.info("Performing cleanup...")
        await self.api_manager.close_all_sessions()
        await self.db_manager.close_pool()
        self.message_queue.close()
        logger.info("Cleanup complete.")

async def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description="Congressional ETL Scraper")
    parser.add_argument("--clear-q", action="store_true", help="Clear the entire message queue before starting")
    parser.add_argument("--clear-q-except-bills", action="store_true", help="Clear the message queue except for bill-related messages")
    parser.add_argument("--clear-q-types", nargs="+", help="Clear the message queue for specified data types")
    parser.add_argument("--data-type", type=str, help="Specific data type to process (e.g., 'bills', 'committees')")
    args = parser.parse_args()

    load_dotenv(".env.info")
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    config_file = "relevant_fields.yaml"
    with open(config_file, "r") as f:
        config = yaml.safe_load(f)

    # Validate data type if specified
    if args.data_type and args.data_type not in config:
        logger.error(f"Invalid data type: {args.data_type}")
        logger.info(f"Available data types: {', '.join(config.keys())}")
        return

    db_manager = DatabaseManager()
    api_manager = APIManager(db_manager)
    csv_exporter = CSVExporter("/mnt/big_data/database-congress/api-congress", config)

    retry_manager = RetryManager()
    message_queue = MessageQueue()

    if args.clear_q:
        logger.info("Clearing entire message queue as requested...")
        message_queue.clear_all()
        logger.info("Message queue cleared.")
    elif args.clear_q_except_bills:
        logger.info("Clearing message queue except for bill-related messages...")
        message_queue.clear_except_bills()
        logger.info("Message queue cleared except for bill-related messages.")
    elif args.clear_q_types:
        logger.info(f"Clearing message queue for specified data types: {', '.join(args.clear_q_types)}")
        for data_type in args.clear_q_types:
            message_queue.clear_type(data_type)
        logger.info("Message queue cleared for specified data types.")

    # If a specific data type is provided, create a modified config with just that type
    if args.data_type:
        single_type_config = {args.data_type: config[args.data_type]}
        config = single_type_config
        logger.info(f"Processing single data type: {args.data_type}")

    etl_coordinator = ETLCoordinator(
        api_manager, 
        db_manager, 
        csv_exporter, 
        config, 
        retry_manager=retry_manager,
        message_queue=message_queue
    )

    try:
        await etl_coordinator.run()
    except Exception as e:
        logger.error(f"An error occurred during the ETL process: {str(e)}", exc_info=True)
    finally:
        await etl_coordinator.cleanup()

if __name__ == "__main__":
    asyncio.run(main())