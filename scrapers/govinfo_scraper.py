import argparse
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncIterator, Dict, List, Optional
from dotenv import load_dotenv
import aiohttp
import yaml
import json
from collections import defaultdict
from http.client import HTTPException
import os
import csv
import aiofiles
import asyncpg
import random

from pycon.govinfo.abstractions import GovInfoAPI
from pycon.govinfo.subcomponents import *
from pycon.api_models import ErrorResult

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class RetryManager:
    """
    Manages retry logic for failed API requests with exponential backoff.
    
    This class provides functionality to retry failed operations with an exponential
    backoff strategy, handle rate limiting, and manage wait times between retries.
    
    Attributes:
        max_retries (int): Maximum number of retry attempts (default: 5)
        base_delay (int): Initial delay in seconds between retries (default: 1)
        max_delay (int): Maximum delay in seconds between retries (default: 60)
    """
    
    def __init__(self, max_retries: int = 5, base_delay: int = 1, max_delay: int = 60):
        """
        Initialize the RetryManager with customizable retry parameters.
        
        Args:
            max_retries: Maximum number of retry attempts
            base_delay: Initial delay between retries in seconds
            max_delay: Maximum delay between retries in seconds
        """
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay

    async def retry_with_backoff(self, coroutine, *args, **kwargs):
        """
        Execute a coroutine with exponential backoff retry logic.
        
        Args:
            coroutine: The async function to execute
            *args: Positional arguments for the coroutine
            **kwargs: Keyword arguments for the coroutine
            
        Returns:
            The result of the successful coroutine execution
            
        Raises:
            Exception: The last exception encountered after all retries are exhausted
        """
        for attempt in range(self.max_retries):
            try:
                # Handle string conversion for endpoints and arguments
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
        """
        Wait until a rate limit reset time has passed.
        
        Args:
            rate_limit_reset: Datetime object indicating when the rate limit resets
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
            error: The error to check
            
        Returns:
            bool: True if the error is a rate limit error, False otherwise
        """
        return isinstance(error, HTTPException) and error.status_code == 429

class DatabaseManager:
    """
    Manages database connections and operations for the GovInfo ETL pipeline.
    
    This class handles PostgreSQL database connections, connection pooling,
    and provides methods for tracking ETL progress and error logging.
    
    Attributes:
        db_config (dict): Database configuration parameters
        pool (asyncpg.pool.Pool): Connection pool for database operations
    """
    
    def __init__(self, env_path: str = ".env"):
        """
        Initialize the DatabaseManager with database configuration from environment variables.
        
        Args:
            env_path: Path to the environment file containing database credentials
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

    async def create_pool(self):
        """Create a connection pool for database operations."""
        self.pool = await asyncpg.create_pool(**self.db_config)

    async def close_pool(self):
        """Close the database connection pool."""
        if self.pool:
            await self.pool.close()

    async def get_last_processed_date(self, data_type: str) -> Optional[str]:
        """
        Retrieve the last processed date for a specific data type.
        
        Args:
            data_type: The type of data being processed
            
        Returns:
            Optional[str]: ISO format date string of last processed date, or None if not found
        """
        async with self.pool.acquire() as conn:
            result = await conn.fetchval("""
                SELECT last_processed_date
                FROM __metadata.govinfo_last_processed_dates
                WHERE data_type = $1
            """, data_type)
        return result

    async def update_last_processed_date(self, data_type: str, last_date: str):
        """
        Update the last processed date for a specific data type.
        
        Args:
            data_type: The type of data being processed
            last_date: ISO format date string of the last processed date
        """
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO __metadata.govinfo_last_processed_dates (data_type, last_processed_date)
                VALUES ($1, $2)
                ON CONFLICT (data_type) DO UPDATE
                SET last_processed_date = EXCLUDED.last_processed_date
            """, data_type, last_date)
        logger.info(f"Updated last processed date for {data_type}: {last_date}")

    async def write_error_to_database(self, url: str, error_message: str, data_type: str):
        """
        Log an error to the database for tracking and analysis.
        
        Args:
            url: The URL that caused the error
            error_message: Description of the error
            data_type: The type of data being processed when the error occurred
        """
        timestamp = datetime.now(timezone.utc).isoformat()
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO __metadata.govinfo_errors (url, error, data_type, timestamp)
                VALUES ($1, $2, $3, $4)
            """, url, error_message, data_type, timestamp)

class CSVExporter:
    """
    Handles asynchronous CSV file export operations with field caching and concurrent write management.
    
    This class provides functionality to export data to CSV files with proper handling of
    complex data types, field management, and concurrent write operations. It includes
    features for maintaining consistent column structures and handling nested data.
    
    Attributes:
        base_directory (str): Base directory for CSV file output
        semaphore (asyncio.Semaphore): Controls concurrent file operations
        field_cache (Dict[str, List[str]]): Cache of field names for each data type
        max_concurrent_files (int): Maximum number of concurrent file operations
    """
    
    def __init__(self, base_directory: str, max_concurrent_files: int = 100):
        """
        Initialize the CSVExporter with specified directory and concurrency settings.
        
        Args:
            base_directory: Directory where CSV files will be saved
            max_concurrent_files: Maximum number of files that can be written to simultaneously
        """
        self.base_directory = base_directory
        os.makedirs(base_directory, exist_ok=True)
        self.semaphore = asyncio.Semaphore(max_concurrent_files)
        self.field_cache: Dict[str, List[str]] = {}

    async def export_to_csv(self, data_type: str, items: List[Dict]) -> None:
        """
        Export a list of items to a CSV file for a specific data type.
        
        Args:
            data_type: Type of data being exported (determines filename)
            items: List of dictionaries containing the data to export
            
        Raises:
            IOError: If there are issues writing to the CSV file
        """
        if not items:
            logger.info(f"No items to export for {data_type}")
            return

        filepath = os.path.join(self.base_directory, f"{data_type}.csv")
        file_exists = os.path.exists(filepath)

        # Get or create fieldnames for this data type
        if data_type not in self.field_cache:
            self.field_cache[data_type] = self._get_all_possible_fields(items)
        fieldnames = self.field_cache[data_type]

        # Process items for consistent structure
        processed_items = self._process_items_for_export(items, fieldnames)

        async with self.semaphore:
            try:
                await self._write_to_csv(filepath, fieldnames, processed_items, file_exists)
                logger.info(f"Exported {len(processed_items)} items to CSV: {filepath}")
            except Exception as e:
                logger.error(f"Error writing to CSV {filepath}: {str(e)}")
                raise

    def _get_all_possible_fields(self, items: List[Dict]) -> List[str]:
        """
        Extract and sort all unique fields from a list of items.
        
        Args:
            items: List of dictionaries to extract fields from
            
        Returns:
            List[str]: Sorted list of all unique fields
        """
        fields: Set[str] = set()
        for item in items:
            fields.update(item.keys())
        return sorted(list(fields))

    def _process_items_for_export(self, items: List[Dict], fieldnames: List[str]) -> List[Dict]:
        """
        Process items to ensure consistent structure and proper value formatting.
        
        Args:
            items: Raw items to process
            fieldnames: List of all possible fields
            
        Returns:
            List[Dict]: Processed items ready for CSV export
        """
        processed_items = []
        for item in items:
            processed_item = {}
            for field in fieldnames:
                value = item.get(field)
                
                # Handle special data types
                if isinstance(value, (list, dict)):
                    processed_item[field] = json.dumps(value) if value else None
                elif value == "":
                    processed_item[field] = None
                else:
                    processed_item[field] = value
                    
            processed_items.append(processed_item)
        return processed_items

    async def _write_to_csv(self, filepath: str, fieldnames: List[str], 
                           items: List[Dict], file_exists: bool) -> None:
        """
        Write processed items to a CSV file asynchronously.
        
        Args:
            filepath: Path to the CSV file
            fieldnames: List of column headers
            items: Processed items to write
            file_exists: Whether the file already exists
            
        Raises:
            IOError: If there are issues writing to the file
        """
        async with aiofiles.open(filepath, 'a' if file_exists else 'w', newline='') as f:
            # Create output buffer
            output = []
            
            # Write headers if new file
            if not file_exists:
                output.append(','.join(f'"{field}"' for field in fieldnames) + '\n')
            
            # Write data rows
            for item in items:
                row = []
                for field in fieldnames:
                    value = item.get(field)
                    if value is None:
                        row.append('')
                    else:
                        # Escape and quote values properly
                        value = str(value).replace('"', '""')
                        row.append(f'"{value}"')
                output.append(','.join(row) + '\n')
            
            # Write everything at once
            await f.write(''.join(output))

    async def close(self) -> None:
        """
        Perform any necessary cleanup operations.
        
        Currently a placeholder for future cleanup operations that might be needed.
        """
        pass

class APIManager:
    """
    Manages API interactions, rate limiting, and session handling for the GovInfo API.
    
    This class coordinates API access across multiple API keys, handles rate limiting,
    manages HTTP sessions, and provides robust error handling for API interactions.
    
    Attributes:
        session_pool (Dict): Pool of HTTP sessions for different data types
        all_api_keys (List[str]): List of available API keys
        govinfo_instances (Dict): Dictionary of GovInfo API instances
        db_manager: Database manager instance for error logging
        semaphore (asyncio.Semaphore): Controls concurrent API requests
        api_key_index (int): Current index in the API key rotation
        api_key_status (Dict): Tracks the status of each API key
    """
    
    def __init__(self, db_manager, env_path: str = ".env.info"):
        """
        Initialize the APIManager with configuration and dependencies.
        
        Args:
            db_manager: Database manager instance for error logging
            env_path: Path to environment file containing API keys
        
        Raises:
            ValueError: If no API keys are found in the environment file
        """
        self.session_pool: Dict[str, aiohttp.ClientSession] = {}
        self.all_api_keys = self._load_api_keys(env_path)
        if not self.all_api_keys:
            raise ValueError("No API keys found. Please check your environment file.")
            
        self.govinfo_instances: Dict[str, GovInfoAPI] = {}
        self.db_manager = db_manager
        self.semaphore = asyncio.Semaphore(10)
        self.data_type_stats: Dict[str, Dict] = {}
        self.api_key_index = 0
        self.api_key_status = {
            key: {"sleeping": False, "sleep_until": None} 
            for key in self.all_api_keys
        }

    def _load_api_keys(self, env_path: str) -> List[str]:
        """
        Load API keys from environment file.
        
        Args:
            env_path: Path to environment file
            
        Returns:
            List[str]: List of API keys found in environment
        """
        load_dotenv(env_path)
        return [
            value for key, value in os.environ.items()
            if key.startswith("GOVINFO_API_KEY")
        ]

    async def set_api_key_sleeping(self, api_key: str, sleep_duration: int) -> None:
        """
        Mark an API key as sleeping for a specified duration.
        
        Args:
            api_key: The API key to set as sleeping
            sleep_duration: Duration in seconds to sleep
        """
        self.api_key_status[api_key]["sleeping"] = True
        self.api_key_status[api_key]["sleep_until"] = (
            datetime.now(timezone.utc) + timedelta(seconds=sleep_duration)
        )

    async def wake_up_api_key(self, api_key: str) -> None:
        """
        Mark an API key as active (no longer sleeping).
        
        Args:
            api_key: The API key to wake up
        """
        self.api_key_status[api_key]["sleeping"] = False
        self.api_key_status[api_key]["sleep_until"] = None

    async def get_active_api_keys(self) -> List[str]:
        """
        Get list of currently active (non-sleeping) API keys.
        
        Returns:
            List[str]: List of active API keys
        """
        now = datetime.now(timezone.utc)
        active_keys = []
        
        for key, status in self.api_key_status.items():
            if not status["sleeping"] or (
                status["sleep_until"] and status["sleep_until"] <= now
            ):
                active_keys.append(key)
                if status["sleeping"]:
                    await self.wake_up_api_key(key)
                    
        return active_keys

    async def fetch_full_item(self, item: Any) -> Optional[Any]:
        """
        Fetch complete details for an item from the API.
        
        Args:
            item: Item object containing basic information
            
        Returns:
            Optional[Any]: Complete item object or None if fetch fails
            
        Note:
            Implements retry logic and error logging for failed fetches
        """
        max_retries = 1
        
        for attempt in range(max_retries):
            async with self.semaphore:
                try:
                    if hasattr(item, 'package_url'):
                        full_item_result = await item._adapter.retrieve(
                            item.package_url, 
                            override=True
                        )
                        
                        if isinstance(full_item_result, ErrorResult):
                            logger.error(
                                f"Error fetching full item details: {full_item_result.error_message}"
                            )
                            if attempt == max_retries - 1:
                                await self.db_manager.write_error_to_database(
                                    url=item.package_url,
                                    error_message=full_item_result.error_message,
                                    data_type=type(item).__name__
                                )
                            else:
                                await self.exponential_backoff(attempt)
                                continue

                        # Create full item instance and merge data
                        item_class = type(item)
                        full_item = item_class(
                            data=full_item_result.data,
                            _pagination=item._pagination,
                            _adapter=item._adapter
                        )
                        
                        # Fill missing fields from original item
                        self._merge_missing_fields(full_item, item)
                        
                        return full_item
                    else:
                        logger.warning(
                            f"Item of type {type(item).__name__} does not have package_url attribute"
                        )
                        return item
                        
                except Exception as e:
                    logger.error(
                        f"Error fetching full item details (attempt {attempt + 1}): {str(e)}", 
                        exc_info=True
                    )
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
    def _merge_missing_fields(full_item: Any, original_item: Any) -> None:
        """
        Copy non-missing fields from original item to full item.
        
        Args:
            full_item: Complete item object that may have missing fields
            original_item: Original item object with potential fill-in values
        """
        for attr in dir(full_item):
            if not attr.startswith('_') and getattr(full_item, attr, None) == 'MISSING':
                original_value = getattr(original_item, attr, None)
                if original_value is not None and original_value != 'MISSING':
                    setattr(full_item, attr, original_value)

    @staticmethod
    async def exponential_backoff(attempt: int, max_delay: int = 60) -> None:
        """
        Implement exponential backoff delay between retries.
        
        Args:
            attempt: Current attempt number
            max_delay: Maximum delay in seconds
        """
        delay = min(2**attempt + random.uniform(0, 1), max_delay)
        await asyncio.sleep(delay)

    async def get_data(self, data_type: str, **kwargs) -> AsyncIterator[List[Any]]:
        """
        Retrieve data from the API with pagination and error handling.
        
        Args:
            data_type: Type of data to retrieve
            **kwargs: Additional parameters for the API request
            
        Yields:
            List[Any]: Batches of retrieved items
            
        Note:
            Implements pagination, error handling, and progress tracking
        """
        govinfo = self.govinfo_instances[data_type]
        method_name = f"get_bulk_{data_type}"
        method = getattr(govinfo, method_name)
        total_count = None
        processed_count = 0
        retry_manager = RetryManager()

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
                    
                for item in items:
                    logger.debug(f"Fetching full item: {item}")
                    try:
                        full_item = await self.fetch_full_item(item)
                        if full_item:
                            full_items.append(full_item)
                    except Exception as e:
                        logger.error(
                            f"Error fetching full item details for {data_type}: {str(e)}", 
                            exc_info=True
                        )

                if full_items:
                    yield full_items
                    processed_count += len(full_items)
                    if total_count:
                        logger.info(
                            f"Progress for {data_type}: {processed_count}/{total_count} "
                            f"({processed_count/total_count*100:.2f}%)"
                        )

                if not items[0]._pagination.get('next') or len(items) > 1000:
                    break

                kwargs['next_url'] = items[0]._pagination['next']

            except Exception as e:
                logger.error(f"Error in get_data for {data_type}: {str(e)}", exc_info=True)
                yield ErrorResult(
                    url=kwargs.get('next_url', f"bulk_{data_type}"), 
                    error_message=str(e)
                )
                await asyncio.sleep(60)

            if 'next_url' not in kwargs:
                break

    async def create_govinfo_instance(self, data_type: str, api_keys: List[str]) -> GovInfoAPI:
        """
        Create a new GovInfo API instance for a specific data type.
        
        Args:
            data_type: Type of data the instance will handle
            api_keys: List of API keys to use
            
        Returns:
            GovInfoAPI: Configured API instance
        """
        session = await self.create_session(data_type)
        self.govinfo_instances[data_type] = GovInfoAPI(
            api_keys, 
            session=session, 
            db_pool=self.db_manager.pool
        )
        self.data_type_stats[data_type] = {
            "items_processed": 0,
            "items_remaining": 0,
            "start_time": datetime.now(timezone.utc)
        }
        logger.info(f"Created GovInfo instance for {data_type}")
        return self.govinfo_instances[data_type]

    async def create_session(self, data_type: str) -> aiohttp.ClientSession:
        """
        Create or retrieve an HTTP session for a data type.
        
        Args:
            data_type: Type of data the session will handle
            
        Returns:
            aiohttp.ClientSession: HTTP session
        """
        if data_type not in self.session_pool:
            self.session_pool[data_type] = aiohttp.ClientSession(
                raise_for_status=True,
                timeout=aiohttp.ClientTimeout(total=300)
            )
        return self.session_pool[data_type]

    async def switch_api_key(self, data_type: str) -> None:
        """
        Rotate to the next available API key for a data type.
        
        Args:
            data_type: Type of data to switch API key for
        """
        self.api_key_index = (self.api_key_index + 1) % len(self.all_api_keys)
        new_key = self.all_api_keys[self.api_key_index]
        self.govinfo_instances[data_type]._adapter.api_key = new_key
        logger.info(f"Switched to API key: {new_key[:5]}... for {data_type}")

    async def close_all_sessions(self) -> None:
        """Close all active HTTP sessions."""
        for session in self.session_pool.values():
            if not session.closed:
                await session.close()
        self.session_pool.clear()

class ETLCoordinator:
    """
    Coordinates the entire ETL (Extract, Transform, Load) pipeline for GovInfo data.
    
    This class orchestrates the complete data pipeline process, managing the extraction
    of data from the GovInfo API, transformation of that data into a consistent format,
    and loading into both CSV files and a database. It handles parallel processing,
    error recovery, and progress tracking.
    
    The pipeline follows these main phases:
    1. Initialization and configuration loading
    2. Data type processing coordination
    3. Extraction from API with pagination
    4. Transformation of raw data
    5. Loading into destination formats
    6. Progress tracking and error handling
    
    Attributes:
        api_manager: Manager for API interactions
        db_manager: Manager for database operations
        csv_exporter: Handler for CSV file exports
        config (Dict): Pipeline configuration
        num_workers (int): Number of parallel workers
        retry_manager: Manager for retry operations
        data_type_queue (asyncio.Queue): Queue for processing data types
    """
    
    def __init__(
        self,
        api_manager,
        db_manager,
        csv_exporter,
        config_file: str,
        num_workers: int = 10,
        retry_manager = None
    ):
        """
        Initialize the ETL coordinator with necessary components and configuration.
        
        Args:
            api_manager: Manager for API interactions
            db_manager: Manager for database operations
            csv_exporter: Handler for CSV file exports
            config_file: Path to YAML configuration file
            num_workers: Number of parallel workers to use
            retry_manager: Optional custom retry manager
        """
        self.api_manager = api_manager
        self.db_manager = db_manager
        self.csv_exporter = csv_exporter
        self.config = self.load_config(config_file)
        self.num_workers = num_workers
        self.retry_manager = retry_manager or RetryManager()
        self.data_type_queue = asyncio.Queue()

    def load_config(self, config_file: str) -> Dict:
        """
        Load and parse the YAML configuration file.
        
        Args:
            config_file: Path to configuration file
            
        Returns:
            Dict: Parsed configuration
            
        Raises:
            yaml.YAMLError: If configuration file is invalid
            FileNotFoundError: If configuration file doesn't exist
        """
        with open(config_file, 'r') as f:
            return yaml.safe_load(f)

    def get_main_data_types(self) -> List[str]:
        """
        Get list of primary data types to process from configuration.
        
        Returns:
            List[str]: List of data type identifiers marked as main
        """
        return [
            data_type
            for data_type, config in self.config.items()
            if config.get('main', False)
        ]

    async def initialize_pipeline(self) -> None:
        """
        Initialize all components needed for the ETL pipeline.
        
        This method:
        1. Creates database connection pool
        2. Sets up API instances for each data type
        3. Distributes API keys across data types
        4. Prepares monitoring and logging
        
        Raises:
            Exception: If initialization of any component fails
        """
        await self.db_manager.create_pool()
        main_data_types = self.get_main_data_types()
        all_api_keys = self.api_manager.all_api_keys
        
        # Calculate keys per data type for even distribution
        keys_per_type = max(1, len(all_api_keys) // len(main_data_types))
        
        # Distribute API keys across data types
        for i, data_type in enumerate(main_data_types):
            start_index = (i * keys_per_type) % len(all_api_keys)
            assigned_keys = all_api_keys[start_index : start_index + keys_per_type]
            
            # Handle wraparound for last group
            if len(assigned_keys) < keys_per_type:
                assigned_keys += all_api_keys[: keys_per_type - len(assigned_keys)]
            
            await self.api_manager.create_govinfo_instance(data_type, assigned_keys)

        logger.info(f"Initialized govinfo instances for data types: {', '.join(main_data_types)}")

    async def run_pipeline(self) -> None:
        """
        Execute the complete ETL pipeline.
        
        This method coordinates the entire ETL process:
        1. Initializes all components
        2. Starts parallel processing of data types
        3. Monitors progress
        4. Handles cleanup
        
        Note:
            This is the main entry point for running the ETL process
        """
        try:
            await self.initialize_pipeline()
            await self.process_all_data_types()
        except Exception as e:
            logger.error(f"An error occurred during the ETL process: {str(e)}", exc_info=True)
        finally:
            await self.cleanup_pipeline()

    async def process_all_data_types(self) -> None:
        """
        Coordinate parallel processing of all data types.
        
        This method:
        1. Queues all data types for processing
        2. Spawns worker tasks
        3. Manages worker lifecycle
        4. Ensures all data types are processed
        """
        main_data_types = self.get_main_data_types()
        
        # Queue all data types for processing
        for data_type in main_data_types:
            await self.data_type_queue.put(data_type)

        # Create and manage workers
        workers = [
            asyncio.create_task(self.worker_process()) 
            for _ in range(self.num_workers)
        ]
        
        # Wait for queue to be empty
        await self.data_type_queue.join()
        
        # Clean up workers
        for worker in workers:
            worker.cancel()
        await asyncio.gather(*workers, return_exceptions=True)

    async def worker_process(self) -> None:
        """
        Individual worker process for handling data types.
        
        This method:
        1. Continuously pulls data types from queue
        2. Processes each data type
        3. Handles errors and retries
        4. Marks tasks as complete
        """
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

    async def cleanup_pipeline(self) -> None:
        """
        Perform cleanup operations after pipeline completion.
        
        This method ensures proper cleanup of:
        1. Database connections
        2. API sessions
        3. File handles
        4. Other resources
        """
        logger.info("Performing cleanup...")
        await self.api_manager.close_all_sessions()
        await self.db_manager.close_pool()
        logger.info("Cleanup complete.")
        
"""
govinfo_etl_processing.py
Data processing methods for the GovInfo ETL Coordinator.
"""

class ETLCoordinator:
    """ETLCoordinator data processing methods."""
    
    async def process_data_type(self, data_type: str) -> None:
        """
        Process all data for a specific data type, managing the complete extraction to load cycle.
        
        This method coordinates:
        1. Retrieval of last processed state
        2. Date range determination
        3. Batch processing
        4. Progress tracking
        5. Error handling
        
        Args:
            data_type: The type of data to process
            
        Raises:
            Exception: If processing fails after retries
        """
        try:
            # Get last processed date and initialize date range
            last_processed_date = await self.db_manager.get_last_processed_date(data_type)
            kwargs, start_date = await self._get_date_range(data_type, last_processed_date)
            
            # Set up data generator and process batches
            data_generator = self.api_manager.get_data(data_type, **kwargs)
            
            while True:
                try:
                    items_batch = await self._fetch_batch_with_dynamic_timeout(data_generator)
                    if not items_batch:
                        logger.info(f"No more items to process for {data_type}")
                        break

                    await self.process_batch(data_type, items_batch)
                    
                    # Update progress tracking
                    batch_latest_date = self._get_latest_date_from_batch(items_batch)
                    if batch_latest_date:
                        await self.db_manager.update_last_processed_date(
                            data_type, 
                            batch_latest_date.isoformat()
                        )

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

    async def process_batch(self, data_type: str, items_batch: List[Any]) -> None:
        """
        Process a batch of items for a specific data type.
        
        This method:
        1. Processes individual items
        2. Handles nested data structures
        3. Manages granule processing
        4. Coordinates CSV export
        
        Args:
            data_type: Type of data being processed
            items_batch: List of items to process
        """
        logger.info(f"Processing batch of {len(items_batch)} items for {data_type}")
        processed_items = []
        nested_data = defaultdict(list)

        for item in items_batch:
            try:
                # Process main item
                processed_item = await self.process_item(item, data_type)
                if processed_item:
                    processed_items.append(processed_item)

                # Handle nested fields
                await self._process_nested_fields(item, data_type, processed_item, nested_data)

                # Process granules if available
                if hasattr(item, 'get_granules'):
                    await self.process_granules(data_type, item)

            except Exception as e:
                logger.error(f"Error processing item for {data_type}: {str(e)}")
                continue

        # Export processed data
        await self._export_processed_data(data_type, processed_items, nested_data)

    async def process_item(self, item: Any, data_type: str) -> Dict[str, Any]:
        """
        Process an individual item, transforming it into the desired format.
        
        This method:
        1. Extracts relevant fields
        2. Transforms data types
        3. Validates data
        4. Handles special field processing
        
        Args:
            item: Raw item to process
            data_type: Type of data being processed
            
        Returns:
            Dict[str, Any]: Processed item in standardized format
        """
        result = {}
        fields = self.config.get(data_type, {}).get('fields', [])
        nested_fields = self.config.get(data_type, {}).get('nested_fields', [])

        # Process main fields
        for field in fields:
            if hasattr(item, field):
                value = getattr(item, field)
                result[field] = self._serialize_value(value)

        # Process nested fields
        if isinstance(nested_fields, list):
            for nested_field in nested_fields:
                field_name, nested_config = self._extract_nested_field_config(nested_field)
                if hasattr(item, field_name):
                    nested_value = getattr(item, field_name)
                    result.update(
                        self._process_nested_field(field_name, nested_value, data_type, nested_config)
                    )

        return result

    async def process_granules(self, data_type: str, item: Any) -> None:
        """
        Process granules (sub-items) associated with a main item.
        
        This method:
        1. Retrieves granules from the API
        2. Processes each granule
        3. Links granules to parent item
        4. Handles export of granule data
        
        Args:
            data_type: Type of data being processed
            item: Parent item containing granules
        """
        granules_data_type = f"{data_type}_granules"
        granules_url = getattr(item, 'granules_url', None)

        if granules_url:
            try:
                async for granule in item.get_granules():
                    processed_granule = await self.process_item(granule, granules_data_type)

                    # Link granule to parent
                    self._link_granule_to_parent(processed_granule, item, data_type)

                    # Process nested fields for granule
                    await self.process_nested_fields(granules_data_type, granule, processed_granule)

                    # Export granule data
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

    async def process_nested_fields(self, data_type: str, item: Any, processed_item: Dict[str, Any]) -> None:
        """
        Process nested fields within an item.
        
        This method:
        1. Identifies nested fields from configuration
        2. Extracts nested data
        3. Processes each nested field
        4. Manages export of nested data
        
        Args:
            data_type: Type of data being processed
            item: Item containing nested fields
            processed_item: Already processed main item data
        """
        nested_fields = self.config.get(data_type, {}).get('nested_fields', [])
        parent_ids = self._get_parent_ids(item, data_type)
        
        if isinstance(nested_fields, list):
            for nested_field in nested_fields:
                field_name, nested_config = self._extract_nested_field_config(nested_field)
                
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
                            await self.csv_exporter.export_to_csv(
                                nested_data_type, 
                                processed_nested_data
                            )

    def _extract_nested_field_config(self, nested_field: Union[str, Dict]) -> Tuple[str, Dict]:
        """
        Extract field name and configuration from nested field specification.
        
        Args:
            nested_field: Either a string field name or dict with configuration
            
        Returns:
            Tuple[str, Dict]: Field name and its configuration
        """
        if isinstance(nested_field, dict):
            field_name = next(iter(nested_field))
            return field_name, nested_field[field_name]
        return nested_field, {}

    def _link_granule_to_parent(self, granule: Dict, parent: Any, data_type: str) -> None:
        """
        Link a granule to its parent item through ID fields.
        
        Args:
            granule: Processed granule data
            parent: Parent item
            data_type: Type of data being processed
        """
        parent_id_fields = self.config.get(data_type, {}).get('id_fields', ['id'])
        for id_field in parent_id_fields:
            parent_id = getattr(parent, id_field, None)
            if parent_id:
                granule[f"parent_{id_field}"] = parent_id

    async def _export_processed_data(
        self, 
        data_type: str, 
        processed_items: List[Dict], 
        nested_data: Dict[str, List]
    ) -> None:
        """
        Export all processed data to appropriate destinations.
        
        Args:
            data_type: Type of data being exported
            processed_items: Main items to export
            nested_data: Nested data items to export
        """
        # Export main items
        if processed_items:
            await self.csv_exporter.export_to_csv(data_type, processed_items)

        # Export nested data
        for nested_field, nested_items in nested_data.items():
            if nested_items:
                await self.csv_exporter.export_to_csv(
                    f"{data_type}_{nested_field}", 
                    nested_items
                )
                
"""
govinfo_etl_utils.py
Utility methods for the GovInfo ETL Coordinator, handling data transformation,
tracking, and error management.
"""

from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Tuple, Optional, Union
import json
import logging

logger = logging.getLogger(__name__)

class ETLCoordinator:
    """ETLCoordinator utility methods for data handling and pipeline management."""

    async def _get_date_range(
        self, 
        data_type: str, 
        last_processed_date: Optional[str]
    ) -> Tuple[Dict[str, str], datetime]:
        """
        Determine the date range for data extraction.
        
        This method calculates the appropriate date range for data extraction,
        considering the last processed date and adding an overlap window to
        ensure no data is missed.
        
        Args:
            data_type: Type of data being processed
            last_processed_date: ISO format date string of last processed date
            
        Returns:
            Tuple[Dict[str, str], datetime]: API parameters and start date
        """
        end_date = datetime.now(tz=timezone.utc)
        overlap_window = timedelta(hours=1)

        if last_processed_date:
            # Remove the 'Z' and add UTC timezone
            start_date = datetime.fromisoformat(
                last_processed_date.rstrip('Z')
            ).replace(tzinfo=timezone.utc) - overlap_window
        else:
            start_date = datetime(1789, 1, 1, tzinfo=timezone.utc)

        latest_date_in_batch = start_date

        kwargs = {
            "start_date": self.format_date(start_date),
            "end_date": self.format_date(end_date)
        }

        return kwargs, latest_date_in_batch

    async def _fetch_batch_with_dynamic_timeout(
        self, 
        data_generator, 
        base_timeout: int = 3600
    ) -> Optional[List[Any]]:
        """
        Fetch a batch of data with dynamic timeout handling based on API key availability.
        
        This method manages the fetching of data batches, handling API key rotation
        and implementing appropriate waiting periods when all keys are exhausted.
        
        Args:
            data_generator: Generator providing data batches
            base_timeout: Base timeout in seconds
            
        Returns:
            Optional[List[Any]]: Batch of items or None if fetch fails
        """
        while True:
            active_keys = await self.api_manager.get_active_api_keys()
            if not active_keys:
                # Calculate wait time based on earliest key recovery
                sleep_time = min(
                    status["sleep_until"] 
                    for status in self.api_manager.api_key_status.values() 
                    if status["sleeping"]
                )
                wait_time = (sleep_time - datetime.now(timezone.utc)).total_seconds()
                logger.info(f"All API keys are sleeping. Waiting for {wait_time} seconds.")
                await asyncio.sleep(wait_time)
            else:
                try:
                    return await asyncio.wait_for(
                        data_generator.__anext__(), 
                        timeout=base_timeout
                    )
                except asyncio.TimeoutError:
                    logger.warning(
                        "Timeout occurred, but some API keys might still be active. Retrying..."
                    )

    def _get_latest_date_from_batch(self, items_batch: List[Any]) -> Optional[datetime]:
        """
        Find the most recent date in a batch of items.
        
        This method examines multiple date fields to determine the latest
        date present in the batch of items.
        
        Args:
            items_batch: List of items to examine
            
        Returns:
            Optional[datetime]: Most recent date found or None if no valid dates
        """
        date_fields = ["last_modified", "dateIssued", "date_issued"]
        latest_date = None
        
        for item in items_batch:
            for field in date_fields:
                date_str = getattr(item, field, None)
                if date_str:
                    try:
                        parsed_date = datetime.fromisoformat(date_str.rstrip('Z'))
                        if parsed_date.tzinfo is None:
                            parsed_date = parsed_date.replace(tzinfo=timezone.utc)
                        if latest_date is None or parsed_date > latest_date:
                            latest_date = parsed_date
                    except (ValueError, AttributeError) as e:
                        logger.warning(
                            f"Error parsing date '{date_str}' for field '{field}': {str(e)}"
                        )
        
        return latest_date

    def _prepare_nested_data_for_csv(
        self, 
        nested_value: Any, 
        parent_ids: Dict[str, Any], 
        nested_config: Dict
    ) -> List[Dict[str, Any]]:
        """
        Prepare nested data structures for CSV export.
        
        This method handles the transformation of complex nested data structures
        into a format suitable for CSV export, maintaining relationships with
        parent records.
        
        Args:
            nested_value: The nested data to process
            parent_ids: Dictionary of parent record identifiers
            nested_config: Configuration for nested data processing
            
        Returns:
            List[Dict[str, Any]]: Processed nested data ready for export
        """
        processed_data = []
        
        try:
            if nested_value is None:
                return []
                
            if isinstance(nested_value, (list, tuple)):
                processed_data.extend(
                    self._process_nested_list(nested_value, parent_ids)
                )
            else:
                processed_data.append(
                    self._process_nested_single(nested_value, parent_ids)
                )
            
            return processed_data
            
        except Exception as e:
            logger.error(f"Error processing nested data: {str(e)}", exc_info=True)
            return []

    def _process_nested_list(
        self, 
        nested_list: List[Any], 
        parent_ids: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Process a list of nested values.
        
        Args:
            nested_list: List of nested values to process
            parent_ids: Dictionary of parent record identifiers
            
        Returns:
            List[Dict[str, Any]]: Processed nested values
        """
        processed_items = []
        
        for item in nested_list:
            if isinstance(item, (str, int, float, bool)):
                processed_items.append({
                    **parent_ids,
                    'value': str(item)
                })
            else:
                item_data = self._extract_fields(item)
                if item_data:
                    processed_items.append({
                        **parent_ids,
                        **item_data
                    })
                    
        return processed_items

    def _process_nested_single(
        self, 
        nested_value: Any, 
        parent_ids: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Process a single nested value.
        
        Args:
            nested_value: Single nested value to process
            parent_ids: Dictionary of parent record identifiers
            
        Returns:
            Dict[str, Any]: Processed nested value
        """
        if isinstance(nested_value, (str, int, float, bool)):
            return {
                **parent_ids,
                'value': str(nested_value)
            }
        else:
            item_data = self._extract_fields(nested_value)
            if item_data:
                return {**parent_ids, **item_data}
            return {**parent_ids}

    def _extract_fields(self, item: Any) -> Dict[str, Any]:
        """
        Extract valid fields from an object.
        
        This method extracts and validates fields from various object types,
        handling different data structures appropriately.
        
        Args:
            item: Object to extract fields from
            
        Returns:
            Dict[str, Any]: Dictionary of valid fields and values
        """
        if hasattr(item, '__dict__'):
            return self._extract_object_fields(item)
        elif isinstance(item, dict):
            return self._extract_dict_fields(item)
        else:
            return self._extract_simple_value(item)

    def _extract_object_fields(self, obj: Any) -> Dict[str, Any]:
        """
        Extract fields from an object with __dict__ attribute.
        
        Args:
            obj: Object to extract fields from
            
        Returns:
            Dict[str, Any]: Dictionary of valid fields
        """
        data = {
            k: v for k, v in vars(obj).items() 
            if v is not None and v != 'MISSING' and v != ''
        }
        return data if data else {}

    def _extract_dict_fields(self, d: Dict) -> Dict[str, Any]:
        """
        Extract fields from a dictionary.
        
        Args:
            d: Dictionary to extract fields from
            
        Returns:
            Dict[str, Any]: Dictionary of valid fields
        """
        data = {
            k: v for k, v in d.items() 
            if v is not None and v != 'MISSING' and v != ''
        }
        return data if data else {}

    def _extract_simple_value(self, value: Any) -> Dict[str, Any]:
        """
        Extract value from a simple type.
        
        Args:
            value: Simple value to process
            
        Returns:
            Dict[str, Any]: Dictionary containing the value if valid
        """
        str_value = str(value)
        return {'value': str_value} if str_value.strip() else {}

    def _serialize_value(self, value: Any) -> Any:
        """
        Serialize a value for storage or transmission.
        
        This method handles the serialization of various data types into
        formats suitable for storage or transmission.
        
        Args:
            value: Value to serialize
            
        Returns:
            Any: Serialized value
        """
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

    @staticmethod
    def format_date(date: datetime) -> str:
        """
        Format a datetime object as a string.
        
        Args:
            date: DateTime object to format
            
        Returns:
            str: Formatted date string
        """
        return date.strftime("%Y-%m-%dT%H:%M:%SZ")

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