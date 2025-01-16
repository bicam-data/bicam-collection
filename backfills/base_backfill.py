import asyncio
import csv
import logging
from datetime import datetime
import os
from typing import Any, Dict, List, Optional, Literal
import aiohttp
from pycon.govinfo.new_govinfo_abstractions import GovInfoAPI
from pycon.congress.congress_abstractions import PyCongress

class APIBackfillProcessor:
    def __init__(
        self, 
        input_csv_path: str,
        data_type: str,
        api_type: Literal['congress', 'govinfo'],
        api_keys: List[str],
        required_columns: List[str],
        logger: Optional[logging.Logger] = None
    ):
        self.input_csv_path = input_csv_path
        self.data_type = data_type
        self.output_directory = "/mnt/big_data/database-congress/api-congress/backfills/"
        self.api_type = api_type
        self.api_keys = api_keys
        self.required_columns = required_columns
        self.logger = logger or logging.getLogger(__name__)
        self.api_client = None
        self.session = None

        # Cache for API objects
        self.object_cache = {}

    async def get_item(self, row: Dict[str, str]) -> Optional[Any]:
        """Get item from cache or API"""
        cache_key = self.get_cache_key(row)
        
        if cache_key in self.object_cache:
            return self.object_cache[cache_key]
            
        try:
            item = await self.fetch_item(row)
            if item:
                self.object_cache[cache_key] = item
            return item
        except Exception as e:
            self.logger.error(f"Error fetching item for {row}: {str(e)}")
            return None

    async def fetch_item(self, row: Dict[str, str]) -> Optional[Any]:
        """Fetch item from API - override in subclass"""
        raise NotImplementedError

    def get_cache_key(self, row: Dict[str, str]) -> str:
        """Get cache key for a row - override in subclass"""
        raise NotImplementedError

    async def process_item(self, row: Dict[str, str]) -> Optional[Dict]:
        """Process item data - override in subclass"""
        raise NotImplementedError

    async def process_nested_data(self, item: Dict, api_item: Optional[Any] = None) -> Dict[str, List[Dict]]:
        """Process nested fields - override in subclass"""
        raise NotImplementedError

    async def process_related_data(self, item: Dict, api_item: Optional[Any] = None) -> Dict[str, List[Dict]]:
        """Process related data - override in subclass"""
        raise NotImplementedError

    async def process_batch(self, rows: List[Dict[str, str]], batch_size: int = 10):
        """Process a batch of rows with rate limiting"""
        main_items = []
        nested_data = {}
        related_data = {}
        
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            for row in batch:
                try:
                    self.logger.info(f"Processing row {row}")
                    # Get API item (using cache)
                    api_item = await self.get_item(row)
                    
                    # Process main item
                    item = await self.process_item(api_item=api_item) if api_item else None
                    if item:
                        main_items.append(item)
                        
                        # Process nested data using cached object
                        self.logger.info(f"Processing nested data for item {item}")
                        new_nested_data = await self.process_nested_data(
                            item, 
                            api_item=api_item  # Always pass the cached object
                        )
                        for key, values in new_nested_data.items():
                            if values:
                                nested_data.setdefault(key, []).extend(values)

                        # Process related data using cached object
                        self.logger.info(f"Processing related data for item {item}")
                        new_related_data = await self.process_related_data(
                            item,
                            api_item=api_item  # Always pass the cached object
                        )
                        for key, values in new_related_data.items():
                            if values:
                                related_data.setdefault(key, []).extend(values)
                    
                except Exception as e:
                    self.logger.error(f"Error processing row {row}: {str(e)}")
                    continue
            
            # Write results
            if main_items:
                await self.write_output_csv(main_items)
            
            for suffix, data in nested_data.items():
                await self.write_output_csv(data, suffix)
                
            for suffix, data in related_data.items():
                await self.write_output_csv(data, suffix)
            
            # Clear batch data
            main_items = []
            nested_data = {}
            related_data = {}
            
            # Clear object cache for the batch
            self.object_cache.clear()
            
            await asyncio.sleep(1)  # Rate limiting between batches
        
    def get_output_path(self, suffix: Optional[str] = None) -> str:
        """Get standardized output path for CSV files"""
        if suffix:
            return os.path.join(self.output_directory, f"{self.data_type}_{suffix}.csv")
        return os.path.join(self.output_directory, f"{self.data_type}.csv")
        
    async def initialize_api(self):
        """Initialize API client based on type"""
        self.session = aiohttp.ClientSession()
        if self.api_type == 'congress':
            self.api_client = PyCongress(
                api_keys=self.api_keys,
                session=self.session
            )
        else:  # govinfo
            self.api_client = GovInfoAPI(
                api_keys=self.api_keys,
                session=self.session
            )

    def validate_csv_columns(self, headers: List[str]) -> bool:
        """Validate that input CSV has required columns"""
        missing_columns = [col for col in self.required_columns if col not in headers]
        if missing_columns:
            self.logger.error(f"Missing required columns: {missing_columns}")
            return False
        return True

    async def read_input_csv(self) -> List[Dict[str, str]]:
        """Read and validate input CSV"""
        try:
            with open(self.input_csv_path, 'r') as f:
                reader = csv.DictReader(f)
                if not self.validate_csv_columns(reader.fieldnames):
                    raise ValueError("Invalid CSV format")
                return list(reader)
        except Exception as e:
            self.logger.error(f"Error reading input CSV: {str(e)}")
            raise

    async def write_output_csv(self, data: List[Dict], suffix: Optional[str] = None):
        """Write data to CSV with consistent naming"""
        if not data:
            return

        output_path = self.get_output_path(suffix)
        
        try:
            # Get all possible fields from the data
            fieldnames = set()
            for item in data:
                fieldnames.update(item.keys())
            fieldnames = sorted(list(fieldnames))
            
            # Check if file exists and get existing fieldnames
            try:
                with open(output_path, 'r') as f:
                    existing_reader = csv.DictReader(f)
                    fieldnames = sorted(list(set(fieldnames) | set(existing_reader.fieldnames)))
            except FileNotFoundError:
                pass

            # Write/append data
            mode = 'a' if os.path.exists(output_path) else 'w'
            with open(output_path, 'a', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
                if mode == 'w':
                    writer.writeheader()
                for item in data:
                    writer.writerow({k: (v if v is not None else '') for k, v in item.items()})

            self.logger.info(f"Successfully wrote {len(data)} rows to {output_path}")
            
        except Exception as e:
            self.logger.error(f"Error writing to {output_path}: {str(e)}")

    def get_unique_id(self, data: Dict) -> str:
        """Get unique identifier for an item - override in subclass"""
        raise NotImplementedError
    def clear_output_csv(self, prefix: Optional[str] = None):
        """Clear output CSV files from directory with prefix"""
        # Get the output directory path
        output_directory = self.output_directory
        
        # Create directory if it doesn't exist
        os.makedirs(output_directory, exist_ok=True)
        
        # Remove matching files
        for file in os.listdir(output_directory):
            if prefix and file.startswith(prefix):
                os.remove(os.path.join(output_directory, file))

    async def run(self, batch_size: int = 10, clear_output: bool = True):
        """Main processing loop"""
        try:
            if clear_output:
                self.clear_output_csv(prefix=self.data_type)
            await self.initialize_api()
            input_rows = await self.read_input_csv()
            self.logger.info(f"Processing {len(input_rows)} input rows")
            
            await self.process_batch(input_rows, batch_size)
            

            
        except Exception as e:
            self.logger.error(f"Error in processing: {str(e)}")
            raise
        finally:
            if self.session:
                await self.session.close()