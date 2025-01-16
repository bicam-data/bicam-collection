from abc import ABC
from dataclasses import dataclass
from datetime import datetime, timezone
import re
from typing import Any, AsyncIterator, Dict, List, Optional
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

from pycon.adapter import RestAdapter
from pycon.exceptions import PyCongressException
from pycon.api_models import Result, ErrorResult


@dataclass(kw_only=True)
class Retriever(ABC):
    """Abstract class used for retrieving and processing API
    responses from the Congress API. This class is meant to be
    inherited by other classes that will implement the methods
    for retrieving and processing the data - some classes that
    require preprocessing of the responses will have class methods
    that can be passed to the _get_items method, while others may not
    be part of classes at all.

    Args:
        ABC (class): Abstract Base Class
    """

    _adapter: RestAdapter
    data: Dict[str, Any]

    def __init__(self, data: Dict[str, Any], _adapter: RestAdapter):
        self._adapter = _adapter
        self.data = data
        self._camel_case_pattern1 = re.compile(r'(.)([A-Z][a-z]+)')
        self._camel_case_pattern2 = re.compile(r'([a-z0-9])([A-Z])')

    def _camel_to_snake(self, name):
        name = self._camel_case_pattern1.sub(r'\1_\2', name)
        name = self._camel_case_pattern2.sub(r'\1_\2', name)
        return name.lower()

    def _dash_to_snake(self, name):
        return name.replace('-', '_')

    def _parse_data(self):
        for key, value in self.data.items():
            if "-" in key:
                key = self._dash_to_snake(key)
            key = self._camel_to_snake(key)
            setattr(self, f"_{key}" if not key.startswith("_") or key != 'data' else key, value)

    def to_dict(self):
        return {k: v for k, v in self.__dict__.items() if not k.startswith('_')}

    def _ensure_adapter(self):
        """Ensure that the object has been initialized with a RestAdapter object.

        Raises:
            PyCongressException
        """
        if not self._adapter:
            raise PyCongressException(
                "Object must be initialized with a RestAdapter object."
            )

    def _construct_endpoint(self, path: str):
        """Construct the endpoint for the API request.
        This differs depending on the class that inherits this class.

        Args:
            path (str): path to the endpoint
        """
        pass

    async def _get_items(
        self,
        path: str,
        cls: Optional[Any] = None,
        verbose: bool = False,
        next_url: Optional[str] = None,
        page_size: Optional[int] = None,
        _id_package_init: Optional[tuple] = None,
    ) -> AsyncIterator[Any]:
        self._ensure_adapter()

        urls = []
        if next_url:
            urls = [next_url]
        else:
            if hasattr(self, path) and path[-1] != 's':
                urls = [getattr(self, path)]
            elif hasattr(self, path) and path[-1] == 's':
                urls = getattr(self, path)
            else:
                raise PyCongressException(f"No attribute found for {path}")

        if not urls:
            return

        for base_url in urls:
            url = base_url
            while url:
                if page_size:
                    url = self._update_page_size(url, page_size)

                try:
                    response = await self._adapter.retrieve(url, override=True)
                    if isinstance(response, ErrorResult):
                        self._adapter._logger.error(f"Error retrieving items: {response.error_message}")
                        await self._write_error_to_database(url, response.error_message, "congressional")
                        yield response
                        break  # Exit the while loop for this URL
                    items = self._extract_items(response.data)

                    if cls:
                        processed_items = await self._process_items(cls, items, verbose, _id_package_init, response.pagination)
                    else:
                        processed_items = items

                    for item in processed_items:
                        yield item

                    url = response.pagination.get('next') if response.pagination else None

                except Exception as e:
                    error = ErrorResult(url=url, error_message=str(e))
                    self._adapter._logger.error(f"Unexpected error: {error.error_message}")
                    yield error
                    await self._write_error_to_database(url, str(e))
                    break  # Exit the while loop for this URL



    def _update_page_size(self, url: str, page_size: int) -> str:
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)
        query_params['limit'] = [str(page_size)]
        new_query = urlencode(query_params, doseq=True)
        return urlunparse(parsed_url._replace(query=new_query))

    def _add_granule_class(self, url: str, granule_class: str) -> str:
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)
        query_params['granuleClass'] = [granule_class]
        new_query = urlencode(query_params, doseq=True)
        return urlunparse(parsed_url._replace(query=new_query))

    async def _get_granules(
        self,
        url: str,
        cls: Any,
        verbose: bool = False,
        page_size: int = 1000,
        granule_class: Optional[str] = None,
    ) -> AsyncIterator[Any]:
        self._ensure_adapter()

        while url:
            url = self._update_page_size(url, page_size)

            if "api_key" not in url:
                url = url + f"&api_key={self._adapter.current_api_key}"

            if granule_class:
                url = self._add_granule_class(url, granule_class)

            try:
                response = await self._adapter.retrieve(url, override=True)

                if isinstance(response, ErrorResult):
                    self._adapter._logger.error(f"Error retrieving granules: {response.error_message}")
                    await self._write_error_to_database(url, response.error_message)
                    yield response
                    break

                if response.data.get("count") == 0:
                    self._adapter._logger.debug("No granules found, skipping.")
                    break

                granules = response.data.get("granules", [])
                for granule_data in granules:
                    if verbose:
                        granule_link = granule_data.get("granuleLink")
                        if granule_link:
                            granule_url_with_key = f"{granule_link}?api_key={self._adapter.current_api_key}"
                            granule_response = await self._adapter.retrieve(granule_url_with_key, override=True)
                            if isinstance(granule_response, Result):
                                granule_data = granule_response.data

                    yield cls(data=granule_data, _pagination=response.pagination, _adapter=self._adapter)

                url = response.pagination.get('next') if response.pagination else None

            except Exception as e:
                error = ErrorResult(url=url, error_message=str(e))
                self._adapter._logger.error(f"Unexpected error: {error.error_message}", exc_info=True)
                yield error
                await self._write_error_to_database(url, str(e), "govinfo")
                break

    async def _write_error_to_database(self, url: str, error: str, db: str):
        if not self._adapter.db_pool:
            self._adapter._logger.error("Database pool not provided. Cannot write error to database.")
            return

        try:
            async with self._adapter.db_pool.acquire() as conn:
                await conn.execute(
                    f"""
                    INSERT INTO __metadata.{db}_errors (url, error, timestamp)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (error, url, timestamp) DO NOTHING
                    """,
                    url, error, str(datetime.now(timezone.utc))
                )
        except Exception as e:
            self._adapter._logger.error(f"Error writing to database: {str(e)}")

    def _extract_items(self, data: Dict[str, Any]) -> List[Any]:
        filtered_data = {k: v for k, v in data.items() if k not in ["request", "pagination"]}
        # self._adapter._logger.warning(f"EXTRACTING ITEMS: {filtered_data}")
        # If we have a single top-level key, use its value
        if len(filtered_data) == 1:
            # self._adapter._logger.warning(f"LEN OF FILTERED DATA IS 1 RETURNING: {list(filtered_data.values())[0]}")
            items = list(filtered_data.values())[0]
        else:
            # self._adapter._logger.warning(f"LEN OF FILTERED DATA IS NOT 1 RETURNING: {filtered_data}")
            items = filtered_data

        if isinstance(items, dict):
            # Check if the first value is a list (like in "committee-bills" case)
            first_value = next(iter(items.values()))
            if isinstance(first_value, list):
                # self._adapter._logger.warning(f"FIRST VALUE IS LIST RETURNING: {first_value}")
                return first_value
            else:
                # For cases like the committee data, return the whole dict
                # self._adapter._logger.warning(f"FIRST VALUE IS NOT LIST RETURNING: {items}")
                return [items]
        elif isinstance(items, list):
            # self._adapter._logger.warning(f"ITEMS IS LIST RETURNING: {items}")
            return items
        else:
            # self._adapter._logger.warning(f"ITEMS IS NOT LIST RETURNING: {items}")
            return [items]

    async def _process_items(self, cls: Any, items: List[Any], verbose: bool, _id_package_init: Optional[tuple], pagination: Optional[Dict[str, Any]]) -> List[Any]:
        processed_items = []
        for item in items:
            if isinstance(item, dict):
                if verbose:
                    if "url" in item:
                        response: Result = await self._adapter.retrieve(item["url"], override=True)
                    elif "granuleLink" in item:
                        granule_url_with_key = item["granuleLink"] + f"?api_key={self._adapter.current_api_key}"
                        response: Result = await self._adapter.retrieve(granule_url_with_key, override=True)
                        data = self._extract_items(response.data)[0]
                    else:
                        data = item
                else:
                    data = item

                # Check if the item is nested within a key (e.g., {"member": {...}})
                if len(data) == 1 and isinstance(list(data.values())[0], dict):
                    data = list(data.values())[0]

                # Check if cls is a function (for custom processing) or a class
                if callable(cls) and not isinstance(cls, type):
                    # It's a function, call it directly
                    processed_item = cls(data, _id_package_init)
                else:
                    # It's a class, instantiate it
                    init_params = {
                        'data': data,
                        '_id_package_init': _id_package_init,
                    }
                    if hasattr(cls.__init__, '__code__'):
                        if '_adapter' in cls.__init__.__code__.co_varnames:
                            init_params['_adapter'] = self._adapter
                        if '_pagination' in cls.__init__.__code__.co_varnames:
                            init_params['_pagination'] = pagination

                    processed_item = cls(**init_params)
                processed_items.append(processed_item)

        return processed_items