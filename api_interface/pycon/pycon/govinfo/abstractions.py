from pycon.adapter import RestAdapter
from api_interface.pycon.pycon.govinfo.components import BillCollection, CongressionalDirectoryPackage, CongressionalReportPackage, TreatyPackage, PrintPackage, HearingPackage
import logging
from datetime import datetime
from typing import Any, AsyncIterator, Callable, List
from urllib.parse import quote, urlparse, urlunparse

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class GovInfoAPI:
    def __init__(self, api_keys, hostname="api.govinfo.gov", ver=None, ssl_verify=True, logger=None, session=None, db_pool=None, **kwargs):
        self._adapter = RestAdapter(
            hostname=hostname,
            api_key=api_keys,
            ver=ver,
            ssl_verify=ssl_verify,
            logger=logger,
            session=session,
            db_pool=db_pool,
        )
        self.items_remaining = 0
        
        # Initialize logger
        if logger is None:
            self.logger = logging.getLogger(__name__)
            self.logger.setLevel(logging.INFO)
            if not self.logger.handlers:
                handler = logging.StreamHandler()
                formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
                handler.setFormatter(formatter)
                self.logger.addHandler(handler)
        else:
            self.logger = logger

    async def paginate(self, func: Callable, page_size: int = 1000, pages: int | str = "all", count: int = 0, *args, **kwargs) -> AsyncIterator[Any]:
        page = 1
        total_count = None
        total_pages = None
        processed_items = 0

        while True:
            api_key = self._adapter.current_api_key
            remaining = self._adapter.check_remaining_requests(api_key)
            self.logger.warning(f"REMAINING REQUESTS for key {api_key}: {remaining} TIME: {datetime.now()}")

            try:
                items = await func(*args, **kwargs)
            except Exception as e:
                self.logger.error(f"Error fetching items: {str(e)}", exc_info=True)
                break

            if not items:
                self.logger.warning(f"No items found on page {page}. Breaking pagination loop.")
                break

            pagination = getattr(items[0], '_pagination', {}) if items else {}
            if total_count is None:
                total_count = pagination.get('count', 0) if pagination else 0
                self.items_remaining = total_count
                total_pages = -(-total_count // page_size) if total_count else None
                self.logger.info(f"Total items: {total_count}, Total pages: {total_pages}")

            items_count = len(items)
            processed_items += items_count
            self.logger.info(f"Processing page {page}/{total_pages} - Items on this page: {items_count}")

            yield items

            if pagination is None or processed_items >= total_count:
                self.logger.info(f"All items processed. Stopping pagination.")
                break

            next_url = pagination.get('next') if pagination else None

            if not next_url:
                self.logger.info(f"No more pages to process. Finished at page {page}/{total_pages}")
                break

            if pages != 'all' and page >= pages:
                self.logger.info(f"Reached requested number of pages ({pages}). Stopping pagination.")
                break

            page += 1
            self.items_remaining = max(0, total_count - processed_items)
            self.logger.warning(
                f"Requesting page {page} of {total_pages} with {self.items_remaining} items remaining..."
            )

            kwargs['next_url'] = next_url  # Update the next_url for the next iteration

        self.logger.info(f"Pagination complete. Processed {processed_items} items out of {total_count}")
    def format_date(self, date_str: str) -> str:
        """Convert date string to the required format."""
        date = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        return date.strftime('%Y-%m-%dT%H:%M:%SZ')

    def construct_url(self, base_url: str, start_date: str, end_date: str, doc_class: str = None) -> str:
        """Construct URL with properly formatted dates and additional parameters."""
        formatted_start = quote(self.format_date(start_date))
        formatted_end = quote(self.format_date(end_date))
        
        # Parse the base_url to ensure we're not duplicating the scheme and netloc
        parsed_url = urlparse(base_url)
        path = f"{parsed_url.path}/{formatted_start}/{formatted_end}"
        
        # Reconstruct the URL without duplicating the scheme and netloc
        url = urlunparse((parsed_url.scheme, parsed_url.netloc, path, '', '', ''))
        
        # Append pageSize and offsetMark as strings
        api_key = self._adapter.current_api_key
        if doc_class:
            url += f"?pageSize=1000&docClass={doc_class}&offsetMark=%2A&api_key={api_key}"
        else:
            url += f"?pageSize=1000&offsetMark=%2A&api_key={api_key}"
        
        return url
    
    
    async def get_bulk_bill_collections(self, start_date: str, end_date: str, **kwargs) -> List[BillCollection]:
        base_url = f"{self._adapter.url}/collections/BILLS"
        if not kwargs.get('next_url'):
            url = self.construct_url(base_url, start_date, end_date)
        else:
            url = kwargs.get('next_url')
        response = await self._adapter.retrieve(url, override=True)

        if response is None or response.data is None:
            self.logger.error(f"No response or data received for URL: {url}")
            return []

        count = response.data.get('count', 0)
        kwargs['count'] = count
        self.logger.info(f"URL: {url}")
        packages = response.data.get('packages', [])
        if not packages:
            self.logger.warning(f"No packages found in response for URL: {url}")
            return []

        data_list = [BillCollection(data=bill, _pagination=response.pagination, _adapter=self._adapter) for bill in packages]

        self.logger.info(f"Retrieved {len(data_list)} Bill Collection packages")

        return data_list

    async def get_bulk_congressional_directories(self, start_date: str, end_date: str, **kwargs) -> List[CongressionalDirectoryPackage]:
        base_url = f"{self._adapter.url}/collections/CDIR"
        if not kwargs.get('next_url'):
            url = self.construct_url(base_url, start_date, end_date)
        else:
            url = kwargs.get('next_url')
        response = await self._adapter.retrieve(url, override=True)

        if response is None or response.data is None:
            self.logger.error(f"No response or data received for URL: {url}")
            return []

        count = response.data.get('count', 0)
        kwargs['count'] = count
        self.logger.info(f"URL: {url}")
        packages = response.data.get('packages', [])
        if not packages:
            self.logger.warning(f"No packages found in response for URL: {url}")
            return []

        data_list = [CongressionalDirectoryPackage(data=cdir, _pagination=response.pagination, _adapter=self._adapter) for cdir in packages]

        self.logger.info(f"Retrieved {len(data_list)} Congressional Directory packages")

        return data_list

    async def get_bulk_congressional_reports(self, start_date: str, end_date: str, **kwargs) -> List[CongressionalReportPackage]:
        base_url = f"{self._adapter.url}/collections/CRPT"
        if not kwargs.get('next_url'):
            url = self.construct_url(base_url, start_date, end_date)
        else:
            url = kwargs.get('next_url')
        response = await self._adapter.retrieve(url, override=True)

        if response is None or response.data is None:
            self.logger.error(f"No response or data received for URL: {url}")
            return []
        count = response.data.get('count', 0)
        kwargs['count'] = count
        self.logger.info(f"URL: {url}")
        packages = response.data.get('packages', [])
        if not packages:
            self.logger.warning(f"No packages found in response for URL: {url}")
            return []

        data_list = [CongressionalReportPackage(data=report, _pagination=response.pagination, _adapter=self._adapter) for report in packages]

        self.logger.info(f"Retrieved {len(data_list)} Congressional Report packages")

        return data_list

    async def get_bulk_treaties(self, start_date: str, end_date: str, **kwargs) -> List[TreatyPackage]:
        base_url = f"{self._adapter.url}/collections/CDOC"
        if not kwargs.get('next_url'):
            url = self.construct_url(base_url, start_date, end_date, doc_class="TDOC")
        else:
            url = kwargs.get('next_url')
        response = await self._adapter.retrieve(url, override=True)
        
        if response is None or response.data is None:
            self.logger.error(f"No response or data received for URL: {url}")
            return []

        self.logger.info(f"Response for pagination: {response.data.get('nextPage')}")
        return [TreatyPackage(data=treaty, _pagination=response.pagination, _adapter=self._adapter) for treaty in response.data.get('packages', [])]

    async def get_bulk_committee_prints(self, start_date: str, end_date: str, **kwargs) -> List[PrintPackage]:
        base_url = f"{self._adapter.url}/collections/CPRT"
        if not kwargs.get('next_url'):
            url = self.construct_url(base_url, start_date, end_date)
        else:
            url = kwargs.get('next_url')
        response = await self._adapter.retrieve(url, override=True)
        
        if response is None or response.data is None:
            self.logger.error(f"No response or data received for URL: {url}")
            return []

        self.logger.info(f"Response for pagination: {response.data.get('nextPage')}")
        return [PrintPackage(data=print_pkg, _pagination=response.pagination, _adapter=self._adapter) for print_pkg in response.data.get('packages', []) if "GPO" not in print_pkg.get('packageId')]

    async def get_bulk_hearings(self, start_date: str, end_date: str, **kwargs) -> List[HearingPackage]:
        base_url = f"{self._adapter.url}/collections/CHRG"
        if not kwargs.get('next_url'):
            url = self.construct_url(base_url, start_date, end_date)
        else:
            url = kwargs.get('next_url')
        response = await self._adapter.retrieve(url, override=True)

        if response is None or response.data is None:
            self.logger.error(f"No response or data received for URL: {url}")
            return []

        self.logger.info(f"Response for pagination: {response.data.get('nextPage')}")
        return [HearingPackage(data=hearing, _pagination=response.pagination, _adapter=self._adapter) for hearing in response.data.get('packages', [])]