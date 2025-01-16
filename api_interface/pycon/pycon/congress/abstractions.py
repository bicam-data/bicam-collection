from datetime import datetime
import logging
import time
import asyncpg
from typing import Any, AsyncIterator, Callable, List, Literal, Optional
from urllib.parse import parse_qs, urlencode, urlparse

from pycon.adapter import RestAdapter
from pycon.retriever_class import Retriever
from pycon.utilis import add_date_range_to_url


from api_interface.pycon.pycon.congress.components import (
        Amendment,
        Bill,
        Committee,
        CommitteeMeeting,
        CommitteePrint,
        CommitteeReport,
        Hearing,
        Member,
        Nomination,
        Treaty,
        Congress
    )
from pycon.api_models import ErrorResult

class PyCongress:
    def __init__(
        self,
        api_keys,
        hostname: str = "api.congress.gov",
        ver: str = "v3",
        ssl_verify: bool = True,
        logger: logging.Logger = logging.Logger(""),
        db_pool: asyncpg.Pool | None = None,
        **kwargs
    ):
        self._adapter = RestAdapter(
            hostname=hostname,
            api_key=api_keys,
            ver=ver,
            ssl_verify=ssl_verify,
            logger=logger,
            db_pool=db_pool,
            **kwargs
        )
        self.items_remaining = 0

    async def paginate(self, func: Callable, page_size: int = 250, pages: int | str = "all", *args, **kwargs) -> AsyncIterator[Any]:
        page = 1
        total_count = None
        total_pages = None

        while True:
            api_key = self._adapter.current_api_key
            remaining = self._adapter.check_remaining_requests(api_key)
            self._adapter._logger.warning(f"{func.__name__.split('get_bulk_')[1] if 'get_bulk_' in func.__name__ else func.__name__} REMAINING REQUESTS for key {api_key}: {remaining} TIME: {datetime.now()}")
            try:
                items = await func(*args, **kwargs)
            except Exception as e:
                self._adapter._logger.error(f"Error fetching items: {str(e)}", exc_info=True)
                break

            if not items:
                self._adapter._logger.warning(f"No items found on page {page}. Breaking pagination loop.")
                break

            if not isinstance(items[0], Retriever):
                self._adapter._logger.warning(f"Unexpected item type on page {page}. Breaking pagination loop.")
                break

            pagination = items[0]._pagination if hasattr(items[0], "_pagination") else {}

            if total_count is None:
                total_count = pagination.get('count', 0)
                self.items_remaining = total_count
                total_pages = -(-total_count // page_size)  # Ceiling division
                self._adapter._logger.info(f"Total items: {total_count}, Total pages: {total_pages}")

            self._adapter._logger.info(f"Processing page {page}/{total_pages} - Items on this page: {len(items)}")

            yield items

            next_url = pagination.get('next')

            if not next_url:
                self._adapter._logger.info(f"No more pages to process. Finished at page {page}/{total_pages}")
                break

            if pages != 'all' and page >= pages:
                self._adapter._logger.info(f"Reached requested number of pages ({pages}). Stopping pagination.")
                break

            page += 1
            self.items_remaining -= len(items)
            self._adapter._logger.warning(
                    f"{type(items[0])}: Requesting page {page + 1} of {total_pages} with {total_count} total items..."
                )

            kwargs['next_url'] = next_url  # Update the next_url for the next iteration

        self._adapter._logger.info(f"Pagination complete. Processed {page} pages out of {total_pages}")
    def _update_page_size(self, url: str, page_size: int) -> str:
        # First, check if the current limit is the same as page_size - if so, return
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)
        current_limit = query_params.get('limit', [''])[0]

        if current_limit and int(current_limit) == page_size:
            return url
        else:
            parsed_url = urlparse(url)
            query_params = parse_qs(parsed_url.query)
            query_params['limit'] = [str(page_size)]
            new_query = urlencode(query_params, doseq=True)
            return parsed_url._replace(query=new_query).geturl()

    async def get_bill(self, congress: int, bill_type: str, bill_number: int, bill_id: str = None) -> Bill:
        alpha_type = "".join([i for i in bill_id if not i.isdigit()]) if bill_id else None
        numeric_number = "".join([i for i in bill_id if i.isdigit()]) if bill_id else None
        endpoint = f"/bill/{congress}/{bill_type}/{bill_number}" if not bill_id else f"/bill/{congress}/{alpha_type}/{numeric_number}"
    
        response = await self._adapter.retrieve(endpoint)
        if response is None or isinstance(response, ErrorResult):
            self._adapter._logger.error(f"Error fetching bill: {getattr(response, 'error_message', 'Unknown error')}")
            return None
        else:
            return Bill(data=response.data, _pagination=response.pagination, _adapter=self._adapter)

    async def get_member(self, bioguide_id: str) -> Member:
        endpoint = f"/member/{bioguide_id}"
    
        response = await self._adapter.retrieve(endpoint)
        if response is None or isinstance(response, ErrorResult):
            self._adapter._logger.error(f"Error fetching member: {getattr(response, 'error_message', 'Unknown error')}")
            return None
        else:
            return Member(data=response.data, _pagination=response.pagination, _adapter=self._adapter)

    async def get_amendment(self, congress: int, amendment_id: str) -> Amendment:
        alpha_type = "".join([i for i in amendment_id if not i.isdigit()])
        numeric_number = "".join([i for i in amendment_id if i.isdigit()])
        endpoint = f"/amendment/{congress}/{alpha_type.lower()}/{numeric_number}"

        response = await self._adapter.retrieve(endpoint)
        if response is None or isinstance(response, ErrorResult):
            self._adapter._logger.error(f"Error fetching amendment: {getattr(response, 'error_message', 'Unknown error')}")
            return None
        else:
            return Amendment(data=response.data, _pagination=response.pagination, _adapter=self._adapter)

    async def get_committee_meeting(self, event_id: str, chamber: Literal["house", "senate", "joint"], congress: int) -> CommitteeMeeting:
        endpoint = f"/committee-meeting/{congress}/{chamber}/{event_id}"

        response = await self._adapter.retrieve(endpoint)
        if response is None or isinstance(response, ErrorResult):
            self._adapter._logger.error(f"Error fetching committee meeting: {getattr(response, 'error_message', 'Unknown error')}")
            return None
        else:
            return CommitteeMeeting(data=response.data, _pagination=response.pagination, _adapter=self._adapter)

    async def get_committee_print(
        self,
        congress: int,
        chamber: Literal["house", "senate", "joint"],
        jacket_number: int
    ) -> CommitteePrint:
        endpoint = f"/committee-print/{congress}/{chamber}/{jacket_number}"

        response = await self._adapter.retrieve(endpoint)
        if response is None or isinstance(response, ErrorResult):
            self._adapter._logger.error(f"Error fetching committee print: {getattr(response, 'error_message', 'Unknown error')}")
            return None
        else:
            return CommitteePrint(data=response.data, _pagination=response.pagination, _adapter=self._adapter)

    async def get_committee_report(self, congress: int, report_id: str) -> CommitteeReport:
        alpha_type = "".join([i for i in report_id if not i.isdigit()])
        numeric_number = "".join([i for i in report_id if i.isdigit()])
        endpoint = f"/committee-report/{congress}/{alpha_type}/{numeric_number}"

        response = await self._adapter.retrieve(endpoint)
        if response is None or isinstance(response, ErrorResult):
            self._adapter._logger.error(f"Error fetching committee report: {getattr(response, 'error_message', 'Unknown error')}")
            return None
        else:
            return CommitteeReport(data=response.data, _pagination=response.pagination, _adapter=self._adapter)

    async def get_committee(self, chamber: Literal["house", "senate", "joint"], committee_code: str) -> Committee:
        endpoint = f"/committee/{chamber}/{committee_code}"

        response = await self._adapter.retrieve(endpoint)
        if response is None or isinstance(response, ErrorResult):
            self._adapter._logger.error(f"Error fetching committee: {getattr(response, 'error_message', 'Unknown error')}")
            return None
        else:
            return Committee(data=response.data, _pagination=response.pagination, _adapter=self._adapter)

    async def get_hearing(self, congress: int, chamber: Literal["house", "senate"], jacket_number: int) -> Hearing:
        endpoint = f"/hearing/{congress}/{chamber}/{jacket_number}"

        response = await self._adapter.retrieve(endpoint)
        if response is None or isinstance(response, ErrorResult):
            self._adapter._logger.error(f"Error fetching hearing: {getattr(response, 'error_message', 'Unknown error')}")
            return None
        else:
            return Hearing(data=response.data, _pagination=response.pagination, _adapter=self._adapter)

    async def get_nomination(self, congress: int, nomination_number: str) -> Nomination:
        endpoint = f"/nomination/{congress}/{nomination_number}"

        response = await self._adapter.retrieve(endpoint)
        if response is None or isinstance(response, ErrorResult):
            self._adapter._logger.error(f"Error fetching nomination: {getattr(response, 'error_message', 'Unknown error')}")
            return None
        else:
            return Nomination(data=response.data, _pagination=response.pagination, _adapter=self._adapter)

    async def get_treaty(self, congress: int, treaty_number: str) -> Treaty:
        endpoint = f"/treaty/{congress}/{treaty_number}"

        response = await self._adapter.retrieve(endpoint)
        if response is None or isinstance(response, ErrorResult):
            self._adapter._logger.error(f"Error fetching treaty: {getattr(response, 'error_message', 'Unknown error')}")
            return None
        else:
            return Treaty(data=response.data, _pagination=response.pagination, _adapter=self._adapter)

    async def get_congress(self, congress_number: int) -> Congress:
        endpoint = f"/congress/{congress_number}"

        response = await self._adapter.retrieve(endpoint)
        if response is None or isinstance(response, ErrorResult):
            self._adapter._logger.error(f"Error fetching congress: {getattr(response, 'error_message', 'Unknown error')}")
            return None
        else:
            return Congress(data=response.data, _pagination=response.pagination, _adapter=self._adapter)

    async def get_bulk_bills(self, congress: Optional[int] = None, bill_type: Optional[str] = None, from_date: str = "", to_date: str = "", **kwargs) -> AsyncIterator[List[Bill]]:
        self._adapter._logger.info("Getting bulk bills...")
        next_url = kwargs.get("next_url", None)
        if next_url:
            endpoint = next_url
        else:
            endpoint = f"/bill"
            if congress:
                endpoint += f"/{congress}"
                if bill_type:
                    endpoint += f"/{bill_type}"

        endpoint = add_date_range_to_url(endpoint, from_date, to_date)
        endpoint = self._update_page_size(endpoint, kwargs.get('page_size', 250))

        response = await self._adapter.retrieve(endpoint, override=bool(kwargs.get("next_url")))

        if response is None or isinstance(response, ErrorResult):
            self._adapter._logger.error(f"Error fetching bills: {getattr(response, 'error_message', 'Unknown error')}")
            return []

        _pagination = response.pagination if response.pagination else {}
        bills = response.data.get('bills', []) if response.data else []
        return [Bill(data=bill_data, _pagination=_pagination, _adapter=self._adapter) for bill_data in bills]

    async def get_bulk_congresses(self, from_date: str = "", to_date: str = "", **kwargs) -> AsyncIterator[Congress]:
        self._adapter._logger.info("Getting bulk congresses...")
        next_url = kwargs.get("next_url", None)
        if next_url:
            endpoint = next_url
        else:
            endpoint = f"/congress"

        endpoint = add_date_range_to_url(endpoint, from_date, to_date)
        endpoint = self._update_page_size(endpoint, kwargs.get('page_size', 250))

        response = await self._adapter.retrieve(endpoint, override=bool(kwargs.get("next_url")))

        if response is None or isinstance(response, ErrorResult):
            self._adapter._logger.error(f"Error fetching congresses: {getattr(response, 'error_message', 'Unknown error')}")
            return []  # Yield None to indicate an error
        else:
            _pagination = response.pagination if response.pagination else {}
            congresses = response.data.get('congresses', []) if response.data else []
            return [Congress(data=congress_data, _pagination=_pagination, _adapter=self._adapter) for congress_data in congresses]

    async def get_bulk_members(self, from_date: str = "", to_date: str = "", **kwargs) -> AsyncIterator[Member]:
        self._adapter._logger.info("Getting bulk members...")
        next_url = kwargs.get("next_url", None)
        if next_url:
            endpoint = next_url
        else:
            endpoint = f"/member"

        endpoint = add_date_range_to_url(endpoint, from_date, to_date)
        endpoint = self._update_page_size(endpoint, kwargs.get('page_size', 250))

        response = await self._adapter.retrieve(endpoint, override=bool(kwargs.get("next_url")))

        if response is None or isinstance(response, ErrorResult):
            self._adapter._logger.error(f"Error fetching members: {getattr(response, 'error_message', 'Unknown error')}")
            return []  # Yield None to indicate an error
        else:
            _pagination = response.pagination if response.pagination else {}
            members = response.data.get('members', []) if response.data else []
            for member in members:
                if member.get("bioguide_id") == "L000550":
                    self._adapter._logger.warning(f"Member {member.get('bioguide_id')} found in API. Sleeping for 300 seconds.")
                    time.sleep(300)

            return [Member(data=member_data, _pagination=_pagination, _adapter=self._adapter) for member_data in members]

    async def get_bulk_amendments(self, congress: Optional[int] = None, from_date: str = "", to_date: str = "", **kwargs) -> AsyncIterator[List[Amendment]]:
        self._adapter._logger.info("Getting bulk amendments...")
        next_url = kwargs.get("next_url", None)
        if next_url:
            endpoint = next_url
        else:
            endpoint = f"/amendment"
            if congress:
                endpoint += f"/{congress}"

        endpoint = add_date_range_to_url(endpoint, from_date, to_date)
        endpoint = self._update_page_size(endpoint, kwargs.get('page_size', 250))

        response = await self._adapter.retrieve(endpoint, override=bool(kwargs.get("next_url")))

        if response is None or isinstance(response, ErrorResult):
            self._adapter._logger.error(f"Error fetching amendments: {getattr(response, 'error_message', 'Unknown error')}")
            return []

        _pagination = response.pagination if response.pagination else {}
        amendments = response.data.get('amendments', []) if response.data else []
        return [Amendment(data=amendment_data, _pagination=_pagination, _adapter=self._adapter) for amendment_data in amendments]

    async def get_bulk_committees(self, congress: Optional[int] = None, 
                                chamber: Optional[Literal["house", "senate", "joint"]] = None, 
                                from_date: str = "", to_date: str = "", **kwargs) -> AsyncIterator[Committee]:
        self._adapter._logger.info("Getting bulk committees...")
        next_url = kwargs.get("next_url", None)
        
        if next_url:
            endpoint = next_url
        else:
            endpoint = f"/committee"
            if congress:
                endpoint += f"/{congress}"
            if chamber:
                endpoint += f"/{chamber}"
            endpoint = add_date_range_to_url(endpoint, from_date, to_date)
            endpoint = self._update_page_size(endpoint, kwargs.get('page_size', 250))
            endpoint = f"{endpoint}&format=json"
        
        response = await self._adapter.retrieve(endpoint, override=bool(kwargs.get("next_url")))
        
        if response is None or isinstance(response, ErrorResult):
            self._adapter._logger.error(f"Error fetching committees: {getattr(response, 'error_message', 'Unknown error')}")
            return []
        else:
            _pagination = response.pagination if response.pagination else {}
            committees = response.data.get('committees', []) if response.data else []
            
            # Create a dictionary to store unique committees
            unique_committees = {}
            
            # Process each committee and keep only unique combinations of systemCode + updateDate
            for committee_data in committees:
                # Create a unique key combining systemCode and updateDate
                unique_key = (committee_data.get('systemCode'), committee_data.get('updateDate'), committee_data.get('name'))
                
                # Only add if this exact combination hasn't been seen before
                if unique_key not in unique_committees:
                    unique_committees[unique_key] = committee_data
            
            # Convert back to list and create Committee objects
            data_list = [Committee(data=committee_data, 
                                _pagination=_pagination, 
                                _adapter=self._adapter) 
                        for committee_data in unique_committees.values()]
            
            return data_list

    async def get_bulk_committeereports(self, congress: Optional[int] = None, from_date: str = "", to_date: str = "", **kwargs) -> AsyncIterator[CommitteeReport]:
        self._adapter._logger.info("Getting bulk committee reports...")
        next_url = kwargs.get("next_url", None)
        if next_url:
            endpoint = next_url
        else:
            endpoint = f"/committee-report"
            if congress:
                endpoint += f"/{congress}"

        endpoint = add_date_range_to_url(endpoint, from_date, to_date)
        endpoint = self._update_page_size(endpoint, kwargs.get('page_size', 250))

        response = await self._adapter.retrieve(endpoint, override=bool(kwargs.get("next_url")))

        if response is None or isinstance(response, ErrorResult):
            self._adapter._logger.error(f"Error fetching committee reports: {getattr(response, 'error_message', 'Unknown error')}")
            return []  # Yield None to indicate an error
        else:
            _pagination = response.pagination if response.pagination else {}
            reports = response.data.get('reports', []) if response.data else []
            return [CommitteeReport(data=report_data, _pagination=_pagination, _adapter=self._adapter) for report_data in reports]

    async def get_bulk_committeeprints(self, congress: Optional[int] = None, chamber: Optional[Literal["house", "senate", "joint"]] = None, from_date: str = "", to_date: str = "", **kwargs) -> AsyncIterator[CommitteePrint]:
        self._adapter._logger.info("Getting bulk committee prints...")
        next_url = kwargs.get("next_url", None)
        if next_url:
            endpoint = next_url
        else:
            endpoint = f"/committee-print"
            if congress:
                endpoint += f"/{congress}"
                if chamber:
                    endpoint += f"/{chamber}"

        endpoint = add_date_range_to_url(endpoint, from_date, to_date)
        endpoint = self._update_page_size(endpoint, kwargs.get('page_size', 250))

        response = await self._adapter.retrieve(endpoint, override=bool(kwargs.get("next_url")))

        if response is None or isinstance(response, ErrorResult):
            self._adapter._logger.error(f"Error fetching committee prints: {getattr(response, 'error_message', 'Unknown error')}")
            return []  # Yield None to indicate an error
        else:
            _pagination = response.pagination if response.pagination else {}
            prints = response.data.get('committeePrints', []) if response.data else []
            return [CommitteePrint(data=print_data, _pagination=_pagination, _adapter=self._adapter) for print_data in prints]

    async def get_bulk_committeemeetings(self, congress: Optional[int] = None, chamber: Optional[Literal["house", "senate", "joint"]] = None, from_date: str = "", to_date: str = "", **kwargs) -> AsyncIterator[CommitteeMeeting]:
        self._adapter._logger.info("Getting bulk committee meetings...")
        next_url = kwargs.get("next_url", None)
        if next_url:
            endpoint = next_url
        else:
            endpoint = f"/committee-meeting"
            if congress:
                endpoint += f"/{congress}"
                if chamber:
                    endpoint += f"/{chamber}"

        endpoint = add_date_range_to_url(endpoint, from_date, to_date)
        endpoint = self._update_page_size(endpoint, kwargs.get('page_size', 250))

        response = await self._adapter.retrieve(endpoint, override=bool(kwargs.get("next_url")))

        if response is None or isinstance(response, ErrorResult):
            self._adapter._logger.error(f"Error fetching committee meetings: {getattr(response, 'error_message', 'Unknown error')}")
            return []  # Yield None to indicate an error
        else:
            _pagination = response.pagination if response.pagination else {}
            meetings = response.data.get('committeeMeetings', []) if response.data else []
            return [CommitteeMeeting(data=meeting_data, _pagination=_pagination, _adapter=self._adapter) for meeting_data in meetings]

    async def get_bulk_hearings(self, congress: Optional[int] = None, chamber: Optional[Literal["house", "senate"]] = None, from_date: str = "", to_date: str = "", **kwargs) -> AsyncIterator[Hearing]:
        self._adapter._logger.info("Getting bulk hearings...")
        next_url = kwargs.get("next_url", None)
        if next_url:
            endpoint = next_url
        else:
            endpoint = f"/hearing"
            if congress:
                endpoint += f"/{congress}"
                if chamber:
                    endpoint += f"/{chamber}"

        endpoint = add_date_range_to_url(endpoint, from_date, to_date)
        endpoint = self._update_page_size(endpoint, kwargs.get('page_size', 250))

        response = await self._adapter.retrieve(endpoint, override=bool(kwargs.get("next_url")))

        if response is None or isinstance(response, ErrorResult):
            self._adapter._logger.error(f"Error fetching hearings: {getattr(response, 'error_message', 'Unknown error')}")
            return []  # Yield None to indicate an error
        else:
            _pagination = response.pagination if response.pagination else {}
            hearings = response.data.get('hearings', []) if response.data else []
            return [Hearing(data=hearing_data, _pagination=_pagination, _adapter=self._adapter) for hearing_data in hearings]

    async def get_bulk_nominations(self, congress: Optional[int] = None, from_date: str = "", to_date: str = "", **kwargs) -> AsyncIterator[Nomination]:
        self._adapter._logger.info("Getting bulk nominations...")
        next_url = kwargs.get("next_url", None)
        if next_url:
            endpoint = next_url
        else:
            endpoint = f"/nomination"
            if congress:
                endpoint += f"/{congress}"

        endpoint = add_date_range_to_url(endpoint, from_date, to_date)
        endpoint = self._update_page_size(endpoint, kwargs.get('page_size', 250))

        response = await self._adapter.retrieve(endpoint, override=bool(kwargs.get("next_url")))

        if response is None or isinstance(response, ErrorResult):
            self._adapter._logger.error(f"Error fetching nominations: {getattr(response, 'error_message', 'Unknown error')}")
            return []  # Yield None to indicate an error
        else:
            _pagination = response.pagination if response.pagination else {}
            nominations = response.data.get('nominations', []) if response.data else []
            return [Nomination(data=nomination_data, _pagination=_pagination, _adapter=self._adapter) for nomination_data in nominations]

    async def get_bulk_treaties(self, congress: Optional[int] = None, from_date: str = "", to_date: str = "", **kwargs) -> AsyncIterator[Treaty]:
        self._adapter._logger.info("Getting bulk treaties...")
        next_url = kwargs.get("next_url", None)
        if next_url:
            endpoint = next_url
        else:
            endpoint = f"/treaty"
            if congress:
                endpoint += f"/{congress}"

        endpoint = add_date_range_to_url(endpoint, from_date, to_date)
        endpoint = self._update_page_size(endpoint, kwargs.get('page_size', 250))

        response = await self._adapter.retrieve(endpoint, override=bool(kwargs.get("next_url")))

        if response is None or isinstance(response, ErrorResult):
            self._adapter._logger.error(f"Error fetching treaties: {getattr(response, 'error_message', 'Unknown error')}")
            return []  # Yield None to indicate an error
        else:
            treaties = response.data.get('treaties', []) if response.data else []
            _pagination = response.pagination if response.pagination else {}
            return [Treaty(data=treaty_data, _pagination=_pagination, _adapter=self._adapter) for treaty_data in treaties]
