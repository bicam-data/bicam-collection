import datetime
import json
from dataclasses import dataclass
import time
from typing import AsyncIterator, Dict, List, Optional, Tuple, Union

from pycon.adapter import RestAdapter
from pycon.exceptions import PyCongressException
from pycon.utilis import process_report_id
import hashlib

from pycon.retriever_class import Retriever
from api_interface.pycon.pycon.congress.subcomponents import (
    CBOCostEstimate,
    CommitteeActivity,
    CommitteeHistory,
    Law,
    LeadershipRole,
    MemberTerm,
    NominationPosition,
    Nominee,
    PartyHistory,
    RecordedVotes,
    Session,
    Subject,
    Summary,
    Title,
    Witness,
    )

@dataclass
class Action(Retriever):
    data: Dict
    _adapter: RestAdapter
    _pagination: Dict
    _id_package_init: Optional[Tuple] = None

    def __init__(self, data: Dict, _pagination: Dict, _adapter: RestAdapter, _id_package_init: Optional[Tuple] = None):
        if "actions" in data and isinstance(data.get("actions"), dict):
            self.data = data.get("actions")
        else:
            self.data = data

        self._pagination = _pagination

        super().__init__(data=self.data, _adapter=_adapter)
        self._id_package_init = _id_package_init

        self._parse_data()
        self._post_init_processing()

    def _post_init_processing(self):
        if self._id_package_init:
            setattr(self, self._id_package_init[0], self._id_package_init[1])

        self._set_action_id()
        self._setup_id_package()
        self._parse_committees()
        self._parse_recorded_votes()
        self._parse_calendar_number()
        self._parse_source_system()


    def __str__(self):
        return self.text if self.text != "NO TEXT" else self.action_code or "Action"

    def __repr__(self):
        return self.__str__()

    def _setup_id_package(self):
        return setattr(self, "_id_package", ("action_id", getattr(self, "action_id", getattr(self, "_action_id", "-99"))))

    def _parse_committees(self):
        self._committees = getattr(self, "_committees", [])
        if not self._committees:
            self._committees = getattr(self, "_committee", [])
            if not isinstance(self._committees, list):
                self._committees = [self._committees]

    def _parse_recorded_votes(self):
        self._recorded_votes_data = getattr(self, "_recorded_votes", {})
        self._recorded_votes = [RecordedVotes(data=vote, _id_package_init=self._id_package_init, action_id=self.action_id) for vote in self._recorded_votes_data if isinstance(vote, dict)]

    def _parse_calendar_number(self):
        calendar_data = getattr(self, "_calendar_number", {})
        self._calendar_number = calendar_data.get("number")
        self._calendar = calendar_data.get("calendar")

    def _parse_source_system(self):
        source_system = getattr(self, "_source_system", {})
        self._source_system = source_system.get("name")
        self._source_system_code = source_system.get("code")

    def _set_action_id(self):
        related_id = self._id_package_init[1]
        action_date = getattr(self, "_action_date", "")
        action_text = getattr(self, "_text", "")
        action_code = getattr(self, "_action_code", "")
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        unique_string = f"{related_id}_{action_date}_{action_text}_{action_code}_{timestamp}"
        self._action_id = hashlib.md5(unique_string.encode()).hexdigest()

    @property
    def action_id(self) -> str:
        return self._action_id

    @property
    def action_code(self) -> str:
        return getattr(self, "_action_code", "-99")

    @property
    def action_date(self) -> str:
        return getattr(self, "_action_date", "-99")

    @property
    def action_time(self) -> Optional[str]:
        return getattr(self, "_action_time", None)

    @property
    def text(self) -> str:
        return getattr(self, "_text", "NO TEXT")

    @property
    def action_type(self) -> Optional[str]:
        return getattr(self, "_type", None)

    @property
    def links(self) -> Optional[List[Dict]]:
        return getattr(self, "_links", [])

    @property
    def committees_urls(self) -> List[str]:
        return [committee.get("url") for committee in self._committees if isinstance(committee, dict)]

    @property
    def committee_names(self) -> List[str]:
        return [committee.get("name") for committee in self._committees if isinstance(committee, dict)]

    @property
    def committee_codes(self) -> List[str]:
        return [committee.get("systemCode") for committee in self._committees if isinstance(committee, dict)]

    @property
    def recorded_votes(self) -> List['RecordedVotes']:
        return getattr(self, "_recorded_votes", [])

    @property
    def source_system(self) -> Optional[str]:
        return getattr(self, "_source_system", None)

    @property
    def source_system_code(self) -> Optional[str]:
        return getattr(self, "_source_system_code", None)

    @property
    def calendar_number(self) -> Optional[str]:
        return getattr(self, "_calendar_number", None)

    @property
    def calendar(self) -> Optional[str]:
        return getattr(self, "_calendar", None)

    @property
    def url(self) -> Optional[str]:
        return getattr(self, "_url", None)

    def _ensure_adapter(self):
        if not self._adapter:
            raise PyCongressException(
                "Object must be initialized with a RestAdapter object."
            )
    async def get_committees(self) -> AsyncIterator['Committee']:
        self._ensure_adapter()
        try:
            if not self._committees:
                yield None

            async for items in self._get_items(
                "committees_urls",
                Committee,
                _id_package_init=self._id_package
            ):
                for committee in items:
                    yield committee
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving committees: {e}")

@dataclass
class Bill(Retriever):
    data: Dict
    _adapter: RestAdapter
    _pagination: Dict
    _id_package_init: Optional[Tuple] = None

    def __init__(self, data: Dict, _pagination: Dict, _adapter: RestAdapter, _id_package_init: Optional[Tuple] = None):
        if "bill" in data and isinstance(data.get("bill"), dict):
            self.data = data.get("bill")
        elif "relatedBills" in data and isinstance(data.get("relatedBills"), dict):
            self.data = data.get("relatedBills")
        elif "committee-bills" in data and isinstance(data.get("committee-bills"), dict):
            self.data = data.get("committee-bills")
        elif "bills" in data and isinstance(data.get("bills"), dict):
            self.data = data.get("bills")
        else:
            self.data = data

        self._pagination = _pagination

        super().__init__(data=self.data, _adapter=_adapter)
        self._id_package_init = _id_package_init

        self._parse_data()
        self._post_init_processing()

    def _post_init_processing(self):
        if self._id_package_init:
            if self._id_package_init[0] == 'bill_id':
                print("SET ORIGINAL BILL ID")
                setattr(self, '_original_bill_id', self._id_package_init[1])
            else:
                setattr(self, self._id_package_init[0], self._id_package_init[1])

        self._parse_bill_id()
        self._setup_id_package()
        self._parse_actions()
        self._parse_amendments()
        self._parse_cbo_cost_estimates()
        self._parse_committees()
        self._parse_cosponsors()
        self._parse_latest_action()
        self._parse_related_bills()
        self._parse_sponsors()
        self._parse_laws()
        self._parse_subjects()
        self._parse_summaries()
        self._parse_titles()
        self._parse_texts()
        self._parse_committee_reports()
        self._parse_relationship_details()

    def _parse_bill_id(self):
        if getattr(self, "_original_bill_id", None) is not None:
            setattr(self, "_bill_id", getattr(self, "_original_bill_id"))
            print("SET ORIGINAL BILL ID TO BILL ID")
        elif getattr(self, "_relationship_details", None) is None:
            bill_number = getattr(self, "_number", None)
            congress = getattr(self, "_congress", None)
            bill_type = getattr(self, "_type", None)
            if bill_number and congress and bill_type:
                _bill_type = str(bill_type).lower()
                setattr(self, "_bill_id", f"{_bill_type}{bill_number}-{congress}")
                setattr(self, "_bill_type", _bill_type)

        # Set _relatedbill_id
        if getattr(self, "_relationship_details", None) is not None:
            bill_number = getattr(self, "_number", None)
            congress = getattr(self, "_congress", None)
            bill_type = getattr(self, "_type", None)
            if bill_number and congress and bill_type:
                _bill_type = str(bill_type).lower()
                self._adapter._logger.info(f"SETTING RELATED BILL ID: {_bill_type}{bill_number}-{congress}")
                print("SETTING RELATED BILL ID")
                setattr(self, "_relatedbill_id", f"{_bill_type}{bill_number}-{congress}")
                setattr(self, "_bill_type", _bill_type)

    def _parse_actions(self):
        actions = getattr(self, '_actions', {})
        self._actions_url = actions.get("url", None)
        self._actions_count = actions.get("count", 0)

    def _parse_amendments(self):
        amendments = getattr(self, '_amendments', {})
        self._amendments_url = amendments.get("url", None)
        self._amendments_count = amendments.get("count", 0)

    def _parse_cbo_cost_estimates(self):
        cbo_cost_estimates = getattr(self, '_cbo_cost_estimates', [])
        self._cbo_cost_estimates = [CBOCostEstimate(data=cbo, _id_package_init=self._id_package) for cbo in cbo_cost_estimates if isinstance(cbo, dict)]

    def _parse_committees(self):
        committees = getattr(self, '_committees', {})
        self._committees_url = committees.get("url", None)
        self._committees_count = committees.get("count", 0)

    def _parse_cosponsors(self):
        cosponsors = getattr(self, '_cosponsors', {})
        self._cosponsors_url = cosponsors.get("url", None)
        self._cosponsors_count = cosponsors.get("count", 0)

    def _parse_latest_action(self):
        latest_action = getattr(self, '_latest_action', {})
        self._latest_action = Action(data=latest_action, _pagination=None,_adapter=self._adapter, _id_package_init=self._id_package)

    def _parse_notes(self):
        notes = getattr(self, '_notes', {})
        self._notes = notes.get("text")

    def _parse_policy_area(self):
        policy_area = getattr(self, '_policy_area', {})
        self._policy_area = policy_area.get("name")

    def _parse_related_bills(self):
        related_bills = getattr(self, '_related_bills', {})
        self._bill_relations_url = related_bills.get("url", None)
        self._bill_relations_count = related_bills.get("count", 0)

    def _parse_sponsors(self):
        sponsors = getattr(self, '_sponsors', [])
        self._sponsors_urls = [sponsor.get("url") for sponsor in sponsors if isinstance(sponsor, dict)]
        self._sponsors_bioguide_ids = [sponsor.get("bioguideId") for sponsor in sponsors if isinstance(sponsor, dict)]
        self._sponsors_names = [sponsor.get("fullName") for sponsor in sponsors if isinstance(sponsor, dict)]

    def _parse_laws(self):
        laws = getattr(self, '_laws', [])
        if laws:
            self._laws = [Law(data=law, _id_package_init=self._id_package) for law in laws if isinstance(law, dict)]
            self._is_law = True
        else:
            self._is_law = False

    def _parse_subjects(self):
        subjects = getattr(self, '_subjects', {})
        self._subjects_url = subjects.get("url", None)
        self._subjects_count = subjects.get("count", 0)

    def _parse_summaries(self):
        summaries = getattr(self, '_summaries', {})
        self._summaries_url = summaries.get("url", None)
        self._summaries_count = summaries.get("count", 0)

    def _parse_titles(self):
        titles = getattr(self, '_titles', {})
        self._titles_url = titles.get("url", None)
        self._titles_count = titles.get("count", 0)

    def _parse_texts(self):
        texts = getattr(self, '_text_versions', {})
        self._texts_url = texts.get("url", None)
        self._texts_count = texts.get("count", 0)

    def _parse_committee_reports(self):
        committee_reports = getattr(self, '_committee_reports', {})
        self._committee_reports_urls = [report.get("url") for report in committee_reports if isinstance(report, dict)]
        self._committeereports = [process_report_id(report.get("citation")) for report in committee_reports if isinstance(report, dict)]

    def _parse_relationship_details(self):
        relationship_details = getattr(self, '_relationship_details', [])
        relationship_length = (len(relationship_details)) if isinstance(relationship_details, list) else 0
        while relationship_length > 0:
            for relationship in relationship_details if isinstance(relationship_details, list) else []:
                if not hasattr(self, f"_relationship_identified_by_{relationship_length}"):
                    setattr(self, f"_relationship_identified_by_{relationship_length}", relationship.get('identifiedBy'))
                    setattr(self, f"_relationship_type_{relationship_length}", relationship.get('type'))
                    relationship_length -= 1

    def _setup_id_package(self):
        return setattr(self, "_id_package", ("bill_id", getattr(self, "bill_id", getattr(self, "_bill_id", "-99"))))

    @property
    def bill_id(self) -> Optional[str]:
        return getattr(self, "_bill_id", "-99")

    @property
    def relatedbill_id(self) -> Optional[str]:
        return getattr(self, "_relatedbill_id", None)

    @property
    def bill_number(self) -> Optional[str]:
        return getattr(self, '_number', None)

    @property
    def bill_type(self) -> Optional[str]:
        return getattr(self, '_bill_type', None)

    @property
    def congress(self) -> Optional[int]:
        return getattr(self, '_congress', None)

    @property
    def introduced_at(self) -> Optional[str]:
        return getattr(self, '_introduced_date', None)

    @property
    def origin_chamber(self) -> Optional[str]:
        return getattr(self, '_origin_chamber', None)

    @property
    def origin_chamber_code(self) -> Optional[str]:
        return getattr(self, '_origin_chamber_code', None)

    @property
    def title(self) -> Optional[str]:
        return getattr(self, '_title', None)

    @property
    def updated_at(self) -> Optional[str]:
        return getattr(self, '_update_date', None)

    @property
    def update_date_including_text(self) -> Optional[str]:
        return getattr(self, '_update_date_including_text', None)

    @property
    def actions_count(self) -> int:
        return getattr(self, '_actions_count', None)

    @property
    def actions_url(self) -> Optional[str]:
        return getattr(self, '_actions_url', None)

    @property
    def amendments_count(self) -> int:
        return getattr(self, '_amendments_count', None)

    @property
    def amendments_url(self) -> Optional[str]:
        return getattr(self, '_amendments_url', None)

    @property
    def cbocostestimates(self) -> List[CBOCostEstimate]:
        return getattr(self, '_cbo_cost_estimates', [])

    @property
    def committees_count(self) -> int:
        return getattr(self, '_committees_count', None)

    @property
    def committees_url(self) -> Optional[str]:
        return getattr(self, '_committees_url', None)

    @property
    def constitutional_authority_statement_text(self) -> Optional[str]:
        return getattr(self, '_constitutional_authority_statement_text', None)

    @property
    def cosponsors_count(self) -> int:
        return getattr(self, '_cosponsors_count', None)

    @property
    def cosponsors_url(self) -> Optional[str]:
        return getattr(self, '_cosponsors_url', None)

    @property
    def is_law(self) -> Optional[bool]:
        return getattr(self, '_is_law', None)

    @property
    def law_number(self) -> Optional[str]:
        return getattr(self, '_law_number', None)

    @property
    def law_type(self) -> Optional[str]:
        return getattr(self, '_law_type', None)

    @property
    def laws(self) -> List[Law]:
        return getattr(self, '_laws', [])

    @property
    def latest_action(self) -> Optional[Action]:
        return getattr(self, '_latest_action', None)

    @property
    def notes(self) -> Optional[str]:
        return getattr(self, '_notes', None)

    @property
    def policy_area(self) -> Optional[str]:
        return getattr(self, '_policy_area', {}).get("name")

    @property
    def billrelations_count(self) -> int:
        return getattr(self, '_bill_relations_count', None)

    @property
    def billrelations_url(self) -> Optional[str]:
        return getattr(self, '_bill_relations_url', None)

    @property
    def sponsors(self) -> List[str]:
        return getattr(self, '_sponsors_bioguide_ids', [])

    @property
    def sponsors_names(self) -> List[str]:
        return getattr(self, '_sponsors_names', [])

    @property
    def sponsors_urls(self) -> List[str]:
        return getattr(self, '_sponsors_urls', [])

    @property
    def subjects_count(self) -> int:
        return getattr(self, '_subjects_count', None)

    @property
    def subjects_url(self) -> Optional[str]:
        return getattr(self, '_subjects_url', None)

    @property
    def summaries_count(self) -> int:
        return getattr(self, '_summaries_count', None)

    @property
    def summaries_url(self) -> Optional[str]:
        return getattr(self, '_summaries_url', None)

    @property
    def titles_count(self) -> int:
        return getattr(self, '_titles_count', None)

    @property
    def titles_url(self) -> Optional[str]:
        return getattr(self, '_titles_url', None)

    @property
    def texts_count(self) -> int:
        return getattr(self, '_texts_count', None)

    @property
    def texts_url(self) -> Optional[str]:
        return getattr(self, '_texts_url', None)

    @property
    def committee_reports_urls(self) -> List[str]:
        return getattr(self, '_committee_reports_urls', [])

    @property
    def relationship_type(self) -> Optional[str]:
        return getattr(self, '_relationship_type', None)

    @property
    def relationship_identified_by_1(self) -> Optional[str]:
        return getattr(self, '_relationship_identified_by_1', None)

    @property
    def relationship_identified_by_2(self) -> Optional[str]:
        return getattr(self, '_relationship_identified_by_2', None)

    @property
    def relationship_identified_by_3(self) -> Optional[str]:
        return getattr(self, '_relationship_identified_by_3', None)

    @property
    def committee_action_date(self) -> Optional[str]:
        return getattr(self, '_action_date', None)

    @property
    def url(self) -> Optional[str]:
        return getattr(self, '_url', None)


    @property
    def _construct_endpoint(self) -> str:
        if self.congress and self.bill_type and self.bill_number:
            return f"/bill/{self.congress}/{self.bill_type.lower()}/{self.bill_number}"
        else:
            raise ValueError(
                "Object needs a congress, type, and bill number or previously defined endpoint."
            )

    async def get_committeeactivities(self):
        def process_committee_activities(items, _id_package_init):
            processed_items = []
            for committee in items.get("committees", []):
                committee_name = committee.get("name")
                committee_code = committee.get("systemCode")
                chamber = committee.get("chamber")

                for activity in committee.get("activities", []):
                    processed_items.append(CommitteeActivity(data={
                        "activity_date": activity.get("date", ''),
                        "activity_name": activity.get("name", ''),
                        "committee": committee_name,
                        "committee_code": committee_code,
                        "chamber": chamber,
                        "subcommittee": None,
                        "subcommittee_code": None
                    }, _id_package_init=_id_package_init))

                for subcommittee in committee.get("subcommittees", []):
                    subcommittee_name = subcommittee.get("name")
                    subcommittee_code = subcommittee.get("systemCode")

                    for subactivity in subcommittee.get("activities", []):
                        processed_items.append(CommitteeActivity(data={
                            "activity_date": subactivity.get("date", ''),
                            "activity_name": subactivity.get("name", ''),
                            "committee": committee_name,
                            "committee_code": committee_code,
                            "chamber": chamber,
                            "subcommittee": subcommittee_name,
                            "subcommittee_code": subcommittee_code
                        }, _id_package_init=_id_package_init))

            return processed_items

        try:
            async for items in self._get_items("committees_url", process_committee_activities, _id_package_init=self._id_package):
                for item in items:
                    yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving committee activities: {e}")

    async def get_actions(self):
        try:
            async for item in self._get_items("actions_url", Action, _id_package_init=self._id_package):
                yield item
        except PyCongressException as e:
                self._adapter._logger.error(f"Error retrieving actions: {e}")

    async def get_amendments(self, verbose=False):
        try:
            async for item in self._get_items("amendments_url", Amendment, verbose=verbose, _id_package_init=self._id_package):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving amendments: {e}")

    async def get_committee_reports(self):
        try:
            async for item in self._get_items("committee_reports_urls", CommitteeReport, _id_package_init=self._id_package):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving committee reports: {e}")

    async def get_cosponsors(self, verbose=False):
        try:
            async for item in self._get_items(
                "cosponsors_url", Member, verbose=verbose, _id_package_init=self._id_package
            ):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving cosponsors: {e}")

    async def get_sponsor_details(self):
        try:
            async for item in self._get_items(
                "sponsors_urls", Member, verbose=True, _id_package_init=self._id_package
            ):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving sponsors: {e}")

    async def get_subjects(self):
        try:
            async for item in self._get_items("subjects_url", Subject, _id_package_init=self._id_package):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving subjects: {e}")

    async def get_summaries(self):
        try:
            async for item in self._get_items("summaries_url", Summary, _id_package_init=self._id_package):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving summaries: {e}")

    async def get_texts(self):
        try:
            async for item in self._get_items(
                "texts_url", TextVersion, _id_package_init=self._id_package
            ):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving text versions: {e}")

    async def get_titles(self):
        try:
            async for item in self._get_items("titles_url", Title, _id_package_init=self._id_package):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving titles: {e}")

    async def get_billrelations(self):
        try:
            async for item in self._get_items("billrelations_url", Bill, _id_package_init=self._id_package):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving related bills: {e}")

@dataclass
class Member(Retriever):
    data: Dict
    _adapter: RestAdapter
    _pagination: Dict
    _id_package_init: Tuple = None

    def __init__(self, data: Dict, _pagination: Dict, _adapter: RestAdapter, _id_package_init: Optional[Tuple] = None):
        if "member" in data and isinstance(data.get("member"), dict):
            self.data = data.get("member")
        else:
            self.data = data
        self._pagination = _pagination

        super().__init__(data=self.data, _adapter=_adapter)
        self._id_package_init = _id_package_init

        self._parse_data()
        self._post_init_processing()

    def _post_init_processing(self):
        if self._id_package_init:
            setattr(self, self._id_package_init[0], self._id_package_init[1])

        self._setup_id_package()

        self._parse_terms()
        self._parse_party_history()
        self._parse_leadership_roles()
        self._parse_address_information()
        self._parse_cosponsored_legislation()
        self._parse_sponsored_legislation()
        self._parse_depiction()
        self.fix_party_history()

        if self.bioguide_id == "L000550":
            self._adapter._logger.warning(f"Member {self.bioguide_id} not found in API. Returning None.")
            time.sleep(30)


    def __str__(self):
        return f"{self.first_name.title()} {self.last_name.title()}" if self.first_name and self.last_name else "Member"

    def __repr__(self):
        return self.__str__()

    def _setup_id_package(self):
        setattr(self, "_id_package", ("bioguide_id", getattr(self, "bioguide_id", getattr(self, "_bioguide_id", "-99"))))

    def _parse_terms(self):
        terms = getattr(self, "_terms", [])
        if isinstance(terms, dict):
            terms = terms.get('item')
        self._terms = [MemberTerm(data=term, _id_package_init=self._id_package) for term in terms]

    def _parse_party_history(self):
        party_history = getattr(self, "_party_history", [])
        self._party_history = [
            PartyHistory(data=party, _id_package_init=self._id_package)
            for party in party_history if isinstance(party, dict)
        ]

    def _parse_leadership_roles(self):
        leadership = getattr(self, "_leadership", [])
        self._leadership_roles = [LeadershipRole(data=role, _id_package_init=self._id_package, _terms=self._terms) for role in leadership]

    def _parse_address_information(self):
        address = getattr(self, "_address_information", {})
        self._office_address = address.get("officeAddress")
        self._office_city = address.get("city")
        self._office_district = address.get("district")
        self._office_zip = address.get("zipCode")
        self._office_phone_number = address.get("phoneNumber")


    def _parse_cosponsored_legislation(self):
        cosponsored_legislation = getattr(self, "_cosponsored_legislation", {})
        self._cosponsored_legislation_url = cosponsored_legislation.get("url")
        self._cosponsored_legislation_count = cosponsored_legislation.get("count", 0)

    def _parse_sponsored_legislation(self):
        sponsored_legislation = getattr(self, "_sponsored_legislation", {})
        self._sponsored_legislation_url = sponsored_legislation.get("url")
        self._sponsored_legislation_count = sponsored_legislation.get("count", 0)

    def _parse_depiction(self):
        depiction = getattr(self, "_depiction", {})
        self._depiction_image_url = depiction.get("imageUrl")
        self._depiction_attribution = depiction.get("attribution")

    def fix_party_history(self):
        if not getattr(self, "party", None) and not getattr(self, "_party_code", None):
            current_party = next((party for party in self._party_history if party.end_year is None), None)
            if current_party:
                self._party = current_party.party_name
                self._party_code = current_party.party_code

    @property
    def bioguide_id(self) -> Optional[str]:
        return getattr(self, "_bioguide_id", "-99")

    @property
    def birth_year(self) -> Optional[str]:
        return getattr(self, "_birth_year", None)

    @property
    def death_year(self) -> Optional[str]:
        return getattr(self, "_death_year", None)

    @property
    def direct_order_name(self) -> Optional[str]:
        return getattr(self, "_direct_order_name", None)

    @property
    def district(self) -> Optional[str]:
        return getattr(self, "_district", None)

    @property
    def first_name(self) -> Optional[str]:
        name = getattr(self, "_first_name", None)
        return name.upper() if isinstance(name, str) else name

    @property
    def honorific_name(self) -> Optional[str]:
        return getattr(self, "_honorific_name", None)

    @property
    def inverted_order_name(self) -> Optional[str]:
        return getattr(self, "_inverted_order_name", None)

    @property
    def last_name(self) -> Optional[str]:
        name = getattr(self, "_last_name", None)
        return name.upper() if isinstance(name, str) else name

    @property
    def middle_name(self) -> Optional[str]:
        name = getattr(self, "_middle_name", None)
        return name.upper() if isinstance(name, str) else name

    @property
    def full_name(self) -> Optional[str]:
        return getattr(self, "_full_name", None)

    @property
    def is_current_member(self) -> Optional[bool]:
        return bool(getattr(self, "_current_member", None)) if getattr(self, "_current_member", None) is not None else None

    @property
    def sponsorship_date(self) -> Optional[str]:
        return getattr(self, "_sponsorship_date", None)

    @property
    def party_code(self) -> Optional[str]:
        return getattr(self, "_party_code", None)

    @property
    def party(self) -> Optional[str]:
        return getattr(self, "_party", None)

    @property
    def state(self) -> Optional[str]:
        return getattr(self, "_state", None)

    @property
    def updated_at(self) -> Optional[str]:
        return getattr(self, "_update_date", None)

    @property
    def nickname(self) -> Optional[str]:
        return getattr(self, "_nick_name", None)

    @property
    def official_url(self) -> Optional[str]:
        return getattr(self, "_official_website_url", None)

    @property
    def suffix_name(self) -> Optional[str]:
        return getattr(self, "_suffix_name", None)

    @property
    def office_address(self) -> Optional[str]:
        return getattr(self, "_office_address", None)

    @property
    def office_city(self) -> Optional[str]:
        return getattr(self, "_office_city", None)

    @property
    def office_district(self) -> Optional[str]:
        return getattr(self, "_office_district", None)

    @property
    def office_zip(self) -> Optional[str]:
        return getattr(self, "_office_zip", None)

    @property
    def office_phone_number(self) -> Optional[str]:
        return getattr(self, "_office_phone_number", None)

    @property
    def terms(self) -> Optional[List[MemberTerm]]:
        return getattr(self, "_terms", [])

    @property
    def party_history(self) -> Optional[List[PartyHistory]]:
        return getattr(self, "_party_history", [])

    @property
    def leadership_roles(self) -> Optional[List[LeadershipRole]]:
        return getattr(self, "_leadership_roles", [])

    @property
    def cosponsored_legislation_count(self) -> int:
        return getattr(self, "_cosponsored_legislation_count", None)

    @property
    def cosponsored_legislation_url(self) -> Optional[str]:
        return getattr(self, "_cosponsored_legislation_url", None)

    @property
    def sponsored_legislation_count(self) -> int:
        return getattr(self, "_sponsored_legislation_count", None)

    @property
    def sponsored_legislation_url(self) -> Optional[str]:
        return getattr(self, "_sponsored_legislation_url", None)

    @property
    def depiction_image_url(self) -> Optional[str]:
        return getattr(self, "_depiction_image_url", None)

    @property
    def depiction_attribution(self) -> Optional[str]:
        return getattr(self, "_depiction_attribution", None)

    @property
    def is_by_request(self) -> bool:
        return getattr(self, "_is_by_request", None) == 'Y' if getattr(self, "_is_by_request", None) is not None else None

    @property
    def is_original_cosponsor(self) -> Optional[bool]:
        return getattr(self, "_is_original_cosponsor", None) if getattr(self, "_is_original_cosponsor", None) is not None else None

    @property
    def url(self) -> Optional[str]:
        return getattr(self, "_url", None)

    def _construct_endpoint(self, path):
        if self.bioguide_id:
            _endpoint = f"/member/{self.bioguide_id}"
        else:
            raise ValueError(
                "Member object needs a Bioguide ID or previously defined endpoint for further functions."
            )
        return f"{_endpoint}/{path}"


    async def get_cosponsored_legislation(self, verbose=False):

        async for item in self._get_items(
            "cosponsored_legislation_url", Bill, verbose=verbose, _id_package_init=self._id_package
        ):
            yield item

    async def get_sponsored_legislation(self, verbose=False):

        async for item in self._get_items(
            "sponsored_legislation_url", Bill, verbose=verbose, _id_package_init=self._id_package
        ):
            yield item

@dataclass
class Amendment(Retriever):
    data: Dict
    _adapter: RestAdapter
    _pagination: Dict
    _id_package_init: Tuple = None

    def __init__(self, data: Dict, _pagination: Dict, _adapter: RestAdapter, _id_package_init: Optional[Tuple] = None):
        if "amendment" in data and isinstance(data.get("amendment"), dict):
            self.data = data.get("amendment")
        else:
            self.data = data

        self._pagination = _pagination

        super().__init__(data=self.data, _adapter=_adapter)
        self._id_package_init = _id_package_init

        self._parse_data()
        self._post_init_processing()

    def _post_init_processing(self):
        if self._id_package_init:
            setattr(self, self._id_package_init[0], self._id_package_init[1])

        self._parse_amendment_id()

        self._setup_id_package()

        self._parse_actions()
        self._parse_amended_bill()
        self._parse_amended_amendment()
        self._parse_amended_treaty()
        self._parse_amendments_to_amendment()
        self._parse_cosponsors()
        self._parse_notes()
        self._parse_sponsors()
        self._parse_texts()
        self._parse_latest_action()


    def __str__(self):
        return f"{str(self.amendment_type).lower()}{self.amendment_number}-{self.congress}" if self.amendment_type and self.amendment_number and self.congress else "Amendment"

    def __repr__(self):
        return self.__str__()

    def _setup_id_package(self):
        setattr(self, "_id_package", ("amendment_id", getattr(self, "amendment_id", getattr(self, "_amendment_id", "-99"))))

    def _parse_amendment_id(self):
        congress = getattr(self, "_congress", None)
        type = getattr(self, "_type", None)
        number = getattr(self, "_number", None)

        if congress and type and number:
            setattr(self, "_amendment_id", f"{str(type).lower()}{number}-{congress}")
        else:
            setattr(self, "_amendment_id", "-99")

    def _parse_actions(self):
        actions = getattr(self, "_actions", {})
        self._actions_url = actions.get("url")
        self._actions_count = actions.get("count", 0)

    def _parse_amended_bill(self):
        amended_bill = getattr(self, "_amended_bill", {})
        self._amended_bill_url = amended_bill.get("url")
        self._amended_bill_id = (
                f"{str(amended_bill.get('type')).lower()}{amended_bill.get('number')}-{amended_bill.get('congress')}"
                if amended_bill
                else None
            )

    def _parse_amended_amendment(self):
        amended_amendment = getattr(self, "_amended_amendment", {})
        self._amended_amendment_url = amended_amendment.get("url")
        self._amended_amendment_id = (
                f"{str(amended_amendment.get('type')).lower()}{amended_amendment.get('number')}-{amended_amendment.get('congress')}"
                if amended_amendment
                else None
            )

    def _parse_amended_treaty(self):
        amended_treaty = getattr(self, "_amended_treaty", {})
        self._amended_treaty_url = amended_treaty.get("url")
        self._amended_treaty_id = (
                f"td{amended_treaty.get('congress')}-{amended_treaty.get('treatyNumber')}"
                if amended_treaty
                else None)

    def _parse_amendments_to_amendment(self):
        amendments_to_amendment = getattr(self, "_amendments_to_amendment", {})
        self._amendments_to_amendment_url = amendments_to_amendment.get("url")
        self._amendments_to_amendment_count = amendments_to_amendment.get("count", 0)

    def _parse_cosponsors(self):
        cosponsors = getattr(self, "_cosponsors", {})
        self._cosponsors_url = cosponsors.get("url")
        self._cosponsors_count = cosponsors.get("count", 0)

    def _parse_notes(self):
        notes = getattr(self, "_notes", [])
        self._notes = [note.get("text") for note in notes if isinstance(note, dict) and "text" in note]

    def _parse_sponsors(self):
        sponsors = getattr(self, "_sponsors", [])
        self._sponsors_urls = [sponsor.get("url") for sponsor in sponsors if isinstance(sponsor, dict) and "url" in sponsor]
        self._sponsors_bioguide_ids = [sponsor.get("bioguideId") for sponsor in sponsors if isinstance(sponsor, dict) and "bioguideId" in sponsor]
        self._sponsors_system_codes = [sponsor.get("systemCode") for sponsor in sponsors if isinstance(sponsor, dict) and "systemCode" in sponsor]
        if self._sponsors_system_codes:
            self._adapter._logger.info(f"Sponsor System Codes: {self._sponsors_system_codes}")

    def _parse_texts(self):
        texts_endpoint = self._construct_endpoint("text")
        self._texts_url = f"https://api.congress.gov/v3/{texts_endpoint}"

    def _parse_latest_action(self):
        latest_action = getattr(self, "_latest_action", {})
        self._latest_action = Action(data=latest_action, _pagination=None, _adapter=self._adapter, _id_package_init=self._id_package) if latest_action else None


    @property
    def congress(self) -> Optional[int]:
        return getattr(self, "_congress", None)
    
    @property
    def chamber(self) -> Optional[str]:
        return getattr(self, "_chamber", None)

    @property
    def amendment_type(self) -> Optional[str]:
        return getattr(self, "_type", None)

    @property
    def amendment_number(self) -> Optional[str]:
        return getattr(self, "_number", None)

    @property
    def proposed_at(self) -> Optional[str]:
        return getattr(self, "_proposed_date", None)

    @property
    def submitted_at(self) -> Optional[str]:
        return getattr(self, "_submitted_date", None)

    @property
    def title(self) -> Optional[str]:
        return getattr(self, "_title", None)

    @property
    def updated_at(self) -> Optional[str]:
        return getattr(self, "_update_date", None)

    @property
    def purpose(self) -> str:
        return getattr(self, "_purpose", "MISSING")

    @property
    def description(self) -> str:
        return getattr(self, "_description", "MISSING")

    @property
    def amendment_id(self) -> Union[str, int]:
        return getattr(self, "_amendment_id", None)

    @property
    def links(self) -> Optional[List[Dict]]:
        return getattr(self, "_links", [])

    @property
    def actions_url(self) -> Optional[str]:
        return getattr(self, "_actions_url", None)

    @property
    def actions_count(self) -> int:
        return getattr(self, "_actions_count", 0)

    @property
    def amended_bills_url(self) -> Optional[str]:
        return getattr(self, "_amended_bill_url", None)

    @property
    def amended_bill_id(self) -> Optional[str]:
        return getattr(self, "_amended_bill_id", None)

    @property
    def amended_amendments_url(self) -> Optional[str]:
        return getattr(self, "_amended_amendment_url", None)

    @property
    def amended_amendment_id(self) -> Optional[str]:
        return getattr(self, "_amended_amendment_id", None)

    @property
    def amended_treaties_url(self) -> Optional[str]:
        return getattr(self, "_amended_treaty_url", None)

    @property
    def amended_treaty_id(self) -> Optional[str]:
        return getattr(self, "_amended_treaty_id", None)

    @property
    def amendments_to_amendment_url(self) -> Optional[str]:
        return getattr(self, "_amendments_to_amendment_url", None)

    @property
    def amendments_to_amendment_count(self) -> int:
        return getattr(self, "_amendments_to_amendment_count", None)

    @property
    def cosponsors_url(self) -> Optional[str]:
        return getattr(self, "_cosponsors_url", None)

    @property
    def cosponsors_count(self) -> int:
        return getattr(self, "_cosponsors_count", None)

    @property
    def latest_action(self) -> Optional['Action']:
        return getattr(self, "_latest_action", None)

    @property
    def notes(self) -> Optional[str]:
        if len(getattr(self, "_notes", [])) > 1:
            self._adapter._logger.warning(f"Amendment {self.amendment_id} has multiple notes. Returning the first one.")
        return getattr(self, "_notes", [])[0] if len(getattr(self, "_notes", [])) > 0 else None

    @property
    def sponsors_url(self) -> Optional[str]:
        return getattr(self, "_sponsors_urls", None)

    @property
    def sponsors(self) -> List[str]:
        return getattr(self, "_sponsors_bioguide_ids", [])

    @property
    def sponsor_committees(self) -> List[str]:
        return getattr(self, "_sponsors_system_codes", [])

    @property
    def texts_url(self) -> Optional[str]:
        return getattr(self, "_texts_url", None)

    @property
    def url(self) -> Optional[str]:
        return getattr(self, "_url", None)

    def _construct_endpoint(self, path):
        if self.congress and self.amendment_type and self.amendment_number:
            _endpoint = f"/amendment/{self.congress}/{str(self.amendment_type).lower()}/{self.amendment_number}"
        else:
            raise ValueError(
                "Amendment object needs a congress, type, and amendment number or previously defined endpoint for further functions."
            )
        return f"{_endpoint}/{path}"

    async def get_texts(self):
        try:
            async for item in self._get_items("texts_url", TextVersion, _id_package_init=self._id_package):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving text versions: {e}")

    async def get_actions(self):
        try:
            async for item in self._get_items("actions_url", Action, _id_package_init=self._id_package):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving actions: {e}")

    async def get_amendedbill(self):
        try:
            async for item in self._get_items("amended_bills_url", Bill, _id_package_init=self._id_package):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving amended bill details: {e}")

    async def get_amended_amendment_details(self):
        try:
            async for item in self._get_items("amended_amendments_url", Amendment, _id_package_init=self._id_package):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving amended amendment details: {e}")

    async def get_amended_treaty_details(self):
        try:
            async for item in self._get_items("amended_treaties_url", Treaty, _id_package_init=self._id_package):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving amended treaty details: {e}")

    async def get_amendments_to_amendment(self, verbose=False):
        try:
            async for item in self._get_items("amendments_to_amendment_url", Amendment, verbose=verbose, _id_package_init=self._id_package):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving amendments to amendment: {e}")

    async def get_cosponsors(self, verbose=False):
        try:
            async for item in self._get_items("cosponsors_url", Member, verbose=verbose, _id_package_init=self._id_package):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving cosponsors: {e}")
    async def get_sponsors(self, verbose=False):
        try:
            async for item in self._get_items(
                "sponsors_urls", Member, verbose=verbose, _id_package_init=self._id_package
            ):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving sponsors: {e}")

@dataclass
class Committee(Retriever):
    data: Dict
    _adapter: RestAdapter
    _pagination: Dict
    _id_package_init: Tuple = None

    def __init__(self, data: Dict, _pagination: Dict, _adapter: RestAdapter, _id_package_init: Optional[Tuple] = None):
        if "committee" in data and isinstance(data.get("committee"), dict):
            self.data = data.get("committee")
        else:
            self.data = data

        self._pagination = _pagination

        super().__init__(data=self.data, _adapter=_adapter)
        self._id_package_init = _id_package_init

        self._parse_data()
        self._post_init_processing()

    def _post_init_processing(self):
        if self._id_package_init:
            if self._id_package_init[0] == 'committee_code':
                self._parent_committee_code = self._id_package_init[1]
            else:
                setattr(self, self._id_package_init[0], self._id_package_init[1])

        self._setup_id_package()
        self._parse_bills()
        self._parse_committee_history()
        self._parse_nominations()
        self._parse_parent()
        self._parse_reports()
        self._parse_subcommittees()

    def __str__(self):
        return self.name if self.name else self.committee_code or "Committee"

    def __repr__(self):
        return self.__str__()

    def _setup_id_package(self):
        setattr(self, "_id_package", ("committee_code", getattr(self, "committee_code", getattr(self, "_system_code", "-99"))))

    def _parse_bills(self):
        bills = getattr(self, "_bills", {})
        self._bills_url = bills.get("url")
        self._bills_count = bills.get("count", 0)

    def _parse_committee_history(self):
        self._committee_history_data = getattr(self, "_history", {})
        self._committee_history = [CommitteeHistory(data=hist, _id_package_init=self._id_package) for hist in self._committee_history_data if isinstance(hist, dict)]

    def _parse_nominations(self):
        nominations = getattr(self, "_nominations", {})
        self._nominations_url = nominations.get("url")
        self._nominations_count = nominations.get("count", 0)

    def _parse_parent(self):
        parent = getattr(self, "_parent", {})
        self._parent_url = parent.get("url")
        self._parent_committee_name = parent.get("name")
        self._parent_committee_code = parent.get("systemCode")
        if not parent:
            self._is_subcommittee = False
        else:
            self._is_subcommittee = True

    def _parse_reports(self):
        reports = getattr(self, "_reports", {})
        self._reports_url = reports.get("url")
        self._reports_count = reports.get("count", 0)

    def _parse_subcommittees(self):
        subcommittees = getattr(self, "_subcommittees", [])
        self._subcommittees_urls = [subcommittee.get("url") for subcommittee in subcommittees if isinstance(subcommittee, dict)]
        self._subcommittee_names = [subcommittee.get("name") for subcommittee in subcommittees if isinstance(subcommittee, dict)]
        self._subcommittee_codes = [subcommittee.get("systemCode") for subcommittee in subcommittees if isinstance(subcommittee, dict)]

    @property
    def chamber(self) -> str:
        return getattr(self, "_chamber", "MISSING")

    @property
    def is_current(self) -> Optional[bool]:
        return getattr(self, "_is_current", None)

    @property
    def name(self) -> str:
        return getattr(self, "_name", "MISSING")

    @property
    def committee_code(self) -> str:
        return getattr(self, "_system_code", "-99")

    @property
    def committee_type(self) -> Optional[str]:
        return getattr(self, "_type", None)

    @property
    def updated_at(self) -> Optional[str]:
        return getattr(self, "_update_date", None)

    @property
    def bills_url(self) -> Optional[str]:
        return getattr(self, "_bills_url", None)

    @property
    def bills_count(self) -> int:
        return getattr(self, "_bills_count", None)

    @property
    def history(self) -> List['CommitteeHistory']:
        return getattr(self, "_committee_history", [])

    @property
    def nominations_url(self) -> Optional[str]:
        return getattr(self, "_nominations_url", None)

    @property
    def nominations_count(self) -> int:
        return getattr(self, "_nominations_count", None)

    @property
    def is_subcommittee(self) -> bool:
        return bool(getattr(self, "_is_subcommittee", None)) if getattr(self, "_is_subcommittee", None) is not None else None

    @property
    def parent_url(self) -> Optional[str]:
        return getattr(self, "_parent_url", None)

    @property
    def parent_committee_name(self) -> Optional[str]:
        return getattr(self, "_parent_committee_name", None)

    @property
    def parent_committee_code(self) -> Optional[str]:
        return getattr(self, "_parent_committee_code", None)

    @property
    def reports_url(self) -> Optional[str]:
        return getattr(self, "_reports_url", None)

    @property
    def reports_count(self) -> int:
        return getattr(self, "_reports_count", None)

    @property
    def subcommittees_urls(self) -> List[str]:
        return getattr(self, "_subcommittees_urls", [])

    @property
    def subcommittee_names(self) -> List[str]:
        return getattr(self, "_subcommittee_names", [])

    @property
    def subcommittees_codes(self) -> List[str]:
        return getattr(self, "_subcommittee_codes", [])

    @property
    def url(self) -> Optional[str]:
        return getattr(self, "_url", None)

    def _construct_endpoint(self, path):
        if self.chamber and self.committee_code:
            _endpoint = f"/committee/{self.chamber}/{self.committee_code}"
        else:
            raise ValueError(
                "Committee object needs a chamber and system code or previously defined endpoint for further functions."
            )
        return f"{_endpoint}/{path}"

    async def get_bills(self, verbose=False):
        try:
            async for item in self._get_items("bills_url", Bill, verbose=verbose, _id_package_init=self._id_package):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving bills: {e}")

    async def get_subcommittees(self):
        if self.is_subcommittee:
            yield self._adapter._logger.info(
                "This is a subcommittee, so there will be no committees available. Try 'get_parent_committee_details'."
            )
        try:
            async for item in self._get_items("subcommittees_urls", Committee, verbose=False, _id_package_init=self._id_package):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving subcommittees: {e}")

    async def get_parent_committee_details(self):
        if not self.is_subcommittee:
            yield self._adapter._logger.info(
                "This is not a subcommittee, so there will be no parent committee available. Try 'get_subcommittee_details'."
            )
        try:
            async for item in self._get_items("parent_url", Committee, _id_package_init=self._id_package):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving parent committee: {e}")

    async def get_associated_reports(self, verbose=False):
        try:
            async for item in self._get_items("reports_url", CommitteeReport, verbose=verbose, _id_package_init=self._id_package):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving reports: {e}")

    async def get_nominations(self, verbose=False):
        try:
            async for item in self._get_items("nominations_url", Nomination, verbose=verbose, _id_package_init=self._id_package):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving nominations: {e}")

    async def get_communications(self):
        raise NotImplementedError("Communications endpoint not yet implemented.")

@dataclass
class CommitteeReport(Retriever):
    data: Dict
    _adapter: RestAdapter
    _pagination: Dict
    _id_package_init: Tuple = None

    def __init__(self, data: Dict, _pagination: Dict, _adapter: RestAdapter, _id_package_init: Optional[Tuple] = None):
        if "committeeReports" in data and isinstance(data.get("committeeReports"), list):
            self.data = data.get("committeeReports")[0]  # quirk of committeeReports endpoint
        else:
            self.data = data

        self._pagination = _pagination

        super().__init__(data=self.data, _adapter=_adapter)
        self._id_package_init = _id_package_init

        self._parse_data()
        self._post_init_processing()

    def _post_init_processing(self):
        if self._id_package_init:
            setattr(self, self._id_package_init[0], self._id_package_init[1])

        self._parse_report_id()
        self._setup_id_package()
        self._parse_texts()
        self._parse_associated_bills()
        self._parse_associated_treaties()

    def __str__(self):
        return (
            f"{str(self.report_type).lower().replace('.', '')}rpt{self.report_number}-{self.congress}"
            if all([self.report_type, self.report_number, self.congress])
            else self.citation or "Committee Report"
        )

    def __repr__(self):
        return self.__str__()

    def _setup_id_package(self):
        setattr(self, "_id_package", ("report_id", getattr(self, "report_id", getattr(self, "_report_id", "-99"))))

    def _parse_report_id(self):
        self._congress = getattr(self, "_congress", None)
        self._report_type = getattr(self, "_report_type", None)
        self._report_number = getattr(self, "_number", None)
        self._report_part = getattr(self, "_part", None)
        self._citation = getattr(self, "_citation", None)

        if self._congress and self._report_type and self._report_number and self._report_part:
            self._report_id = f"{str(self.report_type).lower().replace('.', '')}rpt{self.report_number}-{self.report_part}-{self.congress}"
        elif self._congress and self._report_type and self._report_number:
            self._report_id = f"{str(self.report_type).lower().replace('.', '')}rpt{self.report_number}-{self.congress}"
        elif self._citation:
            self._report_id = process_report_id(self._citation)
        else:
            self._report_id = "-99"

    def _parse_texts(self):
        texts = getattr(self, "_text", {})
        self._texts_url = texts.get("url")
        self._texts_count = texts.get("count", 0)

    def _parse_associated_bills(self):
        associated_bills = getattr(self, "_associated_bill", [])
        self._associated_bills_urls = [bill.get("url") for bill in associated_bills if isinstance(bill, dict)]
        self._associated_bills_ids = [
            f"{str(bill.get('type')).lower()}{bill.get('number')}-{bill.get('congress')}"
            for bill in associated_bills
        ]

    def _parse_associated_treaties(self):
        associated_treaties = getattr(self, "_associated_treaties", [])
        self._associated_treaties_urls = [treaty.get("url") for treaty in associated_treaties if isinstance(treaty, dict)]
        self._associated_treaties_ids = [
            f"td{treaty.get('congress')}-{treaty.get('number')}"
            for treaty in associated_treaties
        ]

    @property
    def chamber(self) -> Optional[str]:
        return getattr(self, "_chamber", None)

    @property
    def citation(self) -> Optional[str]:
        return self._citation or None

    @property
    def congress(self) -> Optional[int]:
        return self._congress or None

    @property
    def is_conference_report(self) -> Optional[bool]:
        return getattr(self, "_is_conference_report", None)

    @property
    def issued_at(self) -> Optional[str]:
        return getattr(self, "_issue_date", None)

    @property
    def report_number(self) -> Optional[str]:
        return self._report_number or None

    @property
    def report_part(self) -> Optional[str]:
        return self._report_part or None

    @property
    def report_type(self) -> Optional[str]:
        return self._report_type or None

    @property
    def session_number(self) -> Optional[str]:
        return getattr(self, "_session_number", None)

    @property
    def title(self) -> Optional[str]:
        return getattr(self, "_title", None)

    @property
    def updated_at(self) -> Optional[str]:
        return getattr(self, "_update_date", None)

    @property
    def report_id(self) -> str:
        return self._report_id or None

    @property
    def texts_url(self) -> Optional[str]:
        return getattr(self, "_texts_url", None)

    @property
    def texts_count(self) -> int:
        return getattr(self, "_texts_count", None)

    @property
    def associated_bills_urls(self) -> List[str]:
        return getattr(self, "_associated_bills_urls", [])

    @property
    def associated_bills_ids(self) -> List[str]:
        return getattr(self, "_associated_bills_ids", [])

    @property
    def associated_treaties_urls(self) -> List[str]:
        return getattr(self, "_associated_treaties_urls", [])

    @property
    def associated_treaties_ids(self) -> List[str]:
        return getattr(self, "_associated_treaties_ids", [])

    @property
    def url(self) -> Optional[str]:
        return getattr(self, "_url", None)

    def _construct_endpoint(self, path):
        if self.congress and self.report_type and self.report_number:
            _endpoint = f"/committee-report/{self.congress}/{self.report_type.lower()}/{self.report_number}"
        else:
            raise ValueError(
                "CommitteeReport object needs a congress, type, and amendment number or previously defined endpoint for further functions."
            )
        return f"{_endpoint}/{path}"

    async def get_texts(self):
        try:
            async for item in self._get_items("texts_url", TextVersion, _id_package_init=self._id_package):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving text versions: {e}")

    async def get_associated_bill_details(self):
        try:
            async for item in self._get_items("associated_bills_urls", Bill, _id_package_init=self._id_package):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving associated bill details: {e}")

    async def get_associated_treaty_details(self):
        try:
            async for item in self._get_items("associated_treaties_urls", Treaty, _id_package_init=self._id_package):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving associated treaty details: {e}")

@dataclass
class Treaty(Retriever):
    data: Dict
    _adapter: RestAdapter
    _pagination: Dict
    _id_package_init: Tuple = None

    def __init__(self, data: Dict, _pagination: Dict, _adapter: RestAdapter, _id_package_init: Optional[Tuple] = None):
        header = data.get("request")
        if header:
            data["congress"] = header.get("congress")
        else:
            data["congress"] = None

        if "treaty" in data and isinstance(data.get("treaty"), dict):
            self.data = data.get("treaty")
        else:
            self.data = data

        self._pagination = _pagination

        super().__init__(data=self.data, _adapter=_adapter)
        self._id_package_init = _id_package_init

        self._parse_data()
        self._post_init_processing()

    def _post_init_processing(self):
        if self._id_package_init:
            setattr(self, self._id_package_init[0], self._id_package_init[1])

        self._parse_treaty_id()
        self._setup_id_package()
        self._parse_actions()
        self._parse_countries_parties()
        self._parse_index_terms()
        self._parse_parts()
        self._parse_related_docs()
        self._parse_titles()

    def __str__(self):
        treaty_num = f"{self.number}{self.suffix}" if self.suffix else self.number
        return (
            f"td{self.congress_received}-{treaty_num}"
            if all([self.congress_received, treaty_num])
            else self.titles[0].title if self.titles else "Treaty"
        )

    def __repr__(self):
        return self.__str__()

    def _setup_id_package(self):
        setattr(self, "_id_package", ("treaty_id", getattr(self, "treaty_id", getattr(self, "_treaty_id", "-99"))))

    def _parse_treaty_id(self):
        self._congress_received = getattr(self, "_congress_received", None)
        self._number = getattr(self, "_number", None)
        self._suffix = getattr(self, "_suffix", None)

        if self._congress_received and self._number:
            self._treaty_id = f"td{self.congress_received}-{self.number}"
        elif self._congress_received and self._number and self._suffix:
            self._treaty_id = f"td{self.congress_received}-{self.number}{self.suffix}"
        else:
            self._treaty_id = "-99"

    def _parse_actions(self):
        actions = getattr(self, "_actions", {})
        self._actions_url = actions.get("url")
        self._actions_count = actions.get("count", 0)

    def _parse_countries_parties(self):
        countriesParties = getattr(self, "_countries_parties", [])
        self._countries_parties = [country.get("name") for country in countriesParties if isinstance(country, dict)]

    def _parse_index_terms(self):
        indexTerms = getattr(self, "_index_terms", [])
        self._index_terms = [term.get("name") for term in indexTerms if isinstance(term, dict)]

    def _parse_parts(self):
        parts = getattr(self, "_parts", {})
        self._parts_urls = parts.get("urls", [])
        self._parts_count = parts.get("count", 0)

    def _parse_related_docs(self):
        relatedDocs = getattr(self, "_related_docs", [])
        self._related_documents_urls = [doc.get("url") for doc in relatedDocs if isinstance(doc, dict)]
        self._related_documents = [doc.get("citation") for doc in relatedDocs if isinstance(doc, dict)]

    def _parse_titles(self):
        titles = getattr(self, "_titles", {})
        self._titles = [Title(data=title, _id_package_init=self._id_package) for title in titles if isinstance(title, dict)]

    @property
    def congress(self) -> Optional[str]:
        return getattr(self, "_congress", None)

    @property
    def congress_considered(self) -> Optional[int]:
        return getattr(self, "_congress_considered", None)

    @property
    def congress_received(self) -> Optional[int]:
        return self._congress_received or None

    @property
    def in_force_at(self) -> Optional[str]:
        return getattr(self, "_in_force_date", None)

    @property
    def number(self) -> Optional[str]:
        return self._number or None

    @property
    def old_number(self) -> Optional[str]:
        return getattr(self, "_old_number", None)

    @property
    def old_number_display_name(self) -> Optional[str]:
        return getattr(self, "_old_number_display_name", None)

    @property
    def resolution_text(self) -> Optional[str]:
        return getattr(self, "_resolution_text", None)

    @property
    def suffix(self) -> Optional[str]:
        return self._suffix or None

    @property
    def topic(self) -> Optional[str]:
        return getattr(self, "_topic", None)

    @property
    def transmitted_at(self) -> Optional[str]:
        return getattr(self, "_transmitted_date", None)

    @property
    def treaty_number(self) -> Optional[str]:
        return getattr(self, "_treaty_num", None)

    @property
    def updated_at(self) -> Optional[str]:
        return getattr(self, "_update_date", None)

    @property
    def treaty_id(self) -> str:
        return self._treaty_id or None

    @property
    def actions_url(self) -> Optional[str]:
        return getattr(self, "_actions_url", None)

    @property
    def actions_count(self) -> int:
        return getattr(self, "_actions_count", None)

    @property
    def country_parties(self) -> List[str]:
        return getattr(self, "_countries_parties", [])

    @property
    def index_terms(self) -> List[str]:
        return getattr(self, "_index_terms", [])

    @property
    def parts_urls(self) -> List[str]:
        return getattr(self, "_parts_urls", [])

    @property
    def parts_count(self) -> int:
        return getattr(self, "_parts_count", None)

    @property
    def associated_reports_urls(self) -> List[str]:
        return getattr(self, "_related_documents_urls", [])

    @property
    def associated_reports(self) -> List[str]:
        return getattr(self, "_related_documents", [])

    @property
    def titles(self) -> List['Title']:
        return getattr(self, "_titles", [])

    @property
    def url(self) -> Optional[str]:
        return getattr(self, "_url", None)

    def _construct_endpoint(self, path):
        if self.congress_received and self.number:
            _endpoint = f"/treaty/{self.congress_received}/{self.number}"
        else:
            raise ValueError(
                "Treaty object needs a congress and treaty number or previously defined endpoint for further functions."
            )
        return f"{_endpoint}/{path}"

    async def get_actions(self):
        try:
            async for item in self._get_items("actions_url", Action, _id_package_init=self._id_package):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving actions: {e}")

    async def get_parts(self, verbose=False):
        if verbose:
            async for item in self._get_items("parts_urls", Treaty, verbose=False, _id_package_init=self._id_package):
                yield item
        else:
            items = []
            async for part_data in self._get_items("parts", None, verbose=verbose, _id_package_init=self._id_package):
                if isinstance(part_data, dict) and "suffix" in part_data:
                    items.append(part_data["suffix"])
                    yield sorted(items)
                else:
                    self._adapter._logger.warning(f"Unexpected part data format: {part_data}")

    async def get_associated_reports(self):
        try:
            async for item in self._get_items("related_docs_urls", CommitteeReport, _id_package_init=self._id_package):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving related documents: {e}")

@dataclass
class Nomination(Retriever):
    data: Dict
    _adapter: RestAdapter
    _pagination: Dict
    _id_package_init: Optional[Tuple] = None

    def __init__(self, data: Dict, _pagination: Dict, _adapter: RestAdapter, _id_package_init: Optional[Tuple] = None):
        if "nomination" in data and isinstance(data.get("nomination"), dict):
            self.data = data.get("nomination")
        else:
            self.data = data

        self._pagination = _pagination

        super().__init__(data=self.data, _adapter=_adapter)
        self._id_package_init = _id_package_init

        self._parse_data()
        self._post_init_processing()

    def _post_init_processing(self):
        if self._id_package_init:
            setattr(self, self._id_package_init[0], self._id_package_init[1])

        self._parse_nomination_id()
        self._setup_id_package()
        self._parse_actions()
        self._parse_committees()
        self._parse_nominees_and_positions()
        self._parse_hearings()
        self._parse_latest_action()

    def __str__(self):
        return (
            f"{self.citation}" if self.citation else f"{self.nomination_number}-{self.part_number}" or "Nomination"
        )

    def __repr__(self):
        return self.__str__()

    def _setup_id_package(self):
        setattr(self, "_id_package", ("nomination_id", getattr(self, "nomination_id", getattr(self, "_nomination_id", "-99"))))

    def _parse_nomination_id(self):
        self._congress = getattr(self, "_congress", None)
        self._citation = getattr(self, "_citation", None)
        self._part_number = getattr(self, "_part_number", None)

        if self._congress and self._citation and self._part_number and self._part_number != '00':
            self._nomination_id = f"{self._citation}-{self._congress}"
        elif self._congress and self._citation and self._part_number == '00':
            self._nomination_id = f"{self._citation}-00-{self._congress}"
        elif self._citation:
            self._nomination_id = self._citation
        else:
            self._nomination_id = "-99"

    def _parse_actions(self):
        actions = getattr(self, "_actions", {})
        self._actions_url = actions.get("url")
        self._actions_count = actions.get("count", 0)

    def _parse_committees(self):
        committees = getattr(self, "_committees", {})
        self._committees_url = committees.get("url")
        self._committees_count = committees.get("count", 0)

    def _parse_nominees_and_positions(self):
        nominees = getattr(self, "_nominees", [])
        self._nominees_urls = [nominee.get("url") for nominee in nominees if isinstance(nominee, dict)]
        self._positions = [NominationPosition(data=position, _id_package_init=self._id_package) for position in nominees if "ordinal" in position and isinstance(position, dict)]

    def _parse_hearings(self):
        hearings = getattr(self, "_hearings", {})
        self._hearings_url = hearings.get("url")
        self._hearings_count = hearings.get("count", 0)

    def _parse_latest_action(self):
        self._latest_action_data = getattr(self, "_latest_action", {})
        self._latest_action = Action(data=self._latest_action_data, _pagination=None, _adapter=self._adapter, _id_package_init=self._id_package) if self._latest_action_data else None

    @property
    def citation(self) -> Optional[str]:
        return self._citation or None

    @property
    def congress(self) -> Optional[int]:
        return self._congress or None

    @property
    def description(self) -> Optional[str]:
        return getattr(self, "_description", None)

    @property
    def executive_calendar_number(self) -> Optional[str]:
        return getattr(self, "_executive_calendar_number", None)

    @property
    def authority_date(self) -> Optional[str]:
        return getattr(self, "_authority_date", None)

    @property
    def is_civilian(self) -> bool:
        return getattr(self, "_is_list", False)

    @property
    def is_privileged(self) -> Optional[bool]:
        return getattr(self, "_is_privileged", False)

    @property
    def nomination_number(self) -> Optional[str]:
        return getattr(self, "_number", None)

    @property
    def part_number(self) -> Optional[str]:
        return self._part_number or None

    @property
    def received_at(self) -> Optional[str]:
        return getattr(self, "_received_date", None)

    @property
    def updated_at(self) -> Optional[str]:
        return getattr(self, "_update_date", None)

    @property
    def nomination_id(self) -> str:
        return self._nomination_id or "-99"

    @property
    def actions_count(self) -> int:
        return getattr(self, "_actions_count", None)

    @property
    def actions_url(self) -> Optional[str]:
        return getattr(self, "_actions_url", None)

    @property
    def committees_count(self) -> int:
        return getattr(self, "_committees_count", None)

    @property
    def committees_url(self) -> Optional[str]:
        return getattr(self, "_committees_url", None)

    @property
    def hearings_count(self) -> int:
        return getattr(self, "_hearings_count", None)

    @property
    def hearings_url(self) -> Optional[str]:
        return getattr(self, "_hearings_url", None)

    @property
    def nominees_urls(self) -> List[str]:
        return getattr(self, "_nominees_urls", [])

    @property
    def nomineepositions(self) -> List['NominationPosition']:
        return getattr(self, "_positions", [])

    @property
    def latest_action(self) -> Optional['Action']:
        return getattr(self, "_latest_action", None)

    @property
    def url(self) -> Optional[str]:
        return getattr(self, "_url", None)

    def _construct_endpoint(self, path):
        if self.nomination_number and self.part_number:
            nominationNumber = f"{self.nomination_number}-{self.part_number}"
        if self.congress and self.nomination_number:
            _endpoint = f"/nomination/{self.congress}/{nominationNumber}"
        else:
            raise ValueError(
                "Nomination object needs a congress and nomination number or previously defined endpoint for further functions."
            )
        return f"{_endpoint}/{path}"

    async def get_nominees(self):
        try:
            nominees_urls = list(enumerate(self.nominees_urls))
            async for item in self._get_items("nominees_urls", Nominee, _id_package_init=self._id_package):
                # Get the current index and url
                if nominees_urls:
                    i, _ = nominees_urls.pop(0)  # Get and remove the first tuple
                    if i < len(self.nomineepositions):
                        nominee_position = self.nomineepositions[i]
                        if nominee_position.ordinal != item.ordinal:
                            item.data["ordinal"] = nominee_position.ordinal
                            setattr(item, "ordinal", nominee_position.ordinal)
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving nominees - {e}")

    async def get_actions(self):
        try:    
            async for item in self._get_items("actions_url", Action, _id_package_init=self._id_package):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving actions - {e}")

    async def get_committees(self, verbose=False):
        try:
            async for item in self._get_items("committees_url", Committee, verbose=verbose, _id_package_init=self._id_package):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving committees - {e}")


    async def get_committeeactivities(self):
        def process_committee_activities(items, _id_package_init):
            processed_items = []
    
            committee_name = items.get("name")
            committee_code = items.get("systemCode")
            chamber = items.get("chamber")

            for activity in items.get("activities", []):
                processed_items.append(CommitteeActivity(data={
                    "activity_date": activity.get("date", ''),
                    "activity_name": activity.get("name", ''),
                    "committee": committee_name,
                    "committee_code": committee_code,
                    "chamber": chamber,
                    "subcommittee": None,
                    "subcommittee_code": None
                }, _id_package_init=_id_package_init))

            for subcommittee in items.get("subcommittees", []):
                subcommittee_name = subcommittee.get("name")
                subcommittee_code = subcommittee.get("systemCode")

                for subactivity in subcommittee.get("activities", []):
                    processed_items.append(CommitteeActivity(data={
                        "activity_date": subactivity.get("date", ''),
                        "activity_name": subactivity.get("name", ''),
                        "committee": committee_name,
                        "committee_code": committee_code,
                        "chamber": chamber,
                        "subcommittee": subcommittee_name,
                        "subcommittee_code": subcommittee_code
                    }, _id_package_init=_id_package_init))

            return processed_items

        try:
            async for items in self._get_items("committees_url", process_committee_activities, _id_package_init=self._id_package):
                for item in items:
                    yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving committee activities: {e}")

    async def get_hearings(self):
        try:
            async for item in self._get_items("hearings_url", Hearing, _id_package_init=self._id_package):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving hearings - {e}")

@dataclass
class Hearing(Retriever):
    data: Dict
    _adapter: RestAdapter
    _pagination: Dict
    _id_package_init: Optional[Tuple] = None

    def __init__(self, data: Dict, _pagination: Dict, _adapter: RestAdapter, _id_package_init: Optional[Tuple] = None):
        if "hearing" in data and isinstance(data.get("hearing"), dict):
            self.data = data.get("hearing")
        else:
            self.data = data

        self._pagination = _pagination

        super().__init__(data=self.data, _adapter=_adapter)
        self._id_package_init = _id_package_init

        self._parse_data()
        self._post_init_processing()

    def _post_init_processing(self):
        if self._id_package_init:
            setattr(self, self._id_package_init[0], self._id_package_init[1])

        self._parse_jacket_number()
        self._setup_id_package()
        self._parse_associated_meeting()
        self._parse_committees()
        self._parse_texts()
        self._parse_dates()

    def __str__(self):
        return str(self.hearing_jacketnumber) or "Hearing"

    def __repr__(self):
        return self.__str__()

    def _setup_id_package(self):
        setattr(self, "_id_package", ("jacketnumber", getattr(self, "jacketnumber", getattr(self, "_jacket_number", "-99"))))

    def _parse_jacket_number(self):
        jacket_number = getattr(self, "_jacket_number", None)
        congress = getattr(self, "_congress", None)
        if jacket_number and len(str(jacket_number)) > 2:
            self._jacketnumber = f"{str(jacket_number)[:2]}-{str(jacket_number)[2:]}-{congress}"
        elif jacket_number and len(str(jacket_number)) <= 2:
            self._jacketnumber = f"{str(jacket_number)}-{congress}"
        else:
            self._jacketnumber = "-99"

    def _parse_associated_meeting(self):
        associated_meeting = getattr(self, "_associated_meeting", {})
        self._associated_meeting_id = associated_meeting.get("eventId")
        self._associated_meeting_url = associated_meeting.get("url")

    def _parse_committees(self):
        committees = getattr(self, "_committees", [])
        self._committees_urls = [committee.get("url") for committee in committees if isinstance(committee, dict)]
        self._committee_names = [committee.get("name") for committee in committees if isinstance(committee, dict)]
        self._committee_codes = [committee.get("systemCode") for committee in committees if isinstance(committee, dict)]

    def _parse_dates(self):
        dates = getattr(self, "_dates", [])
        self._dates = [date.get("date") for date in dates if isinstance(date, dict)]

    def _parse_texts(self):
        texts_data = getattr(self, "_formats", [])
        self._texts = [TextVersion(data=text, _pagination=None, _adapter=self._adapter, _id_package_init=self._id_package) for text in texts_data if isinstance(text, dict)]

    @property
    def chamber(self) -> Optional[str]:
        return getattr(self, "_chamber", None)

    @property
    def citation(self) -> Optional[str]:
        return getattr(self, "_citation", None)

    @property
    def congress(self) -> Optional[int]:
        return getattr(self, "_congress", None)

    @property
    def date(self) -> Optional[str]:
        return getattr(self, "_date", None)

    @property
    def errata_number(self) -> Optional[str]:
        return getattr(self, "_errata_number", None)

    @property
    def hearing_jacketnumber(self) -> str:
        return self._jacketnumber or "-99"

    @property
    def loc_id(self) -> Optional[str]:
        return getattr(self, "_library_of_congress_identifier", None)

    @property
    def hearing_number(self) -> Optional[str]:
        return getattr(self, "_number", None)

    @property
    def part_number(self) -> Optional[str]:
        return getattr(self, "_part", None)

    @property
    def title(self) -> Optional[str]:
        return getattr(self, "_title", None)

    @property
    def updated_at(self) -> Optional[str]:
        return getattr(self, "_update_date", None)

    @property
    def associated_meeting_id(self) -> Optional[str]:
        return getattr(self, "_associated_meeting_id", None)

    @property
    def associated_meeting_url(self) -> Optional[str]:
        return getattr(self, "_associated_meeting_url", None)

    @property
    def committees_urls(self) -> List[str]:
        return getattr(self, "_committees_urls", [])

    @property
    def committees_names(self) -> List[str]:
        return getattr(self, "_committee_names", [])

    @property
    def committee_codes(self) -> List[str]:
        return getattr(self, "_committee_codes", [])

    @property
    def dates(self) -> List[str]:
        return getattr(self, "_dates", [])

    @property
    def formats(self) -> List['TextVersion']:
        return getattr(self, "_texts", [])

    @property
    def url(self) -> Optional[str]:
        return getattr(self, "_url", None)

    def _construct_endpoint(self, path):
        if self.congress and self.chamber and self.hearing_jacketnumber.replace("-", "").isdigit():
            _endpoint = (
                f"/nomination/{self.congress}/{self.chamber}/{self.hearing_jacketnumber}"
            )
        else:
            raise ValueError(
                "Hearing object needs a congress, chamber, and jacket number or previously defined endpoint for further functions."
            )
        return f"{_endpoint}/{path}"

    async def get_associated_meeting(self):
        try:
            async for item in self._get_items("associated_meeting_url", CommitteeMeeting, _id_package_init=self._id_package):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving associated meeting: {e}")

    async def get_committees(self):
        try:
            async for item in self._get_items("committees_urls", Committee, _id_package_init=self._id_package):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving committees: {e}")

@dataclass
class CommitteeMeeting(Retriever):
    data: Dict
    _adapter: RestAdapter
    _pagination: Dict
    _id_package_init: Optional[Tuple] = None

    def __init__(self, data: Dict, _pagination: Dict, _adapter: RestAdapter, _id_package_init: Optional[Tuple] = None):
        if "committeeMeeting" in data and isinstance(data.get("committeeMeeting"), dict):
            self.data = data.get("committeeMeeting")
        else:
            self.data = data

        self._pagination = _pagination

        super().__init__(data=self.data, _adapter=_adapter)
        self._id_package_init = _id_package_init

        self._parse_data()
        self._post_init_processing()

    def _post_init_processing(self):
        if self._id_package_init:
            setattr(self, self._id_package_init[0], self._id_package_init[1])

        self._setup_id_package()
        self._parse_location()
        self._parse_meeting_documents()
        self._parse_related_items()
        self._parse_videos()
        self._parse_committees()
        self._parse_hearing_transcripts()
        self._parse_witness_documents()
        self._parse_witnesses()

    def __str__(self):
        return (
            f"{self.meeting_id} {self.title}"
            if all([self.meeting_id, self.title])
            else (self.title or self.meeting_id or "Committee Meeting")
        )

    def __repr__(self):
        return self.__str__()

    def _setup_id_package(self):
        setattr(self, "_id_package", ("meeting_id", getattr(self, "_event_id", "-99")))

    def _parse_committees(self):
        committees = getattr(self, "_committees", [])
        self._committees_urls = [committee.get("url") for committee in committees if isinstance(committee, dict)]
        self._committee_names = [committee.get("name") for committee in committees if isinstance(committee, dict)]
        self._committee_codes = [committee.get("systemCode") for committee in committees if isinstance(committee, dict)]

    def _parse_hearing_transcripts(self):
        hearing_transcripts = getattr(self, "_hearing_transcript", [])
        self._hearing_transcripts_urls = [hearing_transcript.get("url") for hearing_transcript in hearing_transcripts if isinstance(hearing_transcript, dict)]
        self._hearing_transcripts_list = [
            f"{(str(hearing_transcript.get('jacketNumber'))[:2])}-{str(hearing_transcript.get('jacketNumber'))[2:] if len(str(hearing_transcript.get('jacketNumber'))) > 2 else hearing_transcript.get('jacketNumber')}"
            for hearing_transcript in hearing_transcripts
            if isinstance(hearing_transcript, dict)
        ]

    def _parse_location(self):
        location = getattr(self, "_location", {})
        self._building = location.get("building")
        self._room = location.get("room")

        if isinstance(location.get("address", {}), str):
            address = json.loads(location.get("address", {}))
            self._address = address
        else:
            self._address = location.get("address", {})

    def _parse_meeting_documents(self):
        meetingDocuments = getattr(self, "_meeting_documents", [])
        self._meeting_documents = [TextVersion(data=meetingdoc, _pagination=None, _adapter=self._adapter, _id_package_init=self._id_package) for meetingdoc in meetingDocuments if isinstance(meetingdoc, dict) and meetingdoc.get("documentType") != "Bills and Resolutions"]

    def _parse_related_items(self):
        relatedItems = getattr(self, "_related_items", {})

        for item_type in ["bills", "treaties", "nominations"]:
            items = relatedItems.get(item_type, [])
            if item_type == "bills":
                setattr(self, "_associated_bill_ids", [
                    f"{str(bill.get('type')).lower()}{bill.get('number')}-{bill.get('congress')}"
                    for bill in items
                    if all([bill.get("type"), bill.get("number"), bill.get("congress")])
                    and isinstance(bill, dict)
                ])
                setattr(self, "_associated_bill_urls", [
                    bill.get("url") for bill in items if isinstance(bill, dict)
                ])
            elif item_type == "treaties":
                setattr(self, "_associated_treaty_ids", [
                    f"td{treaty.get('congress')}-{treaty.get('number')}{treaty.get('part', '')}"
                    for treaty in items
                    if treaty.get("number") and treaty.get("congress")
                    and isinstance(treaty, dict)
                ])
                setattr(self, "_associated_treaty_urls", [
                    treaty.get("url") for treaty in items if isinstance(treaty, dict)
                ])
            elif item_type == "nominations":
                setattr(self, "_associated_nomination_ids", [
                    f"PN{nomination.get('number')}-{nomination.get('part')}-{nomination.get('congress')}"
                    for nomination in items
                    if all([nomination.get("number"), nomination.get("part"), nomination.get("congress")])
                    and isinstance(nomination, dict)
                ])
                setattr(self, "_associated_nomination_urls", [
                    nomination.get("url") for nomination in items if isinstance(nomination, dict)
                ])

    def _parse_videos(self):
        self._videos = getattr(self, "_videos", [])
        self._video_names = [video.get("name") for video in self._videos if isinstance(video, dict)]

    def _parse_witness_documents(self):
        witness_documents = getattr(self, "_witness_documents", [])
        self._witness_documents = [TextVersion(data=witnessDocument, _pagination=None,_adapter=self._adapter, _id_package_init=self._id_package) for witnessDocument in witness_documents if isinstance(witnessDocument, dict)]

    def _parse_witnesses(self):
        witnesses = getattr(self, "_witnesses", [])
        self._witnesses = [Witness(data=witness, _id_package_init=self._id_package) for witness in witnesses if isinstance(witness, dict)]

    @property
    def meeting_id(self) -> str:
        return getattr(self, "_event_id", "-99")

    @property
    def chamber(self) -> str:
        return getattr(self, "_chamber", None)

    @property
    def congress(self) -> Optional[int]:
        return getattr(self, "_congress", None)

    @property
    def date(self) -> Optional[str]:
        return getattr(self, "_date", None)

    @property
    def meeting_status(self) -> Optional[str]:
        return getattr(self, "_meeting_status", None)

    @property
    def title(self) -> str:
        return getattr(self, "_title", "").replace("  ", "").replace("\n", "").replace("\t", "")

    @property
    def meeting_type(self) -> Optional[str]:
        return getattr(self, "_type", None)

    @property
    def updated_at(self) -> Optional[str]:
        return getattr(self, "_update_date", None)

    @property
    def committees_urls(self) -> List[str]:
        return getattr(self, "_committees_urls", [])

    @property
    def committees_names(self) -> List[str]:
        return getattr(self, "_committee_names", [])

    @property
    def committee_codes(self) -> List[str]:
        return getattr(self, "_committee_codes", [])

    @property
    def hearing_jacketnumber_urls(self) -> List[str]:
        return getattr(self, "_hearing_transcripts_urls", [])

    @property
    def hearing_jacketnumbers(self) -> List[str]:
        return getattr(self, "_hearing_transcripts_list", [])

    @property
    def building(self) -> Optional[str]:
        return getattr(self, "_building", None)

    @property
    def room(self) -> Optional[str]:
        return getattr(self, "_room", None)
    
    @property
    def address(self) -> Optional[str]:
        return getattr(self, "_address", None)

    @property
    def meeting_documents(self) -> List['TextVersion']:
        return getattr(self, "_meeting_documents", [])

    @property
    def associated_bill_ids(self) -> List[str]:
        return getattr(self, "_associated_bill_ids", [])

    @property
    def associated_bill_urls(self) -> List[str]:
        return getattr(self, "_associated_bill_urls", [])

    @property
    def associated_treaty_ids(self) -> List[str]:
        return getattr(self, "_associated_treaty_ids", [])

    @property
    def associated_treaty_urls(self) -> List[str]:
        return getattr(self, "_associated_treaty_urls", [])

    @property
    def associated_nomination_ids(self) -> List[str]:
        return getattr(self, "_associated_nomination_ids", [])

    @property
    def associated_nomination_urls(self) -> List[str]:
        return getattr(self, "_associated_nomination_urls", [])

    @property
    def videos_urls(self) -> List[str]:
        return getattr(self, "_videos_urls", [])

    @property
    def video_names(self) -> List[str]:
        return getattr(self, "_video_names", [])

    @property
    def witness_documents(self) -> List['TextVersion']:
        return getattr(self, "_witness_documents", [])

    @property
    def witnesses(self) -> List[Witness]:
        return getattr(self, "_witnesses", [])

    @property
    def url(self) -> Optional[str]:
        return getattr(self, "_url", None)

    def _construct_endpoint(self, path):
        if self.congress and self.chamber and self.meeting_id:
            _endpoint = (
                f"/committee-meeting/{self.congress}/{self.chamber}/{self.meeting_id}"
            )
        else:
            raise ValueError(
                "Committee meeting object needs a congress, chamber, and event ID or previously defined endpoint for further functions."
            )
        return f"{_endpoint}/{path}"

    async def get_committees(self):
        try:
            async for item in self._get_items("committees_urls", Committee, _id_package_init=self._id_package):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving committees: {e}")

    async def get_related_bills(self):
        try:
            async for item in self._get_items("associated_bills", Bill, _id_package_init=self._id_package):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving related bills: {e}")

    async def get_related_treaties(self):
        try:
            async for item in self._get_items("associated_treaties", Treaty, _id_package_init=self._id_package):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving related treaties: {e}")

    async def get_related_nominations(self):
        try:
            async for item in self._get_items("associated_nominations", Nomination, _id_package_init=self._id_package):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving related nominations: {e}")

    async def get_hearings(self):
        try:
            async for item in self._get_items("hearing_transcripts_urls", Hearing, _id_package_init=self._id_package):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving related hearings: {e}")

@dataclass
class CommitteePrint(Retriever):
    data: Dict
    _adapter: RestAdapter
    _pagination: Dict
    _id_package_init: Optional[Tuple] = None

    def __init__(self, data: Dict, _pagination: Dict, _adapter: RestAdapter, _id_package_init: Optional[Tuple] = None):
        if "committeePrint" in data and isinstance(data.get("committeePrint"), list):
            self.data = data.get("committeePrint")[0]
        else:
            self.data = data

        self._pagination = _pagination

        super().__init__(data=self.data, _adapter=_adapter)
        self._id_package_init = _id_package_init

        self._parse_data()
        self._post_init_processing()

    def _post_init_processing(self):
        if self._id_package_init:
            setattr(self, self._id_package_init[0], self._id_package_init[1])

        self._parse_jacket_number()
        self._parse_print_id()
        self._setup_id_package()
        self._parse_associated_bills()
        self._parse_committees()
        self._parse_texts()

    def __str__(self):
        return str(self.print_id) or "Committee Print"

    def __repr__(self):
        return self.__str__()

    def _parse_jacket_number(self):
        jacket_number = getattr(self, "_jacket_number", None)
        self._jacket_number = f"{str(jacket_number)[:2]}-{str(jacket_number)[2:]}" or None

    def _parse_print_id(self):
        self._congress = getattr(self, "_congress", None)
        self._chamber = getattr(self, "_chamber", None)

        if self._congress and self._chamber and self._jacket_number:
            self._print_id = f"{str(self._chamber[0]).lower()}prt{str(self._jacket_number).replace('-', '')}-{str(self._congress)}"
        else:
            self._print_id = "-99"

    def _setup_id_package(self):
        setattr(self, "_id_package", ("print_id", getattr(self, "print_id", getattr(self, "_print_id", "-99"))))

    def _parse_associated_bills(self):
        associated_bills = getattr(self, "_associated_bills", [])
        self._associated_bills_urls = [bill.get("url") for bill in associated_bills if isinstance(bill, dict)]
        self._associated_bill_ids = [
            f"{str(bill.get('type')).lower()}{bill.get('number')}-{bill.get('congress')}"
            for bill in associated_bills
            if all([bill.get("type"), bill.get("number"), bill.get("congress")])
            and isinstance(bill, dict)
        ]

    def _parse_committees(self):
        committees = getattr(self, "_committees", [])
        self._committees_urls = [committee.get("url") for committee in committees if isinstance(committee, dict)]
        self._committee_names = [committee.get("name") for committee in committees if isinstance(committee, dict)]
        self._committee_codes = [committee.get("systemCode") for committee in committees if isinstance(committee, dict)]

    def _parse_texts(self):
        texts = getattr(self, "_text", {})
        self._texts_url = texts.get("url")
        self._texts_count = texts.get("count", 0)

    @property
    def chamber(self) -> Optional[str]:
        return getattr(self, "_chamber", None)

    @property
    def citation(self) -> Optional[str]:
        return getattr(self, "_citation", None)

    @property
    def congress(self) -> Optional[int]:
        return getattr(self, "_congress", None)

    @property
    def print_jacketnumber(self) -> str:
        return self._jacket_number or None

    @property
    def print_number(self) -> Optional[str]:
        return getattr(self, "_number", None)

    @property
    def updated_at(self) -> Optional[str]:
        return getattr(self, "_update_date", None)

    @property
    def title(self) -> Optional[str]:
        return getattr(self, "_title", None)

    @property
    def print_id(self) -> str:
        return self._print_id or "-99"

    @property
    def associated_bills_urls(self) -> List[str]:
        return getattr(self, "_associated_bills_urls", [])

    @property
    def associated_bill_ids(self) -> List[str]:
        return getattr(self, "_associated_bill_ids", [])

    @property
    def committees_urls(self) -> List[str]:
        return getattr(self, "_committees_urls", [])

    @property
    def committee_names(self) -> List[str]:
        return getattr(self, "_committee_names", [])

    @property
    def committee_codes(self) -> List[str]:
        return getattr(self, "_committee_codes", [])

    @property
    def texts_url(self) -> List[str]:
        return getattr(self, "_texts_url", [])

    @property
    def texts_count(self) -> List[str]:
        return getattr(self, "_texts_count", None)

    @property
    def url(self) -> Optional[str]:
        return getattr(self, "_url", None)

    def _construct_endpoint(self, path):
        if self.congress and self.chamber and self.print_jacketnumber.replace("-", "").isdigit():
            _endpoint = (
                f"/committee-print/{self.congress}/{self.chamber}/{self.print_jacketnumber}"
            )
        else:
            raise ValueError(
                "CommitteePrint object needs a congress, chamber, and jacket number or previously defined endpoint for further functions."
            )
        return f"{_endpoint}/{path}"

    async def get_associated_bills(self):
        try:
            async for item in self._get_items("associated_bills_urls", Bill, _id_package_init=self._id_package):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving associated bills: {e}")

    async def get_committees(self):
        try:
            async for item in self._get_items("committees_urls", Committee, _id_package_init=self._id_package):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving committees: {e}")

    async def get_texts(self):
        try:
            async for item in self._get_items("texts_url", TextVersion, _id_package_init=self._id_package):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving texts: {e}")

@dataclass
class Congress(Retriever):
    data: Dict
    _adapter: RestAdapter
    _pagination: Dict
    _id_package_init: Optional[Tuple] = None

    def __init__(self, data: Dict, _pagination: Dict, _adapter: RestAdapter, _id_package_init: Optional[Tuple] = None):
        if "congress" in data and isinstance(data.get("congress"), dict):
            self.data = data.get("congress")
        else:
            self.data = data

        self._pagination = _pagination

        super().__init__(data=self.data, _adapter=_adapter)
        self._id_package_init = _id_package_init

        self._parse_data()
        self._post_init_processing()

    def _post_init_processing(self):
        if self._id_package_init:
            setattr(self, self._id_package_init[0], self._id_package_init[1])

        self._setup_id_package()
        self._parse_name()
        self._parse_sessions()

    def __str__(self):
        return str(self.number) or "Congress"

    def __repr__(self):
        return self.__str__()

    def _setup_id_package(self):
        setattr(self, "_id_package", ("congress_no", getattr(self, "number", getattr(self, "_number", "-99"))))

    def _parse_sessions(self):
        sessions_data = getattr(self, "_sessions", [])
        self._sessions = [
            Session(data=session, _id_package_init=self._id_package)
            for session in sessions_data
            if isinstance(session, dict)
        ]

    def _parse_name(self):
        self._name = getattr(self, "_name", None)
        self._number = getattr(self, "_number", None)
        if not self._name and self._number:
            self._name = f"{self._number}th Congress"
        elif self._name and not self._number:
            self._number = int(self._name.split("th Congress")[0].split("rd Congress")[0].split("st Congress")[0].split("nd Congress")[0])
        elif not self._name and not self._number:
            self._name = "Unknown Congress"
            self._number = -99


    @property
    def sessions(self) -> List[Session]:
        return self._sessions or []

    @property
    def name(self) -> str:
        return self._name or "-99"

    @property
    def number(self) -> int:
        return self._number or None

    @property
    def start_year(self) -> Optional[int]:
        return getattr(self, "_start_year", None)

    @property
    def end_year(self) -> Optional[int]:
        return getattr(self, "_end_year", None)

    @property
    def updated_at(self) -> Optional[str]:
        return getattr(self, "_update_date", None)

    @property
    def url(self) -> Optional[str]:
        return getattr(self, "_url", None)

@dataclass
class TextVersion(Retriever):
    data: Dict
    _adapter: RestAdapter
    _pagination: Dict
    _id_package_init: Optional[Tuple] = None

    def __init__(self, data: Dict, _pagination: Dict, _adapter: RestAdapter, _id_package_init: Optional[Tuple] = None):
        if "textVersions" in data and isinstance(data.get("textVersions"), dict):
            self.data = data.get("textVersions")
        elif "text" in data and isinstance(data.get("text"), dict):
            self.data = data.get("text")
        else:
            self.data = data

        self._pagination = _pagination

        super().__init__(data=self.data, _adapter=_adapter)
        self._id_package_init = _id_package_init

        self._parse_data()
        self._post_init_processing()

    def _post_init_processing(self):
        if self._id_package_init:
            setattr(self, self._id_package_init[0], self._id_package_init[1])

        self.parse_data()

    def parse_data(self):
        if "textVersions" in self.data and isinstance(self.data, dict):
            self.parse_text_versions()
        elif "text" in self.data:
            for text in self.data:
                self.parse_format(text)
        elif isinstance(self.data, dict):
            self.parse_single_item()

    def parse_text_versions(self):
        text_versions = getattr(self, "_text_versions", {})
        if not text_versions:
            text = getattr(self, "_text", {})
            if "formats" in text:
                for format in text.get("formats"):
                    self.parse_format(format)
            else:
                self.parse_item(text)
        if isinstance(text_versions, dict):
            self.parse_item(text_versions)
        elif isinstance(text_versions, list) and text_versions:
            self.parse_item(text_versions[0])

    def parse_single_item(self):
        self.parse_item(self.data)

    def parse_item(self, item: Dict):
        self._type = item.get("type")
        self._date = item.get("date")
        self._url = item.get("url")

        if "name" in item:
            self._name = item.get("name")
            self._description = item.get("description")

        if "documentType" in item:
            self._document_type = item.get("documentType")

        formats = item.get("formats", [])
        if isinstance(formats, dict):
            formats = [formats]

        for format_item in formats:
            self.parse_format(format_item)

    def parse_format(self, format_item: Dict):
        format_type = format_item.get("type") or format_item.get("format")
        url = format_item.get("url")
        is_errata = self.parse_errata(format_item.get("isErrata"))

        if format_type == "Formatted Text":
            self._formatted_text = url
            self._formatted_text_is_errata = is_errata
        elif format_type == "Formatted XML":
            self._xml = url
            self._xml_is_errata = is_errata
        elif format_type == "PDF":
            self._pdf = url
            self._pdf_is_errata = is_errata
        elif format_type == "Generated HTML":
            self._html = url
            self._html_is_errata = is_errata
        elif format_type is not None:
            setattr(self, f"_{format_type.lower().replace(' ', '_')}", url)
            setattr(
                self, f"_{format_type.lower().replace(' ', '_')}_is_errata", is_errata
            )

    @staticmethod
    def parse_errata(errata_value: Optional[str]) -> Optional[bool]:
        if errata_value == "Y":
            return True
        elif errata_value == "N":
            return False
        return None

    def __str__(self):
        attributes = [
            f"{key}={value}"
            for key, value in self.__dict__.items()
            if not key.startswith("_") and key != "data"
        ]
        return f"TextVersion({', '.join(attributes)})"

    def __repr__(self):
        return self.__str__()

    @property
    def type(self) -> Optional[str]:
        return getattr(self, "_type", None)

    @property
    def date(self) -> Optional[str]:
        return getattr(self, "_date", None)

    @property
    def url(self) -> Optional[str]:
        return getattr(self, "_url", None)

    @property
    def name(self) -> Optional[str]:
        return getattr(self, "_name", None)

    @property
    def description(self) -> Optional[str]:
        return getattr(self, "_description", None)

    @property
    def document_type(self) -> Optional[str]:
        return getattr(self, "_document_type", None)

    @property
    def formatted_text(self) -> Optional[str]:
        return getattr(self, "_formatted_text", None)

    @property
    def formatted_text_is_errata(self) -> Optional[bool]:
        return getattr(self, "_formatted_text_is_errata", None)

    @property
    def xml(self) -> Optional[str]:
        return getattr(self, "_xml", None)

    @property
    def xml_is_errata(self) -> Optional[bool]:
        return getattr(self, "_xml_is_errata", None)

    @property
    def pdf(self) -> Optional[str]:
        return getattr(self, "_pdf", None)

    @property
    def pdf_is_errata(self) -> Optional[bool]:
        return getattr(self, "_pdf_is_errata", None)

    @property
    def html(self) -> Optional[str]:
        return getattr(self, "_html", None)

    @property
    def html_is_errata(self) -> Optional[bool]:
        return getattr(self, "_html_is_errata", None)

    async def read(self) -> Optional[str]:
        """
        Retrieve the actual text content of the document.
        Prioritizes formatted text, then HTML, then XML.
        Returns None if no readable format is available.
        """
        url = self.formatted_text or self.html or self.xml
        if not url:
            return None

        response = await self._adapter.retrieve(url, override=True)
        return response.text()
