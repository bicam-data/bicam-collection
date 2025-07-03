import datetime
import hashlib
import json
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Optional

from pycon.adapter import RestAdapter
from pycon.exceptions import PyCongressException
from pycon.retriever_class import Retriever
from pycon.utilis import process_report_id

from .subcomponents import (
    CBOCostEstimate,
    CommitteeActivity,
    CommitteeHistory,
    Law,
    LeadershipRole,
    MemberTerm,
    NominationPosition,
    Nominee,
    Note,
    NoteLink,
    PartyHistory,
    RecordedVotes,
    RelatedBill,
    Session,
    Subject,
    Summary,
    Title,
    Witness,
)


@dataclass
class Action(Retriever):
    data: dict
    _adapter: RestAdapter
    _pagination: dict
    _id_package_init: tuple | None = None

    def __init__(
        self,
        data: dict,
        _pagination: dict,
        _adapter: RestAdapter,
        _id_package_init: tuple | None = None,
    ):
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
        return setattr(
            self,
            "_id_package",
            (
                "action_id",
                getattr(self, "action_id", getattr(self, "_action_id", None)),
            ),
        )

    def _parse_committees(self):
        self._committees = getattr(self, "_committees", [])
        if not self._committees:
            self._committees = getattr(self, "_committee", [])
            if not isinstance(self._committees, list):
                self._committees = [self._committees]

    def _parse_recorded_votes(self):
        self._recorded_votes_data = getattr(self, "_recorded_votes", {})
        self._recorded_votes = [
            RecordedVotes(
                data=vote,
                _id_package_init=self._id_package_init,
                action_id=self.action_id,
            )
            for vote in self._recorded_votes_data
            if isinstance(vote, dict)
        ]

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
        unique_string = (
            f"{related_id}_{action_date}_{action_text}_{action_code}_{timestamp}"
        )
        try:
            self._action_id = hashlib.md5(unique_string.encode()).hexdigest()
        except Exception:
            self._id_error = True
            self._action_id = None

    def _construct_endpoint(self, path: str):  # override abstract
        """Action objects do not expose related sub-resources.
        This method is implemented only to satisfy the abstract base-class
        requirement and will raise if called.
        """
        raise PyCongressException("Action has no child endpoints")

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
    def action_time(self) -> str | None:
        return getattr(self, "_action_time", None)

    @property
    def text(self) -> str:
        return getattr(self, "_text", "NO TEXT")

    @property
    def action_type(self) -> str | None:
        return getattr(self, "_type", None)

    @property
    def links(self) -> list[dict] | None:
        return getattr(self, "_links", [])

    @property
    def committees_urls(self) -> list[str]:
        return [
            committee.get("url")
            for committee in self._committees
            if isinstance(committee, dict)
        ]

    @property
    def committee_names(self) -> list[str]:
        return [
            committee.get("name")
            for committee in self._committees
            if isinstance(committee, dict)
        ]

    @property
    def committee_codes(self) -> list[str]:
        return [
            committee.get("systemCode")
            for committee in self._committees
            if isinstance(committee, dict)
        ]

    @property
    def recorded_votes(self) -> list["RecordedVotes"]:
        return getattr(self, "_recorded_votes", [])

    @property
    def source_system(self) -> str | None:
        return getattr(self, "_source_system", None)

    @property
    def source_system_code(self) -> str | None:
        return getattr(self, "_source_system_code", None)

    @property
    def calendar_number(self) -> str | None:
        return getattr(self, "_calendar_number", None)

    @property
    def calendar(self) -> str | None:
        return getattr(self, "_calendar", None)

    @property
    def url(self) -> str | None:
        return getattr(self, "_url", None)

    def _ensure_adapter(self):
        if not self._adapter:
            raise PyCongressException(
                "Object must be initialized with a RestAdapter object."
            )

    async def get_committees(self) -> AsyncIterator["Committee"]:
        self._ensure_adapter()
        try:
            if not self._committees:
                yield None

            async for items in self._get_items(
                "committees_urls", Committee, _id_package_init=self._id_package
            ):
                for committee in items:
                    yield committee
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving committees: {e}")


@dataclass
class Bill(Retriever):
    data: dict
    _adapter: RestAdapter
    _pagination: dict
    _id_package_init: tuple | None = None

    def __init__(
        self,
        data: dict,
        _pagination: dict,
        _adapter: RestAdapter,
        _id_package_init: tuple | None = None,
    ):
        if "bill" in data and isinstance(data.get("bill"), dict):
            self.data = data.get("bill")
        elif "committee-bills" in data and isinstance(
            data.get("committee-bills"), dict
        ):
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
            if self._id_package_init[0] == "bill_id":
                self._original_bill_id = self._id_package_init[1]
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
        self._parse_notes()

    def _parse_bill_id(self):
        bill_number = getattr(self, "_number", None)
        if "½" in bill_number:
            bill_number = bill_number.replace("½", ".5")  # replace half with .5
        congress = getattr(self, "_congress", None)
        bill_type = getattr(self, "_type", None).lower()

        if None in [bill_number, congress, bill_type]:
            self._id_error = True
            self._bill_id = None
        else:
            self._bill_id = f"{bill_type}{bill_number}-{congress}"

    def _parse_actions(self):
        actions = getattr(self, "_actions", {})
        self._actions_url = actions.get("url", None)
        self._actions_count = actions.get("count", 0)

    def _parse_amendments(self):
        amendments = getattr(self, "_amendments", {})
        self._amendments_url = amendments.get("url", None)
        self._amendments_count = amendments.get("count", 0)

    def _parse_cbo_cost_estimates(self):
        cbo_cost_estimates = getattr(self, "_cbo_cost_estimates", [])
        self._cbo_cost_estimates = [
            CBOCostEstimate(data=cbo, _id_package_init=self._id_package)
            for cbo in cbo_cost_estimates
            if isinstance(cbo, dict)
        ]

    def _parse_committees(self):
        committees = getattr(self, "_committees", {})
        self._committees_url = committees.get("url", None)
        self._committees_count = committees.get("count", 0)

    def _parse_cosponsors(self):
        cosponsors = getattr(self, "_cosponsors", {})
        self._cosponsors_url = cosponsors.get("url", None)
        self._cosponsors_count = cosponsors.get("count", 0)

    def _parse_latest_action(self):
        latest_action = getattr(self, "_latest_action", {})
        self._latest_action = Action(
            data=latest_action,
            _pagination=None,
            _adapter=self._adapter,
            _id_package_init=self._id_package,
        )

    def _parse_notes(self):
        """Parse notes and their embedded links.

        Each bill note is a dict with keys like ``text`` and optional ``links``.
        * ``_notes`` becomes a list[Note]
        * ``_notes_links`` becomes a flat list[NoteLink] – one per link dict.
        """
        raw_notes = getattr(self, "_notes", [])

        # Normalise to list
        if isinstance(raw_notes, dict):
            raw_notes = [raw_notes]

        self._notes = [
            Note(data=note_dict, _id_package_init=self._id_package)
            for note_dict in raw_notes
            if isinstance(note_dict, dict)
        ]

        links: list[NoteLink] = []
        for note_obj in self._notes:
            raw_links = note_obj.data.get("links", [])
            if isinstance(raw_links, dict):
                raw_links = [raw_links]
            for link_dict in raw_links:
                if isinstance(link_dict, dict):
                    links.append(
                        NoteLink(
                            note_id=note_obj.note_id,
                            data=link_dict,
                            _id_package_init=self._id_package,
                        )
                    )

        self._notes_links = links

    def _parse_policy_area(self):
        policy_area = getattr(self, "_policy_area", {})
        self._policy_area = policy_area.get("name")

    def _parse_related_bills(self):
        related_bills = getattr(self, "_related_bills", {})
        self._related_bills_url = related_bills.get("url", None)
        self._related_bills_count = related_bills.get("count", 0)

    def _parse_sponsors(self):
        sponsors = getattr(self, "_sponsors", [])
        self._sponsors_urls = [
            sponsor.get("url") for sponsor in sponsors if isinstance(sponsor, dict)
        ]
        self._sponsors_bioguide_ids = [
            sponsor.get("bioguideId")
            for sponsor in sponsors
            if isinstance(sponsor, dict)
        ]
        self._sponsors_names = [
            sponsor.get("fullName") for sponsor in sponsors if isinstance(sponsor, dict)
        ]

    def _parse_laws(self):
        laws = getattr(self, "_laws", [])
        if laws:
            self._laws = [
                Law(data=law, _id_package_init=self._id_package)
                for law in laws
                if isinstance(law, dict)
            ]
            self._is_law = True
        else:
            self._is_law = False

    def _parse_subjects(self):
        subjects = getattr(self, "_subjects", {})
        self._subjects_url = subjects.get("url", None)
        self._subjects_count = subjects.get("count", 0)

    def _parse_summaries(self):
        summaries = getattr(self, "_summaries", {})
        self._summaries_url = summaries.get("url", None)
        self._summaries_count = summaries.get("count", 0)

    def _parse_titles(self):
        titles = getattr(self, "_titles", {})
        self._titles_url = titles.get("url", None)
        self._titles_count = titles.get("count", 0)

    def _parse_texts(self):
        texts = getattr(self, "_text_versions", {})
        self._texts_url = texts.get("url", None)
        self._texts_count = texts.get("count", 0)

    def _parse_committee_reports(self):
        committee_reports = getattr(self, "_committee_reports", {})
        self._committee_reports_urls = [
            report.get("url")
            for report in committee_reports
            if isinstance(report, dict)
        ]
        self._committeereports = [
            process_report_id(report.get("citation"))
            for report in committee_reports
            if isinstance(report, dict)
        ]

    def _setup_id_package(self):
        return setattr(
            self,
            "_id_package",
            ("bill_id", getattr(self, "bill_id", getattr(self, "_bill_id", "-99"))),
        )

    @property
    def bill_id(self) -> str | None:
        return getattr(self, "_bill_id", "-99")

    @property
    def bill_number(self) -> str | None:
        return getattr(self, "_number", None)

    @property
    def bill_type(self) -> str | None:
        return getattr(self, "_bill_type", None)

    @property
    def congress(self) -> int | None:
        return getattr(self, "_congress", None)

    @property
    def introduced_at(self) -> str | None:
        return getattr(self, "_introduced_date", None)

    @property
    def origin_chamber(self) -> str | None:
        return (
            getattr(self, "_origin_chamber", None).lower()
            if getattr(self, "_origin_chamber", None)
            else None
        )

    @property
    def origin_chamber_code(self) -> str | None:
        return getattr(self, "_origin_chamber_code", None)

    @property
    def title(self) -> str | None:
        return getattr(self, "_title", None)

    @property
    def updated_at(self) -> str | None:
        return getattr(self, "_update_date", None)

    @property
    def update_date_including_text(self) -> str | None:
        return getattr(self, "_update_date_including_text", None)

    @property
    def actions_count(self) -> int:
        return getattr(self, "_actions_count", None)

    @property
    def actions_url(self) -> str | None:
        return getattr(self, "_actions_url", None)

    @property
    def amendments_count(self) -> int:
        return getattr(self, "_amendments_count", None)

    @property
    def amendments_url(self) -> str | None:
        return getattr(self, "_amendments_url", None)

    @property
    def cbocostestimates(self) -> list[CBOCostEstimate]:
        return getattr(self, "_cbo_cost_estimates", [])

    @property
    def committees_count(self) -> int:
        return getattr(self, "_committees_count", None)

    @property
    def committees_url(self) -> str | None:
        return getattr(self, "_committees_url", None)

    @property
    def constitutional_authority_statement_text(self) -> str | None:
        return getattr(self, "_constitutional_authority_statement_text", None)

    @property
    def cosponsors_count(self) -> int:
        return getattr(self, "_cosponsors_count", None)

    @property
    def cosponsors_url(self) -> str | None:
        return getattr(self, "_cosponsors_url", None)

    @property
    def is_law(self) -> bool | None:
        return getattr(self, "_is_law", None)

    @property
    def law_number(self) -> str | None:
        return getattr(self, "_law_number", None)

    @property
    def law_type(self) -> str | None:
        return getattr(self, "_law_type", None)

    @property
    def laws(self) -> list[Law]:
        return getattr(self, "_laws", [])

    @property
    def latest_action(self) -> Action | None:
        return getattr(self, "_latest_action", None)

    @property
    def notes(self) -> str | None:
        return getattr(self, "_notes", None)

    @property
    def policy_area(self) -> str | None:
        return getattr(self, "_policy_area", {}).get("name")

    @property
    def related_bills_count(self) -> int:
        return getattr(self, "_related_bills_count", None)

    @property
    def related_bills_url(self) -> str | None:
        return getattr(self, "_related_bills_url", None)

    @property
    def sponsors(self) -> list[str]:
        return getattr(self, "_sponsors_bioguide_ids", [])

    @property
    def sponsors_names(self) -> list[str]:
        return getattr(self, "_sponsors_names", [])

    @property
    def sponsors_urls(self) -> list[str]:
        return getattr(self, "_sponsors_urls", [])

    @property
    def subjects_count(self) -> int:
        return getattr(self, "_subjects_count", None)

    @property
    def subjects_url(self) -> str | None:
        return getattr(self, "_subjects_url", None)

    @property
    def summaries_count(self) -> int:
        return getattr(self, "_summaries_count", None)

    @property
    def summaries_url(self) -> str | None:
        return getattr(self, "_summaries_url", None)

    @property
    def titles_count(self) -> int:
        return getattr(self, "_titles_count", None)

    @property
    def titles_url(self) -> str | None:
        return getattr(self, "_titles_url", None)

    @property
    def texts_count(self) -> int:
        return getattr(self, "_texts_count", None)

    @property
    def texts_url(self) -> str | None:
        return getattr(self, "_texts_url", None)

    @property
    def committee_reports_urls(self) -> list[str]:
        return getattr(self, "_committee_reports_urls", [])

    @property
    def committee_action_date(self) -> str | None:
        return getattr(self, "_action_date", None)

    @property
    def url(self) -> str | None:
        return getattr(self, "_url", None)

    @property
    def _construct_endpoint(self) -> str:
        if self.congress and self.bill_type and self.bill_number:
            return f"/bill/{self.congress}/{self.bill_type.lower()}/{self.bill_number}"
        else:
            raise ValueError(
                "Object needs a congress, type, and bill number or previously defined endpoint."
            )

    async def get_committeeactivities(self):
        try:
            # iterate raw committee payloads
            async for cm in self._get_items("committees_url"):
                # top-level committee first …
                for source in [cm, *cm.get("subcommittees", [])]:
                    base = {
                        "name": source.get("name"),
                        "systemCode": source.get("systemCode"),
                        "chamber": source.get("chamber").lower()
                        if source.get("chamber")
                        else None,
                        "type": source.get("type"),
                    }

                    if source is not cm:  # it's a subcommittee
                        base["type"] = "Subcommittee"

                    for act in source.get("activities", []):
                        yield CommitteeActivity(
                            data={
                                **base,
                                "activity_name": act.get("name"),
                                "activity_date": act.get("date"),
                            },
                            _id_package_init=self._id_package,
                        )
        except PyCongressException as e:
            self._adapter._logger.error("Error retrieving committee activities: %s", e)

    async def get_actions(self):
        try:
            async for item in self._get_items(
                "actions_url", Action, _id_package_init=self._id_package
            ):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving actions: {e}")

    async def get_amendments(self, verbose=False):
        try:
            async for item in self._get_items(
                "amendments_url",
                Amendment,
                verbose=verbose,
                _id_package_init=self._id_package,
            ):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving amendments: {e}")

    async def get_committee_reports(self):
        try:
            async for item in self._get_items(
                "committee_reports_urls",
                CommitteeReport,
                _id_package_init=self._id_package,
            ):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving committee reports: {e}")

    async def get_cosponsors(self, verbose=False):
        try:
            async for item in self._get_items(
                "cosponsors_url",
                Member,
                verbose=verbose,
                _id_package_init=self._id_package,
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
            async for item in self._get_items(
                "subjects_url", Subject, _id_package_init=self._id_package
            ):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving subjects: {e}")

    async def get_summaries(self):
        try:
            async for item in self._get_items(
                "summaries_url", Summary, _id_package_init=self._id_package
            ):
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
            async for item in self._get_items(
                "titles_url", Title, _id_package_init=self._id_package
            ):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving titles: {e}")

    async def get_relatedbills(self):
        try:
            async for item in self._get_items(
                "related_bills_url", RelatedBill, _id_package_init=self._id_package
            ):
                for relationship in item.relationships:
                    yield RelatedBill(
                        data={
                            "bill_id": item.bill_id,
                            "relatedbill_id": item.relatedbill_id,
                            "relationship_identified_by": relationship.get(
                                "identifiedBy"
                            ),
                            "relationship_type": relationship.get("type"),
                            "data": item.data,
                        },
                        _id_package_init=self._id_package,
                    )
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving related bills: {e}")


@dataclass
class Member(Retriever):
    data: dict
    _adapter: RestAdapter
    _pagination: dict
    _id_package_init: tuple = None

    def __init__(
        self,
        data: dict,
        _pagination: dict,
        _adapter: RestAdapter,
        _id_package_init: tuple | None = None,
    ):
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

        self._parse_bioguide_id()

        self._setup_id_package()

        self._parse_terms()
        self._parse_party_history()
        self._parse_leadership_roles()
        self._parse_address_information()
        self._parse_cosponsored_legislation()
        self._parse_sponsored_legislation()
        self._parse_depiction()
        self.fix_party_history()

    def __str__(self):
        return (
            f"{self.first_name.title()} {self.last_name.title()}"
            if self.first_name and self.last_name
            else "Member"
        )

    def __repr__(self):
        return self.__str__()

    def _setup_id_package(self):
        self._id_package = (
            "bioguide_id",
            getattr(self, "bioguide_id", getattr(self, "_bioguide_id", None)),
        )

    def _parse_bioguide_id(self):
        self._bioguide_id = self.data.get("bioguideId")
        if not self._bioguide_id:
            self._id_error = True

    def _parse_terms(self):
        terms = getattr(self, "_terms", [])
        if isinstance(terms, dict):
            terms = terms.get("item")
        self._terms = [
            MemberTerm(data=term, _id_package_init=self._id_package) for term in terms
        ]

    def _parse_party_history(self):
        party_history = getattr(self, "_party_history", [])
        self._party_history = [
            PartyHistory(data=party, _id_package_init=self._id_package)
            for party in party_history
            if isinstance(party, dict)
        ]

    def _parse_leadership_roles(self):
        leadership = getattr(self, "_leadership", [])
        self._leadership_roles = [
            LeadershipRole(
                data=role, _id_package_init=self._id_package, _terms=self._terms
            )
            for role in leadership
        ]

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
            current_party = next(
                (party for party in self._party_history if party.end_year is None), None
            )
            if current_party:
                self._party = current_party.party_name
                self._party_code = current_party.party_code

    @property
    def bioguide_id(self) -> str | None:
        return getattr(self, "_bioguide_id", None)

    @property
    def birth_year(self) -> str | None:
        return getattr(self, "_birth_year", None)

    @property
    def death_year(self) -> str | None:
        return getattr(self, "_death_year", None)

    @property
    def direct_order_name(self) -> str | None:
        return getattr(self, "_direct_order_name", None)

    @property
    def district(self) -> str | None:
        return getattr(self, "_district", None)

    @property
    def first_name(self) -> str | None:
        name = getattr(self, "_first_name", None)
        return name.upper() if isinstance(name, str) else name

    @property
    def honorific_name(self) -> str | None:
        return getattr(self, "_honorific_name", None)

    @property
    def inverted_order_name(self) -> str | None:
        return getattr(self, "_inverted_order_name", None)

    @property
    def last_name(self) -> str | None:
        name = getattr(self, "_last_name", None)
        return name.upper() if isinstance(name, str) else name

    @property
    def middle_name(self) -> str | None:
        name = getattr(self, "_middle_name", None)
        return name.upper() if isinstance(name, str) else name

    @property
    def full_name(self) -> str | None:
        return getattr(self, "_full_name", None)

    @property
    def is_current_member(self) -> bool | None:
        return (
            bool(getattr(self, "_current_member", None))
            if getattr(self, "_current_member", None) is not None
            else None
        )

    @property
    def sponsorship_date(self) -> str | None:
        return getattr(self, "_sponsorship_date", None)

    @property
    def party_code(self) -> str | None:
        return getattr(self, "_party_code", None)

    @property
    def party(self) -> str | None:
        return getattr(self, "_party", None)

    @property
    def state(self) -> str | None:
        return getattr(self, "_state", None)

    @property
    def updated_at(self) -> str | None:
        return getattr(self, "_update_date", None)

    @property
    def nickname(self) -> str | None:
        return getattr(self, "_nick_name", None)

    @property
    def official_url(self) -> str | None:
        return getattr(self, "_official_website_url", None)

    @property
    def suffix_name(self) -> str | None:
        return getattr(self, "_suffix_name", None)

    @property
    def office_address(self) -> str | None:
        return getattr(self, "_office_address", None)

    @property
    def office_city(self) -> str | None:
        return getattr(self, "_office_city", None)

    @property
    def office_district(self) -> str | None:
        return getattr(self, "_office_district", None)

    @property
    def office_zip(self) -> str | None:
        return getattr(self, "_office_zip", None)

    @property
    def office_phone_number(self) -> str | None:
        return getattr(self, "_office_phone_number", None)

    @property
    def terms(self) -> list[MemberTerm] | None:
        return getattr(self, "_terms", [])

    @property
    def party_history(self) -> list[PartyHistory] | None:
        return getattr(self, "_party_history", [])

    @property
    def leadership_roles(self) -> list[LeadershipRole] | None:
        return getattr(self, "_leadership_roles", [])

    @property
    def cosponsored_legislation_count(self) -> int:
        return getattr(self, "_cosponsored_legislation_count", None)

    @property
    def cosponsored_legislation_url(self) -> str | None:
        return getattr(self, "_cosponsored_legislation_url", None)

    @property
    def sponsored_legislation_count(self) -> int:
        return getattr(self, "_sponsored_legislation_count", None)

    @property
    def sponsored_legislation_url(self) -> str | None:
        return getattr(self, "_sponsored_legislation_url", None)

    @property
    def depiction_image_url(self) -> str | None:
        return getattr(self, "_depiction_image_url", None)

    @property
    def depiction_attribution(self) -> str | None:
        return getattr(self, "_depiction_attribution", None)

    @property
    def is_by_request(self) -> bool:
        return (
            getattr(self, "_is_by_request", None) == "Y"
            if getattr(self, "_is_by_request", None) is not None
            else None
        )

    @property
    def is_original_cosponsor(self) -> bool | None:
        return (
            getattr(self, "_is_original_cosponsor", None)
            if getattr(self, "_is_original_cosponsor", None) is not None
            else None
        )

    @property
    def url(self) -> str | None:
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
            "cosponsored_legislation_url",
            Bill,
            verbose=verbose,
            _id_package_init=self._id_package,
        ):
            yield item

    async def get_sponsored_legislation(self, verbose=False):
        async for item in self._get_items(
            "sponsored_legislation_url",
            Bill,
            verbose=verbose,
            _id_package_init=self._id_package,
        ):
            yield item


@dataclass
class Amendment(Retriever):
    data: dict
    _adapter: RestAdapter
    _pagination: dict
    _id_package_init: tuple = None

    def __init__(
        self,
        data: dict,
        _pagination: dict,
        _adapter: RestAdapter,
        _id_package_init: tuple | None = None,
    ):
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
        return (
            f"{str(self.amendment_type).lower()}{self.amendment_number}-{self.congress}"
            if self.amendment_type and self.amendment_number and self.congress
            else "Amendment"
        )

    def __repr__(self):
        return self.__str__()

    def _setup_id_package(self):
        self._id_package = (
            "amendment_id",
            getattr(self, "amendment_id", getattr(self, "_amendment_id", "-99")),
        )

    def _parse_amendment_id(self):
        congress = getattr(self, "_congress", None)
        type = getattr(self, "_type", None)
        number = getattr(self, "_number", None)

        if None in [congress, type, number]:
            self._id_error = True
            self._amendment_id = None
        else:
            self._amendment_id = f"{str(type).lower()}{number}-{congress}"

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
            else None
        )

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
        self._notes = [
            note.get("text")
            for note in notes
            if isinstance(note, dict) and "text" in note
        ]

    def _parse_sponsors(self):
        sponsors = getattr(self, "_sponsors", [])
        self._sponsors_urls = [
            sponsor.get("url")
            for sponsor in sponsors
            if isinstance(sponsor, dict) and "url" in sponsor
        ]
        self._sponsors_bioguide_ids = [
            sponsor.get("bioguideId")
            for sponsor in sponsors
            if isinstance(sponsor, dict) and "bioguideId" in sponsor
        ]
        self._sponsors_system_codes = [
            sponsor.get("systemCode")
            for sponsor in sponsors
            if isinstance(sponsor, dict) and "systemCode" in sponsor
        ]
        if self._sponsors_system_codes:
            self._adapter._logger.info(
                f"Sponsor System Codes: {self._sponsors_system_codes}"
            )

    def _parse_texts(self):
        texts_endpoint = self._construct_endpoint("text")
        self._texts_url = f"https://api.congress.gov/v3/{texts_endpoint}"

    def _parse_latest_action(self):
        latest_action = getattr(self, "_latest_action", {})
        self._latest_action = (
            Action(
                data=latest_action,
                _pagination=None,
                _adapter=self._adapter,
                _id_package_init=self._id_package,
            )
            if latest_action
            else None
        )

    @property
    def congress(self) -> int | None:
        return getattr(self, "_congress", None)

    @property
    def chamber(self) -> str | None:
        return (
            getattr(self, "_chamber", None).lower()
            if getattr(self, "_chamber", None)
            else None
        )

    @property
    def amendment_type(self) -> str | None:
        return getattr(self, "_type", None)

    @property
    def amendment_number(self) -> str | None:
        return getattr(self, "_number", None)

    @property
    def proposed_at(self) -> str | None:
        return getattr(self, "_proposed_date", None)

    @property
    def submitted_at(self) -> str | None:
        return getattr(self, "_submitted_date", None)

    @property
    def title(self) -> str | None:
        return getattr(self, "_title", None)

    @property
    def updated_at(self) -> str | None:
        return getattr(self, "_update_date", None)

    @property
    def purpose(self) -> str:
        return getattr(self, "_purpose", "MISSING")

    @property
    def description(self) -> str:
        return getattr(self, "_description", "MISSING")

    @property
    def amendment_id(self) -> str | int:
        return getattr(self, "_amendment_id", None)

    @property
    def links(self) -> list[dict] | None:
        return getattr(self, "_links", [])

    @property
    def actions_url(self) -> str | None:
        return getattr(self, "_actions_url", None)

    @property
    def actions_count(self) -> int:
        return getattr(self, "_actions_count", 0)

    @property
    def amended_bills_url(self) -> str | None:
        return getattr(self, "_amended_bill_url", None)

    @property
    def amended_bill_id(self) -> str | None:
        return getattr(self, "_amended_bill_id", None)

    @property
    def amended_amendments_url(self) -> str | None:
        return getattr(self, "_amended_amendment_url", None)

    @property
    def amended_amendment_id(self) -> str | None:
        return getattr(self, "_amended_amendment_id", None)

    @property
    def amended_treaties_url(self) -> str | None:
        return getattr(self, "_amended_treaty_url", None)

    @property
    def amended_treaty_id(self) -> str | None:
        return getattr(self, "_amended_treaty_id", None)

    @property
    def amendments_to_amendment_url(self) -> str | None:
        return getattr(self, "_amendments_to_amendment_url", None)

    @property
    def amendments_to_amendment_count(self) -> int:
        return getattr(self, "_amendments_to_amendment_count", None)

    @property
    def cosponsors_url(self) -> str | None:
        return getattr(self, "_cosponsors_url", None)

    @property
    def cosponsors_count(self) -> int:
        return getattr(self, "_cosponsors_count", None)

    @property
    def latest_action(self) -> Optional["Action"]:
        return getattr(self, "_latest_action", None)

    @property
    def notes(self) -> str | None:
        if len(getattr(self, "_notes", [])) > 1:
            self._adapter._logger.warning(
                f"Amendment {self.amendment_id} has multiple notes. Returning the first one."
            )
        return (
            getattr(self, "_notes", [])[0]
            if len(getattr(self, "_notes", [])) > 0
            else None
        )

    @property
    def sponsors_url(self) -> str | None:
        return getattr(self, "_sponsors_urls", None)

    @property
    def sponsors(self) -> list[str]:
        return getattr(self, "_sponsors_bioguide_ids", [])

    @property
    def sponsor_committees(self) -> list[str]:
        return getattr(self, "_sponsors_system_codes", [])

    @property
    def texts_url(self) -> str | None:
        return getattr(self, "_texts_url", None)

    @property
    def url(self) -> str | None:
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
            async for item in self._get_items(
                "texts_url", TextVersion, _id_package_init=self._id_package
            ):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving text versions: {e}")

    async def get_actions(self):
        try:
            async for item in self._get_items(
                "actions_url", Action, _id_package_init=self._id_package
            ):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving actions: {e}")

    async def get_amendedbill(self):
        try:
            async for item in self._get_items(
                "amended_bills_url", Bill, _id_package_init=self._id_package
            ):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving amended bill details: {e}")

    async def get_amended_amendment_details(self):
        try:
            async for item in self._get_items(
                "amended_amendments_url", Amendment, _id_package_init=self._id_package
            ):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(
                f"Error retrieving amended amendment details: {e}"
            )

    async def get_amended_treaty_details(self):
        try:
            async for item in self._get_items(
                "amended_treaties_url", Treaty, _id_package_init=self._id_package
            ):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving amended treaty details: {e}")

    async def get_amendments_to_amendment(self, verbose=False):
        try:
            async for item in self._get_items(
                "amendments_to_amendment_url",
                Amendment,
                verbose=verbose,
                _id_package_init=self._id_package,
            ):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(
                f"Error retrieving amendments to amendment: {e}"
            )

    async def get_cosponsors(self, verbose=False):
        try:
            async for item in self._get_items(
                "cosponsors_url",
                Member,
                verbose=verbose,
                _id_package_init=self._id_package,
            ):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving cosponsors: {e}")

    async def get_sponsors(self, verbose=False):
        try:
            async for item in self._get_items(
                "sponsors_urls",
                Member,
                verbose=verbose,
                _id_package_init=self._id_package,
            ):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving sponsors: {e}")


@dataclass
class Committee(Retriever):
    data: dict
    _adapter: RestAdapter
    _pagination: dict
    _id_package_init: tuple = None

    def __init__(
        self,
        data: dict,
        _pagination: dict,
        _adapter: RestAdapter,
        _id_package_init: tuple | None = None,
    ):
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
            if self._id_package_init[0] == "committee_code":
                self._parent_committee_code = self._id_package_init[1]
            else:
                setattr(self, self._id_package_init[0], self._id_package_init[1])
        self._parse_committee_code()
        self._setup_id_package()
        self._parse_committee_history()
        self._parse_parent()
        self._parse_committeereports()
        self._parse_subcommittees()

    def __str__(self):
        return self.name if self.name else self.committee_code or "Committee"

    def __repr__(self):
        return self.__str__()

    def _setup_id_package(self):
        self._id_package = (
            "committee_code",
            getattr(self, "committee_code", getattr(self, "_system_code", None)),
        )

    def _parse_committee_code(self):
        self._committee_code = getattr(self, "_system_code", None)
        if not self._committee_code:
            self._id_error = True
            self._committee_code = None

    def _parse_bills(self):
        bills = getattr(self, "_bills", {})
        self._bills_url = bills.get("url")
        self._bills_count = bills.get("count", 0)

    def _parse_committee_history(self):
        self._committee_history_data = getattr(self, "_history", {})
        self._committee_history = [
            CommitteeHistory(data=hist, _id_package_init=self._id_package)
            for hist in self._committee_history_data
            if isinstance(hist, dict)
        ]

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

    def _parse_committeereports(self):
        committeereports = getattr(self, "_reports", {})
        self._committeereports_url = committeereports.get("url")
        self._committeereports_count = committeereports.get("count", 0)

    def _parse_subcommittees(self):
        subcommittees = getattr(self, "_subcommittees", [])
        self._subcommittees_urls = [
            subcommittee.get("url")
            for subcommittee in subcommittees
            if isinstance(subcommittee, dict)
        ]
        self._subcommittee_names = [
            subcommittee.get("name")
            for subcommittee in subcommittees
            if isinstance(subcommittee, dict)
        ]
        self._subcommittee_codes = [
            subcommittee.get("systemCode")
            for subcommittee in subcommittees
            if isinstance(subcommittee, dict)
        ]

    @property
    def chamber(self) -> str:
        return (
            getattr(self, "_chamber", None).lower()
            if getattr(self, "_chamber", None)
            else None
        )

    @property
    def is_current(self) -> bool | None:
        return getattr(self, "_is_current", None)

    @property
    def name(self) -> str:
        return getattr(self, "_name", None)

    @property
    def committee_code(self) -> str:
        return getattr(self, "_system_code", None)

    @property
    def committee_type(self) -> str | None:
        return getattr(self, "_type", None)

    @property
    def updated_at(self) -> str | None:
        return getattr(self, "_update_date", None)

    @property
    def bills_url(self) -> str | None:
        return getattr(self, "_bills_url", None)

    @property
    def bills_count(self) -> int:
        return getattr(self, "_bills_count", None)

    @property
    def history(self) -> list["CommitteeHistory"]:
        return getattr(self, "_committee_history", [])

    @property
    def nominations_url(self) -> str | None:
        return getattr(self, "_nominations_url", None)

    @property
    def nominations_count(self) -> int:
        return getattr(self, "_nominations_count", None)

    @property
    def is_subcommittee(self) -> bool:
        return (
            bool(getattr(self, "_is_subcommittee", None))
            if getattr(self, "_is_subcommittee", None) is not None
            else None
        )

    @property
    def parent_url(self) -> str | None:
        return getattr(self, "_parent_url", None)

    @property
    def parent_committee_name(self) -> str | None:
        return getattr(self, "_parent_committee_name", None)

    @property
    def parent_committee_code(self) -> str | None:
        return getattr(self, "_parent_committee_code", None)

    @property
    def committeereports_url(self) -> str | None:
        return getattr(self, "_committeereports_url", None)

    @property
    def committeereports_count(self) -> int:
        return getattr(self, "_committeereports_count", None)

    @property
    def subcommittees_urls(self) -> list[str]:
        return getattr(self, "_subcommittees_urls", [])

    @property
    def subcommittee_names(self) -> list[str]:
        return getattr(self, "_subcommittee_names", [])

    @property
    def subcommittees_codes(self) -> list[str]:
        return getattr(self, "_subcommittee_codes", [])

    @property
    def url(self) -> str | None:
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
            async for item in self._get_items(
                "bills_url", Bill, verbose=verbose, _id_package_init=self._id_package
            ):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving bills: {e}")

    async def get_subcommittees(self):
        if self.is_subcommittee:
            yield self._adapter._logger.info(
                "This is a subcommittee, so there will be no committees available. Try 'get_parent_committee_details'."
            )
        try:
            async for item in self._get_items(
                "subcommittees_urls",
                Committee,
                verbose=False,
                _id_package_init=self._id_package,
            ):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving subcommittees: {e}")

    async def get_parent_committee_details(self):
        if not self.is_subcommittee:
            yield self._adapter._logger.info(
                "This is not a subcommittee, so there will be no parent committee available. Try 'get_subcommittee_details'."
            )
        try:
            async for item in self._get_items(
                "parent_url", Committee, _id_package_init=self._id_package
            ):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving parent committee: {e}")

    async def get_committeereports(self, verbose=False):
        try:
            async for item in self._get_items(
                "reports_url",
                CommitteeReport,
                verbose=verbose,
                _id_package_init=self._id_package,
            ):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving reports: {e}")

    async def get_nominations(self, verbose=False):
        try:
            async for item in self._get_items(
                "nominations_url",
                Nomination,
                verbose=verbose,
                _id_package_init=self._id_package,
            ):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving nominations: {e}")

    async def get_communications(self):
        raise NotImplementedError("Communications endpoint not yet implemented.")


@dataclass
class CommitteeReport(Retriever):
    data: dict
    _adapter: RestAdapter
    _pagination: dict
    _id_package_init: tuple = None

    def __init__(
        self,
        data: dict,
        _pagination: dict,
        _adapter: RestAdapter,
        _id_package_init: tuple | None = None,
    ):
        if "committeeReports" in data and isinstance(
            data.get("committeeReports"), list
        ):
            self.data = data.get("committeeReports")[
                0
            ]  # quirk of committeeReports endpoint
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
        self._id_package = (
            "report_id",
            getattr(self, "report_id", getattr(self, "_report_id", "-99")),
        )

    def _parse_report_id(self):
        self._congress = getattr(self, "_congress", None)
        self._report_type = getattr(self, "_report_type", None)
        self._report_number = getattr(self, "_number", None)
        self._report_part = getattr(self, "_part", 0)
        self._citation = getattr(self, "_citation", None)

        if None in [self._congress, self._report_type, self._report_number]:
            self._id_error = True
            self._report_id = None
        elif self._citation:
            self._report_id = process_report_id(self._citation)
        else:
            self._report_id = f"{str(self.report_type)[0].lower()}rpt{self.report_number}-{self.report_part}-{self.congress}"

    def _parse_texts(self):
        texts = getattr(self, "_text", {})
        self._texts_url = texts.get("url")
        self._texts_count = texts.get("count", 0)

    def _parse_associated_bills(self):
        associated_bills = getattr(self, "_associated_bill", [])
        self._associated_bills_urls = [
            bill.get("url") for bill in associated_bills if isinstance(bill, dict)
        ]
        self._associated_bills_ids = [
            f"{str(bill.get('type')).lower()}{bill.get('number')}-{bill.get('congress')}"
            for bill in associated_bills
        ]

    def _parse_associated_treaties(self):
        associated_treaties = getattr(self, "_associated_treaties", [])
        self._associated_treaties_urls = [
            treaty.get("url")
            for treaty in associated_treaties
            if isinstance(treaty, dict)
        ]
        self._associated_treaties_ids = [
            f"td{treaty.get('congress')}-{treaty.get('number')}"
            for treaty in associated_treaties
        ]

    @property
    def chamber(self) -> str | None:
        return (
            getattr(self, "_chamber", None).lower()
            if getattr(self, "_chamber", None)
            else None
        )

    @property
    def citation(self) -> str | None:
        return self._citation or None

    @property
    def congress(self) -> int | None:
        return self._congress or None

    @property
    def is_conference_report(self) -> bool | None:
        return getattr(self, "_is_conference_report", None)

    @property
    def issued_at(self) -> str | None:
        return getattr(self, "_issue_date", None)

    @property
    def report_number(self) -> str | None:
        return self._report_number or None

    @property
    def report_part(self) -> str | None:
        return self._report_part or None

    @property
    def report_type(self) -> str | None:
        return self._report_type or None

    @property
    def session_number(self) -> str | None:
        return getattr(self, "_session_number", None)

    @property
    def title(self) -> str | None:
        return getattr(self, "_title", None)

    @property
    def updated_at(self) -> str | None:
        return getattr(self, "_update_date", None)

    @property
    def report_id(self) -> str:
        return self._report_id or None

    @property
    def texts_url(self) -> str | None:
        return getattr(self, "_texts_url", None)

    @property
    def texts_count(self) -> int:
        return getattr(self, "_texts_count", None)

    @property
    def associated_bills_urls(self) -> list[str]:
        return getattr(self, "_associated_bills_urls", [])

    @property
    def associated_bills_ids(self) -> list[str]:
        return getattr(self, "_associated_bills_ids", [])

    @property
    def associated_treaties_urls(self) -> list[str]:
        return getattr(self, "_associated_treaties_urls", [])

    @property
    def associated_treaties_ids(self) -> list[str]:
        return getattr(self, "_associated_treaties_ids", [])

    @property
    def url(self) -> str | None:
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
            async for item in self._get_items(
                "texts_url", TextVersion, _id_package_init=self._id_package
            ):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving text versions: {e}")

    async def get_associated_bill_details(self):
        try:
            async for item in self._get_items(
                "associated_bills_urls", Bill, _id_package_init=self._id_package
            ):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(
                f"Error retrieving associated bill details: {e}"
            )

    async def get_associated_treaty_details(self):
        try:
            async for item in self._get_items(
                "associated_treaties_urls", Treaty, _id_package_init=self._id_package
            ):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(
                f"Error retrieving associated treaty details: {e}"
            )


@dataclass
class Treaty(Retriever):
    data: dict
    _adapter: RestAdapter
    _pagination: dict
    _id_package_init: tuple = None

    def __init__(
        self,
        data: dict,
        _pagination: dict,
        _adapter: RestAdapter,
        _id_package_init: tuple | None = None,
    ):
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
        self._parse_committees()

    def __str__(self):
        return self.treaty_id or "Treaty"

    def __repr__(self):
        return self.__str__()

    def _setup_id_package(self):
        self._id_package = (
            "treaty_id",
            getattr(self, "treaty_id", getattr(self, "_treaty_id", None)),
        )

    def _parse_treaty_id(self):
        self._congress_received = getattr(self, "_congress_received", None)
        self._number = getattr(self, "_number", None)
        self._suffix = getattr(self, "_suffix", None)

        treaty_num = f"{self._number}{self._suffix}" if self._suffix else self._number
        if None in [self._congress_received, treaty_num]:
            self._id_error = True
            self._treaty_id = None
        else:
            self._treaty_id = f"td{self._congress_received}-{treaty_num}"

    def _parse_actions(self):
        actions = getattr(self, "_actions", {})
        self._actions_url = actions.get("url")
        self._actions_count = actions.get("count", 0)

    def _parse_countries_parties(self):
        countriesParties = getattr(self, "_countries_parties", [])
        self._countries_parties = [
            country.get("name")
            for country in countriesParties
            if isinstance(country, dict)
        ]

    def _parse_index_terms(self):
        indexTerms = getattr(self, "_index_terms", [])
        self._index_terms = [
            term.get("name") for term in indexTerms if isinstance(term, dict)
        ]

    def _parse_parts(self):
        parts = getattr(self, "_parts", {})
        self._parts_urls = parts.get("urls", [])
        self._parts_count = parts.get("count", 0)

    def _parse_related_docs(self):
        relatedDocs = getattr(self, "_related_docs", [])
        self._related_documents_urls = [
            doc.get("url") for doc in relatedDocs if isinstance(doc, dict)
        ]
        self._related_documents = [
            doc.get("citation") for doc in relatedDocs if isinstance(doc, dict)
        ]

    def _parse_titles(self):
        titles = getattr(self, "_titles", {})
        self._titles = [
            Title(data=title, _id_package_init=self._id_package)
            for title in titles
            if isinstance(title, dict)
        ]

    def _parse_committees(self):
        # construct committees endpoint
        self._committees_url = self._construct_endpoint("committees")

    @property
    def congress(self) -> str | None:
        return getattr(self, "_congress", None)

    @property
    def congress_considered(self) -> int | None:
        return getattr(self, "_congress_considered", None)

    @property
    def congress_received(self) -> int | None:
        return self._congress_received or None

    @property
    def in_force_at(self) -> str | None:
        return getattr(self, "_in_force_date", None)

    @property
    def number(self) -> str | None:
        return self._number or None

    @property
    def old_number(self) -> str | None:
        return getattr(self, "_old_number", None)

    @property
    def old_number_display_name(self) -> str | None:
        return getattr(self, "_old_number_display_name", None)

    @property
    def resolution_text(self) -> str | None:
        return getattr(self, "_resolution_text", None)

    @property
    def suffix(self) -> str | None:
        return self._suffix or None

    @property
    def topic(self) -> str | None:
        return getattr(self, "_topic", None)

    @property
    def transmitted_at(self) -> str | None:
        return getattr(self, "_transmitted_date", None)

    @property
    def treaty_number(self) -> str | None:
        return getattr(self, "_treaty_num", None)

    @property
    def updated_at(self) -> str | None:
        return getattr(self, "_update_date", None)

    @property
    def treaty_id(self) -> str:
        return getattr(self, "_treaty_id", None)

    @property
    def actions_url(self) -> str | None:
        return getattr(self, "_actions_url", None)

    @property
    def actions_count(self) -> int:
        return getattr(self, "_actions_count", None)

    @property
    def countries(self) -> list[str]:
        return getattr(self, "_countries_parties", [])

    @property
    def indexterms(self) -> list[str]:
        return getattr(self, "_index_terms", [])

    @property
    def parts_urls(self) -> list[str]:
        return getattr(self, "_parts_urls", [])

    @property
    def parts_count(self) -> int:
        return getattr(self, "_parts_count", None)

    @property
    def associated_reports_urls(self) -> list[str]:
        return getattr(self, "_related_documents_urls", [])

    @property
    def associated_reports(self) -> list[str]:
        return getattr(self, "_related_documents", [])

    @property
    def titles(self) -> list["Title"]:
        return getattr(self, "_titles", [])

    @property
    def url(self) -> str | None:
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
            async for item in self._get_items(
                "actions_url", Action, _id_package_init=self._id_package
            ):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving actions: {e}")

    async def get_parts(self, verbose=False):
        if verbose:
            async for item in self._get_items(
                "parts_urls", Treaty, verbose=False, _id_package_init=self._id_package
            ):
                yield item
        else:
            items = []
            async for part_data in self._get_items(
                "parts", None, verbose=verbose, _id_package_init=self._id_package
            ):
                if isinstance(part_data, dict) and "suffix" in part_data:
                    items.append(part_data["suffix"])
                    yield sorted(items)
                else:
                    self._adapter._logger.warning(
                        f"Unexpected part data format: {part_data}"
                    )

    async def get_associated_reports(self):
        try:
            async for item in self._get_items(
                "related_docs_urls", CommitteeReport, _id_package_init=self._id_package
            ):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving related documents: {e}")

    async def get_committeeactivities(self, verbose=False):
        try:
            async for item in self._get_items(
                "committees_url",
                CommitteeActivity,
                verbose=verbose,
                _id_package_init=self._id_package,
            ):
                raw_data = item.data
                cleaned_data = {  # leave the "data" in camelCase for now, to parse into snake_case within the subcomponents
                    "chamber": raw_data.get("chamber").lower()
                    if raw_data.get("chamber")
                    else None,
                    "name": raw_data.get("name"),
                    "systemCode": raw_data.get("systemCode"),
                    "type": raw_data.get("type"),
                }
                for activity in raw_data.get("activities", []):
                    yield CommitteeActivity(
                        data=cleaned_data,
                        activity_name=activity.get("name"),
                        activity_date=activity.get("date"),
                        _id_package_init=self._id_package,
                    )
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving committees - {e}")


@dataclass
class Nomination(Retriever):
    data: dict
    _adapter: RestAdapter
    _pagination: dict
    _id_package_init: tuple | None = None

    def __init__(
        self,
        data: dict,
        _pagination: dict,
        _adapter: RestAdapter,
        _id_package_init: tuple | None = None,
    ):
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
            f"{self.citation}"
            if self.citation
            else f"{self.nomination_number}-{self.part_number}" or "Nomination"
        )

    def __repr__(self):
        return self.__str__()

    def _setup_id_package(self):
        self._id_package = (
            "nomination_id",
            getattr(self, "nomination_id", getattr(self, "_nomination_id", "-99")),
        )

    def _parse_nomination_id(self):
        self._congress = getattr(self, "_congress", None)
        self._citation = getattr(self, "_citation", None)
        self._number = getattr(self, "_number", None)
        self._part_number = getattr(self, "_part_number", 00)

        if None in [self._congress, self._number]:
            self._id_error = True
            self._nomination_id = None
        elif self._citation:
            self._nomination_id = f"{self._citation}-{self._congress}"
        else:
            self._nomination_id = f"{self._number}-{self._part_number}-{self._congress}"

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
        self._nominees_urls = [
            nominee.get("url") for nominee in nominees if isinstance(nominee, dict)
        ]
        self._positions = [
            NominationPosition(data=position, _id_package_init=self._id_package)
            for position in nominees
            if "ordinal" in position and isinstance(position, dict)
        ]

    def _parse_hearings(self):
        hearings = getattr(self, "_hearings", {})
        self._hearings_url = hearings.get("url")
        self._hearings_count = hearings.get("count", 0)

    def _parse_latest_action(self):
        self._latest_action_data = getattr(self, "_latest_action", {})
        self._latest_action = (
            Action(
                data=self._latest_action_data,
                _pagination=None,
                _adapter=self._adapter,
                _id_package_init=self._id_package,
            )
            if self._latest_action_data
            else None
        )

    @property
    def citation(self) -> str | None:
        return self._citation or None

    @property
    def congress(self) -> int | None:
        return self._congress or None

    @property
    def description(self) -> str | None:
        return getattr(self, "_description", None)

    @property
    def executive_calendar_number(self) -> str | None:
        return getattr(self, "_executive_calendar_number", None)

    @property
    def authority_date(self) -> str | None:
        return getattr(self, "_authority_date", None)

    @property
    def is_civilian(self) -> bool:
        return getattr(self, "_is_list", False)

    @property
    def is_privileged(self) -> bool | None:
        return getattr(self, "_is_privileged", False)

    @property
    def nomination_number(self) -> str | None:
        return getattr(self, "_number", None)

    @property
    def part_number(self) -> str | None:
        return self._part_number or None

    @property
    def received_at(self) -> str | None:
        return getattr(self, "_received_date", None)

    @property
    def updated_at(self) -> str | None:
        return getattr(self, "_update_date", None)

    @property
    def nomination_id(self) -> str:
        return self._nomination_id or "-99"

    @property
    def actions_count(self) -> int:
        return getattr(self, "_actions_count", None)

    @property
    def actions_url(self) -> str | None:
        return getattr(self, "_actions_url", None)

    @property
    def committees_count(self) -> int:
        return getattr(self, "_committees_count", None)

    @property
    def committees_url(self) -> str | None:
        return getattr(self, "_committees_url", None)

    @property
    def hearings_count(self) -> int:
        return getattr(self, "_hearings_count", None)

    @property
    def hearings_url(self) -> str | None:
        return getattr(self, "_hearings_url", None)

    @property
    def nominees_urls(self) -> list[str]:
        return getattr(self, "_nominees_urls", [])

    @property
    def nomineepositions(self) -> list["NominationPosition"]:
        return getattr(self, "_positions", [])

    @property
    def latest_action(self) -> Optional["Action"]:
        return getattr(self, "_latest_action", None)

    @property
    def url(self) -> str | None:
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
            async for item in self._get_items(
                "nominees_urls", Nominee, _id_package_init=self._id_package
            ):
                # Get the current index and url
                if nominees_urls:
                    i, _ = nominees_urls.pop(0)  # Get and remove the first tuple
                    if i < len(self.nomineepositions):
                        nominee_position = self.nomineepositions[i]
                        if nominee_position.nominee_id != item.nominee_id:
                            item.data["nominee_id"] = nominee_position.nominee_id
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving nominees - {e}")

    async def get_actions(self):
        try:
            async for item in self._get_items(
                "actions_url", Action, _id_package_init=self._id_package
            ):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving actions - {e}")

    async def get_committeeactivities(self, verbose=False):
        try:
            async for item in self._get_items(
                "committees_url",
                CommitteeActivity,
                verbose=verbose,
                _id_package_init=self._id_package,
            ):
                raw_data = item.data
                cleaned_data = {  # leave the "data" in camelCase for now, to parse into snake_case within the subcomponents
                    "chamber": raw_data.get("chamber").lower()
                    if raw_data.get("chamber")
                    else None,
                    "name": raw_data.get("name"),
                    "systemCode": raw_data.get("systemCode"),
                    "type": raw_data.get("type"),
                }
                for activity in raw_data.get("activities", []):
                    yield CommitteeActivity(
                        data=cleaned_data,
                        activity_name=activity.get("name"),
                        activity_date=activity.get("date"),
                        _id_package_init=self._id_package,
                    )
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving committees - {e}")

    async def get_hearings(self):
        try:
            async for item in self._get_items(
                "hearings_url", Hearing, _id_package_init=self._id_package
            ):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving hearings - {e}")


@dataclass
class Hearing(Retriever):
    data: dict
    _adapter: RestAdapter
    _pagination: dict
    _id_package_init: tuple | None = None

    def __init__(
        self,
        data: dict,
        _pagination: dict,
        _adapter: RestAdapter,
        _id_package_init: tuple | None = None,
    ):
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

        self._parse_hearing_id()

        self._setup_id_package()

        self._parse_associated_meeting()
        self._parse_committees()
        self._parse_texts()
        self._parse_dates()

    def __str__(self):
        return str(self.hearing_id) or "Hearing"

    def __repr__(self):
        return self.__str__()

    def _setup_id_package(self):
        self._id_package = (
            "hearing_id",
            getattr(self, "hearing_id", getattr(self, "_hearing_id", None)),
        )

    def _parse_hearing_id(self):
        jacket_number = getattr(self, "_jacket_number", None)
        congress = getattr(self, "_congress", None)
        hearing_type = getattr(self, "_chamber", None)[0].lower()

        if None in [jacket_number, congress, hearing_type]:
            self._id_error = True
            self._hearing_id = None
        else:
            self._hearing_id = f"{hearing_type}hrg{jacket_number}-{congress}"

    def _parse_associated_meeting(self):
        associated_meeting = getattr(self, "_associated_meeting", {})
        self._associated_meeting_id = associated_meeting.get("eventId")
        self._associated_meeting_url = associated_meeting.get("url")

    def _parse_committees(self):
        committees = getattr(self, "_committees", [])
        self._committees_urls = [
            committee.get("url")
            for committee in committees
            if isinstance(committee, dict)
        ]
        self._committee_names = [
            committee.get("name")
            for committee in committees
            if isinstance(committee, dict)
        ]
        self._committee_codes = [
            committee.get("systemCode")
            for committee in committees
            if isinstance(committee, dict)
        ]

    def _parse_dates(self):
        dates = getattr(self, "_dates", [])
        self._dates = [date.get("date") for date in dates if isinstance(date, dict)]

    def _parse_texts(self):
        texts_data = getattr(self, "_formats", [])
        self._texts = [
            TextVersion(
                data=text,
                _pagination=None,
                _adapter=self._adapter,
                _id_package_init=self._id_package,
            )
            for text in texts_data
            if isinstance(text, dict)
        ]

    @property
    def hearing_id(self) -> str | None:
        return getattr(self, "_event_id", None)

    @property
    def chamber(self) -> str | None:
        return (
            getattr(self, "_chamber", None).lower()
            if getattr(self, "_chamber", None)
            else None
        )

    @property
    def citation(self) -> str | None:
        return getattr(self, "_citation", None)

    @property
    def congress(self) -> int | None:
        return getattr(self, "_congress", None)

    @property
    def date(self) -> str | None:
        return getattr(self, "_date", None)

    @property
    def errata_number(self) -> str | None:
        return getattr(self, "_errata_number", None)

    @property
    def jacket_number(self) -> str:
        return getattr(self, "_jacket_number", None)

    @property
    def loc_id(self) -> str | None:
        return getattr(self, "_library_of_congress_identifier", None)

    @property
    def hearing_number(self) -> str | None:
        return getattr(self, "_number", None)

    @property
    def part_number(self) -> str | None:
        return getattr(self, "_part", None)

    @property
    def title(self) -> str | None:
        return getattr(self, "_title", None)

    @property
    def updated_at(self) -> str | None:
        return getattr(self, "_update_date", None)

    @property
    def associated_meeting_id(self) -> str | None:
        return getattr(self, "_associated_meeting_id", None)

    @property
    def associated_meeting_url(self) -> str | None:
        return getattr(self, "_associated_meeting_url", None)

    @property
    def committees_urls(self) -> list[str]:
        return getattr(self, "_committees_urls", [])

    @property
    def committee_names(self) -> list[str]:
        return getattr(self, "_committee_names", [])

    @property
    def committee_codes(self) -> list[str]:
        return getattr(self, "_committee_codes", [])

    @property
    def dates(self) -> list[str]:
        return getattr(self, "_dates", [])

    @property
    def formats(self) -> list["TextVersion"]:
        return getattr(self, "_texts", [])

    @property
    def url(self) -> str | None:
        return getattr(self, "_url", None)

    def _construct_endpoint(self, path):
        if (
            self.congress
            and self.chamber
            and self.jacket_number.replace("-", "").isdigit()
        ):
            _endpoint = (
                f"/nomination/{self.congress}/{self.chamber}/{self.jacket_number}"
            )
        else:
            raise ValueError(
                "Hearing object needs a congress, chamber, and jacket number or previously defined endpoint for further functions."
            )
        return f"{_endpoint}/{path}"

    async def get_associated_meeting(self):
        try:
            async for item in self._get_items(
                "associated_meeting_url",
                CommitteeMeeting,
                _id_package_init=self._id_package,
            ):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving associated meeting: {e}")

    async def get_committees(self):
        try:
            async for item in self._get_items(
                "committees_urls", Committee, _id_package_init=self._id_package
            ):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving committees: {e}")


@dataclass
class CommitteeMeeting(Retriever):
    data: dict
    _adapter: RestAdapter
    _pagination: dict
    _id_package_init: tuple | None = None

    def __init__(
        self,
        data: dict,
        _pagination: dict,
        _adapter: RestAdapter,
        _id_package_init: tuple | None = None,
    ):
        if "committeeMeeting" in data and isinstance(
            data.get("committeeMeeting"), dict
        ):
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
        self._id_package = "meeting_id", getattr(self, "_event_id", None)

    def _parse_meeting_id(self):
        self._event_id = getattr(self, "_event_id", None)
        if None in [self._event_id, self.congress]:
            self._id_error = True
            self._event_id = None
        else:
            self._meeting_id = f"{self.congress}-{self._event_id}"

    def _parse_committees(self):
        committees = getattr(self, "_committees", [])
        self._committees_urls = [
            committee.get("url")
            for committee in committees
            if isinstance(committee, dict)
        ]
        self._committee_names = [
            committee.get("name")
            for committee in committees
            if isinstance(committee, dict)
        ]
        self._committee_codes = [
            committee.get("systemCode")
            for committee in committees
            if isinstance(committee, dict)
        ]

    def _parse_hearing_transcripts(self):
        hearing_transcripts = getattr(self, "_hearing_transcript", [])
        self._hearing_transcripts_urls = [
            hearing_transcript.get("url")
            for hearing_transcript in hearing_transcripts
            if isinstance(hearing_transcript, dict)
        ]
        self._hearing_transcripts_ids = []
        for hearing_transcript in hearing_transcripts:
            if isinstance(hearing_transcript, dict) and hearing_transcript.get("url"):
                url = hearing_transcript.get("url")
                # Parse URL, example: "https://api.congress.gov/v3/hearing/115/house/23885?format=json"
                # Extract congress (after '/hearing/'), chamber (after congress), and jacket number (after chamber)
                try:
                    # Split by '/' and find the parts after 'hearing'
                    parts = url.split("/")
                    hearing_index = parts.index("hearing") if "hearing" in parts else -1
                    if hearing_index >= 0 and hearing_index + 3 < len(parts):
                        congress = parts[hearing_index + 1]
                        chamber = parts[hearing_index + 2]
                        jacket_number = parts[hearing_index + 3].split("?")[
                            0
                        ]  # Remove query params

                        # Format: first letter of chamber + "hrg" + jacket number + "-" + congress
                        hearing_id = (
                            f"{chamber[0].lower()}hrg{jacket_number}-{congress}"
                        )
                        self._hearing_transcripts_ids.append(hearing_id)
                except (IndexError, AttributeError):
                    self._hearing_transcripts_ids.append(
                        hearing_transcript.get("jacketNumber")
                    )

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
        self._meeting_documents = [
            TextVersion(
                data=meetingdoc,
                _pagination={},
                _adapter=self._adapter,
                _id_package_init=self._id_package,
            )
            for meetingdoc in meetingDocuments
            if isinstance(meetingdoc, dict)
            and meetingdoc.get("documentType") != "Bills and Resolutions"
        ]

    def _parse_related_items(self):
        relatedItems = getattr(self, "_related_items", {})

        for item_type in ["bills", "treaties", "nominations"]:
            items = relatedItems.get(item_type, [])
            if item_type == "bills":
                self._associated_bill_ids = [
                    f"{str(bill.get('type')).lower()}{bill.get('number')}-{bill.get('congress') or 'ID ERROR'}"
                    for bill in items
                    if all([bill.get("type"), bill.get("number"), bill.get("congress")])
                    and isinstance(bill, dict)
                ]
                self._associated_bill_urls = [
                    bill.get("url") for bill in items if isinstance(bill, dict)
                ]
            elif item_type == "treaties":
                self._associated_treaty_ids = [
                    f"td{treaty.get('congress')}-{treaty.get('number')}{treaty.get('part', '')}"
                    for treaty in items
                    if treaty.get("number")
                    and treaty.get("congress")
                    and isinstance(treaty, dict)
                ]
                self._associated_treaty_urls = [
                    treaty.get("url") for treaty in items if isinstance(treaty, dict)
                ]
            elif item_type == "nominations":
                self._associated_nomination_ids = [
                    f"PN{nomination.get('number')}-{nomination.get('part', '00')}-{nomination.get('congress')}"
                    for nomination in items
                    if all(
                        [
                            nomination.get("number"),
                            nomination.get("part", "00"),
                            nomination.get("congress"),
                        ]
                    )
                    and isinstance(nomination, dict)
                ]
                self._associated_nomination_urls = [
                    nomination.get("url")
                    for nomination in items
                    if isinstance(nomination, dict)
                ]

    def _parse_videos(self):
        self._videos = getattr(self, "_videos", [])
        self._video_names = [
            video.get("name") for video in self._videos if isinstance(video, dict)
        ]

    def _parse_witness_documents(self):
        witness_documents = getattr(self, "_witness_documents", [])
        self._witness_documents = [
            TextVersion(
                data=witnessDocument,
                _pagination={},
                _adapter=self._adapter,
                _id_package_init=self._id_package,
            )
            for witnessDocument in witness_documents
            if isinstance(witnessDocument, dict)
        ]

    def _parse_witnesses(self):
        witnesses = getattr(self, "_witnesses", [])
        self._witnesses = [
            Witness(data=witness, _id_package_init=self._id_package)
            for witness in witnesses
            if isinstance(witness, dict)
        ]

    @property
    def meeting_id(self) -> str:
        return getattr(self, "_meeting_id", None)

    @property
    def chamber(self) -> str:
        return (
            getattr(self, "_chamber", None).lower()
            if getattr(self, "_chamber", None)
            else None
        )

    @property
    def congress(self) -> int | None:
        return getattr(self, "_congress", None)

    @property
    def date(self) -> str | None:
        return getattr(self, "_date", None)

    @property
    def meeting_status(self) -> str | None:
        return getattr(self, "_meeting_status", None)

    @property
    def title(self) -> str:
        return (
            getattr(self, "_title", "")
            .replace("  ", "")
            .replace("\n", "")
            .replace("\t", "")
        )

    @property
    def meeting_type(self) -> str | None:
        return getattr(self, "_type", None)

    @property
    def updated_at(self) -> str | None:
        return getattr(self, "_update_date", None)

    @property
    def committees_urls(self) -> list[str]:
        return getattr(self, "_committees_urls", [])

    @property
    def committee_names(self) -> list[str]:
        return getattr(self, "_committee_names", [])

    @property
    def committee_codes(self) -> list[str]:
        return getattr(self, "_committee_codes", [])

    @property
    def hearing_jacketnumber_urls(self) -> list[str]:
        return getattr(self, "_hearing_transcripts_urls", [])

    @property
    def hearing_jacketnumbers(self) -> list[str]:
        return getattr(self, "_hearing_transcripts_list", [])

    @property
    def building(self) -> str | None:
        return getattr(self, "_building", None)

    @property
    def room(self) -> str | None:
        return getattr(self, "_room", None)

    @property
    def address(self) -> str | None:
        return getattr(self, "_address", None)

    @property
    def meeting_documents(self) -> list["TextVersion"]:
        return getattr(self, "_meeting_documents", [])

    @property
    def associated_bill_ids(self) -> list[str]:
        return getattr(self, "_associated_bill_ids", [])

    @property
    def associated_bill_urls(self) -> list[str]:
        return getattr(self, "_associated_bill_urls", [])

    @property
    def associated_treaty_ids(self) -> list[str]:
        return getattr(self, "_associated_treaty_ids", [])

    @property
    def associated_treaty_urls(self) -> list[str]:
        return getattr(self, "_associated_treaty_urls", [])

    @property
    def associated_nomination_ids(self) -> list[str]:
        return getattr(self, "_associated_nomination_ids", [])

    @property
    def associated_nomination_urls(self) -> list[str]:
        return getattr(self, "_associated_nomination_urls", [])

    @property
    def videos_urls(self) -> list[str]:
        return getattr(self, "_videos_urls", [])

    @property
    def video_names(self) -> list[str]:
        return getattr(self, "_video_names", [])

    @property
    def witness_documents(self) -> list["TextVersion"]:
        return getattr(self, "_witness_documents", [])

    @property
    def witnesses(self) -> list[Witness]:
        return getattr(self, "_witnesses", [])

    @property
    def url(self) -> str | None:
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

    # async def get_committees(self):
    #     try:
    #         async for item in self._get_items(
    #             "committees_urls", Committee, _id_package_init=self._id_package
    #         ):
    #             yield item
    #     except PyCongressException as e:
    #         self._adapter._logger.error(f"Error retrieving committees: {e}")

    # async def get_related_bills(self):
    #     try:
    #         async for item in self._get_items(
    #             "associated_bills", Bill, _id_package_init=self._id_package
    #         ):
    #             yield item
    #     except PyCongressException as e:
    #         self._adapter._logger.error(f"Error retrieving related bills: {e}")

    # async def get_related_treaties(self):
    #     try:
    #         async for item in self._get_items(
    #             "associated_treaties", Treaty, _id_package_init=self._id_package
    #         ):
    #             yield item
    #     except PyCongressException as e:
    #         self._adapter._logger.error(f"Error retrieving related treaties: {e}")

    # async def get_related_nominations(self):
    #     try:
    #         async for item in self._get_items(
    #             "associated_nominations", Nomination, _id_package_init=self._id_package
    #         ):
    #             yield item
    #     except PyCongressException as e:
    #         self._adapter._logger.error(f"Error retrieving related nominations: {e}")

    # async def get_hearings(self):
    #     try:
    #         async for item in self._get_items(
    #             "hearing_transcripts_urls", Hearing, _id_package_init=self._id_package
    #         ):
    #             yield item
    #     except PyCongressException as e:
    #         self._adapter._logger.error(f"Error retrieving related hearings: {e}")


@dataclass
class CommitteePrint(Retriever):
    data: dict
    _adapter: RestAdapter
    _pagination: dict
    _id_package_init: tuple | None = None

    def __init__(
        self,
        data: dict,
        _pagination: dict,
        _adapter: RestAdapter,
        _id_package_init: tuple | None = None,
    ):
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

        self._parse_print_id()

        self._setup_id_package()
        self._parse_associated_bills()
        self._parse_committees()
        self._parse_texts()

    def __str__(self):
        return str(self.print_id) or "Committee Print"

    def __repr__(self):
        return self.__str__()

    def _parse_print_id(self):
        self._congress = getattr(self, "_congress", None)
        self._chamber = getattr(self, "_chamber", None)
        self._jacket_number = getattr(self, "_jacket_number", None)

        if None in [self._congress, self._chamber, self._jacket_number]:
            self._id_error = True
            self._print_id = None
        else:
            self._print_id = f"{str(self._chamber[0]).lower()}prt{self._jacket_number}-{self._congress}"

    def _setup_id_package(self):
        self._id_package = (
            "print_id",
            getattr(self, "print_id", getattr(self, "_print_id", None)),
        )

    def _parse_associated_bills(self):
        associated_bills = getattr(self, "_associated_bills", [])
        self._associated_bills_urls = [
            bill.get("url") for bill in associated_bills if isinstance(bill, dict)
        ]
        self._associated_bill_ids = [
            f"{str(bill.get('type')).lower()}{bill.get('number')}-{bill.get('congress')}"
            for bill in associated_bills
            if all([bill.get("type"), bill.get("number"), bill.get("congress")])
            and isinstance(bill, dict)
        ]

    def _parse_committees(self):
        committees = getattr(self, "_committees", [])
        self._committees_urls = [
            committee.get("url")
            for committee in committees
            if isinstance(committee, dict)
        ]
        self._committee_names = [
            committee.get("name")
            for committee in committees
            if isinstance(committee, dict)
        ]
        self._committee_codes = [
            committee.get("systemCode")
            for committee in committees
            if isinstance(committee, dict)
        ]

    def _parse_texts(self):
        texts = getattr(self, "_text", {})
        self._texts_url = texts.get("url")
        self._texts_count = texts.get("count", 0)

    @property
    def chamber(self) -> str | None:
        return (
            getattr(self, "_chamber", None).lower()
            if getattr(self, "_chamber", None)
            else None
        )

    @property
    def citation(self) -> str | None:
        return getattr(self, "_citation", None)

    @property
    def congress(self) -> int | None:
        return getattr(self, "_congress", None)

    @property
    def jacket_number(self) -> str:
        return self._jacket_number or None

    @property
    def print_number(self) -> str | None:
        return getattr(self, "_number", None)

    @property
    def updated_at(self) -> str | None:
        return getattr(self, "_update_date", None)

    @property
    def title(self) -> str | None:
        return getattr(self, "_title", None)

    @property
    def print_id(self) -> str:
        return getattr(self, "_print_id", None)

    @property
    def associated_bills_urls(self) -> list[str]:
        return getattr(self, "_associated_bills_urls", [])

    @property
    def associated_bill_ids(self) -> list[str]:
        return getattr(self, "_associated_bill_ids", [])

    @property
    def committees_urls(self) -> list[str]:
        return getattr(self, "_committees_urls", [])

    @property
    def committee_names(self) -> list[str]:
        return getattr(self, "_committee_names", [])

    @property
    def committee_codes(self) -> list[str]:
        return getattr(self, "_committee_codes", [])

    @property
    def texts_url(self) -> list[str]:
        return getattr(self, "_texts_url", [])

    @property
    def texts_count(self) -> list[str]:
        return getattr(self, "_texts_count", None)

    @property
    def url(self) -> str | None:
        return getattr(self, "_url", None)

    def _construct_endpoint(self, path):
        if (
            self.congress
            and self.chamber
            and self.jacket_number.replace("-", "").isdigit()
        ):
            _endpoint = (
                f"/committee-print/{self.congress}/{self.chamber}/{self.jacket_number}"
            )
        else:
            raise ValueError(
                "CommitteePrint object needs a congress, chamber, and jacket number or previously defined endpoint for further functions."
            )
        return f"{_endpoint}/{path}"

    async def get_associated_bills(self):
        try:
            async for item in self._get_items(
                "associated_bills_urls", Bill, _id_package_init=self._id_package
            ):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving associated bills: {e}")

    async def get_committees(self):
        try:
            async for item in self._get_items(
                "committees_urls", Committee, _id_package_init=self._id_package
            ):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving committees: {e}")

    async def get_texts(self):
        try:
            async for item in self._get_items(
                "texts_url", TextVersion, _id_package_init=self._id_package
            ):
                yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving texts: {e}")


@dataclass
class Congress(Retriever):
    data: dict
    _adapter: RestAdapter
    _pagination: dict
    _id_package_init: tuple | None = None

    def _construct_endpoint(self, path: str):  # override abstract
        """Congress objects do not expose related sub-resources.
        This method is implemented only to satisfy the abstract base-class
        requirement and will raise if called.
        """
        raise PyCongressException("Congress has no child endpoints")

    def __init__(
        self,
        data: dict,
        _pagination: dict,
        _adapter: RestAdapter,
        _id_package_init: tuple | None = None,
    ):
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
        self._id_package = (
            "congress_no",
            getattr(self, "number", getattr(self, "_number", "-99")),
        )

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
            self._number = int(
                self._name.split("th Congress")[0]
                .split("rd Congress")[0]
                .split("st Congress")[0]
                .split("nd Congress")[0]
            )
        elif not self._name and not self._number:
            self._name = "Unknown Congress"
            self._number = -99

    @property
    def sessions(self) -> list[Session]:
        return self._sessions or []

    @property
    def name(self) -> str:
        return self._name or "-99"

    @property
    def number(self) -> int:
        return self._number or None

    @property
    def start_year(self) -> int | None:
        return getattr(self, "_start_year", None)

    @property
    def end_year(self) -> int | None:
        return getattr(self, "_end_year", None)

    @property
    def updated_at(self) -> str | None:
        return getattr(self, "_update_date", None)

    @property
    def url(self) -> str | None:
        return getattr(self, "_url", None)


@dataclass
class TextVersion(Retriever):
    data: dict
    _adapter: RestAdapter
    _pagination: dict
    _id_package_init: tuple | None = None

    def __init__(
        self,
        data: dict,
        _pagination: dict,
        _adapter: RestAdapter,
        _id_package_init: tuple | None = None,
    ):
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

    def parse_item(self, item: dict):
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

    def parse_format(self, format_item: dict):
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
    def parse_errata(errata_value: str | None) -> bool | None:
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
    def type(self) -> str | None:
        return getattr(self, "_type", None)

    @property
    def date(self) -> str | None:
        return getattr(self, "_date", None)

    @property
    def url(self) -> str | None:
        return getattr(self, "_url", None)

    @property
    def name(self) -> str | None:
        return getattr(self, "_name", None)

    @property
    def description(self) -> str | None:
        return getattr(self, "_description", None)

    @property
    def document_type(self) -> str | None:
        return getattr(self, "_document_type", None)

    @property
    def formatted_text(self) -> str | None:
        return getattr(self, "_formatted_text", None)

    @property
    def formatted_text_is_errata(self) -> bool | None:
        return getattr(self, "_formatted_text_is_errata", None)

    @property
    def xml(self) -> str | None:
        return getattr(self, "_xml", None)

    @property
    def xml_is_errata(self) -> bool | None:
        return getattr(self, "_xml_is_errata", None)

    @property
    def pdf(self) -> str | None:
        return getattr(self, "_pdf", None)

    @property
    def pdf_is_errata(self) -> bool | None:
        return getattr(self, "_pdf_is_errata", None)

    @property
    def html(self) -> str | None:
        return getattr(self, "_html", None)

    @property
    def html_is_errata(self) -> bool | None:
        return getattr(self, "_html_is_errata", None)

    async def read(self) -> str | None:
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

    def _construct_endpoint(self, path: str):  # override abstract
        """TextVersion objects do not expose related sub-resources.
        This method is implemented only to satisfy the abstract base-class
        requirement and will raise if called.
        """
        raise PyCongressException("TextVersion has no child endpoints")
