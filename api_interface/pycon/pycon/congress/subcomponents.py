from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

@dataclass
class RecordedVotes:
    data: Dict
    _id_package_init: Tuple
    action_id: str

    def __post_init__(self):
        setattr(self, self._id_package_init[0], self._id_package_init[1])

    def __str__(self):
        return f"Recorded Votes: {self.roll_number} - {self.date}"

    def __repr__(self):
        return self.__str__()

    @property
    def chamber(self) -> Optional[str]:
        return self.data.get("chamber")

    @property
    def congress(self) -> Optional[str]:
        return self.data.get("congress")

    @property
    def date(self) -> Optional[str]:
        return self.data.get("date")

    @property
    def roll_number(self) -> Optional[str]:
        return self.data.get("rollNumber")

    @property
    def session_number(self) -> Optional[str]:
        return self.data.get("sessionNumber")

    @property
    def url(self) -> Optional[str]:
        return self.data.get("url")

@dataclass
class CBOCostEstimate:
    data: Dict
    _id_package_init: Tuple

    def __post_init__(self):
        setattr(self, self._id_package_init[0], self._id_package_init[1])

    def __str__(self):
        return f"CBO Cost Estimate: {self.title} - {self.pub_date}"

    def __repr__(self):
        return self.__str__()

    @property
    def description(self) -> Optional[str]:
        return self.data.get("description")

    @property
    def pub_date(self) -> Optional[str]:
        return self.data.get("pubDate")

    @property
    def title(self) -> Optional[str]:
        return self.data.get("title")

    @property
    def url(self) -> Optional[str]:
        return self.data.get("url")

@dataclass
class Summary:
    data: Dict
    _id_package_init: Tuple

    def __post_init__(self):
        if "summaries" in self.data and isinstance(self.data, dict):
            self.data = self.data.get("summaries")

        setattr(self, self._id_package_init[0], self._id_package_init[1])

    def __str__(self):
        return str(self.text) if self.text else "No text available"

    def __repr__(self):
        return self.__str__()

    @property
    def action_date(self) -> Optional[str]:
        return self.data.get("actionDate")

    @property
    def action_desc(self) -> Optional[str]:
        return self.data.get("actionDesc")

    @property
    def text(self) -> Optional[str]:
        return self.data.get("text")

    @property
    def updated_at(self) -> Optional[str]:
        return self.data.get("update_date")

    @property
    def version_code(self) -> Optional[str]:
        return self.data.get("versionCode")

    @staticmethod
    def version_code_table(code: Optional[str] = None):
        version_codes = (
            {
                "versionCode": "00",
                "actionDesc": "Introduced in House",
                "chamber": "House",
            },
            {
                "versionCode": "00",
                "actionDesc": "Introduced in Senate",
                "chamber": "Senate",
            },
            {
                "versionCode": "01",
                "actionDesc": "Reported to Senate with amendment(s)",
                "chamber": "Senate",
            },
            {
                "versionCode": "02",
                "actionDesc": "Reported to Senate amended, 1st committee reporting",
                "chamber": "Senate",
            },
            {
                "versionCode": "03",
                "actionDesc": "Reported to Senate amended, 2nd committee reporting",
                "chamber": "Senate",
            },
            {
                "versionCode": "04",
                "actionDesc": "Reported to Senate amended, 3rd committee reporting",
                "chamber": "Senate",
            },
            {
                "versionCode": "07",
                "actionDesc": "Reported to House",
                "chamber": "House",
            },
            {
                "versionCode": "08",
                "actionDesc": "Reported to House, Part I",
                "chamber": "House",
            },
            {
                "versionCode": "09",
                "actionDesc": "Reported to House, Part II",
                "chamber": "House",
            },
            {
                "versionCode": "12",
                "actionDesc": "Reported to Senate without amendment, 1st committee reporting",
                "chamber": "Senate",
            },
            {
                "versionCode": "13",
                "actionDesc": "Reported to Senate without amendment, 2nd committee reporting",
                "chamber": "Senate",
            },
            {
                "versionCode": "17",
                "actionDesc": "Reported to House with amendment(s)",
                "chamber": "House",
            },
            {
                "versionCode": "18",
                "actionDesc": "Reported to House amended, Part I",
                "chamber": "House",
            },
            {
                "versionCode": "19",
                "actionDesc": "Reported to House amended Part II",
                "chamber": "House",
            },
            {
                "versionCode": "20",
                "actionDesc": "Reported to House amended, Part III",
                "chamber": "House",
            },
            {
                "versionCode": "21",
                "actionDesc": "Reported to House amended, Part IV",
                "chamber": "House",
            },
            {
                "versionCode": "22",
                "actionDesc": "Reported to House amended, Part V",
                "chamber": "House",
            },
            {
                "versionCode": "25",
                "actionDesc": "Reported to Senate",
                "chamber": "Senate",
            },
            {
                "versionCode": "28",
                "actionDesc": "Reported to House without amendment, Part I",
                "chamber": "House",
            },
            {
                "versionCode": "29",
                "actionDesc": "Reported to House without amendment, Part II",
                "chamber": "House",
            },
            {
                "versionCode": "31",
                "actionDesc": "Reported to House without amendment, Part IV",
                "chamber": "House",
            },
            {
                "versionCode": "33",
                "actionDesc": "Laid on table in House",
                "chamber": "House",
            },
            {
                "versionCode": "34",
                "actionDesc": "Indefinitely postponed in Senate",
                "chamber": "Senate",
            },
            {
                "versionCode": "35",
                "actionDesc": "Passed Senate amended",
                "chamber": "Senate",
            },
            {
                "versionCode": "36",
                "actionDesc": "Passed House amended",
                "chamber": "House",
            },
            {
                "versionCode": "37",
                "actionDesc": "Failed of passage in Senate",
                "chamber": "Senate",
            },
            {
                "versionCode": "38",
                "actionDesc": "Failed of passage in House",
                "chamber": "House",
            },
            {
                "versionCode": "39",
                "actionDesc": "Senate agreed to House amendment with amendment",
                "chamber": "Senate",
            },
            {
                "versionCode": "40",
                "actionDesc": "House agreed to Senate amendment with amendment",
                "chamber": "House",
            },
            {
                "versionCode": "43",
                "actionDesc": "Senate disagreed to House amendment",
                "chamber": "Senate",
            },
            {
                "versionCode": "44",
                "actionDesc": "House disagreed to Senate amendment",
                "chamber": "House",
            },
            {
                "versionCode": "45",
                "actionDesc": "Senate receded and concurred with amendment",
                "chamber": "Senate",
            },
            {
                "versionCode": "46",
                "actionDesc": "House receded and concurred with amendment",
                "chamber": "House",
            },
            {
                "versionCode": "47",
                "actionDesc": "Conference report filed in Senate",
                "chamber": "Senate",
            },
            {
                "versionCode": "48",
                "actionDesc": "Conference report filed in House",
                "chamber": "House",
            },
            {"versionCode": "49", "actionDesc": "Public Law", "chamber": None},
            {
                "versionCode": "51",
                "actionDesc": "Line item veto by President",
                "chamber": None,
            },
            {
                "versionCode": "52",
                "actionDesc": "Passed Senate amended, 2nd occurrence",
                "chamber": "Senate",
            },
            {"versionCode": "53", "actionDesc": "Passed House", "chamber": "House"},
            {
                "versionCode": "54",
                "actionDesc": "Passed House, 2nd occurrence",
                "chamber": "House",
            },
            {"versionCode": "55", "actionDesc": "Passed Senate", "chamber": "Senate"},
            {
                "versionCode": "56",
                "actionDesc": "Senate vitiated passage of bill after amendment",
                "chamber": "Senate",
            },
            {
                "versionCode": "58",
                "actionDesc": "Motion to recommit bill as amended by Senate",
                "chamber": "Senate",
            },
            {
                "versionCode": "59",
                "actionDesc": "House agreed to Senate amendment",
                "chamber": "House",
            },
            {
                "versionCode": "60",
                "actionDesc": "Senate agreed to House amendment with amendment, 2nd occurrence",
                "chamber": "Senate",
            },
            {
                "versionCode": "62",
                "actionDesc": "House agreed to Senate amendment with amendment, 2nd occurrence",
                "chamber": "House",
            },
            {
                "versionCode": "66",
                "actionDesc": "House receded and concurred with amendment, 2nd occurrence",
                "chamber": "House",
            },
            {
                "versionCode": "70",
                "actionDesc": "House agreed to Senate amendment without amendment",
                "chamber": "House",
            },
            {
                "versionCode": "71",
                "actionDesc": "Senate agreed to House amendment without amendment",
                "chamber": "Senate",
            },
            {
                "versionCode": "74",
                "actionDesc": "Senate agreed to House amendment",
                "chamber": "Senate",
            },
            {
                "versionCode": "77",
                "actionDesc": "Discharged from House committee",
                "chamber": "House",
            },
            {
                "versionCode": "78",
                "actionDesc": "Discharged from Senate committee",
                "chamber": "Senate",
            },
            {
                "versionCode": "79",
                "actionDesc": "Reported to House without amendment",
                "chamber": "House",
            },
            {
                "versionCode": "80",
                "actionDesc": "Reported to Senate without amendment",
                "chamber": "Senate",
            },
            {
                "versionCode": "81",
                "actionDesc": "Passed House without amendment",
                "chamber": "House",
            },
            {
                "versionCode": "82",
                "actionDesc": "Passed Senate without amendment",
                "chamber": "Senate",
            },
            {
                "versionCode": "83",
                "actionDesc": "Conference report filed in Senate, 2nd conference report",
                "chamber": "Senate",
            },
            {
                "versionCode": "86",
                "actionDesc": "Conference report filed in House, 2nd conference report",
                "chamber": "House",
            },
            {
                "versionCode": "87",
                "actionDesc": "Conference report filed in House, 3rd conference report",
                "chamber": "House",
            },
        )
        if not code:
            return version_codes
        else:
            for version_code in version_codes:
                if version_code["versionCode"] == code:
                    return version_code
            return None

@dataclass
class Subject:
    data: Dict
    _id_package_init: Tuple

    def __post_init__(self):
        setattr(self, self._id_package_init[0], self._id_package_init[1])

    @property
    def subject(self) -> Optional[str]:
        return self.data.get("name")

    @property
    def updated_at(self) -> Optional[str]:
        return self.data.get("updateDate")

    def __str__(self):
        return f"Subject: {self.subject}"

    def __repr__(self):
        return self.__str__()

@dataclass
class Title:
    data: Dict
    _id_package_init: Tuple

    def __post_init__(self):
        if "titles" in self.data and isinstance(self.data, dict):
            self.data = self.data.get("titles")

        setattr(self, self._id_package_init[0], self._id_package_init[1])

    def __str__(self):
        return self.title if self.title else "No title available"

    def __repr__(self):
        return self.__str__()

    @property
    def title(self) -> Optional[str]:
        return self.data.get("title")

    @property
    def title_type_code(self) -> Optional[str]:
        return self.data.get("titleTypeCode")

    @property
    def title_type(self) -> Optional[str]:
        return self.data.get("titleType") or self.title_type_code_table(
            code=self.title_type_code
        )

    @property
    def chamber_code(self) -> Optional[str]:
        return self.data.get("chamberCode")

    @property
    def chamber(self) -> Optional[str]:
        return self.data.get("chamberName")

    @property
    def bill_text_version_code(self) -> Optional[str]:
        return self.data.get("billTextVersionCode")

    @property
    def bill_text_version_name(self) -> Optional[str]:
        return self.data.get("billTextVersionName")

    @staticmethod
    def title_type_code_table(code: Optional[str] = None):
        title_types = (
            {"titleTypeCode": 6, "description": "Official Title as Introduced"},
            {"titleTypeCode": 7, "description": "Official Titles as Amended by House"},
            {"titleTypeCode": 8, "description": "Official Titles as Amended by Senate"},
            {
                "titleTypeCode": 9,
                "description": "Official Title as Agreed to by House and Senate",
            },
            {"titleTypeCode": 14, "description": "Short Titles as Introduced"},
            {"titleTypeCode": 17, "description": "Short Titles as Passed House"},
            {"titleTypeCode": 18, "description": "Short Titles as Passed Senate"},
            {"titleTypeCode": 19, "description": "Short Titles as Enacted"},
            {
                "titleTypeCode": 22,
                "description": "Short Titles as Introduced for portions of this bill",
            },
            {
                "titleTypeCode": 23,
                "description": "Short Titles as Reported to House for portions of this bill",
            },
            {
                "titleTypeCode": 24,
                "description": "Short Titles as Reported to Senate for portions of this bill",
            },
            {
                "titleTypeCode": 25,
                "description": "Short Titles as Passed House for portions of this bill",
            },
            {
                "titleTypeCode": 26,
                "description": "Short Titles as Passed Senate for portions of this bill",
            },
            {
                "titleTypeCode": 27,
                "description": "Short Titles as Enacted for portions of this bill",
            },
            {"titleTypeCode": 30, "description": "Popular Title"},
            {"titleTypeCode": 45, "description": "Display Title"},
            {"titleTypeCode": 101, "description": "Short Title(s) as Introduced"},
            {
                "titleTypeCode": 102,
                "description": "Short Title(s) as Reported to House",
            },
            {
                "titleTypeCode": 103,
                "description": "Short Title(s) as Reported to Senate",
            },
            {"titleTypeCode": 104, "description": "Short Title(s) as Passed House"},
            {"titleTypeCode": 105, "description": "Short Title(s) as Passed Senate"},
            {
                "titleTypeCode": 106,
                "description": "Short Title(s) as Introduced for portions of this bill",
            },
            {
                "titleTypeCode": 107,
                "description": "Short Title(s) as Reported to House for portions of this bill",
            },
            {
                "titleTypeCode": 108,
                "description": "Short Title(s) as Reported to Senate for portions of this bill",
            },
            {
                "titleTypeCode": 109,
                "description": "Short Title(s) as Passed House for portions of this bill",
            },
            {
                "titleTypeCode": 110,
                "description": "Short Title(s) as Passed Senate for portions of this bill",
            },
            {
                "titleTypeCode": 147,
                "description": "Short Title(s) from ENR (Enrolled) bill text",
            },
            {
                "titleTypeCode": 250,
                "description": "Short Title(s) from Engrossed Amendment Senate",
            },
            {
                "titleTypeCode": 253,
                "description": "Short Title(s) from Engrossed Amendment House for portions of this bill",
            },
            {
                "titleTypeCode": 254,
                "description": "Short Title(s) from Engrossed Amendment Senate for portions of this bill",
            },
        )
        if not code:
            return title_types
        else:
            for title_type in title_types:
                if str(title_type["titleTypeCode"]) == str(code):
                    return title_type["description"]
            return None

@dataclass
class CommitteeActivity:
    data: Dict
    _id_package_init: Optional[Tuple[str, str]] = None

    def __post_init__(self):
        if self._id_package_init:
            setattr(self, self._id_package_init[0], self._id_package_init[1])

    @property
    def activity_date(self) -> Optional[str]:
        return self.data.get("activity_date")

    @property
    def activity_name(self) -> Optional[str]:
        return self.data.get("activity_name")

    @property
    def committee(self) -> Optional[str]:
        return self.data.get("committee")

    @property
    def committee_code(self) -> Optional[str]:
        return self.data.get("committee_code")

    @property
    def chamber(self) -> Optional[str]:
        return self.data.get("chamber")

    @property
    def subcommittee(self) -> Optional[str]:
        return self.data.get("subcommittee")

    @property
    def subcommittee_code(self) -> Optional[str]:
        return self.data.get("subcommittee_code")

    def __str__(self):
        return f"{self.activity_name} - {self.committee} ({self.activity_date})"

    def __repr__(self):
        return self.__str__()

@dataclass
class CommitteeHistory:
    data: Dict
    _id_package_init: Tuple

    def __post_init__(self):
        setattr(self, self._id_package_init[0], self._id_package_init[1])

    def __str__(self):
        return f"CommitteeHistory: {self.official_name or self.loc_name}"

    def __repr__(self):
        return self.__str__()

    @property
    def loc_name(self) -> Optional[str]:
        return self.data.get("libraryOfCongressName")

    @property
    def official_name(self) -> Optional[str]:
        return self.data.get("officialName")

    @property
    def start_date(self) -> Optional[str]:
        return self.data.get("startDate")

    @property
    def updated_at(self) -> Optional[str]:
        return self.data.get("updateDate")

    @property
    def end_date(self) -> Optional[str]:
        return self.data.get("endDate")

    @property
    def committee_type_code(self) -> Optional[str]:
        return self.data.get("committeeTypeCode")

    @property
    def establishing_authority(self) -> Optional[str]:
        return self.data.get("establishingAuthority")

    @property
    def loc_linked_data_id(self) -> Optional[str]:
        return self.data.get("locLinkedDataId")

    @property
    def superintendent_document_number(self) -> Optional[str]:
        return self.data.get("superintendentDocumentNumber")

    @property
    def nara_id(self) -> Optional[str]:
        return self.data.get("naraId")

@dataclass
class Nominee:
    data: Dict
    _id_package_init: Tuple

    def __post_init__(self):
        setattr(self, self._id_package_init[0], self._id_package_init[1])
        if "nominees" in self.data and isinstance(self.data, dict):
            self.data = self.data.get("nominees", None)

    def __str__(self):
        return f"{self.first_name} {self.last_name}"

    def __repr__(self):
        return self.__str__()

    @property
    def first_name(self) -> Optional[str]:
        return self.data.get("firstName")

    @property
    def last_name(self) -> Optional[str]:
        return self.data.get("lastName")

    @property
    def middle_name(self) -> Optional[str]:
        return self.data.get("middleName")

    @property
    def ordinal(self) -> Optional[str]:
        return self.data.get("ordinal")

    @property
    def prefix(self) -> Optional[str]:
        return self.data.get("prefix")

    @property
    def suffix(self) -> Optional[str]:
        return self.data.get("suffix")

    @property
    def state(self) -> Optional[str]:
        return self.data.get("state")

    @property
    def effective_date(self) -> Optional[str]:
        return self.data.get("effectiveDate")

    @property
    def predecessor_name(self) -> Optional[str]:
        return self.data.get("predecessorName")

    @property
    def corps_code(self) -> Optional[str]:
        return self.data.get("corpsCode")

@dataclass
class NominationPosition:
    data: Dict
    _id_package_init: Tuple

    def __post_init__(self):
        setattr(self, self._id_package_init[0], self._id_package_init[1])

    def __str__(self):
        return f"{self.position_title} at {self.organization}"

    def __repr__(self):
        return self.__str__()

    @property
    def ordinal(self) -> Optional[str]:
        return self.data.get("ordinal")

    @property
    def intro_text(self) -> Optional[str]:
        return self.data.get("introText")

    @property
    def position_title(self) -> Optional[str]:
        return self.data.get("positionTitle")

    @property
    def organization(self) -> Optional[str]:
        return self.data.get("organization")

    @property
    def nominee_count(self) -> Optional[int]:
        return self.data.get("nomineeCount")

@dataclass
class Witness:
    data: Dict
    _id_package_init: Tuple

    def __post_init__(self):
        setattr(self, self._id_package_init[0], self._id_package_init[1])

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.__str__()

    @property
    def name(self) -> Optional[str]:
        return self.data.get("name")

    @property
    def position(self) -> Optional[str]:
        return self.data.get("position")

    @property
    def organization(self) -> Optional[str]:
        return self.data.get("organization")

@dataclass
class MemberTerm:
    data: dict
    _id_package_init: Tuple

    def __post_init__(self):
        setattr(self, self._id_package_init[0], self._id_package_init[1])

    @property
    def member_type(self) -> Optional[str]:
        return self.data.get("memberType")

    @property
    def congress(self) -> Optional[int]:
        return int(self.data.get("congress", '')) if self.data.get("congress") is not None else None

    @property
    def chamber(self) -> Optional[str]:
        return self.data.get("chamber")

    @property
    def state_code(self) -> Optional[str]:
        return self.data.get("stateCode", [None])

    @property
    def state_name(self) -> Optional[str]:
        return self.data.get("stateName")

    @property
    def start_year(self) -> Optional[str]:
        return self.data.get("startYear")

    @property
    def end_year(self) -> Optional[str]:
        return self.data.get("endYear")

    @property
    def district(self) -> Optional[str]:
        return self.data.get("district")

    def __str__(self):
        return f"{self.member_type} - {self.congress} Congress ({self.start_year}-{self.end_year})"

    def __repr__(self):
        return self.__str__()

@dataclass
class PartyHistory:
    data: dict
    _id_package_init: Tuple

    def __post_init__(self):
        setattr(self, self._id_package_init[0], self._id_package_init[1])

    @property
    def party_code(self) -> Optional[str]:
        return self.data.get("partyAbbreviation")

    @property
    def party_name(self) -> Optional[str]:
        return self.data.get("partyName")

    @property
    def start_year(self) -> Optional[str]:
        return self.data.get("startYear")

    @property
    def end_year(self) -> Optional[str]:
        return self.data.get("endYear")

    def __str__(self):
        return f"{self.party_name} ({self.party_code}): {self.start_year}-{self.end_year or 'present'}"

    def __repr__(self):
        return self.__str__()

@dataclass
class LeadershipRole:
    data: dict
    _id_package_init: Tuple
    _terms: Optional[List[MemberTerm]] = field(default_factory=list)

    def __post_init__(self):
        setattr(self, self._id_package_init[0], self._id_package_init[1])
        self._terms = self._terms or []
        self._congress = self.data.get("congress")

    @property
    def type(self) -> Optional[str]:
        return self.data.get("type")

    @property
    def congress(self) -> Optional[int]:
        return self._congress

    @property
    def chamber(self) -> Optional[str]:
        matching_terms = [term.chamber for term in self._terms if term.congress == self._congress]
        return matching_terms[0] if matching_terms else None

    @property
    def is_current(self) -> bool:
        return bool(getattr(self, "current", None))

    def __str__(self):
        chambers = ", ".join(self.chamber) if self.chamber else "Unknown"
        return f"{self.type} - {self.congress} Congress ({chambers})"

    def __repr__(self):
        return self.__str__()

@dataclass
class Session:
    data: Dict
    _id_package_init: Tuple

    def __post_init__(self):
        setattr(self, self._id_package_init[0], self._id_package_init[1])

    def __str__(self):
        return f"Session {self.session_number} ({self.type})"

    def __repr__(self):
        return self.__str__()

    @property
    def session_number(self) -> Optional[str]:
        return self.data.get("number")

    @property
    def chamber(self) -> Optional[str]:
        return self.data.get("chamber")

    @property
    def type(self) -> Optional[str]:
        return self.data.get("type")

    @property
    def start_date(self) -> Optional[str]:
        return self.data.get("startDate")

    @property
    def end_date(self) -> Optional[str]:
        return self.data.get("endDate")

@dataclass
class Law:
    data: Dict
    _id_package_init: Tuple

    def __post_init__(self):
        setattr(self, self._id_package_init[0], self._id_package_init[1])

    @property
    def law_number(self) -> Optional[str]:
        return self.data.get("number", None)

    @property
    def law_type(self) -> Optional[str]:
        return self.data.get("type", None)
