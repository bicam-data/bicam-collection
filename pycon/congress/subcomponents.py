import hashlib
from dataclasses import dataclass, field


@dataclass
class RecordedVotes:
    data: dict
    _id_package_init: tuple
    action_id: str

    def __post_init__(self):
        setattr(self, self._id_package_init[0], self._id_package_init[1])

    def __str__(self):
        return f"Recorded Votes: {self.roll_number} - {self.date}"

    def __repr__(self):
        return self.__str__()

    @property
    def chamber(self) -> str | None:
        return self.data.get("chamber")

    @property
    def congress(self) -> str | None:
        return self.data.get("congress")

    @property
    def date(self) -> str | None:
        return self.data.get("date")

    @property
    def roll_number(self) -> str | None:
        return self.data.get("rollNumber")

    @property
    def session_number(self) -> str | None:
        return self.data.get("sessionNumber")

    @property
    def url(self) -> str | None:
        return self.data.get("url")


@dataclass
class CBOCostEstimate:
    data: dict
    _id_package_init: tuple

    def __post_init__(self):
        setattr(self, self._id_package_init[0], self._id_package_init[1])

    def __str__(self):
        return f"CBO Cost Estimate: {self.title} - {self.pub_date}"

    def __repr__(self):
        return self.__str__()

    @property
    def description(self) -> str | None:
        return self.data.get("description")

    @property
    def pub_date(self) -> str | None:
        return self.data.get("pubDate")

    @property
    def title(self) -> str | None:
        return self.data.get("title")

    @property
    def url(self) -> str | None:
        return self.data.get("url")


@dataclass
class Summary:
    data: dict
    _id_package_init: tuple

    def __post_init__(self):
        if "summaries" in self.data and isinstance(self.data, dict):
            self.data = self.data.get("summaries")

        setattr(self, self._id_package_init[0], self._id_package_init[1])

    def __str__(self):
        return str(self.text) if self.text else "No text available"

    def __repr__(self):
        return self.__str__()

    @property
    def action_date(self) -> str | None:
        return self.data.get("actionDate")

    @property
    def action_desc(self) -> str | None:
        return self.data.get("actionDesc")

    @property
    def text(self) -> str | None:
        return self.data.get("text")

    @property
    def updated_at(self) -> str | None:
        return self.data.get("update_date")

    @property
    def version_code(self) -> str | None:
        return self.data.get("versionCode")

    @staticmethod
    def version_code_table(code: str | None = None):
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
    data: dict
    _id_package_init: tuple

    def __post_init__(self):
        setattr(self, self._id_package_init[0], self._id_package_init[1])

    @property
    def subject(self) -> str | None:
        return self.data.get("name")

    @property
    def updated_at(self) -> str | None:
        return self.data.get("updateDate")

    def __str__(self):
        return f"Subject: {self.subject}"

    def __repr__(self):
        return self.__str__()


@dataclass
class Title:
    data: dict
    _id_package_init: tuple

    def __post_init__(self):
        if "titles" in self.data and isinstance(self.data, dict):
            self.data = self.data.get("titles")

        setattr(self, self._id_package_init[0], self._id_package_init[1])

    def __str__(self):
        return self.title if self.title else "No title available"

    def __repr__(self):
        return self.__str__()

    @property
    def title(self) -> str | None:
        return self.data.get("title")

    @property
    def title_type_code(self) -> str | None:
        return self.data.get("titleTypeCode")

    @property
    def title_type(self) -> str | None:
        return self.data.get("titleType") or self.title_type_code_table(
            code=self.title_type_code
        )

    @property
    def chamber_code(self) -> str | None:
        return self.data.get("chamberCode")

    @property
    def chamber(self) -> str | None:
        return self.data.get("chamberName")

    @property
    def bill_text_version_code(self) -> str | None:
        return self.data.get("billTextVersionCode")

    @property
    def bill_text_version_name(self) -> str | None:
        return self.data.get("billTextVersionName")

    @staticmethod
    def title_type_code_table(code: str | None = None):
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
    data: dict
    activity_name: str
    activity_date: str
    _id_package_init: tuple[str, str] | None = None

    def __post_init__(self):
        if self._id_package_init:
            setattr(self, self._id_package_init[0], self._id_package_init[1])

    @property
    def committee_name(self) -> str | None:
        return self.data.get("name")

    @property
    def committee_code(self) -> str | None:
        return self.data.get("systemCode")

    @property
    def chamber(self) -> str | None:
        return self.data.get("chamber")

    @property
    def committee_type(self) -> str | None:
        return self.data.get("type")

    def __str__(self):
        return f"{self.activity_name} - {self.committee_name} ({self.activity_date})"

    def __repr__(self):
        return self.__str__()


@dataclass
class CommitteeHistory:
    data: dict
    _id_package_init: tuple

    def __post_init__(self):
        setattr(self, self._id_package_init[0], self._id_package_init[1])

    def __str__(self):
        return f"CommitteeHistory: {self.official_name or self.loc_name}"

    def __repr__(self):
        return self.__str__()

    @property
    def loc_name(self) -> str | None:
        return self.data.get("libraryOfCongressName")

    @property
    def official_name(self) -> str | None:
        return self.data.get("officialName")

    @property
    def start_date(self) -> str | None:
        return self.data.get("startDate")

    @property
    def updated_at(self) -> str | None:
        return self.data.get("updateDate")

    @property
    def end_date(self) -> str | None:
        return self.data.get("endDate")

    @property
    def committee_type_code(self) -> str | None:
        return self.data.get("committeeTypeCode")

    @property
    def establishing_authority(self) -> str | None:
        return self.data.get("establishingAuthority")

    @property
    def loc_linked_data_id(self) -> str | None:
        return self.data.get("locLinkedDataId")

    @property
    def superintendent_document_number(self) -> str | None:
        return self.data.get("superintendentDocumentNumber")

    @property
    def nara_id(self) -> str | None:
        return self.data.get("naraId")


@dataclass
class Nominee:
    data: dict
    _id_package_init: tuple

    def __post_init__(self):
        setattr(self, self._id_package_init[0], self._id_package_init[1])
        if "nominees" in self.data and isinstance(self.data, dict):
            self.data = self.data.get("nominees", None)

    def __str__(self):
        return f"{self.first_name} {self.last_name}"

    def __repr__(self):
        return self.__str__()

    @property
    def first_name(self) -> str | None:
        return self.data.get("firstName")

    @property
    def last_name(self) -> str | None:
        return self.data.get("lastName")

    @property
    def middle_name(self) -> str | None:
        return self.data.get("middleName")

    @property
    def nominee_id(self) -> str | None:
        return self.data.get("ordinal")

    @property
    def prefix(self) -> str | None:
        return self.data.get("prefix")

    @property
    def suffix(self) -> str | None:
        return self.data.get("suffix")

    @property
    def state(self) -> str | None:
        return self.data.get("state")

    @property
    def effective_date(self) -> str | None:
        return self.data.get("effectiveDate")

    @property
    def predecessor_name(self) -> str | None:
        return self.data.get("predecessorName")

    @property
    def corps_code(self) -> str | None:
        return self.data.get("corpsCode")


@dataclass
class NominationPosition:
    data: dict
    _id_package_init: tuple

    def __post_init__(self):
        setattr(self, self._id_package_init[0], self._id_package_init[1])

    def __str__(self):
        return f"{self.position_title} at {self.organization}"

    def __repr__(self):
        return self.__str__()

    @property
    def nominee_id(self) -> str | None:
        return self.data.get("ordinal")

    @property
    def intro_text(self) -> str | None:
        return self.data.get("introText")

    @property
    def position_title(self) -> str | None:
        return self.data.get("positionTitle")

    @property
    def organization(self) -> str | None:
        return self.data.get("organization")

    @property
    def nominee_count(self) -> int | None:
        return self.data.get("nomineeCount")


@dataclass
class Witness:
    data: dict
    _id_package_init: tuple

    def __post_init__(self):
        setattr(self, self._id_package_init[0], self._id_package_init[1])

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.__str__()

    @property
    def name(self) -> str | None:
        return self.data.get("name")

    @property
    def position(self) -> str | None:
        return self.data.get("position")

    @property
    def organization(self) -> str | None:
        return self.data.get("organization")


@dataclass
class MemberTerm:
    data: dict
    _id_package_init: tuple

    def __post_init__(self):
        setattr(self, self._id_package_init[0], self._id_package_init[1])

    @property
    def member_type(self) -> str | None:
        return self.data.get("memberType")

    @property
    def congress(self) -> int | None:
        return (
            int(self.data.get("congress", ""))
            if self.data.get("congress") is not None
            else None
        )

    @property
    def chamber(self) -> str | None:
        return self.data.get("chamber")

    @property
    def state_code(self) -> str | None:
        return self.data.get("stateCode", [None])

    @property
    def state_name(self) -> str | None:
        return self.data.get("stateName")

    @property
    def start_year(self) -> str | None:
        return self.data.get("startYear")

    @property
    def end_year(self) -> str | None:
        return self.data.get("endYear")

    @property
    def district(self) -> str | None:
        return self.data.get("district")

    def __str__(self):
        return f"{self.member_type} - {self.congress} Congress ({self.start_year}-{self.end_year})"

    def __repr__(self):
        return self.__str__()


@dataclass
class PartyHistory:
    data: dict
    _id_package_init: tuple

    def __post_init__(self):
        setattr(self, self._id_package_init[0], self._id_package_init[1])

    @property
    def party_code(self) -> str | None:
        return self.data.get("partyAbbreviation")

    @property
    def party_name(self) -> str | None:
        return self.data.get("partyName")

    @property
    def start_year(self) -> str | None:
        return self.data.get("startYear")

    @property
    def end_year(self) -> str | None:
        return self.data.get("endYear")

    def __str__(self):
        return f"{self.party_name} ({self.party_code}): {self.start_year}-{self.end_year or 'present'}"

    def __repr__(self):
        return self.__str__()


@dataclass
class LeadershipRole:
    data: dict
    _id_package_init: tuple
    _terms: list[MemberTerm] | None = field(default_factory=list)

    def __post_init__(self):
        setattr(self, self._id_package_init[0], self._id_package_init[1])
        self._terms = self._terms or []
        self._congress = self.data.get("congress")

    @property
    def type(self) -> str | None:
        return self.data.get("type")

    @property
    def congress(self) -> int | None:
        return self._congress

    @property
    def chamber(self) -> str | None:
        matching_terms = [
            term.chamber for term in self._terms if term.congress == self._congress
        ]
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
    data: dict
    _id_package_init: tuple

    def __post_init__(self):
        setattr(self, self._id_package_init[0], self._id_package_init[1])

    def __str__(self):
        return f"Session {self.session_number} ({self.type})"

    def __repr__(self):
        return self.__str__()

    @property
    def session_number(self) -> str | None:
        return self.data.get("number")

    @property
    def chamber(self) -> str | None:
        return self.data.get("chamber")

    @property
    def type(self) -> str | None:
        return self.data.get("type")

    @property
    def start_date(self) -> str | None:
        return self.data.get("startDate")

    @property
    def end_date(self) -> str | None:
        return self.data.get("endDate")


@dataclass
class Law:
    data: dict
    _id_package_init: tuple

    def __post_init__(self):
        setattr(self, self._id_package_init[0], self._id_package_init[1])
        self._parse_congress_number()
        self._parse_law_type()
        self._parse_law_id()

    def __str__(self):
        return f"{self.law_number} - {self.law_type}"

    def __repr__(self):
        return self.__str__()

    def _parse_congress_number(self):
        number = self.data.get("number", None)
        self._congress, self._law_number = number.split("-") if number else None

    def _parse_law_type(self):
        type = self.data.get("type", None)
        self._law_type = type.split(" ")[0].lower() if type else None

    def _parse_law_id(self):
        self._law_id = f"PL{self._law_number}"

    @property
    def law_number(self) -> str | None:
        return self._law_number

    @property
    def law_type(self) -> str | None:
        return self._law_type

    @property
    def congress(self) -> str | None:
        return self._congress

    @property
    def law_id(self) -> str | None:
        return self._law_id


@dataclass
class RelatedBill:
    data: dict
    _id_package_init: tuple

    def __post_init__(self):
        setattr(self, self._id_package_init[0], self._id_package_init[1])
        self._parse_relatedbill_id()

    def _parse_relatedbill_id(self):
        bill_number = getattr(self, "number", None)
        if "½" in bill_number:
            bill_number = bill_number.replace("½", ".5")  # replace half with .5
        congress = getattr(self, "congress", None)
        bill_type = getattr(self, "type", None).lower()

        self._relatedbill_id = f"{bill_type}{bill_number}-{congress}"
        if None in [bill_number, congress, bill_type]:
            self._id_error = True

    @property
    def relatedbill_id(self) -> str | None:
        return self._relatedbill_id

    @property
    def relationships(self) -> list[dict] | None:
        return self.data.get("relationshipDetails", None)


class Note:
    data: dict
    _id_package_init: tuple

    def __post_init__(self):
        setattr(self, self._id_package_init[0], self._id_package_init[1])
        self._parse_note_id()

    def __str__(self):
        return f"{self.note_id} - {self.note_text}"

    def __repr__(self):
        return self.__str__()

    def _parse_note_id(self):
        # create a deterministic id from hashing note text and bill_id
        note_text = self.data.get("text", None)
        # 8 hex chars = 32 bits = 4 billion possible combinations, sufficient for notes
        if None in [note_text, self.bill_id]:
            self._id_error = True
        else:
            self._note_id = hashlib.sha256(
                f"{note_text}{self.bill_id}".encode()
            ).hexdigest()[:8]

    @property
    def bill_id(self) -> str | None:
        return self.data.get("billId", None)

    @property
    def note_id(self) -> str | None:
        return self._note_id

    @property
    def note_text(self) -> str | None:
        return self.data.get("text", None)


class NoteLink:
    note_id: str
    data: dict
    _id_package_init: tuple

    def __post_init__(self):
        setattr(self, self._id_package_init[0], self._id_package_init[1])

    @property
    def link_name(self) -> str | None:
        return self.data.get("name")

    @property
    def url(self) -> str | None:
        return self.data.get("url")

    def __str__(self):
        title = self.link_name or "<no-text>"
        return f"{self.note_id} - {title}" if self.note_id else title

    def __repr__(self):
        return self.__str__()
