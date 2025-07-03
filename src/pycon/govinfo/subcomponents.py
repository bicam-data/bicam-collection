from dataclasses import dataclass


@dataclass
class ReferenceCode:
    package_id: str
    granule_id: str
    collection_name: str | None = None
    collection_code: str | None = None
    title: str | None = None
    label: str | None = None
    sections: str = ""


    def __str__(self):
        return f"{self.title} {self.label}"

    def __repr__(self):
        return self.__str__()

@dataclass
class ReferenceStatute:
    package_id: str
    granule_id: str
    collection_name: str | None = None
    collection_code: str | None = None
    title: str | None = None
    label: str | None = None
    pages: str = ""

    def __str__(self):
        return f"{self.title} {self.label}"

    def __repr__(self):
        return self.__str__()

@dataclass
class ReferenceLaw:
    package_id: str
    granule_id: str
    collection_name: str | None = None
    collection_code: str | None = None
    label: str | None = None
    congress: str | None = None
    number: str | None = None

    def __post_init__(self):
        self.congress = int(self.congress) if all([c.isdigit() for c in self.congress]) else self.congress
        self.number = int(self.number) if all([c.isdigit() for c in self.number]) else self.number
        self.law_id = f"{self.label}{self.congress}-{self.number}"

    def __str__(self):
        return self.law_id

    def __repr__(self):
        return self.__str__()

@dataclass
class ReferenceBill:
    package_id: str
    granule_id: str
    collection_name: str | None = None
    collection_code: str | None = None
    number: str | None = None
    congress: str | None = None
    type: str | None = None

    def __post_init__(self):
        self.congress = int(self.congress) if all([c.isdigit() for c in self.congress]) else self.congress
        self.number = int(self.number) if all([c.isdigit() for c in self.number]) else self.number
        self.type = self.type.lower()
        self.bill_id = f"{self.type}{self.number}-{self.congress}"

    def __str__(self):
        return self.bill_id

    def __repr__(self):
        return self.__str__()

@dataclass
class Reference:
    collection_name: str | None = None
    collection_code: str | None = None
    package_id: str | None = None
    granule_id: str | None = None
    content: list[ReferenceCode | ReferenceStatute | ReferenceLaw | ReferenceBill] | None = None

    def __post_init__(self):
        if self.content is not None:
            for k, v in self.content.items():
                setattr(self, k, v)

    def __str__(self):
        return f"{self.collection_name} {self.collection_code} {self.package_id}"

    def __repr__(self):
        return self.__str__()

@dataclass
class ReportRole:
    package_id: str
    granule_id: str
    role: str | None = None
    bioguide_id: str | None = None
    name: str | None = None


    def __str__(self):
        return f"{self.role} {self.bioguide_id} {self.package_id} {self.granule_id}"

    def __repr__(self):
        return self.__str__()

@dataclass
class SerialSet:
    package_id: str
    bag_id: str | None = None
    doc_id: str | None = None
    serial_set_number: str | None = None
    is_glp: bool | None = None

    @property
    def txt_url(self) -> str | None:
        return getattr(self, "_txt_link", None)

@dataclass
class ShortTitle:
    package_id: str
    title: str | None = None
    type: str | None = None
    level: str | None = None

    def __str__(self):
        return f"{self.title} {self.type} {self.level} {self.package_id}"

    def __repr__(self):
        return self.__str__()

@dataclass
class Committee:
    package_id: str
    granule_id: str
    committee_code: str | None = None
    chamber: str | None = None
    committee_name: str | None = None
    type: str | None = None

    def __str__(self):
        return f"{self.committee_code} {self.committee_name} {self.package_id} {self.granule_id}"

    def __repr__(self):
        return self.__str__()
