from dataclasses import dataclass
from typing import List, Optional, Union



@dataclass
class ReferenceCode:
    package_id: str
    granule_id: str
    collection_name: Optional[str] = None
    collection_code: Optional[str] = None
    title: Optional[str] = None
    label: Optional[str] = None
    sections: str = ""


    def __str__(self):
        return f"{self.title} {self.label}"

    def __repr__(self):
        return self.__str__()

@dataclass
class ReferenceStatute:
    package_id: str
    granule_id: str
    collection_name: Optional[str] = None
    collection_code: Optional[str] = None
    title: Optional[str] = None
    label: Optional[str] = None
    pages: str = ""

    def __str__(self):
        return f"{self.title} {self.label}"
    
    def __repr__(self):
        return self.__str__()

@dataclass
class ReferenceLaw:
    package_id: str
    granule_id: str
    collection_name: Optional[str] = None
    collection_code: Optional[str] = None
    label: Optional[str] = None
    congress: Optional[str] = None
    number: Optional[str] = None

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
    collection_name: Optional[str] = None
    collection_code: Optional[str] = None
    number: Optional[str] = None
    congress: Optional[str] = None
    type: Optional[str] = None

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
    collection_name: Optional[str] = None
    collection_code: Optional[str] = None
    package_id: Optional[str] = None
    granule_id: Optional[str] = None
    content: Optional[List[Union[ReferenceCode, ReferenceStatute, ReferenceLaw, ReferenceBill]]] = None

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
    role: Optional[str] = None
    bioguide_id: Optional[str] = None
    name: Optional[str] = None


    def __str__(self):
        return f"{self.role} {self.bioguide_id} {self.package_id} {self.granule_id}"

    def __repr__(self):
        return self.__str__()

@dataclass 
class SerialSet:
    package_id: str
    bag_id: Optional[str] = None
    doc_id: Optional[str] = None
    serial_set_number: Optional[str] = None
    is_glp: Optional[bool] = None

    @property
    def txt_url(self) -> Optional[str]:
        return getattr(self, "_txt_link", None)

@dataclass
class ShortTitle:
    package_id: str
    title: Optional[str] = None
    type: Optional[str] = None
    level: Optional[str] = None

    def __str__(self):
        return f"{self.title} {self.type} {self.level} {self.package_id}"

    def __repr__(self):
        return self.__str__()

@dataclass
class Committee:
    package_id: str
    granule_id: str
    committee_code: Optional[str] = None
    chamber: Optional[str] = None
    committee_name: Optional[str] = None
    type: Optional[str] = None

    def __str__(self):
        return f"{self.committee_code} {self.committee_name} {self.package_id} {self.granule_id}"

    def __repr__(self):
        return self.__str__()