
from typing import List, Optional, Dict, Tuple, Union
from dataclasses import dataclass

from pycon.adapter import RestAdapter
from pycon.exceptions import PyCongressException
from pycon.api_models import ErrorResult
from pycon.retriever_class import Retriever
from api_interface.pycon.pycon.govinfo.subcomponents import ReportRole, ReferenceCode, ReferenceStatute, ReferenceLaw, ReferenceBill, SerialSet, ShortTitle, Committee

import logging

logger = logging.getLogger(__name__)

logger.setLevel(logging.DEBUG)


@dataclass
class BillCollection(Retriever):
    data: Dict
    _pagination: Dict
    _adapter: RestAdapter

    def __init__(self, data: Dict, _pagination: Dict, _adapter: RestAdapter, _id_package_init: Optional[Tuple] = None):
        self.data = data
        self._pagination = _pagination
        super().__init__(data=self.data, _adapter=_adapter)

        self._parse_data()
        self._post_init_processing()

    def _post_init_processing(self):

        self._bill_id = f"{self.data.get('billType')}{self.data.get('billNumber')}-{self.data.get('congress')}" if all([self.data.get('billType'), self.data.get('billNumber'), self.data.get('congress')]) else '-99'
        self._parse_is_private_and_appropriation()
        self._parse_pages()
        self._parse_references()
        self._parse_other_identifier()
        self._parse_download()
        self._parse_short_titles()
    def _parse_is_private_and_appropriation(self):
        self._is_private = getattr(self, '_is_private', '').lower() == 'true'
        self._is_appropriation = getattr(self, '_is_appropriation', '').lower() == 'true'

    def _parse_pages(self):
        if isinstance(getattr(self, '_pages', None), str) and getattr(self, '_pages', None).isdigit():
            self._pages = int(getattr(self, '_pages', None))

    def _parse_references(self):
        self._bill_references = []
        self._law_references = []
        self._statute_references = []
        self._code_references = []
        
        for ref in getattr(self, '_references', []):
            if ref.get('collectionCode') == "USCODE":
                for content in ref.get('contents', []):
                    self._code_references.append(ReferenceCode(
                        collection_name=ref.get('collectionName'),
                        collection_code=ref.get('collectionCode'),
                        package_id=getattr(self, '_package_id', None),
                        granule_id=getattr(self, '_granule_id', None),
                        title=content.get('title'),
                        label=content.get('label'),
                        sections=",".join(map(str, content.get('sections', [])))
                    ))
            elif ref.get('collectionCode') == "STATUTE":
                for content in ref.get('contents', []):
                    self._statute_references.append(ReferenceStatute(
                        collection_name=ref.get('collectionName'),
                        collection_code=ref.get('collectionCode'),
                        package_id=getattr(self, '_package_id', None),
                        granule_id=getattr(self, '_granule_id', None),
                        title=content.get('title'),
                        label=content.get('label'),
                        pages=",".join(map(str, content.get('pages', []))),
                    ))
            elif ref.get('collectionCode') == "PLAW":
                for content in ref.get('contents', []):
                    self._law_references.append(ReferenceLaw(
                        collection_name=ref.get('collectionName'),
                        collection_code=ref.get('collectionCode'),
                        package_id=getattr(self, '_package_id', None),
                        granule_id=getattr(self, '_granule_id', None),
                        number=content.get('number'),
                        congress=content.get('congress'),
                        label=content.get('label'),
                    ))
            elif ref.get('collectionCode') == "BILLS":
                for content in ref.get('contents', []):
                    self._bill_references.append(ReferenceBill(
                        collection_name=ref.get('collectionName'),
                        collection_code=ref.get('collectionCode'),
                        package_id=getattr(self, '_package_id', None),
                        granule_id=getattr(self, '_granule_id', None),
                        number=content.get('number'),
                        congress=content.get('congress'),
                        type=content.get('type')
                    ))

    def _parse_other_identifier(self):
        for key, value in getattr(self, '_other_identifier', {}).items():
            setattr(self, f"_{self._dash_to_snake(self._camel_to_snake(key))}", value)

    def _parse_download(self):
        for key, value in getattr(self, '_download', {}).items():
            setattr(self, f"_{self._dash_to_snake(self._camel_to_snake(key))}", value)

    def _parse_short_titles(self):
        self._short_titles = []
        for title in getattr(self, '_short_title', []):
            self._short_titles.append(ShortTitle(
                title=title.get('title'),
                type=title.get('type'),
                level=title.get('level'),
                package_id=getattr(self, '_package_id', None)
            ))

    @property
    def bill_id(self) -> Optional[str]:
        return getattr(self, '_bill_id', None)

    @property
    def package_id(self) -> Optional[str]:
        return getattr(self, '_package_id', None)

    @property
    def date_issued(self) -> Optional[str]:
        return getattr(self, '_date_issued', None)

    @property
    def last_modified(self) -> Optional[str]:
        return getattr(self, '_last_modified', None)

    @property
    def collection_code(self) -> Optional[str]:
        return getattr(self, '_collection_code', None)

    @property
    def collection_name(self) -> Optional[str]:
        return getattr(self, '_collection_name', None)

    @property
    def category(self) -> Optional[str]:
        return getattr(self, '_category', None)

    @property
    def doc_class(self) -> Optional[str]:
        return getattr(self, '_doc_class', None)

    @property
    def bill_version(self) -> Optional[str]:
        return getattr(self, '_bill_version', None)

    @property
    def origin_chamber(self) -> Optional[str]:
        return getattr(self, '_origin_chamber', None)

    @property
    def current_chamber(self) -> Optional[str]:
        return getattr(self, '_current_chamber', None)

    @property
    def is_private(self) -> Optional[bool]:
        return getattr(self, '_is_private', None)

    @property
    def is_appropriation(self) -> Optional[bool]:
        return getattr(self, '_is_appropriation', None)

    @property
    def government_author1(self) -> Optional[str]:
        return getattr(self, '_government_author1', None)

    @property
    def government_author2(self) -> Optional[str]:
        return getattr(self, '_government_author2', None)

    @property
    def publisher(self) -> Optional[str]:
        return getattr(self, '_publisher', None)

    @property
    def su_doc_class_number(self) -> Optional[str]:
        return getattr(self, '_su_doc_class_number', None)

    @property
    def pages(self) -> Optional[Union[int, str]]:
        return getattr(self, '_pages', None)

    @property
    def reference_bills(self) -> Optional[List[ReferenceBill]]:
        return getattr(self, '_bill_references', [])
    
    @property
    def reference_laws(self) -> Optional[List[ReferenceLaw]]:
        return getattr(self, '_law_references', [])
    
    @property
    def reference_statutes(self) -> Optional[List[ReferenceStatute]]:
        return getattr(self, '_statute_references', [])
    
    @property
    def reference_codes(self) -> Optional[List[ReferenceCode]]:
        return getattr(self, '_code_references', [])

    @property
    def migrated_doc_id(self) -> Optional[str]:
        return getattr(self, "_migrated_doc_id", None)

    @property
    def parent_ils_system_id(self) -> Optional[str]:
        return getattr(self, "_parent_ils_system_id", None)

    @property
    def child_ils_title(self) -> Optional[str]:
        return getattr(self, "_child_ils_title", None)

    @property
    def parent_ils_title(self) -> Optional[str]:
        return getattr(self, "_parent_ils_title", None)

    @property
    def child_ils_system_id(self) -> Optional[str]:
        return getattr(self, "_child_ils_system_id", None)

    @property
    def stock_number(self) -> Optional[str]:
        return getattr(self, "_stock_number", None)
    
    @property
    def package_url(self) -> Optional[str]:
        return getattr(self, "_package_link", None)

    @property
    def details_url(self) -> Optional[str]:
        return getattr(self, "_details_link", None)

    @property
    def premis_url(self) -> Optional[str]:
        return getattr(self, "_premis_link", None)

    @property
    def xml_url(self) -> Optional[str]:
        return getattr(self, "_xml_link", None)

    @property
    def pdf_url(self) -> Optional[str]:
        return getattr(self, "_pdf_link", None)

    @property
    def mods_url(self) -> Optional[str]:
        return getattr(self, "_mods_link", None)

    @property
    def txt_url(self) -> Optional[str]:
        return getattr(self, "_txt_link", None)

    @property
    def zip_url(self) -> Optional[str]:
        return getattr(self, "_zip_link", None)

    @property
    def short_titles(self) -> Optional[List[ShortTitle]]:
        return getattr(self, '_short_titles', [])

@dataclass
class CongressionalDirectoryPackage(Retriever):
    data: Dict
    _pagination: Dict
    _adapter: RestAdapter

    def __init__(self, data: Dict, _pagination: Dict, _adapter: RestAdapter, _id_package_init: Optional[Tuple] = None):
        self.data = data
        self._pagination = _pagination
        super().__init__(data=self.data, _adapter=_adapter)

        self._parse_data()
        self._post_init_processing()

    def _post_init_processing(self):

        self._parse_download()
        self._parse_other_identifier()

    def _parse_download(self):
        self._download = getattr(self, '_download', {})
        for item in ["premisLink", "zipLink", "modsLink", "pdfLink", "txtLink", "xmlLink"]:
            setattr(self, f"_{item}", self._download.get(item))

    def _parse_other_identifier(self):
        self._other_identifier = getattr(self, '_other_identifier', {})
        for key, value in self._other_identifier.items():
            snake_key = self._dash_to_snake(self._camel_to_snake(key))
            setattr(self, f"_{snake_key}", value)

    @property
    def date_issued(self) -> Optional[str]:
        return getattr(self, '_date_issued', None)

    @property
    def document_type(self) -> Optional[str]:
        return getattr(self, '_document_type', None)

    @property
    def congress(self) -> Optional[str]:
        return getattr(self, '_congress', None)

    @property
    def package_id(self) -> Optional[str]:
        return getattr(self, '_package_id', None)

    @property
    def collection_code(self) -> Optional[str]:
        return getattr(self, '_collection_code', None)
    
    @property
    def title(self) -> Optional[str]:
        return getattr(self, '_title', None)

    @property
    def branch(self) -> Optional[str]:
        return getattr(self, '_branch', None)

    @property
    def collection_name(self) -> Optional[str]:
        return getattr(self, '_collection_name', None)

    @property
    def government_author1(self) -> Optional[str]:
        return getattr(self, '_government_author1', None)

    @property
    def government_author2(self) -> Optional[str]:
        return getattr(self, '_government_author2', None)

    @property
    def publisher(self) -> Optional[str]:
        return getattr(self, '_publisher', None)

    @property
    def su_doc_class_number(self) -> Optional[str]:
        return getattr(self, '_su_doc_class_number', None)

    @property
    def doc_class(self) -> Optional[str]:
        return getattr(self, '_doc_class', None)

    @property
    def last_modified(self) -> Optional[str]:
        return getattr(self, '_last_modified', None)

    @property
    def category(self) -> Optional[str]:
        return getattr(self, '_category', None)

    @property
    def migrated_doc_id(self) -> Optional[str]:
        return getattr(self, "_migrated_doc_id", None)

    @property
    def ils_system_id(self) -> Optional[str]:
        return getattr(self, "_ils_system_id", None)

    @property
    def isbn(self) -> Optional[List[str]]:
        return getattr(self, "_isbn", None)

    @property
    def premis_url(self) -> Optional[str]:
        return getattr(self, "_premis_link", None)

    @property
    def zip_url(self) -> Optional[str]:
        return getattr(self, "_zip_link", None)

    @property
    def mods_url(self) -> Optional[str]:
        return getattr(self, "_mods_link", None)

    @property
    def pdf_url(self) -> Optional[str]:
        return getattr(self, "_pdf_link", None)

    @property
    def txt_url(self) -> Optional[str]:
        return getattr(self, "_txt_link", None)

    @property
    def xml_url(self) -> Optional[str]:
        return getattr(self, "_xml_link", None)

    @property
    def package_url(self) -> Optional[str]:
        return getattr(self, "_package_link", None)

    @property
    def granules_url(self) -> Optional[str]:
        return getattr(self, "_granules_link", None)

    async def get_granules(self, verbose=True):
        if not self.granules_url:
            raise PyCongressException("No granules URL available for this Congressional Directory.")

        async for item in self._get_granules(self.granules_url, MemberGranule, verbose=verbose, granule_class="CONGRESSMEMBERSTATE"):
            if isinstance(item, ErrorResult):
                self._adapter._logger.error(f"Error retrieving granule: {item.error_message}")
                continue

            if item.title.strip().endswith("Delegation"):
                continue

            yield item


    async def get_all_granules(self, verbose=True):
        try:
            async for item in self._get_items("granules_url", MemberGranule, verbose=verbose):
                if getattr(item, "data", {}) != {}:
                    if getattr(item, "data").get("count", 1) == 0:
                        continue
                    else:
                        yield item
        except PyCongressException as e:
            self._adapter._logger.error(f"Error retrieving granules: {e}")
@dataclass
class MemberGranule(Retriever):
    data: Dict
    _pagination: Dict
    _adapter: RestAdapter

    def __init__(self, data: Dict, _pagination: Dict, _adapter: RestAdapter, _id_package_init: Optional[Tuple] = None):
        self.data = data
        self._pagination = _pagination
        super().__init__(data=self.data, _adapter=_adapter)

        self._parse_data()
        self._post_init_processing()

    def _post_init_processing(self):

        self._parse_download()
        self._parse_members()
        self._parse_online()

    def _parse_download(self):
        self._download = getattr(self, '_download', {})
        for item in ["premisLink", "txtLink", "zipLink", "modsLink", "pdfLink", "xmlLink"]:
            setattr(self, f"_{self._camel_to_snake(item)}", self._download.get(item))

    def _parse_members(self):
        members = getattr(self, '_members', [])
        for member in members:
            self._gpo_id = member.get('gpoId')
            self._authority_id = member.get('authorityId')
            self._bioguide_id = member.get('bioGuideId')
            self._party = member.get('party')
            self._chamber = member.get('chamber')
            self._congress = member.get('congress')



    def _parse_online(self):
        self._online = getattr(self, '_online', [])
        for item in self._online:
            if "gov" in item:
                setattr(self, "_official_url", item)
            elif "facebook" in item:
                setattr(self, "_facebook_url", item)
            elif "twitter" in item:
                setattr(self, "_twitter_url", item)
            elif "youtube" in item:
                setattr(self, "_youtube_url", item)
            elif "instagram" in item:
                setattr(self, "_instagram_url", item)
            else:
                setattr(self, "_other_url", item)

    @property
    def date_issued(self) -> Optional[str]:
        return getattr(self, '_date_issued', None)

    @property
    def title(self) -> Optional[str]:
        return getattr(self, '_title', None)

    @property
    def package_id(self) -> Optional[str]:
        return getattr(self, '_package_id', None)

    @property
    def biography(self) -> Optional[str]:
        return getattr(self, '_biography', None)

    @property
    def population(self) -> Optional[str]:
        return getattr(self, '_population', None)
    
    @property
    def state(self) -> Optional[str]:
        return getattr(self, '_state', None)

    @property
    def member_name(self) -> Optional[str]:
        return getattr(self, '_member_name', None)
    
    @property
    def party(self) -> Optional[str]:
        return getattr(self, '_party', None)

    @property
    def chamber(self) -> Optional[str]:
        return getattr(self, '_chamber', None)

    @property
    def congress(self) -> Optional[str]:
        return getattr(self, '_congress', None)
    
    @property
    def granule_class(self) -> Optional[str]:
        return getattr(self, '_granule_class', None)

    @property
    def granule_id(self) -> Optional[str]:
        return getattr(self, '_granule_id', None)

    @property
    def sub_granule_class(self) -> Optional[str]:
        return getattr(self, '_sub_granule_class', None)

    @property
    def gpo_id(self) -> Optional[str]:
        return getattr(self, "_gpo_id", None)

    @property
    def authority_id(self) -> Optional[str]:
        return getattr(self, "_authority_id", None)

    @property
    def details_url(self) -> Optional[str]:
        return getattr(self, "_details_link", None)

    @property
    def official_url(self) -> Optional[str]:
        return getattr(self, "_official_url", None)

    @property
    def facebook_url(self) -> Optional[str]:
        return getattr(self, "_facebook_url", None)

    @property
    def twitter_url(self) -> Optional[str]:
        return getattr(self, "_twitter_url", None)

    @property
    def youtube_url(self) -> Optional[str]:
        return getattr(self, "_youtube_url", None)

    @property
    def instagram_url(self) -> Optional[str]:
        return getattr(self, "_instagram_url", None)

    @property
    def other_url(self) -> Optional[str]:
        return getattr(self, "_other_url", None)

    # Linkage properties
    @property
    def bioguide_id(self) -> Optional[str]:
        return getattr(self, "_bioguide_id", None)

    @property
    def granule_url(self) -> Optional[str]:
        return getattr(self, "_granule_link", None)

@dataclass
class CongressionalReportPackage(Retriever):
    data: Dict
    _pagination: Dict
    _adapter: RestAdapter

    def __post_init__(self):
        super().__init__(data=self.data, _adapter=self._adapter)
        self._parse_data()
        self._parse_download()
        self._parse_other_identifier()
        self._parse_pages()
        self._parse_report_id()

    def _parse_report_id(self):
        self._type = getattr(self, "_doc_class", None)
        self._number = getattr(self, "_document_number", None)
        self._congress = getattr(self, "_congress", None)

        self._report_id = f"{self._type}{self._congress}-{self._number}"

    def _parse_download(self):
        self._download = getattr(self, '_download', {})
        for item in ["premisLink", "zipLink", "modsLink", "pdfLink", "txtLink", "xmlLink"]:
            setattr(self, f"_{item}", self._download.get(item))

    def _parse_other_identifier(self):
        self._other_identifier = getattr(self, '_other_identifier', {})
        for key, value in self._other_identifier.items():
            snake_key = self._dash_to_snake(self._camel_to_snake(key))
            setattr(self, f"_{snake_key}", value)

    def _parse_pages(self):
        pages = getattr(self, '_pages', None)
        if isinstance(pages, str) and pages.isdigit():
            self._pages = int(pages)

    def _parse_serialset(self):
        serial_set = getattr(self, '_serial_set', None)
        if serial_set:
            print(serial_set)
        
        self._serial_details = SerialSet(serial_set_number=serial_set.get("serialSetNumber"), is_glp=serial_set.get("isGLP"), doc_id=serial_set.get("docId"), bag_id=serial_set.get("bagId"), package_id=self.package_id)
        self._adapter._logger.info(f"Serial Details: {vars(self._serial_details)}")

    def _parse_subjects(self):
        subject_list = getattr(self, '_subjects', {})
        topics_list = subject_list.get("topics", [])
        self._subject_topics = [topic for topic in topics_list if isinstance(topic, list)]

    @property
    def date_issued(self) -> Optional[str]:
        return getattr(self, '_date_issued', None)

    @property
    def document_type(self) -> Optional[str]:
        return getattr(self, '_document_type', None)

    @property
    def document_number(self) -> Optional[str]:
        return getattr(self, '_document_number', None)

    @property
    def congress(self) -> Optional[str]:
        return getattr(self, '_congress', None)

    @property
    def session(self) -> Optional[str]:
        return getattr(self, '_session', None)

    @property
    def package_id(self) -> Optional[str]:
        return getattr(self, '_package_id', None)

    @property
    def collection_code(self) -> Optional[str]:
        return getattr(self, '_collection_code', None)

    @property
    def title(self) -> Optional[str]:
        return getattr(self, '_title', None)

    @property
    def branch(self) -> Optional[str]:
        return getattr(self, '_branch', None)

    @property
    def collection_name(self) -> Optional[str]:
        return getattr(self, '_collection_name', None)

    @property
    def pages(self) -> Optional[int]:
        return getattr(self, '_pages', None)

    @property
    def government_author1(self) -> Optional[str]:
        return getattr(self, '_government_author1', None)

    @property
    def government_author2(self) -> Optional[str]:
        return getattr(self, '_government_author2', None)

    @property
    def chamber(self) -> Optional[str]:
        return getattr(self, '_chamber', None)

    @property
    def subtitle(self) -> Optional[str]:
        return getattr(self, '_subtitle', None)

    @property
    def publisher(self) -> Optional[str]:
        return getattr(self, '_publisher', None)

    @property
    def su_doc_class_number(self) -> Optional[str]:
        return getattr(self, '_su_doc_class_number', None)

    @property
    def doc_class(self) -> Optional[str]:
        return getattr(self, '_doc_class', None)

    @property
    def last_modified(self) -> Optional[str]:
        return getattr(self, '_last_modified', None)

    @property
    def category(self) -> Optional[str]:
        return getattr(self, '_category', None)

    @property
    def migrated_doc_id(self) -> Optional[str]:
        return getattr(self, "_migrated_doc_id", None)

    @property
    def granules_url(self) -> Optional[str]:
        return getattr(self, '_granules_link', None)

    @property
    def package_url(self) -> Optional[str]:
        return getattr(self, "_package_link", None)

    @property
    def serial_details(self) -> Optional[SerialSet]:
        return self._serial_details

    @property
    def subjects(self) -> Optional[List[str]]:
        return self._subject_topics

    @property
    def premis_url(self) -> Optional[str]:
        return getattr(self, "_premis_link", None)

    @property
    def zip_url(self) -> Optional[str]:
        return getattr(self, "_zip_link", None)

    @property
    def mods_url(self) -> Optional[str]:
        return getattr(self, "_mods_link", None)

    @property
    def pdf_url(self) -> Optional[str]:
        return getattr(self, "pdf_link", None)

    @property
    def txt_url(self) -> Optional[str]:
        return getattr(self, "_txt_link", None)

    @property
    def xml_url(self) -> Optional[str]:
        return getattr(self, "_xml_link", None)

    @property
    def report_id(self) -> Optional[str]:
        return getattr(self, "_report_id", None)

    async def get_granules(self, verbose=True):
        if not self.granules_url:
            self._adapter._logger.warning(f"No granules URL available for {self.package_id}.")
            return

        async for item in self._get_granules(self.granules_url, ReportPartGranule, verbose=verbose):
            if isinstance(item, ErrorResult):
                self._adapter._logger.error(f"Error retrieving granule: {item.error_message}", exc_info=True)
                continue
            self._adapter._logger.info(f"report granule item: {vars(item)}")
            yield item

@dataclass
class ReportPartGranule(Retriever):
    data: Dict
    _pagination: Dict
    _adapter: RestAdapter

    def __init__(self, data: Dict, _pagination: Dict, _adapter: RestAdapter):
        self.data = data
        self._pagination = _pagination
        super().__init__(data=self.data, _adapter=_adapter)

        self._parse_data()
        self._post_init_processing()

    def _post_init_processing(self):
        self._parse_download()
        self._parse_references()
        self._parse_members()
        self._parse_committees()

    def _parse_download(self):
        self._download = getattr(self, '_download', {})
        for item in ["premisLink", "txtLink", "zipLink", "modsLink", "pdfLink"]:
            setattr(self, f"_{item}", self._download.get(item))

    
    def _parse_references(self):
        self._bill_references = []
        self._law_references = []
        self._statute_references = []
        self._code_references = []
        
        for ref in getattr(self, '_references', []):
            if ref.get('collectionCode') == "USCODE":
                for content in ref.get('contents', []):
                    self._code_references.append(ReferenceCode(
                        collection_name=ref.get('collectionName'),
                        collection_code=ref.get('collectionCode'),
                        package_id=getattr(self, '_package_id', None),
                        granule_id=getattr(self, '_granule_id', None),
                        title=content.get('title'),
                        label=content.get('label'),
                        sections=",".join(map(str, content.get('sections', [])))
                    ))
            elif ref.get('collectionCode') == "STATUTE":
                for content in ref.get('contents', []):
                    self._statute_references.append(ReferenceStatute(
                        collection_name=ref.get('collectionName'),
                        collection_code=ref.get('collectionCode'),
                        package_id=getattr(self, '_package_id', None),
                        granule_id=getattr(self, '_granule_id', None),
                        title=content.get('title'),
                        label=content.get('label'),
                        pages=",".join(map(str, content.get('pages', []))),
                    ))
            elif ref.get('collectionCode') == "PLAW":
                for content in ref.get('contents', []):
                    self._law_references.append(ReferenceLaw(
                        collection_name=ref.get('collectionName'),
                        collection_code=ref.get('collectionCode'),
                        package_id=getattr(self, '_package_id', None),
                        granule_id=getattr(self, '_granule_id', None),
                        number=content.get('number'),
                        congress=content.get('congress'),
                        label=content.get('label'),
                    ))
            elif ref.get('collectionCode') == "BILLS":
                for content in ref.get('contents', []):
                    self._bill_references.append(ReferenceBill(
                        collection_name=ref.get('collectionName'),
                        collection_code=ref.get('collectionCode'),
                        package_id=getattr(self, '_package_id', None),
                        granule_id=getattr(self, '_granule_id', None),
                        number=content.get('number'),
                        congress=content.get('congress'),
                        type=content.get('type')
                    ))
                    
    def _parse_members(self):
        self._members_list = []
        for member in getattr(self, '_members', []):
            name_list = member.get('name', [])
            parsed_name = [name_dct.get('parsed') for name_dct in name_list]
            if parsed_name:
                name = parsed_name[0]
            else:
                name = member.get('memberName')
            self._members_list.append(ReportRole(
                package_id=getattr(self, '_package_id', None),
                granule_id=getattr(self, '_granule_id', None),
                role=member.get('role'),
                bioguide_id=member.get('bioGuideId', member.get('memberName')),
                name=name
            ))

    def _parse_committees(self):
        self._committees_list = []
        for committee in getattr(self, '_committees', []):
            self._committees_list.append(Committee(
                committee_code=committee.get('authorityId'),
                committee_name=committee.get('committeeName'),
                package_id=getattr(self, '_package_id', None),
                granule_id=getattr(self, '_granule_id', None),
                chamber=committee.get('chamber'),
                type=committee.get('type')
            ))

    @property
    def date_issued(self) -> Optional[str]:
        return getattr(self, '_date_issued', None)

    @property
    def package_id(self) -> Optional[str]:
        return getattr(self, '_package_id', None)

    @property
    def collection_code(self) -> Optional[str]:
        return getattr(self, '_collection_code', None)

    @property
    def title(self) -> Optional[str]:
        return getattr(self, '_title', None)

    @property
    def collection_name(self) -> Optional[str]:
        return getattr(self, '_collection_name', None)

    @property
    def granule_class(self) -> Optional[str]:
        return getattr(self, '_granule_class', None)

    @property
    def granule_id(self) -> Optional[str]:
        return getattr(self, '_granule_id', None)

    @property
    def doc_class(self) -> Optional[str]:
        return getattr(self, '_doc_class', None)

    @property
    def last_modified(self) -> Optional[str]:
        return getattr(self, '_last_modified', None)

    @property
    def category(self) -> Optional[str]:
        return getattr(self, '_category', None)

    @property
    def reference_bills(self) -> Optional[List[ReferenceBill]]:
        return getattr(self, '_bill_references', [])

    @property
    def reference_laws(self) -> Optional[List[ReferenceLaw]]:
        return getattr(self, '_law_references', [])

    @property
    def reference_codes(self) -> Optional[List[ReferenceCode]]:
        return getattr(self, '_code_references', [])

    @property
    def reference_statutes(self) -> Optional[List[ReferenceStatute]]:
        return getattr(self, '_statute_references', [])

    @property
    def members(self) -> List[ReportRole]:
        return getattr(self, '_members_list', [])
    
    @property
    def committees(self) -> List[Committee]:
        return getattr(self, '_committees_list', [])

    @property
    def details_url(self) -> Optional[str]:
        return getattr(self, "_details_link", None)

    @property
    def premis_url(self) -> Optional[str]:
        return getattr(self, "_premis_link", None)

    @property
    def txt_url(self) -> Optional[str]:
        return getattr(self, "_txt_link", None)

    @property
    def zip_url(self) -> Optional[str]:
        return getattr(self, "_zip_link", None)

    @property
    def mods_url(self) -> Optional[str]:
        return getattr(self, "_mods_link", None)

    @property
    def pdf_url(self) -> Optional[str]:
        return getattr(self, "_pdf_link", None)

@dataclass
class TreatyPackage(Retriever):
    data: Dict
    _pagination: Dict
    _adapter: RestAdapter

    def __init__(self, data: Dict, _pagination: Dict, _adapter: RestAdapter):
        self.data = data
        self._pagination = _pagination
        super().__init__(data=self.data, _adapter=_adapter)

        self._parse_data()
        self._post_init_processing()

    def _post_init_processing(self):
        self._parse_download()
        self._parse_other_identifier()
        self._parse_pages()
        self._parse_treaty_id()

    def _parse_download(self):
        self._download = getattr(self, '_download', {})
        for item in ["premisLink", "zipLink", "modsLink", "pdfLink", "txtLink"]:
            setattr(self, f"_{item}", self._download.get(item))

    def _parse_other_identifier(self):
        self._other_identifier = getattr(self, '_other_identifier', {})
        for key, value in self._other_identifier.items():
            snake_key = self._dash_to_snake(self._camel_to_snake(key))
            setattr(self, f"_{snake_key}", value)

    def _parse_pages(self):
        pages = getattr(self, '_pages', None)
        if isinstance(pages, str) and pages.isdigit():
            self._pages = int(pages)

    def _parse_treaty_id(self):
        self._doc_class = getattr(self, '_doc_class', None)
        self._document_number = getattr(self, '_document_number', None)
        self._congress = getattr(self, '_congress', None)

        self._treaty_id = f"{self._doc_class.lower()}{self._document_number}-{self._congress}"

    @property
    def date_issued(self) -> Optional[str]:
        return getattr(self, '_date_issued', None)

    @property
    def document_type(self) -> Optional[str]:
        return getattr(self, '_document_type', None)

    @property
    def congress(self) -> Optional[str]:
        return getattr(self, '_congress', None)

    @property
    def session(self) -> Optional[str]:
        return getattr(self, '_session', None)

    @property
    def document_number(self) -> Optional[str]:
        return getattr(self, '_document_number', None)

    @property
    def treaty_id(self) -> Optional[str]:
        return getattr(self, '_treaty_id', None)

    @property
    def package_id(self) -> Optional[str]:
        return getattr(self, '_package_id', None)

    @property
    def collection_code(self) -> Optional[str]:
        return getattr(self, '_collection_code', None)

    @property
    def title(self) -> Optional[str]:
        return getattr(self, '_title', None)

    @property
    def branch(self) -> Optional[str]:
        return getattr(self, '_branch', None)

    @property
    def collection_name(self) -> Optional[str]:
        return getattr(self, '_collection_name', None)

    @property
    def pages(self) -> Optional[int]:
        return getattr(self, '_pages', None)

    @property
    def government_author1(self) -> Optional[str]:
        return getattr(self, '_government_author1', None)

    @property
    def government_author2(self) -> Optional[str]:
        return getattr(self, '_government_author2', None)

    @property
    def chamber(self) -> Optional[str]:
        return getattr(self, '_chamber', None)

    @property
    def publisher(self) -> Optional[str]:
        return getattr(self, '_publisher', None)

    @property
    def su_doc_class_number(self) -> Optional[str]:
        return getattr(self, '_su_doc_class_number', None)

    @property
    def doc_class(self) -> Optional[str]:
        return getattr(self, '_doc_class', None)

    @property
    def last_modified(self) -> Optional[str]:
        return getattr(self, '_last_modified', None)

    @property
    def category(self) -> Optional[str]:
        return getattr(self, '_category', None)

    @property
    def migrated_doc_id(self) -> Optional[str]:
        return getattr(self, "_migrated_doc_id", None)

    @property
    def ils_system_id(self) -> Optional[str]:
        return getattr(self, "_ils_system_id", None)

    @property
    def package_url(self) -> Optional[str]:
        return getattr(self, "_package_link", None)

    @property
    def granules_url(self) -> Optional[str]:
        return getattr(self, '_granules_link', None)

    @property
    def premis_url(self) -> Optional[str]:
        return getattr(self, "premis_link", None)

    @property
    def zip_url(self) -> Optional[str]:
        return getattr(self, "zip_link", None)

    @property
    def mods_url(self) -> Optional[str]:
        return getattr(self, "mods_link", None)

    @property
    def pdf_url(self) -> Optional[str]:
        return getattr(self, "pdf_link", None)

    @property
    def txt_url(self) -> Optional[str]:
        return getattr(self, "txt_link", None)

    async def get_granules(self, verbose=True):
        if not self.granules_url:
            self._adapter._logger.warning(f"No granules URL available for {self.package_id}.")
            return

        async for item in self._get_granules(self.granules_url, TreatyPartGranule, verbose=verbose):
            if isinstance(item, ErrorResult):
                self._adapter._logger.error(f"Error retrieving granule: {item.error_message}", exc_info=True)
                continue
            self._adapter._logger.info(f"treaty granule item: {vars(item)}")
            yield item


@dataclass
class TreatyPartGranule(Retriever):
    data: Dict
    _pagination: Dict
    _adapter: RestAdapter

    def __init__(self, data: Dict, _pagination: Dict, _adapter: RestAdapter):
        self.data = data
        self._pagination = _pagination
        super().__init__(data=self.data, _adapter=_adapter)

        self._parse_data()
        self._post_init_processing()

    def _post_init_processing(self):
        self._parse_download()
        self._parse_committees()
    def _parse_download(self):
        self._download = getattr(self, '_download', {})
        for item in ["premisLink", "txtLink", "zipLink", "modsLink", "pdfLink"]:
            setattr(self, f"_{item}", self._download.get(item))

    def _parse_committees(self):
        self._committees_list = []
        for committee in getattr(self, '_committees', []):
            self._committees_list.append(Committee(
                committee_code=committee.get('authorityId'),
                committee_name=committee.get('committeeName'),
                package_id=self.package_id,
                granule_id=self.granule_id,
                chamber=committee.get('chamber'),
                type=committee.get('type')
            ))

    @property
    def summary(self) -> Optional[str]:
        return getattr(self, '_summary', None)

    @property
    def date_issued(self) -> Optional[str]:
        return getattr(self, '_date_issued', None)

    @property
    def is_graphics_in_pdf(self) -> Optional[bool]:
        return getattr(self, '_is_graphics_in_pdf', None) == 'true'

    @property
    def package_id(self) -> Optional[str]:
        return getattr(self, '_package_id', None)

    @property
    def collection_code(self) -> Optional[str]:
        return getattr(self, '_collection_code', None)

    @property
    def title(self) -> Optional[str]:
        return getattr(self, '_title', None)

    @property
    def collection_name(self) -> Optional[str]:
        return getattr(self, '_collection_name', None)

    @property
    def committees(self) -> List[Committee]:
        return getattr(self, '_committees_list', [])

    @property
    def granule_class(self) -> Optional[str]:
        return getattr(self, '_granule_class', None)

    @property
    def granule_id(self) -> Optional[str]:
        return getattr(self, '_granule_id', None)

    @property
    def doc_class(self) -> Optional[str]:
        return getattr(self, '_doc_class', None)

    @property
    def last_modified(self) -> Optional[str]:
        return getattr(self, '_last_modified', None)

    @property
    def category(self) -> Optional[str]:
        return getattr(self, '_category', None)

    @property
    def details_url(self) -> Optional[str]:
        return getattr(self, "_details_link", None)

    @property
    def premis_url(self) -> Optional[str]:
        return getattr(self, "_premis_link", None)

    @property
    def txt_url(self) -> Optional[str]:
        return getattr(self, "_txt_link", None)

    @property
    def zip_url(self) -> Optional[str]:
        return getattr(self, "_zip_link", None)

    @property
    def mods_url(self) -> Optional[str]:
        return getattr(self, "_mods_link", None)

    @property
    def pdf_url(self) -> Optional[str]:
        return getattr(self, "_pdf_link", None)

    async def get_granules(self, verbose=True):
        if not self.granules_url:
            raise PyCongressException("No granules URL available for this Congressional Directory.")

        async for item in self._get_granules(self.granules_url, TreatyPartGranule, verbose=verbose):
            if isinstance(item, ErrorResult):
                self._adapter._logger.error(f"Error retrieving granule: {item.error_message}")
                continue

            yield item

@dataclass
class PrintPackage(Retriever):
    data: Dict
    _pagination: Dict
    _adapter: RestAdapter

    def __init__(self, data: Dict, _pagination: Dict, _adapter: RestAdapter, _id_package_init: Optional[Tuple] = None):
        self.data = data
        self._pagination = _pagination
        super().__init__(data=self.data, _adapter=_adapter)

        self._parse_data()
        self._post_init_processing()

    def _post_init_processing(self):
        self._parse_download()
        self._parse_other_identifier()
        self._parse_pages()

    def _parse_download(self):
        self._download = getattr(self, '_download', {})
        for item in ["premisLink", "zipLink", "modsLink", "pdfLink", "txtLink"]:
            setattr(self, f"_{item}", self._download.get(item))

    def _parse_other_identifier(self):
        self._other_identifier = getattr(self, '_other_identifier', {})
        for key, value in self._other_identifier.items():
            snake_key = self._dash_to_snake(self._camel_to_snake(key))
            setattr(self, f"_{snake_key}", value)

    def _parse_pages(self):
        pages = getattr(self, '_pages', None)
        if isinstance(pages, str) and pages.isdigit():
            self._pages = int(pages)
            
    def _parse_print_id(self):
        # get print number from package id, split between the doc class and the next letter
        # Split on doc_class and take first 5 chars after it
        if self._doc_class and self._package_id:
            try:
                split_parts = self._package_id.split(self._doc_class)
                if len(split_parts) > 1:
                    self._print_number = split_parts[1][:5]
                else:
                    self._print_number = None
            except (AttributeError, IndexError):
                self._print_number = None
        else:
            self._print_number = None
        if self._print_number:
            self._print_id = f"{getattr(self, '_doc_class', None)}{self._print_number}-{getattr(self, '_congress', None)}"
        else:
            self._print_id = "-99"

    @property
    def date_issued(self) -> Optional[str]:
        return getattr(self, '_date_issued', None)

    @property
    def print_id(self) -> Optional[str]:
        return getattr(self, '_print_id', None)
    
    @property
    def congress(self) -> Optional[str]:
        return getattr(self, '_congress', None)
    
    @property
    def document_type(self) -> Optional[str]:
        return getattr(self, '_document_type', None)

    @property
    def session(self) -> Optional[str]:
        return getattr(self, '_session', None)

    @property
    def document_number(self) -> Optional[str]:
        return getattr(self, '_document_number', None)

    @property
    def package_id(self) -> Optional[str]:
        return getattr(self, '_package_id', None)

    @property
    def collection_code(self) -> Optional[str]:
        return getattr(self, '_collection_code', None)

    @property
    def title(self) -> Optional[str]:
        return getattr(self, '_title', None)

    @property
    def branch(self) -> Optional[str]:
        return getattr(self, '_branch', None)

    @property
    def collection_name(self) -> Optional[str]:
        return getattr(self, '_collection_name', None)

    @property
    def pages(self) -> Optional[int]:
        return getattr(self, '_pages', None)

    @property
    def government_author1(self) -> Optional[str]:
        return getattr(self, '_government_author1', None)

    @property
    def government_author2(self) -> Optional[str]:
        return getattr(self, '_government_author2', None)

    @property
    def chamber(self) -> Optional[str]:
        return getattr(self, '_chamber', None)

    @property
    def publisher(self) -> Optional[str]:
        return getattr(self, '_publisher', None)

    @property
    def su_doc_class_number(self) -> Optional[str]:
        return getattr(self, '_su_doc_class_number', None)

    @property
    def doc_class(self) -> Optional[str]:
        return getattr(self, '_doc_class', None)

    @property
    def last_modified(self) -> Optional[str]:
        return getattr(self, '_last_modified', None)

    @property
    def category(self) -> Optional[str]:
        return getattr(self, '_category', None)

    @property
    def migrated_doc_id(self) -> Optional[str]:
        return getattr(self, "_migrated_doc_id", None)

    @property
    def package_url(self) -> Optional[str]:
        return getattr(self, "_package_link", None)

    @property
    def granules_url(self) -> Optional[str]:
        return getattr(self, '_granules_link', None)

    @property
    def premis_url(self) -> Optional[str]:
        return getattr(self, "_premis_link", None)

    @property
    def txt_url(self) -> Optional[str]:
        return getattr(self, "_txt_link", None)

    @property
    def zip_url(self) -> Optional[str]:
        return getattr(self, "_zip_link", None)

    @property
    def mods_url(self) -> Optional[str]:
        return getattr(self, "_mods_link", None)

    @property
    def pdf_url(self) -> Optional[str]:
        return getattr(self, "_pdf_link", None)

    async def get_granules(self, verbose=True):
        if not self.granules_url:
            raise PyCongressException("No granules URL available for this Congressional Directory.")

        async for item in self._get_granules(self.granules_url, PrintPartGranule, verbose=verbose):
            if isinstance(item, ErrorResult):
                self._adapter._logger.error(f"Error retrieving granule: {item.error_message}")
                continue

            yield item



@dataclass
class PrintPartGranule(Retriever):
    data: Dict
    _pagination: Dict
    _adapter: RestAdapter

    def __init__(self, data: Dict, _pagination: Dict, _adapter: RestAdapter):
        self.data = data
        self._pagination = _pagination
        super().__init__(data=self.data, _adapter=_adapter)

        self._parse_data()
        self._post_init_processing()

    def _post_init_processing(self):
        self._parse_download()
        self._parse_references()
        self._parse_committees()

    def _parse_download(self):
        self._download = getattr(self, '_download', {})
        for item in ["premisLink", "txtLink", "zipLink", "modsLink", "pdfLink"]:
            setattr(self, f"_{item}", self._download.get(item))

    def _parse_committees(self):
        self._committees_list = []
        for committee in getattr(self, '_committees', []):
            self._committees_list.append(Committee(
                committee_code=committee.get('authorityId'),
                committee_name=committee.get('committeeName'),
                package_id=self.package_id,
                granule_id=self.granule_id,
                chamber=committee.get('chamber'),
                type=committee.get('type')
            ))

    
    def _parse_references(self):
        self._bill_references = []
        self._law_references = []
        self._statute_references = []
        self._code_references = []
        
        for ref in getattr(self, '_references', []):
            if ref.get('collectionCode') == "USCODE":
                for content in ref.get('contents', []):
                    self._code_references.append(ReferenceCode(
                        collection_name=ref.get('collectionName'),
                        collection_code=ref.get('collectionCode'),
                        package_id=getattr(self, '_package_id', None),
                        granule_id=getattr(self, '_granule_id', None),
                        title=content.get('title'),
                        label=content.get('label'),
                        sections=",".join(map(str, content.get('sections', [])))
                    ))
            elif ref.get('collectionCode') == "STATUTE":
                for content in ref.get('contents', []):
                    self._statute_references.append(ReferenceStatute(
                        collection_name=ref.get('collectionName'),
                        collection_code=ref.get('collectionCode'),
                        package_id=getattr(self, '_package_id', None),
                        granule_id=getattr(self, '_granule_id', None),
                        title=content.get('title'),
                        label=content.get('label'),
                        pages=",".join(map(str, content.get('pages', []))),
                    ))
            elif ref.get('collectionCode') == "PLAW":
                for content in ref.get('contents', []):
                    self._law_references.append(ReferenceLaw(
                        collection_name=ref.get('collectionName'),
                        collection_code=ref.get('collectionCode'),
                        package_id=getattr(self, '_package_id', None),
                        granule_id=getattr(self, '_granule_id', None),
                        number=content.get('number'),
                        congress=content.get('congress'),
                        label=content.get('label'),
                    ))
            elif ref.get('collectionCode') == "BILLS":
                for content in ref.get('contents', []):
                    self._bill_references.append(ReferenceBill(
                        collection_name=ref.get('collectionName'),
                        collection_code=ref.get('collectionCode'),
                        package_id=getattr(self, '_package_id', None),
                        granule_id=getattr(self, '_granule_id', None),
                        number=content.get('number'),
                        congress=content.get('congress'),
                        type=content.get('type')
                    ))

    @property
    def date_issued(self) -> Optional[str]:
        return getattr(self, '_date_issued', None)

    @property
    def is_graphics_in_pdf(self) -> Optional[bool]:
        return getattr(self, '_is_graphics_in_pdf', None) == 'true'

    @property
    def package_id(self) -> Optional[str]:
        return getattr(self, '_package_id', None)

    @property
    def collection_code(self) -> Optional[str]:
        return getattr(self, '_collection_code', None)

    @property
    def title(self) -> Optional[str]:
        return getattr(self, '_title', None)

    @property
    def collection_name(self) -> Optional[str]:
        return getattr(self, '_collection_name', None)

    @property
    def granule_class(self) -> Optional[str]:
        return getattr(self, '_granule_class', None)

    @property
    def granule_id(self) -> Optional[str]:
        return getattr(self, '_granule_id', None)

    @property
    def doc_class(self) -> Optional[str]:
        return getattr(self, '_doc_class', None)

    @property
    def last_modified(self) -> Optional[str]:
        return getattr(self, '_last_modified', None)

    @property
    def category(self) -> Optional[str]:
        return getattr(self, '_category', None)

    @property
    def reference_bills(self) -> List[ReferenceBill]:
        return getattr(self, '_bill_references', [])

    @property
    def reference_laws(self) -> List[ReferenceLaw]:
        return getattr(self, '_law_references', [])

    @property
    def reference_codes(self) -> List[ReferenceCode]:
        return getattr(self, '_code_references', [])

    @property
    def reference_statutes(self) -> List[ReferenceStatute]:
        return getattr(self, '_statute_references', [])

    @property
    def details_url(self) -> Optional[str]:
        return getattr(self, "_details_link", None)

    @property
    def premis_url(self) -> Optional[str]:
        return getattr(self, "_premis_link", None)

    @property
    def txt_url(self) -> Optional[str]:
        return getattr(self, "_txt_link", None)

    @property
    def zip_url(self) -> Optional[str]:
        return getattr(self, "_zip_link", None)

    @property
    def mods_url(self) -> Optional[str]:
        return getattr(self, "_mods_link", None)

    @property
    def pdf_url(self) -> Optional[str]:
        return getattr(self, "_pdf_link", None)

    @property
    def committees(self) -> List[Committee]:
        return getattr(self, '_committees_list', [])

    @property
    def related_url(self) -> Optional[str]:
        return getattr(self, "_related_link", None)

    @property
    def granules_url(self) -> Optional[str]:
        return getattr(self, '_granules_link', None)


@dataclass
class HearingPackage(Retriever):
    data: Dict
    _pagination: Dict
    _adapter: RestAdapter

    def __init__(self, data: Dict, _pagination: Dict, _adapter: RestAdapter):
        self.data = data
        self._pagination = _pagination
        super().__init__(data=self.data, _adapter=_adapter)

        self._parse_data()
        self._post_init_processing()

    def _post_init_processing(self):
        self._parse_download()
        self._parse_other_identifier()
        self._parse_pages()

    def _parse_download(self):
        self._download = getattr(self, '_download', {})
        for item in ["premisLink", "zipLink", "modsLink", "pdfLink", "txtLink"]:
            setattr(self, f"_{item}", self._download.get(item))

    def _parse_other_identifier(self):
        self._other_identifier = getattr(self, '_other_identifier', {})
        for key, value in self._other_identifier.items():
            snake_key = self._dash_to_snake(self._camel_to_snake(key))
            setattr(self, f"_{snake_key}", value)

    def _parse_pages(self):
        pages = getattr(self, '_pages', None)
        if isinstance(pages, str) and pages.isdigit():
            self._pages = int(pages)

    def parse_held_dates(self):
        print(getattr(self, '_held_dates', []))
    
        self._held_dates_str = ",".join(map(str, getattr(self, '_held_dates', [])))

    @property
    def date_issued(self) -> Optional[str]:
        return getattr(self, '_date_issued', None)

    @property
    def document_type(self) -> Optional[str]:
        return getattr(self, '_document_type', None)

    @property
    def congress(self) -> Optional[str]:
        return getattr(self, '_congress', None)

    @property
    def session(self) -> Optional[str]:
        return getattr(self, '_session', None)

    @property
    def package_id(self) -> Optional[str]:
        return getattr(self, '_package_id', None)

    @property
    def collection_code(self) -> Optional[str]:
        return getattr(self, '_collection_code', None)

    @property
    def title(self) -> Optional[str]:
        return getattr(self, '_title', None)

    @property
    def branch(self) -> Optional[str]:
        return getattr(self, '_branch', None)

    @property
    def collection_name(self) -> Optional[str]:
        return getattr(self, '_collection_name', None)

    @property
    def pages(self) -> Optional[int]:
        return getattr(self, '_pages', None)

    @property
    def held_dates(self) -> Optional[str]:
        return getattr(self, '_held_dates_str', None)

    @property
    def government_author1(self) -> Optional[str]:
        return getattr(self, '_government_author1', None)

    @property
    def government_author2(self) -> Optional[str]:
        return getattr(self, '_government_author2', None)

    @property
    def chamber(self) -> Optional[str]:
        return getattr(self, '_chamber', None)

    @property
    def publisher(self) -> Optional[str]:
        return getattr(self, '_publisher', None)

    @property
    def su_doc_class_number(self) -> Optional[str]:
        return getattr(self, '_su_doc_class_number', None)

    @property
    def doc_class(self) -> Optional[str]:
        return getattr(self, '_doc_class', None)

    @property
    def last_modified(self) -> Optional[str]:
        return getattr(self, '_last_modified', None)

    @property
    def category(self) -> Optional[str]:
        return getattr(self, '_category', None)

    @property
    def migrated_doc_id(self) -> Optional[str]:
        return getattr(self, '_migrated_doc_id', None)

    @property
    def package_url(self) -> Optional[str]:
        return getattr(self, "_package_link", None)

    @property
    def granules_url(self) -> Optional[str]:
        return getattr(self, '_granules_link', None)

    @property
    def premis_url(self) -> Optional[str]:
        return getattr(self, "_premis_link", None)

    @property
    def zip_url(self) -> Optional[str]:
        return getattr(self, "_zip_link", None)

    @property
    def mods_url(self) -> Optional[str]:
        return getattr(self, "_mods_link", None)

    @property
    def pdf_url(self) -> Optional[str]:
        return getattr(self, "_pdf_link", None)

    @property
    def txt_url(self) -> Optional[str]:
        return getattr(self, "_txt_link", None)

    async def get_granules(self, verbose=True):
        if not self.granules_url:
            raise PyCongressException("No granules URL available for this Congressional Directory.")

        async for item in self._get_granules(self.granules_url, HearingPartGranule, verbose=verbose):
            if isinstance(item, ErrorResult):
                self._adapter._logger.error(f"Error retrieving granule: {item.error_message}")
                continue

            yield item


@dataclass
class HearingPartGranule(Retriever):
    data: Dict
    _pagination: Dict
    _adapter: RestAdapter

    def __init__(self, data: Dict, _pagination: Dict, _adapter: RestAdapter):
        self.data = data
        self._pagination = _pagination
        super().__init__(data=self.data, _adapter=_adapter)

        self._parse_data()
        self._post_init_processing()

    def _post_init_processing(self):
        self._parse_download()
        self._parse_references()
        self._parse_committees()
        self._parse_members()


    def _parse_download(self):
        self._download = getattr(self, '_download', {})
        for item in ["premisLink", "txtLink", "zipLink", "modsLink", "pdfLink"]:
            setattr(self, f"_{item}", self._download.get(item))


    def _parse_references(self):
        self._bill_references = []
        self._law_references = []
        self._statute_references = []
        self._code_references = []
        
        for ref in getattr(self, '_references', []):
            if ref.get('collectionCode') == "USCODE":
                for content in ref.get('contents', []):
                    self._code_references.append(ReferenceCode(
                        collection_name=ref.get('collectionName'),
                        collection_code=ref.get('collectionCode'),
                        package_id=getattr(self, '_package_id', None),
                        granule_id=getattr(self, '_granule_id', None),
                        title=content.get('title'),
                        label=content.get('label'),
                        sections=",".join(map(str, content.get('sections', [])))
                    ))
            elif ref.get('collectionCode') == "STATUTE":
                for content in ref.get('contents', []):
                    self._statute_references.append(ReferenceStatute(
                        collection_name=ref.get('collectionName'),
                        collection_code=ref.get('collectionCode'),
                        package_id=getattr(self, '_package_id', None),
                        granule_id=getattr(self, '_granule_id', None),
                        title=content.get('title'),
                        label=content.get('label'),
                        pages=",".join(map(str, content.get('pages', []))),
                    ))
            elif ref.get('collectionCode') == "PLAW":
                for content in ref.get('contents', []):
                    self._law_references.append(ReferenceLaw(
                        collection_name=ref.get('collectionName'),
                        collection_code=ref.get('collectionCode'),
                        package_id=getattr(self, '_package_id', None),
                        granule_id=getattr(self, '_granule_id', None),
                        number=content.get('number'),
                        congress=content.get('congress'),
                        label=content.get('label'),
                    ))
            elif ref.get('collectionCode') == "BILLS":
                for content in ref.get('contents', []):
                    self._bill_references.append(ReferenceBill(
                        collection_name=ref.get('collectionName'),
                        collection_code=ref.get('collectionCode'),
                        package_id=getattr(self, '_package_id', None),
                        granule_id=getattr(self, '_granule_id', None),
                        number=content.get('number'),
                        congress=content.get('congress'),
                        type=content.get('type')
                    ))

    def _parse_committees(self):
        self._committees_list = []
        for committee in getattr(self, '_committees', []):
            self._committees_list.append(Committee(
                package_id=getattr(self, '_package_id', None),
                granule_id=getattr(self, '_granule_id', None),
                committee_code=committee.get('authorityId'),
                committee_name=committee.get('committeeName'),
                type=committee.get('type'),
                chamber=committee.get('chamber')
            ))

    def _parse_members(self):
        self._members_list = []
        for member in getattr(self, '_members', []):
            name_list = member.get('name', [])
            parsed_name = [name_dct.get('parsed') for name_dct in name_list]
            if parsed_name:
                name = parsed_name[0]
            else:
                name = member.get('memberName')
            self._members_list.append(ReportRole(
                package_id=getattr(self, '_package_id', None),
                granule_id=getattr(self, '_granule_id', None),
                role=member.get('role'),
                bioguide_id=member.get('bioGuideId', member.get('memberName')),
                name=name
            ))

    @property
    def date_issued(self) -> Optional[str]:
        return getattr(self, '_date_issued', None)

    @property
    def graphics_in_pdf(self) -> Optional[bool]:
        return getattr(self, '_graphics_in_pdf', None) == 'true'

    @property
    def package_id(self) -> Optional[str]:
        return getattr(self, '_package_id', None)

    @property
    def collection_code(self) -> Optional[str]:
        return getattr(self, '_collection_code', None)

    @property
    def title(self) -> Optional[str]:
        return getattr(self, '_title', None)

    @property
    def is_appropriation(self) -> Optional[bool]:
        return getattr(self, '_is_appropriation', None) == 'true'

    @property
    def collection_name(self) -> Optional[str]:
        return getattr(self, '_collection_name', None)

    @property
    def granule_class(self) -> Optional[str]:
        return getattr(self, '_granule_class', None)

    @property
    def granule_id(self) -> Optional[str]:
        return getattr(self, '_granule_id', None)

    @property
    def jacketnumber(self) -> Optional[str]:
        return getattr(self, '_jacket_id', None)

    @property
    def doc_class(self) -> Optional[str]:
        return getattr(self, '_doc_class', None)

    @property
    def last_modified(self) -> Optional[str]:
        return getattr(self, '_last_modified', None)

    @property
    def category(self) -> Optional[str]:
        return getattr(self, '_category', None)

    @property
    def witnesses(self) -> Optional[str | List[str]]:
        return getattr(self, '_witnesses') if isinstance(getattr(self, '_witnesses'), list) else [getattr(self, '_witnesses')]

    @property
    def reference_bills(self) -> List[ReferenceBill]:
        return getattr(self, '_bill_references', [])

    @property
    def reference_laws(self) -> List[ReferenceLaw]:
        return getattr(self, '_law_references', [])

    @property
    def reference_statutes(self) -> List[ReferenceStatute]:
        return getattr(self, '_statute_references', [])
    
    @property
    def reference_codes(self) -> List[ReferenceCode]:
        return getattr(self, '_code_references', [])

    @property
    def committees(self) -> List[Committee]:
        return getattr(self, '_committees_list', [])

    @property
    def members(self) -> List[ReportRole]:
        return getattr(self, '_members_list', [])

    @property
    def details_url(self) -> Optional[str]:
        return getattr(self, "_details_link", None)
    
    @property
    def related_url(self) -> Optional[str]:
        return getattr(self, "_related_link", None)
    
    @property
    def granules_url(self) -> Optional[str]:
        return getattr(self, "_granule_link", None)

    @property
    def premis_url(self) -> Optional[str]:
        return getattr(self, "_premis_link", None)

    @property
    def txt_url(self) -> Optional[str]:
        return getattr(self, "_txt_link", None)

    @property
    def zip_url(self) -> Optional[str]:
        return getattr(self, "_zip_link", None)

    @property
    def mods_url(self) -> Optional[str]:
        return getattr(self, "_mods_link", None)

    @property
    def pdf_url(self) -> Optional[str]:
        return getattr(self, "_pdf_link", None)
