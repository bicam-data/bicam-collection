"""
Core module for matching bill references to corpus bills.

This module provides the core functionality for matching extracted bill references
to actual bills in the congressional corpus. It includes:

- Data structures for efficient bill lookup and matching
- Utilities for normalizing and comparing bill titles
- Classes for managing the matching process and state
- Functions for processing references in parallel
"""

import gc
from typing import Dict, List, Set, Optional, Tuple, NamedTuple
from dataclasses import dataclass
import re
from collections import defaultdict
import logging
import asyncpg
from rapidfuzz import fuzz
from tqdm import tqdm
import asyncio
from multiprocessing import Pool
from functools import lru_cache
import multiprocessing as mp
import polars as pl
import psutil

from section_processor import TITLE_ENDING_WORDS

logging.basicConfig(level=logging.INFO)

# Schema definitions for bills and references tables
BILLS_SCHEMA = {
    'congress': pl.Int64,
    'bill_type': pl.Utf8,
    'bill_number': pl.Utf8,
    'titles': pl.List(pl.Utf8),
    'official_titles': pl.List(pl.Utf8),
    'law_number': pl.Utf8,
    'bill_id': pl.Utf8
}

REFS_SCHEMA = {
    'reference_id': pl.Int64,
    'reference_type': pl.Utf8,
    'bill_type': pl.Utf8,
    'bill_number': pl.Utf8,
    'law_number': pl.Utf8,
    'title': pl.Utf8,
    'congress_number': pl.Int64,
    'congress_source': pl.Utf8
}

class MemoryMonitor:
    """Monitor memory usage and trigger cleanup when needed.
    
    Attributes:
        threshold (int): Memory usage threshold percentage to trigger cleanup
    """
    def __init__(self, threshold_percent=80):
        self.threshold = threshold_percent
        
    def check_memory(self):
        """Check memory usage and cleanup if above threshold.
        
        Returns:
            bool: True if cleanup was performed, False otherwise
        """
        memory = psutil.virtual_memory()
        if memory.percent > self.threshold:
            gc.collect()
            return True
        return False

@dataclass(frozen=True)
class BillInfo:
    """Information about a bill from the corpus.
    
    Attributes:
        congress (int): Congress number
        bill_type (str): Type of bill (e.g. 'hr', 's')
        bill_number (str): Bill number
        titles (Tuple[str, ...]): All titles associated with bill
        official_titles (Tuple[str, ...]): Official titles only
        law_number (Optional[str]): Public law number if enacted
    """
    congress: int
    bill_type: str
    bill_number: str
    titles: Tuple[str, ...]
    official_titles: Tuple[str, ...]
    law_number: Optional[str] = None
    
    def __post_init__(self):
        # Convert lists to tuples if necessary
        if isinstance(self.titles, list):
            object.__setattr__(self, 'titles', tuple(self.titles or ()))
        if isinstance(self.official_titles, list):
            object.__setattr__(self, 'official_titles', tuple(self.official_titles or ()))
        
        # Ensure we always have tuples, even if None was passed
        if self.titles is None:
            object.__setattr__(self, 'titles', ())
        if self.official_titles is None:
            object.__setattr__(self, 'official_titles', ())
    
    def __hash__(self):
        return hash((self.congress, self.bill_type, self.bill_number))
    
    def __eq__(self, other):
        if not isinstance(other, BillInfo):
            return False
        return (self.congress == other.congress and 
                self.bill_type == other.bill_type and 
                self.bill_number == other.bill_number)

class MatchResult(NamedTuple):
    """Result of matching a reference to corpus bills.
    
    Attributes:
        reference_id (Optional[int]): ID of matched reference
        match_type (str): Type of match (e.g. 'high_confidence', 'unmatched')
        confidence_score (float): Confidence score between 0-1
        extracted_title (Optional[str]): Title from reference
        extracted_bill_number (Optional[str]): Bill number from reference
        extracted_law_number (Optional[str]): Law number from reference
        matched_congress (Optional[int]): Congress number of matched bill
        matched_bill_type (Optional[str]): Bill type of matched bill
        matched_bill_number (Optional[str]): Bill number of matched bill
        matched_title (Optional[str]): Title of matched bill
        matched_law_number (Optional[str]): Law number of matched bill
        bill_id (Optional[str]): Full bill ID of matched bill
    """
    reference_id: Optional[int]
    match_type: str
    confidence_score: float
    extracted_title: Optional[str]
    extracted_bill_number: Optional[str]
    extracted_law_number: Optional[str]
    matched_congress: Optional[int] = None
    matched_bill_type: Optional[str] = None
    matched_bill_number: Optional[str] = None
    matched_title: Optional[str] = None
    matched_law_number: Optional[str] = None
    bill_id: Optional[str] = None

@dataclass
class AppropriationsBill:
    """Information about an appropriations bill.
    
    Attributes:
        bill_type (str): Type of appropriations bill
        bill_number (str): Bill number
        congresses (Set[int]): All congresses this bill appears in
        titles (Set[str]): All appropriations-related titles
        normalized_titles (Set[str]): Normalized versions for matching
    """
    bill_type: str
    bill_number: str
    congresses: Set[int]  # Track all congresses this bill appears in
    titles: Set[str]      # All appropriations-related titles
    normalized_titles: Set[str]  # Normalized versions for matching

class AppropriationsTrie:
    """Specialized structure for appropriations bills.
    
    Provides efficient lookup and matching for appropriations bills.
    
    Attributes:
        bills (Dict): Maps bill identifiers to AppropriationsBill objects
        title_to_bills (Dict): Maps normalized titles to bill identifiers
    """
    def __init__(self):
        self.bills: Dict[Tuple[str, str], AppropriationsBill] = {}  # (bill_type, number) -> AppropriationsBill
        self.title_to_bills: Dict[str, Set[Tuple[str, str]]] = defaultdict(set)  # normalized title -> set of (type, number)
    
    def add_bill(self, bill: BillInfo) -> None:
        """Add a bill if it's an appropriations bill.
        
        Args:
            bill: BillInfo object to potentially add
        """
        has_appropriations = False
        appropriations_titles = set()
        
        # Convert to tuples before combining
        all_titles = tuple(filter(None, tuple(bill.titles or ()) + tuple(bill.official_titles or ())))
        
        # Check all titles for appropriations
        for title in all_titles:
            if not title:
                continue
            if 'appropriation' in title.lower():
                has_appropriations = True
                appropriations_titles.add(title)
        
        if not has_appropriations:
            return
        
        key = (bill.bill_type, bill.bill_number)
        if key not in self.bills:
            self.bills[key] = AppropriationsBill(
                bill_type=bill.bill_type,
                bill_number=bill.bill_number,
                congresses={bill.congress},
                titles=appropriations_titles,
                normalized_titles={normalize_appropriations_title(t) for t in appropriations_titles}
            )
        else:
            self.bills[key].congresses.add(bill.congress)
            self.bills[key].titles.update(appropriations_titles)
            self.bills[key].normalized_titles.update(
                normalize_appropriations_title(t) for t in appropriations_titles
            )
        
        # Update title index
        for title in appropriations_titles:
            normalized = normalize_appropriations_title(title)
            self.title_to_bills[normalized].add(key)
    
    def find_matching_bills(self, title: str) -> List[Tuple[AppropriationsBill, str, float]]:
        """Find matching appropriations bills for a title.
        
        Args:
            title: Title to match against

        Returns:
            List of tuples containing (bill, matched_title, confidence_score)
        """
        normalized_query = normalize_appropriations_title(title)
        matches = []
        
        for bill_key in self.title_to_bills.get(normalized_query, set()):
            bill = self.bills[bill_key]
            # Find the best matching original title
            best_score = 0
            best_title = None
            for orig_title in bill.titles:
                score = calculate_title_similarity(title, orig_title)
                if score > best_score:
                    best_score = score
                    best_title = orig_title
            if best_title:
                matches.append((bill, best_title, best_score))
        
        return sorted(matches, key=lambda x: x[2], reverse=True)

class BillTrie:
    """Trie structure for fast bill lookups with memory optimization.
    
    Provides efficient storage and retrieval of bills by congress, type,
    number and law number.
    
    Attributes:
        congress_nodes: Maps congress numbers to bill type nodes
        law_nodes: Maps law numbers to bills
        official_titles: Maps normalized titles to bills
        memory_monitor: Monitors memory usage
    """
    def __init__(self):
        self.congress_nodes = {}
        self.law_nodes = {}
        self.official_titles = {}
        self.memory_monitor = MemoryMonitor()
    
    def add_bill(self, bill: BillInfo):
        """Add a bill to the trie with memory monitoring.
        
        Args:
            bill: BillInfo object to add
        """
        if self.memory_monitor.check_memory():
            logging.info("Memory threshold reached, performed cleanup")
            
        if bill.congress not in self.congress_nodes:
            self.congress_nodes[bill.congress] = {}
        if bill.bill_type not in self.congress_nodes[bill.congress]:
            self.congress_nodes[bill.congress][bill.bill_type] = {}
            
        self.congress_nodes[bill.congress][bill.bill_type][bill.bill_number] = bill
        
        if bill.law_number:
            self.law_nodes[bill.law_number] = bill
            
        for title in bill.official_titles:
            normalized_title = normalize_title(title)
            if normalized_title not in self.official_titles:
                self.official_titles[normalized_title] = set()
            self.official_titles[normalized_title].add(bill)
    
    def get_bill(self, congress: int, bill_type: str, bill_number: str) -> Optional[BillInfo]:
        """Get a bill by congress, type and number.
        
        Args:
            congress: Congress number
            bill_type: Type of bill
            bill_number: Bill number

        Returns:
            BillInfo if found, None otherwise
        """
        try:
            return self.congress_nodes[congress][bill_type][bill_number]
        except KeyError:
            return None
    
    def get_by_law(self, law_number: str) -> Optional[BillInfo]:
        """Get a bill by law number.
        
        Args:
            law_number: Public law number

        Returns:
            BillInfo if found, None otherwise
        """
        return self.law_nodes.get(law_number)
    
    @lru_cache(maxsize=100000)
    def get_by_official_title(self, title: str) -> Set[BillInfo]:
        """Get bills matching an official title.
        
        Args:
            title: Title to match

        Returns:
            Set of matching BillInfo objects
        """
        return self.official_titles.get(normalize_title(title), set())

class ReferenceMatcher:
    """Matches extracted references to corpus bills.
    
    Main class for matching bill references to actual bills in the corpus.
    Handles different types of references and matching strategies.
    
    Attributes:
        bill_trie: Trie structure for bill lookups
        appropriations_trie: Specialized trie for appropriations bills
        title_threshold: Minimum similarity threshold for title matches
        wrong_title_threshold: Threshold for wrong title detection
        pools: Process pools for parallel matching
        memory_monitor: Monitors memory usage
        appropriations_titles: Standard appropriations bill titles
    """
    def __init__(self, bill_trie: BillTrie):
        self.bill_trie = bill_trie
        self.appropriations_trie = AppropriationsTrie()
        self.title_threshold = 0.5
        self.wrong_title_threshold = 0.3
        self.pools = None
        self.num_pools = None
        self.current_pool = 0
        self.memory_monitor = MemoryMonitor()
        # Load standard appropriations titles
        self.appropriations_titles = {
            normalize_appropriations_title(title) for title in [
                "Agriculture, Rural Development, Food and Drug Administration, and Related Agencies Appropriations Act",
                "Commerce, Justice, Science, and Related Agencies Appropriations Act",
                "Department of Defense Appropriations Act",
                "Energy and Water Development and Related Agencies Appropriations Act",
                "Financial Services and General Government Appropriations Act",
                "Department of Homeland Security Appropriations Act",
                "Department of the Interior, Environment, and Related Agencies Appropriations Act",
                "Departments of Labor, Health and Human Services, and Education, and Related Agencies Appropriations Act",
                "Legislative Branch Appropriations Act",
                "Military Construction, Veterans Affairs, and Related Agencies Appropriations Act",
                "Department of State, Foreign Operations, and Related Programs Appropriations Act",
                "Transportation, Housing and Urban Development, and Related Agencies Appropriations Act"
            ]
        }
        
        # Build appropriations trie
        for congress in self.bill_trie.congress_nodes:
            for bill_type in self.bill_trie.congress_nodes[congress]:
                for bill_number, bill in self.bill_trie.congress_nodes[congress][bill_type].items():
                    self.appropriations_trie.add_bill(bill)
    
    def initialize_pools(self):
        """Initialize process pools for parallel matching."""
        if self.pools is None:
            self.num_pools = max(1, (mp.cpu_count() - 1) // 2)
            self.pools = [Pool(processes=1) for _ in range(self.num_pools)]

    def cleanup_pools(self):
        """Clean up process pools and free memory."""
        if hasattr(self, 'pools') and self.pools is not None:
            for pool in self.pools:
                try:
                    pool.close()
                    pool.join()
                except Exception as e:
                    logging.error(f"Error cleaning up pool: {str(e)}")
            self.pools = None
            gc.collect()

    def match_reference(self, reference: Dict) -> Optional[MatchResult]:
        """Match a single reference to corpus bills.
        
        Args:
            reference: Dictionary containing reference information

        Returns:
            MatchResult if successful, None if reference invalid
        """
        try:
            if not reference or 'reference_id' not in reference:
                logging.warning("Invalid reference object")
                return None
            
            ref_type = reference.get('reference_type')
            logging.info(f"Matching reference type: {ref_type}")
            # Skip combined types
            if ref_type in ('number_in_combined', 'title_in_combined'):
                return None
            
            if ref_type in ('bill', 'bill_with_title', 'bill_with_title_combined'):
                return self._match_bill_number(reference)
            elif ref_type in ('law', 'law_with_title'):
                return self._match_law_number(reference)
            elif ref_type == 'title':
                return self._match_title_only(reference)
            else:
                logging.warning(f"Unknown reference type: {ref_type}")
                return self._create_unmatched(reference)
        except Exception as e:
            logging.error(f"Error matching reference {reference.get('reference_id')}: {str(e)}")
            return self._create_unmatched(reference)
    
    def _match_bill_number(self, reference: Dict) -> MatchResult:
        """Match reference with bill number.
        
        Args:
            reference: Dictionary containing reference information

        Returns:
            MatchResult with match details
        """
        congress = reference.get('congress_number')
        bill_type = reference.get('bill_type')
        bill_number = reference.get('bill_number')
        
        logging.info(f"Attempting bill number match - Congress: {congress}, Type: {bill_type}, Number: {bill_number}")
        
        if not all([congress, bill_type, bill_number]):
            logging.warning(f"Missing required fields - Congress: {congress}, Type: {bill_type}, Number: {bill_number}")
            return self._create_unmatched(reference)
            
        bill = self.bill_trie.get_bill(congress, bill_type, bill_number)
        if not bill:
            logging.warning(f"No matching bill found in trie for {bill_type}{bill_number}-{congress}")
            return self._create_unmatched(reference)
        
        logging.info(f"Found matching bill in trie: {bill}")
            
        # Match title if present
        if reference.get('title'):
            return self._match_with_title(reference, bill)
        
        # Number only match
        corpus_title = (bill.official_titles[0] if bill.official_titles 
                       else bill.titles[0] if bill.titles 
                       else None)
        
        return MatchResult(
            reference_id=reference['reference_id'],
            match_type='high_confidence_match',
            confidence_score=1.0,
            extracted_title=reference.get('title'),
            extracted_bill_number=bill_number,
            extracted_law_number=reference.get('law_number'),
            matched_congress=congress,
            matched_bill_type=bill_type,
            matched_bill_number=bill_number,
            matched_title=corpus_title,
            matched_law_number=None,
            bill_id=f"{bill_type}{bill_number}-{congress}"
        )
    
    @staticmethod
    def standardize_law_number(law_text: str) -> str:
        """Standardize law number format to match trie.
        
        Args:
            law_text: Raw law number text

        Returns:
            Standardized law number format
        """
        # Extract just the numbers and hyphen
        nums = re.sub(r'[^\d-]', '', law_text)
        # Return standardized format with no space
        return f"PL{nums}"

    def _match_law_number(self, reference: Dict) -> MatchResult:
        """Match reference with law number.
        
        Args:
            reference: Dictionary containing reference information

        Returns:
            MatchResult with match details
        """
        law_number = reference.get('law_number')
        if not law_number:
            logging.warning(f"No law number provided in reference")
            return self._create_unmatched(reference)
            
        # Standardize the format before lookup
        standardized_law = self.standardize_law_number(law_number)  # Note the self. here
        logging.info(f"Looking up law number {law_number} (standardized: {standardized_law})")
        bill = self.bill_trie.get_by_law(standardized_law)
        if not bill:
            logging.warning(f"No match found for law number {standardized_law}")
            return self._create_unmatched(reference)
        
        # Match title if present
        if reference.get('title'):
            return self._match_with_title(reference, bill)
        
        return MatchResult(
            reference_id=reference['reference_id'],
            match_type='high_confidence_match',
            confidence_score=1.0,
            extracted_title=reference.get('title'),
            extracted_bill_number=reference.get('bill_number'),
            extracted_law_number=law_number,
            matched_congress=bill.congress,
            matched_bill_type=bill.bill_type,
            matched_bill_number=bill.bill_number,
            matched_title=bill.official_titles[0] if bill.official_titles else None,
            matched_law_number=law_number,
            bill_id=f"{bill.bill_type}{bill.bill_number}-{bill.congress}"
        )
    
    def _match_title_only(self, reference: Dict) -> Optional[MatchResult]:
        """Match reference by title only.
        
        Args:
            reference: Dictionary containing reference information

        Returns:
            MatchResult with match details
        """
        if not reference.get('title'):
            return self._create_unmatched(reference)
        
        # Check if this is an appropriations title
        if 'approp' in reference['title'].lower():
            result = self._match_appropriations_title(reference)
            if result:
                return result
        
        extracted_title = reference['title']
        congress_info = reference.get('congress_number')

        # Handle acronym titles specially
        is_acronym = bool(re.match(r'^[A-Z]{2,}(?:\s*[-\']\s*[A-Z]+)*\s+(?:' + '|'.join(TITLE_ENDING_WORDS) + r')\b', extracted_title))
        
        # Adjust threshold based on title type
        base_threshold = 0.6 if not is_acronym else 0.9
        
        # Create two versions of the title for matching
        year_pattern = re.compile(r'\bof\s+(?:19|20)\d{2}\b')
        titles_to_check = [extracted_title]
        if not year_pattern.search(extracted_title):
            titles_to_check.append(year_pattern.sub('', extracted_title).strip())
        
        best_match = None
        best_score = 0
        
        for title_variant in titles_to_check:
            normalized_title = normalize_title(title_variant)
            corpus_title_set = self.bill_trie.get_by_official_title(normalized_title)
            
            for bill in corpus_title_set:
                if congress_info and bill.congress != congress_info:
                    continue
                    
                for bill_title in bill.official_titles:
                    if not bill_title:
                        continue
                    
                    # Calculate similarity score with word match bonus
                    base_score = calculate_title_similarity(title_variant, bill_title)
                    
                    # Add bonus for exact word matches
                    title_words = set(re.findall(r'\b\w+\b', title_variant.lower()))
                    corpus_words = set(re.findall(r'\b\w+\b', bill_title.lower()))
                    word_overlap = len(title_words & corpus_words) / max(len(title_words), len(corpus_words))
                    
                    final_score = base_score * 0.7 + word_overlap * 0.3
                    
                    if final_score > best_score and final_score >= base_threshold:
                        best_score = final_score
                        best_match = (bill, bill_title, final_score)
        
        if best_match:
            bill, matched_title, confidence = best_match
            match_type = 'high_confidence_match' if confidence >= 0.8 else 'moderate_confidence_match'
            
            return MatchResult(
                reference_id=reference['reference_id'],
                match_type=match_type,
                confidence_score=confidence,
                extracted_title=extracted_title,
                matched_congress=bill.congress,
                matched_bill_type=bill.bill_type,
                matched_bill_number=bill.bill_number,
                matched_title=matched_title,
                bill_id=f"{bill.bill_type}{bill.bill_number}-{bill.congress}"
            )
        
        return self._create_unmatched(reference)
    
    def _match_with_title(self, reference: Dict, bill: BillInfo) -> MatchResult:
        """Match reference title with bill titles.
        
        Args:
            reference: Dictionary containing reference information
            bill: BillInfo object to match against

        Returns:
            MatchResult with match details
        """
        if not reference or not reference.get('title'):
            logging.debug("No reference or title provided, returning unmatched")
            return self._create_unmatched(reference)
        
        extracted_title = reference['title']
        reference_id = reference.get('reference_id')
        
        if reference_id is None:
            logging.warning("Reference has no reference_id.")
        
        logging.debug(f"\n=== Starting title match for reference {reference_id or 'Unknown'} ===")
        logging.debug(f"Extracted title: '{extracted_title}'")
        logging.debug(f"Bill info: Congress {bill.congress}, {bill.bill_type}{bill.bill_number}")
        
        # Determine if this is a formal title that should match against official titles first
        formal_title_patterns = [
            r'^To\s+',
            r'^A bill to\s+',
            r'^A (?:joint\s+|concurrent\s+)?resolution\s+',
            r'^[A-Z][a-z]+ing\b(?!.*(?:Act|Bill|Resolution)$)'
        ]
        
        is_formal_title = any(re.match(pattern, extracted_title) for pattern in formal_title_patterns)
        logging.debug(f"Is formal title format? {is_formal_title}")
        
        # Determine which titles to check first based on format
        if is_formal_title:
            primary_titles = tuple(bill.official_titles or ())
            secondary_titles = tuple(bill.titles or ())
            
            normalized_extracted = normalize_title(extracted_title)
            
            # First check for prefix matches in primary titles
            for title in primary_titles:
                if not title:
                    continue
                if title.lower().startswith(normalized_extracted.lower()):
                    logging.debug(f"Found prefix match in primary titles: '{title}'")
                    return MatchResult(
                        reference_id=reference['reference_id'],
                        match_type='high_confidence_match',
                        confidence_score=0.9,  # High confidence for prefix match
                        extracted_title=extracted_title,
                        extracted_bill_number=reference.get('bill_number'),
                        extracted_law_number=reference.get('law_number'),
                        matched_congress=bill.congress,
                        matched_bill_type=bill.bill_type,
                        matched_bill_number=bill.bill_number,
                        matched_title=title,
                        bill_id=f"{bill.bill_type}{bill.bill_number}-{bill.congress}"
                    )
            
            # If no prefix match in primary, check secondary
            for title in secondary_titles:
                if not title:
                    continue
                if title.lower().startswith(normalized_extracted.lower()):
                    logging.debug(f"Found prefix match in secondary titles: '{title}'")
                    return MatchResult(
                        reference_id=reference['reference_id'],
                        match_type='high_confidence_match',
                        confidence_score=0.9,
                        extracted_title=extracted_title,
                        extracted_bill_number=reference.get('bill_number'),
                        extracted_law_number=reference.get('law_number'),
                        matched_congress=bill.congress,
                        matched_bill_type=bill.bill_type,
                        matched_bill_number=bill.bill_number,
                        matched_title=title,
                        bill_id=f"{bill.bill_type}{bill.bill_number}-{bill.congress}"
                    )
        else:
            primary_titles = tuple(bill.titles or ())
            secondary_titles = tuple(bill.official_titles or ())
        
        logging.debug(f"\nChecking primary titles first ({len(primary_titles) if primary_titles else 0} titles):")
        
        # First check for exact matches in primary titles
        normalized_extracted = normalize_title(extracted_title)
        for title in (primary_titles or []):
            if not title:
                continue
            if normalize_title(title) == normalized_extracted:
                logging.debug(f"Found exact match in primary titles: '{title}'")
                return MatchResult(
                    reference_id=reference['reference_id'],
                    match_type='high_confidence_match',
                    confidence_score=1.0,
                    extracted_title=extracted_title,
                    extracted_bill_number=reference.get('bill_number'),
                    extracted_law_number=reference.get('law_number'),
                    matched_congress=bill.congress,
                    matched_bill_type=bill.bill_type,
                    matched_bill_number=bill.bill_number,
                    matched_title=title,
                    bill_id=f"{bill.bill_type}{bill.bill_number}-{bill.congress}"
                )
        
        # If no exact match in primary, check secondary
        logging.debug(f"\nChecking secondary titles ({len(secondary_titles) if secondary_titles else 0} titles):")
        for title in (secondary_titles or []):
            if not title:
                continue
            if normalize_title(title) == normalized_extracted:
                logging.debug(f"Found exact match in secondary titles: '{title}'")
                return MatchResult(
                    reference_id=reference['reference_id'],
                    match_type='high_confidence_match',
                    confidence_score=1.0,
                    extracted_title=extracted_title,
                    extracted_bill_number=reference.get('bill_number'),
                    extracted_law_number=reference.get('law_number'),
                    matched_congress=bill.congress,
                    matched_bill_type=bill.bill_type,
                    matched_bill_number=bill.bill_number,
                    matched_title=title,
                    bill_id=f"{bill.bill_type}{bill.bill_number}-{bill.congress}"
                )
        
        # If no exact matches, proceed with fuzzy matching on all titles
        year_pattern = re.compile(r'\bof\s+(?:19|20)\d{2}\b')
        has_year = bool(year_pattern.search(extracted_title))
        
        best_score = 0
        best_title = None
        
        all_titles = tuple(filter(None, primary_titles + secondary_titles))
        logging.debug(f"\nProceeding with fuzzy matching against all {len(all_titles)} titles:")
        
        for i, title in enumerate(all_titles, 1):
            logging.debug(f"\nComparing with title {i}: '{title}'")
            
            corpus_title_to_check = (
                year_pattern.sub('', title).strip() 
                if not has_year 
                else title
            )
            
            title_to_check = (
                year_pattern.sub('', extracted_title).strip() 
                if not has_year 
                else extracted_title
            )
            
            score = calculate_title_similarity(title_to_check, corpus_title_to_check)
            logging.debug(f"Similarity score: {score}")
            
            if score > best_score:
                logging.debug(f"New best score! Previous: {best_score}, New: {score}")
                best_score = score
                best_title = title
        
        if best_title is None:
            logging.warning(f"No matching title found for reference {reference['reference_id']}")
            return self._create_unmatched(reference)
        
        match_type = ('high_confidence_match' if best_score >= self.wrong_title_threshold 
                    else 'wrong_title')
        
        return MatchResult(
            reference_id=reference['reference_id'],
            match_type=match_type,
            confidence_score=best_score,
            extracted_title=extracted_title,
            extracted_bill_number=reference.get('bill_number'),
            extracted_law_number=reference.get('law_number'),
            matched_congress=bill.congress,
            matched_bill_type=bill.bill_type,
            matched_bill_number=bill.bill_number,
            matched_title=best_title,
            bill_id=f"{bill.bill_type}{bill.bill_number}-{bill.congress}"
        )
    
    def _create_unmatched(self, reference: Dict, match_type: str = 'unmatched') -> MatchResult:
        """Create unmatched result with all required fields.
        
        Args:
            reference (Dict): The reference dictionary containing extracted information
            match_type (str, optional): The type of match result. Defaults to 'unmatched'
            
        Returns:
            MatchResult: A MatchResult object with all fields set to None/empty except reference info
        """
        return MatchResult(
            reference_id=reference.get('reference_id') if reference else None,
            match_type=match_type,
            confidence_score=0.0,
            extracted_title=reference.get('title'),
            extracted_bill_number=reference.get('bill_number'),
            extracted_law_number=reference.get('law_number'),
            matched_congress=None,
            matched_bill_type=None,
            matched_bill_number=None,
            matched_title=None,
            matched_law_number=None,
            bill_id=None
        )
    
    def _match_appropriations_title(self, reference: Dict) -> Optional[MatchResult]:
        """Special matching for appropriations titles.
        
        Attempts to match appropriations bill titles using a specialized trie structure.
        
        Args:
            reference (Dict): The reference dictionary containing extracted information
            
        Returns:
            Optional[MatchResult]: A MatchResult if a match is found, None otherwise
        """
        if not reference or not reference.get('title'):
            return None
        
        matches = self.appropriations_trie.find_matching_bills(reference['title'])
        if not matches:
            return None
        
        best_match = matches[0]
        bill, matched_title, confidence = best_match
        
        return MatchResult(
            reference_id=reference.get('reference_id'),
            match_type='high_confidence_match' if confidence >= 0.9 else 'wrong_title',
            confidence_score=confidence,
            extracted_title=reference['title'],
            extracted_bill_number=reference.get('bill_number'),
            extracted_law_number=reference.get('law_number'),
            matched_congress=max(bill.congresses),
            matched_bill_type=bill.bill_type,
            matched_bill_number=bill.bill_number,
            matched_title=matched_title,
            matched_law_number=None,
            bill_id=f"{bill.bill_type}{bill.bill_number}-{max(bill.congresses)}"
        )

async def combine_nearby_references(pool: asyncpg.Pool, run_id: int) -> None:
    """Combine references and titles based on typical patterns.
    
    Analyzes extracted references to find and combine related bill numbers and titles
    that appear near each other in the text.
    
    Args:
        pool (asyncpg.Pool): Database connection pool
        run_id (int): ID of the current extraction run
        
    Raises:
        Exception: If there is an error during reference combination
    """
    try:
        logging.info("Starting reference combination analysis...")
        
        async with pool.acquire() as conn:
            # First get max existing reference_id
            max_ref_id = await conn.fetchval("""
                SELECT COALESCE(MAX(reference_id), 0) 
                FROM lobbied_bill_matching.extracted_references
                WHERE run_id = $1
            """, run_id)
            
            # Fetch all references at once into memory
            all_refs = await conn.fetch("""
                SELECT 
                    er.reference_id,
                    er.filing_uuid,
                    er.section_id,
                    er.reference_type,
                    er.bill_type,
                    er.bill_id,
                    er.title,
                    er.start_position::INTEGER,
                    er.end_position::INTEGER,
                    er.congress_number,
                    er.full_match
                FROM lobbied_bill_matching.extracted_references er
                WHERE er.run_id = $1
                AND er.reference_type IN ('bill', 'title')
                ORDER BY er.section_id, er.start_position
            """, run_id)

            logging.debug(f"Fetched {len(all_refs)} references for combination.")

            # Group references by section in memory
            sections_dict = {}
            for ref in all_refs:
                key = (ref['filing_uuid'], ref['section_id'])
                if key not in sections_dict:
                    sections_dict[key] = []
                sections_dict[key].append(dict(ref))

            # Process all sections in memory
            all_updates = []
            all_ref_ids_to_update = set()
            next_ref_id = max_ref_id + 1

            for (filing_uuid, section_id), refs in sections_dict.items():
                i = 0
                while i < len(refs):
                    ref = refs[i]

                    # Pattern 1: Number(s) followed by dash then title
                    if ref['reference_type'] == 'number':
                        companion_numbers = [ref]
                        next_pos = i + 1

                        # Look for companion bills
                        while (next_pos < len(refs) and 
                               refs[next_pos]['reference_type'] == 'number' and
                               refs[next_pos]['start_position'] - companion_numbers[-1]['end_position'] <= 5):
                            companion_numbers.append(refs[next_pos])
                            next_pos += 1

                        # Look for title
                        if (next_pos < len(refs) and 
                            refs[next_pos]['reference_type'] == 'title' and
                            refs[next_pos]['start_position'] - companion_numbers[-1]['end_position'] <= 10):

                            title_ref = refs[next_pos]
                            logging.debug(f"Combining references: {[r['reference_id'] for r in companion_numbers]} + {title_ref['reference_id']}")

                            # Add all reference IDs to update set
                            for num_ref in companion_numbers:
                                if num_ref['reference_id'] is not None:
                                    all_ref_ids_to_update.add(num_ref['reference_id'])
                                else:
                                    logging.warning(f"Number reference with None reference_id encountered: {num_ref}")

                            if title_ref['reference_id'] is not None:
                                all_ref_ids_to_update.add(title_ref['reference_id'])
                            else:
                                logging.warning(f"Title reference with None reference_id encountered: {title_ref}")

                            # Create combined references with new reference_ids
                            for num_ref in companion_numbers:
                                combined_ref = (
                                    run_id, 
                                    filing_uuid, 
                                    section_id,
                                    'bill_with_title_combined',
                                    num_ref['bill_type'], 
                                    num_ref['bill_id'],
                                    title_ref['title'],
                                    num_ref['start_position'],
                                    title_ref['end_position'],
                                    num_ref['congress_number'],
                                    next_ref_id,  # Ensure reference_id is included
                                    num_ref.get('full_match')  # Include full_match if available
                                )
                                all_updates.append(combined_ref)
                                logging.debug(f"Assigning new reference_id {next_ref_id} to combined reference.")
                                next_ref_id += 1
                            
                            i = next_pos + 1
                            continue
                    i += 1

            logging.info(f"Total new combined references to insert: {len(all_updates)}")
            logging.debug(f"Next reference_id to assign: {next_ref_id}")

            # Batch insert all new combinations
            if all_updates:
                async with conn.transaction():
                    # Insert new combinations in chunks
                    chunk_size = 5000
                    for idx in range(0, len(all_updates), chunk_size):
                        chunk = all_updates[idx:idx + chunk_size]
                        logging.debug(f"Inserting chunk {idx // chunk_size + 1}: {len(chunk)} records.")
                        await conn.executemany("""
                            INSERT INTO lobbied_bill_matching.extracted_references (
                                run_id, filing_uuid, section_id, reference_type,
                                bill_type, bill_id, title, start_position, end_position,
                                congress_number, reference_id, full_match
                            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                        """, chunk)

                    logging.debug("All combined references inserted successfully.")

                    # Update all original references in one query
                    await conn.execute("""
                        UPDATE lobbied_bill_matching.extracted_references
                        SET reference_type = 
                            CASE 
                                WHEN reference_type = 'number' THEN 'number_in_combined'
                                WHEN reference_type = 'title' THEN 'title_in_combined'
                            END
                        WHERE reference_id = ANY($1)
                        AND run_id = $2
                    """, list(all_ref_ids_to_update), run_id)

            logging.info(f"Updated {len(all_ref_ids_to_update)} original references' types.")

        logging.info(f"Processed {len(sections_dict)} sections, created {len(all_updates)} combinations.")

    except Exception as e:
        logging.error(f"Error during reference combination: {str(e)}", exc_info=True)
        raise

def extract_year_from_title(title: str) -> Optional[int]:
    """Extract year from title and convert to congress number.
    
    Args:
        title (str): The title to extract year from
        
    Returns:
        Optional[int]: Congress number calculated from extracted year, or None if no year found
    """
    # Look for year patterns
    year_patterns = [
        r'\b(?:19|20)\d{2}\b',  # Standard year
        r'\bFY\s*(?:19|20)?\d{2}\b',  # Fiscal year
        r'\b(?:of|for|in)\s+(?:19|20)\d{2}\b'  # Year with preposition
    ]
    
    for pattern in year_patterns:
        match = re.search(pattern, title)
        if match:
            year_str = re.sub(r'[^\d]', '', match.group())
            year = int(year_str)
            if len(year_str) == 2:  # Handle two-digit years
                year = 2000 + year if year < 50 else 1900 + year
            return ((year - 1789) // 2) + 1
    
    return None

@lru_cache(maxsize=100000)
def normalize_title(title: str) -> str:
    """Normalize title for comparison while preserving case-sensitive information.
    
    Args:
        title (str): The title to normalize
        
    Returns:
        str: Normalized title with consistent spacing and removed year patterns
    """
    if not title:
        return ""
    
    
    # Remove year patterns at end of title
    title = re.sub(r',\s*(?:19|20)\d{2}(?:\s*$|\s*(?:and|through)\s*(?:19|20)\d{2}\s*$)', '', title)
    
    # Remove "of {year}" patterns
    title = re.sub(r'\bof\s+(?:19|20)\d{2}\b', '', title)
    
    # Normalize whitespace while preserving case
    title = ' '.join(title.split())
    
    return title

def calculate_title_similarity(title1: str, title2: str) -> float:
    """Calculate similarity between two titles with case-sensitive comparison.
    
    Args:
        title1 (str): First title to compare
        title2 (str): Second title to compare
        
    Returns:
        float: Similarity score between 0 and 1, where 1 is exact match
    """
    # Normalize spacing but preserve case
    title1 = normalize_title(title1)
    title2 = normalize_title(title2)
    
    # Quick length check
    if abs(len(title1) - len(title2)) / max(len(title1), len(title2)) > 0.5:
        return 0.0
        
    # Calculate case-sensitive word overlap
    words1 = set(title1.split())
    words2 = set(title2.split())
    word_overlap = len(words1 & words2) / max(len(words1), len(words2))
    
    # Require at least some minimum word overlap
    if word_overlap < 0.3:  # At least 30% of words should match
        return 0.0
    
    # Calculate case-sensitive string similarity
    fuzzy_score = fuzz.ratio(title1, title2) / 100.0
    
    # Weight word overlap more heavily than fuzzy matching
    weighted_score = 0.3 * fuzzy_score + 0.7 * word_overlap
    
    # Additional penalty for length difference
    length_ratio = min(len(title1), len(title2)) / max(len(title1), len(title2))
    
    return weighted_score * length_ratio

def get_congress_range(congress: Optional[int], source: str) -> List[int]:
    """Get range of congresses to search.
    
    Args:
        congress (Optional[int]): Base congress number
        source (str): Source of congress number ('explicit', 'year', 'filing_year', etc)
        
    Returns:
        List[int]: List of congress numbers to search
    """
    if not congress:
        return list(range(105, 118))
    elif source in ('explicit', 'year'):
        return [congress]
    elif source == 'filing_year':
        return list(range(max(1, congress - 1), congress + 2))
    else:
        return list(range(max(1, congress - 3), congress + 4))

async def load_corpus_bills(pool: asyncpg.Pool) -> BillTrie:
    """Load all bills from corpus into trie structure.
    
    Args:
        pool (asyncpg.Pool): Database connection pool
        
    Returns:
        BillTrie: Trie structure containing all loaded bills
        
    Raises:
        Exception: If there is an error loading bills
    """
    trie = BillTrie()
    
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            WITH staging_bills AS (
                SELECT 
                    NULLIF(TRIM(CAST(b.congress AS TEXT)), '')::INTEGER as congress,
                    b.bill_type,
                    NULLIF(TRIM(CAST(b.bill_number AS TEXT)), '') as bill_number,
                    array_agg(DISTINCT t.title) FILTER (WHERE t.title IS NOT NULL) as titles,
                    array_agg(DISTINCT t.title) FILTER (
                        WHERE position('official' in lower(t.title_type)) > 0
                    ) as official_titles,
                    -- Standardize law number format here
                    CASE 
                        WHEN l.law_id IS NOT NULL THEN 
                            'PL' || REGEXP_REPLACE(l.law_id, '[^\d-]', '', 'g')
                    END AS law_number
                FROM bicam.bills b
                LEFT JOIN bicam.bills_titles t ON b.bill_id = t.bill_id
                LEFT JOIN bicam.bills_laws l ON b.bill_id = l.bill_id
                WHERE b.congress IS NOT NULL
                  AND CAST(b.congress AS TEXT) != ''
                  AND b.bill_type IS NOT NULL
                  AND CAST(b.bill_number AS TEXT) != ''
                GROUP BY b.congress, b.bill_type, b.bill_number, l.law_id
            ),
            relational_bills AS (
                SELECT 
                    b.congress_num as congress,
                    b.bill_type,
                    NULLIF(TRIM(CAST(b.bill_number AS TEXT)), '') as bill_number,
                    array_agg(DISTINCT t.bill_title) FILTER (WHERE t.bill_title IS NOT NULL) as titles,
                    array_agg(DISTINCT t.bill_title) FILTER (
                        WHERE t.bill_title_type = 'official'
                    ) as official_titles,
                    -- Standardize law number format here too
                    CASE 
                        WHEN l.law_id IS NOT NULL THEN 
                            'PL' || REGEXP_REPLACE(l.law_id, '[^\d-]', '', 'g')
                    END AS law_number
                FROM relational___congress.bills b
                LEFT JOIN relational___congress.bill_titles t ON b.bill_id = t.bill_id
                LEFT JOIN relational___congress.bill__law l ON b.bill_id = l.bill_id
                WHERE b.congress_num IS NOT NULL
                  AND b.bill_type IS NOT NULL
                  AND CAST(b.bill_number AS TEXT) != ''
                GROUP BY b.congress_num, b.bill_type, b.bill_number, l.law_id
            )
            SELECT * FROM staging_bills
            UNION ALL
            SELECT * FROM relational_bills
        """)
        
        logging.info(f"Loaded {len(rows)} bills from corpus")
        
        for row in tqdm(rows, desc="Building bill trie"):
            try:
                if not row['congress'] or not row['bill_type'] or not row['bill_number']:
                    continue
                    
                bill = BillInfo(
                    congress=row['congress'],
                    bill_type=row['bill_type'],
                    bill_number=str(row['bill_number']),
                    titles=row['titles'] or [],
                    official_titles=row['official_titles'] or [],
                    law_number=row['law_number']
                )
                trie.add_bill(bill)
            except Exception as e:
                logging.error(f"Error processing bill row: {str(e)}")
                continue
            
        logging.info(f"Built trie with {len(trie.congress_nodes)} congress nodes")
    
    return trie

def standardize_bill_type(chamber: str, res_type: Optional[str] = None, leg_type: Optional[str] = None) -> Optional[str]:
    """Standardize bill type based on chamber and resolution type.
    
    Args:
        chamber (str): Chamber ('house' or 'senate')
        res_type (Optional[str]): Resolution type ('concurrent', 'joint', etc)
        leg_type (Optional[str]): Legislation type ('bill', 'resolution', etc)
        
    Returns:
        Optional[str]: Standardized bill type code or None if invalid input
    """
    if not chamber:
        return None
        
    chamber = chamber.lower().replace('-', '').strip()
    res_type = res_type.lower().replace('-', '').strip() if res_type else None
    leg_type = leg_type.lower().replace('-', '').strip() if leg_type else None
    
    if chamber.startswith('h'):
        if not res_type:
            if not leg_type or leg_type.startswith('b'):
                return 'hr'
            elif leg_type.startswith('r'):
                return 'hres'
        elif res_type.startswith('c'):
            return 'hconres'
        elif res_type.startswith('j'):
            return 'hjres'
    elif chamber.startswith('s'):
        if not res_type:
            if not leg_type or leg_type.startswith('b'):
                return 's'
            elif leg_type.startswith('r'):
                return 'sres'
        elif res_type.startswith('c'):
            return 'sconres'
        elif res_type.startswith('j'):
            return 'sjres'
    return None



class MatchingManager:
    """Manages bill matching state and processes."""
    def __init__(self):
        self.bill_trie: Optional[BillTrie] = None
        self.matcher: Optional[ReferenceMatcher] = None
        self._lock = asyncio.Lock()
    
    async def initialize(self, pool: asyncpg.Pool):
        """Initialize the bill trie and matcher if not already done.
        
        Args:
            pool (asyncpg.Pool): Database connection pool
        """
        async with self._lock:
            if self.bill_trie is None:
                logging.info("Loading corpus bills...")
                self.bill_trie = await load_corpus_bills(pool)
                self.matcher = ReferenceMatcher(self.bill_trie)
                logging.info("Bill trie initialized and ready")
    

                

    async def match_references(self, pool: asyncpg.Pool, run_id: int, max_retries: int = 3, batch_size: int = 10000) -> None:
        """Match all references for a run with retries and batching.
        
        Args:
            pool (asyncpg.Pool): Database connection pool
            run_id (int): ID of the current extraction run
            max_retries (int, optional): Maximum number of retries per operation. Defaults to 3
            batch_size (int, optional): Number of references to process per batch. Defaults to 10000
            
        Raises:
            Exception: If there is an error during matching
        """
        if self.matcher is None:
            await self.initialize(pool)
            
        try:
            logging.info(f"Starting reference matching for run {run_id}")
            
            async with pool.acquire() as conn:
                # Get total count first
                total_count = await conn.fetchval("""
                    SELECT COUNT(*) 
                    FROM lobbied_bill_matching.extracted_references
                    WHERE run_id = $1
                """, run_id)
            
                if not total_count:
                    logging.warning(f"No references found for run {run_id}")
                    return
                    
                logging.info(f"Found {total_count} references to match")
                
                # Process in batches with retries
                offset = 0
                with tqdm(total=total_count, desc="Matching references") as pbar:
                    while offset < total_count:
                        retry_count = 0
                        while retry_count < max_retries:
                            try:
                                async with conn.transaction():
                                # ... rest of code unchanged ...
                                

                                    # Set longer timeout for large queries
                                    await conn.execute('SET statement_timeout = 300000')  # 5 minutes
                                    
                                    # Fetch batch of references
                                    refs = await conn.fetch("""
                                        SELECT 
                                            reference_id,
                                            filing_uuid,
                                            section_id,
                                            reference_type,
                                            bill_type,
                                            bill_id,
                                            REGEXP_REPLACE(bill_id, '[^0-9]', '', 'g') AS bill_number,
                                            law_number,
                                            title,
                                            full_match,
                                            start_position,
                                            end_position,
                                            congress_number,
                                            congress_detection_source,
                                            congress_confidence
                                        FROM lobbied_bill_matching.extracted_references
                                        WHERE run_id = $1
                                        ORDER BY reference_id
                                        OFFSET $2
                                        LIMIT $3
                                    """, run_id, offset, batch_size)
                                    
                                    if not refs:
                                        break
                                    
                                    # Process matches
                                    # Use self.matcher instead of creating new one
                                    matches = []
                                    for ref in refs:
                                        try:
                                            match = self.matcher.match_reference(dict(ref))  # Use self.matcher here
                                            if match:
                                                matches.append((
                                                run_id,
                                                ref['reference_id'],
                                                match.match_type,
                                                match.confidence_score,
                                                ref['title'],  # extract from original reference
                                                ref['bill_id'],  # extract from original reference
                                                ref.get('law_number'),  # extract from original reference
                                                match.matched_congress,
                                                match.matched_bill_type,
                                                match.matched_bill_number,
                                                match.matched_law_number,
                                                match.matched_title,
                                                match.bill_id
                                            ))
                                        except Exception as e:
                                            logging.error(f"Error matching reference {ref['reference_id']}: {str(e)}")
                                    
                                    # Store matches with retry
                                    if matches:
                                        store_retry_count = 0
                                        while store_retry_count < max_retries:
                                            try:
                                                await conn.executemany("""
                                                    INSERT INTO lobbied_bill_matching.reference_matches (
                                                        run_id,
                                                        reference_id,
                                                        match_type,
                                                        confidence_score,
                                                        extracted_title,
                                                        extracted_bill_number,
                                                        extracted_law_number,
                                                        matched_congress,
                                                        matched_bill_type,
                                                        matched_bill_number,
                                                        matched_title,
                                                        matched_law_number,
                                                        bill_id
                                                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                                                """, matches)
                                                break
                                            except Exception as e:
                                                store_retry_count += 1
                                                if store_retry_count == max_retries:
                                                    logging.error(f"Error storing matches after {max_retries} attempts: {str(e)}")
                                                    raise
                                                logging.warning(f"Error storing matches (attempt {store_retry_count}/{max_retries}): {str(e)}")
                                                await asyncio.sleep(store_retry_count * 2)
                                    
                                    offset += len(refs)
                                    pbar.update(len(refs))
                                    break  # Success, exit retry loop
                                    
                            except asyncio.TimeoutError:
                                retry_count += 1
                                if retry_count == max_retries:
                                    logging.error(f"Operation timed out after {max_retries} attempts")
                                    raise
                                logging.warning(f"Operation timed out (attempt {retry_count}/{max_retries}), retrying...")
                                await asyncio.sleep(retry_count * 5)  # Exponential backoff
                                
                            except Exception as e:
                                retry_count += 1
                                if retry_count == max_retries:
                                    logging.error(f"Operation failed after {max_retries} attempts: {str(e)}")
                                    raise
                                logging.warning(f"Operation failed (attempt {retry_count}/{max_retries}): {str(e)}")
                                await asyncio.sleep(retry_count * 2)
                
                logging.info("Reference matching complete")
                
        except Exception as e:
            logging.error(f"Error during matching: {str(e)}", exc_info=True)
            raise

def process_batch(refs: List[Dict], bill_trie: BillTrie) -> List[MatchResult]:
    """Process a batch of references in a worker process.
    
    Args:
        refs (List[Dict]): List of reference dictionaries to process
        bill_trie (BillTrie): Trie structure containing bill corpus
        
    Returns:
        List[MatchResult]: List of match results for the batch
    """
    matcher = ReferenceMatcher(bill_trie)
    results = []
    for ref in refs:
        try:
            match = matcher.match_reference(ref)
            if match is not None:  # Only add non-None results
                results.append(match)
        except Exception as e:
            logging.error(f"Error processing reference {ref.get('reference_id')}: {str(e)}")
            continue
    return results
    
async def store_matches(conn: asyncpg.Pool, run_id: int, matches: List[Optional[MatchResult]]) -> None:
    """Store match results in database with retries and chunking.
    
    Args:
        conn (asyncpg.Pool): Database connection pool
        run_id (int): ID of the current extraction run
        matches (List[Optional[MatchResult]]): List of match results to store
        
    Raises:
        Exception: If there is an error storing matches
    """
    # Filter out None values and validate matches
    valid_matches = [
        match for match in matches 
        if match is not None and hasattr(match, 'reference_id') and match.reference_id is not None
    ]
    
    if not valid_matches:
        return

    # Process in smaller chunks to avoid timeouts
    chunk_size = 1000
    max_retries = 3
    retry_delay = 1

    for i in range(0, len(valid_matches), chunk_size):
        chunk = valid_matches[i:i + chunk_size]
        
        for attempt in range(max_retries):
            try:
                await conn.executemany("""
                    INSERT INTO lobbied_bill_matching.reference_matches (
                        run_id,
                        reference_id,
                        match_type,
                        confidence_score,
                        extracted_title,
                        extracted_bill_number,
                        extracted_law_number,
                        matched_congress,
                        matched_bill_type,
                        matched_bill_number,
                        matched_law_number,
                        matched_title,
                        bill_id
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                """, [
                    (
                        run_id,
                        match.reference_id,
                        match.match_type,
                        match.confidence_score,
                        match.extracted_title,
                        match.extracted_bill_number,
                        match.extracted_law_number,
                        match.matched_congress,
                        match.matched_bill_type,
                        match.matched_bill_number,
                        match.matched_law_number,
                        match.matched_title,
                        match.bill_id
                    )
                    for match in chunk
                ])
                break  # Success - exit retry loop
                
            except Exception as e:
                if attempt == max_retries - 1:  # Last attempt
                    logging.error(f"Error storing matches after {max_retries} attempts: {str(e)}")
                    raise
                else:
                    logging.warning(f"Error storing matches (attempt {attempt + 1}/{max_retries}): {str(e)}")
                    await asyncio.sleep(retry_delay * (attempt + 1))  # Exponential backoff

def normalize_appropriations_title(title: str) -> str:
    """Normalize appropriations bill titles for comparison."""
    # Remove fiscal year references
    title = re.sub(r'\b(?:for\s+)?(?:fiscal\s+year|FY)\s*(?:19|20)\d{2}\b', '', title, flags=re.I)
    
    # Replace hyphens with commas
    title = re.sub(r'\s*-\s*', ', ', title)
    
    # Expand common acronyms and abbreviations
    acronyms = {
        'HUD': 'Housing and Urban Development',
        'FDA': 'Food and Drug Administration',
        'DOD': 'Department of Defense',
        'DoD': 'Department of Defense',
        'DOS': 'Department of State',
        'DoS': 'Department of State',
        'DHS': 'Department of Homeland Security',
        'HHS': 'Department of Health and Human Services',
        'VA': 'Veterans Affairs',
        'DOI': 'Department of the Interior',
        'DoI': 'Department of the Interior',
        'DOL': 'Department of Labor',
        'DoL': 'Department of Labor',
        'Labor': 'Department of Labor',
        'State': 'Department of State',
        'Health': 'Health and Human Services',
        'Housing': 'Housing and Urban Development',
        'Defense': 'Department of Defense',
        'Interior': 'Department of the Interior',
        'Homeland': 'Department of Homeland Security',
    }
    
    for acronym, full_name in acronyms.items():
        title = re.sub(r'\b' + acronym + r'\b', full_name, title, flags=re.I)
    
    # Remove common suffixes
    title = re.sub(r'\s*(?:bill|act|appropriations|approps?)\s*$', '', title, flags=re.I)
    
    # Normalize spacing
    title = re.sub(r'\s+', ' ', title).strip()
    
    return title
