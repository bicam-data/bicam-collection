"""
Module for processing legislative text sections to identify and extract bill/law references and titles.

This module provides functionality to:
- Find and extract bill numbers, law numbers, and associated titles from legislative text
- Clean and validate titles using various patterns and rules
- Handle different formats of bill/law references including ranges and companion bills
- Process sections with timeout handling for long-running operations

Key components:
- Pattern matching using regular expressions for bills, laws, titles
- Title validation and cleaning with support for acronyms and special cases
- Congress number detection and year conversion
- Reference context extraction with title association
"""

import re
from typing import List, Dict, Any, Optional, Tuple, Set
from dataclasses import dataclass
import logging
from db_utils import FilingSection
from timeout_handler import timeout_handler, BatchTimeoutManager
import signal

logger = logging.getLogger(__name__)

# Title-ending words and phrases that signal potential bill/law titles
TITLE_ENDING_WORDS = {
    'Act',
    'Bill', 
    'Resolution',
    'Appropriations',
    'Trade Agreement',
}

# Update YEAR_SUFFIX_PATTERN to handle more formats
YEAR_SUFFIX_PATTERN = r'(?:\s*(?:of|,)\s*(?:19|20)\d{2})?'

# Common stop words that don't contribute to meaningful title comparison
STOP_WORDS = {
    'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by',
    'this', 'that', 'these', 'those', 'such', 'as', 'is', 'are', 'was', 'were', 'be', 'been',
    'being', 'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would', 'shall', 'should',
    'may', 'might', 'must', 'can', 'could', 'about', 'without', 'into', 'onto', 'upon',
    'within', 'among', 'throughout', 'through', 'during', 'before', 'after', 'above',
    'below', 'up', 'down', 'under', 'over', 'again', 'further', 'then', 'once', 'any',
    'all', 'both', 'each', 'few', 'more', 'most', 'other', 'some', 'its', 'his', 'her',
    'their', 'our', 'my', 'your'
}

# Patterns for matching different parts of bill references
BILL_TYPE_PATTERNS = {
    'chamber': r'(?<![\'])(?:House(?:\s+of\s+Representatives?)?|H[., ]*R[.,]*|H[.,]*|Senate|Sen[.,]*|S[.,]*)',
    'res_type': r'Concurrent|Con[.,]*|C[.,]*|Joint|J[.,]*',
    'leg_type': r'Resolution|Res[.,]*|R[.,]*|Bill|B[.,]*',
    'nid': r'(?:\d+)(?:-\d+)?[x_]*'
}

# Core pattern for matching bill numbers
BILL_NUM_PATTERN = re.compile(
    fr'\b(({BILL_TYPE_PATTERNS["chamber"]})\s*(?::|\.|\s)\s*({BILL_TYPE_PATTERNS["res_type"]})?\s*'
    fr'({BILL_TYPE_PATTERNS["leg_type"]})?(?-i:s)?\s*(?::|\.|\s)*)\s*({BILL_TYPE_PATTERNS["nid"]})',
    re.I | re.VERBOSE
)

# Pattern for matching law numbers (e.g. P.L. 115-232)
LAW_NUM_PATTERN = re.compile(
    r'\b(?:PL|P\.L\.|P\. ?L\.|Pub\.?|Public\s+Law|Private\s+Law)\s*[-]?\s*(?:\d{2,3})[-](?:\d{1,3})\b\s*(?::|,|\s)*',
    re.I
)

# Pattern for matching Congress numbers (e.g. 115th Congress)
CONGRESS_PATTERN = re.compile(
    r'\b(?:(?:1[0-9][0-9]|[1-9][0-9]|[1-9])(?:st|nd|rd|th))\s*(?:Congress|Cong\.?)?\b|\(\s*(?:1[0-9][0-9]|[1-9][0-9]|[1-9])(?:st|nd|rd|th)\s*\)'
)

# Pattern for identifying nested titles within other titles
NESTED_TITLE_PATTERN = re.compile(
    fr"""
    (?:
        \b(?:{'|'.join(TITLE_ENDING_WORDS)})\b  # Match any title ending word
        (?!{YEAR_SUFFIX_PATTERN}\s*(?:\)|$))    # Negative lookahead for year suffix at end
        .*?                                      # Non-greedy match of any text
        \b(?:{'|'.join(TITLE_ENDING_WORDS)})\b  # Another title ending word
        {YEAR_SUFFIX_PATTERN}                    # Optional year suffix
        \s*(?:\)|$)                             # End of title
    )""",
    re.VERBOSE | re.I
)

# Pattern for matching groups of related bill/law references
BILL_GROUP_PATTERN = re.compile(
    fr"""
    (?:
        # Bill reference
        (?:{BILL_TYPE_PATTERNS["chamber"]})\s*
        (?:{BILL_TYPE_PATTERNS["res_type"]})?\s*
        (?:{BILL_TYPE_PATTERNS["leg_type"]})?\s*
        {BILL_TYPE_PATTERNS["nid"]}
        |
        # Law reference
        (?:PL|P\.L\.|P\. ?L\.|Pub\.?|Public\s+Law|Private\s+Law)
        \s*[-]?\s*
        (?:\d{{2,3}})[-](?:\d{{1,3}})
    )
    (?:
        \s*[,/]\s*
        (?:{BILL_TYPE_PATTERNS["chamber"]})\s*
        (?:{BILL_TYPE_PATTERNS["res_type"]})?\s*
        (?:{BILL_TYPE_PATTERNS["leg_type"]})?\s*
        {BILL_TYPE_PATTERNS["nid"]}
    )*
    """,
    re.VERBOSE | re.I
)


@dataclass(frozen=True)
class BillNumber:
    """
    Immutable class representing a bill number.
    
    Attributes:
        number (str): The bill number
        is_range_start (bool): Whether this is the start of a bill number range
        is_range_end (bool): Whether this is the end of a bill number range
    """
    number: str
    is_range_start: bool = False
    is_range_end: bool = False

@dataclass
class ReferenceContext:
    """
    Context information for bill/law references and their associated titles.
    
    Attributes:
        start (int): Start position of reference in text
        end (int): End position of reference in text
        text (str): The reference text
        title_before (Optional[str]): Title appearing before the reference
        title_after (Optional[str]): Title appearing after the reference
        is_companion_group (bool): Whether this is part of a companion bill group
        reference_type (str): Type of reference ('bill' or 'law')
    """
    start: int
    end: int
    text: str
    title_before: Optional[str] = None
    title_after: Optional[str] = None
    is_companion_group: bool = False
    reference_type: str = 'bill'  # 'bill' or 'law'



def analyze_section_pattern(text: str) -> Dict[str, Any]:
    """
    Analyze a section of text to determine patterns of bill/law references.
    
    Args:
        text (str): The text to analyze
        
    Returns:
        Dict[str, Any]: Analysis results containing pattern type and reference contexts
    """
    # Split text into logical segments on newlines or periods
    segments = re.split(r'[.\n](?=\s*[A-Z])', text)
    
    contexts = []
    
    for segment in segments:
        # Find all bill/law references in this segment
        bill_refs = list(BILL_NUM_PATTERN.finditer(segment))
        law_refs = list(LAW_NUM_PATTERN.finditer(segment))
        
        # Process each reference in the segment
        for ref in bill_refs + law_refs:
            ref_contexts = extract_reference_context(
                segment,
                ref.start(),
                ref.end(),
                'bill' if ref in bill_refs else 'law'
            )
            
            if isinstance(ref_contexts, list):
                contexts.extend(ref_contexts)
            elif ref_contexts:
                contexts.append(ref_contexts)
    
    return {
        'pattern': 'mixed',  # Simplified since we're handling each case individually
        'contexts': contexts
    }


def is_part_of_nested_title(text: str, ref_start: int, ref_end: int) -> bool:
    """
    Check if a reference is part of a larger nested title.
    
    Args:
        text (str): The full text being analyzed
        ref_start (int): Start position of reference
        ref_end (int): End position of reference
        
    Returns:
        bool: True if reference is part of nested title, False otherwise
    """
    # Look around the reference for nested title pattern
    context_start = max(0, ref_start - 200)
    context_end = min(len(text), ref_end + 200)
    context = text[context_start:context_end]
    
    for match in NESTED_TITLE_PATTERN.finditer(context):
        # Convert match positions to full text positions
        match_start = context_start + match.start()
        match_end = context_start + match.end()
        
        # If reference falls within nested title, return True
        if match_start <= ref_start and match_end >= ref_end:
            return True
            
    return False

def extract_reference_context(text: str, start: int, end: int, ref_type: str) -> Optional[ReferenceContext]:
    """
    Extract context around a bill/law reference with title association.
    
    Args:
        text (str): The text containing the reference
        start (int): Start position of reference
        end (int): End position of reference
        ref_type (str): Type of reference ('bill' or 'law')
        
    Returns:
        Optional[ReferenceContext]: Context object if valid reference found, None otherwise
    """
    MAX_TITLE_DISTANCE = 300  # Increased to handle longer titles
    
    def find_title_before_reference(text: str, ref_start: int) -> Optional[str]:
        """Find title that comes before a bill reference."""
        title_pattern = re.compile(
            fr"""
            (?:the\s+)?                           # Optional "the"
            (?P<title>
                (?:[A-Z][A-Za-z\s,&\-]+?)        # Words
                (?:\s*\([^)]*\))?                # Optional parenthetical
                \s*
                (?:{'|'.join(TITLE_ENDING_WORDS)})   # Ending words
                (?:\s+(?:of|,)\s+\d{{4}})?        # Optional year
            )
            [,\s]*\(?                            # Optional opening parenthesis
            $                                    # End of text
            """,
            re.VERBOSE | re.I
        )
        
        look_behind = text[max(0, ref_start - MAX_TITLE_DISTANCE):ref_start]
        match = title_pattern.search(look_behind)
        if match:
            return match.group('title').strip()
        return None

    def find_title_after_reference(text: str, ref_end: int) -> Optional[str]:
        """Find title that comes after a bill reference."""
        title_pattern = re.compile(
            fr"""
            ^                                    # Start of text
            (?:[-:]\s+)?                        # Optional dash or colon with whitespace
            (?:the\s+)?                         # Optional "the"
            (?P<title>
                (?:[A-Z][A-Za-z\s,&\-]+?)      # Words
                \s*
                (?:{'|'.join(TITLE_ENDING_WORDS)})  # Ending words
                (?:\s+(?:of|,)\s+\d{{4}})?      # Optional year
            )
            """,
            re.VERBOSE | re.I
        )
        
        look_ahead = text[ref_end:min(len(text), ref_end + MAX_TITLE_DISTANCE)]
        match = title_pattern.search(look_ahead)
        if match:
            return match.group('title').strip()
        return None

    # Look for title before reference
    title_before = find_title_before_reference(text, start)
    
    # Look for title after reference if no title found before
    title_after = None
    if not title_before:
        title_after = find_title_after_reference(text, end)
    
    # Extract companion bills if present
    reference_text = text[start:end].strip()
    
    # Create context with the title if found
    return ReferenceContext(
        start=start,
        end=end,
        text=reference_text,
        title_before=title_before,
        title_after=title_after,
        reference_type=ref_type
    )

def process_section_for_titles(text: str) -> List[Dict[str, Any]]:
    """
    Process a section looking for titles with bill numbers in various formats.
    
    Args:
        text (str): The text to process
        
    Returns:
        List[Dict[str, Any]]: List of found titles with their metadata
    """
    all_matches = []

    # Find standalone titles first
    standalone_title_pattern = re.compile(
        fr"""
        (?:^|\.|\n|\s)                        # Start boundaries
        (?P<title>
            (?:To\samend\s|A\sbill\sto\s)?    # Common formal title starts
            (?:[A-Z][A-Za-z\s,&\-]+?)        # Words
            \s+
            (?:{'|'.join(TITLE_ENDING_WORDS)})\b  # Title ending word
            (?:\s+(?:of|,)\s+\d{{4}})?        # Optional year
        )
        (?=\.|$|\n|\s)                        # End boundaries
        """,
        re.VERBOSE | re.I
    )
    
    for match in standalone_title_pattern.finditer(text):
        title_text = match.group('title').strip()
        if title_text and not is_part_of_nested_title(text, match.start(), match.end()):
            # Additional validation
            if validate_title(title_text, allow_acronyms=True):
                # Make sure this title isn't near a bill reference
                context_start = max(0, match.start() - 50)
                context_end = min(len(text), match.end() + 50)
                context = text[context_start:context_end]
                
                if not BILL_NUM_PATTERN.search(context):
                    all_matches.append({
                        'type': 'title',
                        'start': match.start(),
                        'end': match.end(),
                        'title': title_text,
                        'text': title_text
                    })
    
    colon_pattern = re.compile(
        fr"""
        (?P<bills>
            {BILL_NUM_PATTERN.pattern}
            (?:\s*[/]\s*{BILL_NUM_PATTERN.pattern})*   # Additional bills
        )
        \s*[:]\s*                                      # Colon separator
        (?P<title>
            (?:The\s+)?                                # Optional "The"
            (?:[A-Z][A-Za-z\s,&\-]+?)                 # Words
            (?:{'|'.join(TITLE_ENDING_WORDS)})        # Ending words
            (?:\s+(?:of|,)\s+\d{{4}})?                # Optional year
        )
        (?=\s*[.]\s*(?:[A-Z]|$))                      # Look ahead for sentence boundary
        """,
        re.VERBOSE | re.X
    )
    
    # Pattern for "Title (Bill Numbers)"
    parenthetical_pattern = re.compile(
        fr"""
        (?P<title>
            (?:the\s+)?
            (?:[A-Z][A-Za-z\s,&\-]+?)
            (?:{'|'.join(TITLE_ENDING_WORDS)})
            (?:\s+(?:of|,)\s+\d{{4}})?
        )
        \s*
        \((?P<bills>[^)]+)\)  # Capture bill numbers in parentheses
        (?!\s*[A-Za-z])       # Not followed by letters
        """,
        re.VERBOSE | re.I
    )
    
        # Pattern for multiple bills and title in parentheses
    parenthetical_inline_pattern = re.compile(
        fr"""
        \(                                              # Opening parenthesis
        (?P<bills>
            (?:{BILL_NUM_PATTERN.pattern}              # First bill
            (?:\s*[,/]\s*{BILL_NUM_PATTERN.pattern})*) # Additional bills with comma or slash
        )
        \s*[,;]\s*                                     # Comma or semicolon separator
        (?P<title>
            (?:the\s+)?                                # Optional "The"
            (?:[A-Z][A-Za-z\s,&\-]+?)                 # Words
            (?:{'|'.join(TITLE_ENDING_WORDS)})        # Ending words
            (?:\s+(?:of|,)\s+\d{{4}})?                # Optional year
        )
        \)                                             # Closing parenthesis
        """,
        re.VERBOSE | re.I
    )
    
    # Pattern for bills and title without parentheses
    inline_pattern = re.compile(
        fr"""
        (?P<bills>
            {BILL_NUM_PATTERN.pattern}                 # First bill
            (?:\s*[,/]\s*{BILL_NUM_PATTERN.pattern})*  # Additional bills
        )
        \s*[,;]\s*                                    # Comma or semicolon separator
        (?P<title>
            (?:the\s+)?                               # Optional "The"
            (?:[A-Z][A-Za-z\s,&\-]+?)                # Words
            (?:{'|'.join(TITLE_ENDING_WORDS)})       # Ending words
            (?:\s+(?:of|,)\s+\d{{4}})?               # Optional year
        )
        (?=\s*[).])                                  # Look ahead for end markers
        """,
        re.VERBOSE | re.I
    )
    
    # Pattern for "Bill Number - Title"
    post_reference_pattern = re.compile(
        fr"""
        (?P<bill>{BILL_NUM_PATTERN.pattern})   # Bill reference
        \s*
        [-:]\s*                                # Dash or colon separator
        (?P<title>
            (?:the\s+)?
            (?:[A-Z][A-Za-z\s,&\-]+?)
            (?:{'|'.join(TITLE_ENDING_WORDS)})
            (?:\s+(?:of|,)\s+\d{{4}})?
        )
        (?!\s*[A-Za-z])                        # Not followed by letters
        """,
        re.VERBOSE | re.I
    )

    # Pattern for "Bill1 / Bill2, Title"
    bill_group_pattern = re.compile(
        fr"""
        (?P<bills>
            {BILL_NUM_PATTERN.pattern}
            (?:\s*[,/]\s*{BILL_NUM_PATTERN.pattern})*   # Additional bills
        )
        \s*[,]\s*                                       # Comma separator (removed slash)
        (?P<title>
            (?:the\s+)?
            (?:[A-Z][A-Za-z\s,&\-]+?)
            (?:{'|'.join(TITLE_ENDING_WORDS)})
            (?:\s+(?:of|,)\s+\d{{4}})?
        )
        (?!\s*[A-Za-z])                                 # Not followed by letters
        """,
        re.VERBOSE | re.I
    )

    # Update dash pattern to be more flexible
    dash_pattern = re.compile(
        fr"""
        (?P<bills>
            {BILL_NUM_PATTERN.pattern}
            (?:\s*[/]\s*{BILL_NUM_PATTERN.pattern})*     # Additional bills
        )
        \s*[-–—]\s*                                      # All types of dashes
        (?P<title>
            (?:the\s+)?                                  # Optional "The"
            (?:[A-Z][A-Za-z0-9\s,&\-]+?)                # Words (added numbers)
            (?:{'|'.join(TITLE_ENDING_WORDS)})          # Ending words
            (?:\s+(?:of|,)\s+\d{{4}})?                 # Optional year
            (?:\s+(?:\(introduced\s+\d+/\d+/\d+\))?     # Optional introduced date
            (?:\s*[-]\s*[^.]*?)?                        # Optional trailing info
        )
        (?=\s*$|\s*[.]\s*|\s*\n|\s|$)                   # More flexible end boundaries
        """,
        re.VERBOSE | re.I
    )

    # Pattern for title before bills in parentheses
    title_first_pattern = re.compile(
        fr"""
        (?P<title>
            (?:the\s+)?
            (?:[A-Z][A-Za-z\s,&\-]+?)
            (?:{'|'.join(TITLE_ENDING_WORDS)})
            (?:\s+(?:of|,|\(|\)|and|for)\s+[^()]*?)?     # More flexible ending
        )
        \s*
        \(
        (?P<bills>
            (?:Bill\s+No\.\s*)?                          # Optional "Bill No." prefix
            {BILL_NUM_PATTERN.pattern}
            (?:\s*[/]\s*{BILL_NUM_PATTERN.pattern})*     # Additional bills
        )
        \)
        """,
        re.VERBOSE | re.I
    )

    # Pattern for title before bills without parentheses
    title_first_no_parens = re.compile(
        fr"""
        (?P<title>
            (?:the\s+)?
            (?:[A-Z][A-Za-z\s,&\-]+?)
            (?:{'|'.join(TITLE_ENDING_WORDS)})
            (?:\s+(?:of|,|\(|\)|and|for)\s+[^,]*?)?      # More flexible ending
        )
        \s*[,]\s*
        (?P<bills>
            (?:Bill\s+No\.\s*)?                          # Optional "Bill No." prefix
            {BILL_NUM_PATTERN.pattern}
            (?:\s*[/]\s*{BILL_NUM_PATTERN.pattern})*     # Additional bills
        )
        """,
        re.VERBOSE | re.I
    )

    # Add new pattern for "A bill to..." titles
    bill_to_pattern = re.compile(
        fr"""
        (?P<bills>
            {BILL_NUM_PATTERN.pattern}
            (?:\s*[,/]\s*{BILL_NUM_PATTERN.pattern})*   # Additional bills
        )
        \s*[,;]\s*                                      # Comma or semicolon separator
        (?P<title>
            A\s+bill\s+to\s+                           # Starts with "A bill to"
            [^;]+?                                     # Match everything until semicolon or period
            (?:                                        # Optional "or the X Act" suffix
                \s+or\s+the\s+
                (?:[A-Z][A-Za-z0-9\s,&\-]+?\s+)?
                (?:{'|'.join(TITLE_ENDING_WORDS)})
            )?
        )
        (?=[;.]|\s*$|\s*(?:{BILL_NUM_PATTERN.pattern}))  # Look ahead for end or next bill
        """,
        re.VERBOSE | re.I
    )

    # Update semicolon-separated pattern
    semicolon_pattern = re.compile(
        fr"""
        (?P<bills>
            {BILL_NUM_PATTERN.pattern}
            (?:\s*[,/]\s*{BILL_NUM_PATTERN.pattern})*   # Additional bills
        )
        \s*[,;]\s*                                      # Comma or semicolon separator
        (?P<title>
            (?:the\s+)?
            (?:[A-Z][A-Za-z0-9\s,&\-]+?)
            (?:{'|'.join(TITLE_ENDING_WORDS)})
            (?:\s+(?:of|,)\s+\d{{4}})?                 # Optional year
            (?:\s+or\s+the\s+                          # Optional alternate title
                (?:[A-Z][A-Za-z0-9\s,&\-]+?\s+)?
                (?:{'|'.join(TITLE_ENDING_WORDS)})
            )?
        )
        (?=[;.]|\s*$|\s*(?:{BILL_NUM_PATTERN.pattern}))  # Look ahead for end or next bill
        """,
        re.VERBOSE | re.I
    )

    # Add more space-separated pattern with more flexible matching
    space_separated_pattern = re.compile(
        fr"""
        (?P<bills>
            {BILL_NUM_PATTERN.pattern}
            (?:\s*[/&]\s*{BILL_NUM_PATTERN.pattern})*     # Additional bills
        )
        \s+                                               # Just whitespace separator
        (?P<title>
            (?:the\s+)?                                   # Optional "The"
            (?:[A-Z][A-Za-z0-9\s,&\-]+?)                 # Words (added numbers)
            (?:{'|'.join(TITLE_ENDING_WORDS)})           # Ending words
            (?:\s+(?:of|,)\s+\d{{4}})?                   # Optional year
            (?:\s+(?:\(introduced\s+\d+/\d+/\d+\))?      # Optional introduced date
            (?:\s*[-]\s*[^.]*?)?                         # Optional trailing info
        )
        (?=\s*$|\s*[.]\s*|\s*\n|\s|$)                   # End of line or period
        """,
        re.VERBOSE | re.I
    )
    
    
    
    # Add pattern for title before bill in parentheses
    title_before_bill_pattern = re.compile(
        fr"""
        (?P<title>
            (?:the\s+)?
            (?:[A-Z][A-Za-z0-9\s,&\-]+?)
            (?:{'|'.join(TITLE_ENDING_WORDS)})
            (?:\s+(?:of|,|\(|\)|and|for)\s+[^()]*?)?     # More flexible ending
        )
        \s*
        \(
        (?P<bills>
            (?:Bill\s+No\.\s*)?                          # Optional "Bill No." prefix
            {BILL_NUM_PATTERN.pattern}
            (?:\s*[/]\s*{BILL_NUM_PATTERN.pattern})*     # Additional bills
        )
        \)
        """,
        re.VERBOSE | re.I
    )

    # Add pattern for bill reference at end of sentence
    sentence_end_pattern = re.compile(
        fr"""
        (?P<title>
            (?:the\s+)?
            (?:[A-Z][A-Za-z0-9\s,&\-]+?)
            (?:{'|'.join(TITLE_ENDING_WORDS)})
            (?:\s+(?:of|,)\s+\d{{4}})?                   # Optional year
        )
        \s*
        \(?                                              # Optional opening parenthesis
        (?P<bills>
            {BILL_NUM_PATTERN.pattern}
        )
        \)?                                              # Optional closing parenthesis
        [.,]?                                            # Optional period or comma
        \s*
        """,
        re.VERBOSE | re.I
    )

    # Add pattern for bill with parenthetical description
    bill_with_description_pattern = re.compile(
        fr"""
        (?P<bills>{BILL_NUM_PATTERN.pattern})
        \s*
        \(
            (?P<title>[^)]+?)                           # Capture anything in parentheses
        \)
        """,
        re.VERBOSE | re.I
    )
    patterns = [
        bill_to_pattern,
        semicolon_pattern,
        parenthetical_pattern,
        parenthetical_inline_pattern,
        post_reference_pattern,
        bill_group_pattern,
        colon_pattern,
        inline_pattern,
        space_separated_pattern,
        dash_pattern,
        title_first_pattern,
        title_first_no_parens,
        title_before_bill_pattern,    # Add new patterns
        sentence_end_pattern,
        bill_with_description_pattern
    ]
    
    # Process all patterns
    for pattern in patterns:
        for match in pattern.finditer(text):
            title = match.group('title').strip()
            bills_text = match.group('bills')
            
            # Extract all bill references from the bills text
            bill_refs = list(BILL_NUM_PATTERN.finditer(bills_text))
            
            # Process each bill reference
            for bill_ref in bill_refs:
                try:
                    components = bill_ref.groups()[1:4]
                    bill_type = standardize_bill_type(*components)
                    if bill_type:
                        bill_number = bill_ref.groups()[-1]
                        
                        # Clean up title
                        # Remove introduced date if present
                        title = re.sub(r'\s*\(introduced\s+\d+/\d+/\d+\)', '', title)
                        # Remove trailing info after dash
                        title = re.split(r'\s*-\s*(?:all\s+provisions|.*?provisions)', title)[0].strip()
                        # Remove "Reconciliation Bill" and similar descriptors
                        title = re.sub(r'\s*\((?:Reconciliation\s+Bill|[^)]*?\s+Bill)\)', '', title, flags=re.I)
                        # Clean up any remaining parentheses
                        title = re.sub(r'\s*\([^)]*\)', '', title)
                        
                        # Skip if title is too short or just a descriptor
                        if len(title) < 4 or title.lower() in ['bill', 'act']:
                            continue
                        
                        all_matches.append({
                            'title': title,
                            'bill_type': bill_type.lower(),
                            'bill_number': bill_number,
                            'bill_id': f"{bill_type}{bill_number}",
                            'type': 'bill_with_title',
                            'start': match.start(),
                            'end': match.end(),
                            'text': title
                        })
                except Exception as e:
                    logging.error(f"Error processing bill reference: {str(e)}")

    return all_matches


def identify_companion_groups(contexts: List[ReferenceContext]) -> List[ReferenceContext]:
    """
    Identify and mark companion bill groups with improved pattern recognition.
    
    Args:
        contexts: List of ReferenceContext objects containing bill/law references
        
    Returns:
        List of ReferenceContext objects with companion groups marked and titles matched
        
    This function identifies sequences of bill/law references that appear to be companion bills
    by analyzing the text between references. It then matches titles to the references based on
    proximity. References in a companion group are marked with is_companion_group=True.
    """
    result = []
    i = 0
    
    while i < len(contexts):
        current = contexts[i]
        group = [current]
        last_end = current.end
        
        # Look ahead for sequential bill references
        j = i + 1
        while j < len(contexts):
            next_ref = contexts[j]
            between_text = contexts[0].text[last_end:next_ref.start]
            
            # Check if references are part of a sequence
            if len(between_text.strip()) <= 50 and not re.search(r'\.|\n', between_text):
                group.append(next_ref)
                last_end = next_ref.end
                j += 1
            else:
                break
        
        # Process group
        if len(group) > 1:
            # Find all titles in the group
            titles = []
            for ref in group:
                if ref.title_before:
                    titles.append((ref.title_before, ref.start))
                if ref.title_after:
                    titles.append((ref.title_after, ref.start))
            
            # Match titles with closest references
            for idx, ref in enumerate(group):
                closest_title = None
                min_distance = float('inf')
                
                for title, title_start in titles:
                    distance = abs(title_start - ref.start)
                    if distance < min_distance:
                        min_distance = distance
                        closest_title = title
                
                # Create new context with matched title
                if closest_title:
                    new_context = ReferenceContext(
                        start=ref.start,
                        end=ref.end,
                        text=ref.text,
                        title_before=closest_title,
                        title_after=None,
                        is_companion_group=True,
                        reference_type=ref.reference_type
                    )
                    result.append(new_context)
                else:
                    result.append(ref)
            
            i = j
        else:
            result.append(current)
            i += 1
    
    return result


def find_title_before(text: str) -> Optional[Tuple[str, int, int]]:
    """
    Find potential title before a reference.
    
    Args:
        text: String to search for title
        
    Returns:
        Optional tuple containing:
        - Title text
        - Start position
        - End position
        Returns None if no title found
        
    Searches for titles that end with standard title words (e.g. Act, Bill) and optional year.
    """
    for ending_word in TITLE_ENDING_WORDS:
        pattern = fr"""
            (?P<title>
                (?:[A-Z][A-Za-z\s,\-]*\s+)*?  # Words
                \b{ending_word}\b             # Ending word
                {YEAR_SUFFIX_PATTERN}          # Optional year
            )
            \s*$                              # End of string
        """
        match = re.search(pattern, text, re.VERBOSE | re.I)
        if match:
            return (
                match.group('title'),
                match.start(),
                match.end()
            )
    return None

def find_title_after(text: str) -> Optional[Tuple[str, int, int]]:
    """
    Find potential title after a reference.
    
    Args:
        text: String to search for title
        
    Returns:
        Optional tuple containing:
        - Title text
        - Start position
        - End position
        Returns None if no title found
        
    Searches for titles that follow a reference, with optional separator characters.
    """
    for ending_word in TITLE_ENDING_WORDS:
        pattern = fr"""
            ^
            (?P<separator>[\s:,\-–—]+)?       # Optional separator
            (?P<title>
                (?:[A-Z][A-Za-z\s,\-]*\s+)*?  # Words
                \b{ending_word}\b             # Ending word
                {YEAR_SUFFIX_PATTERN}          # Optional year
            )
        """
        match = re.search(pattern, text, re.VERBOSE | re.I)
        if match:
            return (
                match.group('title'),
                match.start('title'),
                match.end('title')
            )
    return None

def find_and_clean_titles(text: str, filing_year: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    Find and clean all titles, including standalone titles and parenthesized references.
    
    Args:
        text: Text to search for titles
        filing_year: Optional filing year for context
        
    Returns:
        List of dictionaries containing title matches with metadata
        
    Finds all bill and law references in text and attempts to associate them with titles.
    Handles various title formats including parenthetical references and comma-separated titles.
    """
    all_matches = []
    
    # First, find ALL bill and law numbers in the text
    for bill_match in BILL_NUM_PATTERN.finditer(text):
        try:
            if len(bill_match.groups()) >= 5:  # Ensure we have all expected groups
                chamber, res_type, leg_type = bill_match.groups()[1:4]
                bill_type = standardize_bill_type(chamber, res_type, leg_type)
                if bill_type:
                    bill_number = bill_match.groups()[-1]
                    all_matches.append({
                        'type': 'bill',
                        'start': bill_match.start(),
                        'end': bill_match.end(),
                        'bill_type': bill_type.lower(),
                        'bill_number': bill_number,
                        'bill_id': f"{bill_type}{bill_number}",
                        'text': bill_match.group(0).strip(),
                        'reference_type': 'bill'
                    })
        except Exception as e:
            logging.error(f"Error processing bill reference: {str(e)}")

    for law_match in LAW_NUM_PATTERN.finditer(text):
        try:
            law_number = "PL " + re.sub(r'[^\d-]', '', law_match.group(0))
            all_matches.append({
                'type': 'law',
                'start': law_match.start(),
                'end': law_match.end(),
                'law_number': law_number,
                'text': law_match.group(0).strip(),
                'reference_type': 'law'
            })
        except Exception as e:
            logging.error(f"Error processing law reference: {str(e)}")

    # Then look for title associations using various patterns
    parenthetical_pattern = re.compile(
        fr"""
        \(
        (?:P\.?L\.?\s*(?:\d{{2,3}})[-](?:\d{{1,3}})|{BILL_NUM_PATTERN.pattern})  # First reference
        (?:\s*(?:and|,|/)\s*(?:P\.?L\.?\s*(?:\d{{2,3}})[-](?:\d{{1,3}})|{BILL_NUM_PATTERN.pattern}))*  # Additional refs
        \s*[,;]\s*
        (?P<title>
            (?:the\s+)?
            (?:[A-Z][A-Za-z\s,&\-]+?)
            (?:{'|'.join(TITLE_ENDING_WORDS)})
            (?:\s+(?:of|,)\s+\d{{4}})?
        )
        \)
        """,
        re.VERBOSE | re.I
    )

    comma_title_pattern = re.compile(
        fr"""
        (?:
            (?:P\.?L\.?\s*(?:\d{{2,3}})[-](?:\d{{1,3}})|{BILL_NUM_PATTERN.pattern})
            (?:\s*(?:and|,|/)\s*(?:P\.?L\.?\s*(?:\d{{2,3}})[-](?:\d{{1,3}})|{BILL_NUM_PATTERN.pattern}))*
        )
        \s*[,;]\s*
        (?P<title>
            (?:the\s+)?
            (?:[A-Z][A-Za-z\s,&\-]+?)
            (?:{'|'.join(TITLE_ENDING_WORDS)})
            (?:\s+(?:of|,)\s+\d{{4}})?
        )
        (?=[\s,;.)])
        """,
        re.VERBOSE | re.I
    )

    # Process both patterns
    for pattern in [parenthetical_pattern, comma_title_pattern]:
        for match in pattern.finditer(text):
            try:
                title_text = match.group('title').strip()
                match_text = match.group(0)

                # Find all bill and law numbers in this match
                for bill_ref in BILL_NUM_PATTERN.finditer(match_text):
                    if len(bill_ref.groups()) >= 5:
                        chamber, res_type, leg_type = bill_ref.groups()[1:4]
                        bill_type = standardize_bill_type(chamber, res_type, leg_type)
                        if bill_type:
                            bill_id = f"{bill_type}{bill_ref.groups()[-1]}"
                            # Update existing match with title
                            for existing in all_matches:
                                if existing.get('bill_id') == bill_id:
                                    existing.update({
                                        'title': title_text,
                                        'type': 'bill_with_title'
                                    })

                for law_ref in LAW_NUM_PATTERN.finditer(match_text):
                    law_number = "PL " + re.sub(r'[^\d-]', '', law_ref.group(0))
                    # Update existing match with title
                    for existing in all_matches:
                        if existing.get('law_number') == law_number:
                            existing.update({
                                'title': title_text,
                                'type': 'law_with_title'
                            })

            except Exception as e:
                logging.error(f"Error processing title match: {str(e)}")

    return all_matches

def add_reference_with_title(
    all_titles: List[Dict],
    covered_ranges: List[Tuple[int, int]],
    seen_titles: Set[str],
    context: ReferenceContext,
    title_text: str,
    title_before: bool,
    filing_year: Optional[int]
) -> None:
    """
    Add a reference with its associated title to the results.
    
    Args:
        all_titles: List to append results to
        covered_ranges: List of text ranges already processed
        seen_titles: Set of titles already found
        context: ReferenceContext object
        title_text: Title text to associate
        title_before: Whether title appears before reference
        filing_year: Optional filing year for context
        
    Cleans title, extracts reference components, and adds complete reference to results.
    Handles deduplication and position tracking.
    """
    cleaned_title, is_law, congress_number = clean_title(title_text, filing_year)
    if cleaned_title:
        # Determine actual start and end positions based on title position
        start = min(context.start, context.start - len(title_text) if title_before else context.start)
        end = max(context.end, context.end + len(title_text) if not title_before else context.end)
        
        if not any(r_start <= start < r_end for r_start, r_end in covered_ranges):
            covered_ranges.append((start, end))
            seen_titles.add(cleaned_title.lower())
            
            components = extract_reference_components(context.text)
            if components:
                all_titles.append({
                    'type': f'{context.reference_type}_with_title',
                    'start': start,
                    'end': end,
                    **components,
                    'text': cleaned_title,
                    'title': cleaned_title,
                    'is_law': is_law or context.reference_type == 'law',
                    'congress_number': congress_number
                })

def add_standalone_reference(
    all_titles: List[Dict],
    covered_ranges: List[Tuple[int, int]],
    context: ReferenceContext,
    filing_year: Optional[int]
) -> None:
    """
    Add a standalone reference without title to the results.
    
    Args:
        all_titles: List to append results to
        covered_ranges: List of text ranges already processed
        context: ReferenceContext object
        filing_year: Optional filing year for context
        
    Adds reference without title, handling deduplication and position tracking.
    """
    if not any(start <= context.start < end for start, end in covered_ranges):
        covered_ranges.append((context.start, context.end))
        components = extract_reference_components(context.text)
        if components:
            all_titles.append({
                'type': context.reference_type,
                'start': context.start,
                'end': context.end,
                **components,
                'text': context.text.strip()
            })
            
def detect_congress(text: str, matches: List[Dict], filing_year: int) -> Dict[int, Dict]:
    """
    Detect congress numbers for all matches with comprehensive validation.
    
    Args:
        text: Text to analyze
        matches: List of reference matches
        filing_year: Filing year for context
        
    Returns:
        Dictionary mapping match start positions to congress information
        
    Uses multiple strategies to detect congress numbers:
    1. Explicit congress mentions
    2. Special prefixes indicating current congress
    3. Century references
    4. Year patterns in titles
    5. Fallback to filing year
    """
    congress_info = {}
    explicit_matches = []
    
    # Find explicit congress mentions
    for match in CONGRESS_PATTERN.finditer(text):
        congress_num = int(re.search(r'\d+', match.group(0)).group())
        filing_year_congress = year_to_congress(filing_year)
        if abs(congress_num - filing_year_congress) <= 3:
            explicit_matches.append(congress_num)
    
    # Get majority congress if multiple valid explicit matches
    majority_congress = max(explicit_matches, key=explicit_matches.count) if explicit_matches else None

    year_patterns = [
        r'(?:Act|Bill|Resolution)\s+of\s+(?:19|20)(\d{2})\b',  # Standard "of year"
        r'(?:FY|Fiscal\s+Year)\s*(?:19|20)?(\d{2})\b',  # Fiscal year references
        r'(?:19|20)(\d{2})\s+(?:FY|Fiscal\s+Year)\b',  # Year followed by FY
        r'\bof\s+(?:19|20)(\d{2})\b',  # Generic "of year"
        r'\b(?:19|20)(\d{2})\s+(?:Bill|Act|Authorization|Resolution)\b',  # Year followed by type
        r'(?:,\s*|\s+)(?:19|20)(\d{2})\b'  # Year after comma or space
    ]

    for match in matches:
        match_start = match['start']
        title_text = match.get('title', '')  # Changed from 'text' to 'title'
        
        logging.info(f"Match data: title={title_text}, text={match.get('text', '')}")  # Debug both fields
        
        # Priority 1: Special prefixes indicate current congress
        if title_text and re.match(r'^(?:To |A bill to |A resolution )', title_text, re.I):
            congress_info[match_start] = {
                'number': year_to_congress(filing_year),
                'source': 'filing_year',
                'confidence': 0.7
            }
            continue
        
        # Priority 2: Century references use filing year
        if title_text and re.search(r'\b(?:2[0-1]|1\d)(?:st|nd|rd|th)\b(?:\s+[Cc]entury\b)?', title_text):
            congress_info[match_start] = {
                'number': year_to_congress(filing_year),
                'source': 'filing_year',
                'confidence': 0.7
            }
            continue
        
        # Priority 3: Explicit congress nearby
        if majority_congress:
            congress_info[match_start] = {
                'number': majority_congress,
                'source': 'explicit',
                'confidence': 1.0
            }
            continue
            
        year_found = False
        if title_text:
            logging.info(f"Title text: {title_text}")
            for i, pattern in enumerate(year_patterns):
                logging.info(f"Trying pattern {i}: {pattern}")
                year_match = re.search(pattern, title_text, re.I)
                if year_match:
                    logging.info(f"Pattern {i} matched! Match groups: {year_match.groups()}")
                    year = int(year_match.group(1))
                    full_year = 2000 + year if year < 50 else 1900 + year
                    
                    congress_num = year_to_congress(full_year)
                    filing_year_congress = year_to_congress(filing_year)
                    
                    if abs(congress_num - filing_year_congress) <= 3:
                        congress_info[match_start] = {
                            'number': congress_num,
                            'source': 'year',
                            'confidence': 0.9
                        }
                        year_found = True
                        logging.info(f"Using year {full_year} -> congress {congress_num}")
                        break
                else:
                    logging.info(f"No match for pattern {i}")
        
        if not year_found:
            logging.info("No year patterns matched, using filing year")
            congress_info[match_start] = {
                'number': year_to_congress(filing_year),
                'source': 'filing_year',
                'confidence': 0.7
            }
    
    return congress_info

def process_single_section(section: FilingSection, timeout_manager: Optional[BatchTimeoutManager] = None) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Process a single section with improved logging.
    
    Args:
        section: FilingSection object to process
        timeout_manager: Optional timeout manager
        
    Returns:
        Tuple containing:
        - List of reference matches with titles
        - List of unmatched sections
        
    Main processing function that:
    1. Finds all titles
    2. Processes matches
    3. Adds congress information
    4. Handles errors and timeouts
    """
    text = section.text
    all_matches = []
    
    try:
        logging.debug(f"Starting processing for section {section.section_id} (length: {len(text)})")
        
        # Find and process all titles
        logging.debug(f"Finding titles in section {section.section_id}")
        title_matches = find_and_clean_titles(text, section.filing_year)
        logging.debug(f"Found {len(title_matches)} initial matches in section {section.section_id}")
        
        # Process matches
        for match in title_matches:
            match.update({
                'filing_uuid': section.filing_uuid,
                'section_id': section.section_id
            })
            all_matches.append(match)
        
        # Add congress information
        logging.debug(f"Detecting congress numbers for {len(all_matches)} matches in section {section.section_id}")
        congress_info = detect_congress(text, all_matches, section.filing_year)
        
        final_results = []
        for match in all_matches:
            congress_data = congress_info.get(match['start'], {})
            match.update({
                'congress_number': congress_data.get('number'),
                'congress_source': congress_data.get('source'),
                'congress_confidence': congress_data.get('confidence', 0.0)
            })
            final_results.append(match)
        
        # Log summary of results
        logging.info(f"Section {section.section_id}: Found {len(final_results)} matches " +
                    f"({sum(1 for m in final_results if '_with_title' in m['type'])} with titles)")
        
        # Track unmatched sections
        unmatched_sections = []
        if not final_results:
            unmatched_sections.append({
                'filing_uuid': section.filing_uuid,
                'section_id': section.section_id,
                'text': text,
                'filing_year': section.filing_year
            })
            logging.debug(f"No matches found in section {section.section_id}")
        
        return final_results, unmatched_sections
        
    except Exception as e:
        logging.error(f"Error processing section {section.section_id}: {str(e)}", exc_info=True)
        return [], []

def year_to_congress(year: int) -> int:
    """
    Convert year to congress number.
    
    Args:
        year: Year to convert
        
    Returns:
        Congress number
        
    Congress starts in odd years. For example:
    - 107th Congress: 2001-2002
    - 108th Congress: 2003-2004
    """
    return ((year - 1789) // 2) + 1

def standardize_bill_type(chamber: str, res_type: Optional[str], leg_type: Optional[str]) -> Optional[str]:
    """
    Standardize bill type with caching for performance.
    
    Args:
        chamber: Chamber (House/Senate)
        res_type: Resolution type (concurrent/joint)
        leg_type: Legislation type (bill/resolution)
        
    Returns:
        Standardized bill type or None if invalid
        
    Examples:
    - House bill -> 'hr'
    - Senate resolution -> 'sres'
    - House joint resolution -> 'hjres'
    """
    if not chamber:
        return None
    
    chamber = chamber.lower()
    res_type = res_type.lower() if res_type else None
    leg_type = leg_type.lower() if leg_type else None
    
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

def process_single_section_with_timeout(section: FilingSection, timeout: int = 60) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Process a single section with timeout handling.
    
    Args:
        section: FilingSection object to process
        timeout: Timeout in seconds (default 60)
        
    Returns:
        Same as process_single_section
        
    Wraps process_single_section with signal-based timeout handling.
    """
    # Set up signal-based timeout
    signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(timeout)
    
    try:
        return process_single_section(section)
    finally:
        signal.alarm(0)

def clean_title(title: str, filing_year: Optional[int] = None, is_special_case: bool = False) -> Tuple[Optional[str], bool, Optional[int]]:
    """
    Clean and validate a title.
    
    Args:
        title: Title to clean
        filing_year: Optional filing year for context
        is_special_case: Whether to use special case handling
        
    Returns:
        Tuple containing:
        - Cleaned title or None if invalid
        - Whether title is from a law
        - Congress number if detected
        
    Cleans titles by:
    1. Removing common prefixes
    2. Standardizing format
    3. Validating length and content
    4. Detecting congress/year information
    """
    if not isinstance(title, str):
        return None, False, None
        
    congress_number = None
    is_law = False
    
    # Remove common prefix patterns
    prefix_patterns = [
        r'^as\s+(?:amended|passed|reported|introduced|modified|marked[\s-]?up|drafted)(?:\s+(?:by|in|to)\s+[^,\.]*)?[,\s]+',
        r'^the\s+(?:revised|amended|modified|updated|draft|final|proposed)\s+',
        r'^(?:the|a|an)\s+bill\s+(?:to|for|that)\s+[^,\.]*?[,\s]+',
        r'^(?:the|a|an)\s+resolution\s+(?:to|for|that)\s+[^,\.]*?[,\s]+',
    ]
    
    for pattern in prefix_patterns:
        title = re.sub(pattern, '', title, flags=re.I)
    
    # Clean up the title
    title = re.sub(r'^[,;\s:\.]+', '', title).strip()
    title = re.sub(r'^["\'\s]+', '', title).strip()
    title = re.sub(r'["\'\s]+$', '', title).strip()
    
    # Remove any trailing garbage after valid endings
    valid_endings = '|'.join(TITLE_ENDING_WORDS)
    year_suffix = r'(?:\s*(?:of|,)\s*(?:19|20)\d{2})?'
    ending_pattern = fr'\b({valid_endings}){year_suffix}'
    
    match = re.search(ending_pattern, title, re.I)
    if match:
        title = title[:match.end()].strip()
    
    # For acronym titles, we reduce the minimum length requirement
    is_acronym = bool(re.match(r'^[A-Z]{2,}(?:\s*[-\']\s*[A-Z]+)*\s+(?:' + '|'.join(TITLE_ENDING_WORDS) + r')\b', title))
    
    if not is_acronym and len(title.split()) < 2:
        return None, False, None
    
    # Check for year in cleaned title
    year_match = re.search(r'(?:of|for|in)\s+(?:FY\s*)?(?:19|20)(\d{2})\b', title)
    if year_match:
        year = int("20" + year_match.group(1)) if int(year_match.group(1)) < 50 else int("19" + year_match.group(1))
        if filing_year:
            filing_congress = year_to_congress(filing_year)
            act_congress = year_to_congress(year)
            if filing_congress - act_congress > 3:
                is_law = True
        congress_number = act_congress
    else:
        congress_number = year_to_congress(filing_year) if filing_year else None
    
    # Validate with acronym support
    if not validate_title(title, allow_acronyms=True):
        return None, False, None

    return title, is_law, congress_number

def find_standalone_titles(text: str) -> List[Dict[str, Any]]:
    """
    Find standalone titles in text without associated bill/law references.
    
    Args:
        text: Text to search for titles
        
    Returns:
        List of dictionaries containing title matches
        
    Finds titles that appear without explicit bill/law references using:
    1. Standard title patterns
    2. Acronym-style title patterns
    """
    titles = []
    
    # Pattern for standard titles
    standard_title_pattern = fr"""
        (?:^|\.|\n|\s)                        # Start of text, sentence, line or space
        (?!.*?(?:{BILL_NUM_PATTERN.pattern})) # Negative lookahead for bill numbers
        (?P<title>
            (?:[A-Z][A-Za-z\s,\-]+\s+)*?     # Words (more flexible word matching)
            \b(?:{'|'.join(TITLE_ENDING_WORDS)})\b  # Title ending word
            (?:\s*(?:of|,)\s*(?:19|20)\d{2})?  # Optional year in either format
        )
        (?=\.|$|\n|\s)                        # End of sentence, text, line or space
    """
    
    # New pattern for acronym-style acts
    acronym_title_pattern = fr"""
        (?:^|\.|\n|\s)                        # Start boundaries
        (?!.*?(?:{BILL_NUM_PATTERN.pattern})) # Negative lookahead for bill numbers
        (?P<title>
            [A-Z]{{2,}}                       # At least two uppercase letters
            (?:\s*[-']\s*[A-Z]+)*            # Optional hyphenated/apostrophe parts
            \s+                               # Space after acronym
            (?:{'|'.join(TITLE_ENDING_WORDS)})\b  # Title ending word
            (?:\s*(?:of|,)\s*(?:19|20)\d{2})?  # Optional year
        )
        (?=\.|$|\n|\s)                        # End boundaries
    """
    
    patterns = [
        re.compile(standard_title_pattern, re.VERBOSE | re.I),
        re.compile(acronym_title_pattern, re.VERBOSE | re.I)
    ]
    
    for pattern in patterns:
        for match in pattern.finditer(text):
            title_text = match.group('title').strip()
            if title_text and not is_part_of_nested_title(text, match.start(), match.end()):
                # Additional validation
                if validate_title(title_text, allow_acronyms=True):
                    titles.append({
                        'start': match.start(),
                        'end': match.end(),
                        'text': title_text,
                        'boundary_type': 'standalone'
                    })
    
    return titles

def validate_title(title: str, allow_acronyms: bool = False) -> bool:
    """
    Validate a title string to ensure it meets formatting requirements.
    
    Args:
        title (str): The title string to validate
        allow_acronyms (bool): Whether to allow acronym-style titles (e.g. "SAFE Act")
        
    Returns:
        bool: True if title is valid, False otherwise
        
    Validation rules:
    - Must have at least one word
    - Must start with capital letter or "the"
    - Must end with valid suffix word (e.g. "Act", "Bill") optionally followed by year
    - If allow_acronyms=True, accepts titles starting with 2+ uppercase letters
    """
    # Must have minimum length
    if len(title.split()) < 1:  # Reduced for acronyms
        return False
    
    # Check if it's an acronym title
    if allow_acronyms and re.match(r'^[A-Z]{2,}(?:\s*[-\']\s*[A-Z]+)*\s+(?:' + '|'.join(TITLE_ENDING_WORDS) + r')\b', title):
        return True
        
    # Standard title validation
    if not (title[0].isupper() or title.lower().startswith('the ')):
        return False
        
    # Must end with a valid suffix (including year variations)
    valid_ending = False
    for ending_word in TITLE_ENDING_WORDS:
        # Match exact ending or ending with year
        if (title.endswith(ending_word) or 
            re.search(fr'{ending_word}\s*(?:of|,)\s*(?:19|20)\d{{2}}$', title)):
            valid_ending = True
            break
            
    return valid_ending


def extract_bill_range(start_num: str, end_num: str, bill_type: str) -> List[BillNumber]:
    """
    Extract a range of sequential bill numbers between start and end.
    
    Args:
        start_num (str): Starting bill number in range
        end_num (str): Ending bill number in range 
        bill_type (str): Type of bill (e.g. "HR", "S")
        
    Returns:
        List[BillNumber]: List of BillNumber objects for each number in range.
        Empty list if range is invalid.
        
    Validates that:
    - Start and end are valid integers
    - End is greater than start
    - Range is not more than 100 bills
    """
    try:
        start = int(re.sub(r'\D', '', start_num))
        end = int(re.sub(r'\D', '', end_num))
        if end < start or (end - start) > 100:  # Sanity check
            return []
        return [BillNumber(str(i), i == start, i == end) 
                for i in range(start, end + 1)]
    except ValueError:
        return []

def standardize_law_number(law_text: str) -> str:
    """
    Standardize a public law number to a consistent format.
    
    Args:
        law_text (str): Raw law number text
        
    Returns:
        str: Standardized law number in format "PL###-###"
        
    Example:
        >>> standardize_law_number("Public Law 115-232")
        "PL115-232"
    """
    # Remove all non-digit, non-hyphen chars
    nums = re.sub(r'[^\d-]', '', law_text)
    # Create standardized format with no space
    return f"PL{nums}"


def extract_reference_components(reference_text: str) -> Dict[str, Any]:
    """
    Extract and parse components from a bill or law reference string.
    
    Args:
        reference_text (str): Text containing bill/law reference
        
    Returns:
        Dict[str, Any]: Dictionary containing extracted components:
            For bills:
                - bill_type: Standardized bill type (e.g. "hr", "s")
                - bill_number: Primary bill number
                - bill_id: Combined bill type and number
                - bill_numbers: Tuple of BillNumber objects if range
                - is_range: Whether reference is a bill range
            For laws:
                - law_number: Standardized law number
            Empty dict if no valid reference found
            
    Handles:
    - Single bill references (e.g. "H.R. 123")
    - Bill number ranges (e.g. "H.R. 100-105") 
    - Public law references (e.g. "P.L. 115-232")
    """
    try:
        # First try bill number pattern to get the full reference
        bill_match = BILL_NUM_PATTERN.match(reference_text)
        if bill_match and len(bill_match.groups()) >= 5:
            chamber, res_type, leg_type = bill_match.groups()[1:4]
            bill_type = standardize_bill_type(chamber, res_type, leg_type)
            if bill_type:
                num_part = bill_match.groups()[-1]
                # Check for range in the number part
                range_match = re.match(r'(\d+)-(\d+)', num_part)
                if range_match:
                    start_num, end_num = range_match.groups()
                    bill_numbers = extract_bill_range(start_num, end_num, bill_type)
                    if bill_numbers:
                        return {
                            'bill_type': bill_type.lower(),
                            'bill_number': bill_numbers[0].number,  # Use first number as primary
                            'bill_id': f"{bill_type}{bill_numbers[0].number}",
                            'bill_numbers': tuple(bill_numbers),
                            'is_range': True
                        }
                else:
                    return {
                        'bill_type': bill_type.lower(),
                        'bill_number': num_part,
                        'bill_id': f"{bill_type}{num_part}",
                        'bill_numbers': tuple([BillNumber(num_part)]),
                        'is_range': False
                    }

        
        law_match = LAW_NUM_PATTERN.match(reference_text)
        if law_match:
            try:
                # Standardize format to match trie
                law_number = standardize_law_number(reference_text)
                return {
                    'law_number': law_number
                }
            except (IndexError, AttributeError):
                pass
        
        return {}
        
    except Exception as e:
        logging.error(f"Error in extract_reference_components: {str(e)}")
        return {}
