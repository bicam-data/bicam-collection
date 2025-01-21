"""
Module for handling regex pattern matching timeouts.

This module provides classes and functions to handle timeouts that occur during regex pattern
matching operations. It includes:
- Timeout tracking and storage in database
- Signal-based timeout handling for regex operations 
- Batch management of timeouts across multiple sections
- Summary reporting of timeout statistics
"""

import signal
from dataclasses import dataclass
from typing import List, Any
import asyncpg
import logging

@dataclass
class TimeoutSection:
    """Information about a section that timed out during processing.
    
    Attributes:
        filing_uuid (str): UUID of the filing containing the timed out section
        section_id (str): ID of the specific section that timed out
        chunk_id (int): ID of the text chunk within the section
        start_offset (int): Character offset where timeout occurred
        pattern_type (str): Type of pattern being matched ('number', 'law', or 'title')
        processing_time (float): Time spent processing before timeout
        error_message (str): Description of the timeout error
        text_length (int): Length of text being processed when timeout occurred
    """
    filing_uuid: str
    section_id: str
    chunk_id: int
    start_offset: int
    pattern_type: str  # 'number', 'law', or 'title'
    processing_time: float
    error_message: str
    text_length: int

class RegexTimeout(Exception):
    """Exception raised when regex matching times out.
    
    Used to interrupt long-running regex operations that exceed the timeout threshold.
    """
    pass

def timeout_handler(signum, frame):
    """Signal handler for timeout.
    
    Args:
        signum: Signal number
        frame: Current stack frame
        
    Raises:
        RegexTimeout: Always raises this exception to interrupt the operation
    """
    raise RegexTimeout("Regex pattern matching timed out")

def finditer_with_timeout(pattern, text: str, timeout: int = 30) -> List[Any]:
    """Execute finditer with a timeout.
    
    Wraps re.finditer() with a timeout mechanism to prevent infinite/long-running matches.
    
    Args:
        pattern: Compiled regex pattern to match
        text (str): Text to search for matches
        timeout (int): Maximum seconds to allow for matching (default: 30)
        
    Returns:
        List[Any]: List of match objects found before timeout
        
    Raises:
        RegexTimeout: If matching exceeds timeout duration
    """
    matches = []
    
    # Set up signal-based timeout
    signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(timeout)
    
    try:
        # Collect all matches
        matches.extend(pattern.finditer(text))
        return matches
    finally:
        # Disable the alarm
        signal.alarm(0)

class TimeoutTracker:
    """Track and store timeout information.
    
    Manages collection and persistence of timeout events to database.
    
    Attributes:
        pool (asyncpg.Pool): Database connection pool
        timeouts (List[TimeoutSection]): Collection of timeout events
    """
    
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool
        self.timeouts: List[TimeoutSection] = []
    
    async def initialize_tracking(self, run_id: int):
        """Initialize timeout tracking table.
        
        Creates the database table for storing timeout information if it doesn't exist.
        
        Args:
            run_id (int): ID of the current processing run
        """
        async with self.pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS lobbied_bill_matching.timeout_sections (
                    timeout_id BIGSERIAL PRIMARY KEY,
                    run_id BIGINT REFERENCES lobbied_bill_matching.processing_runs(run_id),
                    filing_uuid UUID NOT NULL,
                    section_id TEXT NOT NULL,
                    chunk_id INTEGER NOT NULL,
                    start_offset INTEGER NOT NULL,
                    pattern_type TEXT NOT NULL,
                    text_length INTEGER NOT NULL,
                    processing_time FLOAT NOT NULL,
                    error_message TEXT,
                    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            """)
    
    async def store_timeout(self, run_id: int, timeout_info: TimeoutSection):
        """Store information about a timed-out section.
        
        Persists a single timeout event to the database.
        
        Args:
            run_id (int): ID of the current processing run
            timeout_info (TimeoutSection): Information about the timeout event
        """
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO lobbied_bill_matching.timeout_sections (
                    run_id,
                    filing_uuid,
                    section_id,
                    chunk_id,
                    start_offset,
                    pattern_type,
                    text_length,
                    processing_time,
                    error_message
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            """,
                run_id,
                timeout_info.filing_uuid,
                str(timeout_info.section_id),
                timeout_info.chunk_id,
                timeout_info.start_offset,
                timeout_info.pattern_type,
                timeout_info.text_length,
                timeout_info.processing_time,
                timeout_info.error_message
            )
    
    async def store_all_timeouts(self, run_id: int):
        """Store all collected timeouts.
        
        Persists all collected timeout events to the database.
        
        Args:
            run_id (int): ID of the current processing run
        """
        for timeout in self.timeouts:
            await self.store_timeout(run_id, timeout)
    
    def add_timeout(self, timeout: TimeoutSection):
        """Add a timeout to the collection.
        
        Args:
            timeout (TimeoutSection): Timeout event to add
        """
        self.timeouts.append(timeout)
    
    async def print_summary(self, run_id: int):
        """Print summary of timed-out sections.
        
        Queries the database and logs statistics about timeout events grouped by pattern type.
        
        Args:
            run_id (int): ID of the processing run to summarize
        """
        async with self.pool.acquire() as conn:
            timeouts = await conn.fetch("""
                SELECT 
                    pattern_type,
                    COUNT(*) as count,
                    AVG(text_length) as avg_length,
                    AVG(processing_time) as avg_time,
                    COUNT(DISTINCT filing_uuid) as num_filings
                FROM lobbied_bill_matching.timeout_sections
                WHERE run_id = $1
                GROUP BY pattern_type
            """, run_id)
            
            if timeouts:
                logging.info("\nTimeout Summary:")
                logging.info("----------------")
                for t in timeouts:
                    logging.info(f"Pattern type: {t['pattern_type']}")
                    logging.info(f"  Number of timeouts: {t['count']}")
                    logging.info(f"  Average text length: {t['avg_length']:.0f} characters")
                    logging.info(f"  Average processing time: {t['avg_time']:.2f} seconds")
                    logging.info(f"  Affected filings: {t['num_filings']}")
                    logging.info("")
                    
class BatchTimeoutManager:
    """Manage timeouts for a batch of sections.
    
    Provides batch management of timeout events through a TimeoutTracker.
    
    Attributes:
        tracker (TimeoutTracker): Tracker instance for storing timeout events
    """
    
    def __init__(self, tracker: TimeoutTracker):
        self.tracker = tracker
    
    def handle_timeout(self, 
                        filing_uuid: str,
                        section_id: str,
                        chunk_id: int,
                        start_offset: int,
                        pattern_type: str,
                        text_length: int,
                        error: Exception) -> None:
        """Handle a timeout occurrence.
        
        Creates a TimeoutSection instance and adds it to the tracker.
        
        Args:
            filing_uuid (str): UUID of the filing
            section_id (str): ID of the section
            chunk_id (int): ID of the text chunk
            start_offset (int): Character offset of timeout
            pattern_type (str): Type of pattern being matched
            text_length (int): Length of text being processed
            error (Exception): The timeout exception that occurred
        """
        timeout_info = TimeoutSection(
            filing_uuid=filing_uuid,
            section_id=section_id,
            chunk_id=chunk_id,
            start_offset=start_offset,
            pattern_type=pattern_type,
            processing_time=15.0,  # Default timeout value
            error_message=str(error),
            text_length=text_length
        )
        self.tracker.add_timeout(timeout_info)