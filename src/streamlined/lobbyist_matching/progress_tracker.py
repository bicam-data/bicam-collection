"""
Progress tracking module for bill reference extraction and matching.

This module provides functionality to track the progress of bill reference extraction
and matching runs using SQLite. It maintains state for:
- Run status and stage
- Processed filings and sections
- Run parameters and progress metrics

The tracker uses SQLite for persistence and provides methods to:
- Initialize and track new runs
- Update run status and stage
- Mark filings/sections as processed
- Query progress and state information
"""

import sqlite3
import logging
from typing import Optional, Dict, Any, List
import json

class ProgressTracker:
    """Tracks progress of bill reference extraction and matching runs.
    
    Uses SQLite to maintain state and track progress across runs. Handles run initialization,
    status updates, and progress tracking for filings and sections.
    
    Attributes:
        db_path (str): Path to SQLite database file
        
    Example:
        tracker = ProgressTracker("progress.db")
        tracker.start_run(1, {"sample_size": 1000})
        tracker.mark_filings_processed(1, ["uuid1", "uuid2"])
    """
    
    def __init__(self, db_path: str = "progress.db"):
        """Initialize progress tracker.
        
        Args:
            db_path (str): Path to SQLite database file. Defaults to "progress.db"
        """
        self.db_path = db_path
        self.setup_db()
        
    def setup_db(self):
        """Create SQLite database and required tables.
        
        Creates the following tables if they don't exist:
        - runs: Tracks overall run status and progress
        - processed_filings: Records processed filing UUIDs
        - processed_sections: Records processed section IDs
        """
        conn = sqlite3.connect(self.db_path)
        try:
            c = conn.cursor()
            
            # Create tables
            c.execute("""
                CREATE TABLE IF NOT EXISTS runs (
                    run_id INTEGER PRIMARY KEY,
                    status TEXT NOT NULL,
                    stage TEXT NOT NULL,
                    last_filing_uuid TEXT,
                    last_section_id TEXT,
                    last_reference_id INTEGER,
                    parameters TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            c.execute("""
                CREATE TABLE IF NOT EXISTS processed_filings (
                    run_id INTEGER,
                    filing_uuid TEXT NOT NULL,
                    status TEXT NOT NULL,
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (run_id, filing_uuid)
                )
            """)
            
            c.execute("""
                CREATE TABLE IF NOT EXISTS processed_sections (
                    run_id INTEGER,
                    section_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (run_id, section_id)
                )
            """)
            
            conn.commit()
        finally:
            conn.close()
    
    def start_run(self, run_id: int, parameters: Optional[Dict] = None) -> None:
        """Initialize a new processing run.
        
        Args:
            run_id (int): Unique identifier for the run
            parameters (Optional[Dict]): Run parameters to store. Defaults to None
        """
        conn = sqlite3.connect(self.db_path)
        try:
            c = conn.cursor()
            c.execute("""
                INSERT INTO runs (run_id, status, stage, parameters)
                VALUES (?, 'running', 'extraction', ?)
            """, (run_id, json.dumps(parameters) if parameters else None))
            conn.commit()
            logging.info(f"Started tracking run {run_id}")
        finally:
            conn.close()
    
    def update_stage(self, run_id: int, stage: str) -> None:
        """Update the processing stage of a run.
        
        Args:
            run_id (int): Run identifier
            stage (str): New processing stage
        """
        conn = sqlite3.connect(self.db_path)
        try:
            c = conn.cursor()
            c.execute("""
                UPDATE runs 
                SET stage = ?, updated_at = CURRENT_TIMESTAMP
                WHERE run_id = ?
            """, (stage, run_id))
            conn.commit()
            logging.info(f"Updated run {run_id} to stage: {stage}")
        finally:
            conn.close()
    
    def mark_filings_processed(self, run_id: int, filing_uuids: List[str]) -> None:
        """Mark multiple filings as processed for a run.
        
        Args:
            run_id (int): Run identifier
            filing_uuids (List[str]): List of filing UUIDs to mark as processed
        """
        if not filing_uuids:
            return
            
        conn = sqlite3.connect(self.db_path)
        try:
            c = conn.cursor()
            # Use executemany for better performance
            c.executemany("""
                INSERT OR REPLACE INTO processed_filings (run_id, filing_uuid, status)
                VALUES (?, ?, 'completed')
            """, [(run_id, uuid) for uuid in filing_uuids])
            
            # Update last filing in runs table
            c.execute("""
                UPDATE runs 
                SET last_filing_uuid = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE run_id = ?
            """, (filing_uuids[-1], run_id))
            conn.commit()
        finally:
            conn.close()
    
    def get_last_filing(self) -> Optional[str]:
        """Get the UUID of the last processed filing.
        
        Returns:
            Optional[str]: UUID of last processed filing, or None if no filings processed
        """
        conn = sqlite3.connect(self.db_path)
        try:
            c = conn.cursor()
            c.execute("""
                SELECT last_filing_uuid
                FROM runs
                ORDER BY updated_at DESC
                LIMIT 1
            """)
            result = c.fetchone()
            return result[0] if result else None
        finally:
            conn.close()
    
    def get_unprocessed_filings(self, run_id: int, filing_uuids: List[str]) -> List[str]:
        """Get list of filings that haven't been processed yet.
        
        Args:
            run_id (int): Run identifier
            filing_uuids (List[str]): List of filing UUIDs to check
            
        Returns:
            List[str]: List of filing UUIDs that haven't been processed
        """
        if not filing_uuids:
            return []
            
        conn = sqlite3.connect(self.db_path)
        try:
            c = conn.cursor()
            placeholders = ','.join('?' * len(filing_uuids))
            c.execute(f"""
                SELECT filing_uuid FROM processed_filings
                WHERE run_id = ? AND filing_uuid IN ({placeholders})
            """, (run_id, *filing_uuids))
            
            processed = {row[0] for row in c.fetchall()}
            return [uuid for uuid in filing_uuids if uuid not in processed]
        finally:
            conn.close()
    
    def get_run_progress(self, run_id: int) -> Dict[str, Any]:
        """Get progress information for a run.
        
        Args:
            run_id (int): Run identifier
            
        Returns:
            Dict[str, Any]: Dictionary containing:
                - status: Current run status
                - stage: Current processing stage
                - last_filing_uuid: UUID of last processed filing
                - last_section_id: ID of last processed section
                - parameters: Run parameters
                - processed_filings: Count of processed filings
                - processed_sections: Count of processed sections
                
            Returns None if run not found
        """
        conn = sqlite3.connect(self.db_path)
        try:
            c = conn.cursor()
            c.execute("""
                SELECT status, stage, last_filing_uuid, last_section_id, parameters
                FROM runs
                WHERE run_id = ?
            """, (run_id,))
            row = c.fetchone()
            
            if not row:
                return None
                
            c.execute("""
                SELECT COUNT(*) FROM processed_filings
                WHERE run_id = ?
            """, (run_id,))
            filing_count = c.fetchone()[0]
            
            c.execute("""
                SELECT COUNT(*) FROM processed_sections
                WHERE run_id = ?
            """, (run_id,))
            section_count = c.fetchone()[0]
            
            return {
                'status': row[0],
                'stage': row[1],
                'last_filing_uuid': row[2],
                'last_section_id': row[3],
                'parameters': json.loads(row[4]) if row[4] else None,
                'processed_filings': filing_count,
                'processed_sections': section_count
            }
        finally:
            conn.close() 