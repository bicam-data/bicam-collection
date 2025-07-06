"""
Main module for processing and matching bill references in lobbying filings.

This module provides functionality for:
1. Extracting bill references from filing sections
2. Matching references to actual bills
3. Post-processing and combining nearby matches
4. Managing database connections and system resources
5. Handling interrupts and cleanup

Key classes:
- ResourceManager: Manages system resources and DB connections
- MemoryMonitor: Monitors system memory usage
- CheckpointManager: Manages processing checkpoints for recovery

Key functions:
- process_filings: Main processing pipeline for extracting and matching references
- match_only: Matches references for an existing extraction run
- ensure_schema_exists: Ensures required DB schema exists
- main: CLI entrypoint with argument parsing
"""

import asyncio
import logging
from datetime import datetime
import os
import sys
import asyncpg
from dotenv import load_dotenv
import argparse
import multiprocessing as mp
from typing import Dict, Optional
import signal
import atexit
import gc
import psutil
from contextlib import asynccontextmanager
import socket

from tqdm import tqdm

from db_utils import DatabaseInterface
from batch_processor import BatchProcessor
from timeout_handler import TimeoutTracker
from schema_setup import (
    create_schema, 
    initialize_run,
)
from matcher import MatchingManager
from post_processor import post_process_all

matching_manager = MatchingManager()


class ResourceManager:
    """Manage system resources and database connections.
    
    Handles connection pooling and memory monitoring to prevent resource exhaustion.
    
    Args:
        pool_min: Minimum number of DB connections in pool
        pool_max: Maximum number of DB connections in pool
    """
    def __init__(self, pool_min: int = 2, pool_max: int = 10):
        self.pool_min = pool_min
        self.pool_max = pool_max
        self.memory_monitor = MemoryMonitor(threshold_percent=80)
        
    @asynccontextmanager
    async def get_connection(self, pool: asyncpg.Pool):
        """Get database connection with resource management.
        
        Acquires connection from pool and configures work memory settings.
        Releases connection and triggers GC if memory usage is high.
        
        Args:
            pool: Database connection pool
            
        Yields:
            Database connection
        """
        conn = await pool.acquire()
        try:
            await conn.execute("""
                SET LOCAL work_mem = '64MB';
                SET LOCAL maintenance_work_mem = '128MB';
                SET LOCAL temp_buffers = '32MB';
                SET LOCAL effective_cache_size = '256MB';
            """)
            yield conn
        finally:
            await pool.release(conn)
            if self.memory_monitor.check_memory():
                gc.collect()

class MemoryMonitor:
    """Monitor system memory usage.
    
    Args:
        threshold_percent: Memory usage percentage threshold to trigger GC
    """
    def __init__(self, threshold_percent: int = 80):
        self.threshold = threshold_percent
        
    def check_memory(self) -> bool:
        """Check if memory usage exceeds threshold.
        
        Returns:
            True if memory usage exceeds threshold, False otherwise
        """
        memory = psutil.virtual_memory()
        return memory.percent > self.threshold

class CheckpointManager:
    """Manage processing checkpoints for recovery.
    
    Handles saving and retrieving checkpoint information to allow resuming
    interrupted processing runs.
    
    Args:
        pool: Database connection pool
    """
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool
    
    async def save_checkpoint(self, run_id: int, last_filing):
        """Save processing checkpoint.
        
        Args:
            run_id: ID of processing run
            last_filing: UUID of last processed filing
        """
        # Convert UUID to string if needed
        if hasattr(last_filing, 'hex'):  # If it's a UUID
            last_filing = str(last_filing)
            
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO lobbied_bill_matching.checkpoints 
                (run_id, last_filing_uuid, created_at)
                VALUES ($1, $2::uuid, CURRENT_TIMESTAMP)
                ON CONFLICT (run_id) DO UPDATE
                SET last_filing_uuid = EXCLUDED.last_filing_uuid,
                    created_at = CURRENT_TIMESTAMP
            """, run_id, last_filing)
    
    async def get_last_checkpoint(self, run_id: int) -> Optional[str]:
        """Get last saved checkpoint for a run.
        
        Args:
            run_id: ID of processing run
            
        Returns:
            UUID of last processed filing if exists, None otherwise
        """
        async with self.pool.acquire() as conn:
            return await conn.fetchval("""
                SELECT last_filing_uuid
                FROM lobbied_bill_matching.checkpoints
                WHERE run_id = $1
            """, run_id)
    
    async def get_run_status(self, run_id: int) -> Optional[str]:
        """Get status of processing run.
        
        Args:
            run_id: ID of processing run
            
        Returns:
            Status string if run exists, None otherwise
        """
        async with self.pool.acquire() as conn:
            return await conn.fetchval("""
                SELECT status
                FROM lobbied_bill_matching.processing_runs
                WHERE run_id = $1
            """, run_id)
            
    async def resume_run(self, run_id: int) -> bool:
        """Check if run can be resumed and update status.
        
        Args:
            run_id: ID of processing run
            
        Returns:
            True if run can be resumed, False otherwise
        """
        status = await self.get_run_status(run_id)
        if status == 'running':
            return True
        elif status == 'interrupted':
            async with self.pool.acquire() as conn:
                await conn.execute("""
                    UPDATE lobbied_bill_matching.processing_runs
                    SET status = 'running',
                        end_time = NULL
                    WHERE run_id = $1
                """, run_id)
            return True
        return False

async def ensure_schema_exists(pool: asyncpg.Pool, command_timeout: int = 300):
    """Ensure schema exists without dropping existing data.
    
    Creates schema and tables if they don't exist.
    
    Args:
        pool: Database connection pool
        command_timeout: Timeout in seconds for schema creation
        
    Returns:
        True if schema exists/created successfully, False otherwise
    """
    async with pool.acquire() as conn:
        # Set longer timeout for schema creation
        await conn.execute('SET statement_timeout TO 300000')  # 5 minutes
        
        try:
            # Check if schema exists
            schema_exists = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.schemata 
                    WHERE schema_name = 'lobbied_bill_matching'
                )
            """)
            if not schema_exists:
                return False

            else:
                    # Ensure tables exist with longer timeout
                await conn.execute("""
                        CREATE TABLE IF NOT EXISTS lobbied_bill_matching.processing_runs (
                            run_id SERIAL PRIMARY KEY,
                            parent_run_id INTEGER REFERENCES lobbied_bill_matching.processing_runs(run_id),
                            start_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                            end_time TIMESTAMP WITH TIME ZONE,
                            total_filings INTEGER DEFAULT 0,
                            total_sections INTEGER DEFAULT 0,
                            parameters JSONB,
                            status TEXT DEFAULT 'running',
                            description TEXT
                        );

                        CREATE TABLE IF NOT EXISTS lobbied_bill_matching.checkpoints (
                            run_id INTEGER PRIMARY KEY REFERENCES lobbied_bill_matching.processing_runs(run_id),
                            last_filing_uuid UUID,
                            created_at TIMESTAMP WITH TIME ZONE
                        );

                        CREATE TABLE IF NOT EXISTS lobbied_bill_matching.extracted_references (
                            reference_id SERIAL PRIMARY KEY,
                            run_id INTEGER REFERENCES lobbied_bill_matching.processing_runs(run_id),
                            filing_uuid UUID,
                            section_id TEXT,
                            reference_type TEXT,
                            bill_type TEXT,
                            bill_id TEXT,
                            law_number TEXT,
                            title TEXT,
                            full_match TEXT,
                            start_position INTEGER,
                            end_position INTEGER,
                            is_law BOOLEAN DEFAULT FALSE,
                            congress_number INTEGER,
                            congress_detection_source TEXT,
                            congress_confidence FLOAT,
                            UNIQUE(run_id, filing_uuid, bill_id, law_number, title, section_id)
                        );
                        
                        -- Add any missing indices
                        CREATE INDEX IF NOT EXISTS idx_extracted_refs_run_id 
                        ON lobbied_bill_matching.extracted_references(run_id);
                        
                        CREATE INDEX IF NOT EXISTS idx_extracted_refs_filing 
                        ON lobbied_bill_matching.extracted_references(filing_uuid);
                            """)
                return True
                
        except asyncio.TimeoutError:
            logging.error("Timeout while creating schema, defaulting to schema already exists")
            return True
        except Exception as e:
            logging.error(f"Error creating schema: {str(e)}", exc_info=True)
            return True
        finally:
            # Reset timeout to default
            await conn.execute('RESET statement_timeout')

async def match_only(
    db_config: Dict[str, str],
    extraction_run_id: int,
    description: str = "Matching run",
    max_retries: int = 3,
    retry_delay: int = 5
) -> int:
    """Match references for an existing extraction run and combine nearby matches.
    
    Args:
        db_config: Database connection configuration
        extraction_run_id: ID of extraction run to match
        description: Description for matching run
        max_retries: Maximum number of connection retries
        retry_delay: Delay between retries in seconds
        
    Returns:
        ID of matching run
        
    Raises:
        ValueError: If no references found for extraction run
    """
    db = None
    attempt = 0
    
    while attempt < max_retries:
        try:
            db = await DatabaseInterface.create_pool(
                min_size=2,
                max_size=10,
                **db_config
            )
            
            async with db.pool.acquire() as conn:
                # Disable database timeouts
                await conn.execute('SET statement_timeout TO 0')
                await conn.execute('SET idle_in_transaction_session_timeout TO 0')
                await conn.execute('SET lock_timeout TO 0')
                
                # Get total count first
                total_refs = await conn.fetchval("""
                    SELECT COUNT(*) 
                    FROM lobbied_bill_matching.extracted_references_v2
                    WHERE run_id = $1::INTEGER
                """, extraction_run_id)
                
                if not total_refs:
                    raise ValueError(f"No references found for run ID {extraction_run_id}")
                
                logging.info(f"Found {total_refs} references to copy")
                
                # Create new matching run
                matching_run_id = await conn.fetchval("""
                    INSERT INTO lobbied_bill_matching.processing_runs (
                        parent_run_id,
                        total_filings,
                        total_sections,
                        parameters,
                        status,
                        description
                    )
                    SELECT 
                        $1::INTEGER,
                        total_filings,
                        total_sections,
                        jsonb_build_object(
                            'description', $2::TEXT,
                            'parent_run_id', $1::INTEGER,
                            'type', 'matching_only'
                        ),
                        'running'::TEXT,
                        $2::TEXT
                    FROM lobbied_bill_matching.processing_runs
                    WHERE run_id = $1::INTEGER
                    RETURNING run_id
                """, extraction_run_id, description)
                
                # Copy in batches with progress tracking
                batch_size = 50000
                copied_refs = 0
                
                with tqdm(total=total_refs, desc="Copying references") as pbar:
                    while copied_refs < total_refs:
                        try:
                            # Use asyncio.shield to prevent timeout cancellation during copy
                            async with conn.transaction():
                                await asyncio.shield(conn.execute("""
                                    WITH batch AS (
                                        SELECT 
                                            reference_id,
                                            filing_uuid,
                                            section_id,
                                            reference_type,
                                            bill_type,
                                            bill_id,
                                            law_number,
                                            title,
                                            full_match,
                                            start_position,
                                            end_position,
                                            congress_number,
                                            congress_detection_source,
                                            congress_confidence
                                        FROM lobbied_bill_matching.extracted_references_v2
                                        WHERE run_id = $1::INTEGER
                                        ORDER BY reference_id
                                        OFFSET $3
                                        LIMIT $4
                                    )
                                    INSERT INTO lobbied_bill_matching.extracted_references_v2 (
                                        reference_id,
                                        run_id,
                                        filing_uuid,
                                        section_id,
                                        reference_type,
                                        bill_type,
                                        bill_id,
                                        law_number,
                                        title,
                                        full_match,
                                        start_position,
                                        end_position,
                                        congress_number,
                                        congress_detection_source,
                                        congress_confidence
                                    )
                                    SELECT 
                                        reference_id,
                                        $2::INTEGER as run_id,
                                        filing_uuid,
                                        section_id,
                                        reference_type,
                                        bill_type,
                                        bill_id,
                                        law_number,
                                        title,
                                        COALESCE(full_match, title) as full_match,
                                        start_position,
                                        end_position,
                                        congress_number,
                                        congress_detection_source,
                                        congress_confidence
                                    FROM batch
                                """, extraction_run_id, matching_run_id, copied_refs, batch_size))
                            
                            copied_refs += batch_size
                            pbar.update(batch_size if copied_refs < total_refs else total_refs - (copied_refs - batch_size))
                            await asyncio.sleep(0.1)
                            
                        except asyncio.TimeoutError:
                            # If timeout occurs, continue from where we left off
                            logging.warning(f"Timeout occurred at {copied_refs} refs, continuing...")
                            continue
                        
            logging.info("Combination complete. Starting matching...")
            await matching_manager.match_references(db.pool, matching_run_id)
            
            return matching_run_id
            
        except (asyncpg.exceptions.CannotConnectNowError, asyncpg.exceptions.ConnectionDoesNotExistError) as e:
            attempt += 1
            if attempt < max_retries:
                logging.warning(f"Database connection failed, retrying in {retry_delay} seconds... (Attempt {attempt}/{max_retries})")
                await asyncio.sleep(retry_delay)
                retry_delay *= 2
            else:
                logging.error(f"Failed to connect to database after {max_retries} attempts")
                raise
        except Exception as e:
            logging.error(f"Matching failed: {str(e)}", exc_info=True)
            raise
        finally:
            if db:
                try:
                    await db.close()
                except Exception as e:
                    logging.error(f"Error closing database connection: {str(e)}")

async def process_filings(
    db_config: Dict[str, str],
    sample_size: Optional[int] = None,
    batch_size: int = 50,
    max_concurrent_batches: int = 2,
    max_workers_per_batch: Optional[int] = None,
    min_sections_per_filing: int = 1,
    description: str = "Production run",
    year_range: Optional[tuple] = None
) -> int:
    """Process filings with optimized resource usage.
    
    Main processing pipeline that:
    1. Sets up database schema
    2. Gets filtered sections to process
    3. Processes sections in batches
    4. Matches references
    5. Runs post-processing
    
    Args:
        db_config: Database connection configuration
        sample_size: Number of random sections to process
        batch_size: Number of sections per batch
        max_concurrent_batches: Maximum number of concurrent batches
        max_workers_per_batch: Number of worker processes per batch
        min_sections_per_filing: Minimum sections required per filing
        description: Description for processing run
        year_range: Tuple of (start_year, end_year) to filter filings
        
    Returns:
        ID of processing run
        
    Raises:
        Exception: If processing fails
    """
    
    try:
        db = await DatabaseInterface.create_pool(
            min_size=2,
            max_size=10,
            **db_config
        )
        
        logging.info("Setting up database schema...")
        if not await ensure_schema_exists(db.pool):
            create_schema(db.pool)
        
        # Get sections based on filters
        sections = await db.get_filtered_sections(
            limit=sample_size,
            min_text_length=min_sections_per_filing,
            year_range=year_range,
            random_sample=sample_size is not None
        )
        
        if not sections:
            logging.error("No sections found matching criteria")
            return -1
            
        total_filings = len({section.filing_uuid for section in sections})
        logging.info(f"Processing {len(sections)} sections from {total_filings} filings")
        
        run_id = await initialize_run(
            db.pool,
            total_filings=total_filings,
            total_sections=len(sections),
            parameters={
                'description': description,
                'batch_size': batch_size,
                'max_concurrent_batches': max_concurrent_batches,
                'max_workers_per_batch': max_workers_per_batch or (mp.cpu_count() // 4),
                'min_sections_per_filing': min_sections_per_filing,
                'sample_size': sample_size,
                'year_range': year_range,
                'version': '2.0'
            }
        )
        
        timeout_tracker = TimeoutTracker(db.pool)
        await timeout_tracker.initialize_tracking(run_id)
        
        # Process sections
        processor = BatchProcessor(
            db=db,
            batch_size=batch_size,
            max_concurrent_batches=max_concurrent_batches,
            max_workers_per_batch=max_workers_per_batch or (mp.cpu_count() // 4),
            timeout_tracker=timeout_tracker
        )
        
        results = await processor.process_all_sections(run_id, sections)
        
        logging.info("Initializing matching manager...")
        await matching_manager.initialize(db.pool)
        logging.info("Matching references...")
        await matching_manager.match_references(db.pool, run_id)
        
        # Run post-processing
        logging.info("Running post-processing...")
        await post_process_all(db.pool, run_id)
        
        # Mark run as complete
        async with db.pool.acquire() as conn:
            await conn.execute("""
                UPDATE lobbied_bill_matching.processing_runs
                SET 
                    status = 'completed',
                    end_time = CURRENT_TIMESTAMP
                WHERE run_id = $1
            """, run_id)
        
        return run_id
        
    except Exception as e:
        logging.error(f"Processing failed: {str(e)}", exc_info=True)
        if 'run_id' in locals():
            async with db.pool.acquire() as conn:
                await conn.execute("""
                    UPDATE lobbied_bill_matching.processing_runs
                    SET status = 'failed',
                        end_time = CURRENT_TIMESTAMP
                    WHERE run_id = $1""", run_id)
        raise
    finally:
        await db.close()

def cleanup_resources():
    """Cleanup system resources and file descriptors.
    
    Forces garbage collection and closes file descriptors.
    """
    gc.collect()
    
    # Close file descriptors
    for fd in range(3, 1024):
        try:
            os.close(fd)
        except OSError:
            continue
    
    # Force garbage collection
    gc.collect()

def signal_handler(signum, frame):
    """Handle interrupt signals gracefully.
    
    Cleans up resources before raising KeyboardInterrupt.
    """
    logging.warning("Received interrupt signal. Cleaning up...")
    cleanup_resources()
    raise KeyboardInterrupt()

async def create_db_pool_with_retry(db_config: Dict[str, str], max_retries: int = 5, retry_delay: int = 5) -> Optional[DatabaseInterface]:
    """Create database pool with retries for network issues.
    
    Args:
        db_config: Database connection configuration
        max_retries: Maximum number of connection retries
        retry_delay: Delay between retries in seconds
        
    Returns:
        Database interface if successful, None otherwise
    """
    attempt = 0
    while attempt < max_retries:
        try:
            db = await DatabaseInterface.create_pool(**db_config)
            return db
        except (socket.gaierror, ConnectionError, asyncpg.exceptions.CannotConnectNowError) as e:
            attempt += 1
            if attempt < max_retries:
                logging.warning(f"Database connection failed (attempt {attempt}/{max_retries}), retrying in {retry_delay} seconds...")
                await asyncio.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                logging.error(f"Failed to connect to database after {max_retries} attempts")
                raise
    return None

async def main():
    """Main entry point for bill reference extraction.
    
    Parses command line arguments and runs appropriate processing pipeline.
    Handles cleanup and logging configuration.
    """
    # Register cleanup handler
    atexit.register(cleanup_resources)
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(f'bill_extraction_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
        ]
    )
    
    parser = argparse.ArgumentParser(description='Extract bill references from filing sections')
    parser.add_argument('--sample-size', type=int, default=None,
                       help='Number of random sections to process')
    parser.add_argument('--batch-size', type=int, default=50,
                       help='Number of sections per batch')
    parser.add_argument('--max-batches', type=int, default=2,
                       help='Maximum number of concurrent batches')
    parser.add_argument('--workers-per-batch', type=int,
                       default=max(2, mp.cpu_count() // 4),
                       help='Number of worker processes per batch')
    parser.add_argument('--min-sections', type=int, default=1,
                       help='Minimum number of sections per filing')
    parser.add_argument('--description', type=str, default='Production run',
                       help='Description for this processing run')
    parser.add_argument('--year-start', type=int, default=None,
                       help='Start year for filtering filings')
    parser.add_argument('--year-end', type=int, default=None,
                       help='End year for filtering filings')
    parser.add_argument('--match-only', type=int,
                       help='Extraction run ID to match without new extraction')
    parser.add_argument('--post-process', type=int,
                       help='Run ID to perform all post-processing steps')
    
    args = parser.parse_args()
    
    load_dotenv()
    
    db_config = {
        'host': os.getenv('POSTGRESQL_HOST'),
        'port': int(os.getenv('POSTGRESQL_PORT', 5432)),
        'user': os.getenv('POSTGRESQL_USER'),
        'password': os.getenv('POSTGRESQL_PASSWORD'),
        'database': os.getenv('POSTGRESQL_DB')
    }
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        db = await create_db_pool_with_retry(db_config)
        if not db:
            logging.error("Could not establish database connection")
            return
            
        if args.post_process is not None:
            try:
                await post_process_all(db.pool, args.post_process)
                logging.info(f"Post-processing completed for run ID: {args.post_process}")
            except Exception as e:
                logging.error(f"Error during post-processing: {str(e)}")
                raise
        elif args.match_only is not None:
            try:
                await matching_manager.initialize(db.pool)
                await matching_manager.match_references(db.pool, args.match_only)
                logging.info(f"Matching completed for run ID: {args.match_only}")
            except Exception as e:
                logging.error(f"Error during matching: {str(e)}")
                raise
        else:
            year_range = None
            if args.year_start is not None and args.year_end is not None:
                year_range = (args.year_start, args.year_end)
            
            run_id = await process_filings(
                db_config=db_config,
                sample_size=args.sample_size,
                batch_size=args.batch_size,
                max_concurrent_batches=args.max_batches,
                max_workers_per_batch=args.workers_per_batch,
                min_sections_per_filing=args.min_sections,
                description=args.description,
                year_range=year_range
            )
            logging.info(f"Processing completed successfully with run ID: {run_id}")
    
    except KeyboardInterrupt:
        logging.info("Processing interrupted by user")
        cleanup_resources()
    except Exception as e:
        logging.error(f"Processing failed: {str(e)}", exc_info=True)
        cleanup_resources()
        raise

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Process terminated by user")
    except Exception as e:
        logging.error(f"Process failed: {str(e)}", exc_info=True)
        sys.exit(1)