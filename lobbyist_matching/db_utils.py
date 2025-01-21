"""
Database utilities module for bill reference extraction and matching.

This module provides database interface functionality for:
- Managing connection pools to PostgreSQL
- Fetching filing sections with flexible filtering
- Bulk inserting matched bill references
- Tracking unmatched sections
- Cleaning up run data

Key classes:
- FilingSection: Immutable data class for filing sections
- DatabaseInterface: Main interface for database operations

The module uses asyncpg for async database operations and includes retry logic
and connection pooling for reliability.
"""

import asyncio
from typing import List, Dict, Any, Optional
import asyncpg
import logging
from dataclasses import dataclass
import uuid

@dataclass(frozen=True)
class FilingSection:
    """Immutable filing section data for safe multiprocessing.
    
    Attributes:
        filing_uuid: Unique identifier for the filing
        section_id: Unique identifier for the section
        text: Issue text content of the section
        filing_year: Year the filing was submitted
    """
    filing_uuid: str
    section_id: str
    text: str
    filing_year: int

class DatabaseInterface:
    """Interface for database operations with connection pooling.
    
    Provides methods for database operations related to bill reference extraction
    and matching, with built-in connection pooling and retry logic.
    
    Attributes:
        pool: asyncpg connection pool for database access
    """
    
    def __init__(self, pool: asyncpg.Pool):
        """Initialize database interface with connection pool.
        
        Args:
            pool: asyncpg connection pool to use for database operations
        """
        self.pool = pool
    
    @classmethod
    async def create_pool(cls, 
                         host: str,
                         port: int,
                         user: str,
                         password: str,
                         database: str,
                         min_size: int = 10,
                         max_size: int = 50) -> 'DatabaseInterface':
        """Create database interface with connection pool.
        
        Args:
            host: Database host address
            port: Database port number
            user: Database username
            password: Database password
            database: Database name
            min_size: Minimum number of connections in pool
            max_size: Maximum number of connections in pool
            
        Returns:
            DatabaseInterface instance with initialized connection pool
        """
        pool = await asyncpg.create_pool(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            min_size=min_size,
            max_size=max_size,
            command_timeout=60
        )
        return cls(pool)

    async def get_filtered_sections(
        self, 
        limit: Optional[int] = None,
        min_text_length: int = 3,
        specific_filings: Optional[List[str]] = None,
        year_range: Optional[tuple] = None,
        random_sample: bool = False,
        batch_size: int = 1000,
        max_retries: int = 3,
        retry_delay: int = 5
    ) -> List[FilingSection]:
        """
        Get filing sections with flexible filtering options and batched fetching.
        
        Args:
            limit: Maximum number of sections to return
            min_text_length: Minimum length of issue text
            specific_filings: List of specific filing UUIDs to include
            year_range: Tuple of (start_year, end_year) to filter
            random_sample: If True, returns a random sample
            batch_size: Number of rows to fetch per batch
            max_retries: Maximum number of retry attempts per batch
            retry_delay: Initial delay between retries (doubles after each retry)
            
        Returns:
            List of FilingSection objects matching the filter criteria
            
        Raises:
            asyncio.TimeoutError: If database query times out
            asyncpg.exceptions.PostgresConnectionError: If database connection fails
        """
        all_sections = []
        total_fetched = 0
        last_section_id = None
        
        while True:
            retry_count = 0
            while retry_count < max_retries:
                try:
                    async with self.pool.acquire() as conn:
                        # Set a longer timeout for large queries
                        await conn.execute('SET statement_timeout = 300000')  # 5 minutes
                        
                        # Build base query
                        if random_sample:
                            query_parts = ["""
                                WITH filtered_sections AS (
                                    SELECT DISTINCT
                                        fs.filing_uuid,
                                        fs.section_id,
                                        COALESCE(fst.issue_text, '') as text,
                                        f.filing_year
                                    FROM relational___lda.filing_sections fs
                                    JOIN relational___lda.filing_sections_text fst 
                                        ON fs.section_id = fst.section_id
                                    JOIN relational___lda.filings f 
                                        ON fs.filing_uuid = f.filing_uuid
                                    WHERE fst.issue_text IS NOT NULL
                                    AND length(fst.issue_text) > $1
                                """]
                        else:
                            query_parts = ["""
                                SELECT DISTINCT
                                    fs.filing_uuid,
                                    fs.section_id,
                                    COALESCE(fst.issue_text, '') as text,
                                    f.filing_year
                                FROM relational___lda.filing_sections fs
                                JOIN relational___lda.filing_sections_text fst 
                                    ON fs.section_id = fst.section_id
                                JOIN relational___lda.filings f 
                                    ON fs.filing_uuid = f.filing_uuid
                                WHERE fst.issue_text IS NOT NULL
                                AND length(fst.issue_text) > $1
                            """]

                        params = [min_text_length]
                        param_counter = 2

                        # Add pagination for non-random queries
                        if not random_sample and last_section_id is not None:
                            query_parts.append(f"AND fs.section_id > ${param_counter}")
                            params.append(last_section_id)
                            param_counter += 1

                        if specific_filings:
                            filing_uuids = [str(f) if isinstance(f, uuid.UUID) else f 
                                        for f in specific_filings]
                            query_parts.append(f"AND fs.filing_uuid = ANY(${param_counter}::uuid[])")
                            params.append(filing_uuids)
                            param_counter += 1

                        if year_range:
                            start_year, end_year = year_range
                            query_parts.append(f"AND f.filing_year BETWEEN ${param_counter} AND ${param_counter + 1}")
                            params.extend([start_year, end_year])
                            param_counter += 2

                        # Handle random sampling
                        if random_sample:
                            query_parts.append(""")
                                SELECT * FROM filtered_sections
                                ORDER BY random()
                            """)
                        else:
                            query_parts.append("ORDER BY fs.section_id")

                        # Add batch limit
                        current_batch_size = min(batch_size, limit - total_fetched if limit else batch_size)
                        query_parts.append(f"LIMIT ${param_counter}")
                        params.append(current_batch_size)

                        query = "\n".join(query_parts)
                        logging.debug(f"Executing query with params: {params}")
                        
                        rows = await conn.fetch(query, *params)
                        
                        if not rows:
                            return all_sections
                        
                        batch_sections = [
                            FilingSection(
                                filing_uuid=str(row['filing_uuid']),
                                section_id=str(row['section_id']),
                                text=row['text'],
                                filing_year=row['filing_year']
                            )
                            for row in rows
                        ]
                        
                        all_sections.extend(batch_sections)
                        total_fetched += len(batch_sections)
                        
                        if not random_sample:
                            last_section_id = str(rows[-1]['section_id'])
                        
                        if limit and total_fetched >= limit:
                            return all_sections[:limit]
                        
                        break  # Success, exit retry loop
                        
                except (asyncio.TimeoutError, asyncpg.exceptions.PostgresConnectionError) as e:
                    retry_count += 1
                    if retry_count == max_retries:
                        logging.error(f"Failed to fetch sections after {max_retries} attempts")
                        raise
                    
                    wait_time = retry_delay * (2 ** (retry_count - 1))  # Exponential backoff
                    logging.warning(f"Fetch attempt {retry_count} failed, retrying in {wait_time}s: {str(e)}")
                    await asyncio.sleep(wait_time)
                    
                except Exception as e:
                    logging.error(f"Unexpected error fetching sections: {str(e)}")
                    raise
            
            if not rows:  # No more rows to fetch
                break
                
            await asyncio.sleep(0.1)  # Small delay between batches
        
        return all_sections
    async def bulk_insert_references(self, conn: asyncpg.Connection, run_id: int,
        references: List[Dict[str, Any]], batch_size: int = 1000) -> None:
        """Insert bill references in bulk with batching.
        
        Args:
            conn: Database connection to use
            run_id: ID of the current processing run
            references: List of reference dictionaries to insert
            batch_size: Number of references to insert per batch
            
        Raises:
            Exception: If batch insertion fails
        """
        if not references:
            logging.debug("No references to insert")
            return
            
        logging.info(f"Inserting {len(references)} references in batches of {batch_size}")
        
        # Process in batches
        for i in range(0, len(references), batch_size):
            batch = references[i:i + batch_size]
            
            try:
                # Extract all values up front for better error handling
                filing_uuids = []
                section_ids = []
                ref_types = []
                bill_types = []
                bill_ids = []
                titles = []
                full_matches = []
                starts = []
                ends = []
                is_laws = []
                law_numbers = []
                congress_numbers = []
                congress_sources = []
                congress_confidences = []
                bill_number_data = []  # Store (ref_index, bill_numbers) tuples
                
                for ref_idx, ref in enumerate(batch):
                    filing_uuids.append(ref['filing_uuid'])
                    section_ids.append(str(ref['section_id']))
                    ref_types.append(ref['type'])
                    bill_types.append(ref.get('bill_type'))
                    bill_ids.append(ref.get('bill_id'))
                    law_numbers.append(ref.get('law_number'))
                    titles.append(ref.get('title'))
                    full_matches.append(ref['text'])
                    starts.append(ref['start'])
                    ends.append(ref['end'])
                    is_laws.append(ref.get('is_law', False))
                    congress_numbers.append(ref.get('congress_number'))
                    congress_sources.append(ref.get('congress_source', 'none'))
                    congress_confidences.append(ref.get('congress_confidence', 0.0))
                    
                    # Store bill numbers for later insertion
                    if ref.get('bill_numbers'):
                        bill_number_data.append((ref_idx, ref['bill_numbers']))

                # Insert main references
                reference_ids = await conn.fetch("""
                    INSERT INTO lobbied_bill_matching.extracted_references (
                        run_id, filing_uuid, section_id, reference_type,
                        bill_type, bill_id, law_number, title, full_match,
                        start_position, end_position, is_law,
                        congress_number, congress_detection_source,
                        congress_confidence
                    )
                    SELECT $1, UNNEST($2::uuid[]), UNNEST($3::text[]),
                        UNNEST($4::text[]), UNNEST($5::text[]),
                        UNNEST($6::text[]), UNNEST($7::text[]), UNNEST($8::text[]),
                        UNNEST($9::text[]), UNNEST($10::int[]),
                        UNNEST($11::int[]), UNNEST($12::boolean[]),
                        UNNEST($13::int[]), UNNEST($14::text[]),
                        UNNEST($15::float[])
                        ON CONFLICT (run_id, filing_uuid, bill_id, law_number, title, section_id)
                        DO NOTHING
                    RETURNING reference_id
                """, run_id, filing_uuids, section_ids, ref_types,
                    bill_types, bill_ids, law_numbers, titles, full_matches,
                    starts, ends, is_laws, congress_numbers,
                    congress_sources, congress_confidences)

                # Insert bill numbers if present
                if bill_number_data:
                    bill_number_values = []
                    for ref_idx, bill_numbers in bill_number_data:
                        # Skip if reference wasn't inserted due to conflict
                        if ref_idx < len(reference_ids):
                            ref_id = reference_ids[ref_idx]['reference_id']
                            for bill_num in bill_numbers:
                                bill_number_values.append((
                                    ref_id,
                                    bill_num.number,
                                    bill_num.is_range_start,
                                    bill_num.is_range_end
                                ))
                    
                    if bill_number_values:
                        await conn.executemany("""
                            INSERT INTO lobbied_bill_matching.bill_numbers (
                                reference_id, bill_number,
                                is_range_start, is_range_end
                            ) VALUES ($1, $2, $3, $4)
                        """, bill_number_values)
                        
            except Exception as e:
                logging.error(f"Error inserting batch: {str(e)}", exc_info=True)
                continue

    async def insert_unmatched_sections(self, conn: asyncpg.Connection, run_id: int,
        unmatched_sections: List[Dict[str, Any]]) -> None:
        """Insert unmatched sections into tracking table.
        
        Args:
            conn: Database connection to use
            run_id: ID of the current processing run
            unmatched_sections: List of section dictionaries that had no matches
        """
        if not unmatched_sections:
            logging.debug("No unmatched sections to insert")
            return
        
        await conn.executemany("""
            INSERT INTO lobbied_bill_matching.unmatched_sections (
                run_id, filing_uuid, section_id, issue_text, filing_year
            )
            VALUES ($1, $2, $3, $4, $5)
        """, [(run_id, section['filing_uuid'], str(section['section_id']), 
              section['text'], section['filing_year']) for section in unmatched_sections])

    async def cleanup_run(self, run_id: int) -> Dict[str, int]:
        """
        Remove all data associated with a specific run_id.
        
        Args:
            run_id: ID of the processing run to clean up
            
        Returns:
            Dictionary mapping table names to number of rows deleted
            
        Note:
            Returns -1 for tables where deletion failed
        """
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                tables = [
                    'bill_numbers',
                    'timeout_sections',
                    'unmatched_sections',
                    'paragraph_matches',
                    'section_paragraphs',
                    'reference_matches',
                    'extracted_references_v2',
                    'extracted_references',
                    'processing_runs'
                ]
                
                results = {}
                for table in tables:
                    try:
                        delete_count = await conn.fetchval(f"""
                            DELETE FROM lobbied_bill_matching.{table}
                            WHERE run_id = $1
                            RETURNING COUNT(*)
                        """, run_id)
                        results[table] = delete_count or 0
                    except Exception as e:
                        logging.error(f"Error cleaning up table {table}: {str(e)}")
                        results[table] = -1  # Mark as error
                        
                return results
    
    async def close(self):
        """Close the connection pool."""
        await self.pool.close()