"""
Database schema setup and initialization module.

This module handles the creation and management of database schemas for the bill matching system.
It provides functions to:
- Create and initialize all required database tables and indexes
- Set up tracking tables for processing runs and timeouts
- Initialize new processing runs
- Update schema constraints as needed

The schema includes tables for:
- Processing runs tracking
- Extracted bill references
- Bill numbers and ranges
- Reference matches
- Timeout tracking
- Section paragraphs and matches
- Comparison results

Tables are created in the 'lobbied_bill_matching' schema.
"""

import json
from typing import Dict, List, Optional
import asyncpg


async def create_schema(pool: asyncpg.Pool, rebuild_matches_only: bool = False):
    """Create all necessary database tables and indexes.
    
    Creates the complete database schema including tables for tracking runs,
    storing extracted references, matches, timeouts, and comparison results.
    
    Args:
        pool (asyncpg.Pool): Database connection pool
        rebuild_matches_only (bool): If True, only recreates the matches table. 
                                   If False, rebuilds entire schema.
    
    Note:
        Requires user confirmation before proceeding with schema changes.
        Creates indexes for optimized query performance.
        Sets up views for simplified reference querying.
    """
    
    # Get confirmation before proceeding with schema changes
    confirmation = input("\nWARNING: This will drop and recreate database tables. Are you sure you want to proceed? (y/N): ")
    if confirmation.lower() != 'y':
        print("Operation cancelled.")
        return
    
    async with pool.acquire() as conn:
        if not rebuild_matches_only:
            await conn.execute("""
                -- Create schema if it doesn't exist
                CREATE SCHEMA IF NOT EXISTS lobbied_bill_matching;
                
                -- Drop tables if they exist (for clean recreation)
                DROP TABLE IF EXISTS lobbied_bill_matching.extracted_references CASCADE;
                DROP TABLE IF EXISTS lobbied_bill_matching.bill_numbers CASCADE;
                DROP TABLE IF EXISTS lobbied_bill_matching.processing_runs CASCADE;
                DROP TABLE IF EXISTS lobbied_bill_matching.timeout_sections CASCADE;
                DROP TABLE IF EXISTS lobbied_bill_matching.unmatched_sections CASCADE;
                
                -- Table to track processing runs
                CREATE TABLE lobbied_bill_matching.processing_runs (
                    run_id BIGSERIAL PRIMARY KEY,
                    start_time TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    end_time TIMESTAMP WITH TIME ZONE,
                    total_filings INTEGER NOT NULL,
                    total_sections INTEGER NOT NULL,
                    sample_size INTEGER,
                    random_seed INTEGER,
                    parameters JSONB,  -- Store any additional processing parameters
                    status TEXT NOT NULL DEFAULT 'running',
                    error_message TEXT,
                    last_processed_filing UUID,  -- Track last processed filing
                    last_processed_section TEXT,  -- Track last processed section
                    processing_stage TEXT,  -- Track which stage we're in (extraction, matching, etc.)
                    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
                
                -- Table for extracted references
                CREATE TABLE lobbied_bill_matching.extracted_references (
                reference_id BIGSERIAL PRIMARY KEY,
                run_id BIGINT NOT NULL REFERENCES lobbied_bill_matching.processing_runs(run_id),
                filing_uuid UUID NOT NULL,
                section_id TEXT NOT NULL,
                reference_type TEXT NOT NULL CHECK (reference_type IN (
                    'number', 'title', 'number_with_title', 'law', 'law_with_title',
                    'combined', 'combined_original'  -- Add new reference types
                )),
                bill_type TEXT,  -- hr, s, hres, etc.
                bill_id TEXT,    -- Combined bill_type + number (e.g., "hr1234")
                law_number TEXT,
                title TEXT,
                full_match TEXT NOT NULL,
                start_position INTEGER NOT NULL,
                end_position INTEGER NOT NULL,
                is_law BOOLEAN NOT NULL DEFAULT FALSE,
                detected_year INTEGER,
                congress_number INTEGER,
                congress_detection_source TEXT,
                congress_confidence FLOAT,
                created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (run_id, filing_uuid, bill_id, law_number, title, section_id)
            );

                -- Table for bill numbers associated with references
                CREATE TABLE lobbied_bill_matching.bill_numbers (
                    bill_number_id BIGSERIAL PRIMARY KEY,
                    reference_id BIGINT NOT NULL REFERENCES lobbied_bill_matching.extracted_references(reference_id),
                    bill_number TEXT NOT NULL,
                    is_range_start BOOLEAN NOT NULL DEFAULT FALSE,
                    is_range_end BOOLEAN NOT NULL DEFAULT FALSE,
                    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
                
                -- Table for timeout tracking
                CREATE TABLE lobbied_bill_matching.timeout_sections (
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
                );
                
                CREATE TABLE lobbied_bill_matching.unmatched_sections (
                    unmatched_id BIGSERIAL PRIMARY KEY,
                    run_id BIGINT REFERENCES lobbied_bill_matching.processing_runs(run_id),
                    filing_uuid UUID NOT NULL,
                    section_id TEXT NOT NULL,
                    issue_text TEXT NOT NULL,
                    filing_year INTEGER NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
                
                -- Indexes for better query performance
                CREATE INDEX idx_extracted_refs_filing ON lobbied_bill_matching.extracted_references(filing_uuid);
                CREATE INDEX idx_extracted_refs_run ON lobbied_bill_matching.extracted_references(run_id);
                CREATE INDEX idx_extracted_refs_congress ON lobbied_bill_matching.extracted_references(congress_number);
                CREATE INDEX idx_extracted_refs_type ON lobbied_bill_matching.extracted_references(reference_type);
                CREATE INDEX idx_bill_numbers_reference ON lobbied_bill_matching.bill_numbers(reference_id);
                CREATE INDEX idx_bill_numbers_number ON lobbied_bill_matching.bill_numbers(bill_number);
                CREATE INDEX idx_timeout_sections_run ON lobbied_bill_matching.timeout_sections(run_id);
                CREATE INDEX idx_timeout_sections_filing ON lobbied_bill_matching.timeout_sections(filing_uuid);
                
                -- Create view for easy querying of complete references
                CREATE OR REPLACE VIEW lobbied_bill_matching.complete_references AS
                SELECT 
                    er.*,
                    array_agg(DISTINCT bn.bill_number) as bill_numbers,
                    array_agg(DISTINCT 
                        CASE 
                            WHEN bn.is_range_start THEN bn.bill_number 
                        END
                    ) FILTER (WHERE bn.is_range_start) as range_start_numbers,
                    array_agg(DISTINCT 
                        CASE 
                            WHEN bn.is_range_end THEN bn.bill_number 
                        END
                    ) FILTER (WHERE bn.is_range_end) as range_end_numbers
                FROM lobbied_bill_matching.extracted_references er
                LEFT JOIN lobbied_bill_matching.bill_numbers bn ON er.reference_id = bn.reference_id
                GROUP BY er.reference_id, er.run_id, er.filing_uuid, er.section_id, er.reference_type, 
                         er.bill_type, er.title, er.full_match, er.start_position, er.end_position, 
                         er.is_law, er.detected_year, er.congress_number, er.congress_detection_source, 
                         er.congress_confidence, er.created_at;
            """)

        # Always recreate matches table
        await conn.execute("""
            DROP TABLE IF EXISTS lobbied_bill_matching.reference_matches CASCADE;
            
            CREATE TABLE lobbied_bill_matching.reference_matches (
                match_id SERIAL PRIMARY KEY,
                run_id INTEGER REFERENCES lobbied_bill_matching.processing_runs(run_id),
                reference_id INTEGER REFERENCES lobbied_bill_matching.extracted_references(reference_id),
                match_type TEXT NOT NULL,
                confidence_score FLOAT,
                -- Original extracted values
                extracted_title TEXT,
                extracted_bill_number TEXT,
                extracted_law_number TEXT,
                -- Matched values
                matched_congress INTEGER,
                matched_bill_type TEXT,
                matched_bill_number TEXT,
                matched_law_number TEXT,
                matched_title TEXT,  -- Will contain closest match title even for unmatched
                bill_id TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Create year_to_congress function
        await conn.execute("""
            CREATE OR REPLACE FUNCTION lobbied_bill_matching.year_to_congress(year INTEGER) 
            RETURNS INTEGER AS $$
            BEGIN
                RETURN ((year - 1789) / 2) + 1;
            END;
            $$ LANGUAGE plpgsql IMMUTABLE;
        """)

        # Add paragraph tracking table
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS lobbied_bill_matching.section_paragraphs (
                paragraph_id SERIAL PRIMARY KEY,
                run_id INTEGER REFERENCES lobbied_bill_matching.processing_runs(run_id),
                section_id INTEGER NOT NULL,
                paragraph_text TEXT NOT NULL,
                paragraph_ordinal INTEGER NOT NULL,
                start_position INTEGER NOT NULL,
                end_position INTEGER NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE TABLE IF NOT EXISTS lobbied_bill_matching.paragraph_matches (
                paragraph_match_id SERIAL PRIMARY KEY,
                paragraph_id INTEGER REFERENCES lobbied_bill_matching.section_paragraphs(paragraph_id),
                match_id INTEGER REFERENCES lobbied_bill_matching.reference_matches(match_id),
                run_id INTEGER REFERENCES lobbied_bill_matching.processing_runs(run_id),
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (paragraph_id, match_id)
            );
            
            CREATE INDEX IF NOT EXISTS idx_section_paragraphs_run_section 
            ON lobbied_bill_matching.section_paragraphs(run_id, section_id);
            
            CREATE INDEX IF NOT EXISTS idx_paragraph_matches_run 
            ON lobbied_bill_matching.paragraph_matches(run_id);
        """)

        await conn.execute("""
            ALTER TABLE lobbied_bill_matching.reference_matches 
            ADD COLUMN IF NOT EXISTS updated_bill_id TEXT,
            ADD COLUMN IF NOT EXISTS update_source TEXT;  -- To track which process updated it
        """)

        # Add comparison results table
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS lobbied_bill_matching.compare_lobbied_bills (
                comparison_id SERIAL PRIMARY KEY,
                run_id INTEGER REFERENCES lobbied_bill_matching.processing_runs(run_id),
                filing_uuid UUID NOT NULL,
                general_issue_code VARCHAR,
                bill_id TEXT NOT NULL,
                source VARCHAR NOT NULL,  -- 'reference_matches' or 'lobbied_bill_compare'
                issue_text TEXT,
                filing_api_url TEXT,
                filing_document_url TEXT,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE INDEX IF NOT EXISTS idx_compare_lobbied_bills_run 
            ON lobbied_bill_matching.compare_lobbied_bills(run_id);
            
            CREATE INDEX IF NOT EXISTS idx_compare_lobbied_bills_bill 
            ON lobbied_bill_matching.compare_lobbied_bills(bill_id);
        """)

async def get_random_filings(pool: asyncpg.Pool, num_filings: int = None) -> List[str]:
    """Get random filing UUIDs with a minimum number of sections.
    
    Retrieves random filings from the database that have valid issue text sections.
    
    Args:
        pool (asyncpg.Pool): Database connection pool
        num_filings (int, optional): Number of filings to retrieve. If None, returns all.
        
    Returns:
        List[str]: List of filing records containing UUID, section ID and issue text
    """
    async with pool.acquire() as conn:
        if num_filings:
            return await conn.fetch("""
                SELECT
                    f.filing_uuid, fs.section_id, fst.issue_text
                FROM relational___lda.filings f
                JOIN relational___lda.filing_sections fs ON f.filing_uuid = fs.filing_uuid
                JOIN relational___lda.filing_sections_text fst ON fs.section_id = fst.section_id
                WHERE fst.issue_text IS NOT NULL
                AND fst.issue_text != 'null'
                AND fst.issue_text != ''
                AND length(fst.issue_text) > 3
                ORDER BY random()
                LIMIT $1
            """, num_filings)
        else:
            return await conn.fetch("""
                SELECT
                    f.filing_uuid, fs.section_id, fst.issue_text
                FROM relational___lda.filings f
                JOIN relational___lda.filing_sections fs ON f.filing_uuid = fs.filing_uuid
                JOIN relational___lda.filing_sections_text fst ON fs.section_id = fst.section_id
                WHERE fst.issue_text IS NOT NULL
                AND fst.issue_text != 'null'
                AND fst.issue_text != ''
                AND length(fst.issue_text) > 3
            """)

async def initialize_run(
    pool: asyncpg.Pool,
    total_filings: int,
    total_sections: int,
    parameters: Optional[Dict] = None
) -> int:
    """Initialize a new processing run.
    
    Creates a new run record in the processing_runs table.
    
    Args:
        pool (asyncpg.Pool): Database connection pool
        total_filings (int): Total number of filings to process
        total_sections (int): Total number of sections to process
        parameters (Dict, optional): Additional run parameters to store
        
    Returns:
        int: ID of the newly created run
    """
    async with pool.acquire() as conn:
        return await conn.fetchval("""
            INSERT INTO lobbied_bill_matching.processing_runs (
                total_filings,
                total_sections,
                parameters,
                status
            ) VALUES ($1, $2, $3::jsonb, 'running')
            RETURNING run_id
        """,
            total_filings,
            total_sections,
            json.dumps(parameters) if parameters else None
        )

async def update_reference_types(pool: asyncpg.Pool) -> None:
    """Update reference_type check constraint to include new types.
    
    Updates the check constraint on the extracted_references table to include
    all valid reference types. Handles errors gracefully by ensuring a constraint
    always exists.
    
    Args:
        pool (asyncpg.Pool): Database connection pool
    """
    async with pool.acquire() as conn:
        await conn.execute("""
            DO $$
            BEGIN
                -- Drop the existing constraint
                ALTER TABLE lobbied_bill_matching.extracted_references 
                DROP CONSTRAINT IF EXISTS extracted_references_reference_type_check;
                
                -- Add the new constraint with updated types
                ALTER TABLE lobbied_bill_matching.extracted_references 
                ADD CONSTRAINT extracted_references_reference_type_check 
                CHECK (reference_type IN (
                    'number', 'title', 'number_with_title', 'law', 'law_with_title',
                    'combined', 'combined_original'
                ));
            EXCEPTION
                WHEN others THEN
                    -- If there's an error, ensure we still have a constraint
                    ALTER TABLE lobbied_bill_matching.extracted_references 
                    ADD CONSTRAINT extracted_references_reference_type_check 
                    CHECK (reference_type IN (
                        'number', 'title', 'number_with_title', 'law', 'law_with_title',
                        'combined', 'combined_original'
                    ));
            END $$;
        """)