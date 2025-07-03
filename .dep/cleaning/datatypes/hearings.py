import logging
import os
from pathlib import Path
import sys

# Add parent directory to Python path
sys.path.append(str(Path(__file__).parent.parent))

from cleaner import DataCleaner
import duckdb
import argparse
from typing import List

from cleaning_coordinator import CleaningCoordinator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def clean_hearings(resume_from: str = None, coordinator: CleaningCoordinator = None):
    """Clean hearings data"""
    logger.info("Starting hearings cleaning process...")
    
    if coordinator is None:
        coordinator = CleaningCoordinator()
        coordinator.register_module('hearings', clean_hearings, 'congressional')
        coordinator.initialize()
    
    cleaner = DataCleaner("hearings", coordinator=coordinator)
    logger.info(f"Created cleaner with old_schema: {cleaner.old_schema}, new_schema: {cleaner.new_schema}")
    
    conn_string = f"host={os.getenv('POSTGRESQL_HOST')} port={os.getenv('POSTGRESQL_PORT')} dbname={os.getenv('POSTGRESQL_DATABASE')} user={os.getenv('POSTGRESQL_USERNAME')} password={os.getenv('POSTGRESQL_PASSWORD')}"

    def clean_hearings_table(df):
        logger.info("Transforming hearings data...")
        
        # Convert to pandas
        pdf = df.df()
        
        # Process with DuckDB SQL
        result_df = duckdb.sql("""
            SELECT DISTINCT  -- Add DISTINCT to handle duplicates
                hearing_jacketnumber,
                CASE 
                    WHEN LOWER(chamber) = 'house' THEN 'hhrg'
                    WHEN LOWER(chamber) = 'senate' THEN 'shrg'
                    ELSE 'jhrg'
                END || 
                REGEXP_REPLACE(hearing_jacketnumber, '-', '') || '-' ||
                REGEXP_REPLACE(congress::VARCHAR, '[^0-9]', '') 
                AS hearing_id,
                loc_id::VARCHAR as loc_id,
                title::VARCHAR as title,
                REGEXP_REPLACE(congress::VARCHAR, '[^0-9]', '')::VARCHAR as congress,
                LOWER(chamber)::VARCHAR as chamber,
                REGEXP_REPLACE(hearing_number::VARCHAR, '[^0-9]', '')::VARCHAR as hearing_number,
                REGEXP_REPLACE(part_number::VARCHAR, '[^0-9]', '')::VARCHAR as part_number,
                citation::VARCHAR as citation,
                updated_at::VARCHAR as updated_at
            FROM pdf
            WHERE hearing_jacketnumber IS NOT NULL
        """).df()   
        
        logger.info("Sample of transformed hearings:")
        logger.info(result_df.head(5))
        
        return result_df

    cleaner.register_custom_cleaner("_hearings", clean_hearings_table)

    cleaner.register_custom_cleaner(
        "_hearings_formats",
        lambda df: (
            # Convert to pandas first
            pdf := df.df(),
            
            # Create new DuckDB connection and query
            duckdb.sql(f"""
                WITH hearing_ids AS (
                    SELECT 
                        h.hearing_jacketnumber,
                        h.chamber,
                        h.congress,
                        CASE 
                            WHEN LOWER(h.chamber) = 'house' THEN 'hhrg'
                            WHEN LOWER(h.chamber) = 'senate' THEN 'shrg'
                            ELSE 'jhrg'
                        END || 
                        REGEXP_REPLACE(h.hearing_jacketnumber, '-', '') || '-' ||
                        REGEXP_REPLACE(h.congress::VARCHAR, '[^0-9]', '') 
                        as hearing_id
                    FROM postgres_scan(
                        '{conn_string}',
                        '{cleaner.old_schema}',
                        '_hearings'
                    ) h
                )
                SELECT
                    hi.hearing_id::VARCHAR AS hearing_id,
                    MAX(CASE
                        WHEN p.type = 'Formatted Text' THEN p.url
                        ELSE NULL
                    END)::VARCHAR as formatted_text,
                    MAX(CASE
                        WHEN p.type = 'Raw Text' THEN p.url
                        ELSE NULL
                    END)::VARCHAR as raw_text,
                    MAX(CASE
                        WHEN p.type = 'PDF' THEN p.url
                        ELSE NULL
                    END)::VARCHAR as pdf,
                    MAX(CASE
                        WHEN p.type = 'Generated HTML' THEN p.url
                        ELSE NULL
                    END)::VARCHAR as html,
                    MAX(CASE
                        WHEN p.type = 'Formatted XML' THEN p.url
                        ELSE NULL
                    END)::VARCHAR as xml,
                    MAX(CASE
                        WHEN p.type = 'Portable Network Graphics' THEN p.url
                        ELSE NULL
                    END)::VARCHAR as png
                FROM pdf p
                JOIN hearing_ids hi ON p.hearing_jacketnumber = hi.hearing_jacketnumber
                WHERE hi.hearing_id IS NOT NULL
                GROUP BY hi.hearing_id
            """).df()
        )[1]
    )
    cleaner.register_custom_cleaner(
        "_hearings_committee_codes",
        lambda df: (
            # Convert to pandas first
            pdf := df.df(),
            
            # Debug: print column names
            logger.info(f"Input columns: {pdf.columns.tolist()}"),
            
            # Create new DuckDB connection and query
            duckdb.sql(f"""
                WITH hearing_ids AS (
                    SELECT 
                        hearing_jacketnumber,
                        CASE 
                            WHEN LOWER(chamber) = 'house' THEN 'hhrg'
                            WHEN LOWER(chamber) = 'senate' THEN 'shrg'
                            ELSE 'jhrg'
                        END || 
                        REGEXP_REPLACE(hearing_jacketnumber, '-', '') || '-' ||
                        REGEXP_REPLACE(congress::VARCHAR, '[^0-9]', '') 
                        AS hearing_id
                    FROM postgres_scan(
                        '{conn_string}',
                        '{cleaner.old_schema}',
                        '_hearings'
                    )
                )
                SELECT
                    hi.hearing_id::VARCHAR as hearing_id,
                    pdf.committee_code::VARCHAR as committee_code
                FROM pdf
                JOIN hearing_ids hi ON pdf.hearing_jacketnumber = hi.hearing_jacketnumber
                WHERE hi.hearing_id IS NOT NULL
                  AND pdf.committee_code IS NOT NULL
            """).df()
        )[2]
    )

    def clean_hearings_dates(df):
        logger.info("Transforming hearings dates...")
        
        pdf = df.df()
        
        result_df = duckdb.sql(f"""
            WITH hearing_ids AS (
                SELECT 
                    hearing_jacketnumber,
                    CASE 
                        WHEN LOWER(chamber) = 'house' THEN 'hhrg'
                        WHEN LOWER(chamber) = 'senate' THEN 'shrg'
                        ELSE 'jhrg'
                    END || 
                    REGEXP_REPLACE(hearing_jacketnumber, '-', '') || '-' ||
                    REGEXP_REPLACE(congress::VARCHAR, '[^0-9]', '') 
                    AS hearing_id
                FROM postgres_scan(
                    '{conn_string}',
                    '{cleaner.old_schema}',
                    '_hearings'
                )
            )
            SELECT
                d.hearing_jacketnumber,  -- Keep for validation
                h.hearing_id,
                d.hearing_date
            FROM pdf d
            JOIN hearing_ids h ON d.hearing_jacketnumber = h.hearing_jacketnumber
            WHERE h.hearing_id IS NOT NULL
                AND d.hearing_date IS NOT NULL
        """).df()
        
        return result_df

    cleaner.register_custom_cleaner("_hearings_dates", clean_hearings_dates)
    
    cleaner.register_custom_cleaner("_hearings", clean_hearings_table, key_columns=["hearing_jacketnumber"])

    # Run the cleaning process
    logger.info("Starting cleaning process...")
    cleaner.clean_all(resume_from=resume_from)
    logger.info("Completed hearings cleaning process")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Clean hearings data')
    parser.add_argument('--resume-from', type=str, help='Table name to resume processing from')
    
    args = parser.parse_args()
    clean_hearings(resume_from=args.resume_from)