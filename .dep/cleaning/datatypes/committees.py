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

# Configure DuckDB to be quiet globally
duckdb.default_connection.execute("SET enable_progress_bar=true")
# Set up PostgreSQL connection string
conn_string = f"host={os.getenv('POSTGRESQL_HOST')} port={os.getenv('POSTGRESQL_PORT')} dbname={os.getenv('POSTGRESQL_DATABASE')} user={os.getenv('POSTGRESQL_USERNAME')} password={os.getenv('POSTGRESQL_PASSWORD')}"

def clean_committees_table(df):
    logger.info("Transforming committees data...")
    
    pdf = df.df()
    
    result_df = duckdb.sql("""
        SELECT
            committee_code,
            name,
            CASE
                WHEN chamber IS NOT NULL OR chamber != 'MISSING' THEN LOWER(chamber)
                WHEN LEFT(committee_code, 1) = 'h' THEN 'house'
                WHEN LEFT(committee_code, 1) = 's' THEN 'senate'
                WHEN LEFT(committee_code, 1) = 'j' THEN 'joint'
            END as chamber,
            CASE 
                WHEN LOWER(is_subcommittee) IN ('true', 't', 'yes', 'y', '1') THEN true
                ELSE false 
            END as is_subcommittee,
            CASE 
                WHEN LOWER(is_current) IN ('true', 't', 'yes', 'y', '1') THEN true
                ELSE false 
            END as is_current,
            REGEXP_REPLACE(bills_count::VARCHAR, '[^0-9]', '') as bills_count,
            REGEXP_REPLACE(reports_count::VARCHAR, '[^0-9]', '') as reports_count,
            REGEXP_REPLACE(nominations_count::VARCHAR, '[^0-9]', '') as nominations_count,
            updated_at
        FROM pdf
        WHERE committee_code IS NOT NULL
    """).df()
    
    return result_df

def clean_committees_bills(df):
    logger.info("Transforming committees bills data...")
    
    pdf = df.df()
    
    result_df = duckdb.sql("""
        SELECT
            committee_code,
            bill_id,
            relationship_type,
            committee_action_date,
            updated_at
        FROM pdf
        WHERE committee_code IS NOT NULL
        AND bill_id IS NOT NULL
        AND relationship_type IS NOT NULL
        AND committee_action_date IS NOT NULL
    """).df()
    
    return result_df

def clean_committees_history(df):
    logger.info("Transforming committees history data...")
    
    pdf = df.df()
    
    result_df = duckdb.sql("""
        SELECT
            committee_code,
            name,
            loc_name,
            started_at,
            ended_at,
            LOWER(committee_type) as committee_type,
            establishing_authority,
            su_doc_class_number,
            nara_id,
            loc_linked_data_id,
            updated_at
        FROM pdf
        WHERE committee_code IS NOT NULL
        AND started_at IS NOT NULL
        AND ended_at IS NOT NULL
    """).df()
    
    return result_df

def clean_committees_subcommittees(df):
    logger.info("Transforming committees subcommittees data...")
    
    pdf = df.df()
    
    result_df = duckdb.sql("""
        SELECT
            committee_code,
            subcommittee_code
        FROM pdf
        WHERE committee_code IS NOT NULL
        AND subcommittee_code IS NOT NULL
    """).df()
    
    return result_df

def clean_committees_reports(df):
    logger.info("Starting committees reports transformation...")
    
    try:
        pdf = df.df()
        logger.info("Successfully converted input to DataFrame")
        
        logger.info("Executing SQL transformation...")
        result_df = duckdb.sql(f"""
            WITH base_data AS (
                SELECT
                    committee_code,
                    report_id,
                    -- Flag if report_id contains 'rpt' or has spaces
                    POSITION('rpt' IN report_id) > 0 as has_rpt,
                    POSITION(' ' IN report_id) > 0 as has_space
                FROM pdf
                WHERE committee_code IS NOT NULL
                    AND report_id IS NOT NULL
                    AND TRIM(report_id) != ''
                    AND TRIM(committee_code) != ''
            )
            -- Log intermediate results
            {duckdb.sql("SELECT COUNT(*) FROM base_data").df().iloc[0,0]} as base_count,
            
            committeereports AS (
                SELECT 
                    report_id,
                    citation
                FROM postgres_scan(
                    '{conn_string}',
                    'congressional',
                    'committeereports'
                )
                WHERE citation IS NOT NULL
            ),
            processed_reports AS (
                SELECT 
                    bd.committee_code,
                    CASE
                        WHEN NOT bd.has_rpt AND NOT bd.has_space THEN 
                            SUBSTRING(bd.report_id, 1, 1) || 'rpt' || SUBSTRING(bd.report_id, 2)
                        WHEN bd.has_space THEN COALESCE(cr.report_id, bd.report_id)
                        ELSE bd.report_id
                    END as report_id
                FROM base_data bd
                LEFT JOIN committeereports cr ON bd.report_id = cr.citation
            )
            SELECT DISTINCT
                committee_code,
                report_id
            FROM processed_reports
            WHERE report_id IS NOT NULL
                AND committee_code IS NOT NULL
                AND report_id != 'null'
                AND LENGTH(TRIM(report_id)) > 0  -- Additional check for empty strings
            ORDER BY committee_code, report_id
        """).df()
        
        logger.info(f"SQL transformation complete. Found {len(result_df)} rows before dropna")
        
        # Add validation checks before dropna
        null_report_ids = result_df[result_df['report_id'].isnull()]
        if not null_report_ids.empty:
            logger.error("Found NULL report_ids before dropna:")
            logger.error(null_report_ids)
            
        # Add final validation check
        result_df = result_df.dropna(subset=['report_id'])
        
        logger.info(f"Found {len(result_df)} valid committee reports after dropna")
        if len(result_df) > 0:
            logger.info("Sample of transformed committee reports:")
            logger.info(result_df.head())
        else:
            logger.error("No valid committee reports found after transformation!")
            raise ValueError("No valid committee reports found after transformation")
        
        return result_df
        
    except Exception as e:
        logger.error(f"Error in clean_committees_reports: {str(e)}")
        logger.error("Input DataFrame info:")
        logger.error(df.df().info())
        raise

    # Register cleaners
def clean_committees_base(coordinator: CleaningCoordinator = None, resume_from: str = None):
    """Clean only the base committees table"""
    logger.info("Starting base committees cleaning process...")
    
    if coordinator is None:
        coordinator = CleaningCoordinator()
        coordinator.register_module('committees_base', clean_committees_base, 'congressional')
        coordinator.initialize()
    
    cleaner = DataCleaner("committees", coordinator=coordinator, resume_from=resume_from)
    
    # Clear any existing cleaners
    cleaner.custom_cleaners = {}
    
    # Register only base table cleaners
    cleaner.register_custom_cleaner("_committees", clean_committees_table)
    cleaner.register_custom_cleaner("_committees_history", clean_committees_history)
    cleaner.register_custom_cleaner("_committees_subcommittees_codes", clean_committees_subcommittees)
    
    # Only clean these specific tables
    tables_to_clean = ["_committees", "_committees_history", "_committees_subcommittees_codes"]
    for table in tables_to_clean:
        cleaner.clean_table(table)
    
    logger.info("Completed base committees cleaning process")

def clean_committees_relationships(coordinator: CleaningCoordinator = None, resume_from: str = None):
    """Clean committees relationship tables that depend on other modules"""
    logger.info("Starting committees relationships cleaning process...")
    
    if coordinator is None:
        coordinator = CleaningCoordinator()
        coordinator.register_module('committees_relationships', clean_committees_relationships, 'congressional')
        coordinator.initialize()
    
    cleaner = DataCleaner("committees", coordinator=coordinator, resume_from=resume_from)
    
    # Clear any existing cleaners
    cleaner.custom_cleaners = {}
    
    # Register only relationship cleaners
    cleaner.register_custom_cleaner("_committees_bills", clean_committees_bills)
    cleaner.register_custom_cleaner("_committees_committeereports", clean_committees_reports)
    
    # Only clean these specific tables
    tables_to_clean = ["_committees_bills", "_committees_associated_reports"]
    for table in tables_to_clean:
        cleaner.clean_table(table)
    
    logger.info("Completed committees relationships cleaning process")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Clean committees data')
    parser.add_argument('--resume-from', type=str, help='Table name to resume processing from')

    args = parser.parse_args()
    clean_committees_base(resume_from=args.resume_from)
    logger.info("Completed base committees cleaning process")
    clean_committees_relationships(resume_from=args.resume_from)
    logger.info("Completed committees relationships cleaning process")