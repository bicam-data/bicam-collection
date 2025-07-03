import logging
from pathlib import Path
from cleaner import DataCleaner
import duckdb
import argparse
from typing import List

from cleaning_coordinator import CleaningCoordinator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configure DuckDB to be quiet globally
duckdb.default_connection.execute("SET enable_progress_bar=true")

def clean_congresses(resume_from: str = None, coordinator: CleaningCoordinator = None):
    """Clean congresses data"""
    logger.info("Starting congresses cleaning process...")
    
    if coordinator is None:
        coordinator = CleaningCoordinator()
        coordinator.register_module('congresses', clean_congresses, 'congressional')
        coordinator.initialize()
    
    cleaner = DataCleaner("congresses", coordinator=coordinator)
    logger.info(f"Created cleaner with old_schema: {cleaner.old_schema}, new_schema: {cleaner.new_schema}")

    def clean_congresses_table(df):
        logger.info("Transforming congresses data...")
        
        pdf = df.df()
        
        result_df = duckdb.sql("""
            SELECT
                REGEXP_REPLACE(congress_number::VARCHAR, '[^0-9]', '') as congress_number,
                name,
                REGEXP_REPLACE(start_year::VARCHAR, '[^0-9]', '') as start_year,
                REGEXP_REPLACE(end_year::VARCHAR, '[^0-9]', '') as end_year,
                updated_at
            FROM pdf
            WHERE congress_number IS NOT NULL
        """).df()
        
        logger.info("Sample of transformed congresses:")
        logger.info(result_df.head())
        
        return result_df

    def clean_congresses_sessions(df):
        logger.info("Transforming congresses sessions data...")
        
        pdf = df.df()
        
        result_df = duckdb.sql("""
            SELECT
                REGEXP_REPLACE(congress_number::VARCHAR, '[^0-9]', '') as congress_number,
                REGEXP_REPLACE(session::VARCHAR, '[^0-9]', '') as session,
                CASE 
                    WHEN LOWER(chamber) LIKE '%%house%%' THEN 'house'
                    WHEN LOWER(chamber) LIKE '%%senate%%' THEN 'senate'
                    ELSE LOWER(chamber)
                END as chamber,
                type,
                start_date,
                end_date
            FROM pdf
            WHERE congress_number IS NOT NULL
            AND session IS NOT NULL
            AND chamber IS NOT NULL
            AND type IS NOT NULL
        """).df()
        
        logger.info("Sample of transformed sessions:")
        logger.info(result_df.head())
        
        return result_df

    # Register cleaners
    cleaner.register_custom_cleaner("_congresses", clean_congresses_table)
    cleaner.register_custom_cleaner("_congresses_sessions", clean_congresses_sessions)

    # Run the cleaning process
    logger.info("Starting cleaning process...")
    cleaner.clean_all(resume_from=resume_from)
    logger.info("Completed congresses cleaning process")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Clean congresses data')
    parser.add_argument('--resume-from', type=str, help='Table name to resume processing from')
    
    args = parser.parse_args()
    clean_congresses(resume_from=args.resume_from)