import logging
import os
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

def clean_nominations(resume_from: str = None, coordinator: CleaningCoordinator = None):
    """Clean nominations data"""
    logger.info("Starting nominations cleaning process...")

    if coordinator is None:
        coordinator = CleaningCoordinator()
        coordinator.register_module('nominations', clean_nominations, 'congressional')
        coordinator.initialize()
    
    cleaner = DataCleaner("nominations", coordinator=coordinator)
    logger.info(f"Created cleaner with old_schema: {cleaner.old_schema}, new_schema: {cleaner.new_schema}")
    

        # Set up PostgreSQL connection string
    conn_string = f"host={os.getenv('POSTGRESQL_HOST')} port={os.getenv('POSTGRESQL_PORT')} dbname={os.getenv('POSTGRESQL_DATABASE')} user={os.getenv('POSTGRESQL_USERNAME')} password={os.getenv('POSTGRESQL_PASSWORD')}"
    with duckdb.connect() as conn:

        # Query directly without creating a table
        df = conn.sql(f"""
            SELECT hearing_id, hearing_jacketnumber, congress
            FROM postgres_scan(
                '{conn_string}',
                '{cleaner.new_schema}',
                'hearings'
            )
            """)
        hearings_arrow = df.arrow()
        
    cleaner.register_custom_cleaner(
        "_nominations", 
        lambda df: (
            pdf := df.df(),
            duckdb.sql("""
            SELECT
                nomination_id,
                nomination_number,
                REGEXP_REPLACE(part_number::VARCHAR, '[^0-9]', '') as part_number,
                REGEXP_REPLACE(congress::VARCHAR, '[^0-9]', '') as congress,
                description,
                CASE 
                    WHEN LOWER(is_privileged) IN ('true', 't', 'yes', 'y', '1') THEN true
                    ELSE false 
                END as is_privileged,
                CASE 
                    WHEN LOWER(is_civilian) IN ('true', 't', 'yes', 'y', '1') THEN true
                    ELSE false 
                END as is_civilian,
                received_at,
                authority_date,
                executive_calendar_number,
                citation,
                REGEXP_REPLACE(committees_count::VARCHAR, '[^0-9]', '') as committees_count,
                REGEXP_REPLACE(actions_count::VARCHAR, '[^0-9]', '') as actions_count,
                updated_at
            FROM pdf
            WHERE nomination_id IS NOT NULL
        """)
    )[0]
    )
    cleaner.register_custom_cleaner(
        "_nominations_actions",
        lambda df: (
            pdf := df.df(),
            duckdb.sql("""
            SELECT
                action_id,
                nomination_id,
                action_code,
                action_type,
                action_date,
                text
            FROM pdf
            WHERE nomination_id IS NOT NULL
            AND action_id IS NOT NULL
        """)
    )[0]
    )

    cleaner.register_custom_cleaner(
        "_nominations_actions_committee_codes",
        lambda df: (
            pdf := df.df(),
            duckdb.sql("""
            SELECT
                action_id,
                nomination_id,
                committee_code
            FROM pdf
            WHERE action_id IS NOT NULL
            AND nomination_id IS NOT NULL
            AND committee_code IS NOT NULL
        """)
    )[0]
    )

    cleaner.register_custom_cleaner(
        "_nominations_committeeactivities",
        lambda df: (
            pdf := df.df(),
            duckdb.sql("""
            SELECT
                nomination_id,
                committee_code,
                activity_name,
                activity_date
            FROM pdf
            WHERE nomination_id IS NOT NULL
            AND committee_code IS NOT NULL
            AND activity_name IS NOT NULL
            AND activity_date IS NOT NULL
        """)
    )[0]
    )


    def clean_nominations_hearings(df):
        logger.info("Transforming nominations hearings...")
        
        # Convert to pandas
        pdf = df.df()
        
        # Set up connection string within function scope
        conn_string = f"host={os.getenv('POSTGRESQL_HOST')} port={os.getenv('POSTGRESQL_PORT')} dbname={os.getenv('POSTGRESQL_DATABASE')} user={os.getenv('POSTGRESQL_USERNAME')} password={os.getenv('POSTGRESQL_PASSWORD')}"
        
        # Process with DuckDB SQL
        result_df = duckdb.sql(f"""
            WITH nomination_congresses AS (
                SELECT
                    nomination_id,
                    hearing_jacketnumber,
                    CAST(SPLIT_PART(nomination_id, '-', 3) AS INTEGER) as congress
                FROM pdf
                WHERE hearing_jacketnumber IS NOT NULL
            ),
            hearings_data AS (
                SELECT 
                    hearing_id::VARCHAR as hearing_id, 
                    hearing_jacketnumber::VARCHAR as hearing_jacketnumber, 
                    CAST(congress AS INTEGER) as congress
                FROM postgres_scan(
                    '{conn_string}',
                    '{cleaner.new_schema}',
                    'hearings'
                )
                WHERE hearing_id IS NOT NULL
                AND hearing_jacketnumber IS NOT NULL
            )
            SELECT DISTINCT
                n.nomination_id,
                h.hearing_id
            FROM nomination_congresses n
            JOIN hearings_data h 
                ON n.hearing_jacketnumber = h.hearing_jacketnumber
                AND CAST(SPLIT_PART(nomination_id, '-', 3) AS INTEGER) = h.congress
            WHERE h.hearing_id IS NOT NULL
        """).df()
        
        logger.info(f"Found {len(result_df)} matching hearing records")
        if len(result_df) > 0:
            logger.info("Sample of transformed nominations hearings:")
            logger.info(result_df.head())
        else:
            logger.warning("No matching hearing records found")
        
        return result_df

    cleaner.register_custom_cleaner(
        "_nominations_hearings",
        clean_nominations_hearings
    )

    cleaner.register_custom_cleaner(
        "_nominations_nominees",
        lambda df: (
            pdf := df.df(),
            duckdb.sql("""
            SELECT
                nomination_id,
                REGEXP_REPLACE(ordinal::VARCHAR, '[^0-9]', '') as ordinal,
                first_name,
                middle_name,
                last_name,
                suffix,
                state,
                effective_date,
                predecessor_name,
                corps_code
            FROM pdf
            WHERE nomination_id IS NOT NULL
            AND first_name IS NOT NULL
            AND last_name IS NOT NULL
        """)
    )[0]
    )

    cleaner.register_custom_cleaner(
        "_nominations_nomineepositions",
        lambda df: (
            pdf := df.df(),
            duckdb.sql("""
            SELECT
                nomination_id,
                REGEXP_REPLACE(ordinal::VARCHAR, '[^0-9]', '') as ordinal,
                position_title,
                organization,
                intro_text,
                REGEXP_REPLACE(nominee_count::VARCHAR, '[^0-9]', '') as nominee_count
            FROM pdf
            WHERE nomination_id IS NOT NULL
            AND ordinal IS NOT NULL
        """)
    )[0]
    )

    # Run the cleaning process

    logger.info("Starting cleaning process...")
    cleaner.clean_all(resume_from=resume_from)
    logger.info("Completed nominations cleaning process")

        
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Clean nominations data')
    parser.add_argument('--resume-from', type=str, help='Table name to resume processing from')
    
    args = parser.parse_args()
    clean_nominations(resume_from=args.resume_from)