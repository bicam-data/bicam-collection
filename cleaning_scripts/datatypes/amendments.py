import logging
from pathlib import Path
from cleaner import DataCleaner
import duckdb
import argparse
from typing import List
import os

from cleaning_coordinator import CleaningCoordinator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configure DuckDB to be quiet globally
duckdb.default_connection.execute("SET enable_progress_bar=true")
def extract_amendment_relationships(rel: duckdb.DuckDBPyRelation) -> None:
    """Extract and save amendment relationships before any cleaning"""
    logger.info("Extracting amendment relationships...")
    
    # Convert to pandas DataFrame first
    df = rel.df()
    logger.info(f"Columns available in DataFrame: {df.columns.tolist()}")
    
    # Extract bills using pandas - ensure both columns are not null
    bills = df[['amendment_id', 'amended_bill_id']].dropna(subset=['amendment_id', 'amended_bill_id'])
    bills = bills.rename(columns={'amended_bill_id': 'bill_id'})
    bills.to_parquet('amended_bills.parquet')
    
    # Extract treaties using pandas - ensure both columns are not null
    treaties = df[['amendment_id', 'amended_treaty_id']].dropna(subset=['amendment_id', 'amended_treaty_id'])
    treaties = treaties.rename(columns={'amended_treaty_id': 'treaty_id'})
    treaties.to_parquet('amended_treaties.parquet')
    
    # Extract amendments using pandas - ensure both columns are not null
    amendments = df[['amendment_id', 'amended_amendment_id']].dropna(subset=['amendment_id', 'amended_amendment_id'])
    amendments.to_parquet('amended_amendments.parquet')

def clean_amendments(resume_from: str = None, coordinator: CleaningCoordinator = None):
    """Clean amendments data"""
    logger.info("Starting amendments cleaning process...")

    if coordinator is None:
        coordinator = CleaningCoordinator()
        coordinator.register_module('amendments', clean_amendments, 'congressional')
        coordinator.initialize()
    
    cleaner = DataCleaner("amendments", coordinator=coordinator)
    logger.info(f"Created cleaner with old_schema: {cleaner.old_schema}, new_schema: {cleaner.new_schema}")
    

    # First register our table configs so they'll be created
    amended_table_configs = [
        {
            'old_name': 'NONE',  # Changed to match schema
            'new_name': 'amendments_amended_bills',
            'columns': [
                {'old_name': 'amendment_id', 'new_name': 'amendment_id', 'old_type': 'TEXT', 'new_type': 'TEXT', 'key': True},
                {'old_name': 'bill_id', 'new_name': 'bill_id', 'old_type': 'TEXT', 'new_type': 'TEXT', 'key': True}
            ]
        },
        {
            'old_name': 'NONE',  # Changed to match schema
            'new_name': 'amendments_amended_treaties',
            'columns': [
                {'old_name': 'amendment_id', 'new_name': 'amendment_id', 'old_type': 'TEXT', 'new_type': 'TEXT', 'key': True},
                {'old_name': 'treaty_id', 'new_name': 'treaty_id', 'old_type': 'TEXT', 'new_type': 'TEXT', 'key': True}
            ]
        },
        {
            'old_name': 'NONE',  # Changed to match schema
            'new_name': 'amendments_amended_amendments',
            'columns': [
                {'old_name': 'amendment_id', 'new_name': 'amendment_id', 'old_type': 'TEXT', 'new_type': 'TEXT', 'key': True},
                {'old_name': 'amended_amendment_id', 'new_name': 'amended_amendment_id', 'old_type': 'TEXT', 'new_type': 'TEXT', 'key': True}
            ]
        }
    ]

    # First extract relationships from raw data
    logger.info("Loading raw amendments data...")
    with duckdb.connect() as conn:
        # Load PostgreSQL extension
        conn.install_extension("postgres_scanner")
        conn.load_extension("postgres_scanner")
        
        # Set up PostgreSQL connection string
        conn_string = f"host={os.getenv('POSTGRESQL_HOST')} port={os.getenv('POSTGRESQL_PORT')} dbname={os.getenv('POSTGRESQL_DATABASE')} user={os.getenv('POSTGRESQL_USERNAME')} password={os.getenv('POSTGRESQL_PASSWORD')}"
        
        # Query directly without creating a table
        df = conn.sql(f"""
            SELECT * FROM postgres_scan(
                '{conn_string}',
                '{cleaner.old_schema}',
                '_amendments'
            )
        """)
        
        extract_amendment_relationships(df)
        logger.info("Extracted amendment relationships")
    # Register custom cleaners for each table
    # Main amendments cleaner - cast all outputs to VARCHAR
    cleaner.register_custom_cleaner(
        "_amendments", 
        lambda df: (
            logger.info("Transforming amendments data..."),
            df.select("""
                amendment_id::VARCHAR as amendment_id,
                amendment_type::VARCHAR as amendment_type,
                REGEXP_REPLACE(amendment_number::VARCHAR, '[^0-9]', '')::VARCHAR as amendment_number,
                REGEXP_REPLACE(congress::VARCHAR, '[^0-9]', '')::VARCHAR as congress,
                CASE
                    WHEN chamber = 'Senate' THEN 'senate'
                    WHEN chamber = 'House of Representatives' THEN 'house'
                    ELSE LOWER(chamber)
                END::VARCHAR as chamber,
                purpose::VARCHAR as purpose,
                description::VARCHAR as description,
                proposed_at::VARCHAR as proposed_at,
                submitted_at::VARCHAR as submitted_at,
                CAST(EXISTS (
                    SELECT 1 FROM read_parquet('amended_bills.parquet') ab
                    WHERE ab.amendment_id = amendment_id AND ab.bill_id IS NOT NULL
                ) AS VARCHAR) as is_bill_amendment,
                CAST(EXISTS (
                    SELECT 1 FROM read_parquet('amended_treaties.parquet') at
                    WHERE at.amendment_id = amendment_id AND at.treaty_id IS NOT NULL
                ) AS VARCHAR) as is_treaty_amendment,
                CAST(EXISTS (
                    SELECT 1 FROM read_parquet('amended_amendments.parquet') aa
                    WHERE aa.amendment_id = amendment_id AND aa.amended_amendment_id IS NOT NULL
                ) AS VARCHAR) as is_amendment_amendment,
                notes::VARCHAR as notes,
                REGEXP_REPLACE(actions_count::VARCHAR, '[^0-9]', '')::VARCHAR as actions_count,
                REGEXP_REPLACE(cosponsors_count::VARCHAR, '[^0-9]', '')::VARCHAR as cosponsors_count,
                REGEXP_REPLACE(amendments_to_amendment_count::VARCHAR, '[^0-9]', '')::VARCHAR as amendments_to_amendment_count,
                updated_at::VARCHAR as updated_at
            """)
            .filter("amendment_id IS NOT NULL")
            .filter("chamber IN ('house', 'senate')")
        )[1]
    )

    # Actions cleaner
    cleaner.register_custom_cleaner(
        "_amendments_actions",
        lambda df: (
            df.select("""
                action_id::VARCHAR as action_id,
                amendment_id::VARCHAR as amendment_id,
                action_code::VARCHAR as action_code,
                action_date::VARCHAR as action_date,
                text::VARCHAR as text,
                action_type::VARCHAR as action_type,
                source_system::VARCHAR as source_system,
                REGEXP_REPLACE(source_system_code::VARCHAR, '[^0-9]', '')::VARCHAR as source_system_code
            """)
            .filter("amendment_id IS NOT NULL")
            .filter("action_id IS NOT NULL")
        )
    )

    # Recorded votes cleaner
    cleaner.register_custom_cleaner(
        "_amendments_actions_recorded_votes",
        lambda df: (
            df.select("""
                action_id::VARCHAR as action_id,
                amendment_id::VARCHAR as amendment_id,
                CASE
                    WHEN chamber = 'Senate' THEN 'senate'
                    WHEN chamber = 'House of Representatives' THEN 'house'
                    ELSE LOWER(chamber)
                END::VARCHAR as chamber,
                REGEXP_REPLACE(congress::VARCHAR, '[^0-9]', '')::VARCHAR as congress,
                date::VARCHAR as date,
                REGEXP_REPLACE(roll_number::VARCHAR, '[^0-9]', '')::VARCHAR as roll_number,
                REGEXP_REPLACE(session::VARCHAR, '[^0-9]', '')::VARCHAR as session,
                url::VARCHAR as url
            """)
            .filter("amendment_id IS NOT NULL")
            .filter("action_id IS NOT NULL")
            .filter("chamber IN ('house', 'senate')")
        )
    )

    # Sponsors cleaner
    cleaner.register_custom_cleaner(
        "_amendments_sponsors",
        lambda df: (
            df.select("""
                amendment_id::VARCHAR as amendment_id,
                bioguide_id::VARCHAR as bioguide_id
            """)
            .filter("amendment_id IS NOT NULL")
            .filter("bioguide_id IS NOT NULL")
        )
    )

    # Cosponsors cleaner
    cleaner.register_custom_cleaner(
        "_amendments_cosponsors",
        lambda df: (
            df.select("""
                amendment_id::VARCHAR as amendment_id,
                bioguide_id::VARCHAR as bioguide_id
            """)
            .filter("amendment_id IS NOT NULL")
            .filter("bioguide_id IS NOT NULL")
        )
    )

    # Texts cleaner
    cleaner.register_custom_cleaner(
        "_amendments_texts",
        lambda df: (
            df.select("""
                amendment_id::VARCHAR as amendment_id,
                date::VARCHAR as date,
                type::VARCHAR as type,
                NULL::VARCHAR as raw_text,
                pdf::VARCHAR as pdf,
                html::VARCHAR as html
            """)
            .filter("amendment_id IS NOT NULL")
            .filter("date IS NOT NULL")
            .filter("type IS NOT NULL")
        )
    )
    
    # Run the cleaning process
    logger.info("Starting cleaning process...")
    cleaner.clean_all(resume_from=resume_from)
    
    # Now load and write the extracted tables
    logger.info("Writing extracted amended tables...")
    if Path('amended_bills.parquet').exists():
        amended_bills = duckdb.read_parquet('amended_bills.parquet')
        cleaner.write_data(
            amended_bills, 
            f"{cleaner.new_schema}.amendments_amended_bills",
            key_columns=['amendment_id', 'bill_id']
        )
        Path('amended_bills.parquet').unlink()
        
    if Path('amended_treaties.parquet').exists():
        amended_treaties = duckdb.read_parquet('amended_treaties.parquet')
        cleaner.write_data(
            amended_treaties, 
            f"{cleaner.new_schema}.amendments_amended_treaties",
            key_columns=['amendment_id', 'treaty_id']
        )
        Path('amended_treaties.parquet').unlink()
        
    if Path('amended_amendments.parquet').exists():
        amended_amendments = duckdb.read_parquet('amended_amendments.parquet')
        cleaner.write_data(
            amended_amendments, 
            f"{cleaner.new_schema}.amendments_amended_amendments",
            key_columns=['amendment_id', 'amended_amendment_id']
        )
        Path('amended_amendments.parquet').unlink()
    
    logger.info("Completed amendments cleaning process")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Clean amendments data')
    parser.add_argument('--resume-from', type=str, help='Table name to resume processing from')
    
    args = parser.parse_args()
    clean_amendments(resume_from=args.resume_from)