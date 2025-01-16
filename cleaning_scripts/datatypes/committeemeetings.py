import logging
import os
from pathlib import Path
from cleaner import DataCleaner
import duckdb
import argparse
import ast
import time
from typing import List

from cleaning_coordinator import CleaningCoordinator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def parse_and_transform(df: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:
    """Parse address and perform the transformation all in one step"""
    
    # Convert to pandas to handle the dictionary parsing
    pdf = df.df()
    
    # Initialize new columns
    pdf['extracted_building'] = None
    pdf['street_address'] = None
    pdf['city'] = None
    pdf['state'] = None
    pdf['zip_code'] = None
    
    # Process address field
    for idx, row in pdf.iterrows():
        if row['address'] and row['address'] != '{}':
            try:
                # Convert string representation of dict to actual dict
                address_dict = ast.literal_eval(row['address'])
                
                # Extract components
                pdf.at[idx, 'extracted_building'] = address_dict.get('building_name')
                pdf.at[idx, 'street_address'] = address_dict.get('street-address')  # Note the hyphen
                pdf.at[idx, 'city'] = address_dict.get('city')
                pdf.at[idx, 'state'] = address_dict.get('state')
                pdf.at[idx, 'zip_code'] = address_dict.get('postal_code')
            except (ValueError, SyntaxError) as e:
                logger.warning(f"Failed to parse address for meeting {row['meeting_id']}: {e}")
                continue
    
    # Convert back to DuckDB for final transformation
    with duckdb.connect() as conn:
        df_with_address = conn.from_df(pdf)
        
        # Do the final transformation immediately
        result = df_with_address.select("""
            meeting_id::VARCHAR as meeting_id,
            title::VARCHAR as title,
            meeting_type::VARCHAR as meeting_type,
            LOWER(chamber)::VARCHAR as chamber,
            REGEXP_REPLACE(congress::VARCHAR, '[^0-9]', '')::VARCHAR as congress,
            date::VARCHAR as date,
            room::VARCHAR as room,
            COALESCE(building, extracted_building)::VARCHAR as building,
            address::VARCHAR as address,
            street_address::VARCHAR as street_address,
            city::VARCHAR as city,
            state::VARCHAR as state,
            zip_code::VARCHAR as zip_code,
            meeting_status::VARCHAR as meeting_status,
            updated_at::VARCHAR as updated_at
        """).filter("meeting_id IS NOT NULL")
        
        # Convert back to pandas to return
        return result.df()



def clean_committeemeetings(resume_from: str = None, coordinator: CleaningCoordinator = None):
    """Clean committee meetings data"""
    logger.info("Starting committee meetings cleaning process...")
    
    if coordinator is None:
        coordinator = CleaningCoordinator()
        coordinator.register_module('committeemeetings', clean_committeemeetings, 'congressional')
        coordinator.initialize()
    
    cleaner = DataCleaner("committeemeetings", coordinator=coordinator, resume_from=resume_from)
    logger.info(f"Created cleaner with old_schema: {cleaner.old_schema}, new_schema: {cleaner.new_schema}")

    cleaner.register_custom_cleaner(
        "_committeemeetings", 
        lambda df: (
            logger.info("Transforming committee meetings data..."),
            
            # Do all transformations in one step
            result_df := parse_and_transform(df),
            
            # Show debug output
            logger.info("Sample of transformed data:"),
            logger.info(result_df[['meeting_id', 'building', 'street_address', 'city', 'state', 'zip_code']].head()),
            
            # Return the pandas DataFrame directly
            result_df
        )[4]  # Return the pandas DataFrame
    )

    cleaner.register_custom_cleaner(
        "_committeemeetings_associated_bill_ids",
        lambda df: df.select("""
            meeting_id,
            bill_id
        """)
        .filter("meeting_id IS NOT NULL")
        .filter("bill_id IS NOT NULL")
    )

    cleaner.register_custom_cleaner(
        "_committeemeetings_associated_nomination_ids",
        lambda df: df.select("""
            meeting_id,
            nomination_id
        """)
        .filter("meeting_id IS NOT NULL")
        .filter("nomination_id IS NOT NULL")
    )

    cleaner.register_custom_cleaner(
        "_committeemeetings_associated_treaty_ids",
        lambda df: df.select("""
            meeting_id,
            treaty_id
        """)
        .filter("meeting_id IS NOT NULL")
        .filter("treaty_id IS NOT NULL")
    )

    cleaner.register_custom_cleaner(
        "_committeemeetings_committee_codes",
        lambda df: df.select("""
            meeting_id,
            committee_code
        """)
        .filter("meeting_id IS NOT NULL")
        .filter("committee_code IS NOT NULL")
    )

    def clean_committee_meetings_hearings(df):
        logger.info("Transforming committee meetings hearings data...")
        
        pdf = df.df()
        
        # Set up PostgreSQL connection string
        conn_string = f"host={os.getenv('POSTGRESQL_HOST')} port={os.getenv('POSTGRESQL_PORT')} dbname={os.getenv('POSTGRESQL_DATABASE')} user={os.getenv('POSTGRESQL_USERNAME')} password={os.getenv('POSTGRESQL_PASSWORD')}"
        
        result_df = duckdb.sql(f"""
            WITH base_meetings AS (
                SELECT 
                    meeting_id,
                    congress::VARCHAR as congress
                FROM postgres_scan(
                    '{conn_string}',
                    '{cleaner.old_schema}',
                    '_committeemeetings'
                )
            ),
            hearings_data AS (
                SELECT 
                    hearing_id::VARCHAR as hearing_id,
                    hearing_jacketnumber::VARCHAR as hearing_jacketnumber,
                    congress::VARCHAR as congress
                FROM postgres_scan(
                    '{conn_string}',
                    '{cleaner.new_schema}',
                    'hearings'
                )
                WHERE hearing_id IS NOT NULL
                AND hearing_jacketnumber IS NOT NULL
            )
            SELECT DISTINCT
                pdf.meeting_id,
                pdf.hearing_jacketnumber,
                h.hearing_id
            FROM pdf
            JOIN base_meetings m ON pdf.meeting_id = m.meeting_id
            JOIN hearings_data h 
                ON pdf.hearing_jacketnumber = h.hearing_jacketnumber
                AND m.congress = h.congress
            WHERE h.hearing_id IS NOT NULL
            ORDER BY meeting_id, hearing_jacketnumber
        """).df()
        
        logger.info("Sample of transformed meetings hearings:")
        logger.info(result_df.head())
        
        return result_df

    # Then register it:
    cleaner.register_custom_cleaner("_committeemeetings_hearingjacketnumbers", clean_committee_meetings_hearings)

    cleaner.register_custom_cleaner(
        "_committeemeetings_meeting_documents",
        lambda df: df.select("""
            meeting_id,
            name,
            document_type,
            description,
            url
        """)
        .filter("meeting_id IS NOT NULL")
        .filter("url IS NOT NULL")
    )

    cleaner.register_custom_cleaner(
        "_committeemeetings_witness_documents",
        lambda df: df.select("""
            meeting_id,
            document_type,
            url
        """)
        .filter("meeting_id IS NOT NULL")
        .filter("url IS NOT NULL")
    )

    cleaner.register_custom_cleaner(
        "_committeemeetings_witnesses",
        lambda df: df.select("""
            meeting_id,
            name,
            organization,
            position
        """)
        .filter("meeting_id IS NOT NULL")
        .filter("name IS NOT NULL")
        .filter("organization IS NOT NULL")
        .filter("position IS NOT NULL")
    )

    # Run the cleaning process
    logger.info("Starting cleaning process...")
    cleaner.clean_all(resume_from=resume_from)
    logger.info("Completed committee meetings cleaning process")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Clean committee meetings data')
    parser.add_argument('--resume-from', type=str, help='Table name to resume processing from')
    
    args = parser.parse_args()
    clean_committeemeetings(resume_from=args.resume_from)