import logging
from pathlib import Path
from cleaner import DataCleaner
import duckdb
import argparse

from cleaning_coordinator import CleaningCoordinator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configure DuckDB to be quiet globally
duckdb.default_connection.execute("SET enable_progress_bar=true")

def clean_members(resume_from: str = None, coordinator: CleaningCoordinator = None):
    """Clean members data"""
    logger.info("Starting members cleaning process...")

    if coordinator is None:
        coordinator = CleaningCoordinator()
        coordinator.register_module('members', clean_members, 'congressional')
        coordinator.initialize()
    
    cleaner = DataCleaner("members", coordinator=coordinator, resume_from=resume_from)
    logger.info(f"Created cleaner with old_schema: {cleaner.old_schema}, new_schema: {cleaner.new_schema}")

    # Register custom cleaners for each table
    cleaner.register_custom_cleaner(
    "_members",
    lambda df: df.select("""
        bioguide_id,
        direct_order_name,
        inverted_order_name,
        honorific_prefix,
        first_name,
        middle_name,
        last_name,
        suffix,
        nickname,
        party,
        state,
        district,
        birth_year,
        death_year,
        official_url,
        office_address,
        office_city,
        office_district,
        office_zip,
        office_phone,
        sponsored_legislation_count,
        cosponsored_legislation_count,
        depiction_image_url,
        depiction_attribution,
        is_current_member,
        updated_at,
        CASE 
            WHEN middle_name IS NOT NULL THEN UPPER(first_name) || ' ' || UPPER(middle_name) || ' ' || UPPER(last_name)
            WHEN middle_name IS NULL THEN UPPER(first_name) || ' ' || UPPER(last_name)
            ELSE UPPER(first_name) || ' ' || UPPER(last_name)
        END as normalized_name
    """)
)

    cleaner.register_custom_cleaner(
        "_members_leadership_roles",
        lambda df: (
            df.select("""
                bioguide_id,
                -- Standardize chamber names as specified
                CASE
                    WHEN chamber = 'Senate' THEN 'senate'
                    WHEN chamber = 'House of Representatives' THEN 'house'
                    ELSE LOWER(chamber)
                END as chamber,
                -- Clean numeric and boolean fields
                REGEXP_REPLACE(congress::VARCHAR, '[^0-9]', '') as congress,
                CASE 
                    WHEN LOWER(is_current) IN ('true', 't', 'yes', 'y', '1') THEN 'true'
                    WHEN LOWER(is_current) IN ('false', 'f', 'no', 'n', '0') THEN 'false'
                    ELSE NULL 
                END as is_current,
                role
            """)
            .filter("bioguide_id IS NOT NULL")
            .filter("chamber IN ('house', 'senate')")
        )
    )

    cleaner.register_custom_cleaner(
        "_members_party_history",
        lambda df: (
            df.select("""
                bioguide_id,
                party_code,
                party_name,
                REGEXP_REPLACE(start_year::VARCHAR, '[^0-9]', '') as start_year,
                REGEXP_REPLACE(end_year::VARCHAR, '[^0-9]', '') as end_year
            """)
            .filter("bioguide_id IS NOT NULL")
            .filter("party_code IS NOT NULL")
            .filter("TRY_CAST(start_year AS INTEGER) IS NOT NULL")
        )
    )

    cleaner.register_custom_cleaner(
        "_members_terms",
        lambda df: (
            df.select("""
                bioguide_id,
                member_type,
                -- Standardize chamber names as specified
                CASE
                    WHEN chamber = 'Senate' THEN 'senate'
                    WHEN chamber = 'House of Representatives' THEN 'house'
                    ELSE LOWER(chamber)
                END as chamber,
                REGEXP_REPLACE(congress::VARCHAR, '[^0-9]', '') as congress,
                REGEXP_REPLACE(start_year::VARCHAR, '[^0-9]', '') as start_year,
                REGEXP_REPLACE(end_year::VARCHAR, '[^0-9]', '') as end_year,
                state_name,
                state_code,
                REGEXP_REPLACE(district::VARCHAR, '[^0-9]', '') as district
            """)
            .filter("bioguide_id IS NOT NULL")
            .filter("chamber IN ('house', 'senate')")
            .filter("TRY_CAST(start_year AS INTEGER) IS NOT NULL")
            .filter("TRY_CAST(end_year AS INTEGER) IS NOT NULL")
        )
    )

    # Run the cleaning process
    logger.info("Starting cleaning process...")
    cleaner.clean_all(resume_from=resume_from)
    logger.info("Completed members cleaning process")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Clean members data')
    parser.add_argument('--resume-from', type=str, help='Table name to resume processing from')

    args = parser.parse_args()
    clean_members(resume_from=args.resume_from)