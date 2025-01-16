import logging
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).parent.parent))
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

def generate_isbn_table(df: duckdb.DuckDBPyRelation) -> List[tuple[str, duckdb.DuckDBPyRelation]]:
    """Generate ISBN table from congressional directories data"""
    logger.info("Generating ISBN table...")
    
    isbn_table = df.select("""
        package_id,
        isbn
    """).filter("package_id IS NOT NULL")
    
    return [("congressional_directories_isbn", isbn_table)]

def clean_congressional_directories(resume_from: str = None, coordinator: CleaningCoordinator = None):
    """Clean congressional directories data"""
    logger.info("Starting congressional directories cleaning process...")

    if coordinator is None:
        coordinator = CleaningCoordinator()
        coordinator.register_module('congressional_directories', clean_congressional_directories, 'govinfo')
        coordinator.initialize()
    
    cleaner = DataCleaner("congressional_directories", coordinator=coordinator)
    logger.info(f"Created cleaner with old_schema: {cleaner.old_schema}, new_schema: {cleaner.new_schema}")

    # Main directories cleaner - now without isbn
    cleaner.register_custom_cleaner(
        "_congressional_directories",
        lambda df: (
            logger.info("Transforming congressional directories data..."),
            df.select("""
                package_id,
                title,
                congress,
                issued_at,
                branch,
                government_author1,
                government_author2,
                publisher,
                collection_code,
                ils_system_id,
                migrated_doc_id,
                su_doc_class_number,
                text_url,
                pdf_url,
                last_modified
            """)
            .filter("package_id IS NOT NULL")
        )[1]
    )

    # Members cleaner
    cleaner.register_custom_cleaner(
        "_congressional_directories_granules",
        lambda df: (
            df.select("""
                granule_id,
                package_id,
                bioguide_id,
                title,
                biography,
                CASE 
                    WHEN member_type IS NULL THEN NULL
                    ELSE UPPER(SUBSTRING(TRIM(member_type), 1, 1)) || LOWER(SUBSTRING(TRIM(member_type), 2))
                END as member_type,
                population,
                gpo_id,
                authority_id,
                official_url,
                twitter_url,
                instagram_url,
                facebook_url,
                youtube_url,
                other_url
            """)
            .filter("granule_id IS NOT NULL")
            .filter("package_id IS NOT NULL")
            .filter("member_type IS NOT NULL")
        )
    )

    # Run the cleaning process
    logger.info("Starting cleaning process...")
    cleaner.clean_all(resume_from=resume_from)
    logger.info("Completed congressional directories cleaning process")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Clean congressional directories data')
    parser.add_argument('--resume-from', type=str, help='Table name to resume processing from')
    
    args = parser.parse_args()
    clean_congressional_directories(resume_from=args.resume_from) 