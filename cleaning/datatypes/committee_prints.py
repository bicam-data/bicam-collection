import logging
from pathlib import Path
import sys

# Add parent directory to Python path
sys.path.append(str(Path(__file__).parent.parent))
import pandas as pd
from cleaner import DataCleaner
import duckdb
import argparse
from typing import List
from cleaning_coordinator import CleaningCoordinator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def clean_committee_prints(resume_from: str = None, coordinator: CleaningCoordinator = None):
    """Clean committee prints data"""
    logger.info("Starting committee prints cleaning process...")

    if coordinator is None:
        coordinator = CleaningCoordinator()
        coordinator.register_module('committee_prints', clean_committee_prints, 'govinfo')
        coordinator.initialize()

    cleaner = DataCleaner("committee_prints", coordinator=coordinator)
    logger.info(f"Created cleaner with old_schema: {cleaner.old_schema}, new_schema: {cleaner.new_schema}")

    # Main committee prints cleaner
    cleaner.register_custom_cleaner(
        "_committee_prints",
        lambda df: (
            df.select("""
                package_id,
                CASE
                WHEN package_id LIKE 'CPRT-%' THEN
                    REPLACE(LOWER(REGEXP_EXTRACT(package_id, '([A-Za-z]+(?:\d+|null))')) || '-' ||
                    REGEXP_EXTRACT(package_id, '\d+'), 'null', '')
                ELSE NULL
                END as print_id,
                title,
                LOWER(chamber) as chamber,
                REGEXP_REPLACE(congress::VARCHAR, '[^0-9]', '')::INTEGER as congress,
                REGEXP_REPLACE(session::VARCHAR, '[^0-9]', '')::INTEGER as session,
                REGEXP_REPLACE(pages::VARCHAR, '[^0-9]', '')::INTEGER as pages,
                document_number,
                issued_at,
                branch,
                government_author1,
                government_author2,
                publisher,
                collection_code,
                migrated_doc_id,
                su_doc_class_number,
                last_modified
            """)
            .filter("package_id IS NOT NULL")
        )
    )

    # Granules cleaner
    cleaner.register_custom_cleaner(
        "_committee_prints_granules",
        lambda df: (
            df.select("""
                granule_id,
                package_id
            """)
            .filter("granule_id IS NOT NULL")
            .filter("package_id IS NOT NULL")
        )
    )

    # Committees cleaner - handles both committee code and name
    cleaner.register_custom_cleaner(
        "_committee_prints_committees",
        lambda df: (
            df.select("""
                print_id,
                granule_id,
                committee_code,
                CASE WHEN committee_name = 'missing' THEN NULL ELSE committee_name END as committee_name,
                chamber
            """)
            .filter("print_id IS NOT NULL")
            .filter("granule_id IS NOT NULL")
        )
    )

    # Reference bills cleaner
    cleaner.register_custom_cleaner(
        "_committee_prints_reference_bills",
        lambda df: (
            df.select("""
                granule_id,
                package_id,
                bill_id
            """)
            .filter("granule_id IS NOT NULL")
            .filter("package_id IS NOT NULL")
            .filter("bill_id IS NOT NULL")
        )
    )

    # Run the cleaning process
    logger.info("Starting cleaning process...")
    cleaner.clean_all(resume_from=resume_from)
    logger.info("Completed committee prints cleaning process")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Clean committee prints data')
    parser.add_argument('--resume-from', type=str, help='Table name to resume processing from')
    
    args = parser.parse_args()
    clean_committee_prints(resume_from=args.resume_from)