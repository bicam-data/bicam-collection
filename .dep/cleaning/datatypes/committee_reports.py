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

def clean_committee_reports(resume_from: str = None, coordinator: CleaningCoordinator = None):
    """Clean committee reports data"""
    logger.info("Starting committee reports cleaning process...")

    if coordinator is None:
        coordinator = CleaningCoordinator()
        coordinator.register_module('congressional_reports', clean_committee_reports, 'govinfo')
        coordinator.initialize()

    cleaner = DataCleaner("congressional_reports", coordinator=coordinator)
    logger.info(f"Created cleaner with old_schema: {cleaner.old_schema}, new_schema: {cleaner.new_schema}")

    # Main committee reports cleaner
    cleaner.register_custom_cleaner(
        "_congressional_reports",
        lambda df: (
            df.select("""
                package_id,
                -- Transform report_id (e.g., CRPT-118hrpt860 -> hrpt860-118)
                CASE 
                    WHEN package_id LIKE 'CRPT-%' THEN
                        LOWER(REGEXP_EXTRACT(package_id, '[A-Za-z]+(?:\d+)')) || '-' ||
                        REGEXP_EXTRACT(package_id, '\d+')
                    ELSE lower(report_id)
                END as report_id,
                title,
                subtitle,
                LOWER(chamber) as chamber,
                REGEXP_REPLACE(congress::VARCHAR, '[^0-9]', '')::INTEGER as congress,
                REGEXP_REPLACE(session::VARCHAR, '[^0-9]', '')::INTEGER as session,
                REGEXP_REPLACE(pages::VARCHAR, '[^0-9]', '')::INTEGER as pages,
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
        "_congressional_reports_granules",
        lambda df: (
            df.select("""
                granule_id,
                package_id
            """)
            .filter("granule_id IS NOT NULL")
            .filter("package_id IS NOT NULL")
        )
    )

    # Committees cleaner
    cleaner.register_custom_cleaner(
        "_congressional_reports_granules_committees",
        lambda df: (
            df.select("""
                granule_id,
                package_id,
                committee_code,
                committee_name,
                chamber
            """)
            .filter("granule_id IS NOT NULL")
            .filter("package_id IS NOT NULL")
        )
    )

    # Reference bills cleaner
    cleaner.register_custom_cleaner(
        "_congressional_reports_granules_reference_bills",
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
    
    cleaner.register_custom_cleaner(
        "_congressional_reports_granules_members",
        lambda df: (
            df.select("""
                granule_id,
                package_id,
                bioguide_id
                """)
            .filter("granule_id IS NOT NULL")
            .filter("package_id IS NOT NULL")
        )
    )
    

    # Run the cleaning process
    logger.info("Starting cleaning process...")
    cleaner.clean_all(resume_from=resume_from)
    logger.info("Completed committee reports cleaning process")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Clean committee reports data')
    parser.add_argument('--resume-from', type=str, help='Table name to resume processing from')
    
    args = parser.parse_args()
    clean_committee_reports(resume_from=args.resume_from)