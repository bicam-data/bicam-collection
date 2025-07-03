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

def clean_committeeprints(resume_from: str = None, coordinator: CleaningCoordinator = None):
    """Clean committee prints data"""
    logger.info("Starting committee prints cleaning process...")
    
    if coordinator is None:
        coordinator = CleaningCoordinator()
        coordinator.register_module('committeeprints', clean_committeeprints, 'congressional')
        coordinator.initialize()
    
    cleaner = DataCleaner("committeeprints", coordinator=coordinator, resume_from=resume_from)
    logger.info(f"Created cleaner with old_schema: {cleaner.old_schema}, new_schema: {cleaner.new_schema}")

    cleaner.register_custom_cleaner(
        "_committeeprints",
        lambda df: df.select("""
            print_id::VARCHAR as print_id,
            print_jacketnumber::VARCHAR as print_jacketnumber,
            REGEXP_REPLACE(congress::VARCHAR, '[^0-9]', '')::VARCHAR as congress,
            LOWER(chamber)::VARCHAR as chamber,
            title::VARCHAR as title,
            print_number::VARCHAR as print_number,
            citation::VARCHAR as citation,
            updated_at::VARCHAR as updated_at
        """))

    cleaner.register_custom_cleaner(
        "_committeeprints_associated_bill_ids",
        lambda df: df.select("""
            print_id::VARCHAR as print_id,
            bill_id::VARCHAR as bill_id
        """)
        .filter("print_id IS NOT NULL")
        .filter("bill_id IS NOT NULL")
    )

    cleaner.register_custom_cleaner(
        "_committeeprints_committee_codes",
        lambda df: df.select("""
            print_id::VARCHAR as print_id,
            committee_code::VARCHAR AS committee_code
        """)
        .filter("print_id IS NOT NULL")
        .filter("committee_code IS NOT NULL")
    )

    cleaner.register_custom_cleaner(
        "_committeeprints_texts",
        lambda df: df.aggregate("""
            print_id,
            -- Group by print_id and use MAX to combine URLs
            MAX(CASE
                WHEN type = 'Formatted Text' THEN url
                ELSE NULL
            END)::VARCHAR as formatted_text,
            NULL::VARCHAR as raw_text,
            MAX(CASE
                WHEN type = 'PDF' THEN url
                ELSE NULL
            END)::VARCHAR as pdf,
            MAX(CASE
                WHEN type = 'Generated HTML' THEN url
                ELSE NULL
            END)::VARCHAR as html,
            MAX(CASE
                WHEN type = 'Formatted XML' THEN url
                ELSE NULL
            END)::VARCHAR as xml,
            MAX(CASE
                WHEN type = 'Portable Network Graphics' THEN url
                ELSE NULL
            END)::VARCHAR as png
        """, "print_id")
        .filter("print_id IS NOT NULL")
        .filter("print_id != '-99'")
    )
    # Run the cleaning process
    logger.info("Starting cleaning process...")
    cleaner.clean_all(resume_from=resume_from)
    logger.info("Completed committee prints cleaning process")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Clean committee prints data')
    parser.add_argument('--resume-from', type=str, help='Table name to resume processing from')
    
    args = parser.parse_args()
    clean_committeeprints(resume_from=args.resume_from)