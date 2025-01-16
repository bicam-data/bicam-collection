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

def clean_committeereports(resume_from: str = None, coordinator: CleaningCoordinator = None):
    """Clean committee reports data"""
    logger.info("Starting committee reports cleaning process...")
    
    if coordinator is None:
        coordinator = CleaningCoordinator()
        coordinator.register_module('committeereports', clean_committeereports, 'congressional')
        coordinator.initialize()

    cleaner = DataCleaner("committeereports", coordinator=coordinator, resume_from=resume_from)
    logger.info(f"Created cleaner with old_schema: {cleaner.old_schema}, new_schema: {cleaner.new_schema}")
    logger.info("Transforming committee reports data..."),
    # Show original data sample
    logger.info("Original data sample:"),
    cleaner.register_custom_cleaner(
        "_committeereports", 
        lambda df: (df.select("""
            -- First determine report type based on original report_id
            CASE 
                WHEN report_id LIKE 'H%' OR report_type LIKE 'H%' THEN 'hrpt'
                WHEN report_id LIKE 'S%' OR report_type LIKE 'S%' THEN 'srpt'
                WHEN report_id LIKE 'E%' OR report_type LIKE 'E%' THEN 'erpt'
                ELSE NULL
            END::VARCHAR as report_type,
            
            REGEXP_REPLACE(report_number::VARCHAR, '[^0-9]', '')::VARCHAR as report_number,
            REGEXP_REPLACE(report_part::VARCHAR, '[^0-9]', '')::VARCHAR as report_part,
            REGEXP_REPLACE(congress::VARCHAR, '[^0-9]', '')::VARCHAR as congress,
            session::VARCHAR as session,
            title::VARCHAR as title,
            
            -- Standardize chamber names
            LOWER(chamber)::VARCHAR as chamber,
            
            citation::VARCHAR as citation,
            -- Convert boolean
            CASE 
                WHEN LOWER(is_conference_report) IN ('true', 't', 'yes', 'y', '1') THEN 'true'
                ELSE 'false' 
            END::VARCHAR as is_conference_report,
            
            issued_at::VARCHAR as issued_at,
            REGEXP_REPLACE(texts_count::VARCHAR, '[^0-9]', '')::VARCHAR as texts_count,
            updated_at::VARCHAR as updated_at,
            
            -- Create new report_id
            CASE 
                WHEN REGEXP_REPLACE(report_part::VARCHAR, '[^0-9]', '') IS NOT NULL 
                    AND TRIM(REGEXP_REPLACE(report_part::VARCHAR, '[^0-9]', '')) != '' THEN 
                    (CASE 
                        WHEN report_id LIKE 'H%' OR report_type LIKE 'H%' THEN 'hrpt'
                        WHEN report_id LIKE 'S%' OR report_type LIKE 'S%' THEN 'srpt'
                        WHEN report_id LIKE 'E%' OR report_type LIKE 'E%' THEN 'erpt'
                    END) || 
                    REGEXP_REPLACE(report_number::VARCHAR, '[^0-9]', '') || '-' ||
                    REGEXP_REPLACE(report_part::VARCHAR, '[^0-9]', '') || '-' ||
                    REGEXP_REPLACE(congress::VARCHAR, '[^0-9]', '')
                ELSE 
                    (CASE 
                        WHEN report_id LIKE 'H%' OR report_type LIKE 'H%' THEN 'hrpt'
                        WHEN report_id LIKE 'S%' OR report_type LIKE 'S%' THEN 'srpt'
                        WHEN report_id LIKE 'E%' OR report_type LIKE 'E%' THEN 'erpt'
                    END) || 
                    REGEXP_REPLACE(report_number::VARCHAR, '[^0-9]', '') || '-' ||
                    REGEXP_REPLACE(congress::VARCHAR, '[^0-9]', '')
            END::VARCHAR as report_id
        """))  # Close tuple after the select
        .filter("report_id != '-99'")
    )

    cleaner.register_custom_cleaner(
        "_committeereports_associated_bills_ids",
        lambda df: df.select("""
            REGEXP_REPLACE(REGEXP_REPLACE(report_id, 'rept', ''), 'x', '')::VARCHAR as report_id,
            bill_id::VARCHAR as bill_id
        """)
        .filter("report_id IS NOT NULL")
        .filter("bill_id IS NOT NULL")
    )

    # Associated treaties
    cleaner.register_custom_cleaner(
        "_committeereports_associated_treaties_ids",
        lambda df: df.select("""
            REGEXP_REPLACE(REGEXP_REPLACE(report_id, 'rept', ''), 'x', '')::VARCHAR as report_id,
            treaty_id::VARCHAR as treaty_id
        """)
        .filter("report_id IS NOT NULL")
        .filter("treaty_id IS NOT NULL")
    )

   # Texts
    cleaner.register_custom_cleaner(
        "_committeereports_texts",
        lambda df: (
            df.aggregate("""
                REGEXP_REPLACE(REGEXP_REPLACE(report_id, 'rept', ''), 'x', '')::VARCHAR as report_id,
                MAX(formatted_text)::VARCHAR as formatted_text,
                MAX(raw_text)::VARCHAR as raw_text,
                MAX(pdf)::VARCHAR as pdf,
                CAST(MAX(CAST(lower(formatted_text_is_errata) AS BOOLEAN)) AS VARCHAR) as formatted_text_is_errata,
                CAST(MAX(CAST(lower(pdf_is_errata) AS BOOLEAN)) AS VARCHAR) as pdf_is_errata
            """, 
            "report_id")
            .filter("report_id IS NOT NULL")
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
    clean_committeereports(resume_from=args.resume_from)