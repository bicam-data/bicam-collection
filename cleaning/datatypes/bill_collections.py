import logging
from pathlib import Path
import sys

import pandas as pd

# Add parent directory to Python path
sys.path.append(str(Path(__file__).parent.parent))

from cleaner import DataCleaner
import duckdb
import argparse
from typing import List
import hashlib
import uuid
from cleaning_coordinator import CleaningCoordinator
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configure DuckDB to be quiet globally
duckdb.default_connection.execute("SET enable_progress_bar=true")
CHUNK_COUNT = 0  # Global counter for chunks

def clean_bill_collections(resume_from: str = None, coordinator: CleaningCoordinator = None):
    """Clean bill collections data"""
    global CHUNK_COUNT  # Declare we'll use the global counter
    CHUNK_COUNT = 0  # Reset at start of cleaning
    
    logger.info("Starting bill collections cleaning process...")

    if coordinator is None:
        coordinator = CleaningCoordinator()
        coordinator.register_module('bill_collections', clean_bill_collections, 'congressional')
        coordinator.initialize()

    cleaner = DataCleaner("bill_collections", coordinator=coordinator, resume_from=resume_from)

    logger.info(f"Created cleaner with old_schema: {cleaner.old_schema}, new_schema: {cleaner.new_schema}")

    # Register custom cleaners for each table
    cleaner.register_custom_cleaner(
        "_bill_collections", 
        lambda df: (
            df.select("""
                package_id,
                bill_id,
                UPPER(bill_version) as bill_version,
                LOWER(origin_chamber) as origin_chamber,
                LOWER(current_chamber) as current_chamber,
                CASE 
                    WHEN LOWER(is_appropriation) IN ('true', 't', 'yes', 'y', '1') THEN TRUE
                    ELSE FALSE 
                END as is_appropriation,
                CASE 
                    WHEN LOWER(is_private) IN ('true', 't', 'yes', 'y', '1') THEN TRUE
                    ELSE FALSE 
                END as is_private,
                REGEXP_REPLACE(pages::VARCHAR, '[^0-9]', '')::INTEGER as pages,
                issued_at,
                government_author1,
                government_author2,
                publisher,
                collection_code,
                stock_number,
                su_doc_class_number,
                migrated_doc_id,
                child_ils_system_id,
                parent_ils_system_id,
                mods_url,
                pdf_url,
                premis_url,
                txt_url,
                xml_url,
                zip_url,
                last_modified
            """)
            .filter("package_id IS NOT NULL")
            .order("package_id")
        )
    )

    def reference_codes_cleaner(df):
        """Custom cleaner for reference codes with proper scope handling"""
        global CHUNK_COUNT
        global_count = CHUNK_COUNT * 100000  # Much larger gap between chunks
        logger.info(f"Processing chunk {CHUNK_COUNT}, starting at {global_count}")
        CHUNK_COUNT += 1
        
        # First create the reference code
        df = df.select("""
            package_id,
            -- Simpler concatenation logic
            TRIM(CONCAT_WS('-', 
                NULLIF(TRIM(REPLACE(reference_codes_label, '.', '')), ''),
                NULLIF(TRIM(reference_codes_title), '')
            )) as reference_code,
            reference_codes_sections
        """)
        
        # Then add the bill_code_id in a separate step
        return df.select(f"""
            package_id,
            reference_code,
            LPAD(({global_count} + ROW_NUMBER() OVER (ORDER BY package_id))::VARCHAR, 8, '0') as bill_code_id,
            reference_codes_sections
        """).filter("package_id IS NOT NULL").filter("reference_code IS NOT NULL")

    cleaner.register_custom_cleaner(
        "_bill_collections_reference_codes",
        reference_codes_cleaner
    )

    def generate_reference_codes_sections(df: duckdb.DuckDBPyRelation) -> List[tuple[str, pd.DataFrame]]:
        """Generate sections table from reference codes using pandas string operations"""
        logger.info("Generating reference codes sections...")
        
        # Convert to pandas DataFrame
        pdf = df.df()
        
        # Create empty list to store all rows
        all_sections = []
        
        # Process each row
        for _, row in pdf.iterrows():
            if pd.notna(row['reference_codes_sections']):
                sections = row['reference_codes_sections'].split(',')
                for section in sections:
                    section = section.strip()
                    if section:
                        all_sections.append({
                            'bill_code_id': row['bill_code_id'],
                            'code_section': section
                        })
        
        # Create DataFrame from collected sections
        sections_df = pd.DataFrame(all_sections)
        
        return [('bills_reference_codes_sections', sections_df)]

    sections_table_config = [{
        'old_name': 'bills_reference_codes_sections',
        'new_name': 'bills_reference_codes_sections',
        'columns': [
            {'old_name': 'bill_code_id', 'new_name': 'bill_code_id', 'old_type': 'TEXT', 'new_type': 'TEXT', 'key': True},
            {'old_name': 'code_section', 'new_name': 'code_section', 'old_type': 'TEXT', 'new_type': 'TEXT', 'key': True}
        ]
    }]

    cleaner.register_new_table_generator(
        '_bill_collections_reference_codes',
        generate_reference_codes_sections,
        sections_table_config
    )

    cleaner.register_custom_cleaner(
        "_bill_collections_reference_laws",
        lambda df: (
            df.select("""
                package_id,
                -- First create the law_id by cleaning up the input
                CASE 
                    WHEN law_id LIKE 'Public Law%' THEN REPLACE(law_id, 'Public Law', 'PL')
                    WHEN law_id LIKE 'Private Law%' THEN REPLACE(law_id, 'Private Law', 'PL')
                    ELSE law_id
                END as law_id,
                law_type
            """)
            .filter("package_id IS NOT NULL")
            .filter("law_id IS NOT NULL")
        )
    )

    def statute_cleaner(df):
        """Custom cleaner for statutes with proper scope handling"""
        global CHUNK_COUNT
        global_count = CHUNK_COUNT * 100000  # Much larger gap between chunks
        logger.info(f"Processing chunk {CHUNK_COUNT}, starting at {global_count}")
        CHUNK_COUNT += 1
        
        # First create the base statute info
        df = df.select("""
            package_id,
            TRIM(CONCAT_WS('',
                NULLIF(TRIM(LOWER(reference_statutes_label)), ''),
                NULLIF(TRIM(reference_statutes_title), '')
            )) as reference_statute,
            reference_statutes_pages
        """)
        
        # Then add the bill_statute_id in a separate step using same LPAD approach
        return df.select(f"""
            package_id,
            reference_statute,
            LPAD(({global_count} + ROW_NUMBER() OVER (ORDER BY package_id))::VARCHAR, 8, '0') as bill_statute_id,
            reference_statutes_pages
        """).filter("package_id IS NOT NULL").filter("reference_statute IS NOT NULL")

    cleaner.register_custom_cleaner(
        "_bill_collections_reference_statutes",
        statute_cleaner
    )

    def generate_statute_pages(df: duckdb.DuckDBPyRelation) -> List[tuple[str, pd.DataFrame]]:
        """Generate pages table from reference statutes using pandas string operations"""
        logger.info("Generating statute pages...")
        
        # Convert to pandas DataFrame
        pdf = df.df()
        
        # Create empty list to store all rows
        all_pages = []
        
        # Process each row
        for _, row in pdf.iterrows():
            if pd.notna(row['reference_statutes_pages']):
                pages = row['reference_statutes_pages'].split(',')
                for page in pages:
                    page = page.strip()
                    if page:  # Only add non-empty pages
                        all_pages.append({
                            'bill_statute_id': row['bill_statute_id'],
                            'page': page
                        })
        
        # Create DataFrame from collected pages
        pages_df = pd.DataFrame(all_pages)
        
        return [('bills_reference_statutes_pages', pages_df)]

    pages_table_config = [{
        'old_name': 'bills_reference_statutes_pages',
        'new_name': 'bills_reference_statutes_pages',
        'columns': [
            {'old_name': 'bill_statute_id', 'new_name': 'bill_statute_id', 'old_type': 'TEXT', 'new_type': 'TEXT', 'key': True},
            {'old_name': 'page', 'new_name': 'page', 'old_type': 'TEXT', 'new_type': 'TEXT', 'key': True}
        ]
    }]

    cleaner.register_new_table_generator(
        '_bill_collections_reference_statutes',
        generate_statute_pages,
        pages_table_config
    )

    cleaner.register_custom_cleaner(
        "_bill_collections_short_titles",
        lambda df: (
            df.select("""
                package_id,
                short_title,
                level,
                type
            """)
            .filter("package_id IS NOT NULL")
            .filter("short_title IS NOT NULL")
        )
    )

    # Run the cleaning process
    logger.info("Starting cleaning process...")
    cleaner.clean_all(resume_from=resume_from)
    logger.info("Completed bill collections cleaning process")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Clean bill collections data')
    parser.add_argument('--resume-from', type=str, help='Table name to resume processing from')

    args = parser.parse_args()
    clean_bill_collections(resume_from=args.resume_from)