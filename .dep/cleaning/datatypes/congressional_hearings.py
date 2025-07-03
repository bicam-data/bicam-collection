import logging
from pathlib import Path
import sys
import os

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

def extract_appropriations(rel: duckdb.DuckDBPyRelation) -> None:
    """Extract and save appropriations relationships before any cleaning"""
    logger.info("Extracting appropriations relationships...")
    
    # Convert to pandas DataFrame first
    df = rel.df()
    logger.info(f"Columns available in DataFrame: {df.columns.tolist()}")
    
    # Extract bills using pandas - ensure both columns are not null
    appropriations = df[['package_id', 'is_appropriation']].dropna(subset=['package_id', 'is_appropriation'])
    appropriations.to_parquet('appropriations.parquet')


def clean_hearings(resume_from: str = None, coordinator: CleaningCoordinator = None):
    """Clean hearings data"""
    logger.info("Starting hearings cleaning process...")

    # Configure DuckDB settings for complex queries
    duckdb.sql("SET max_expression_depth=10000;")

    if coordinator is None:
        coordinator = CleaningCoordinator()
        coordinator.register_module('congressional_hearings', clean_hearings, 'govinfo')
        coordinator.initialize()

    cleaner = DataCleaner("congressional_hearings", coordinator=coordinator)
    logger.info(f"Created cleaner with old_schema: {cleaner.old_schema}, new_schema: {cleaner.new_schema}")

    # # Process granules before registering cleaners
    # process_granules(cleaner.old_schema)
    conn_string = f"host={os.getenv('POSTGRESQL_HOST')} port={os.getenv('POSTGRESQL_PORT')} dbname={os.getenv('POSTGRESQL_DATABASE')} user={os.getenv('POSTGRESQL_USERNAME')} password={os.getenv('POSTGRESQL_PASSWORD')}"
    with duckdb.connect() as conn:
        conn.install_extension("postgres_scanner")
        conn.load_extension("postgres_scanner")
        # Query directly without creating a table
        df = conn.sql(f"""
            SELECT * FROM postgres_scan(
                '{conn_string}',
                '_staging_govinfo',
                '_congressional_hearings_granules'
            )
        """)
        
        
        extract_appropriations(df)
        logger.info("Extracted appropriations relationships")
    # Main hearings cleaner with join to granules for is_appropriation
    cleaner.register_custom_cleaner(
        "_hearings",
        lambda df: (
            # First create views
            duckdb.sql(f"""
                CREATE OR REPLACE VIEW hearings_source AS SELECT * FROM df;
                CREATE OR REPLACE VIEW granules_source AS (
                    SELECT DISTINCT 
                        package_id,
                        is_appropriation
                    FROM postgres_scan(
                        '{conn_string}',
                        '_staging_govinfo',
                        '_congressional_hearings_granules'
                    )
                );
            """),
            # Then execute the main query
            result := duckdb.sql("""
                SELECT
                    h.package_id,
                    CASE WHEN h.package_id ~ '\d' THEN
                        REGEXP_REPLACE(
                            h.package_id,
                            '^(?:GPO-)?CHRG-(\d+)([a-z]+)(\d.*)',
                            '\2\3-\1'
                        )
                    ELSE h.package_id
                    END as hearing_id,
                    h.title,
                    LOWER(h.chamber) as chamber,
                    REGEXP_REPLACE(h.congress::VARCHAR, '[^0-9]', '')::INTEGER as congress,
                    REGEXP_REPLACE(h.session::VARCHAR, '[^0-9]', '')::INTEGER as session,
                    REGEXP_REPLACE(h.pages::VARCHAR, '[^0-9]', '')::INTEGER as pages,
                    COALESCE(CAST(g.is_appropriation AS BOOLEAN), FALSE) as is_appropriation,
                    h.issued_at,
                    h.branch,
                    h.government_author1,
                    h.government_author2,
                    h.publisher,
                    h.collection_code,
                    h.migrated_doc_id,
                    h.su_doc_class_number,
                    h.last_modified
                FROM hearings_source h
                LEFT JOIN granules_source g ON h.package_id = g.package_id
                WHERE h.package_id IS NOT NULL
                  AND (h.package_id IS NOT NULL  -- Skip rows where all values are NULL
                       OR h.title IS NOT NULL 
                       OR h.chamber IS NOT NULL
                       OR h.congress IS NOT NULL
                       OR h.session IS NOT NULL)
            """),
            # Clean up views
            duckdb.sql("DROP VIEW IF EXISTS hearings_source"),
            duckdb.sql("DROP VIEW IF EXISTS granules_source"),
            # Return the result
            result
        )[-1]  # Get the last non-None result
    )

    # Granules cleaner
    cleaner.register_custom_cleaner(
        "_congressional_hearings_granules",
        lambda df: (
            df.select("""
                granule_id,
                package_id,
            """)
            .filter("granule_id IS NOT NULL")
            .filter("package_id IS NOT NULL")
        )
    )

    # Committees cleaner
    cleaner.register_custom_cleaner(
        "_congressional_hearings_granules_committees",
        lambda df: (
            df.select("""
                granule_id,
                package_id,
                committee_code,
                CASE 
                    WHEN committee_name = UPPER(committee_name) THEN UPPER(SUBSTRING(TRIM(committee_name), 1, 1)) || LOWER(SUBSTRING(TRIM(committee_name), 2))
                    ELSE committee_name
                END as committee_name
            """)
            .filter("granule_id IS NOT NULL")
            .filter("package_id IS NOT NULL")
        )
    )

    # Members cleaner
    cleaner.register_custom_cleaner(
        "_congressional_hearings_granules_members",
        lambda df: (
            df.select("""
                granule_id,
                package_id,
                bioguide_id,
                name
            """)
            .filter("granule_id IS NOT NULL")
            .filter("package_id IS NOT NULL")
            .filter("bioguide_id IS NOT NULL")
        )
    )

    # Reference bills cleaner
    cleaner.register_custom_cleaner(
        "_congressional_hearings_granules_reference_bills",
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

    # Witnesses cleaner
    cleaner.register_custom_cleaner(
        "_congressional_hearings_granules_witnesses",
        lambda df: (
            df.select("""
                granule_id,
                witness
            """)
            .filter("granule_id IS NOT NULL")
        )
    )

    # Run the cleaning process
    logger.info("Starting cleaning process...")
    cleaner.clean_all(resume_from=resume_from)
    
    # Cleanup temporary parquet files
    Path('appropriations.parquet').unlink(missing_ok=True)
    
    logger.info("Completed hearings cleaning process")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Clean hearings data')
    parser.add_argument('--resume-from', type=str, help='Table name to resume processing from')
    
    args = parser.parse_args()
    clean_hearings(resume_from=args.resume_from)