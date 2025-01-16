import logging
import os
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parent.parent))
import pandas as pd
from cleaner import DataCleaner
import duckdb
import argparse
from typing import List
from cleaning_coordinator import CleaningCoordinator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def extract_summaries(rel: duckdb.DuckDBPyRelation) -> None:
    """Extract and save summaries relationships before any cleaning"""
    logger.info("Extracting summaries relationships...")
    
    # Convert to pandas DataFrame first
    df = rel.df()
    logger.info(f"Columns available in DataFrame: {df.columns.tolist()}")
    
    # Extract summaries using pandas - ensure both columns are not null
    summaries = df[['package_id', 'summary']].dropna(subset=['package_id', 'summary'])
    summaries.to_parquet('summaries.parquet')


def clean_treaties(resume_from: str = None, coordinator: CleaningCoordinator = None):
    """Clean treaties data"""
    logger.info("Starting treaties cleaning process...")
    duckdb.sql("SET max_expression_depth=10000;")
    if coordinator is None:
        coordinator = CleaningCoordinator()
        coordinator.register_module('treaty_docs', clean_treaties, 'govinfo')
        coordinator.initialize()

    cleaner = DataCleaner("treaty_docs", coordinator=coordinator)
    logger.info(f"Created cleaner with old_schema: {cleaner.old_schema}, new_schema: {cleaner.new_schema}")

    conn_string = f"host={os.getenv('POSTGRESQL_HOST')} port={os.getenv('POSTGRESQL_PORT')} dbname={os.getenv('POSTGRESQL_DATABASE')} user={os.getenv('POSTGRESQL_USERNAME')} password={os.getenv('POSTGRESQL_PASSWORD')}"
    with duckdb.connect() as conn:
        conn.install_extension("postgres_scanner")
        conn.load_extension("postgres_scanner")
        # Query directly without creating a table
        df = conn.sql(f"""
            SELECT * FROM postgres_scan(
                '{conn_string}',
                '_staging_govinfo', 
                '_treaty_docs_granules'
            )
        """)
        
        extract_summaries(df)
        logger.info("Extracted summaries relationships")

    # Main treaties cleaner
    cleaner.register_custom_cleaner(
        "_treaty_docs",
        lambda df: (
            # Convert input df to pandas immediately
            base_df := df.df(),  # Convert DuckDBPyRelation to pandas DataFrame
            
            # First transform the main dataframe using the pandas DataFrame
            base_data := duckdb.sql("""
                SELECT
                    package_id,
                    title,
                    REGEXP_REPLACE(congress::VARCHAR, '[^0-9]', '')::INTEGER as congress,
                    REGEXP_REPLACE(session::VARCHAR, '[^0-9]', '')::INTEGER as session,
                    LOWER(chamber) as chamber,
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
                FROM base_df
                WHERE package_id IS NOT NULL
                    AND (package_id IS NOT NULL
                        OR title IS NOT NULL 
                        OR chamber IS NOT NULL
                        OR congress IS NOT NULL
                        OR session IS NOT NULL)
            """).df(),  # Convert to pandas DataFrame

            # Get granules data separately - also as pandas
            granules := duckdb.sql(f"""
                SELECT DISTINCT 
                    package_id,
                    summary
                FROM postgres_scan(
                    '{conn_string}',
                    '_staging_govinfo',
                    '_treaty_docs_granules'
                )
            """).df(),  # Convert to pandas DataFrame

            # Final join and treaty_id creation using pandas
            result := (
                base_data.merge(
                    granules, 
                    on='package_id', 
                    how='left'
                )
                .assign(
                    summary=lambda df: df['summary'].fillna(''),
                    treaty_id=lambda df: 'td' + df['package_id'].str.split('tdoc').str[0].str.split('-').str[-1] + '-' + 
                                       df['package_id'].str.split('tdoc').str[-1]
                )
                [['package_id', 'treaty_id', 'title', 'congress', 'session', 'chamber',
                  'summary', 'pages', 'issued_at', 'branch', 'government_author1',
                  'government_author2', 'publisher', 'collection_code', 'migrated_doc_id',
                  'su_doc_class_number', 'last_modified']]
            ),
            
            # Return the result
            result
        )[-1]
    )


    # Granules cleaner
    cleaner.register_custom_cleaner(
        "_treaty_docs_granules",
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
        "_treaty_docs_committees",
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

    # Run the cleaning process
    logger.info("Starting cleaning process...")
    cleaner.clean_all(resume_from=resume_from)
    logger.info("Completed treaties cleaning process")
    Path('summaries.parquet').unlink(missing_ok=True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Clean treaties data')
    parser.add_argument('--resume-from', type=str, help='Table name to resume processing from')
    
    args = parser.parse_args()
    clean_treaties(resume_from=args.resume_from)