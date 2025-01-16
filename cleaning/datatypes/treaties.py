import logging
from pathlib import Path
from cleaner import DataCleaner
import duckdb
import argparse
from typing import List

from cleaning_coordinator import CleaningCoordinator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configure DuckDB to be quiet globally
duckdb.default_connection.execute("SET enable_progress_bar=true")

def clean_treaties(resume_from: str = None, coordinator: CleaningCoordinator = None):
    """Clean treaties data"""

    if coordinator is None:
        coordinator = CleaningCoordinator()
        coordinator.register_module('treaties', clean_treaties, 'congressional')
        coordinator.initialize()
    
    cleaner = DataCleaner("treaties", coordinator=coordinator)
    logger.info(f"Created cleaner with old_schema: {cleaner.old_schema}, new_schema: {cleaner.new_schema}")

    cleaner.register_custom_cleaner(
        "_treaties", 
        lambda df: (
            # Convert to pandas first
            pdf := df.df(),
            
            logger.info("Transforming treaties data..."),
            
            # Show original data sample
            logger.info("Original data sample:"),
            logger.info(pdf.head()),
            
            # Transform data using DuckDB SQL and return directly
            duckdb.sql("""
                SELECT
                    treaty_id,
                    treaty_number,
                    suffix,
                    congress_received,
                    congress_considered,
                    topic,
                    in_force_at,
                    transmitted_at,
                    -- Remove HTML tags from resolution text
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            resolution_text,
                            '<[^>]+>',
                            ''
                        ),
                        '\\s+',
                        ' '
                    ) as resolution_text,
                    REGEXP_REPLACE(parts_count::VARCHAR, '[^0-9]', '') as parts_count,
                    
                    old_number,
                    old_number_display_name,
                    updated_at
                FROM pdf
                WHERE treaty_id IS NOT NULL
            """)
        )[0]
    )

    cleaner.register_custom_cleaner(
        "_treaties_actions",
        lambda df: (
            pdf := df.df(),
            duckdb.sql("""
            SELECT
                treaty_id,
                action_id,
                action_code,
                action_date,
                text,
                action_type
                FROM pdf
                WHERE treaty_id IS NOT NULL
                AND action_id IS NOT NULL
            """).df()
        )[0]
    )

    cleaner.register_custom_cleaner(
        "_treaties_country_parties",
        lambda df: (
            pdf := df.df(),
            duckdb.sql("""
                SELECT
                    treaty_id,
                    country
                FROM pdf
                WHERE treaty_id IS NOT NULL
            """).df()
        )[0]
    )

    cleaner.register_custom_cleaner(
        "_treaties_index_terms",
        lambda df: (
            pdf := df.df(),
            duckdb.sql("""
                SELECT
                    treaty_id,
                    index_term
                FROM pdf
                WHERE treaty_id IS NOT NULL
                AND index_term IS NOT NULL
            """).df()
        )[0]
    )

    cleaner.register_custom_cleaner(
        "_treaties_titles",
        lambda df: (
            pdf := df.df(),
            duckdb.sql("""
                SELECT
                    treaty_id,
                    title,
                    CASE 
                    WHEN LOWER(title_type) LIKE '%%popular%%' THEN 'popular'
                    WHEN LOWER(title_type) LIKE '%%formal%%' THEN 'formal'
                    WHEN LOWER(title_type) LIKE '%%short%%' THEN 'short'
                        ELSE NULL
                    END as title_type
                FROM pdf
                WHERE treaty_id IS NOT NULL
                AND title IS NOT NULL
            """).df()
        )[0]
    )

    # Run the cleaning process
    logger.info("Starting cleaning process...")
    cleaner.clean_all(resume_from=resume_from)
    logger.info("Completed treaties cleaning process")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Clean treaties data')
    parser.add_argument('--resume-from', type=str, help='Table name to resume processing from')
    
    args = parser.parse_args()
    clean_treaties(resume_from=args.resume_from)