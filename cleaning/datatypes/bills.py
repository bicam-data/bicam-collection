import logging
from pathlib import Path
import sys

# Add parent directory to Python path
sys.path.append(str(Path(__file__).parent.parent))

from cleaner import DataCleaner
import duckdb
import json
from typing import List, Dict, Any
import gc
import argparse

from cleaning_coordinator import CleaningCoordinator


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configure DuckDB to be quiet globally
duckdb.default_connection.execute("SET enable_progress_bar=true")

def parse_note_links(note: Dict[str, Any]) -> List[Dict[str, str]]:
    """Extract links from a note dictionary"""
    links = []
    if 'links' in note:
        for link in note['links']:
            links.append({
                'name': link.get('name', ''),
                'url': link.get('url', '')
            })
    return links

def generate_notes_tables(df: duckdb.DuckDBPyRelation) -> List[tuple[str, duckdb.DuckDBPyRelation]]:
    """Generate notes and notes_links tables from bills notes column"""
    logger.info("Generating notes tables...")
    
    # Debug counts using aggregate
    df.aggregate("""
        COUNT(*) as total_rows,
        COUNT(notes) as notes_count,
        COUNT(*) FILTER (WHERE notes IS NOT NULL) as non_null_notes
    """).show()
    
    # Debug sample of raw notes
    logger.info("Sample of raw notes:")
    df.filter('notes IS NOT NULL').select('notes').limit(5).show()
    
    # Parse JSON notes with better error handling
    df = df.select("""
        bill_id,
        TRY_CAST(
            CASE 
                WHEN notes IS NULL OR notes = '' THEN NULL
                -- Handle single-quoted JSON by replacing with double quotes
                WHEN POSITION('"' IN notes) = 0 THEN 
                    REPLACE(
                        REPLACE(notes, '''', '"'),
                        'None', 'null'
                    )
                ELSE notes
            END
            AS JSON
        ) as parsed_notes,
        updated_at
    """)
    
    # Debug parsed notes
    logger.info("Sample of parsed notes:")
    df.filter('parsed_notes IS NOT NULL').select('parsed_notes').limit(5).show()
    
    # Debug parsed notes count
    df.aggregate("COUNT(*) FILTER (WHERE parsed_notes IS NOT NULL) as valid_json_notes").show()
    
    notes_expanded = df.filter('parsed_notes IS NOT NULL').select("""
        bill_id,
        parsed_notes::JSON as notes_array,
        updated_at,
        -- Generate a global sequence number across all notes
        ROW_NUMBER() OVER (ORDER BY bill_id) as note_number,
        -- Get array indices for processing
        UNNEST(generate_series(CAST(0 AS BIGINT), 
                            CAST(json_array_length(parsed_notes::JSON) - 1 AS BIGINT))) as array_idx
    """)

    # Add note text and links array
    notes_expanded = notes_expanded.select("""
        bill_id,
        -- Keep the global sequence number
        note_number,
        array_idx,
        notes_array,
        updated_at,
        json_extract_string(notes_array, '$[' || CAST(array_idx AS BIGINT) || '].text') as note_text,
        json_extract(notes_array, '$[' || CAST(array_idx AS BIGINT) || '].links') as links_array
    """)

    # Create final notes table
    notes_df = notes_expanded.select("""
        bill_id,
        note_number,
        note_text,
        updated_at
    """).filter('note_text IS NOT NULL')

    # Create links table that uses the same note_number
    links_base = notes_expanded.filter('links_array IS NOT NULL').select("""
        bill_id,
        -- Keep the same note_number from parent note
        note_number,
        links_array,
        updated_at,
        UNNEST(generate_series(CAST(0 AS BIGINT), 
                            CAST(json_array_length(links_array) - 1 AS BIGINT))) as link_idx
    """)

    # Extract link details while maintaining parent note_number
    links_expanded = links_base.select("""
        bill_id,
        note_number,  -- This will now reference the correct parent note
        ROW_NUMBER() OVER (PARTITION BY bill_id, note_number ORDER BY link_idx) as link_number,
        json_extract_string(links_array, '$[' || CAST(link_idx AS BIGINT) || '].name') as link_name,
        json_extract_string(links_array, '$[' || CAST(link_idx AS BIGINT) || '].url') as link_url,
        updated_at
    """).filter('link_name IS NOT NULL OR link_url IS NOT NULL')

    # Debug output
    logger.info("Sample of generated notes:")
    notes_df.limit(5).show()
    logger.info(f"Total notes generated: {notes_df.aggregate('COUNT(*) as count').fetchone()[0]}")

    logger.info("Sample of generated links:")
    links_expanded.limit(5).show()
    logger.info(f"Total links generated: {links_expanded.aggregate('COUNT(*) as count').fetchone()[0]}")

    return [
        ('bills_notes', notes_df),
        ('bills_notes_links', links_expanded)
    ]

def clean_bills(resume_from: str = None, coordinator: CleaningCoordinator = None):
    """Clean bills data"""
    logger.info("Starting bills cleaning process...")

    if coordinator is None:
        coordinator = CleaningCoordinator()
        coordinator.register_module('bills', clean_bills, 'congressional')
        coordinator.initialize()
    
    cleaner = DataCleaner("bills", coordinator=coordinator, resume_from=resume_from)
    logger.info(f"Created cleaner with old_schema: {cleaner.old_schema}, new_schema: {cleaner.new_schema}")
    
    # Register custom cleaners
    cleaner.register_custom_cleaner(
        "_bills", 
        lambda df: (
            # Debug counts using separate aggregate queries
            df.aggregate("COUNT(*) as initial_count").show(),
            df.filter("bill_id != '-99'")
                .aggregate("COUNT(*) as after_bill_id").show(),

            # Actual data transformation
            df.filter("bill_id != '-99'")
                .select("*, LOWER(origin_chamber) as origin_chamber_temp")
                .filter("origin_chamber_temp IN ('house', 'senate') OR origin_chamber_temp IS NULL")
                .select("""
                    bill_id, bill_type,
                    -- Convert all numeric columns that might have fractions
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(bill_number::VARCHAR, '½', '.5'),
                        '[^0-9.]', ''
                    ) as bill_number,
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(congress::VARCHAR, '½', '.5'),
                        '[^0-9.]', ''
                    ) as congress,
                    title,
                    origin_chamber_temp as origin_chamber,
                    CASE 
                        WHEN origin_chamber_temp = 'house' THEN 'H'
                        WHEN origin_chamber_temp = 'senate' THEN 'S'
                        ELSE NULL 
                    END as origin_chamber_code,
                    introduced_at, 
                    constitutional_authority_statement,
                    is_law, 
                    notes, 
                    policy_area,
                    -- Handle count columns too
                    REGEXP_REPLACE(actions_count::VARCHAR, '[^0-9]', '') as actions_count,
                    REGEXP_REPLACE(amendments_count::VARCHAR, '[^0-9]', '') as amendments_count,
                    REGEXP_REPLACE(committees_count::VARCHAR, '[^0-9]', '') as committees_count,
                    REGEXP_REPLACE(cosponsors_count::VARCHAR, '[^0-9]', '') as cosponsors_count,
                    REGEXP_REPLACE(summaries_count::VARCHAR, '[^0-9]', '') as summaries_count,
                    REGEXP_REPLACE(subjects_count::VARCHAR, '[^0-9]', '') as subjects_count,
                    REGEXP_REPLACE(titles_count::VARCHAR, '[^0-9]', '') as titles_count,
                    REGEXP_REPLACE(texts_count::VARCHAR, '[^0-9]', '') as texts_count,
                    updated_at
              """)
              .filter("TRY_CAST(bill_number AS FLOAT) > 0")
              .filter("TRY_CAST(congress AS INTEGER) > 0")
        )[2]  # Return the actual transformed data
    )
    logger.info("Registered custom cleaners")
    
    cleaner.register_custom_cleaner('_bills_cbocostestimates', lambda df: (
        df.select("""
            bill_id,
            pub_date,
            title,
            url,
            *, REGEXP_REPLACE(
                REGEXP_REPLACE(description, '<p>|</p>', ''),
                '<(?!.*?(?:href=|a=))[^>]+>|</[^>]+>', 
                ''
            ) as description
        """)
    ))
    cleaner.register_custom_cleaner('_bills_summaries', lambda df: (
        df.select("""
            bill_id,
            action_date,
            action_desc,
            REGEXP_REPLACE(
                REGEXP_REPLACE(text, '<p>|</p>', ''),
                REGEXP_REPLACE(text, '<b>|</b>', ''),
                '<(?!.*?(?:href=|a=))[^>]+>|</[^>]+>', 
                ''
            ) as text,
            version_code
        """)
    ))
    cleaner.register_custom_cleaner('_bills_laws', lambda df: (
        df.select("""
            *,
            'PL' || law_number as law_id
        """)
    ))
    cleaner.register_custom_cleaner('_bills_titles', lambda df: (
        logger.info(f"Available columns in _bills_titles: {df.columns}"),
        
        df.select("""
            bill_id,
            title,
            title_type,
            CASE 
                WHEN chamber = 'S' THEN 'senate'
                WHEN chamber = 'H' THEN 'house'
                ELSE NULL
            END as chamber,
            bill_text_version_code,
            bill_text_version_name,
            title_type_code
        """)
    )[1])
    
    cleaner.register_custom_cleaner('_bills_billrelations', lambda df: (
        df.select("""
            bill_id,
            related_bill_id,
            CASE 
                WHEN identification_entity = 'House' THEN 'house'
                WHEN identification_entity = 'Senate' THEN 'senate'
                ELSE identification_entity
            END as identification_entity,
        """)
    ))
    
    cleaner.register_custom_cleaner('_bills_actions_recorded_votes', lambda df: (
        df.select("""
            action_id,
            bill_id,
            date,
            LOWER(chamber) as chamber,
            congress,
            roll_number,
            session,
            url
        """)
    ))

    # Register notes table generator with configs
    notes_table_configs = [
        {
            'old_name': 'bills_notes',
            'new_name': 'bills_notes',
            'columns': [
                {'old_name': 'bill_id', 'new_name': 'bill_id', 'old_type': 'TEXT', 'new_type': 'TEXT', 'key': True},
                {'old_name': 'note_text', 'new_name': 'note_text', 'old_type': 'TEXT', 'new_type': 'TEXT', 'key': False},
                {'old_name': 'note_number', 'new_name': 'note_number', 'old_type': 'INTEGER', 'new_type': 'INTEGER', 'key': True},
                {'old_name': 'updated_at', 'new_name': 'updated_at', 'old_type': 'TEXT', 'new_type': 'TIMESTAMP WITH TIME ZONE', 'key': False}
            ]
        },
        {
            'old_name': 'bills_notes_links',
            'new_name': 'bills_notes_links',
            'columns': [
                {'old_name': 'bill_id', 'new_name': 'bill_id', 'old_type': 'TEXT', 'new_type': 'TEXT', 'key': True},
                {'old_name': 'note_number', 'new_name': 'note_number', 'old_type': 'INTEGER', 'new_type': 'INTEGER', 'key': True},
                {'old_name': 'link_name', 'new_name': 'link_name', 'old_type': 'TEXT', 'new_type': 'TEXT', 'key': False},
                {'old_name': 'link_url', 'new_name': 'link_url', 'old_type': 'TEXT', 'new_type': 'TEXT', 'key': True},
                {'old_name': 'updated_at', 'new_name': 'updated_at', 'old_type': 'TEXT', 'new_type': 'TIMESTAMP WITH TIME ZONE', 'key': False}
            ]
        }
    ]

    def notes_table_generator(df: duckdb.DuckDBPyRelation) -> List[tuple[str, duckdb.DuckDBPyRelation]]:
        return generate_notes_tables(df)  # Now returns both tables
    
    cleaner.register_new_table_generator('_bills', notes_table_generator, notes_table_configs)
    logger.info("Registered table generators")
    
    # Run the cleaning process
    logger.info("Starting cleaning process...")
    cleaner.clean_all(resume_from=resume_from)
    logger.info("Completed bills cleaning process")

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description='Clean bills data')
    parser.add_argument('--resume-from', type=str, help='Table name to resume processing from')
    
    args = parser.parse_args()
    clean_bills(resume_from=args.resume_from)