import logging
import argparse
from tqdm import tqdm

import psycopg2
from datatypes import (
    amendments,
    bills,
    committeemeetings,
    committees,
    committeeprints,
    committeereports,
    congresses,
    hearings,
    members,
    nominations,
    treaties,
    congressional_directories,
    bill_collections,
    committee_prints,
    committee_reports,
    congressional_hearings,
    treaty_docs
)
from cleaning_coordinator import CleaningCoordinator
from dotenv import load_dotenv
import os

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def add_raw_text(
    source_table: str,
    target_table: str,
    source_schema: str,
    target_schema: str,
    source_id_columns: list,
    target_id_columns: list,
    source_text_column: str,
    target_text_column: str,
    batch_size: int = 1000
):
    """
    Process text data in batches to manage memory usage.

    Args:
        source_table (str): Source table name
        target_table (str): Target table name
        source_schema (str): Source schema name
        target_schema (str): Target schema name
        source_id_columns (list): List of ID columns in source table
        target_id_columns (list): List of ID columns in target table
        source_text_column (str): Column name containing text in source table
        target_text_column (str): Column name where text will be stored in target table
        batch_size (int): Number of rows to process in each batch
    """
    logger.info(f"Adding raw text from {source_schema}.{source_table} to {target_schema}.{target_table}")

    db_config = {
        'host': os.getenv('POSTGRESQL_HOST'),
        'port': os.getenv('POSTGRESQL_PORT'),
        'user': os.getenv('POSTGRESQL_USERNAME'),
        'password': os.getenv('POSTGRESQL_PASSWORD'),
        'database': os.getenv('POSTGRESQL_DATABASE')
    }

    conn = psycopg2.connect(**db_config)

    # Get total count for progress bar using a regular cursor
    count_cursor = conn.cursor()
    count_cursor.execute(f"SELECT COUNT(*) FROM {source_schema}.{source_table}")
    total_rows = count_cursor.fetchone()[0]
    count_cursor.close()

    # Main query with OFFSET/LIMIT for batching
    main_query = f"""
        SELECT {', '.join(source_id_columns)}, {source_text_column}
        FROM {source_schema}.{source_table}
        ORDER BY {source_id_columns[0]}
        OFFSET %s LIMIT %s
    """
    update_cursor = conn.cursor()
    offset = 0
    with tqdm(total=total_rows, desc="Processing rows") as pbar:
        while True:
            # Create a new cursor for each batch fetch
            fetch_cursor = conn.cursor('fetch_cursor')
            fetch_cursor.execute(main_query, (offset, batch_size))

            rows = fetch_cursor.fetchall()
            fetch_cursor.close()

            if not rows:
                break

            # Process batch
            for row in rows:
                id_values = row[:-1]  # All columns except the last one (text)
                text_value = row[-1]

                where_clause = " AND ".join(f"{col} = %s" for col in target_id_columns)
                update_query = f"""
                    UPDATE {target_schema}.{target_table}
                    SET {target_text_column} = %s
                    WHERE {where_clause}
                    AND {target_text_column} IS NULL
                """

                update_cursor.execute(update_query, (text_value,) + id_values)

            conn.commit()  # Commit after each batch
            pbar.update(len(rows))
            offset += batch_size

            # Free up memory
            del rows

    update_cursor.close()
    conn.close()
    logger.info(f"Successfully added raw text from {source_schema}.{source_table} to {target_schema}.{target_table}")
    
# function to update amendment columns "is_amendment_amendment", "is_bill_amendment", "is_treaty_amendment" based on if amendment_id is in amendments_amended_bills, amendments_amended_treaties, amendments_amended_amendments tables
def update_amendment_columns():
    logger.info("Starting update of amendment columns")
    db_config = {
        'host': os.getenv('POSTGRESQL_HOST'),
        'port': os.getenv('POSTGRESQL_PORT'),
        'user': os.getenv('POSTGRESQL_USERNAME'),
        'password': os.getenv('POSTGRESQL_PASSWORD'),
        'database': os.getenv('POSTGRESQL_DATABASE')
    }
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()
    
    BATCH_SIZE = 1000  # Process 1000 amendments at a time
    
    def update_in_batches(amendments, update_query):
        if not amendments:
            return
        
        # Convert tuple to list for slicing
        amendments_list = list(amendments)
        for i in range(0, len(amendments_list), BATCH_SIZE):
            batch = tuple(amendments_list[i:i + BATCH_SIZE])
            cursor.execute(update_query, (batch,))
            conn.commit()
    
    logger.info("Fetching amendments_amended_bills")
    cursor.execute("SELECT amendment_id FROM congressional.amendments_amended_bills")
    amendments_amended_bills = tuple(row[0] for row in cursor.fetchall())
    
    logger.info("Fetching amendments_amended_treaties")
    cursor.execute("SELECT amendment_id FROM congressional.amendments_amended_treaties")
    amendments_amended_treaties = tuple(row[0] for row in cursor.fetchall())
    
    logger.info("Fetching amendments_amended_amendments")
    cursor.execute("SELECT amendment_id FROM congressional.amendments_amended_amendments")
    amendments_amended_amendments = tuple(row[0] for row in cursor.fetchall())
    
    # Update in batches
    logger.info("Updating is_amendment_amendment column")
    cursor.execute("UPDATE congressional.amendments SET is_amendment_amendment = FALSE")
    conn.commit()
    update_in_batches(
        amendments_amended_amendments,
        "UPDATE congressional.amendments SET is_amendment_amendment = TRUE WHERE amendment_id IN %s"
    )
    
    logger.info("Updating is_bill_amendment column")
    cursor.execute("UPDATE congressional.amendments SET is_bill_amendment = FALSE")
    conn.commit()
    if amendments_amended_bills:
        for i in range(0, len(amendments_amended_bills), BATCH_SIZE):
            batch = tuple(amendments_amended_bills[i:i + BATCH_SIZE])
            cursor.execute("""
                UPDATE congressional.amendments 
                SET is_bill_amendment = TRUE 
                WHERE amendment_id IN %s 
                AND amendment_id NOT IN (SELECT amendment_id FROM congressional.amendments_amended_amendments)
            """, (batch,))
            conn.commit()
    
    logger.info("Updating is_treaty_amendment column")
    cursor.execute("UPDATE congressional.amendments SET is_treaty_amendment = FALSE")
    conn.commit()
    if amendments_amended_treaties:
        for i in range(0, len(amendments_amended_treaties), BATCH_SIZE):
            batch = tuple(amendments_amended_treaties[i:i + BATCH_SIZE])
            cursor.execute("""
                UPDATE congressional.amendments 
                SET is_treaty_amendment = TRUE 
                WHERE amendment_id IN %s 
                AND amendment_id NOT IN (SELECT amendment_id FROM congressional.amendments_amended_amendments)
            """, (batch,))
            conn.commit()
    
    cursor.close()
    conn.close()
    logger.info("Successfully updated amendment columns")


def process_phase(modules: list, coordinator: CleaningCoordinator, start_from: str = None, phase_name: str = "") -> bool:
    """Process a single phase of modules"""
    coordinator.cleaning_modules = []
    coordinator.dependency_graph = None
    
    logger.info(f"\n{'='*20} Processing {phase_name} {'='*20}")
    
    # Register modules for this phase
    for name, func, schema in modules:
        coordinator.register_module(name, func, schema)
    
    coordinator.initialize()
    
    if start_from:
        phase_names = [name for name, _, _ in modules]
        if start_from not in phase_names:
            return False
        start_idx = phase_names.index(start_from)
        modules = modules[start_idx:]
    
    # Map special cases for verification
    table_name_map = {
        'committees_base': 'committees',
        'committees_relationships': 'committees'
    }
    
    for name, func, schema in modules:
        try:
            logger.info(f"Processing module {name}...")
            func(coordinator=coordinator)
            
            main_table = name.lstrip('_')
            # Use mapping for verification
            verify_table = table_name_map.get(main_table, main_table)
            
            count = coordinator.verify_table_population(schema, verify_table)
            if count == 0:
                raise Exception(f"No data was written to {schema}.{verify_table}")
            
            logger.info(f"Successfully completed {name} cleaning script with {count} rows")
            
        except Exception as e:
            logger.error(f"Error in {name} cleaning script: {str(e)}")
            raise
            
    return start_from and True

def main(post_only=False):
    parser = argparse.ArgumentParser(description='Run all cleaning scripts')
    parser.add_argument('--resume-from', type=str, help='Module name to resume from')
    parser.add_argument('--post-only', action='store_true', help='Only run post-processing steps')
    args = parser.parse_args()

    if not args.post_only:

        coordinator = CleaningCoordinator()
        
            # Level 1: No dependencies
        modules_phase1 = [
            ('congresses', congresses.clean_congresses, 'congressional'),
        ]
        
        # Level 2: Depends only on congresses
        modules_phase2 = [
            ('members', members.clean_members, 'congressional'),
            ('treaties', treaties.clean_treaties, 'congressional'),
        ]
        
        # Level 3: Base committees table
        modules_phase3 = [
            ('committees_base', committees.clean_committees_base, 'congressional'),  # New function for base table only
        ]
        
        # Level 4A: Dependencies on committees_base 
        modules_phase4a = [
            ('bills', bills.clean_bills, 'congressional'),
            ('hearings', hearings.clean_hearings, 'congressional'),
        ]

        # Level 4B: Dependencies on 4A
        modules_phase4b = [
            ('nominations', nominations.clean_nominations, 'congressional'),
        ]
        # Level 5: Dependencies on bills and earlier
        modules_phase5 = [
            ('amendments', amendments.clean_amendments, 'congressional'),
            ('committeereports', committeereports.clean_committeereports, 'congressional'),
            ('committeeprints', committeeprints.clean_committeeprints, 'congressional'),
        ]
        
        # Level 6: Final dependencies and relationship tables
        modules_phase6 = [
            ('committees_relationships', committees.clean_committees_relationships, 'congressional'),  # New function for relationship tables
        ]
        
        modules_phase7 = [
            ('committeemeetings', committeemeetings.clean_committeemeetings, 'congressional'),
        ]
        
        modules_phase8 = [
            ('congressional_directories', congressional_directories.clean_congressional_directories, 'govinfo'),
            ('bill_collections', bill_collections.clean_bill_collections, 'govinfo'),
            ('committee_prints', committee_prints.clean_committee_prints, 'govinfo'),
            ('committee_reports', committee_reports.clean_committee_reports, 'govinfo'),
            ('congressional_hearings', congressional_hearings.clean_hearings, 'govinfo'),
            ('treaty_docs', treaty_docs.clean_treaties, 'govinfo'),
        ]
        
        phases = [
            ("Phase 1 - No Dependencies", modules_phase1),
            ("Phase 2 - Congress Dependencies", modules_phase2),
            ("Phase 3 - Base Committees", modules_phase3),
            ("Phase 4A - Primary Tables", modules_phase4a),
            ("Phase 4B - Dependencies on 4A", modules_phase4b),
            ("Phase 5 - Secondary Dependencies", modules_phase5),
            ("Phase 6 - Final Dependencies", modules_phase6),
            ("Phase 7 - Committee Meetings", modules_phase7),
            ("Phase 8 - GovInfo", modules_phase8),
        ]
        
        found_module = False
        for phase_name, modules in phases:
            if found_module:
                process_phase(modules, coordinator, phase_name=phase_name)
            else:
                found_module = process_phase(modules, coordinator, args.resume_from, phase_name)

    add_raw_text('_scraped_bill_ids', 'bills_texts', '_staging_congressional', 'bicam', ['bill_id', 'url'],  ['bill_id', 'formatted_text'], 'text', 'raw_text')
    add_raw_text('_scraped_print_ids', 'committeeprints_texts', '_staging_congressional', 'bicam', ['print_id', 'url'],  ['print_id', 'formatted_text'],  'text', 'raw_text')
    add_raw_text('_scraped_report_ids', 'committeereports_texts', '_staging_congressional', 'bicam', ['report_id', 'url'],  ['report_id', 'formatted_text'],  'text', 'raw_text')
    add_raw_text('_scraped_hearing_ids', 'hearings_texts', '_staging_congressional', 'bicam', ['hearing_id', 'url'],  ['hearing_id', 'formatted_text'],  'text', 'raw_text')
    add_raw_text('_scraped_amendment_ids', 'amendments_texts', '_staging_congressional', 'bicam', ['amendment_id', 'url'],  ['amendment_id', 'html'],  'text', 'raw_text')
    # update_amendment_columns()
if __name__ == "__main__":
    main(post_only=False)
    
    # TODO LIST:
# 1: final cleaning for congressional
# 4: link up govinfo/congressional into BICAM
# 4.5: RESCRAPE SPOTCHECK SCRIPT
# 5: export zips
# 6: finish lobbying matching
# 7: clean/port over code to github
### Processing order notes:
# standardize dates
# remove tags
# add raw texts
# redo counts for tables
# expand ids into parts in main tables
# make script to join the two
# rescrape congressional directories


# make spot check for committee bills/reports: hsru00, hsvr00
# hsag00
# hsap00
# hsju00
# hswm00
# slin00
# ssap00
# ssas00
# ssfi00
# ssfr00
# ssju00
# ssra00
# jslc00
# jspr00
# ssva00
# ssbu00
# scnc00
# hsbu00
# hsas29
# hsvr09
# hsha27
# hswm04
# hswm06
# hsju13
# hswm02
# hsap24
# hlfd00
# hsvr08
# hswm01
# hsba21
# hsvr11
# hlzs00
# hsvr10


# FULL CHECKS WITHOUT SUBCOMMITTEES
# hsap23



# establishing authority link for committee history
