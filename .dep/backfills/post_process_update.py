import os
import psycopg2
from tqdm import tqdm
import logging

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
    
if __name__ == "__main__":
    update_amendment_columns()
    add_raw_text('_scraped_bill_ids', 'bills_texts', '_staging_congressional', 'bicam', ['bill_id', 'url'],  ['bill_id', 'formatted_text'], 'text', 'raw_text')
    add_raw_text('_scraped_print_ids', 'committeeprints_texts', '_staging_congressional', 'bicam', ['print_id', 'url'],  ['print_id', 'formatted_text'],  'text', 'raw_text')
    add_raw_text('_scraped_report_ids', 'committeereports_texts', '_staging_congressional', 'bicam', ['report_id', 'url'],  ['report_id', 'formatted_text'],  'text', 'raw_text')
    add_raw_text('_scraped_hearing_ids', 'hearings_texts', '_staging_congressional', 'bicam', ['hearing_id', 'url'],  ['hearing_id', 'formatted_text'],  'text', 'raw_text')
    add_raw_text('_scraped_amendment_ids', 'amendments_texts', '_staging_congressional', 'bicam', ['amendment_id', 'url'],  ['amendment_id', 'html'],  'text', 'raw_text')
