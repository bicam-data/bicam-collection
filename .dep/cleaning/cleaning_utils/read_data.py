import duckdb 
import psycopg2
import os
from dotenv import load_dotenv
import logging
from typing import List, Tuple, Dict, Any
from tqdm import tqdm
import gc
import time
import pandas as pd

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_tables(schema_name: str, data_type: str) -> List[str]:
    """
    Get all tables in schema beginning with data type prefix
    Returns table names without schema prefix
    
    Args:
        schema_name: Schema to search in
        data_type: Type of data (e.g. 'bills', 'members')
    """
    conn = psycopg2.connect(
        host=os.getenv("POSTGRESQL_HOST"),
        port=os.getenv("POSTGRESQL_PORT"),
        database=os.getenv("POSTGRESQL_DATABASE"),
        user=os.getenv("POSTGRESQL_USERNAME"),
        password=os.getenv("POSTGRESQL_PASSWORD")
    )
    cursor = conn.cursor()
    
    # Handle schema name with or without underscore
    
    cursor.execute(
        """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = %s 
        AND table_name LIKE %s
        """, 
        (schema_name, f"_{data_type}%")
    )

    return [row[0] for row in cursor.fetchall()]

def read_data_in_chunks(qualified_table_name: str, chunk_size: int = 100000):
    """Read data in chunks to handle large tables"""
    conn = None
    try:
        conn = psycopg2.connect(
            host=os.getenv("POSTGRESQL_HOST"),
            port=os.getenv("POSTGRESQL_PORT"),
            database=os.getenv("POSTGRESQL_DATABASE"),
            user=os.getenv("POSTGRESQL_USERNAME"),
            password=os.getenv("POSTGRESQL_PASSWORD")
        )
        conn.set_session(readonly=True)  # Set read-only mode for safety
        
        # Check if table exists
        schema, table = qualified_table_name.split('.')
        with conn.cursor() as check_cursor:
            check_cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = %s 
                    AND table_name = %s
                )
            """, (schema, table))
            
            if not check_cursor.fetchone()[0]:
                logger.warning(f"Table {qualified_table_name} does not exist")
                return

        # Get column names and check for duplicates
        with conn.cursor() as col_cursor:
            col_cursor.execute(f"SELECT * FROM {qualified_table_name} LIMIT 0")
            if col_cursor.description is None:
                logger.warning(f"Table {qualified_table_name} is empty or has no columns")
                return
            colnames = [desc[0] for desc in col_cursor.description]
            
            # Check for duplicate column names
            if len(colnames) != len(set(colnames)):
                logger.error(f"Found duplicate column names in {qualified_table_name}: {colnames}")
                duplicates = [col for col in colnames if colnames.count(col) > 1]
                raise ValueError(f"Duplicate column names found: {duplicates}. This must be fixed in the source data.")

        # Get total count
        with conn.cursor() as count_cursor:
            count_cursor.execute(f"SELECT COUNT(*) FROM {qualified_table_name}")
            total_rows = count_cursor.fetchone()[0]
            
            if total_rows == 0:
                logger.warning(f"Table {qualified_table_name} is empty")
                return

        # Fetch data in chunks using a server-side cursor
        with conn.cursor(name='fetch_cursor', scrollable=False, withhold=True) as fetch_cursor:
            fetch_cursor.itersize = chunk_size
            fetch_cursor.execute(f"SELECT * FROM {qualified_table_name}")
            
            with tqdm(total=total_rows, desc=f"Reading {table}", unit="rows") as pbar:
                while True:
                    try:
                        rows = fetch_cursor.fetchmany(chunk_size)
                        if not rows:
                            break
                            
                        # Create DataFrame with explicit column names to prevent auto-renaming
                        chunk_df = pd.DataFrame(rows, columns=colnames)
                        pbar.update(len(chunk_df))
                        yield chunk_df
                    except Exception as e:
                        logger.error(f"Error fetching chunk from {qualified_table_name}: {str(e)}")
                        break

    except Exception as e:
        logger.error(f"Error reading from {qualified_table_name}: {str(e)}")
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass