import tempfile
from pathlib import Path
import io
import csv
import time
import concurrent.futures
from contextlib import contextmanager
from typing import List, Literal, Optional
from dotenv import load_dotenv
import logging
from tqdm import tqdm
import os
import psycopg2
import hashlib
import re
from tenacity import retry, stop_after_attempt, wait_exponential

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
class DatabaseOperation:
    """Class to manage database operations with automatic connection/cursor handling"""
    def __init__(self, conn=None, cursor=None):
        self.conn = conn
        self.cursor = cursor
        
    def ensure_connection(self):
        """Ensure we have a valid connection and cursor"""
        if self.conn is None or self.conn.closed:
            self.conn = psycopg2.connect(
                host=os.getenv("POSTGRESQL_HOST"),
                port=os.getenv("POSTGRESQL_PORT"),
                database=os.getenv("POSTGRESQL_DATABASE"),
                user=os.getenv("POSTGRESQL_USERNAME"),
                password=os.getenv("POSTGRESQL_PASSWORD"),
                connect_timeout=30,
                keepalives=1,
                keepalives_idle=30,
                keepalives_interval=10,
                keepalives_count=5
            )
            self.conn.set_session(autocommit=True)
            
        if self.cursor is None or self.cursor.closed:
            self.cursor = self.conn.cursor()
            self.cursor.execute("SET session_replication_role = replica")
            self.cursor.execute("SET synchronous_commit = off")
            self.cursor.execute("SET maintenance_work_mem = '1GB'")
            self.cursor.execute("SET temp_buffers = '1GB'")
            self.cursor.execute("SET work_mem = '256MB'")

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def execute(self, sql, params=None):
        """Execute SQL with automatic connection recovery"""
        try:
            self.ensure_connection()
            if params:
                self.cursor.execute(sql, params)
            else:
                self.cursor.execute(sql)
            return self.cursor
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            # Close everything and force reconnection
            if self.cursor and not self.cursor.closed:
                self.cursor.close()
            if self.conn and not self.conn.closed:
                self.conn.close()
            self.cursor = None
            self.conn = None
            raise  # Let retry handle it

    def cleanup(self):
        """Clean up connections"""
        if self.cursor and not self.cursor.closed:
            self.cursor.close()
        if self.conn and not self.conn.closed:
            self.conn.close()



def ensure_constraint(cursor, schema_name: str, table_name: str, key_columns: List[str], text_prefix_length: int = 1000, df=None):
    """Helper function to check and fix constraints"""
    logger.debug(f"Starting ensure_constraint for {schema_name}.{table_name}")
    logger.debug(f"Key columns: {key_columns}")
    
    try:
        # Check if table exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = %s AND table_name = %s
            )
        """, (schema_name, table_name))
        
        exists_result = cursor.fetchone()
        if not exists_result or not exists_result[0]:
            logger.warning(f"Table {schema_name}.{table_name} does not exist yet")
            return

        # Check for existing unique constraints and primary keys
        cursor.execute("""
            SELECT indexname, indexdef
            FROM pg_indexes
            WHERE schemaname = %s 
            AND tablename = %s
            AND (indexdef LIKE '%%UNIQUE%%' OR indexdef LIKE '%%PRIMARY KEY%%')
        """, (schema_name, table_name))
        
        existing_indexes = cursor.fetchall() or []
        logger.debug(f"Found existing indexes: {existing_indexes}")
        
        # First try with regular columns (no LEFT expressions)
        try:
            if not existing_indexes:
                idx_name = f"{table_name}_unique_idx"
                index_sql = f"""
                    CREATE UNIQUE INDEX {idx_name}
                    ON {schema_name}.{table_name}
                    ({', '.join(key_columns)})
                """
                logger.debug(f"Creating new index with SQL: {index_sql}")
                cursor.execute(index_sql)
                logger.debug("New index created successfully")
            
        except psycopg2.errors.ProgramLimitExceeded as e:
            if "index row size" in str(e).lower():
                logger.warning("Index row size exceeded, falling back to prefix-based index")
                # Create index with LEFT expressions for text columns
                needed_expressions = [
                    f"LEFT({col}, {text_prefix_length})" if df is not None and df[col].dtype == 'object' else col
                    for col in key_columns
                ]
                
                index_sql = f"""
                    CREATE UNIQUE INDEX {idx_name}
                    ON {schema_name}.{table_name}
                    ({', '.join(needed_expressions)})
                """
                logger.debug(f"Creating prefix-based index with SQL: {index_sql}")
                cursor.execute(index_sql)
                logger.debug("New prefix-based index created successfully")
            else:
                raise
        except psycopg2.Error as e:
            logger.warning(f"Failed to create new index: {str(e)}")
            logger.warning("Continuing without new index")
            return
            
    except Exception as e:
        logger.error(f"Error ensuring constraints: {str(e)}")
        logger.error("Exception details:", exc_info=True)
        raise

    logger.debug("ensure_constraint completed successfully")
    
def get_dependency_order(db, schema_name: str, table_name: str, data_type_prefix: str) -> List[tuple[str, str]]:
    """Get dependent tables only within the same data type prefix"""
    # First get all related tables within the data type
    db.execute("""
        WITH RECURSIVE deps AS (
            -- Base case: tables that directly reference our target table
            SELECT DISTINCT 
                tc.table_schema as schema_name,
                tc.table_name as table_name,
                ccu.table_name as referenced_table,
                1 as level
            FROM information_schema.table_constraints AS tc 
            JOIN information_schema.constraint_column_usage AS ccu
                ON tc.constraint_name = ccu.constraint_name
                AND tc.table_schema = ccu.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
                AND tc.table_schema = %s
                -- Only include tables that start with our data type prefix
                AND tc.table_name LIKE %s
                AND ccu.table_name = %s
            
            UNION
            
            -- Recursive case: tables that reference our dependent tables
            SELECT DISTINCT
                tc.table_schema,
                tc.table_name,
                ccu.table_name,
                deps.level + 1
            FROM information_schema.table_constraints AS tc 
            JOIN information_schema.constraint_column_usage AS ccu
                ON tc.constraint_name = ccu.constraint_name
                AND tc.table_schema = ccu.table_schema
            JOIN deps 
                ON tc.table_schema = deps.schema_name 
                AND ccu.table_name = deps.table_name
            WHERE tc.constraint_type = 'FOREIGN KEY'
                AND tc.table_schema = %s
                -- Only include tables that start with our data type prefix
                AND tc.table_name LIKE %s
        )
        SELECT DISTINCT schema_name, table_name, level
        FROM deps 
        -- Additional filter to ensure we only get tables with our prefix
        WHERE table_name LIKE %s
        ORDER BY level DESC
    """, (schema_name, f"{data_type_prefix}_%", table_name, 
          schema_name, f"{data_type_prefix}_%", f"{data_type_prefix}_%"))
    
    return [(row[0], row[1]) for row in db.cursor.fetchall()]

def safe_truncate_tables(db: DatabaseOperation, schema_name: str, table_name: str, 
                        data_type_prefix: str) -> None:
    """Safely truncate tables without affecting other data types"""
    try:
        # Get all dependent tables in correct order
        dependent_tables = get_dependency_order(db, schema_name, table_name, data_type_prefix)
        
        # Add the main table if it's not already included
        tables_to_truncate = [(schema_name, table_name)] + dependent_tables
        
        # Verify all tables start with our prefix
        for _, table in tables_to_truncate:
            if not table.startswith(data_type_prefix):
                logger.warning(f"Skipping table {table} as it doesn't match prefix {data_type_prefix}")
                return
        
        # First disable triggers to prevent cascading effects
        db.execute("SET session_replication_role = 'replica'")
        
        # Truncate tables in reverse order
        for schema, table in reversed(tables_to_truncate):
            truncate_sql = f"TRUNCATE TABLE {schema}.{table}"
            logger.debug(f"Truncating table: {schema}.{table}")
            db.execute(truncate_sql)
            
    finally:
        # Re-enable triggers
        db.execute("SET session_replication_role = 'origin'")

def create_temp_table(db: DatabaseOperation, temp_table: str, columns: List[str]) -> None:
    """Create a temp table that survives connection resets"""
    try:
        # Create temp table with a session-independent persistence
        db.execute(f"DROP TABLE IF EXISTS {temp_table}")
        db.execute(f"""
            CREATE UNLOGGED TABLE {temp_table} (
                {', '.join(f"{col} TEXT" for col in columns)}
            )
        """)
    except Exception as e:
        logger.error(f"Error creating temp table {temp_table}: {str(e)}")
        raise

def process_chunk_with_recovery(db: DatabaseOperation, chunk_df, temp_table: str, schema_name: str,
                              table_name: str, insert_sql: str, df_columns: List[str], 
                              checkpoint_size: int = 10000) -> int:
    """Process chunks with progressive recovery"""
    total_processed = 0
    sub_chunks = []
    
    # Split into smaller sub-chunks for more frequent commits
    for i in range(0, len(chunk_df), checkpoint_size):
        sub_chunks.append(chunk_df.iloc[i:i + checkpoint_size])
    
    for sub_chunk in sub_chunks:
        retries = 0
        max_retries = 3
        
        while retries < max_retries:
            try:
                # Create fresh connection for each sub-chunk
                db.ensure_connection()
                
                # Create temp table
                create_temp_table(db, temp_table, df_columns)
                
                # Get count before insertion
                db.execute(f"SELECT COUNT(*) FROM {schema_name}.{table_name}")
                before_count = db.cursor.fetchone()[0]
                
                # Copy to temp table
                output = io.StringIO()
                sub_chunk.to_csv(output, index=False, header=False, 
                               quoting=csv.QUOTE_NONNUMERIC,
                               escapechar='\\', doublequote=True, na_rep='')
                output.seek(0)
                
                copy_sql = f"""
                    COPY {temp_table} ({', '.join(df_columns)})
                    FROM STDIN WITH (FORMAT CSV, QUOTE '"', ESCAPE '\\', NULL '')
                """
                
                db.cursor.copy_expert(sql=copy_sql, file=output)
                
                # Verify temp table
                db.execute(f"SELECT COUNT(*) FROM {temp_table}")
                temp_count = db.cursor.fetchone()[0]
                if temp_count != len(sub_chunk):
                    raise Exception(f"Temp table count mismatch. Expected {len(sub_chunk)}, got {temp_count}")
                
                # Execute insert
                db.execute(insert_sql)
                
                # Verify insertion
                db.execute(f"SELECT COUNT(*) FROM {schema_name}.{table_name}")
                after_count = db.cursor.fetchone()[0]
                rows_inserted = after_count - before_count
                
                total_processed += rows_inserted
                
                # Clean up temp table
                db.execute(f"DROP TABLE IF EXISTS {temp_table}")
                
                # Success - break retry loop
                break
                
            except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                retries += 1
                logger.warning(f"Connection error processing sub-chunk, attempt {retries}: {str(e)}")
                
                try:
                    db.execute(f"DROP TABLE IF EXISTS {temp_table}")
                except:
                    pass
                
                if retries >= max_retries:
                    raise Exception(f"Failed to process sub-chunk after {max_retries} attempts: {str(e)}")
                
                time.sleep(2 ** retries)
                db.cleanup()  # Force connection cleanup
                
                # Create fresh connection
                db = DatabaseOperation()
    
    return total_processed

def write_data(df, qualified_table_name: str, key_columns: List[str] = None, 
               text_prefix_length: int = 1000, if_exists: Optional[Literal['replace', 'truncate', 'append']] = 'truncate', 
               chunk_size: int = 50000):
    """PostgreSQL writer with robust temp table handling"""
    schema_name, table_name = qualified_table_name.split('.')
    logger.debug(f"Key columns: {key_columns}")
    insert_sql = None
    temp_table = f"temp_{table_name}_{int(time.time())}"
    
    # Filter out rows where all values are NULL
    df = df.dropna(how='all')
    if len(df) == 0:
        logger.warning(f"All rows were NULL for {qualified_table_name}, skipping write")
        return
        
    db = DatabaseOperation()
    try:
        db.ensure_connection()
        cursor = db.cursor
        cursor.execute("SET session_replication_role = replica")
        cursor.execute("SET synchronous_commit = off")
        db.execute("SET maintenance_work_mem = '1GB'")
        db.execute("SET temp_buffers = '1GB'")
        db.execute("SET work_mem = '256MB'")

        # Get target table columns and their types
        db.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """, (schema_name, table_name))

        results = cursor.fetchall()
        target_columns = [row[0] for row in results]
        column_info = {row[0]: {'type': row[1]} for row in results}

        logger.debug(f"Target columns: {target_columns}")
        logger.debug(f"Source columns: {df.columns.tolist()}")
        logger.debug(f"Number of rows in source: {len(df)}")

        # Convert float columns to int where target is integer
        for col in df.columns:
            if col in column_info and column_info[col]['type'].upper() == 'INTEGER':
                if df[col].dtype == 'float64':
                    # Convert float to int, handling NaN values
                    df[col] = df[col].fillna(-999999)  # Temporary placeholder
                    df[col] = df[col].astype('int64')
                    df.loc[df[col] == -999999, col] = None  # Restore NULL values
                    logger.debug(f"Converted column {col} from float to int")

        # Create temp table for staging with all source columns
        temp_table = f"temp_{table_name}_{int(time.time())}"
        temp_columns = [f"{col} TEXT" for col in df.columns]
        db.execute(f"DROP TABLE IF EXISTS {temp_table}")  # Ensure clean slate
        db.execute(f"""
            CREATE TEMP TABLE {temp_table} (
                {', '.join(temp_columns)}
            )
        """)

        if if_exists in ('truncate', 'replace'):
            # Get the data type prefix (e.g., 'bills', 'amendments')
            data_type_prefix = table_name.split('_')[0]

            # Get all dependent tables in correct order
            dependent_tables = get_dependency_order(db, schema_name, table_name, data_type_prefix)

            if if_exists == 'truncate':
                # Collect all tables that need to be truncated
                tables_to_truncate = [(schema_name, table_name)] + dependent_tables

                # Build single TRUNCATE statement for all tables
                truncate_tables = [f"{schema}.{table}" for schema, table in tables_to_truncate]
                if truncate_tables:
                    truncate_sql = f"TRUNCATE TABLE {', '.join(truncate_tables)} CASCADE"
                    db.execute(truncate_sql)
                    logger.debug(f"Truncated tables: {', '.join(truncate_tables)}")

            elif if_exists == 'replace':
                # Drop tables in reverse dependency order
                for dep_schema, dep_table in dependent_tables:
                    db.execute(f"DROP TABLE IF EXISTS {dep_schema}.{dep_table}")
                    logger.debug(f"Dropped dependent table {dep_schema}.{dep_table}")

                db.execute(f"DROP TABLE IF EXISTS {schema_name}.{table_name}")
                logger.debug(f"Dropped main table {schema_name}.{table_name}")

        # Try to create regular primary key first
        if key_columns:
            try:
                ensure_constraint(cursor, schema_name, table_name, key_columns, text_prefix_length, df)
            except psycopg2.errors.ProgramLimitExceeded as e:
                error_message = str(e).lower()
                logger.warning(f"Index limit exceeded, attempting fallback solution. Error: {e}")

                # Drop existing primary key and indexes
                db.execute("""
                    DO $$ 
                    BEGIN
                        -- Drop primary key
                        EXECUTE 'ALTER TABLE ' || quote_ident(%s) || '.' || quote_ident(%s) || ' DROP CONSTRAINT IF EXISTS ' || quote_ident(%s || '_pkey');

                        -- Drop any existing unique indexes
                        FOR ix IN (
                            SELECT indexname 
                            FROM pg_indexes 
                            WHERE schemaname = %s 
                            AND tablename = %s 
                            AND indexdef LIKE '%%UNIQUE%%'
                        ) LOOP
                            EXECUTE 'DROP INDEX IF EXISTS ' || ix.indexname;
                        END LOOP;
                    END $$;
                """, (schema_name, table_name, table_name, schema_name, table_name))

                # Create new primary key using MD5 hash for text columns
                idx_name = f"{table_name}_pkey"
                needed_expressions = [
                    f"md5({col})" if df[col].dtype == 'object' and col in ['text'] else col  # Only hash the text column
                    for col in key_columns
                ]

                index_sql = f"""
                    ALTER TABLE {schema_name}.{table_name} 
                    ADD PRIMARY KEY ({', '.join(needed_expressions)})
                """
                logger.debug(f"Creating new primary key with MD5 hash: {index_sql}")
                db.execute(index_sql)
                logger.debug("New primary key created successfully")

                # Update conflict clause to use MD5 for text column
                conflict_cols = [
                    f"md5({col})" if df[col].dtype == 'object' and col in ['text'] else col
                    for col in key_columns
                ]
                conflict_clause = f"ON CONFLICT ({', '.join(conflict_cols)}) DO NOTHING"
        # Check if we have a prefix-based index
        db.execute("""
            SELECT indexdef 
            FROM pg_indexes 
            WHERE schemaname = %s 
            AND tablename = %s 
            AND indexdef LIKE '%%left%%'
        """, (schema_name, table_name))

        prefix_index = cursor.fetchone()
        using_prefix_index = bool(prefix_index)

        # Build conflict clause based on existing index structure
        if key_columns:
            if using_prefix_index:
                # Use LEFT expressions to match the existing index
                conflict_cols = [
                    f'LEFT({col}, {text_prefix_length})' if df[col].dtype == 'object' else col
                    for col in key_columns
                ]
            else:
                conflict_cols = key_columns

            conflict_clause = f"ON CONFLICT ({', '.join(conflict_cols)}) DO NOTHING"
        else:
            conflict_clause = ""

        # Get target table columns and their types in correct order
        db.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """, (schema_name, table_name))

        results = cursor.fetchall()
        target_columns = [row[0] for row in results]
        column_info = {row[0]: {'type': row[1]} for row in results}

        logger.debug(f"Target columns: {target_columns}")
        logger.debug(f"Source columns: {df.columns.tolist()}")
        logger.debug(f"Number of rows in source: {len(df)}")

        # Build cast expressions for each target column
        cast_columns = []
        for col in target_columns:
            col_type = column_info[col]['type'].upper()
            # First handle NULL/empty string conversion for all types
            base_expr = f"""
                CASE 
                    WHEN {col} IS NULL THEN NULL
                    WHEN TRIM({col}) = '' THEN NULL
                    WHEN LOWER(TRIM({col})) = 'none' THEN NULL
                    WHEN LOWER(TRIM({col})) = 'null' THEN NULL
                    ELSE TRIM({col})
                END
            """

            # Then handle type-specific casting
            if col_type == 'INTEGER':
                cast_columns.append(f"""
                    CASE 
                        WHEN ({base_expr}) IS NULL THEN NULL
                        WHEN POSITION('.' IN ({base_expr})) > 0 
                            THEN SPLIT_PART(({base_expr}), '.', 1)::integer
                        ELSE ({base_expr})::integer
                    END as {col}
                """)
            elif col_type == 'BOOLEAN':
                cast_columns.append(f"""
                    CASE 
                        WHEN ({base_expr}) IS NULL THEN NULL
                        WHEN LOWER(({base_expr})) IN ('true', 't', 'yes', 'y', '1') THEN TRUE
                        WHEN LOWER(({base_expr})) IN ('false', 'f', 'no', 'n', '0') THEN FALSE
                        ELSE NULL
                    END as {col}
                """)
            elif col_type == 'TIMESTAMP WITHOUT TIME ZONE':
                cast_columns.append(f"""
                    CASE 
                        WHEN ({base_expr}) IS NULL THEN NULL
                        WHEN TRIM({base_expr}) = '' THEN NULL
                        ELSE TRIM({base_expr})::timestamp
                    END as {col}
                """)
            elif col_type == 'TIMESTAMP WITH TIME ZONE':
                cast_columns.append(f"""
                    CASE 
                        WHEN ({base_expr}) IS NULL THEN NULL
                        WHEN TRIM({base_expr}) = '' THEN NULL
                        ELSE TRIM({base_expr})::timestamp with time zone
                    END as {col}
                """)
            elif col_type == 'TIMESTAMP':
                cast_columns.append(f"""
                    CASE 
                        WHEN ({base_expr}) IS NULL THEN NULL
                        WHEN TRIM({base_expr}) = '' THEN NULL
                        ELSE TRIM({base_expr})::timestamp
                    END as {col}
                """)
            elif col_type == 'DATE':
                cast_columns.append(f"""
                    CASE 
                        WHEN ({base_expr}) IS NULL THEN NULL
                        ELSE ({base_expr})::date
                    END as {col}
                """)
            elif col_type == 'DOUBLE':
                cast_columns.append(f"""
                    CASE 
                        WHEN ({base_expr}) IS NULL THEN NULL
                        ELSE ({base_expr})::double
                    END as {col}
                """)
            else:
                cast_columns.append(f"{base_expr} as {col}")

        # Build insert SQL with explicit target columns
        insert_sql = f"""
            INSERT INTO {schema_name}.{table_name} ({', '.join(target_columns)})
            SELECT {', '.join(cast_columns)}
            FROM {temp_table}
            {conflict_clause}
        """
        logger.debug(f"Built insert SQL: {insert_sql}")  # Add debug logging

        total_inserted = 0
        checkpoint_size = 5000  # Size of sub-chunks for more frequent commits

        with tqdm(total=len(df), desc=f"Writing to {table_name}", unit="rows") as pbar:
            for start_idx in range(0, len(df), chunk_size):
                try:
                    end_idx = min(start_idx + chunk_size, len(df))
                    chunk_df = df.iloc[start_idx:end_idx]

                    # Process chunk with recovery mechanism
                    rows_inserted = process_chunk_with_recovery(
                        db, chunk_df, temp_table, schema_name,
                        table_name, insert_sql, df.columns.tolist(),
                        checkpoint_size=checkpoint_size
                    )

                    total_inserted += rows_inserted
                    pbar.update(rows_inserted)

                except Exception as e:
                    logger.error(f"Failed to process chunk {start_idx}:{end_idx}: {str(e)}")
                    logger.error(f"Successfully processed {total_inserted} rows before failure")
                    raise

                finally:
                    try:
                        db.execute(f"DROP TABLE IF EXISTS {temp_table}")
                    except:
                        pass
        # Final verification
        db.execute(f"SELECT COUNT(*) FROM {schema_name}.{table_name}")
        final_count = db.cursor.fetchone()[0]

        if final_count == 0:
            raise Exception(f"No data was written to {qualified_table_name}")

        logger.debug(f"Successfully wrote {final_count} rows to {qualified_table_name}")

    except Exception as e:
        logger.error(f"Error writing data to {qualified_table_name}: {str(e)}")
        if insert_sql:
            logger.error(f"Last Insert SQL was: {insert_sql}")
        raise
    finally:
        try:
            db.execute(f"DROP TABLE IF EXISTS {temp_table}")
        except:
            pass
        db.cleanup()