from datetime import datetime
import time
import asyncpg
import asyncio
import csv
import logging
import os
import sys
import argparse
from urllib.parse import quote_plus
from tqdm import tqdm

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

csv.field_size_limit(sys.maxsize)

def setup_pool():
    db = {
        'user': os.getenv('POSTGRESQL_USER'),
        'password': quote_plus(os.getenv('POSTGRESQL_PASSWORD')),
        'host': os.getenv('POSTGRESQL_HOST'),
        'port': os.getenv('POSTGRESQL_PORT'),
        'database': os.getenv('POSTGRESQL_DB')
    }
    dsn = f"postgresql://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['database']}"
    return asyncpg.create_pool(dsn)

def convert_value(value, target_type):
    if value == '':
        return None
    if target_type.startswith('int'):
        return int(value)
    if target_type.startswith('float') or target_type.startswith('numeric'):
        return float(value)
    if target_type.startswith('timestamp') or target_type.startswith('date'):
        try:
            # Try parsing ISO 8601 format with timezone
            return datetime.strptime(value, '%Y-%m-%dT%H:%M:%S%z')
        except ValueError:
            try:
                # Try parsing ISO 8601 format without timezone
                return datetime.strptime(value, '%Y-%m-%dT%H:%M:%S')
            except ValueError:
                try:
                    # Try parsing date only
                    return datetime.strptime(value, '%Y-%m-%d').date()
                except ValueError:
                    try:
                        # Try parsing date and time without T separator
                        return datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
                    except ValueError:
                        try:
                            # Try parsing date and time with timezone
                            return datetime.strptime(value, '%Y-%m-%d %H:%M:%S%z')
                        except ValueError:
                            # If all else fails, return None
                            return None
    if target_type == 'boolean':
        return value.lower() in ('true', 't', 'yes', 'y', '1')
    return value  # Keep as string for text, varchar, etc.

async def get_column_types(conn, schema, table_name):
    table_name = f"_{table_name}" if "staging" in schema else table_name
    query = """
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = $1 AND table_name = $2
    """
    results = await conn.fetch(query, schema, table_name)
    return {row['column_name']: row['data_type'].lower() for row in results}

async def create_table(conn, combined_table_name, headers):
    columns = ', '.join([f'"{header}" TEXT' for header in headers])
    await conn.execute(f'CREATE TABLE IF NOT EXISTS {combined_table_name} ({columns})')

async def increase_column_size(conn, table_name, column_name, new_size):
    await conn.execute(f'ALTER TABLE "{table_name}" ALTER COLUMN "{column_name}" TYPE VARCHAR({new_size})')

async def create_table_if_not_exists(conn, combined_table_name, headers):
    columns = ', '.join([f'"{header}" TEXT' for header in headers])
    create_table_query = f'''
    CREATE TABLE IF NOT EXISTS {combined_table_name} (
        {columns}
    )
    '''
    await conn.execute(create_table_query)

async def insert_csv(pool, file_path, schema, batch_size=10000):
    table_name = os.path.splitext(os.path.basename(file_path))[0]
    separator = '_' if "staging" in schema else ''
    combined_table_name = f'{schema}.{separator}{table_name}'
    problematic_rows = []

    # Create problematic rows filename
    dir_path = os.path.dirname(file_path)
    problematic_file = os.path.join(dir_path, f"{table_name}_problematic.csv")

    async with pool.acquire() as conn:
        try:
            # Count total rows for progress bar
            with open(file_path, 'r', newline='', encoding='utf-8') as f:
                total_rows = sum(1 for _ in f) - 1  # Subtract 1 for header

            with open(file_path, 'r', newline='', encoding='utf-8') as f:
                reader = csv.reader(f)
                headers = next(reader)
                
                # Create table if it doesn't exist
                columns = ', '.join([f'"{header}" TEXT' for header in headers])
                await conn.execute(f'CREATE TABLE IF NOT EXISTS {combined_table_name} ({columns})')

                # Get column types
                column_types = await get_column_types(conn, schema, table_name)

                # Get initial row count
                initial_count = await conn.fetchval(f'SELECT COUNT(*) FROM {combined_table_name}')

                # Prepare the insert statement
                insert_query = f'''
                    INSERT INTO {combined_table_name} ({", ".join(f'"{h}"' for h in headers)})
                    VALUES ({", ".join("$" + str(i) for i in range(1, len(headers) + 1))})
                    ON CONFLICT DO NOTHING
                '''
                
                batch = []
                original_rows = []  # Store original rows for error tracking
                total_processed = 0

                with tqdm(total=total_rows, desc=f"Processing {combined_table_name}", unit="rows") as pbar:
                    for row in reader:
                        try:
                            # Convert values based on column types
                            converted_row = [convert_value(cell, column_types.get(header, 'text')) 
                                          for cell, header in zip(row, headers)]
                            batch.append(converted_row)
                            original_rows.append(row)  # Store original row
                            
                            if len(batch) >= batch_size:
                                try:
                                    await conn.executemany(insert_query, batch)
                                except Exception as e:
                                    # Add failed rows to problematic_rows
                                    problematic_rows.extend(original_rows)
                                    logger.error(f"Error inserting batch: {str(e)}")
                                
                                total_processed += len(batch)
                                pbar.update(len(batch))
                                batch = []
                                original_rows = []
                        
                        except Exception as e:
                            problematic_rows.append(row)
                            logger.error(f"Error processing row: {str(e)}")
                    
                    # Process remaining batch
                    if batch:
                        try:
                            await conn.executemany(insert_query, batch)
                        except Exception as e:
                            problematic_rows.extend(original_rows)
                            logger.error(f"Error inserting final batch: {str(e)}")
                        
                        total_processed += len(batch)
                        pbar.update(len(batch))

            # Write problematic rows to new CSV if any exist
            if problematic_rows:
                with open(problematic_file, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow(headers)  # Write headers
                    writer.writerows(problematic_rows)
                logger.info(f"Wrote {len(problematic_rows)} problematic rows to {problematic_file}")
            
            # Get final row count
            final_count = await conn.fetchval(f'SELECT COUNT(*) FROM {combined_table_name}')
            total_inserted = final_count - initial_count

            logger.info(f"Total rows in {combined_table_name}: {final_count}")
            logger.info(f"Rows processed: {total_processed}")
            logger.info(f"New rows inserted: {total_inserted}")
            logger.info(f"Problematic rows: {len(problematic_rows)}")
            
            if total_processed != total_inserted:
                logger.info(f"Some rows already existed in the table or were skipped due to errors.")
            
        except Exception as e:
            logger.error(f"Error processing data from {file_path}: {str(e)}")
            raise

async def process_csv_files(directory, schema, data_type=None):
    pool = await setup_pool()
    
    try:
        csv_files = [f for f in os.listdir(directory) if f.endswith('.csv') and not f.endswith('_problematic.csv')]
        if data_type:
            # Filter for files that match the data type
            csv_files = [f for f in csv_files if f.startswith(f"{data_type}")]
            if not csv_files:
                logger.warning(f"No CSV files found for data type: {data_type}")
                return
            logger.info(f"Found {len(csv_files)} files for data type: {data_type}")
        
        for file in tqdm(csv_files, desc="Processing CSV files", unit="file"):
            await insert_csv(pool, os.path.join(directory, file), schema)
    finally:
        await pool.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Insert CSV files into PostgreSQL database.")
    parser.add_argument("directory", help="Directory containing CSV files")
    parser.add_argument("--schema", default="public", help="Database schema to use (default: public)")
    parser.add_argument("--data-type", help="Specific data type to process (e.g., 'bills', 'committees')")
    
    args = parser.parse_args()
    
    directory_path = args.directory
    schema_name = args.schema
    data_type = args.data_type
    
    asyncio.run(process_csv_files(directory_path, schema_name, data_type))