import psycopg2 as pg
import os
from dotenv import load_dotenv
import subprocess

load_dotenv()

def main():
    """
    Clean up and reset the congressional data pipeline.

    This function performs several cleanup operations:
    1. Removes all files in the api-congress directory
    2. Drops all tables in the _sandbox_ryan schema related to congressional data
    3. Truncates all tables in the congressional schema
    4. Resets the last processed dates to 1789-01-01
    5. Clears the congressional errors table

    The function connects to the PostgreSQL database using environment variables
    and executes the cleanup operations in a single transaction.

    Environment Variables Required:
        POSTGRESQL_DB: Database name
        POSTGRESQL_USER: Database user
        POSTGRESQL_PASSWORD: Database password 
        POSTGRESQL_HOST: Database host
        POSTGRESQL_PORT: Database port

    Raises:
        psycopg2.Error: If there are any database connection or query execution errors
    """
    # Remove all files in api-congress directory
    command = "rm -r /mnt/big_data/database-congress/api-congress/*"
    try:
        subprocess.run(command, shell=True)
    except Exception as e:
        pass
        
    # Connect to the database
    conn = pg.connect(
        dbname=os.getenv("POSTGRESQL_DB"),
        user=os.getenv("POSTGRESQL_USER"),
        password=os.getenv("POSTGRESQL_PASSWORD"),
        host=os.getenv("POSTGRESQL_HOST"),
        port=os.getenv("POSTGRESQL_PORT"))

    cur = conn.cursor()

    # Get all tables in congressional schema and truncate them
    cur.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = '_staging_congressional'
        AND table_type = 'BASE TABLE';
    """)
    prod_tables = cur.fetchall()
    if prod_tables is not None:
        for table in prod_tables:
            cur.execute(f"truncate table _staging_congressional.{table[0]}")
    else:
        pass

    # Reset last processed dates to beginning
    cur.execute("""
        update __metadata.last_processed_dates
        set last_processed_date = '1789-01-01'
    """)
    
    # Clear error tracking table
    cur.execute("""
        truncate table __metadata.congressional_errors;
    """)

    # Commit all changes
    conn.commit()

    # Close database connection
    cur.close()

if __name__ == "__main__":
    main()