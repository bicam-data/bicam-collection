import psycopg2
import pandas as pd
import os
import zipfile
from collections import defaultdict
from tqdm import tqdm

from dotenv import load_dotenv

load_dotenv()

BILLS_EXTRA_TABLES = [
    "ref_bill_summary_version_codes",
    "ref_bill_version_codes",
    "ref_title_type_codes",
    "crosswalk_bills_voteview"
]

def export_schema_tables(
    db_config,
    schema,
    output_dir
):
    """
    Export all tables from a PostgreSQL schema into zip files grouped by table prefix
    """
    # Create output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Connect to PostgreSQL
    conn = psycopg2.connect(**db_config)

    try:
        # Get all tables from the specified schema
        query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = %s 
            AND table_type = 'BASE TABLE'
        """
        
        tables = pd.read_sql_query(query, conn, params=[schema])
        
        # Group tables by prefix
        prefix_groups = defaultdict(list)
        for table in tables['table_name']:
            prefix = table.split('_')[0]
            prefix_groups[prefix].append(table)
            
        # Add special reference tables to bills group if it exists
        if 'bills' in prefix_groups:
            for ref_table in BILLS_EXTRA_TABLES:
                if ref_table in tables['table_name'].values and ref_table not in prefix_groups['bills']:
                    prefix_groups['bills'].append(ref_table)
        
        # Export each group to a separate zip file
        for prefix, group_tables in prefix_groups.items():
            zip_path = os.path.join(output_dir, f"{prefix}_tables.zip")
            print(f"\nProcessing {prefix} group ({len(group_tables)} tables)...")
            
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                # Create progress bar for tables in this group
                for table in tqdm(group_tables, desc=f"Exporting {prefix} tables"):
                    # Get row count for progress bar
                    count_query = f"SELECT COUNT(*) FROM {schema}.{table}"
                    row_count = pd.read_sql_query(count_query, conn).iloc[0,0]
                    
                    # Export table in chunks with progress bar
                    chunk_size = 100000  # Adjust based on your memory constraints
                    chunks = []
                    
                    with tqdm(total=row_count, desc=f"Rows from {table}", leave=False) as pbar:
                        for chunk_df in pd.read_sql_query(
                            f"SELECT * FROM {schema}.{table}",
                            conn,
                            chunksize=chunk_size
                        ):
                            chunks.append(chunk_df)
                            pbar.update(len(chunk_df))
                    
                    # Combine chunks and save to CSV
                    df = pd.concat(chunks, ignore_index=True)
                    temp_csv = f"{table}.csv"
                    df.to_csv(temp_csv, index=False)
                    
                    # Add CSV to zip file
                    zip_file.write(temp_csv, f"{table}.csv")
                    
                    # Remove temporary CSV file
                    os.remove(temp_csv)
            
            print(f"Created {zip_path} with {len(group_tables)} tables")

    finally:
        conn.close()

if __name__ == "__main__":

    db_config = {
        "host": os.getenv("DB_HOST"),
        "database": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "port": os.getenv("DB_PORT")
    }

    export_schema_tables(
        db_config,
        schema="bicam",
        output_dir="exports"
    )