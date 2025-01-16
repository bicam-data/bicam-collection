import psycopg2
import pandas as pd
import os
import zipfile
from collections import defaultdict
from tqdm import tqdm
from dotenv import load_dotenv
import gc

load_dotenv()

# Additional reference tables to include with bills
BILLS_EXTRA_TABLES = [
    "ref_bill_summary_version_codes",
    "ref_bill_version_codes",
    "ref_title_type_codes",
    "crosswalk_bills_voteview"
]

def get_prefix(table_name):
    """
    Determine the correct prefix for a table, skipping ref_ and crosswalk_ prefixes
    """
    # Skip ref_ and crosswalk_ prefixes
    if table_name.startswith(('ref_', 'crosswalk_')):
        return None
    return table_name.split('_')[0]

def write_chunks_to_csv(chunks, temp_csv):
    """Write chunks to CSV file with proper encoding for text fields"""
    first_chunk = True
    for chunk in chunks:
        chunk.to_csv(
            temp_csv,
            mode='w' if first_chunk else 'a',
            index=False,
            header=first_chunk,
            encoding='utf-8'
        )
        first_chunk = False
        # Clear chunk from memory
        del chunk
        gc.collect()

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
            prefix = get_prefix(table)
            if prefix:  # Only add tables with valid prefixes
                prefix_groups[prefix].append(table)
            
        # Add special reference tables to bills group if it exists
        if 'bills' in prefix_groups:
            for ref_table in BILLS_EXTRA_TABLES:
                if ref_table in tables['table_name'].values and ref_table not in prefix_groups['bills']:
                    prefix_groups['bills'].append(ref_table)
        
        # Clear tables DataFrame from memory
        del tables
        gc.collect()
        
        # Export each group to a separate zip file
        for prefix, group_tables in prefix_groups.items():
            zip_path = os.path.join(output_dir, f"{prefix}.zip")
            print(f"\nProcessing {prefix} group ({len(group_tables)} tables)...")
            
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                # Create progress bar for tables in this group
                for table in tqdm(group_tables, desc=f"Exporting {prefix} tables"):
                    # Get row count for progress bar
                    count_query = f"SELECT COUNT(*) FROM {schema}.{table}"
                    row_count = pd.read_sql_query(count_query, conn).iloc[0,0]
                    
                    # Use smaller chunk size for tables with text fields
                    chunk_size = 10000  # Reduced chunk size for better memory management
                    
                    temp_csv = os.path.join(output_dir, f"{table}.csv")

                    
                    print(f"\nExporting {table} ({row_count:,} rows)")
                    chunks = []
                    
                    # Process chunks directly to CSV instead of holding in memory
                    with tqdm(total=row_count, desc=f"Rows from {table}", leave=False) as pbar:
                        for chunk_df in pd.read_sql_query(
                            f"SELECT * FROM {schema}.{table}",
                            conn,
                            chunksize=chunk_size
                        ):
                            # Write chunk directly to CSV
                            chunk_df.to_csv(
                                temp_csv,
                                mode='a' if os.path.exists(temp_csv) else 'w',
                                header=not os.path.exists(temp_csv),
                                index=False,
                                encoding='utf-8'
                            )
                            pbar.update(len(chunk_df))
                            
                            # Clear chunk from memory
                            del chunk_df
                            gc.collect()
                    
                    # Add CSV to zip file
                    zip_file.write(temp_csv, f"{table}.csv")
                    
                    # Remove temporary CSV file
                    os.remove(temp_csv)
                    
                    # Force garbage collection
                    gc.collect()
            
            print(f"Created {zip_path} with {len(group_tables)} tables")

    finally:
        conn.close()

if __name__ == "__main__":
    db_config = {
        "host": os.getenv("POSTGRESQL_HOST"),
        "database": os.getenv("POSTGRESQL_DB"),
        "user": os.getenv("POSTGRESQL_USER"),
        "password": os.getenv("POSTGRESQL_PASSWORD"),
        "port": os.getenv("POSTGRESQL_PORT")
    }
    
    export_schema_tables(
        db_config=db_config,
        schema="bicam",
        output_dir="/mnt/big_data/database-congress/bicam-exports"
    )