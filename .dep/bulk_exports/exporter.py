import psycopg2
import pandas as pd
import os
import zipfile
from collections import defaultdict
from tqdm import tqdm
from dotenv import load_dotenv
import gc
import argparse
import shutil

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
    output_dir,
    clean_dir=False,
    prefix_filter=None  # Added prefix filter argument
):
    """
    Export tables from a PostgreSQL schema into zip files grouped by table prefix.
    Optionally filters by a specific prefix.
    """
    # Clean output directory if requested (cleans entire directory)
    if clean_dir and os.path.exists(output_dir):
        print(f"Cleaning output directory: {output_dir}")
        shutil.rmtree(output_dir)
        
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
        
        tables_df = pd.read_sql_query(query, conn, params=[schema])
        all_table_names = tables_df['table_name'].tolist() # Keep a list of all tables for ref checks
        
        # Group tables by prefix
        prefix_groups = defaultdict(list)
        for table in all_table_names:
            prefix = get_prefix(table)
            if prefix:  # Only add tables with valid prefixes
                prefix_groups[prefix].append(table)
            
        # Add special reference tables to bills group if it exists
        if 'bills' in prefix_groups:
            for ref_table in BILLS_EXTRA_TABLES:
                # Check if ref_table exists in the database schema
                if ref_table in all_table_names and ref_table not in prefix_groups['bills']:
                    prefix_groups['bills'].append(ref_table)
        
        # Filter groups if a prefix_filter is provided
        if prefix_filter:
            if prefix_filter in prefix_groups:
                print(f"Filtering export to only include the '{prefix_filter}' prefix.")
                target_group = prefix_groups[prefix_filter]
                prefix_groups = {prefix_filter: target_group}
            else:
                print(f"Warning: Prefix '{prefix_filter}' not found or has no associated tables. No tables will be exported.")
                prefix_groups = {} # Clear groups to prevent export

        # Clear tables DataFrame from memory
        del tables_df
        gc.collect()
        
        # Export each selected group to a separate zip file
        for prefix, group_tables in prefix_groups.items():
            zip_path = os.path.join(output_dir, f"bicam_{prefix}.zip")
            print(f"\nProcessing {prefix} group ({len(group_tables)} tables)...")
            
            # Remove existing zip file for this prefix if it exists, before creating a new one
            if os.path.exists(zip_path):
                os.remove(zip_path)
                
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                # Create progress bar for tables in this group
                for table in tqdm(group_tables, desc=f"Exporting {prefix} tables"):
                    # Get row count for progress bar
                    count_query = f"SELECT COUNT(*) FROM {schema}.{table}"
                    row_count = pd.read_sql_query(count_query, conn).iloc[0,0]
                    
                    chunk_size = 10000
                    
                    temp_csv = os.path.join(output_dir, f"{table}.csv")
                    # Ensure temp csv doesn't exist from a previous failed run
                    if os.path.exists(temp_csv):
                        os.remove(temp_csv)
                    
                    print(f"\nExporting {table} ({row_count:,} rows)")
                    
                    # Process chunks directly to CSV instead of holding in memory
                    with tqdm(total=row_count, desc=f"Rows from {table}", leave=False) as pbar:
                        first_chunk = True
                        for chunk_df in pd.read_sql_query(
                            f"SELECT * FROM {schema}.{table}",
                            conn,
                            chunksize=chunk_size
                        ):
                            # Write chunk directly to CSV
                            chunk_df.to_csv(
                                temp_csv,
                                mode='a' if not first_chunk else 'w',
                                header=first_chunk,
                                index=False,
                                encoding='utf-8'
                            )
                            first_chunk = False
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
            
            print(f"Created/Updated {zip_path} with {len(group_tables)} tables")

        # Re-create the master zip file ("bicam.zip") containing all individual prefix zips
        # This ensures it's always up-to-date, even when only one prefix was processed
        master_zip_path = os.path.join(output_dir, "bicam.zip")
        if os.path.exists(master_zip_path):
            print(f"Removing existing master zip: {master_zip_path}")
            os.remove(master_zip_path)
            
        print("\nCreating master bicam.zip file...")
        with zipfile.ZipFile(master_zip_path, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            prefix_zip_count = 0
            for file in sorted(os.listdir(output_dir)): # Sort for consistent order
                if file.startswith("bicam_") and file.endswith(".zip"):
                    individual_zip_path = os.path.join(output_dir, file)
                    zip_file.write(individual_zip_path, file)
                    prefix_zip_count += 1
            print(f"Created {master_zip_path} containing {prefix_zip_count} prefix zip files.")

    finally:
        conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Export PostgreSQL schema tables to CSV files')
    parser.add_argument('--clean', action='store_true', help='Clean output directory before export')
    parser.add_argument('--prefix', type=str, default=None, help='Only export tables with this prefix (e.g., bills)') # Added prefix argument
    args = parser.parse_args()
    
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
        output_dir="/mnt/big_data/database-congress/bicam-exports",
        clean_dir=args.clean,
        prefix_filter=args.prefix # Pass prefix argument
    )