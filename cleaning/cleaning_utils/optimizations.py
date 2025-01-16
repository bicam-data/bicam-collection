import duckdb
from typing import Dict, Any, List
import pandas as pd
from tqdm import tqdm

def optimize_types(df: duckdb.DuckDBPyRelation, table_config: Dict[str, Any]) -> duckdb.DuckDBPyRelation:
    """
    Optimize data types based on schema configuration while preserving columns
    """
    # Ensure we're working with a DuckDB relation
    if isinstance(df, pd.DataFrame):
        with duckdb.connect() as temp_conn:
            temp_conn.execute("SET enable_progress_bar=false")
            df = temp_conn.from_df(df)
    
    # Get the current columns
    current_columns = df.columns
    
    # Create casting expressions for existing columns only
    optimize_expr = []
    for col in current_columns:
        # Find the column config
        col_config = None
        if isinstance(table_config, dict) and 'columns' in table_config:
            try:
                # Handle both list of dicts and dict formats
                if isinstance(table_config['columns'], list):
                    col_config = next((c for c in table_config['columns'] if isinstance(c, dict) and c.get('old_name') == col), None)
                elif isinstance(table_config['columns'], dict):
                    col_config = table_config['columns'].get(col)
            except (TypeError, AttributeError) as e:
                # Log error but continue processing
                print(f"Error processing column {col}: {str(e)}")
                print(f"Table config type: {type(table_config)}")
                print(f"Columns type: {type(table_config.get('columns'))}")
                col_config = None

        if col_config and isinstance(col_config, dict) and 'old_type' in col_config:
            # Cast to old_type to preserve data before transformation
            optimize_expr.append(
                f"CAST({col} AS {col_config['old_type']}) as {col}"
            )
        else:
            # Keep column as-is if not in config or missing type info
            optimize_expr.append(col)
    
    # Join expressions and create new relation
    select_sql = ','.join(optimize_expr)
    try:
        return df.select(select_sql)
    except Exception as e:
        print(f"Error in final SQL: {select_sql}")
        raise

def process_in_batches(df: duckdb.DuckDBPyRelation,
                      process_func: callable,
                      batch_size: int = 100000,
                      **kwargs) -> duckdb.DuckDBPyRelation:
    """Process large dataframes in batches"""
    # Ensure we're working with a DuckDB relation
    if isinstance(df, pd.DataFrame):
        with duckdb.connect() as temp_conn:
            temp_conn.execute("SET enable_progress_bar=false")
            df = temp_conn.from_df(df)
            
    total_rows = df.count().fetchone()[0]
    if total_rows <= batch_size:
        return process_func(df, **kwargs)

    with duckdb.connect() as temp_conn:
        
        batches = []
        for offset in tqdm(range(0, total_rows, batch_size), desc="Processing batches"):
            batch = df.limit(batch_size, offset=offset)
            processed = process_func(batch, **kwargs)
            batches.append(processed.df())
            
        return temp_conn.from_df(pd.concat(batches, ignore_index=True))

def deduplicate_with_index(conn: duckdb.DuckDBPyConnection,
                          df: duckdb.DuckDBPyRelation,
                          key_columns: List[str],
                          temp_table: str) -> duckdb.DuckDBPyRelation:
    """Deduplicate using temporary table with index"""
    # Create temporary table and index
    conn.execute(f"CREATE TEMPORARY TABLE {temp_table} AS SELECT * FROM df")
    conn.execute(f"CREATE INDEX idx_{temp_table} ON {temp_table}({','.join(key_columns)})")
    
    # Use subquery for deduplication
    dedup_query = f"""
        WITH ranked AS (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY {','.join(key_columns)}
                       ORDER BY {','.join(key_columns)}
                   ) as rn
            FROM {temp_table}
        )
        SELECT {','.join(df.columns)} 
        FROM ranked 
        WHERE rn = 1
    """
    
    result = conn.execute(dedup_query)
    
    # Clean up
    conn.execute(f"DROP TABLE IF EXISTS {temp_table}")
    
    return conn.from_df(result.df())