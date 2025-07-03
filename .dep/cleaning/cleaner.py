import time
import duckdb
import pandas as pd
import yaml
from pathlib import Path
from typing import Dict, List, Any, Callable, Optional
from cleaning_utils.read_data import get_tables, read_data_in_chunks
from cleaning_utils.insert_data import write_data, DatabaseOperation
import logging
from functools import lru_cache
from cleaning_utils.optimizations import optimize_types
from cleaning_coordinator import CleaningCoordinator
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class DataCleaner:
    def __init__(self, data_type: str, coordinator: CleaningCoordinator=None, resume_from: str=None):
        """
        Initialize cleaner with data type
            
        Args:
            data_type: e.g. "bills", "members", etc.
        """
        logger.info(f"Initializing DataCleaner for data type: {data_type}")
        self.data_type = data_type.lstrip('_')
        
        # Load config
        self.schema_config = self._load_schema_config()
        logger.info("Loaded schema config")
        
        self.custom_cleaners: Dict[str, Callable] = {}
        self.new_table_generators: Dict[str, Callable] = {}
        
        # Get schema information
        self.data_type_config = self.schema_config['data_types'][self.data_type]
        self.old_schema = self.data_type_config['old_schema']
        self.new_schema = self.data_type_config['new_schema']
        logger.info(f"Set up schemas - old: {self.old_schema}, new: {self.new_schema}")
        
        # Configure DuckDB to be quiet
        self.duck_conn = duckdb.connect()
        self.duck_conn.execute("SET enable_progress_bar=true")
        
        self.coordinator = coordinator  # Store reference to coordinator
        self.truncated_tables = set()
        self.processed_tables = set()
        
    def _load_schema_config(self) -> Dict[str, Any]:
        """Load the schema configuration from staging_info.yml"""
        schema_path = Path(__file__).parent / "datatypes" / "staging_info.yml"
        logger.info(f"Loading schema config from: {schema_path}")
        if not schema_path.exists():
            raise FileNotFoundError(f"Schema config file not found at {schema_path}")
        with open(schema_path) as f:
            return yaml.safe_load(f)

    @lru_cache(maxsize=32)
    def _get_table_config(self, old_table_name: str) -> Optional[Dict[str, Any]]:
        """Cached table configuration lookup"""
        for table_id, table_config in self.data_type_config['tables'].items():
            if table_config['old_name'] == old_table_name:
                return {
                    'old_schema': self.old_schema,
                    'new_schema': self.new_schema,
                    'old_table_name': table_config['old_name'],
                    'new_table_name': table_config['new_name'],
                    'columns': table_config['columns']
                }
        return None  # Return None instead of raising an error

    def register_custom_cleaner(self, table_name: str, 
                                cleaner_func: Callable[[duckdb.DuckDBPyRelation], duckdb.DuckDBPyRelation],
                                key_columns: Optional[List[str]] = None) -> None:
        """Register a custom cleaning function for a specific table"""
        self.custom_cleaners[table_name] = cleaner_func
        if key_columns:
            self.key_columns = key_columns  # Store the key columns for this table

    def register_new_table_generator(self, 
                                   source_table: str, 
                                   generator_func: Callable[[duckdb.DuckDBPyRelation], List[tuple[str, duckdb.DuckDBPyRelation]]], 
                                   table_configs: Optional[List[Dict[str, Any]]] = None) -> None:
        """
        Register a function that generates new tables from a source table
        
        Args:
            source_table: Source table name
            generator_func: Function that returns list of (new_table_name, dataframe) tuples
            table_configs: List of configurations for the generated tables, matching staging_info.yml format
                Example:
                [{
                    'old_name': 'bills_notes',
                    'new_name': 'bills_notes',
                    'columns': [
                        {'old_name': 'bill_id', 'new_name': 'bill_id', 'old_type': 'TEXT', 'new_type': 'TEXT', 'key': True},
                        ...
                    ]
                }]
        """
        self.new_table_generators[source_table] = generator_func
        
        if table_configs:
            # Add configs to data_type_config
            for config in table_configs:
                table_name = config['new_name']
                self.data_type_config['tables'][table_name] = config

    def _create_missing_columns(self, df: duckdb.DuckDBPyRelation, table_config: Dict[str, Any]) -> duckdb.DuckDBPyRelation:
        """Create columns that exist in yaml but not in data"""
        try:
            existing_cols = df.columns
            new_cols = []
            
            for col in table_config.get('columns', []):
                # Skip if not a dictionary
                if not isinstance(col, dict):
                    logger.warning(f"Skipping invalid column config: {col}")
                    continue
                    
                if col.get('old_name') == 'NONE' and col.get('old_type') == 'NONE':
                    new_name = col.get('new_name')
                    if new_name and new_name not in existing_cols:
                        new_cols.append(f"NULL::{col.get('new_type', 'TEXT')} as {new_name}")
            
            if new_cols:
                # Add new columns to existing ones
                select_expr = [f"{c}" for c in existing_cols] + new_cols
                return df.select(",".join(select_expr))
            
            return df
            
        except Exception as e:
            logger.error(f"Error in _create_missing_columns: {str(e)}")
            logger.error(f"Table config: {table_config}")
            logger.error(f"DataFrame columns: {df.columns}")
            raise

    def _drop_extra_columns(self, df: duckdb.DuckDBPyRelation, table_config: Dict[str, Any]) -> duckdb.DuckDBPyRelation:
        """Drop columns that exist in data but not in yaml config"""
        try:
            # Handle different column configuration formats
            if isinstance(table_config['columns'], list):
                valid_columns = [col['old_name'] for col in table_config['columns'] 
                               if isinstance(col, dict) and col.get('old_name') != 'NONE']
            elif isinstance(table_config['columns'], dict):
                valid_columns = [k for k in table_config['columns'].keys()]
            else:
                logger.error(f"Unexpected columns configuration type: {type(table_config['columns'])}")
                logger.error(f"Columns config: {table_config['columns']}")
                raise ValueError("Invalid columns configuration format")

            existing_columns = df.columns
            columns_to_keep = [col for col in existing_columns if col in valid_columns]
            
            if not columns_to_keep:
                logger.warning(f"No valid columns found to keep. Config columns: {valid_columns}")
                logger.warning(f"Existing columns: {existing_columns}")
                return df
            
            return df.select(','.join(columns_to_keep))
            
        except Exception as e:
            logger.error(f"Error in _drop_extra_columns: {str(e)}")
            logger.error(f"Table config: {table_config}")
            logger.error(f"DataFrame columns: {df.columns}")
            raise

    def _rename_columns(self, df: duckdb.DuckDBPyRelation, table_config: Dict[str, Any]) -> duckdb.DuckDBPyRelation:
        """Rename columns according to yaml config"""
        try:
            rename_map = {}
            for col in table_config.get('columns', []):
                if not isinstance(col, dict):
                    logger.warning(f"Skipping invalid column config: {col}")
                    continue
                    
                old_name = col.get('old_name')
                new_name = col.get('new_name')
                if old_name and new_name and old_name in df.columns:
                    rename_map[old_name] = new_name
            
            if rename_map:
                select_expr = []
                for col in df.columns:
                    if col in rename_map:
                        select_expr.append(f"{col} as {rename_map[col]}")
                    else:
                        select_expr.append(col)
                return df.select(",".join(select_expr))
            
            return df
            
        except Exception as e:
            logger.error(f"Error in _rename_columns: {str(e)}")
            logger.error(f"Table config: {table_config}")
            logger.error(f"DataFrame columns: {df.columns}")
            raise

    def _cast_columns(self, df: duckdb.DuckDBPyRelation, table_config: Dict[str, Any]) -> duckdb.DuckDBPyRelation:
        """Cast columns to their new types with proper NULL handling"""
        try:
            existing_cols = df.columns
            cast_expr = []
            
            for col in existing_cols:
                try:
                    # Find the matching config for this column
                    col_config = next(
                        (c for c in table_config.get('columns', []) 
                         if isinstance(c, dict) and (c.get('new_name') == col or c.get('old_name') == col)),
                        None
                    )
                    
                    # Base NULL check for all columns
                    base_null_check = f"""
                        CASE 
                            WHEN {col} IS NULL THEN NULL
                            WHEN CAST({col} AS VARCHAR) = '' THEN NULL 
                            WHEN LOWER(CAST({col} AS VARCHAR)) = 'none' THEN NULL
                            WHEN LOWER(CAST({col} AS VARCHAR)) = 'null' THEN NULL
                            ELSE CAST({col} AS VARCHAR)
                        END
                    """
                    
                    if col_config and isinstance(col_config, dict):
                        try:
                            # Now try to cast with proper error handling
                            new_type = col_config.get('new_type', '').upper()
                            new_name = col_config.get('new_name', col)
                            
                            if new_type == 'INTEGER':
                                cast_expr.append(f"""
                                    CASE 
                                        WHEN ({base_null_check}) IS NULL THEN NULL
                                        WHEN REGEXP_REPLACE(CAST({col} AS VARCHAR), '[^0-9]', '') = '' THEN NULL
                                        ELSE CAST(REGEXP_REPLACE(CAST({col} AS VARCHAR), '[^0-9]', '') AS INTEGER)
                                    END as {new_name}
                                """)
                            elif new_type == 'BOOLEAN':
                                cast_expr.append(f"""
                                    CASE 
                                        WHEN ({base_null_check}) IS NULL THEN NULL
                                        WHEN LOWER(CAST({base_null_check} AS VARCHAR)) IN ('true', 't', 'yes', 'y', '1') THEN TRUE
                                        WHEN LOWER(CAST({base_null_check} AS VARCHAR)) IN ('false', 'f', 'no', 'n', '0') THEN FALSE
                                        ELSE NULL
                                    END as {new_name}
                                """)
                            else:
                                cast_expr.append(f"""
                                    CASE
                                        WHEN ({base_null_check}) IS NULL THEN NULL
                                        WHEN LENGTH(TRIM(CAST({base_null_check} AS VARCHAR))) = 0 THEN NULL
                                        ELSE TRY_CAST({base_null_check} AS {new_type})
                                    END as {new_name}
                                """)
                        except Exception as e:
                            logger.warning(f"Failed to cast column {col}, applying NULL handling without cast: {str(e)}")
                            cast_expr.append(f"""
                                CASE 
                                    WHEN {col} IS NULL THEN NULL
                                    WHEN LENGTH(TRIM(CAST({col} AS VARCHAR))) = 0 THEN NULL
                                    ELSE {col}
                                END as {col}
                            """)
                    else:
                        cast_expr.append(f"""
                            CASE 
                                WHEN {col} IS NULL THEN NULL
                                WHEN LENGTH(TRIM(CAST({col} AS VARCHAR))) = 0 THEN NULL
                                ELSE {col}
                            END as {col}
                        """)
                except Exception as e:
                    logger.error(f"Error processing column {col}: {str(e)}")
                    cast_expr.append(col)
            
            return df.select(','.join(cast_expr))
            
        except Exception as e:
            logger.error(f"Error in _cast_columns: {str(e)}")
            logger.error(f"Table config: {table_config}")
            logger.error(f"DataFrame columns: {df.columns}")
            raise

    def _deduplicate(self, df: duckdb.DuckDBPyRelation, table_config: Dict[str, Any], conn: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyRelation:
        """Deduplicate based on key columns with optimizations"""
        key_columns = [
            col['new_name']  # Use new_name since columns should be renamed by now
            for col in table_config['columns']
            if col['key'] is True and col['old_name'] != 'NONE'
        ]
        
        self.key_columns = key_columns
        
        if not key_columns:
            return df
        
        logger.info(f"Deduplicating on columns: {key_columns}")
        
        try:
            # Create a unique temp table name
            temp_table = f"temp_dedup_{table_config['old_table_name']}_{int(time.time())}"
            
            # Get the column list from the relation
            columns = df.columns
            
            # Verify key columns exist in the data
            missing_keys = [col for col in key_columns if col not in columns]
            if missing_keys:
                print(df)
                raise ValueError(f"Key columns not found in data: {missing_keys}")
                
            # Create temp table with explicit column selection
            create_table_sql = f"""
                CREATE TEMPORARY TABLE {temp_table} AS 
                SELECT {', '.join(columns)} 
                FROM df
            """
            conn.execute(create_table_sql)
            
            # First count total rows for logging
            original_count = conn.execute(f"SELECT COUNT(*) FROM {temp_table}").fetchone()[0]
            
            # Perform deduplication while preserving all columns
            dedup_sql = f"""
                WITH ranked AS (
                    SELECT *, 
                        ROW_NUMBER() OVER (
                            PARTITION BY {','.join(key_columns)}
                            ORDER BY {','.join(key_columns)}
                        ) as rn
                    FROM {temp_table}
                )
                SELECT {', '.join(columns)}  -- Select only original columns
                FROM ranked 
                WHERE rn = 1
            """
            
            # Execute deduplication and store result
            deduped_df = conn.execute(dedup_sql).df()
            
            # Get count after deduplication
            new_count = len(deduped_df)
            
            logger.info(f"Removed {original_count - new_count} duplicate rows")
            
            # Convert back to DuckDB relation
            return conn.from_df(deduped_df)
            
        finally:
            # Clean up
            try:
                conn.execute(f"DROP TABLE IF EXISTS {temp_table}")
            except:
                pass

    def _apply_custom_cleaning(self, df: duckdb.DuckDBPyRelation, table_name: str) -> duckdb.DuckDBPyRelation:
        """Apply registered custom cleaning function if it exists"""
        if table_name in self.custom_cleaners:
            return self.custom_cleaners[table_name](df)
        return df
    
    def track_truncated_tables(self, schema_name: str, table_name: str, data_type_prefix: str) -> List[str]:
        """Track tables that get truncated via CASCADE"""
        # Get all tables that could be affected by CASCADE
        db = DatabaseOperation()
        db.execute("""
            WITH RECURSIVE deps AS (
                -- Direct dependencies
                SELECT DISTINCT
                    tc.table_schema,
                    tc.table_name,
                    ccu.table_name as referenced_table,
                    1 as level
                FROM information_schema.table_constraints tc
                JOIN information_schema.constraint_column_usage ccu 
                    ON tc.constraint_name = ccu.constraint_name
                WHERE tc.constraint_type = 'FOREIGN KEY'
                    AND ccu.table_schema = %s
                    AND ccu.table_name = %s
                
                UNION ALL
                
                -- Recursive dependencies
                SELECT DISTINCT
                    tc.table_schema,
                    tc.table_name,
                    ccu.table_name,
                    d.level + 1
                FROM information_schema.table_constraints tc
                JOIN information_schema.constraint_column_usage ccu 
                    ON tc.constraint_name = ccu.constraint_name
                JOIN deps d ON d.table_name = ccu.table_name
                WHERE tc.constraint_type = 'FOREIGN KEY'
                    AND tc.table_schema = %s
            )
            SELECT DISTINCT table_name 
            FROM deps 
            WHERE table_name LIKE %s
            ORDER BY table_name
        """, (schema_name, table_name, schema_name, f"{data_type_prefix}_%"))
        
        affected_tables = [row[0] for row in db.cursor.fetchall()]
        
        # Add to truncated set
        for table in affected_tables:
            if table not in self.processed_tables:
                self.truncated_tables.add(table)
        
        return affected_tables

    def _generate_new_tables(self, df: duckdb.DuckDBPyRelation, source_table: str) -> None:
        """Generate new tables from source table using registered generator"""
        if source_table in self.new_table_generators:
            generated_tables = self.new_table_generators[source_table](df)
            for table_name, table_df in generated_tables:
                qualified_name = f"{self.new_schema}.{table_name}"
                logger.info(f"Writing generated table {qualified_name}")
                
                # Get the config for this generated table
                table_config = next(
                    (config for config in self.data_type_config['tables'].values() 
                     if config['new_name'] == table_name),
                    None
                )
                
                if table_config:
                    # Get key columns from the generated table's config
                    key_columns = [
                        col['new_name']
                        for col in table_config['columns']
                        if col.get('key', False)
                    ]
                    logger.info(f"Using key columns for {table_name}: {key_columns}")
                    
                    # Write with the correct key columns
                    self.write_data(
                        table_df, 
                        qualified_name,
                        if_exists='truncate',
                        chunk_size=250000,
                        key_columns=key_columns  # Override self.key_columns
                    )
                else:
                    logger.warning(f"No config found for generated table {table_name}")
                    self.write_data(table_df, qualified_name)

    def clean_table(self, table_name: str) -> None:
        """Clean a single table and track truncations"""
        try:
            table_config = self._get_table_config(table_name)
            if table_config is None:
                logger.info(f"Skipping table {table_name} - no configuration found")
                return
                
            logger.info(f"Cleaning table {table_name}...")
            
                    # Check with coordinator if table should be deferred
            if self.coordinator and self.coordinator.should_defer_table(self.new_schema, table_config['new_table_name']):
                logger.info(f"Deferring table {table_name} for later processing")
                self.coordinator.add_deferred_table(self.new_schema, table_config['new_table_name'], self.data_type)
                return

            
            # Check if table was truncated and needs reprocessing
            if table_name in self.truncated_tables and table_name in self.processed_tables:
                logger.info(f"Reprocessing truncated table {table_name}")
            
            chunks = []
            full_table_name = f"{self.old_schema}.{table_name}"
            
            with duckdb.connect() as conn:
                conn.execute("SET enable_progress_bar=true")
                
                for chunk_df in read_data_in_chunks(full_table_name):
                    if chunk_df is not None and not chunk_df.empty:
                        # Convert pandas DataFrame to DuckDB relation
                        df = conn.from_df(chunk_df)
                        df = optimize_types(df, table_config)
                        
                        # Apply transformations in correct order
                        transformations = [
                            ("Dropping extra columns", self._drop_extra_columns),
                            ("Creating missing columns", self._create_missing_columns),
                            ("Renaming columns", self._rename_columns),
                            ("Applying custom cleaning", self._apply_custom_cleaning),
                            ("Deduplicating", lambda df, config: self._deduplicate(df, config, conn)),
                            ("Casting columns", self._cast_columns)
                        ]
                        
                        for desc, func in transformations:
                            logger.info(f"Transforming {table_name} - {desc}")
                            if func == self._apply_custom_cleaning:
                                df = func(df, table_name)
                            else:
                                df = func(df, table_config)
                        
                        # Store back as pandas
                        chunks.append(df.df())
                        
                        if len(chunks) >= 5:
                            combined_df = pd.concat(chunks, ignore_index=True)
                            df = conn.from_df(combined_df)
                            df = self._deduplicate(df, table_config, conn)
                            chunks = [df.df()]
                
                if chunks:
                    # Final processing
                    combined_df = pd.concat(chunks, ignore_index=True)
                    df = conn.from_df(combined_df)
                    df = self._deduplicate(df, table_config, conn)
                    
                    # Ensure we have a pandas DataFrame for writing
                    final_df = df.df() if isinstance(df, duckdb.DuckDBPyRelation) else df
                    
                    qualified_table_name = f"{self.new_schema}.{table_config['new_table_name']}"

                    # Write data with deduplication handling
                    self.write_data(
                        final_df, 
                        qualified_table_name,
                        if_exists='truncate',
                        chunk_size=250000
                    )
                    
                    self.processed_tables.add(table_name)

                    logger.info(f"Successfully processed table {table_name}")
                    if 'TRUNCATE' in str(table_config.get('if_exists', '')).upper():
                        affected_tables = self.track_truncated_tables(
                            self.old_schema, table_name, table_name.split('_')[0]
                        )
                        if affected_tables:
                            logger.info(f"Tracked cascaded truncations for tables: {affected_tables}")
                
                    logger.info(f"Successfully processed table {table_name}")
                    
                    # Generate any new tables if configured
                    self._generate_new_tables(df, table_name)
                
                if self.coordinator:
                    self.coordinator.mark_table_processed(self.new_schema, table_config['new_table_name'])
                    
            chunks.clear()
            del chunks
                    
        except Exception as e:
            logger.error(f"Error processing table {table_name}: {str(e)}")
            raise
    
    def clean_all(self, resume_from: str = None) -> None:
        """Clean all tables with coordinator integration"""
        tables = self._get_tables_in_order()
        
        if resume_from:
            try:
                start_idx = tables.index(resume_from)
                tables = tables[start_idx:]
                logger.info(f"Resuming from {resume_from}")
            except ValueError:
                logger.warning(f"Resume point {resume_from} not found, starting from beginning")
        
        # First pass: regular tables
        regular_tables = []
        deferred_tables = []
        
        for table in tables:
            table_config = self._get_table_config(table)
            if table_config:
                qualified_name = f"{self.new_schema}.{table_config['new_table_name']}"
                if self.coordinator and self.coordinator.should_defer_table(self.new_schema, table_config['new_table_name']):
                    deferred_tables.append(table)
                else:
                    regular_tables.append(table)
        
        # Process regular tables
        for table in regular_tables:
            try:
                self.clean_table(table)
            except Exception as e:
                logger.error(f"Failed at table {table}. To resume, use resume_from='{table}'")
                raise
        
        # Let coordinator know we've finished regular processing
        if self.coordinator:
            self.coordinator.notify_module_completion(self.data_type)

    def _get_tables_in_order(self) -> List[str]:
        """Get tables sorted with parent tables first"""
        tables = get_tables(self.old_schema, self.data_type)
        
        # Separate parent and child tables
        parent_tables = []
        child_tables = []
        
        for table in tables:
            # Parent table name matches data_type exactly or is prefixed with underscore
            if table == self.data_type or table == f"_{self.data_type}":
                parent_tables.append(table)
            else:
                child_tables.append(table)
        
        # Sort each group alphabetically for consistent ordering
        parent_tables.sort()
        child_tables.sort()
        
        logger.info(f"Processing order - Parent tables: {parent_tables}, Child tables: {child_tables}")
        
        # Return parent tables first, then child tables
        return parent_tables + child_tables

    def write_data(self, df: Any, qualified_table_name: str, 
                   if_exists: str = 'truncate', chunk_size: int = 250000,
                   key_columns: Optional[List[str]] = None) -> None:
        """Write data to database with schema handling and type checking
        
        Args:
            df: pandas DataFrame or DuckDB relation to write
            qualified_table_name: schema.table_name format string
            if_exists: How to handle existing table ('truncate' by default)
            chunk_size: Number of rows per chunk
            key_columns: Override default key columns from self.key_columns
        """
        # If it's a DuckDB relation, convert to pandas
        if isinstance(df, duckdb.DuckDBPyRelation):
            df = df.df()
        # Use provided key_columns or fall back to self.key_columns
        keys = key_columns if key_columns is not None else self.key_columns
        write_data(df, qualified_table_name, key_columns=keys, if_exists=if_exists, chunk_size=chunk_size)
