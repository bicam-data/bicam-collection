import logging
import psycopg2
import os
from typing import List, Optional, Dict, Set, Tuple, Any, Callable

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CleaningCoordinator:
    def __init__(self):
        self.truncated_tables = {}  # schema.table -> set of dependent tables
        self.processed_tables = set()  # tracks fully processed tables
        self.deferred_tables = set()  # tables that need to be processed at the end
        self.module_completion_order = []  # Track order of completed modules
        self.deferred_table_modules = {}  # track which module owns each deferred table
        self.module_dependencies = {}  # track dependencies between modules
        self.cleaning_modules = []  # Will be populated by register_module
        self.module_main_tables = {}  # Track main table for each module
        self.dependency_graph = None  # Will be built after all modules are registered

    def mark_table_processed(self, schema: str, table: str):
        """Mark a table as processed and check for dependent tables"""
        full_name = f"{schema}.{table}"
        logger.info(f"Marking table {full_name} as processed")
        
        self.processed_tables.add(full_name)
        
        # Check if this was a deferred table
        if full_name in self.deferred_table_modules:
            module_name = self.deferred_table_modules[full_name]
            
            # Only complete the module if this was the main table
            if full_name == f"{schema}.{module_name}":
                # Check if all module tables are done
                all_module_tables_done = all(
                    f"{schema}.{table}" in self.processed_tables
                    for schema, table in self.get_module_tables(module_name)
                )
                
                if all_module_tables_done:
                    self.notify_module_completion(module_name)

    def notify_module_completion(self, module_name: str):
        """Track module completion and trigger deferred processing if needed"""
        # Verify the main table exists before marking complete
        module_info = next((m for m in self.cleaning_modules if m[0] == module_name), None)
        if module_info:
            _, _, schema = module_info
            main_table = module_name.lstrip('_')
            main_table_name = f"{schema}.{main_table}"
            
            # Only mark complete if main table exists and has data
            if main_table_name in self.processed_tables:
                count = self.verify_table_population(schema, main_table)
                if count > 0:
                    if module_name not in self.module_completion_order:
                        self.module_completion_order.append(module_name)
                        logger.info(f"Module {module_name} completed. Completion order: {self.module_completion_order}")
                    
                    # Check if we can process any deferred tables
                    self.process_ready_deferred_tables()

    def register_module(self, name: str, clean_func: Callable, schema: str):
        """Register a cleaning module"""
        self.cleaning_modules.append((name, clean_func, schema))
        logger.info(f"Registered cleaning module: {name}")

    def initialize(self):
        """Initialize coordinator after all modules are registered"""
        self.dependency_graph = self.build_full_dependency_graph()
        
    
    
    def build_full_dependency_graph(self) -> Dict[str, Set[str]]:
        """Build complete dependency graph for all tables"""
        logger.info("Building complete dependency graph...")
        
        # Collect all unique schemas from registered modules
        schemas = {schema for _, _, schema in self.cleaning_modules}
        schemas.update({schema.replace('_staging_', '') for _, _, schema in self.cleaning_modules if schema.startswith('_staging_')})
        
        logger.info(f"Building dependency graph for schemas: {schemas}")
        
            # Track generated tables so we can exclude them from dependencies
        generated_tables = set()
        for module in self.cleaning_modules:
            name, _, schema = module
            # Add known generated tables (like notes tables)
            generated_tables.add(f"{schema}.{name}_notes")
            generated_tables.add(f"{schema}.{name}_notes_links")
    
        
        db = psycopg2.connect(
            host=os.getenv("POSTGRESQL_HOST"),
            port=os.getenv("POSTGRESQL_PORT"),
            database=os.getenv("POSTGRESQL_DATABASE"),
            user=os.getenv("POSTGRESQL_USERNAME"),
            password=os.getenv("POSTGRESQL_PASSWORD")
        )
        try:
            with db.cursor() as cur:
                schema_list = ', '.join(f"'{s}'" for s in schemas)
                staging_schema_list = ', '.join(f"'_staging_{s}'" for s in schemas)
                
                cur.execute(f"""
                    WITH RECURSIVE fk_tree AS (
                        -- Base case: direct foreign keys
                        SELECT 
                            tc.table_schema || '.' || tc.table_name as dependent_table,
                            ccu.table_schema || '.' || ccu.table_name as referenced_table,
                            1 as level,
                            ARRAY[tc.table_schema || '.' || tc.table_name] as path
                        FROM information_schema.table_constraints tc
                        JOIN information_schema.constraint_column_usage ccu 
                            ON tc.constraint_name = ccu.constraint_name
                        WHERE tc.constraint_type = 'FOREIGN KEY'
                        AND (tc.table_schema IN ({schema_list}) 
                            OR tc.table_schema IN ({staging_schema_list}))
                        AND (ccu.table_schema IN ({schema_list})
                            OR ccu.table_schema IN ({staging_schema_list}))
                        
                        UNION
                        
                        -- Recursive case: transitive dependencies
                        SELECT 
                            tc.table_schema || '.' || tc.table_name,
                            ccu.table_schema || '.' || ccu.table_name,
                            ft.level + 1,
                            ft.path || (tc.table_schema || '.' || tc.table_name)
                        FROM information_schema.table_constraints tc
                        JOIN information_schema.constraint_column_usage ccu 
                            ON tc.constraint_name = ccu.constraint_name
                        JOIN fk_tree ft ON ft.dependent_table = ccu.table_schema || '.' || ccu.table_name
                        WHERE tc.constraint_type = 'FOREIGN KEY'
                            AND (tc.table_schema IN ({schema_list})
                                OR tc.table_schema IN ({staging_schema_list}))
                            AND NOT (tc.table_schema || '.' || tc.table_name = ANY(ft.path))
                    )
                    SELECT DISTINCT 
                        referenced_table,
                        array_agg(DISTINCT dependent_table) as dependent_tables
                    FROM fk_tree
                    GROUP BY referenced_table
                    ORDER BY referenced_table;
                """)

                dependency_graph = {}
                for row in cur.fetchall():
                    referenced_table = row[0]
                    dependent_tables = set(row[1])
                    # Filter out generated tables from dependencies
                    dependent_tables = {t for t in dependent_tables if t not in generated_tables}
                    
                    if dependent_tables:  # Only add if there are non-generated dependencies
                        dependency_graph[referenced_table] = dependent_tables

                        # Log dependencies for inspection
                        logger.info(f"Dependencies for {referenced_table}: {dependent_tables}")

                logger.info(f"Dependency graph: {dependency_graph}")
                return dependency_graph
        finally:
            db.close()

    def identify_deferred_tables(self) -> Set[str]:
        """Identify tables that will be affected by future truncations"""
        deferred = set()
        processed_types = set()
        
        for name, _, schema in self.cleaning_modules:
            table_name = f"{schema}.{name}"
            
            # Look for dependencies from future data types
            for dep_table in self.dependency_graph.get(table_name, set()):
                dep_type = dep_table.split('.')[-1].split('_')[0]
                
                # If the dependent table is from a data type we haven't processed yet
                if dep_type not in processed_types:
                    deferred.add(dep_table)
                    logger.info(f"Marking {dep_table} for deferred processing due to dependency on {table_name}")
            
            processed_types.add(name)
        
        return deferred
    
    def should_defer_table(self, schema: str, table: str) -> bool:
        """Check if a table should be deferred based on dependencies"""
        full_name = f"{schema}.{table}"
        logger.info(f"Checking deferrals for {full_name}")
        
        # Get current module's name (parent module of this table)
        current_module = table.split('_')[0]
        
        if full_name in self.dependency_graph:
            dependent_tables = self.dependency_graph[full_name]
            logger.info(f"Found dependencies: {dependent_tables}")
            
            for dep_table in dependent_tables:
                # Skip dependencies from same module
                dep_module_name = dep_table.split('.')[-1].split('_')[0]
                if dep_module_name == current_module:
                    continue
                    
                module_info = self.get_module_for_table(dep_table)
                if module_info is None:
                    logger.warning(f"No module found for dependent table {dep_table}")
                    continue
                    
                dep_module = module_info[0]
                if dep_module not in self.module_completion_order:
                    logger.info(f"Deferring {full_name} due to dependency on {dep_table} from {dep_module}")
                    return True
        return False
        
    def get_module_for_table(self, full_table_name: str) -> Optional[Tuple[str, Any, str]]:
        """Find the cleaning module responsible for a table"""
        schema, table = full_table_name.split('.')
        table_prefix = table.split('_')[0]
        
        logger.info(f"Looking for module for table {full_table_name}")
        logger.info(f"Table prefix: {table_prefix}")
        logger.info(f"Available modules: {[name for name, _, _ in self.cleaning_modules]}")
        
        for name, func, schema_name in self.cleaning_modules:
            if name == table_prefix and schema_name == schema:
                return (name, func, schema_name)
        logger.warning(f"No module found for table {full_table_name}")
        return None

    def get_module_tables(self, module_name: str) -> List[Tuple[str, str]]:
        """Get all tables associated with a module"""
        module_tables = []
        for (schema, table), owner in self.deferred_table_modules.items():
            if owner == module_name:
                module_tables.append((schema, table))
        return module_tables

    def add_deferred_table(self, schema: str, table: str, owner_module: str):
        """Track a deferred table and its owning module"""
        full_name = f"{schema}.{table}"
        self.deferred_table_modules[full_name] = owner_module
        
        # Update module dependencies
        if owner_module not in self.module_dependencies:
            self.module_dependencies[owner_module] = set()
        
        # Add dependencies from other modules
        if full_name in self.dependency_graph:
            for dep_table in self.dependency_graph[full_name]:
                dep_module = self.get_module_for_table(dep_table)
                if dep_module and dep_module[0] != owner_module:
                    self.module_dependencies[owner_module].add(dep_module[0])
                    logger.info(f"Added module dependency: {owner_module} depends on {dep_module[0]}")

    def get_ready_modules(self) -> Set[str]:
        """Get modules whose dependencies are all satisfied"""
        ready_modules = set()
        completed_modules = set(self.module_completion_order)
        
        for module, deps in self.module_dependencies.items():
            if deps.issubset(completed_modules) and module not in completed_modules:
                ready_modules.add(module)
        
        return ready_modules

    def process_ready_deferred_tables(self):
        """Process deferred tables whose dependencies are now met"""
        ready_modules = self.get_ready_modules()
        
        for module in ready_modules:
            module_tables = [
                (schema, table) for (schema, table), owner in self.deferred_table_modules.items()
                if owner == module
            ]
            
            if module_tables:
                logger.info(f"Processing deferred tables for module {module}")
                module_info = next((m for m in self.cleaning_modules if m[0] == module), None)
                if module_info:
                    _, clean_func, _ = module_info
                    try:
                        clean_func()
                        # Remove processed tables from deferred tracking
                        for schema, table in module_tables:
                            full_name = f"{schema}.{table}"
                            self.deferred_table_modules.pop(full_name, None)
                    except Exception as e:
                        logger.error(f"Error processing deferred tables for {module}: {str(e)}")
                        raise

    def verify_table_population(self, schema_name: str, table_name: str) -> int:
        """Verify that a table has data and return the count"""
        conn = psycopg2.connect(
            host=os.getenv("POSTGRESQL_HOST"),
            port=os.getenv("POSTGRESQL_PORT"),
            database=os.getenv("POSTGRESQL_DATABASE"),
            user=os.getenv("POSTGRESQL_USERNAME"),
            password=os.getenv("POSTGRESQL_PASSWORD")
        )
        try:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {schema_name}.{table_name}")
                count = cur.fetchone()[0]
                logger.info(f"Table {schema_name}.{table_name} has {count} rows")
                return count
        except Exception as e:
            logger.error(f"Error verifying table population for {schema_name}.{table_name}: {str(e)}")
            return 1
        finally:
            conn.close()

    def run_cleaning_scripts(self, resume_from: Optional[str] = None):
        """Run all cleaning scripts with proper dependency handling"""
        # Calculate start index if resuming
        start_idx = 0
        if resume_from:
            for idx, (name, _, _) in enumerate(self.cleaning_modules):
                if name == resume_from:
                    start_idx = idx
                    break
            else:
                raise ValueError(f"Module {resume_from} not found")

        # First identify all tables that will need deferred processing
        self.deferred_tables = self.identify_deferred_tables()
        logger.info(f"Identified tables for deferred processing: {self.deferred_tables}")
        
        # Initial pass: process non-deferred tables
        for name, clean_func, schema in self.cleaning_modules[start_idx:]:
            try:
                logger.info(f"Processing module {name}...")
                clean_func()
                self.notify_module_completion(name)
                
                # Verify main table population
                main_table = name.lstrip('_')
                try:
                    count = self.verify_table_population(schema, main_table)
                except Exception as e:
                    logger.error(f"Error verifying table population for {schema}.{main_table}: {str(e)}")
                    continue
                
                if count == 0:
                    raise Exception(f"No data was written to {schema}.{main_table}")
                
                logger.info(f"Successfully completed {name} cleaning script with {count} rows")
                
            except Exception as e:
                logger.error(f"Error in {name} cleaning script: {str(e)}")
                raise

        # Final pass: process any remaining deferred tables
        logger.info("Processing remaining deferred tables...")
        while self.deferred_table_modules:
            ready_modules = self.get_ready_modules()
            if not ready_modules:
                remaining = list(self.deferred_table_modules.keys())
                logger.error(f"Unable to process remaining deferred tables due to dependencies: {remaining}")
                break
                
            for module in ready_modules:
                try:
                    logger.info(f"Processing deferred tables for module {module}")
                    module_info = next((m for m in self.cleaning_modules if m[0] == module), None)
                    if module_info:
                        _, clean_func, _ = module_info
                        clean_func()
                        self.notify_module_completion(module)
                except Exception as e:
                    logger.error(f"Error processing deferred tables for {module}: {str(e)}")
                    raise