import logging
import sys
from pathlib import Path

# Add parent directory to Python path
sys.path.append(str(Path(__file__).parent.parent))

from cleaning_coordinator import CleaningCoordinator
from datatypes import bills

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def debug_bills():
    # Create coordinator with just bills
    coordinator = CleaningCoordinator()
    
    # First register and run congresses since bills depends on it
    coordinator.register_module('congresses', lambda: None, 'congressional')  # Dummy function since tables exist
    coordinator.register_module('bills', bills.clean_bills, 'congressional')
    
    coordinator.initialize()
    
    # Add debug logging
    logger.info("Starting bills processing...")
    logger.info(f"Dependency graph: {coordinator.dependency_graph}")
    
    try:
        bills.clean_bills(coordinator=coordinator)
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        # Check processed tables
        logger.info(f"Processed tables: {coordinator.processed_tables}")
        logger.info(f"Module completion order: {coordinator.module_completion_order}")
        raise

if __name__ == "__main__":
    debug_bills()