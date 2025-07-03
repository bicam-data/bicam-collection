"""
Bicam Collection Libraries

This package contains reusable utilities and libraries for the bicam-collection
data pipeline system.
"""

from .checkpoint import CheckpointManager, HierarchicalProgressTracker, ProgressTracker
from .config import BicamConfig, create_sample_config, load_config
from .data_type_router import (
    DataTypeRegistry,
    DataTypeRouter,
    create_router,
    get_global_registry,
    register_data_type,
)
from .database import DatabaseManager, setup_database, setup_database_sync
from .run_tracking import RunContext, RunManager, get_run_manager, init_run_manager
from .schema import DataTypeSchema, SchemaManager, ScrapingSchema, get_schema

__all__ = [
    # Checkpoint management
    "CheckpointManager",
    "HierarchicalProgressTracker",
    "ProgressTracker",
    # Configuration
    "BicamConfig",
    "load_config",
    "create_sample_config",
    # Data type routing
    "DataTypeRegistry",
    "DataTypeRouter",
    "create_router",
    "get_global_registry",
    "register_data_type",
    # Database
    "DatabaseManager",
    "setup_database",
    "setup_database_sync",
    # Run tracking
    "RunManager",
    "RunContext",
    "get_run_manager",
    "init_run_manager",
    # Schema management
    "SchemaManager",
    "ScrapingSchema",
    "DataTypeSchema",
    "get_schema",
]
