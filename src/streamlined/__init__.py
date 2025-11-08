"""
Streamlined Architecture for Bicam Collection

A simplified, efficient 4-layer architecture that reduces complexity by 50%
while preserving all existing custom logic through a plugin system.

Core Components:
- Executor: Direct execution coordinator
- Fetcher: Integrated fetching with parallel processing
- Cleaner: Focused data cleaning with validation
- Normalizer: Focused database normalization

Usage:
    from streamlined import execute_pipeline, ResourceCoordinator

    coordinator = ResourceCoordinator()
    results = await execute_pipeline(
        coordinator=coordinator,
        data_type="bills",
        phases=["raw", "staging", "production"]
    )
"""

# Core components
# API clients
from .api_clients import (
    CongressionalAPIClient,
    GovInfoAPIClient,
)
from .cleaner import Cleaner
from .executor import Executor, execute_pipeline
from .fetcher import Fetcher

# Legacy utilities (deprecated - use streamlined versions above)
from .libs import (
    HierarchicalCheckpointManager,
    RunManager,
)
from .normalizer import Normalizer

# Plugin system
from .plugins import (
    CleanerPlugin,
    FetcherPlugin,
    NormalizerPlugin,
    get_registry,
)

# Processing components
from .processing import (
    AdaptiveWorkQueue,
)

# Resource management (prioritize streamlined versions)
from .resources import (
    APIKeyManager,
    CheckpointManager,
    ClientManager,
    Config,  # Use streamlined version
    DatabaseManager,  # Use streamlined version
    # Dagster integration
    ProcessingResource,
    ResourceCoordinator,
    RunTrackingManager,
    StorageManager,
    create_dagster_resource,
    get_coordinator_from_context,
    processing_resource,
    setup_database,  # Simple database setup
)

__all__ = [
    # Core components
    "Executor",
    "Fetcher",
    "Cleaner",
    "Normalizer",
    "execute_pipeline",
    # Resource management (streamlined versions)
    "ResourceCoordinator",
    "Config",  # Primary config class
    "DatabaseManager",  # Primary database manager
    "setup_database",  # Simple database setup
    "APIKeyManager",
    "CheckpointManager",
    "RunTrackingManager",
    "StorageManager",
    "ClientManager",
    # Plugin system
    "get_registry",
    "FetcherPlugin",
    "CleanerPlugin",
    "NormalizerPlugin",
    # Processing
    "AdaptiveWorkQueue",
    # API clients
    "CongressionalAPIClient",
    "GovInfoAPIClient",
    # Legacy utilities (deprecated)
    "HierarchicalCheckpointManager",
    "RunManager",
    # Dagster integration
    "ProcessingResource",
    "create_dagster_resource",
    "get_coordinator_from_context",
    "processing_resource",
]

__version__ = "2.0.0"
__author__ = "Bicam Collection Team"
