"""
Streamlined Architecture for Bicam Collection

A simplified, efficient 4-layer architecture that reduces complexity by 50%
while preserving all existing custom logic through a plugin system.

Core Components:
- StreamlinedExecutor: Direct execution coordinator
- StreamlinedFetcher: Integrated fetching with parallel processing
- StreamlinedCleaner: Focused data cleaning with validation
- StreamlinedNormalizer: Focused database normalization

Usage:
    from streamlined import execute_streamlined_pipeline, ResourceCoordinator

    coordinator = ResourceCoordinator()
    results = await execute_streamlined_pipeline(
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
from .cleaner import StreamlinedCleaner
from .executor import StreamlinedExecutor, execute_streamlined_pipeline
from .fetcher import StreamlinedFetcher

# Legacy utilities (deprecated - use streamlined versions above)
from .libs import (
    HierarchicalCheckpointManager,
    RunManager,
)
from .normalizer import StreamlinedNormalizer

# Plugin system
from .plugins import (
    CleanerPlugin,
    FetcherPlugin,
    NormalizerPlugin,
    get_consolidated_registry,
)

# Processing components
from .processing import (
    AdaptiveWorkQueue,
    DynamicKeyPool,
)

# Resource management (prioritize streamlined versions)
from .resources import (
    APIKeyManager,
    CheckpointManager,
    ClientManager,
    DatabaseManager,  # Use streamlined version
    ResourceCoordinator,
    RunTrackingManager,
    StorageManager,
    StreamlinedConfig,  # Use streamlined version
    # Dagster integration
    StreamlinedProcessingResource,
    create_streamlined_dagster_resource,
    get_streamlined_coordinator_from_context,
    setup_streamlined_database,  # Simple database setup
    streamlined_processing_resource,
)

__all__ = [
    # Core components
    "StreamlinedExecutor",
    "StreamlinedFetcher",
    "StreamlinedCleaner",
    "StreamlinedNormalizer",
    "execute_streamlined_pipeline",
    # Resource management (streamlined versions)
    "ResourceCoordinator",
    "StreamlinedConfig",  # Primary config class
    "DatabaseManager",  # Primary database manager
    "setup_streamlined_database",  # Simple database setup
    "APIKeyManager",
    "CheckpointManager",
    "RunTrackingManager",
    "StorageManager",
    "ClientManager",
    # Plugin system
    "get_consolidated_registry",
    "FetcherPlugin",
    "CleanerPlugin",
    "NormalizerPlugin",
    # Processing
    "AdaptiveWorkQueue",
    "DynamicKeyPool",
    # API clients
    "CongressionalAPIClient",
    "GovInfoAPIClient",
    # Legacy utilities (deprecated)
    "HierarchicalCheckpointManager",
    "RunManager",
    # Dagster integration
    "StreamlinedProcessingResource",
    "create_streamlined_dagster_resource",
    "get_streamlined_coordinator_from_context",
    "streamlined_processing_resource",
]

__version__ = "2.0.0"
__author__ = "Bicam Collection Team"
