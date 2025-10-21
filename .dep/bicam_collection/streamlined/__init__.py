"""
Streamlined Architecture for Bicam Collection

This module implements a simplified 4-layer architecture that reduces complexity
from the original 8-layer pipeline while preserving all existing custom logic
through a plugin system.

Architecture Overview:
1. Enhanced CLI (Direct execution)
2. Direct Asset Executor (Combined Pipeline Runner + Generalized Assets)
3. Streamlined Components (Fetcher, Cleaner, Normalizer with integrated processing)
4. Integrated Processing (WorkQueue + KeyPool + Storage)

Key Benefits:
- 50% reduction in execution path complexity
- Centralized error handling
- Direct execution without unnecessary indirection
- Preserved custom logic through plugin system
- SOLID compliance with focused responsibilities

Usage Examples:
    # Direct execution without Dagster
    from bicam_collection.streamlined import StreamlinedExecutor, ResourceCoordinator

    coordinator = ResourceCoordinator()
    executor = StreamlinedExecutor(coordinator)
    results = await executor.execute_data_type("bills", ["raw", "staging", "production"])

    # Or use the convenience function
    from bicam_collection.streamlined import execute_streamlined_pipeline

    results = await execute_streamlined_pipeline(
        coordinator=coordinator,
        data_type="bills",
        phases=["raw", "staging", "production"]
    )
"""

# Core streamlined components
from .cleaner import StreamlinedCleaner
from .executor import StreamlinedExecutor, execute_streamlined_pipeline
from .fetcher import StreamlinedFetcher
from .normalizer import StreamlinedNormalizer
from .plugins.base import CleanerPlugin, FetcherPlugin, NormalizerPlugin

# Plugin system
from .plugins.registry import PluginRegistry, get_plugin_registry
from .processing.key_pool import DynamicKeyPool, PooledAPIKey

# Processing components
from .processing.work_queue import AdaptiveWorkQueue, WorkChunk
from .resources.config import StreamlinedConfig

# Resource management
from .resources.coordinator import ResourceCoordinator
from .resources.managers import (
    APIKeyManager,
    CheckpointManager,
    ClientManager,
    DatabaseManager,
    RunTrackingManager,
    StorageManager,
)

__all__ = [
    # Core components
    "StreamlinedExecutor",
    "StreamlinedFetcher",
    "StreamlinedCleaner",
    "StreamlinedNormalizer",
    "execute_streamlined_pipeline",
    # Resource management
    "ResourceCoordinator",
    "StreamlinedConfig",
    "DatabaseManager",
    "APIKeyManager",
    "CheckpointManager",
    "RunTrackingManager",
    "StorageManager",
    "ClientManager",
    # Plugin system
    "get_plugin_registry",
    "PluginRegistry",
    "FetcherPlugin",
    "CleanerPlugin",
    "NormalizerPlugin",
    # Processing components
    "AdaptiveWorkQueue",
    "WorkChunk",
    "DynamicKeyPool",
    "PooledAPIKey",
]

# Version info
__version__ = "1.0.0"
__description__ = "Streamlined Architecture for Bicam Collection"
