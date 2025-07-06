"""
Resource Management for Streamlined Pipeline

This module provides specialized resource managers that replace the God Object pattern
with focused, single-responsibility components that can be composed together.

Key improvements:
- DatabaseManager: Focused database connection pooling and schema setup
- APIKeyManager: Focused API key distribution and management
- CheckpointManager: Focused checkpoint operations
- RunTrackingManager: Focused run tracking operations
- StorageManager: Focused storage operations
- ClientManager: Focused API client lifecycle
- ResourceCoordinator: Lightweight composition of all managers
- StreamlinedProcessingResource: Dagster integration for ResourceCoordinator
"""

from .config import StreamlinedConfig
from .coordinator import ResourceCoordinator
from .dagster_integration import (
    StreamlinedProcessingResource,
    create_streamlined_dagster_resource,
    get_streamlined_coordinator_from_context,
    streamlined_processing_resource,
)
from .managers import (
    APIKeyManager,
    CheckpointManager,
    ClientManager,
    DatabaseManager,
    RunTrackingManager,
    StorageManager,
    setup_streamlined_database,
)

__all__ = [
    "DatabaseManager",
    "APIKeyManager",
    "CheckpointManager",
    "RunTrackingManager",
    "StorageManager",
    "ClientManager",
    "ResourceCoordinator",
    "StreamlinedConfig",
    "setup_streamlined_database",
    # Dagster integration
    "StreamlinedProcessingResource",
    "create_streamlined_dagster_resource",
    "get_streamlined_coordinator_from_context",
    "streamlined_processing_resource",
]
