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
- ProcessingResource: Dagster integration for ResourceCoordinator
"""

from .config import Config
from .coordinator import ResourceCoordinator
from .dagster_integration import (
    ProcessingResource,
    create_dagster_resource,
    get_coordinator_from_context,
    processing_resource,
)
from .managers import (
    APIKeyManager,
    CheckpointManager,
    ClientManager,
    DatabaseManager,
    RunTrackingManager,
    StorageManager,
    setup_database,
)

__all__ = [
    "DatabaseManager",
    "APIKeyManager",
    "CheckpointManager",
    "RunTrackingManager",
    "StorageManager",
    "ClientManager",
    "ResourceCoordinator",
    "Config",
    "setup_database",
    # Dagster integration
    "ProcessingResource",
    "create_dagster_resource",
    "get_coordinator_from_context",
    "processing_resource",
]
