"""
Resource Management for Streamlined Pipeline

This module provides specialized resource managers that replace the God Object pattern
with focused, single-responsibility components that can be composed together.

Key improvements:
- DatabaseManager: Focused database connection pooling
- APIKeyManager: Focused API key distribution and management
- CheckpointManager: Focused checkpoint operations
- RunTrackingManager: Focused run tracking operations
- StorageManager: Focused storage operations
- ClientManager: Focused API client lifecycle
- ResourceCoordinator: Lightweight composition of all managers
"""

from .config import StreamlinedConfig
from .coordinator import ResourceCoordinator
from .managers import (
    APIKeyManager,
    CheckpointManager,
    ClientManager,
    DatabaseManager,
    RunTrackingManager,
    StorageManager,
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
]
