"""
Lightweight API clients for Congressional and GovInfo data.

This module provides simplified API clients that return dictionaries
instead of complex objects, reducing dependencies and making data
handling more straightforward.
"""

from .congressional_api import (
    CongressionalAPIClient,
    CongressionalAPIError,
)
from .govinfo_api import (
    GovInfoAPIClient,
    GovInfoAPIError,
)

__all__ = [
    # Congressional API Client
    "CongressionalAPIClient",
    "CongressionalAPIError",
    # GovInfo API Client
    "GovInfoAPIClient",
    "GovInfoAPIError",
]
