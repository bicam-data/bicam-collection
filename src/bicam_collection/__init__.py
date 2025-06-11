"""
BICAM Collection - Bulk Ingestion of Congressional Actions & Materials

A modern ELT pipeline for processing congressional and government data
from Congress.gov and GovInfo.gov APIs.
"""

__version__ = "0.2.0"
__author__ = "Ryan Delano"
__description__ = "ELT pipeline for congressional data processing"

# Core exports
from .core.config import Settings
from .core.exceptions import BicamError

__all__ = [
    "Settings",
    "BicamError",
    "__version__",
] 