"""Exception classes for BICAM Collection."""

from typing import Optional, Any, Dict


class BicamError(Exception):
    """Base exception for BICAM Collection."""
    
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.message = message
        self.details = details or {}
    
    def __str__(self) -> str:
        if self.details:
            return f"{self.message} | Details: {self.details}"
        return self.message


class ConfigurationError(BicamError):
    """Raised when there's a configuration issue."""
    pass


class ExtractionError(BicamError):
    """Raised when data extraction fails."""
    
    def __init__(self, message: str, source: Optional[str] = None, 
                 url: Optional[str] = None, **kwargs):
        super().__init__(message, **kwargs)
        self.source = source
        self.url = url


class TransformationError(BicamError):
    """Raised when data transformation fails."""
    
    def __init__(self, message: str, data_type: Optional[str] = None, 
                 record_id: Optional[str] = None, **kwargs):
        super().__init__(message, **kwargs)
        self.data_type = data_type
        self.record_id = record_id


class DatabaseError(BicamError):
    """Raised when database operations fail."""
    
    def __init__(self, message: str, query: Optional[str] = None, 
                 table: Optional[str] = None, **kwargs):
        super().__init__(message, **kwargs)
        self.query = query
        self.table = table


class APIError(BicamError):
    """Raised when API operations fail."""
    
    def __init__(self, message: str, status_code: Optional[int] = None,
                 endpoint: Optional[str] = None, **kwargs):
        super().__init__(message, **kwargs)
        self.status_code = status_code
        self.endpoint = endpoint


class ValidationError(BicamError):
    """Raised when data validation fails."""
    
    def __init__(self, message: str, field: Optional[str] = None,
                 value: Optional[Any] = None, **kwargs):
        super().__init__(message, **kwargs)
        self.field = field
        self.value = value


class RetryExhaustedError(BicamError):
    """Raised when retry attempts are exhausted."""
    
    def __init__(self, message: str, attempts: int, last_error: Optional[Exception] = None):
        super().__init__(message, {"attempts": attempts, "last_error": str(last_error) if last_error else None})
        self.attempts = attempts
        self.last_error = last_error 