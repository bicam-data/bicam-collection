"""Unit tests for exception classes."""

import pytest
from bicam_collection.core.exceptions import (
    BicamError,
    ConfigurationError,
    ExtractionError,
    TransformationError,
    DatabaseError,
    APIError,
    ValidationError,
    RetryExhaustedError
)


class TestBicamError:
    """Test base exception class."""
    
    def test_basic_error(self):
        """Test basic error creation."""
        error = BicamError("Test error message")
        assert str(error) == "Test error message"
        assert error.message == "Test error message"
        assert error.details == {}
    
    def test_error_with_details(self):
        """Test error with details."""
        details = {"field": "value", "code": 123}
        error = BicamError("Test error", details=details)
        assert error.details == details
        assert "Details:" in str(error)
        assert "field" in str(error)


class TestConfigurationError:
    """Test configuration error."""
    
    def test_configuration_error(self):
        """Test configuration error creation."""
        error = ConfigurationError("Invalid configuration")
        assert isinstance(error, BicamError)
        assert str(error) == "Invalid configuration"


class TestExtractionError:
    """Test extraction error."""
    
    def test_extraction_error_basic(self):
        """Test basic extraction error."""
        error = ExtractionError("Extraction failed")
        assert isinstance(error, BicamError)
        assert error.source is None
        assert error.url is None
    
    def test_extraction_error_with_context(self):
        """Test extraction error with source and URL."""
        error = ExtractionError(
            "API request failed",
            source="congress_api",
            url="https://api.congress.gov/v3/bills"
        )
        assert error.source == "congress_api"
        assert error.url == "https://api.congress.gov/v3/bills"


class TestTransformationError:
    """Test transformation error."""
    
    def test_transformation_error_basic(self):
        """Test basic transformation error."""
        error = TransformationError("Transform failed")
        assert isinstance(error, BicamError)
        assert error.data_type is None
        assert error.record_id is None
    
    def test_transformation_error_with_context(self):
        """Test transformation error with context."""
        error = TransformationError(
            "Invalid data format",
            data_type="bills",
            record_id="hr1234-118"
        )
        assert error.data_type == "bills"
        assert error.record_id == "hr1234-118"


class TestDatabaseError:
    """Test database error."""
    
    def test_database_error_basic(self):
        """Test basic database error."""
        error = DatabaseError("Database connection failed")
        assert isinstance(error, BicamError)
        assert error.query is None
        assert error.table is None
    
    def test_database_error_with_context(self):
        """Test database error with query and table."""
        error = DatabaseError(
            "Insert failed",
            query="INSERT INTO bills VALUES (...)",
            table="bills"
        )
        assert error.query == "INSERT INTO bills VALUES (...)"
        assert error.table == "bills"


class TestAPIError:
    """Test API error."""
    
    def test_api_error_basic(self):
        """Test basic API error."""
        error = APIError("API request failed")
        assert isinstance(error, BicamError)
        assert error.status_code is None
        assert error.endpoint is None
    
    def test_api_error_with_context(self):
        """Test API error with status code and endpoint."""
        error = APIError(
            "Rate limit exceeded",
            status_code=429,
            endpoint="/v3/bills"
        )
        assert error.status_code == 429
        assert error.endpoint == "/v3/bills"


class TestValidationError:
    """Test validation error."""
    
    def test_validation_error_basic(self):
        """Test basic validation error."""
        error = ValidationError("Validation failed")
        assert isinstance(error, BicamError)
        assert error.field is None
        assert error.value is None
    
    def test_validation_error_with_context(self):
        """Test validation error with field and value."""
        error = ValidationError(
            "Invalid bill number",
            field="bill_number",
            value="invalid"
        )
        assert error.field == "bill_number"
        assert error.value == "invalid"


class TestRetryExhaustedError:
    """Test retry exhausted error."""
    
    def test_retry_exhausted_error_basic(self):
        """Test basic retry exhausted error."""
        error = RetryExhaustedError("Max retries reached", attempts=3)
        assert isinstance(error, BicamError)
        assert error.attempts == 3
        assert error.last_error is None
        assert "attempts" in error.details
    
    def test_retry_exhausted_error_with_last_error(self):
        """Test retry exhausted error with last error."""
        last_error = ValueError("Original error")
        error = RetryExhaustedError(
            "Max retries reached",
            attempts=5,
            last_error=last_error
        )
        assert error.attempts == 5
        assert error.last_error == last_error
        assert error.details["last_error"] == "Original error" 