"""Unit tests for configuration management."""

import os
import pytest
from unittest.mock import patch, mock_open
from bicam_collection.core.config import (
    DatabaseConfig,
    APIConfig, 
    ProcessingConfig,
    LoggingConfig,
    Settings
)
from bicam_collection.core.exceptions import ConfigurationError


class TestDatabaseConfig:
    """Test database configuration."""
    
    def test_database_config_creation(self):
        """Test creating database config with environment variables."""
        with patch.dict(os.environ, {
            'POSTGRESQL_HOST': 'localhost',
            'POSTGRESQL_PORT': '5432',
            'POSTGRESQL_DATABASE': 'testdb',
            'POSTGRESQL_USER': 'testuser',
            'POSTGRESQL_PASSWORD': 'testpass'
        }):
            config = DatabaseConfig()
            assert config.host == 'localhost'
            assert config.port == 5432
            assert config.database == 'testdb'
            assert config.user == 'testuser'
            assert config.password == 'testpass'
    
    def test_database_config_dsn(self):
        """Test DSN generation."""
        config = DatabaseConfig(
            host='localhost',
            port=5432,
            database='testdb',
            user='testuser', 
            password='testpass'
        )
        expected_dsn = 'postgresql://testuser:testpass@localhost:5432/testdb'
        assert config.dsn == expected_dsn
    
    def test_legacy_env_fallback(self):
        """Test fallback to legacy environment variables."""
        with patch.dict(os.environ, {
            'POSTGRESQL_HOST': 'localhost',
            'POSTGRESQL_PORT': '5432',
            'POSTGRESQL_DB': 'legacydb',  # Legacy name
            'POSTGRESQL_USERNAME': 'legacyuser',  # Legacy name
            'POSTGRESQL_PASSWORD': 'testpass'
        }):
            config = DatabaseConfig()
            assert config.database == 'legacydb'
            assert config.user == 'legacyuser'


class TestAPIConfig:
    """Test API configuration."""
    
    def test_api_config_single_key(self):
        """Test API config with single Congress API key."""
        with patch.dict(os.environ, {
            'CONGRESS_API_KEY': 'single_key',
            'GOVINFO_API_KEY': 'govinfo_key'
        }):
            config = APIConfig()
            assert config.congress_api_keys == ['single_key']
            assert config.govinfo_api_key == 'govinfo_key'
    
    def test_api_config_multiple_keys(self):
        """Test API config with multiple Congress API keys."""
        with patch.dict(os.environ, {
            'CONGRESS_API_KEYS': 'key1,key2,key3',
            'GOVINFO_API_KEY': 'govinfo_key'
        }):
            config = APIConfig()
            assert config.congress_api_keys == ['key1', 'key2', 'key3']
    
    def test_api_config_defaults(self):
        """Test API config default values."""
        with patch.dict(os.environ, {
            'GOVINFO_API_KEY': 'govinfo_key'
        }):
            config = APIConfig()
            assert config.rate_limit == 22
            assert config.max_retries == 3
            assert config.timeout == 30


class TestProcessingConfig:
    """Test processing configuration."""
    
    def test_processing_config_defaults(self):
        """Test processing config default values."""
        config = ProcessingConfig()
        assert config.batch_size == 100
        assert config.max_workers == 10
        assert config.chunk_size == 1000
        assert config.enable_parallel is True
    
    def test_processing_config_custom(self):
        """Test processing config with custom values."""
        with patch.dict(os.environ, {
            'BATCH_SIZE': '50',
            'MAX_WORKERS': '5',
            'CHUNK_SIZE': '500',
            'ENABLE_PARALLEL': 'false'
        }):
            config = ProcessingConfig()
            assert config.batch_size == 50
            assert config.max_workers == 5
            assert config.chunk_size == 500
            assert config.enable_parallel is False


class TestLoggingConfig:
    """Test logging configuration."""
    
    def test_logging_config_defaults(self):
        """Test logging config default values."""
        config = LoggingConfig()
        assert config.level == 'INFO'
        assert config.format == 'json'
        assert config.file_path is None


class TestSettings:
    """Test main settings class."""
    
    def test_settings_initialization(self):
        """Test settings initialization."""
        with patch.dict(os.environ, {
            'POSTGRESQL_HOST': 'localhost',
            'POSTGRESQL_PORT': '5432',
            'POSTGRESQL_DATABASE': 'testdb',
            'POSTGRESQL_USER': 'testuser',
            'POSTGRESQL_PASSWORD': 'testpass',
            'GOVINFO_API_KEY': 'govinfo_key',
            'ENVIRONMENT': 'test'
        }):
            settings = Settings()
            assert settings.environment == 'test'
            assert settings.database.host == 'localhost'
            assert hasattr(settings, 'api')
            assert hasattr(settings, 'processing')
            assert hasattr(settings, 'logging')
    
    @patch('builtins.open', mock_open(read_data='CONGRESS_API_KEY=legacy_key'))
    @patch('os.path.exists')
    def test_legacy_env_file_loading(self, mock_exists):
        """Test loading legacy .env files."""
        mock_exists.return_value = True
        
        with patch.dict(os.environ, {
            'POSTGRESQL_HOST': 'localhost',
            'POSTGRESQL_PORT': '5432', 
            'POSTGRESQL_DATABASE': 'testdb',
            'POSTGRESQL_USER': 'testuser',
            'POSTGRESQL_PASSWORD': 'testpass',
            'GOVINFO_API_KEY': 'govinfo_key'
        }):
            settings = Settings()
            # Should attempt to load legacy files
            mock_exists.assert_called() 