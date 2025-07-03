# Bicam Collection CLI and Configuration System

A modern, modular command-line interface and configuration system for the bicam-collection data pipeline.

## Overview

The Bicam Collection CLI provides a unified interface for managing all aspects of the congressional and government data pipeline, including:

- ✅ **Database Setup & Management** - Automated schema creation and validation
- ✅ **Configuration Management** - Pydantic-based validation and environment integration  
- ✅ **Data Extraction** - Congressional and GovInfo API scraping
- ✅ **Data Processing** - Cleaning, merging, and transformation
- ✅ **Data Export** - Multiple format support with S3 integration
- ✅ **Lobbyist Matching** - Advanced text matching capabilities
- ✅ **Pipeline Orchestration** - End-to-end automated execution

## Quick Start

### 1. Setup Environment

```bash
# Install dependencies
pip install -r requirements.txt

# Create environment file
cp .env.example .env
# Edit .env with your database credentials and API keys
```

### 2. Create Configuration

```bash
# Generate sample configuration
python bicam_cli.py config create-sample

# Edit the generated config.yaml file
# Or use environment variables (see Configuration section)
```

### 3. Setup Database

```bash
# Setup database schemas and tables
python bicam_cli.py database setup

# Validate database setup
python bicam_cli.py database validate
```

### 4. Run Pipeline

```bash
# Run complete pipeline
python bicam_cli.py pipeline

# Or run individual components
python bicam_cli.py scrape congressional
python bicam_cli.py clean
python bicam_cli.py export
```

## Configuration

The system supports both YAML configuration files and environment variables with full Pydantic validation.

### Environment Variables

Set these in your `.env` file:

```bash
# Database
POSTGRESQL_HOST=localhost
POSTGRESQL_PORT=5432
POSTGRESQL_DATABASE=bicam_collection
POSTGRESQL_USERNAME=postgres
POSTGRESQL_PASSWORD=your_password

# API Keys
CONGRESSIONAL_API_KEY=your_api_key
GOVINFO_API_KEY=your_api_key

# Environment
ENVIRONMENT=development
DEBUG=false
```

### Configuration File

Create a `config.yaml` file (see `sample_config.yaml` for full example):

```yaml
environment: development
debug: false

database:
  host: localhost
  port: 5432
  database: bicam_collection
  username: postgres
  password: your_password

scraping:
  congressional_api_key: YOUR_API_KEY
  govinfo_api_key: YOUR_API_KEY
  output_directory: /data/bicam-raw

processing:
  chunk_size: 1000
  max_workers: 4
  enable_parallel_processing: true

export:
  export_directory: /data/bicam-exports
  export_formats: [csv, parquet]
  compress_exports: true
```

## CLI Commands

### Configuration Management

```bash
# Create sample configuration file
python bicam_cli.py config create-sample

# Validate configuration
python bicam_cli.py config validate
```

### Database Management

```bash
# Setup database (create schemas, tables)
python bicam_cli.py database setup

# Setup with recreation (drops existing)
python bicam_cli.py database setup --recreate

# Validate database setup
python bicam_cli.py database validate

# Show database information
python bicam_cli.py database info
```

### Data Extraction/Scraping

```bash
# Scrape congressional data
python bicam_cli.py scrape congressional

# Scrape govinfo data
python bicam_cli.py scrape govinfo

# Scrape specific data types
python bicam_cli.py scrape congressional --data-types bills amendments

# Scrape with custom config
python bicam_cli.py --config custom_config.yaml scrape congressional
```

### Data Processing

```bash
# Clean all data
python bicam_cli.py clean

# Clean specific data types
python bicam_cli.py clean --data-types bills amendments

# Merge cleaned data
python bicam_cli.py merge
```

### Data Export

```bash
# Export with default formats
python bicam_cli.py export

# Export specific format
python bicam_cli.py export --format csv
python bicam_cli.py export --format parquet
```

### Lobbyist Matching

```bash
# Run lobbyist matching
python bicam_cli.py lobbyist-matching
```

### Pipeline Operations

```bash
# Run complete pipeline
python bicam_cli.py pipeline

# Skip database setup
python bicam_cli.py pipeline --skip-setup
```

### System Status

```bash
# Show system status
python bicam_cli.py status

# Verbose output
python bicam_cli.py --verbose status
```

## Configuration Validation

The system uses Pydantic for comprehensive configuration validation:

### Database Configuration
- ✅ Connection string validation
- ✅ Pool size limits
- ✅ Timeout validation

### Path Validation
- ✅ Directory creation/access checks
- ✅ File existence validation
- ✅ Permission validation

### API Configuration
- ✅ Rate limit validation
- ✅ Timeout bounds checking
- ✅ Retry attempt limits

### Processing Configuration
- ✅ Memory limit validation
- ✅ Worker count optimization
- ✅ Chunk size optimization

## Architecture

### Configuration System (`config.py`)

Pydantic-based configuration with:
- Environment variable integration
- YAML file support
- Comprehensive validation
- Type safety
- Documentation generation

### Database Setup (`database_setup.py`)

Async database management with:
- Automated schema creation
- Connection pooling
- Validation and health checks
- SQL file execution
- Error handling and rollback

### CLI Interface (`bicam_cli.py`)

Modular CLI with:
- Structured logging
- Progress tracking
- Error handling
- Subprocess management
- Pipeline orchestration

## Advanced Usage

### Custom Configuration

```python
from config import BicamConfig, DatabaseConfig

# Programmatic configuration
config = BicamConfig(
    database=DatabaseConfig(
        host="localhost",
        database="custom_db",
        username="user",
        password="pass"
    ),
    environment="production"
)

# Validate configuration
config.validate_all_paths()
```

### Direct Database Management

```python
from database_setup import DatabaseManager
from config import load_config

config = load_config()
db_manager = DatabaseManager(config)

# Setup database programmatically
await db_manager.setup_schemas()
validation_results = await db_manager.validate_database_setup()
```

### Pipeline Integration

```python
from bicam_cli import BicamCLI

cli = BicamCLI("custom_config.yaml")

# Run individual components
await cli.setup_database()
cli.run_scraper("congressional")
cli.run_cleaning()
cli.run_export()
```

## Error Handling

The system provides comprehensive error handling:

- **Configuration Errors**: Invalid settings, missing files, permission issues
- **Database Errors**: Connection failures, schema issues, validation failures  
- **Processing Errors**: Memory limits, worker failures, data corruption
- **Network Errors**: API timeouts, rate limiting, connection issues

All errors are logged with structured logging for debugging.

## Performance Optimization

### Parallel Processing
- Configurable worker processes
- Memory limit enforcement
- Chunk size optimization

### Database Optimization  
- Connection pooling
- Async operations
- Batch processing
- Index optimization

### Network Optimization
- Rate limiting
- Request retries
- Connection reuse
- Timeout management

## Monitoring and Logging

### Structured Logging
- JSON-formatted logs
- Contextual information
- Error tracking
- Performance metrics

### Progress Tracking
- Real-time progress bars
- Status reporting
- Time estimation
- Throughput metrics

### Health Checks
- Database connectivity
- API availability
- Resource utilization
- Data validation

## Development

### Adding New Commands

1. Add command parser in `create_parser()`
2. Implement handler method in `BicamCLI`
3. Add command logic in `main()`
4. Update documentation

### Adding Configuration Options

1. Add fields to appropriate config class in `config.py`
2. Add validation if needed
3. Update sample configuration
4. Update documentation

### Testing

```bash
# Run with test configuration
python bicam_cli.py --config test_config.yaml status

# Validate configuration
python bicam_cli.py config validate

# Test database connection
python bicam_cli.py database validate
```

## Troubleshooting

### Common Issues

**Configuration not found:**
```bash
# Create sample config
python bicam_cli.py config create-sample
```

**Database connection failed:**
```bash
# Check database status
python bicam_cli.py database info

# Validate setup
python bicam_cli.py database validate
```

**API rate limiting:**
```bash
# Check API configuration
python bicam_cli.py --verbose status
```

### Debug Mode

```bash
# Enable verbose logging
python bicam_cli.py --verbose <command>

# Enable debug mode via config
DEBUG=true python bicam_cli.py <command>
```

### Support

For issues or questions:
1. Check configuration validation
2. Review logs for error details
3. Verify database connectivity
4. Test with minimal configuration

## Examples

See the `examples/` directory for:
- Sample configurations
- Common workflows
- Integration patterns
- Performance tuning

---

## License

MIT License - see LICENSE file for details. 