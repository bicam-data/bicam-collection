# Phase 1 Dagster Integration Setup Guide

This guide walks you through setting up and running Phase 1 of the Dagster integration for bicam-collection.

## Phase 1 Overview

Phase 1 focuses on:
- ✅ Basic Dagster infrastructure setup  
- ✅ Core asset definitions (scraping, normalization, cleaning, export)
- ✅ Wrapper processors that use existing heavyweight components
- ✅ Simple job orchestration
- ✅ Basic scheduling and sensing capabilities
- ✅ Development tooling and validation

**What's NOT included in Phase 1:**
- Complex partitioning strategies
- Advanced failure recovery
- Dynamic configuration management
- Full production monitoring
- Sophisticated caching mechanisms

## Prerequisites

1. **Environment Setup**: Ensure your bicam-collection environment is properly configured:
   ```bash
   # Ensure your database is running and configured
   # Ensure API keys are set in .env file
   # Ensure all dependencies are installed
   ```

2. **Database Configuration**: Update your database credentials in the resources if needed:
   - Edit `src/bicam_collection/dagster_pipeline/resources.py`
   - Update the `DatabaseResource` default values or set via environment variables

3. **API Keys**: Update API keys in the resources:
   - Edit `src/bicam_collection/dagster_pipeline/resources.py`
   - Update `CongressionalScraperResource` and `GovInfoScraperResource` with your keys

## Getting Started

### Step 1: Validate the Pipeline

First, validate that all Dagster definitions are correct:

```bash
python dagster_dev.py --validate
```

This should show you:
- ✅ All assets discovered
- ✅ All jobs defined
- ✅ All schedules available
- ✅ All sensors configured

### Step 2: Start the Dagster UI

Launch the development UI to explore the pipeline:

```bash
python dagster_dev.py --ui
# or simply
python dagster_dev.py
```

The UI will be available at: http://localhost:3000

### Step 3: Explore the Asset Lineage

In the Dagster UI:
1. Go to the "Assets" tab
2. Click on "View asset lineage"
3. You should see the data flow: Raw → Normalized → Cleaned → Final → Exports

### Step 4: Test Individual Assets

Start by materializing a single asset:

```bash
# Test the health check first
python dagster_dev.py --job dev_test_job

# Then try a simple data asset
python dagster_dev.py --materialize raw_congressional_bills
```

### Step 5: Run a Complete Pipeline

Try running the bills pipeline end-to-end:

```bash
python dagster_dev.py --job bills_pipeline_job
```

## Phase 1 Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        DAGSTER LAYER                           │
├─────────────────────────────────────────────────────────────────┤
│  Assets          Jobs           Schedules       Sensors         │
│  ├─ raw_*        ├─ scraping    ├─ daily_*      ├─ api_change   │
│  ├─ normalized_* ├─ cleaning    ├─ weekly_*     ├─ file_trigger │
│  ├─ cleaned_*    ├─ export      └─ monthly_*    └─ failure_*    │
│  └─ final_*      └─ health                                      │
├─────────────────────────────────────────────────────────────────┤
│                    PROCESSOR WRAPPERS                           │
├─────────────────────────────────────────────────────────────────┤
│  CongressionalDataProcessor  │  CleaningCoordinator            │
│  └─ Wraps existing DataProcessor                                │
├─────────────────────────────────────────────────────────────────┤
│                   EXISTING HEAVYWEIGHT                          │
│                      COMPONENTS                                 │
├─────────────────────────────────────────────────────────────────┤
│  DataProcessor    │  CleaningCoordinator   │  RunManager        │
│  CheckpointManager│  Normalizer           │  DatabaseSetup      │
└─────────────────────────────────────────────────────────────────┘
```

## Available Jobs

| Job Name | Description | Usage |
|----------|-------------|-------|
| `dev_test_job` | Simple health check for testing | `python dagster_dev.py --job dev_test_job` |
| `bills_pipeline_job` | Complete bills processing pipeline | `python dagster_dev.py --job bills_pipeline_job` |
| `congressional_scraping_job` | Scrape all Congressional data | `python dagster_dev.py --job congressional_scraping_job` |
| `govinfo_scraping_job` | Scrape GovInfo data | `python dagster_dev.py --job govinfo_scraping_job` |
| `normalization_job` | Normalize raw data | `python dagster_dev.py --job normalization_job` |
| `cleaning_job` | Clean normalized data | `python dagster_dev.py --job cleaning_job` |
| `export_job` | Export final data | `python dagster_dev.py --job export_job` |
| `health_check_job` | System health monitoring | `python dagster_dev.py --job health_check_job` |
| `cleanup_job` | System maintenance | `python dagster_dev.py --job cleanup_job` |
| `full_pipeline_job` | Complete end-to-end pipeline | `python dagster_dev.py --job full_pipeline_job` |

## Available Assets

| Asset Name | Description | Dependencies |
|------------|-------------|--------------|
| `raw_congressional_bills` | Raw bills data from Congress API | None |
| `raw_congressional_members` | Raw members data | None |
| `raw_congressional_bulk_data` | Bulk data (committees, nominations, amendments) | None |
| `raw_govinfo_bill_collections` | Raw GovInfo data | None |
| `normalized_congressional_bills` | Normalized bills | `raw_congressional_bills` |
| `normalized_bills_actions` | Normalized bill actions | `raw_congressional_bills` |
| `cleaned_congressional_bills` | Cleaned bills data | `normalized_congressional_bills` |
| `cleaned_congressional_members` | Cleaned members data | `raw_congressional_members` |
| `final_bicam_dataset` | Final merged dataset | Cleaned assets + GovInfo |
| `csv_export` | CSV export files | `final_bicam_dataset` |
| `parquet_export` | Parquet export files | `final_bicam_dataset` |

## Development Workflow

### Testing Individual Components

1. **Test Health Checks:**
   ```bash
   python dagster_dev.py --job health_check_job
   ```

2. **Test Single Asset:**
   ```bash
   python dagster_dev.py --materialize raw_congressional_bills
   ```

3. **Test Asset Dependencies:**
   ```bash
   # This will also materialize dependencies
   python dagster_dev.py --materialize cleaned_congressional_bills
   ```

### Using File Triggers (for testing)

You can test the sensor system using simple file triggers:

```bash
# Trigger the bills pipeline
touch /tmp/dagster_trigger_bills.flag

# Trigger an export
touch /tmp/request_export.flag
```

### Monitoring Progress

1. **Via Dagster UI:** http://localhost:3000
   - View run history
   - Monitor asset materializations
   - Check job status

2. **Via Logs:** All Dagster runs include detailed logging

3. **Via Database:** Existing checkpoint and run management systems still work

## Configuration

### Database Configuration

Update `src/bicam_collection/dagster_pipeline/resources.py`:

```python
"database": DatabaseResource(
    host="your_db_host",
    port=5432,
    database="your_db_name",
    username="your_username",
    password="your_password",
    pool_size=20,
    max_overflow=30,
),
```

### API Configuration

Update API keys in the same file:

```python
"congressional_scraper": CongressionalScraperResource(
    api_key="your_actual_congressional_api_key",
    # ... other config
),
"govinfo_scraper": GovInfoScraperResource(
    api_key="your_actual_govinfo_api_key",
    # ... other config
),
```

## Troubleshooting

### Common Issues

1. **Import Errors:**
   ```bash
   # Make sure you're running from the project root
   pwd  # Should end with bicam-collection
   python dagster_dev.py --validate
   ```

2. **Database Connection Errors:**
   - Check your .env file has correct database credentials
   - Ensure the database is running
   - Test connection with existing CLI: `python -m bicam_collection.cli validate-database`

3. **API Key Issues:**
   - Check .env file has API keys set
   - Update resource configurations with correct keys
   - Test with existing scrapers first

4. **Asset Materialization Failures:**
   - Check Dagster UI for detailed error logs
   - Verify underlying processors work with existing CLI
   - Start with simple assets like health checks

### Debug Mode

For detailed debugging, you can run individual assets in debug mode:

```python
# In a Python shell or script
from bicam_collection.dagster_pipeline.definitions import defs
from dagster import materialize

# Materialize with detailed logging
result = materialize(
    [raw_congressional_bills],
    resources=defs.get_resource_defs(),
)
```

## Next Steps After Phase 1

Once Phase 1 is working:

1. **Phase 2: Enhanced Processing**
   - Add sophisticated partitioning
   - Implement dynamic configuration
   - Add advanced error handling

2. **Phase 3: Production Features**
   - Full monitoring and alerting
   - Production-ready resource management
   - Advanced caching and optimization

3. **Phase 4: Migration Completion**
   - Deprecate old CLI commands
   - Full Dagster-native implementation
   - Performance optimization

## Support

If you encounter issues:

1. Check the Dagster UI for detailed error messages
2. Verify the underlying processors work with the existing CLI
3. Review logs in the `.dagster_home/storage` directory
4. Test components individually before running full pipelines

The Phase 1 integration preserves all existing functionality while adding Dagster orchestration on top. If something doesn't work in Dagster, it should still work via the existing CLI system. 

# Bicam Collection - Dagster Phase 2 Integration Complete

## Phase 2: Enhanced Resource Migration - Status: ✅ COMPLETED

This document describes the completed Phase 2 integration of Dagster into the bicam-collection project. Phase 2 focused on migrating the existing heavyweight orchestrator components to enhanced Dagster resources while preserving all existing functionality.

## Architecture Overview

The Phase 2 architecture follows the Dagster documentation patterns for stateful resource management:

```
┌─────────────────────────────────────────────────────────────┐
│                    Dagster Phase 2 Pipeline                │
├─────────────────┬─────────────────┬─────────────────────────┤
│   Assets        │    Jobs         │    Schedules/Sensors    │
│ ┌─────────────┐ │ ┌─────────────┐ │ ┌─────────────────────┐ │
│ │Raw Data     │ │ │Scraping     │ │ │Daily Schedules      │ │
│ │Normalized   │ │ │Normalization│ │ │Failure Recovery     │ │
│ │Cleaned      │ │ │Cleaning     │ │ │API Status Monitor   │ │
│ │Exported     │ │ │Export       │ │ │Smart Export Trigger │ │
│ └─────────────┘ │ └─────────────┘ │ └─────────────────────┘ │
└─────────────────┴─────────────────┴─────────────────────────┘
├─────────────────────────────────────────────────────────────┤
│               Enhanced Resource Layer (Phase 2)            │
│ ┌─────────────────────────────────────────────────────────┐ │
│ │ ConfigurableResource + Stateful Client Pattern         │ │
│ │ • Database: Connection pooling + heavyweight DB objects │ │
│ │ • API: Session management + heavyweight key managers   │ │
│ │ • Checkpoint: Run tracking + heavyweight checkpoints   │ │
│ │ • Processors: Lightweight + heavyweight integrations   │ │
│ └─────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
├─────────────────────────────────────────────────────────────┤
│                  Heavyweight Components                    │
│ ┌─────────────────────────────────────────────────────────┐ │
│ │ Original Heavyweight Components (Preserved)             │ │
│ │ • CongressionalDataProcessor                            │ │
│ │ • GovInfoDataProcessor                                  │ │
│ │ • CheckpointManager                                     │ │
│ │ • RunManager                                            │ │
│ │ • APIKeyManager                                         │ │
│ │ • CleaningCoordinator                                   │ │
│ └─────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

## Phase 2 Enhancements

### 1. Enhanced Database Resources ✅
- **EnhancedDatabaseResource**: ConfigurableResource with environment variable fallbacks
- **DatabaseClient**: Stateful client managing connection pools and heavyweight DB objects
- **Integration**: 
  - Direct asyncpg connection pools
  - CongressionalDatabaseConnection wrapper
  - GovInfoDatabaseConnection wrapper
  - Connection reuse and proper cleanup

### 2. Enhanced API Client Resources ✅
- **EnhancedAPIClientResource**: ConfigurableResource with session configuration
- **APIClient**: Stateful client managing HTTP sessions and API key management
- **Integration**:
  - aiohttp session pooling with DNS caching
  - APIKeyManager integration for both Congressional and GovInfo APIs
  - Environment variable-based API key loading
  - Rate limiting and retry logic preservation

### 3. Enhanced Checkpoint Resources ✅
- **EnhancedCheckpointResource**: ConfigurableResource with checkpoint configuration
- **CheckpointClient**: Stateful client managing checkpoint and run managers
- **Integration**:
  - CheckpointManager with SQLite/PostgreSQL support
  - RunManager with full run tracking capabilities
  - Hierarchical progress tracking preservation
  - Error recovery and metrics collection

### 4. Enhanced Processor Resources ✅
- **EnhancedCongressionalResource**: ConfigurableResource for Congressional processing
- **EnhancedGovInfoResource**: ConfigurableResource for GovInfo processing
- **ProcessorClient**: Stateful client managing lightweight and heavyweight processors
- **Integration**:
  - Lightweight Dagster-compatible processors
  - Heavyweight DataProcessor preservation
  - Resource dependency injection
  - Configuration and schema management

### 5. Enhanced Cleaning Resources ✅
- **EnhancedCleaningResource**: ConfigurableResource for data cleaning
- **Integration**:
  - CleaningCoordinator wrapper
  - Batch processing and validation preservation
  - Progress tracking capabilities

## Resource Client Pattern

Following Dagster best practices, Phase 2 implements the **ConfigurableResource + Stateful Client** pattern:

```python
class EnhancedDatabaseResource(ConfigurableResource):
    """Frozen ConfigurableResource with configuration only"""
    host: str = "localhost"
    port: int = 5432
    # ... other config fields
    
    def get_client(self) -> DatabaseClient:
        """Returns stateful client for resource management"""
        return DatabaseClient(self._get_connection_config(), self.env_path)

class DatabaseClient:
    """Stateful client that can be modified and manages connections"""
    def __init__(self, config: Dict[str, Any], env_path: str):
        # Stateful attributes initialized here
        self._pool: Optional[asyncpg.Pool] = None
        self._congressional_db: Optional[CongressionalDatabaseConnection] = None
```

## Available Resources

| Resource | Purpose | Client | Heavyweight Integration |
|----------|---------|--------|-------------------------|
| `EnhancedDatabaseResource` | Database connections | `DatabaseClient` | CongressionalDatabaseConnection, GovInfoDatabaseConnection |
| `EnhancedAPIClientResource` | HTTP sessions & API keys | `APIClient` | APIKeyManager |
| `EnhancedCheckpointResource` | Checkpoints & run tracking | `CheckpointClient` | CheckpointManager, RunManager |
| `EnhancedCongressionalResource` | Congressional processing | `ProcessorClient` | CongressionalDataProcessor |
| `EnhancedGovInfoResource` | GovInfo processing | `ProcessorClient` | GovInfoDataProcessor |
| `EnhancedCleaningResource` | Data cleaning | Direct method | CleaningCoordinator |

## Asset Integration

Assets now use the client pattern for resource access:

```python
@asset
async def raw_congressional_bills(
    context: AssetExecutionContext,
    congressional_scraper: EnhancedCongressionalResource,
    database: EnhancedDatabaseResource,
    api_client: EnhancedAPIClientResource,
    checkpoint_manager: EnhancedCheckpointResource,
) -> MaterializeResult:
    # Get stateful clients
    checkpoint_client = checkpoint_manager.get_client()
    run_manager = await checkpoint_client.get_run_manager()
    
    processor_client = congressional_scraper.get_client()
    processor = await processor_client.get_lightweight_processor(
        database_resource=database,
        api_resource=api_client,
        checkpoint_resource=checkpoint_manager,
    )
    
    # Use heavyweight functionality through lightweight interface
    await processor.process_data_type("bills")
```

## Pipeline Status

### ✅ Core Components
- **Assets**: 11 assets with heavyweight integration
- **Jobs**: 10 jobs (7 asset-based, 3 op-based)
- **Schedules**: 9 time-based schedules
- **Sensors**: 6 event-driven sensors
- **Resources**: 6 enhanced resources with client pattern

### ✅ Validation Status
```bash
python dagster_dev.py --validate
✅ Found 11 assets
✅ Found 10 jobs  
✅ Found 9 schedules
✅ Found 6 sensors
✅ Pipeline validation completed successfully!
```

### ✅ Available Operations
```bash
# Validate pipeline definitions
python dagster_dev.py --validate

# Start Dagster UI (http://localhost:3000)
python dagster_dev.py --ui

# Materialize specific assets
python dagster_dev.py --materialize raw_congressional_bills

# Run specific jobs
python dagster_dev.py --job congressional_scraping_job
```

## Preserved Functionality

All existing heavyweight functionality has been preserved:

### Database Operations
- ✅ Connection pooling and configuration
- ✅ Environment variable fallbacks
- ✅ Error logging and metadata management
- ✅ Last processed date tracking
- ✅ PostgreSQL and SQLite support

### API Management
- ✅ Rate limiting and retry logic
- ✅ API key rotation and management
- ✅ Session pooling and DNS caching
- ✅ Authentication and authorization
- ✅ Multi-API support (Congressional, GovInfo)

### Checkpoint System
- ✅ Hierarchical progress tracking
- ✅ Run management and tracking
- ✅ Error recovery and resumption
- ✅ Metrics collection and reporting
- ✅ SQLite and PostgreSQL backends

### Data Processing
- ✅ Heavyweight DataProcessor functionality
- ✅ Batch processing and parallel execution
- ✅ Data validation and cleaning
- ✅ Export capabilities (CSV, Parquet)
- ✅ Configuration and schema management

## Environment Configuration

Phase 2 maintains all existing environment variable support:

```bash
# Database configuration
POSTGRESQL_HOST=localhost
POSTGRESQL_PORT=5432
POSTGRESQL_DATABASE=bicam_collection
POSTGRESQL_USERNAME=bicam_user
POSTGRESQL_PASSWORD=bicam_password

# API configuration
CONGRESS_API_KEYS=key1,key2,key3
GOVINFO_API_KEYS=key1,key2,key3

# Processing configuration
PROCESSING_BATCH_SIZE=100
ENVIRONMENT=development
DEBUG=true
```

## Next Steps: Phase 3

Phase 2 is now complete and ready for Phase 3: Asset Creation (Week 3-4), which will focus on:

1. **Enhanced Asset Dependencies**: Complex dependency chains
2. **Asset Partitioning**: Time-based and custom partitioning
3. **Asset Checks**: Data quality validation
4. **Asset Materialization Policies**: Smart refresh strategies
5. **Asset Groups**: Logical organization and selective execution

## Troubleshooting

### Common Issues

1. **Resource Initialization**: Ensure all environment variables are properly set
2. **Database Connections**: Check PostgreSQL connectivity and credentials
3. **API Keys**: Verify API keys are valid and properly formatted
4. **Dependencies**: Ensure all required packages are installed

### Getting Help

```bash
# Check pipeline status
python dagster_dev.py --validate

# View Dagster UI for detailed logs
python dagster_dev.py --ui

# Check environment variables
env | grep -E "(POSTGRESQL|API_KEY|PROCESSING)"
```

The Phase 2 integration successfully preserves all existing heavyweight functionality while providing Dagster orchestration capabilities and setting the foundation for advanced Phase 3 features. 