# Streamlined Architecture for Bicam Collection

A simplified, efficient 4-layer architecture that reduces complexity by 50% while preserving all existing custom logic through a plugin system.

## 🎯 Overview

The streamlined architecture transforms the original 8-layer complex pipeline into 4 focused layers:

### Before (8 Complex Layers)
```
CLI → Pipeline Runner → Generalized Assets → ProcessingResource →
BaseFetcher → CongressionalBaseFetcher → SpecificFetcher → OptimizedParallelProcessor
```

### After (4 Focused Layers)
```
Enhanced CLI → Direct Asset Executor → Streamlined Components → Integrated Processing
```

## 🚀 Key Benefits

- **50% reduction** in execution path complexity
- **Direct execution** without unnecessary indirection
- **Centralized error handling** improves reliability
- **Focused components** follow SOLID principles
- **Plugin system** preserves all existing custom logic
- **Better performance** through reduced overhead

## 📦 Architecture Components

### 1. StreamlinedExecutor
Direct execution coordinator that orchestrates the pipeline.

```python
from bicam_collection.streamlined import StreamlinedExecutor, ResourceCoordinator

coordinator = ResourceCoordinator()
executor = StreamlinedExecutor(coordinator)
results = await executor.execute_data_type("bills", ["raw", "staging", "production"])
```

### 2. StreamlinedFetcher
Integrated fetching with built-in parallel processing.

```python
from bicam_collection.streamlined import StreamlinedFetcher

fetcher = StreamlinedFetcher(coordinator)
results = await fetcher.fetch_data_type("bills", from_date="2024-01-01", limit=100)
```

### 3. StreamlinedCleaner
Focused data cleaning with integrated validation.

```python
from bicam_collection.streamlined import StreamlinedCleaner

cleaner = StreamlinedCleaner(coordinator)
results = await cleaner.clean_data_type("bills", batch_size=50)
```

### 4. StreamlinedNormalizer
Focused database normalization with batch processing.

```python
from bicam_collection.streamlined import StreamlinedNormalizer

normalizer = StreamlinedNormalizer(coordinator)
results = await normalizer.normalize_data_type("bills", batch_size=100)
```

## 🔌 Plugin System

The plugin system automatically preserves all existing custom logic:

- **Fetcher plugins** wrap existing fetcher classes (BillsFetcher, etc.)
- **Cleaner plugins** wrap existing cleaner classes (BillsCleaner, etc.)  
- **Normalizer plugins** wrap existing normalizer classes (BillsDatabaseNormalizer, etc.)

All custom methods are preserved:
- `get_actions`, `get_cosponsors`, `get_texts`, etc. (fetchers)
- `_clean_bills_singular`, `_clean_bills_actions_singular`, etc. (cleaners)
- Custom normalization logic (normalizers)

### Plugin Auto-Registration

```python
from bicam_collection.streamlined import get_plugin_registry

registry = get_plugin_registry()
await registry.auto_register_plugins()  # Discovers and registers all existing implementations
```

## 💻 Usage Examples

### Direct Execution (Recommended)

```python
from bicam_collection.streamlined import execute_streamlined_pipeline, ResourceCoordinator

# Simple execution
coordinator = ResourceCoordinator()
results = await execute_streamlined_pipeline(
    coordinator=coordinator,
    data_type="bills",
    phases=["raw", "staging", "production"],
    from_date="2024-01-01",
    limit=100
)
```

### Individual Components

```python
from bicam_collection.streamlined import (
    StreamlinedFetcher, 
    StreamlinedCleaner, 
    StreamlinedNormalizer,
    ResourceCoordinator
)

coordinator = ResourceCoordinator()
fetcher = StreamlinedFetcher(coordinator)
cleaner = StreamlinedCleaner(coordinator)
normalizer = StreamlinedNormalizer(coordinator)

# Use components individually
fetch_results = await fetcher.fetch_data_type("bills", limit=10)
clean_results = await cleaner.clean_data_type("bills")
normalize_results = await normalizer.normalize_data_type("bills")
```

### Configuration

```python
from bicam_collection.streamlined import StreamlinedConfig, ResourceCoordinator

# Load from environment
config = StreamlinedConfig.from_environment()

# Create custom configuration
from bicam_collection.streamlined.resources.config import DatabaseConfig, APIConfig

custom_config = StreamlinedConfig(
    database=DatabaseConfig(host="localhost", port=5432, name="bicam_test"),
    api=APIConfig(base_url="https://api.congress.gov", timeout=30)
)

coordinator = ResourceCoordinator(custom_config)
```

## 🖥️ CLI Usage

The streamlined architecture includes a command-line interface:

### Basic Commands

```bash
# Process bills data
python -m bicam_collection.streamlined.cli process bills --phases raw staging production

# Process with date range and limit
python -m bicam_collection.streamlined.cli process nominations --phases raw --from-date 2024-01-01 --limit 100

# List supported data types
python -m bicam_collection.streamlined.cli list-types

# Test plugin integration
python -m bicam_collection.streamlined.cli test-plugins bills

# Check status
python -m bicam_collection.streamlined.cli status bills

# Show configuration
python -m bicam_collection.streamlined.cli config --validate
```

### Available Commands

- `process` - Process data for a specific type
- `list-types` - List supported data types
- `test-plugins` - Test plugin integration
- `status` - Get execution status
- `plugin-info` - Get plugin information
- `config` - Show configuration

## 📊 Performance Improvements

| Metric | Before | After | Improvement |
|--------|--------|--------|-------------|
| **Execution Layers** | 8 | 4 | **50% reduction** |
| **Error Propagation Hops** | 8 | 4 | **50% reduction** |
| **Resource Management** | 1 God Object | 7 Focused Managers | **SOLID compliance** |
| **Debugging Complexity** | High | Low | **Easier troubleshooting** |
| **Code Maintainability** | Complex | Simple | **Easier to extend** |

## 🏗️ Resource Management

The streamlined architecture uses focused resource managers instead of a God Object:

```python
from bicam_collection.streamlined.resources import (
    DatabaseManager,
    APIKeyManager,
    CheckpointManager,
    RunTrackingManager,
    StorageManager,
    ClientManager,
    ResourceCoordinator
)

# Resource coordinator composes all managers
coordinator = ResourceCoordinator()
await coordinator.initialize()

# Access individual managers
db_manager = coordinator.database_manager
api_key_manager = coordinator.api_key_manager
storage_manager = coordinator.storage_manager
```

## 🧪 Testing

### Plugin Testing

```python
from bicam_collection.streamlined import StreamlinedFetcher

fetcher = StreamlinedFetcher(coordinator)
test_results = await fetcher.test_plugin_integration("bills")
print(test_results)
```

### Configuration Testing

```python
from bicam_collection.streamlined import StreamlinedConfig

config = StreamlinedConfig.from_environment()
validation_errors = config.validate()
if validation_errors:
    print(f"Configuration errors: {validation_errors}")
```

## 📁 Directory Structure

```
streamlined/
├── __init__.py              # Main module exports
├── executor.py              # StreamlinedExecutor
├── fetcher.py               # StreamlinedFetcher
├── cleaner.py               # StreamlinedCleaner
├── normalizer.py            # StreamlinedNormalizer
├── cli.py                   # Command-line interface
├── resources/               # Resource management
│   ├── __init__.py
│   ├── config.py            # Configuration management
│   ├── coordinator.py       # Resource coordinator
│   └── managers.py          # Focused resource managers
├── plugins/                 # Plugin system
│   ├── __init__.py
│   ├── base.py              # Plugin protocols
│   ├── registry.py          # Plugin registry
│   └── congressional.py     # Congressional plugins
├── processing/              # Integrated processing
│   ├── __init__.py
│   ├── work_queue.py        # Adaptive work queue
│   └── key_pool.py          # Dynamic key pool
└── examples/                # Usage examples
    └── usage_example.py     # Comprehensive examples
```

## 🔄 Migration from Legacy

The streamlined architecture coexists with the legacy system:

1. **No breaking changes** - All existing code continues to work
2. **Gradual migration** - Migrate data types one by one
3. **Plugin preservation** - All custom logic is automatically preserved
4. **Performance benefits** - Immediate improvements for migrated data types

### Migration Steps

1. **Test with single data type**:
   ```bash
   python -m bicam_collection.streamlined.cli process bills --phases raw
   ```

2. **Compare performance**:
   ```python
   # Time both approaches and compare
   ```

3. **Migrate additional data types**:
   ```bash
   python -m bicam_collection.streamlined.cli process nominations --phases raw staging
   ```

## 🐛 Troubleshooting

### Common Issues

1. **Plugin not found**: Check if plugins are registered
   ```bash
   python -m bicam_collection.streamlined.cli test-plugins
   ```

2. **Configuration errors**: Validate configuration
   ```bash
   python -m bicam_collection.streamlined.cli config --validate
   ```

3. **Resource initialization**: Check resource manager setup
   ```python
   coordinator = ResourceCoordinator()
   await coordinator.initialize()
   ```

### Debug Mode

```python
import logging
logging.basicConfig(level=logging.DEBUG)

# Run with debug logging
results = await execute_streamlined_pipeline(...)
```

## 📚 Examples

See `examples/usage_example.py` for comprehensive usage examples including:

- Direct execution
- Individual component usage
- Plugin inspection
- Configuration options
- Error handling
- Plugin testing

## 🤝 Contributing

When adding new data types or custom logic:

1. **Create plugins** that wrap existing implementations
2. **Register plugins** with the plugin registry
3. **Test integration** using the CLI or test methods
4. **Add examples** for new functionality

## 📄 License

Same as the main bicam-collection project. 