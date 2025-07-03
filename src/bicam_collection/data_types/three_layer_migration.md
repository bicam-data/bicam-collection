# Data Pipeline Architecture Migration Manifest

## Overview

This document outlines the migration from a Congressional-centric base class architecture to a proper three-layer abstraction hierarchy that treats all data sources as first-class citizens.

## Current State Analysis

### Problematic Architecture

The current `src/bicam_collection/data_types/base/` classes are not truly generic:

- Hardcoded Congressional schema names (`bicam_raw_congressional`, `bicam_staging_congressional`)
- Congressional-specific progress tracking (`"congressional"` system name)
- 3-phase processing pattern baked into "base" classes
- Congressional API assumptions in method signatures

### Issues with Current Approach

1. **False Abstraction**: "Base" classes are actually Congressional-specific
2. **Technical Debt**: Adding GovInfo requires working around Congressional assumptions
3. **Poor Extensibility**: Future data sources face the same Congressional bias
4. **Architectural Inconsistency**: Congressional gets special treatment as "default"

## Target Architecture

### Three-Layer Hierarchy

```bash
Layer 1: Abstract Base Classes (Pure Interfaces)
├── AbstractFetcher
├── AbstractDatabaseNormalizer  
├── AbstractCleaner
└── AbstractSpecializedAssets

Layer 2: Source-Specific Base Classes
├── Congressional Base Classes
│   ├── CongressionalBaseFetcher (3-phase pattern)
│   ├── CongressionalBaseNormalizer
│   ├── CongressionalBaseCleaner
│   └── CongressionalBaseAssets
└── GovInfo Base Classes
    ├── GovInfoBaseFetcher (4-phase pattern)
    ├── GovInfoBaseNormalizer
    ├── GovInfoBaseCleaner
    └── GovInfoBaseAssets

Layer 3: Data Type Implementations
├── Congressional Data Types
│   ├── BillsFetcher (inherits CongressionalBaseFetcher)
│   ├── AmendmentsFetcher
│   └── ...
└── GovInfo Data Types
    ├── BillsCollectionFetcher (inherits GovInfoBaseFetcher)
    ├── CongressionalReportsFetcher
    └── ...
```

## Migration Steps

### Phase 1: Create Abstract Layer (Layer 1)

#### 1.1 Create Abstract Directory Structure

```bash
mkdir -p src/bicam_collection/data_types/abstract
touch src/bicam_collection/data_types/abstract/__init__.py
```

#### 1.2 Extract Pure Abstractions

**Create `abstract_fetcher.py`:**

```python
from abc import ABC, abstractmethod
from typing import Any

class AbstractFetcher(ABC):
    """
    Pure abstract fetcher with no data source assumptions.
    
    Defines only the essential interface that all fetchers must implement,
    without imposing any specific processing patterns or schema conventions.
    """
    
    def __init__(self, client, db_pool=None, data_type_name=None, 
                 checkpoint_manager=None, run_manager=None):
        self.client = client
        self.db_pool = db_pool
        self.data_type_name = data_type_name
        self.checkpoint_manager = checkpoint_manager
        self.run_manager = run_manager
        self.progress_tracker = None
        self.current_run_id = None
    
    @abstractmethod
    def extract_item_id(self, item_data: dict[str, Any]) -> str:
        """Extract standardized ID from item data."""
        pass
    
    @abstractmethod
    async def process_items(self, **kwargs) -> dict[str, Any]:
        """Main processing method - implementation completely defined by subclasses."""
        pass
    
    @abstractmethod
    def setup_progress_tracker(self):
        """Setup progress tracking - source-specific implementation required."""
        pass
    
    @abstractmethod
    async def store_raw_data(self, schema: str, table: str, items: list[dict], **kwargs):
        """Store raw data - must specify schema explicitly, no defaults."""
        pass
```

**Create `abstract_database_normalizer.py`:**

```python
from abc import ABC, abstractmethod
from typing import Any

class AbstractDatabaseNormalizer(ABC):
    """
    Pure abstract normalizer with no schema assumptions.
    """
    
    def __init__(self, config_path=None, db_pool=None, checkpoint_manager=None,
                 run_manager=None, data_type_name=None, 
                 target_schema=None, source_schema=None):
        # IMPORTANT: No default schema values
        self.config_path = config_path
        self.db_pool = db_pool
        self.checkpoint_manager = checkpoint_manager
        self.run_manager = run_manager
        self.data_type_name = data_type_name
        self.target_schema = target_schema
        self.source_schema = source_schema
        self.progress_tracker = None
        self.current_run_id = None
        
        # Validate required schemas
        if not target_schema or not source_schema:
            raise ValueError("target_schema and source_schema must be explicitly provided")
    
    @abstractmethod
    def get_main_table_name(self) -> str:
        """Get the main table name for this data type."""
        pass
    
    @abstractmethod
    def get_foreign_key_column(self) -> str:
        """Get the foreign key column name used in related tables."""
        pass
    
    @abstractmethod
    async def process_items(self, item_ids: list[str], **kwargs) -> dict[str, Any]:
        """Main normalization processing method."""
        pass
    
    @abstractmethod
    def setup_progress_tracker(self):
        """Setup progress tracking - source-specific implementation."""
        pass
```

**Create `abstract_cleaner.py`:**

```python
from abc import ABC, abstractmethod
from typing import Any

class AbstractCleaner(ABC):
    """
    Pure abstract cleaner with no schema assumptions.
    """
    
    def __init__(self, config_path=None, db_pool=None, checkpoint_manager=None,
                 run_manager=None, data_type_name=None,
                 staging_schema=None, production_schema=None):
        # IMPORTANT: No default schema values
        self.config_path = config_path
        self.db_pool = db_pool
        self.checkpoint_manager = checkpoint_manager
        self.run_manager = run_manager
        self.data_type_name = data_type_name
        self.staging_schema = staging_schema
        self.production_schema = production_schema
        self.progress_tracker = None
        self.current_run_id = None
        
        # Validate required schemas
        if not staging_schema or not production_schema:
            raise ValueError("staging_schema and production_schema must be explicitly provided")
    
    @abstractmethod
    async def process_items(self, **kwargs) -> dict[str, Any]:
        """Main cleaning processing method."""
        pass
    
    @abstractmethod
    def setup_progress_tracker(self):
        """Setup progress tracking - source-specific implementation."""
        pass
    
    @abstractmethod
    def get_data_types_to_process(self) -> list[str]:
        """Get list of data types to clean."""
        pass
```

**Create `abstract_specialized_assets.py`:**

```python
from abc import ABC, abstractmethod
from typing import Any

class AbstractSpecializedAssets(ABC):
    """
    Pure abstract assets with no source assumptions.
    """
    
    def __init__(self, data_type_name: str, source_system: str):
        self.data_type_name = data_type_name
        self.source_system = source_system  # "congressional", "govinfo", etc.
    
    @abstractmethod
    def get_fetcher_class(self):
        """Get the fetcher class for this data type."""
        pass
    
    @abstractmethod
    def get_database_normalizer_class(self):
        """Get the database normalizer class for this data type."""
        pass
    
    @abstractmethod
    def get_cleaner_class(self):
        """Get the cleaner class for this data type."""
        pass
    
    @abstractmethod
    def create_complete_pipeline_asset(self, **kwargs):
        """Create complete pipeline asset."""
        pass
    
    @abstractmethod
    def create_data_quality_report_asset(self, **kwargs):
        """Create data quality report asset."""
        pass
```

#### 1.3 Create Abstract Layer `__init__.py`

```python
"""
Abstract base classes for data pipeline components.

These classes define pure interfaces with no assumptions about:
- Data sources (Congressional, GovInfo, etc.)
- Schema naming conventions  
- API patterns or processing flows
- Progress tracking systems

All concrete implementations must inherit from these abstracts
through source-specific base classes.
"""

from .abstract_fetcher import AbstractFetcher
from .abstract_database_normalizer import AbstractDatabaseNormalizer
from .abstract_cleaner import AbstractCleaner
from .abstract_specialized_assets import AbstractSpecializedAssets

__all__ = [
    "AbstractFetcher",
    "AbstractDatabaseNormalizer", 
    "AbstractCleaner",
    "AbstractSpecializedAssets",
]
```

### Phase 2: Migrate Congressional Classes (Layer 2)

#### 2.1 Restructure Congressional Directory

```bash
# Move existing base classes to congressional-specific location
mkdir -p src/bicam_collection/data_types/congressional/base
mv src/bicam_collection/data_types/base/* src/bicam_collection/data_types/congressional/base/

# Rename files to be explicit about Congressional specificity
cd src/bicam_collection/data_types/congressional/base
mv base_fetcher.py congressional_base_fetcher.py
mv base_database_normalizer.py congressional_base_normalizer.py
mv base_cleaner.py congressional_base_cleaner.py
mv base_specialized_assets.py congressional_base_specialized_assets.py
```

#### 2.2 Update Congressional Base Classes

**Modify `congressional_base_fetcher.py`:**

```python
"""
Congressional-specific base fetcher implementing the 3-phase pattern.
"""

from ...abstract import AbstractFetcher
from abc import abstractmethod
from collections.abc import AsyncIterator
from typing import Any

class CongressionalBaseFetcher(AbstractFetcher):
    """
    Congressional-specific base fetcher.
    
    Implements the Congressional API 3-phase pattern:
    1. fetch_list_data() → bulk endpoints
    2. fetch_full_data() → individual item URLs
    3. fetch_related_data() → related endpoints from full data
    
    Provides Congressional-specific defaults for schemas and progress tracking.
    """
    
    def setup_progress_tracker(self):
        """Congressional-specific progress tracker setup."""
        if not self.checkpoint_manager or not self.data_type_name:
            logger.warning("Cannot setup progress tracker - missing dependencies")
            return None

        self.progress_tracker = HierarchicalProgressTracker.create_for_stage(
            self.checkpoint_manager,
            "congressional",  # Congressional-specific system name
            self.data_type_name,
            ProcessingStage.SCRAPING,
        )
        return self.progress_tracker
    
    # Define Congressional-specific 3-phase pattern
    @abstractmethod
    async def fetch_list_data(self, **kwargs) -> AsyncIterator[list[dict[str, Any]]]:
        """Phase 1: Fetch list data from Congressional bulk endpoints."""
        pass
    
    @abstractmethod
    async def fetch_full_data(self, item_url: str) -> dict[str, Any] | None:
        """Phase 2: Fetch complete item data from Congressional URL."""
        pass
    
    async def fetch_related_data(self, full_data: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
        """Phase 3: Fetch related data (optional override)."""
        return {}
    
    async def process_items(self, **kwargs) -> dict[str, Any]:
        """Congressional-specific 3-phase processing implementation."""
        # Move the existing process_items logic here
        # (Keep all the existing implementation from base_fetcher.py)
        pass
    
    async def store_raw_data(self, schema: str = "bicam_raw_congressional", **kwargs):
        """Congressional storage with default Congressional schema."""
        # Move existing store_raw_data implementation here with Congressional defaults
        pass
```

**Modify `congressional_base_normalizer.py`:**

```python
"""
Congressional-specific base database normalizer.
"""

from ...abstract import AbstractDatabaseNormalizer

class CongressionalBaseDatabaseNormalizer(AbstractDatabaseNormalizer):
    """
    Congressional-specific base normalizer.
    
    Provides Congressional schema defaults and Congressional-specific
    progress tracking configuration.
    """
    
    def __init__(self, **kwargs):
        # Provide Congressional-specific schema defaults
        kwargs.setdefault("target_schema", "bicam_staging_congressional")
        kwargs.setdefault("source_schema", "bicam_raw_congressional")
        super().__init__(**kwargs)
    
    def setup_progress_tracker(self):
        """Congressional-specific progress tracker setup."""
        if not self.checkpoint_manager or not self.data_type_name:
            logger.warning("Cannot setup progress tracker - missing dependencies")
            return None

        self.progress_tracker = HierarchicalProgressTracker.create_for_stage(
            self.checkpoint_manager,
            "congressional",  # Congressional-specific system name
            self.data_type_name,
            ProcessingStage.NORMALIZATION,
        )
        return self.progress_tracker
    
    # Keep all existing implementation from base_database_normalizer.py
    async def process_items(self, item_ids: list[str], **kwargs) -> dict[str, Any]:
        # Move existing implementation here
        pass
```

**Modify `congressional_base_cleaner.py`:**

```python
"""
Congressional-specific base cleaner.
"""

from ...abstract import AbstractCleaner

class CongressionalBaseCleaner(AbstractCleaner):
    """
    Congressional-specific base cleaner.
    
    Provides Congressional schema defaults and Congressional-specific
    progress tracking configuration.
    """
    
    def __init__(self, **kwargs):
        # Provide Congressional-specific schema defaults
        kwargs.setdefault("staging_schema", "bicam_staging_congressional")
        kwargs.setdefault("production_schema", "bicam_congressional")
        super().__init__(**kwargs)
    
    def setup_progress_tracker(self):
        """Congressional-specific progress tracker setup."""
        if not self.checkpoint_manager or not self.data_type_name:
            logger.warning("Cannot setup progress tracker - missing dependencies")
            return None

        self.progress_tracker = HierarchicalProgressTracker.create_for_stage(
            self.checkpoint_manager,
            "congressional",  # Congressional-specific system name
            self.data_type_name,
            ProcessingStage.CLEANING,
        )
        return self.progress_tracker
    
    # Keep all existing implementation from base_cleaner.py
    async def process_items(self, **kwargs) -> dict[str, Any]:
        # Move existing implementation here
        pass
```

**Modify `congressional_base_specialized_assets.py`:**

```python
"""
Congressional-specific base specialized assets.
"""

from ...abstract import AbstractSpecializedAssets

class CongressionalBaseSpecializedAssets(AbstractSpecializedAssets):
    """
    Congressional-specific base assets.
    
    Provides Congressional-specific asset configuration and schema references.
    """
    
    def __init__(self, data_type_name: str):
        super().__init__(data_type_name, source_system="congressional")
    
    # Keep all existing implementation from base_specialized_assets.py
    # Update hardcoded schema references to use Congressional-specific values
```

#### 2.3 Update Congressional Base `__init__.py`

```python
"""
Congressional-specific base classes.

These classes inherit from the abstract base classes and provide
Congressional-specific implementations including:
- 3-phase processing pattern (list → full → related)
- Congressional schema defaults
- Congressional progress tracking configuration
- Congressional API patterns
"""

from .congressional_base_fetcher import CongressionalBaseFetcher
from .congressional_base_database_normalizer import CongressionalBaseDatabaseNormalizer
from .congressional_base_cleaner import CongressionalBaseCleaner
from .congressional_base_specialized_assets import CongressionalBaseSpecializedAssets

__all__ = [
    "CongressionalBaseFetcher",
    "CongressionalBaseDatabaseNormalizer",
    "CongressionalBaseCleaner", 
    "CongressionalBaseSpecializedAssets",
]
```

### Phase 3: Update Congressional Data Type Implementations (Layer 3)

#### 3.1 Update Import Statements

**Update `congressional/bills/fetcher.py`:**

```python
# OLD
from ...base import BaseFetcher

# NEW  
from ..base import CongressionalBaseFetcher

class BillsFetcher(CongressionalBaseFetcher):
    # Rest of implementation stays the same
    pass
```

**Update all other Congressional data type files:**

- `congressional/bills/database_normalizer.py`
- `congressional/bills/cleaner.py`
- `congressional/bills/specialized_assets.py`
- `congressional/amendments/*`
- All other Congressional data types

#### 3.2 Remove Old Base Directory

```bash
# After confirming all imports are updated
rm -rf src/bicam_collection/data_types/base
```

### Phase 4: Create GovInfo Layer (Layer 2)

#### 4.1 Create GovInfo Base Directory Structure

```bash
mkdir -p src/bicam_collection/data_types/govinfo/base
touch src/bicam_collection/data_types/govinfo/base/__init__.py
```

#### 4.2 Implement GovInfo Base Classes

**Create `govinfo_base_fetcher.py`:**

```python
"""
GovInfo-specific base fetcher implementing the 4-phase pattern.
"""

from ...abstract import AbstractFetcher
from abc import abstractmethod
from collections.abc import AsyncIterator
from typing import Any

class GovInfoBaseFetcher(AbstractFetcher):
    """
    GovInfo-specific base fetcher.
    
    Implements the GovInfo API 4-phase pattern:
    1. fetch_collection_data() → collections/packages
    2. fetch_package_data() → package metadata
    3. fetch_granules_list() → granules for package
    4. fetch_granule_data() → full granule data
    
    Provides GovInfo-specific defaults for schemas and progress tracking.
    """
    
    def setup_progress_tracker(self):
        """GovInfo-specific progress tracker setup."""
        if not self.checkpoint_manager or not self.data_type_name:
            logger.warning("Cannot setup progress tracker - missing dependencies")
            return None

        self.progress_tracker = HierarchicalProgressTracker.create_for_stage(
            self.checkpoint_manager,
            "govinfo",  # GovInfo-specific system name
            self.data_type_name,
            ProcessingStage.SCRAPING,
        )
        return self.progress_tracker
    
    # Define GovInfo-specific 4-phase pattern
    @abstractmethod
    async def fetch_collection_data(self, **kwargs) -> AsyncIterator[list[dict[str, Any]]]:
        """Phase 1: Fetch collections/packages from GovInfo."""
        pass
    
    @abstractmethod
    async def fetch_package_data(self, package_url: str) -> dict[str, Any] | None:
        """Phase 2: Fetch package metadata."""
        pass
    
    @abstractmethod
    async def fetch_granules_list(self, package_data: dict[str, Any]) -> list[dict[str, Any]]:
        """Phase 3: Fetch granules list for package."""
        pass
    
    @abstractmethod
    async def fetch_granule_data(self, granule_url: str) -> dict[str, Any] | None:
        """Phase 4: Fetch full granule data."""
        pass
    
    async def process_items(self, **kwargs) -> dict[str, Any]:
        """GovInfo-specific 4-phase processing implementation."""
        # Implement 4-phase GovInfo processing pattern
        logger.info(f"Starting {self.data_type_name} GovInfo processing")
        
        # Phase 1: Collections/Packages
        # Phase 2: Package metadata
        # Phase 3: Granules list
        # Phase 4: Granule data
        # Storage of all phases
        pass
    
    async def store_raw_data(self, schema: str = "bicam_raw_govinfo", **kwargs):
        """GovInfo storage with default GovInfo schema."""
        # Implement with GovInfo schema defaults
        pass
```

**Create `govinfo_base_normalizer.py`:**

```python
"""
GovInfo-specific base database normalizer.
"""

from ...abstract import AbstractDatabaseNormalizer

class GovInfoBaseDatabaseNormalizer(AbstractDatabaseNormalizer):
    """
    GovInfo-specific base normalizer.
    
    Provides GovInfo schema defaults and handles package/granule relationships.
    """
    
    def __init__(self, **kwargs):
        # Provide GovInfo-specific schema defaults
        kwargs.setdefault("target_schema", "bicam_staging_govinfo")
        kwargs.setdefault("source_schema", "bicam_raw_govinfo")
        super().__init__(**kwargs)
    
    def setup_progress_tracker(self):
        """GovInfo-specific progress tracker setup."""
        if not self.checkpoint_manager or not self.data_type_name:
            logger.warning("Cannot setup progress tracker - missing dependencies")
            return None

        self.progress_tracker = HierarchicalProgressTracker.create_for_stage(
            self.checkpoint_manager,
            "govinfo",  # GovInfo-specific system name
            self.data_type_name,
            ProcessingStage.NORMALIZATION,
        )
        return self.progress_tracker
    
    async def process_items(self, item_ids: list[str], **kwargs) -> dict[str, Any]:
        """GovInfo-specific normalization handling packages and granules."""
        # Implement GovInfo package/granule normalization logic
        pass
```

#### 4.3 Complete GovInfo Base Classes

Create `govinfo_base_cleaner.py` and `govinfo_base_specialized_assets.py` following the same pattern.

#### 4.4 Create GovInfo Base `__init__.py`

```python
"""
GovInfo-specific base classes.

These classes inherit from the abstract base classes and provide
GovInfo-specific implementations including:
- 4-phase processing pattern (collection → package → granules → granule data)
- GovInfo schema defaults
- GovInfo progress tracking configuration
- GovInfo API patterns
"""

from .govinfo_base_fetcher import GovInfoBaseFetcher
from .govinfo_base_database_normalizer import GovInfoBaseDatabaseNormalizer  
from .govinfo_base_cleaner import GovInfoBaseCleaner
from .govinfo_base_specialized_assets import GovInfoBaseSpecializedAssets

__all__ = [
    "GovInfoBaseFetcher",
    "GovInfoBaseDatabaseNormalizer",
    "GovInfoBaseCleaner",
    "GovInfoBaseSpecializedAssets", 
]
```

### Phase 5: Create GovInfo Data Types (Layer 3)

#### 5.1 Create GovInfo Data Type Structure

```bash
mkdir -p src/bicam_collection/data_types/govinfo/bills_collection
mkdir -p src/bicam_collection/data_types/govinfo/congressional_reports
mkdir -p src/bicam_collection/data_types/govinfo/hearings
# etc.
```

#### 5.2 Implement GovInfo Data Types

**Create `govinfo/bills_collection/fetcher.py`:**

```python
"""
Bills Collection fetcher for GovInfo using 4-phase pattern.
"""

from ..base import GovInfoBaseFetcher
from typing import Any

class BillsCollectionFetcher(GovInfoBaseFetcher):
    """
    Bills collection fetcher implementing GovInfo 4-phase pattern.
    """
    
    def extract_item_id(self, item_data: dict[str, Any]) -> str:
        """Extract package ID from GovInfo package data."""
        return item_data.get("packageId", "ID_ERROR")
    
    async def fetch_collection_data(self, **kwargs):
        """Phase 1: Fetch bills collection/packages."""
        async for batch in self.client.retrieve_collection_data(
            collection_code="BILLS",
            from_date=kwargs.get("from_date"),
            to_date=kwargs.get("to_date"),
            **kwargs
        ):
            yield batch
    
    async def fetch_package_data(self, package_url: str):
        """Phase 2: Fetch package metadata."""
        return await self.client.retrieve_package_data_from_url(
            package_url, expected_key="package"
        )
    
    async def fetch_granules_list(self, package_data: dict[str, Any]):
        """Phase 3: Get granules list for this package."""
        granules_url = package_data.get("granulesUrl")
        if not granules_url:
            return []
        
        return await self.client.retrieve_granules_from_url(
            granules_url, expected_key="granules"
        )
    
    async def fetch_granule_data(self, granule_url: str):
        """Phase 4: Fetch individual granule data.""" 
        return await self.client.retrieve_granule_data_from_url(
            granule_url, expected_key="granule"
        )
```

### Phase 6: Update Configuration and Integration

#### 6.1 Update Schema SQL Files

Create GovInfo-specific schema files:

- `src/bicam_collection/libs/sql/build_raw_schema_govinfo.sql`
- `src/bicam_collection/libs/sql/build_staging_schema_govinfo.sql`
- `src/bicam_collection/libs/sql/build_prod_schema_govinfo.sql`

#### 6.2 Update Data Type Router

```python
# In libs/data_type_router.py
def get_data_type_handlers():
    return {
        # Congressional data types
        "bills": {
            "fetcher": "bicam_collection.data_types.congressional.bills.BillsFetcher",
            "normalizer": "bicam_collection.data_types.congressional.bills.BillsDatabaseNormalizer",
            "cleaner": "bicam_collection.data_types.congressional.bills.BillsCleaner",
        },
        # GovInfo data types  
        "bills_collection": {
            "fetcher": "bicam_collection.data_types.govinfo.bills_collection.BillsCollectionFetcher",
            "normalizer": "bicam_collection.data_types.govinfo.bills_collection.BillsCollectionDatabaseNormalizer",
            "cleaner": "bicam_collection.data_types.govinfo.bills_collection.BillsCollectionCleaner",
        },
    }
```

#### 6.3 Update Dagster Definitions

```python
# In dagster_pipeline/definitions.py
from ..data_types.congressional.bills import get_specialized_assets as get_bills_assets
from ..data_types.govinfo.bills_collection import get_specialized_assets as get_bills_collection_assets

def create_all_assets():
    all_assets = []
    
    # Congressional assets
    bills_assets = get_bills_assets()
    all_assets.extend(bills_assets.get_all_assets())
    
    # GovInfo assets
    bills_collection_assets = get_bills_collection_assets()  
    all_assets.extend(bills_collection_assets.get_all_assets())
    
    return all_assets
```

## Migration Validation

### Testing Strategy

#### 1. Layer 1 Validation

- Ensure abstract classes cannot be instantiated
- Verify all required methods are marked as abstract
- Test that concrete implementations must provide all abstract methods

#### 2. Layer 2 Validation

- Test Congressional base classes with existing Congressional data types
- Verify GovInfo base classes work with new GovInfo implementations
- Ensure schema defaults are applied correctly
- Validate progress tracking uses correct system names

#### 3. Layer 3 Validation  

- Run existing Congressional data types through full pipeline
- Test new GovInfo data types through full pipeline
- Verify data flows correctly through all layers
- Validate database schemas are created correctly

#### 4. Integration Testing

- Test Dagster pipeline with both Congressional and GovInfo assets
- Verify data type router correctly routes to appropriate implementations
- Test that both source systems can run simultaneously

### Rollback Plan

If issues arise during migration:

1. **Phase 1-2 Issues**: Revert abstract layer, restore original base classes
2. **Phase 3 Issues**: Revert Congressional import changes, use original base classes
3. **Phase 4-5 Issues**: Remove GovInfo implementations, keep Congressional working
4. **Phase 6 Issues**: Revert configuration changes, disable GovInfo integrations

## Benefits Achieved

### ✅ **Proper Abstraction**

- True abstract interfaces with no source assumptions
- Clear separation between contract and implementation
- Easy to understand inheritance hierarchy

### ✅ **Equal Treatment**

- Congressional and GovInfo are peers at Layer 2
- No architectural favoritism or bias
- Each source can define its own patterns and conventions

### ✅ **Extensibility**

- New data sources only need Layer 2 implementations
- New data types only need Layer 3 implementations  
- Framework naturally supports different API patterns

### ✅ **Maintainability**

- Clear responsibility boundaries at each layer
- Changes at one layer don't affect others unnecessarily
- Easy to reason about and debug

### ✅ **Flexibility**

- Congressional: 3-phase pattern (list → full → related)
- GovInfo: 4-phase pattern (collection → package → granules → granule data)
- Future: Any pattern needed by new data sources

## Completion Criteria

Migration is complete when:

1. ✅ All abstract base classes are implemented and tested
2. ✅ Congressional base classes inherit from abstracts with Congressional defaults
3. ✅ All existing Congressional data types use Congressional base classes
4. ✅ GovInfo base classes are implemented with GovInfo defaults  
5. ✅ At least one GovInfo data type is fully implemented and tested
6. ✅ Both Congressional and GovInfo pipelines run successfully
7. ✅ Integration tests pass for both source systems
8. ✅ Documentation is updated to reflect new architecture

This migration transforms the codebase from Congressional-centric to truly multi-source, establishing a solid foundation for future data source integrations.
