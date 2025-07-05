# Integration Guide: Migrating to Optimized Dynamic Key Pool

## Overview

This guide walks through integrating the optimized dynamic key pool system into your existing Congressional data processing pipeline.

## Step 1: Update SystemAPIKeyManager

Modify your existing `SystemAPIKeyManager` to support the new dynamic pool mode:

```python
# In system_api_key_manager.py

class SystemAPIKeyManager:
    def __init__(
        self,
        api_keys: list[str],
        default_keys_per_client: int = 2,
        enable_parallelization: bool = True,
        use_dynamic_pool: bool = False  # New parameter
    ):
        # ... existing init code ...
        
        self.use_dynamic_pool = use_dynamic_pool
        if use_dynamic_pool:
            from .dynamic_key_pool import DynamicKeyPool
            self.dynamic_pool = DynamicKeyPool(api_keys)
    
    def get_dynamic_pool(self) -> Optional[DynamicKeyPool]:
        """Get the dynamic pool if enabled."""
        return self.dynamic_pool if self.use_dynamic_pool else None
```

## Step 2: Update CongressionalBaseFetcher

Modify the fetcher to support both old and new processing modes:

```python
# In congressional_base_fetcher.py

class CongressionalBaseFetcher(BaseFetcher):
    async def _execute_redistribution_strategy(
        self,
        api_clients: list,
        distribution_strategy: dict,
        from_date: str | None,
        to_date: str | None,
        limit: int | None,
        batch_size: int,
        **kwargs,
    ) -> dict[str, Any]:
        """Execute redistribution strategy."""
        
        # Check if we should use the new optimized processor
        if hasattr(self, 'processing_resource') and hasattr(self.processing_resource, 'use_dynamic_pool'):
            if self.processing_resource.use_dynamic_pool:
                # Use new optimized processor
                from .optimized_parallel_processor import OptimizedParallelProcessor
                
                processor = OptimizedParallelProcessor(
                    api_keys=self.processing_resource.api_keys,
                    client_class=self.client.__class__,
                    db_pool=self.db_pool
                )
                
                return await processor.process_data_type(
                    fetcher=self,
                    data_type=self.data_type_name,
                    from_date=from_date,
                    to_date=to_date,
                    limit=limit,
                    **kwargs
                )
        
        # Fall back to existing implementation
        return await super()._execute_redistribution_strategy(
            api_clients, distribution_strategy, from_date, to_date, 
            limit, batch_size, **kwargs
        )
```

## Step 3: Update Processing Resource Configuration

Update your processing resource to enable the new mode:

```python
# In your dagster asset or main processing script

@asset
async def process_congressional_bills():
    # Initialize with dynamic pool enabled
    api_key_manager = SystemAPIKeyManager(
        api_keys=API_KEYS,
        enable_parallelization=True,
        use_dynamic_pool=True  # Enable new mode
    )
    
    # Create processing resource
    processing_resource = CongressionalProcessingResource(
        api_key_manager=api_key_manager,
        db_pool=db_pool
    )
    
    # Create fetcher
    fetcher = BillsFetcher(
        client=client,
        db_pool=db_pool,
        data_type_name="bills"
    )
    fetcher.set_processing_resource(processing_resource)
    
    # Process with optimal settings
    results = await fetcher.process_items(
        from_date=from_date,
        to_date=to_date,
        limit=250,  # Page size
        enable_parallelization=True,
        redistribute_idle_sessions=True,  # This will trigger the new mode
        max_concurrent=50  # More workers than keys
    )
    
    return results
```

## Step 4: Configuration Recommendations

### Optimal Settings for 1M+ Records with 32 API Keys

```python
# Recommended configuration
config = {
    "api_keys": 32,  # Your 32 keys
    "num_workers": 48,  # 1.5x keys
    "chunk_size": 5000,  # Start with 5k records per chunk
    "page_size": 250,  # API page size
    "rate_limit_threshold": 4995,  # Switch keys at 4995/5000 requests
    "max_concurrent": 50,  # Maximum concurrent operations
}
```

### Expected Performance

With this configuration:
- **32 keys × 5,000 requests = 160,000 total requests available**
- **1M records ÷ 250 per page = 4,000 pages needed**
- **You should complete in one wave without any rate limiting!**

The key improvements:
1. All 32 keys working simultaneously
2. 48 workers ensuring keys are always busy
3. Dynamic reallocation when keys approach limits
4. No idle time from fixed pairings

### Monitoring and Tuning

```python
# Add monitoring to track performance
async def monitor_processing(processor, interval=30):
    """Monitor processing progress."""
    while True:
        status = processor.key_pool.get_pool_status()
        
        print(f"\n=== Processing Status ===")
        print(f"Keys Available: {status['available']}/{status['total_keys']}")
        print(f"Capacity Used: {status['capacity_used_percent']:.1f}%")
        print(f"In Use: {status['in_use']}")
        print(f"Rate Limited: {status['rate_limited']}")
        
        # Show per-key status
        for key_info in status['keys_detail'][:5]:  # Top 5
            print(f"  {key_info['key']}: {key_info['state']} - "
                  f"{key_info['requests']}/{key_info['remaining']} used")
        
        await asyncio.sleep(interval)
```

## Step 5: Gradual Migration

For safety, you can migrate gradually:

### Phase 1: Test with Small Dataset
```python
# Test with bills from last 7 days
results = await fetcher.process_items(
    from_date=(datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d"),
    to_date=datetime.now().strftime("%Y-%m-%d"),
    enable_parallelization=True,
    redistribute_idle_sessions=True
)
```

### Phase 2: A/B Testing
```python
# Run both old and new in parallel, compare results
use_new_mode = os.environ.get("USE_DYNAMIC_POOL", "false").lower() == "true"

api_key_manager = SystemAPIKeyManager(
    api_keys=API_KEYS,
    enable_parallelization=True,
    use_dynamic_pool=use_new_mode
)
```

### Phase 3: Full Migration
Once validated, enable for all data types and remove old code.

## Troubleshooting

### Issue: Workers waiting for keys
**Solution**: Increase worker count or reduce chunk size

### Issue: Rate limits still hit
**Solution**: Lower `rate_limit_threshold` to switch keys earlier (e.g., 4990)

### Issue: Uneven work distribution
**Solution**: Enable adaptive chunk sizing in `AdaptiveWorkQueue`

## Performance Metrics to Track

1. **Total Processing Time**: Should decrease by 40-60%
2. **Key Utilization**: Should be >90% (vs ~50% with paired keys)
3. **Rate Limit Hits**: Should be near zero
4. **Records per Second**: Should increase significantly

## Rollback Plan

If issues arise, you can instantly rollback by setting:
```python
use_dynamic_pool=False  # Reverts to old behavior
```

The system maintains backward compatibility with all existing code.