select
    category,
    partition_value,
    root_item_count,
    fetched_detail_count,
    missing_detail_count,
    confirmed_source_error_detail_count,
    actionable_missing_detail_count,
    quality_status
from {{ ref('fct_root_detail_coverage') }}
where root_item_count <> fetched_detail_count + missing_detail_count
   or missing_detail_count <>
      confirmed_source_error_detail_count + actionable_missing_detail_count
   or (actionable_missing_detail_count = 0 and quality_status <> 'pass')
   or (actionable_missing_detail_count > 0 and quality_status <> 'incomplete')
