select *
from {{ ref('fct_collection_coverage') }}
where current_response_count < 0
   or scheduled_fetch_count < 0
   or successful_fetch_count < 0
   or failed_fetch_count < 0
   or expected_fetch_count < 0
   or expected_child_fetch_count < 0
   or child_scheduled_fetch_count < 0
   or child_successful_fetch_count < 0
   or child_failed_fetch_count < 0
   or unsatisfied_fetch_count < 0
   or child_unsatisfied_fetch_count < 0
