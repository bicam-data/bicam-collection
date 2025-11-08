{{ config(materialized='view', tags=['stg','govinfo','congressionalreports']) }}

with flat as (
  {{ select_flatten_payload('govinfo_raw', 'congressionalreports_raw') }}
)
select
  package_id,
  {{ govinfo_set_id('package_id') }} as package_set_id,
  0::int as part_number,
  raw_id,
  source_doc_id,
  scraped_at as processed_at_ts,
  payload
from flat
