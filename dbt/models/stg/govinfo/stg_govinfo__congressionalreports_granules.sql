{{ config(materialized='view', tags=['stg','govinfo','congressionalreports','granules']) }}

with flat as (
  {{ select_flatten_payload('govinfo_raw', 'congressionalreports_granules_raw') }}
)
select
  granule_id,
  {{ govinfo_set_id('granule_id') }}  as package_set_id,
  {{ govinfo_part_number('granule_id') }} as part_number,
  raw_id,
  source_doc_id,
  scraped_at as processed_at_ts,
  payload
from flat
