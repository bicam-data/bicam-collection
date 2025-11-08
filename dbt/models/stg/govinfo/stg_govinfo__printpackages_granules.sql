{{ config(materialized='view', tags=['stg','govinfo','printpackages','granules']) }}

{%- set src = source('govinfo_staging', 'printpackages_granules') -%}
{%- set cols = columns_in(src) -%}

with src as (
  select * from {{ src }}
)
select
  s.*,
  {{ govinfo_set_id('s.granule_id') }} as package_set_id,
  {{ govinfo_part_number('s.granule_id') }} as part_number
  {%- if 'processed_at' in cols %}
  , {{ safe_timestamp('s.processed_at') }} as processed_at_ts
  {%- endif %}
from src as s

