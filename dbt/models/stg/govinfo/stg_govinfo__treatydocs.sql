{{ config(materialized='view', tags=['stg','govinfo','treatydocs']) }}

{%- set src = source('govinfo_staging', 'treatydocs') -%}
{%- set cols = columns_in(src) -%}

with src as (
  select * from {{ src }}
)
select
  s.*,
  {{ govinfo_set_id('s.package_id') }} as package_set_id,
  0::int as part_number
  {%- if 'processed_at' in cols %}
  , {{ safe_timestamp('s.processed_at') }} as processed_at_ts
  {%- endif %}
from src as s

