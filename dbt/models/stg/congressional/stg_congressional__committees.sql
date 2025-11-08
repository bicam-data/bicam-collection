{{ config(materialized='view', tags=['stg','congressional','committees']) }}

{%- set src = source('congressional_staging', 'committees') -%}
{%- set cols = columns_in(src) -%}

with src as (
  select * from {{ src }}
)
select
  s.*
  {%- if 'processed_at' in cols %}
  , {{ safe_timestamp('s.processed_at') }} as processed_at_ts
  {%- endif %}
from src as s

