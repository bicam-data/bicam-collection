{{ config(materialized='view', tags=['stg','congressional','congresses']) }}

{%- set src = source('congressional_staging', 'congresses') -%}
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

