{{ config(materialized='view', tags=['stg','congressional','committeemeetings']) }}

{%- set src = source('congressional_staging', 'committeemeetings') -%}
{%- set cols = columns_in(src) -%}

with src as (
  select * from {{ src }}
)
select
  s.*
  {%- if 'processed_at' in cols %}
  , {{ safe_timestamp('s.processed_at') }} as processed_at_ts
  {%- endif %}
  {%- if 'description' in cols %}
  , {{ clean_long_text('s.description') }} as description_clean
  {%- endif %}
from src as s

