{{ config(materialized='view', tags=['stg','congressional','treaties']) }}

{%- set src = source('congressional_staging', 'treaties') -%}
{%- set cols = columns_in(src) -%}

with src as (
  select * from {{ src }}
)
select
  s.*
  {%- if 'processed_at' in cols %}
  , {{ safe_timestamp('s.processed_at') }} as processed_at_ts
  {%- endif %}
  {%- if 'title' in cols %}
  , {{ clean_long_text('s.title') }} as title_clean
  {%- endif %}
from src as s

