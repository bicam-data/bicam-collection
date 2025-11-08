{{ config(materialized='view', tags=['stg','congressional','amendments'], enabled=false) }}

{%- set src = source('congressional_staging', 'amendments') -%}
{%- set cols = columns_in(src) -%}

with src as (
  select * from {{ src }}
)

select
  -- pass-through all columns
  s.*
  {# derived/standardized columns (only if base column exists) #}
  {%- if 'chamber' in cols %}
  , {{ standardize_chamber('s.chamber') }} as chamber_std
  {%- endif %}
  {%- if 'processed_at' in cols %}
  , {{ safe_timestamp('s.processed_at') }} as processed_at_ts
  {%- endif %}
  {%- if 'title' in cols %}
  , {{ clean_long_text('s.title') }} as title_clean
  {%- endif %}
from src as s
