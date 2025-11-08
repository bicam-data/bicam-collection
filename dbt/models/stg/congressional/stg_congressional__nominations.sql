{{ config(materialized='view', tags=['stg','congressional','nominations']) }}

{%- set src = source('congressional_staging', 'nominations') -%}
{%- set cols = columns_in(src) -%}

with src as (
  select * from {{ src }}
)
select
  s.*
  {%- if 'processed_at' in cols %}
  , {{ safe_timestamp('s.processed_at') }} as processed_at_ts
  {%- endif %}
  {%- if 'nominee_full_name' in cols %}
  , {{ clean_long_text('s.nominee_full_name') }} as nominee_full_name_clean
  {%- endif %}
from src as s

