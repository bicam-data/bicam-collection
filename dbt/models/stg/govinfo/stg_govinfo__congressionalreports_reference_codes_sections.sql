{{ config(materialized='view', tags=['stg','govinfo','congressionalreports']) }}

{%- set src = source('govinfo_staging', 'congressionalreports_reference_codes_sections') -%}
{%- set cols = columns_in(src) -%}

with src as (select * from {{ src }})
select
  s.*
  {# package_id may not exist here; rely on parent linking when needed #}
  {%- if 'processed_at' in cols %}
  , {{ safe_timestamp('s.processed_at') }} as processed_at_ts
  {%- endif %}
from src as s

