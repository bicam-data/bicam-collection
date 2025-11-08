{{ config(materialized='view', tags=['stg','congressional','committeereports']) }}

{%- set src = source('congressional_staging', 'committeereports_committees') -%}
{%- set cols = columns_in(src) -%}

with src as (select * from {{ src }})
select
  s.*
  {%- if 'report_id' in cols %}
  , {{ govinfo_set_id('s.report_id') }} as report_set_id
  {%- endif %}
  {%- if 'processed_at' in cols %}
  , {{ safe_timestamp('s.processed_at') }} as processed_at_ts
  {%- endif %}
from src as s

