{{ config(materialized='view', tags=['stg','govinfo','congressionalreports']) }}

{%- set src = source('govinfo_staging', 'congressionalreports_committees') -%}
{%- set cols = columns_in(src) -%}

with src as (select * from {{ src }})
select
  s.*
  {%- if 'package_id' in cols %}
  , {{ govinfo_set_id('s.package_id') }} as package_set_id
  {%- endif %}
  {%- if 'processed_at' in cols %}
  , {{ safe_timestamp('s.processed_at') }} as processed_at_ts
  {%- endif %}
from src as s

