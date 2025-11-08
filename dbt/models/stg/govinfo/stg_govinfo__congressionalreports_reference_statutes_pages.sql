{{ config(materialized='view', tags=['stg','govinfo','congressionalreports']) }}

{%- set src = source('govinfo_staging', 'congressionalreports_reference_statutes_pages') -%}
{%- set cols = columns_in(src) -%}

with src as (select * from {{ src }})
select
  s.*
  {# no package_id expected; link via report_statute_id if needed #}
  {%- if 'processed_at' in cols %}
  , {{ safe_timestamp('s.processed_at') }} as processed_at_ts
  {%- endif %}
from src as s

