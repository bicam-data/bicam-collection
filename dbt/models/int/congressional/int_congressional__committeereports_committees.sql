{{ config(materialized='table', tags=['int','congressional','committeereports']) }}

{%- set cols = columns_in(ref('stg_congressional__committeereports_committees')) -%}

with base as (
  select * from {{ ref('stg_congressional__committeereports_committees') }}
),
dedup as (
  select b.*,
         row_number() over (
           partition by
             {%- if 'report_id' in cols %} b.report_id, {%- endif %}
             {%- if 'committee_code' in cols %} b.committee_code, {%- endif %}
             1
           order by b.processed_at_ts desc nulls last
         ) rn
  from base b
)
select * from dedup where rn = 1

