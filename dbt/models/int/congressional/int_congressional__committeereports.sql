{{ config(materialized='table', tags=['int','congressional','committeereports']) }}

with base as (
  select * from {{ ref('stg_congressional__committeereports') }}
),
dedup as (
  select b.*,
         row_number() over (partition by b.report_id order by b.processed_at_ts desc nulls last) rn
  from base b
)
select * from dedup where rn = 1

