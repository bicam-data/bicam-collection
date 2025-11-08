{{ config(materialized='table', tags=['int','congressional','committees']) }}

with base as (
  select * from {{ ref('stg_congressional__committees') }}
),
dedup as (
  select b.*,
         row_number() over (partition by b.committee_code order by b.processed_at_ts desc nulls last) rn
  from base b
)
select * from dedup where rn = 1

