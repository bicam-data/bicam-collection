{{ config(materialized='table', tags=['int','congressional','treaties']) }}

with base as (
  select * from {{ ref('stg_congressional__treaties') }}
),
dedup as (
  select b.*,
         row_number() over (partition by b.treaty_id order by b.processed_at_ts desc nulls last) rn
  from base b
)
select * from dedup where rn = 1

