{{ config(materialized='table', tags=['int','govinfo','congressionalreports','granules']) }}

with base as (
  select * from {{ ref('stg_govinfo__congressionalreports_granules') }}
),
dedup as (
  select b.*,
         row_number() over (partition by b.granule_id order by b.processed_at_ts desc nulls last) rn
  from base b
)
select * from dedup where rn = 1

