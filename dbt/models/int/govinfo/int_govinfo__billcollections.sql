{{ config(materialized='table', tags=['int','govinfo','billcollections']) }}

with base as (
  select * from {{ ref('stg_govinfo__billcollections') }}
),
dedup as (
  select b.*,
         row_number() over (partition by b.package_id order by b.processed_at_ts desc nulls last) rn
  from base b
)
select * from dedup where rn = 1

