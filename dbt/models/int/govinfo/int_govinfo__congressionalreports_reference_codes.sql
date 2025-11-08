{{ config(materialized='table', tags=['int','govinfo','congressionalreports']) }}

with base as (
  select * from {{ ref('stg_govinfo__congressionalreports_reference_codes') }}
),
dedup as (
  select b.*,
         row_number() over (partition by b.id order by b.processed_at_ts desc nulls last) rn
  from base b
)
select * from dedup where rn = 1

