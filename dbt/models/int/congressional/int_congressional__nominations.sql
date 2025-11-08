{{ config(materialized='table', tags=['int','congressional','nominations']) }}

with base as (
  select * from {{ ref('stg_congressional__nominations') }}
),
dedup as (
  select b.*,
         row_number() over (partition by b.nomination_id order by b.processed_at_ts desc nulls last) rn
  from base b
)
select * from dedup where rn = 1

