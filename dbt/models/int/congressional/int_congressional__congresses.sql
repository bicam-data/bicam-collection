{{ config(materialized='table', tags=['int','congressional','congresses']) }}

with base as (
  select * from {{ ref('stg_congressional__congresses') }}
),
dedup as (
  select b.*,
         row_number() over (partition by b.congress_number order by b.processed_at_ts desc nulls last) rn
  from base b
)
select * from dedup where rn = 1

