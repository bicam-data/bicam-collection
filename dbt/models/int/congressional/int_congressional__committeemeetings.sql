{{ config(materialized='table', tags=['int','congressional','committeemeetings']) }}

with base as (
  select * from {{ ref('stg_congressional__committeemeetings') }}
),
dedup as (
  select b.*,
         row_number() over (partition by b.meeting_id order by b.processed_at_ts desc nulls last) rn
  from base b
)
select * from dedup where rn = 1

