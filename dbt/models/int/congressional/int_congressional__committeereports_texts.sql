{{ config(materialized='table', tags=['int','congressional','committeereports']) }}

with base as (
  select * from {{ ref('stg_congressional__committeereports_texts') }}
),
dedup as (
  select b.*,
         row_number() over (
           partition by coalesce(b.id::text, b.report_id::text, b.pdf::text)
           order by b.processed_at_ts desc nulls last
         ) rn
  from base b
)
select * from dedup where rn = 1

