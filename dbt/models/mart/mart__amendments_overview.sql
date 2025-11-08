{{ config(materialized='view', tags=['mart','congressional','amendments'], enabled=false) }}

with core as (
  select * from {{ ref('core__amendments') }}
)
select
  source_system,
  chamber_std,
  count(*) as amendments_count,
  sum(coalesce(actions_count,0)) as actions_total,
  sum(coalesce(texts_count,0)) as texts_total
from core
group by 1,2
