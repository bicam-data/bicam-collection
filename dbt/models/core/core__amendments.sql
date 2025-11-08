{{ config(materialized='table', tags=['core','congressional','amendments'], enabled=false) }}

-- Current core implementation includes congressional; add govinfo union when available

select
  'congressional'::text as source_system,
  c.*
from {{ ref('int_congressional__amendments') }} c
