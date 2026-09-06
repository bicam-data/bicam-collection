{{ config(
    schema='bicam', materialized='table', tags=['bicam_public', 'amendments', 'bills'],
    indexes=[
      {'columns': ['amendment_id', 'bill_id'], 'unique': true},
      {'columns': ['bill_id']}
    ]
) }}

select distinct
    right_id as amendment_id,
    left_id as bill_id
from {{ ref('int_congress_internal_relationship_union') }}
where relationship_type = 'bill_amendment'
  and right_id is not null
  and left_id is not null
