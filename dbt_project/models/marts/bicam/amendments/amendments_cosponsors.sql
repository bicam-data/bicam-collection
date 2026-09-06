{{ config(
    schema='bicam', materialized='table', tags=['bicam_public', 'amendments', 'members'],
    indexes=[
      {'columns': ['amendment_id', 'bioguide_id'], 'unique': true},
      {'columns': ['bioguide_id']}
    ]
) }}

select
    relationships.legislation_id as amendment_id,
    relationships.member_id as bioguide_id
from {{ ref('bridge_members__legislation') }} as relationships
where relationships.legislation_type = 'amendment'
  and relationships.relationship_role = 'cosponsor'
