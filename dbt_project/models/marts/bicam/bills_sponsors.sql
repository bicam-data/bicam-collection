{{ config(schema='bicam', materialized='table', tags=['bills', 'public'], indexes=[{'columns': ['bill_id', 'bioguide_id'], 'unique': true}, {'columns': ['bioguide_id']}]) }}
select relationships.legislation_id as bill_id, relationships.member_id as bioguide_id
from {{ ref('bridge_members__legislation') }} as relationships
join {{ ref('bills') }} as bills
  on bills.bill_id = relationships.legislation_id
where relationships.legislation_type = 'bill'
  and relationships.relationship_role = 'sponsor'
