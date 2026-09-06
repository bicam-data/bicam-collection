{{ config(
    schema='bicam',
    materialized='table',
    tags=['bicam_public', 'committees', 'bills'],
    indexes=[
      {'columns': ['committee_code', 'bill_id'], 'unique': true},
      {'columns': ['bill_id']},
      {'columns': ['committee_action_date']}
    ]
) }}

/* The internal relationship union retains an edge observed from either
   Congress.gov direction.  Attributes are taken from the committee-side
   listing when that listing is available; reciprocal-only edges remain
   visible with nullable descriptive attributes. */
with union_edges as (
    select left_id as committee_code, right_id as bill_id
    from {{ ref('int_congress_internal_relationship_union') }}
    where relationship_type = 'committee_bill'
), attributes as (
    select
        committee_id as committee_code,
        bill_id,
        max(relationship_type) as relationship_type,
        max(committee_action_date::date) as committee_action_date,
        max(updated_at) as updated_at
    from {{ ref('stg_congress__committee_bills') }}
    group by 1, 2
)
select
    edges.committee_code,
    edges.bill_id,
    attributes.relationship_type,
    attributes.committee_action_date,
    attributes.updated_at
from union_edges as edges
left join attributes
  on attributes.committee_code = edges.committee_code
 and attributes.bill_id = edges.bill_id
where edges.committee_code is not null
  and edges.bill_id is not null
