{{ config(
    materialized="table",
    tags=["operations", "quality", "congress_internal_reconciliation", "amendments"]
) }}

/* Member↔amendment reciprocity uses the member legislation lists and the
   amendment sponsor/cosponsor lists. The cosponsor endpoint must use the
   same complete-baseline-plus-delta selection as the other child callers:
   reading only ``int_raw_responses__current`` loses non-overlapping
   incremental windows and can make the member-side reciprocity audit appear
   to disagree with the amendment-side source. */
with member_edges as (
    select distinct
        case when sponsorship_type = 'sponsor'
            then 'member_amendment_sponsor' else 'member_amendment_cosponsor' end as relationship_type,
        bioguide_id as left_id,
        amendment_id as right_id,
        'member' as left_source,
        'left' as edge_side
    from {{ ref('stg_congress__member_amendments') }}
    where bioguide_id is not null
      and amendment_id is not null
), amendment_sponsor_edges as (
    select distinct
        'member_amendment_sponsor' as relationship_type,
        bioguide_id as left_id,
        amendment_id as right_id,
        'amendment' as left_source,
        'right' as edge_side
    from {{ ref('stg_congress__amendment_sponsors') }}
    where bioguide_id is not null
      and amendment_id is not null
), amendment_cosponsor_edges as (
    select distinct
        'member_amendment_cosponsor' as relationship_type,
        bioguide_id as left_id,
        amendment_id as right_id,
        'amendment' as left_source,
        'right' as edge_side
    from {{ ref('int_congress_amendment_cosponsor_observations') }}
    where bioguide_id is not null
      and amendment_id is not null
)
select relationship_type, left_id, right_id, left_source, edge_side
from member_edges
union all
select relationship_type, left_id, right_id, left_source, edge_side
from amendment_sponsor_edges
union all
select relationship_type, left_id, right_id, left_source, edge_side
from amendment_cosponsor_edges
