{{ config(
    materialized="table",
    tags=["members", "bills", "amendments", "reconciliation", "bridge"]
) }}

/*
   Congress.gov exposes sponsorship from both sides: member detail lists
   legislation, while bill/amendment detail lists members.  The reciprocity
   fact has already unioned those observations.  Publish that union as a
   consumer-facing bridge so a researcher gets the same relationship whether
   they start from a member or from legislation, without silently replacing
   either source's evidence.
*/
select
    {{ hash_to_uuid("relationship_type || '|' || left_id || '|' || right_id") }} as bridge_id,
    left_id as member_id,
    right_id as legislation_id,
    case
        when relationship_type like 'member_bill_%' then 'bill'
        when relationship_type like 'member_amendment_%' then 'amendment'
    end as legislation_type,
    case
        when relationship_type like '%_cosponsor' then 'cosponsor'
        when relationship_type like '%_sponsor' then 'sponsor'
    end as relationship_role,
    left_present as member_side_present,
    right_present as legislation_side_present,
    evidence_directions,
    evidence_sources,
    evidence_count,
    reciprocity_status,
    left_endpoint_available as member_endpoint_available,
    right_endpoint_available as legislation_endpoint_available
from {{ ref('fct_congress_internal_reciprocity') }}
where relationship_type in (
    'member_bill_sponsor',
    'member_bill_cosponsor',
    'member_amendment_sponsor',
    'member_amendment_cosponsor'
)
