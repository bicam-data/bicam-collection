/* Every unioned member sponsorship edge must be exposed exactly once in the
   release-facing bridge, with the same direction flags and status. */
with expected as (
    select
        relationship_type,
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
), actual as (
    select
        case
            when legislation_type = 'bill' and relationship_role = 'sponsor'
                then 'member_bill_sponsor'
            when legislation_type = 'bill' and relationship_role = 'cosponsor'
                then 'member_bill_cosponsor'
            when legislation_type = 'amendment' and relationship_role = 'sponsor'
                then 'member_amendment_sponsor'
            when legislation_type = 'amendment' and relationship_role = 'cosponsor'
                then 'member_amendment_cosponsor'
        end as relationship_type,
        member_id,
        legislation_id,
        legislation_type,
        relationship_role,
        member_side_present,
        legislation_side_present,
        evidence_directions,
        evidence_sources,
        evidence_count,
        reciprocity_status,
        member_endpoint_available,
        legislation_endpoint_available
    from {{ ref('bridge_members__legislation') }}
)
select
    coalesce(expected.relationship_type, actual.relationship_type) as relationship_type,
    coalesce(expected.member_id, actual.member_id) as member_id,
    coalesce(expected.legislation_id, actual.legislation_id) as legislation_id
from expected
full outer join actual using (
    relationship_type,
    member_id,
    legislation_id,
    legislation_type,
    relationship_role,
    member_side_present,
    legislation_side_present,
    evidence_directions,
    evidence_sources,
    evidence_count,
    reciprocity_status,
    member_endpoint_available,
    legislation_endpoint_available
)
where expected.relationship_type is null
   or actual.relationship_type is null
