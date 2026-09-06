/* The committee/bill bridge is a release-facing projection of the canonical
   union; no endpoint direction may be dropped. */
with expected as (
    select
        left_id as committee_id,
        right_id as bill_id,
        left_present as committee_side_present,
        right_present as bill_side_present,
        evidence_directions,
        evidence_sources,
        evidence_count,
        reciprocity_status,
        left_endpoint_available as committee_endpoint_available,
        right_endpoint_available as bill_endpoint_available
    from {{ ref('fct_congress_internal_reciprocity') }}
    where relationship_type = 'committee_bill'
), actual as (
    select
        committee_id,
        bill_id,
        committee_side_present,
        bill_side_present,
        evidence_directions,
        evidence_sources,
        evidence_count,
        reciprocity_status,
        committee_endpoint_available,
        bill_endpoint_available
    from {{ ref('bridge_committees__bills') }}
)
select
    coalesce(expected.committee_id, actual.committee_id) as committee_id,
    coalesce(expected.bill_id, actual.bill_id) as bill_id
from expected
full outer join actual using (
    committee_id,
    bill_id,
    committee_side_present,
    bill_side_present,
    evidence_directions,
    evidence_sources,
    evidence_count,
    reciprocity_status,
    committee_endpoint_available,
    bill_endpoint_available
)
where expected.committee_id is null
   or actual.committee_id is null
