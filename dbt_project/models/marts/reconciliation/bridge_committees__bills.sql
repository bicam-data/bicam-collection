{{ config(
    materialized="table",
    tags=["committees", "bills", "reconciliation", "bridge"]
) }}

/*
   Committee detail exposes an authored/committee-side bill listing, while
   bill detail exposes referral-side committee evidence.  Keep the canonical
   union once and expose both direction flags so downstream analyses do not
   have to choose one endpoint and lose the other.
*/
select
    {{ hash_to_uuid("relationship_type || '|' || left_id || '|' || right_id") }} as bridge_id,
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
