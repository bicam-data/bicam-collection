{{ config(tags=['reconciliation', 'quality', 'identity_review']) }}

-- The public join key must never diverge from the evidence-backed decision.
select
    member_reference_bridge_id,
    source_member_reference_key,
    resolved_bioguide_id,
    linkable_bioguide_id,
    bioguide_id
from {{ ref('bridge_govinfo__member_references') }}
where linkable_bioguide_id is distinct from resolved_bioguide_id
   or bioguide_id is distinct from resolved_bioguide_id
