{{ config(tags=['reconciliation', 'quality', 'identity_review']) }}

-- A person-level null must be either an explicitly reviewed non-federal
-- participant or an active review item. Silent nulls cannot enter releases.
select
    member_reference_person_id,
    source_member_reference_key,
    reference_kind,
    resolution_status,
    review_disposition,
    requires_manual_review
from {{ ref('stg_govinfo__member_references') }}
where bioguide_id is null
  and review_disposition is distinct from 'reviewed_non_federal_participant'
  and not requires_manual_review
