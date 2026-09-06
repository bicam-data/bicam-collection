{{ config(tags=['reconciliation', 'quality', 'identity_review']) }}

-- A source omission that was not resolved must remain visible to reviewers.
select
    member_reference_bridge_id,
    source_member_reference_key,
    resolution_status,
    requires_manual_review
from {{ ref('bridge_govinfo__member_references') }}
where source_bioguide_id is null
  and resolved_bioguide_id is null
  and review_disposition is null
  and not requires_manual_review
