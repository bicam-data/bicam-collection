{{ config(tags=['reconciliation', 'quality', 'identity_review']) }}

-- Every deterministic or reviewed member decision must expose a joinable ID.
select
    member_reference_bridge_id,
    source_member_reference_key,
    resolution_status,
    resolved_bioguide_id,
    linkable_bioguide_id
from {{ ref('bridge_govinfo__member_references') }}
where resolution_status in (
        'source_bioguide',
        'manual_override',
        'unique_name_context_candidate',
        'unique_name_global_candidate',
        'unique_last_name_context_candidate'
    )
  and linkable_bioguide_id is null
