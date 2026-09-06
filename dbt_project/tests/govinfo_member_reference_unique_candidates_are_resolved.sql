{{ config(tags=['reconciliation', 'quality', 'identity_review']) }}

-- A unique evidence-backed proposal must be the ID exposed by the bridge.
select
    refs.member_reference_bridge_id,
    refs.source_member_reference_key,
    refs.resolution_status,
    refs.resolved_bioguide_id,
    refs.candidate_bioguide_ids
from {{ ref('bridge_govinfo__member_references') }} as refs
where refs.resolution_status in (
        'unique_name_context_candidate',
        'unique_name_global_candidate',
        'unique_last_name_context_candidate'
    )
  and (
      refs.resolved_bioguide_id is null
      or cardinality(refs.candidate_bioguide_ids) <> 1
      or not (refs.resolved_bioguide_id = any(refs.candidate_bioguide_ids))
  )
