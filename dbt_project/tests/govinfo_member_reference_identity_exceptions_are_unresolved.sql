{{ config(tags=['reconciliation', 'quality', 'identity_review']) }}

select
    exceptions.member_reference_bridge_id,
    exceptions.source_member_reference_key,
    refs.resolved_bioguide_id
from {{ ref('fct_govinfo_member_reference_identity_exceptions') }} as exceptions
left join {{ ref('bridge_govinfo__member_references') }} as refs
  using (member_reference_bridge_id)
where refs.resolved_bioguide_id is not null
   or refs.requires_manual_review is not true
