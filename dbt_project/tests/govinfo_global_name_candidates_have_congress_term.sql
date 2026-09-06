{{ config(tags=['reconciliation', 'quality', 'identity_review']) }}

-- A globally unique name is not sufficient evidence for a historical link.
-- When GovInfo supplies a Congress, the resolved Bioguide must have a
-- Congress.gov term in that Congress; otherwise the reference belongs in the
-- explicit identity-review queue.
select
    refs.member_reference_bridge_id,
    refs.source_member_reference_key,
    refs.congress,
    refs.resolved_bioguide_id
from {{ ref('bridge_govinfo__member_references') }} as refs
where refs.resolution_status = 'unique_name_global_candidate'
  and refs.congress is not null
  and not exists (
      select 1
      from {{ ref('stg_congress__member_terms') }} as terms
      where terms.bioguide_id = refs.resolved_bioguide_id
        and terms.congress = refs.congress
  )
