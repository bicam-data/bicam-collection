{{ config(tags=['reconciliation', 'quality', 'identity_review']) }}

-- Every populated bridge ID must be a real Congress.gov member identifier.
select
    member_reference_bridge_id,
    source_member_reference_key,
    resolved_bioguide_id
from {{ ref('bridge_govinfo__member_references') }} as refs
where resolved_bioguide_id is not null
  and (
      resolved_bioguide_id !~ '^[A-Z][0-9]{6}$'
      or not exists (
          select 1
          from {{ ref('stg_congress__member_detail') }} as members
          where members.bioguide_id = refs.resolved_bioguide_id
      )
  )
