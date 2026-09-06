{{ config(tags=['reconciliation', 'quality', 'identity_review']) }}

select
    member_reference_component_bridge_id,
    component_key,
    resolved_bioguide_id
from {{ ref('bridge_govinfo__member_reference_components') }} as components
where resolved_bioguide_id is not null
  and (
      resolved_bioguide_id !~ '^[A-Z][0-9]{6}$'
      or not exists (
          select 1
          from {{ ref('stg_congress__member_detail') }} as members
          where members.bioguide_id = components.resolved_bioguide_id
      )
  )
