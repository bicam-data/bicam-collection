{{ config(
    schema='bicam',
    materialized='table',
    tags=['bicam_public', 'hearings'],
    indexes=[
      {'columns': ['hearing_id', 'bioguide_id'], 'unique': true},
      {'columns': ['bioguide_id']}
    ]
) }}

/* Use the evidence-backed all-GovInfo identity bridge so absent source IDs
   are filled only where the member reconciliation has a unique resolution. */
select distinct
    identity_map.hearing_id,
    member_refs.linkable_bioguide_id::text as bioguide_id
from {{ ref('bridge_govinfo__member_references') }} as member_refs
join {{ ref('int_govinfo_hearing_identity_map') }} as identity_map
  on identity_map.package_id = member_refs.package_id
join {{ ref('hearings') }} as hearings
  on hearings.hearing_id = identity_map.hearing_id
join {{ ref('members') }} as members
  on members.bioguide_id = member_refs.linkable_bioguide_id
where member_refs.category = 'govinfo-hearings'
  and member_refs.linkable_bioguide_id is not null

