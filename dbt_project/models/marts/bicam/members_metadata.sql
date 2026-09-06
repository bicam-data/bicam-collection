{{ config(
    schema='bicam',
    materialized='table',
    tags=['bicam_public', 'members'],
    indexes=[
      {'columns': ['govinfo_granule_id'], 'unique': true},
      {'columns': ['bioguide_id']},
      {'columns': ['govinfo_package_id']}
    ]
) }}

/*
   Preserve the published grain: one row per Congressional Directory granule.
   Member granules carry a resolved Bioguide when source evidence permits it;
   frontmatter, organization, state-map, and unresolved member granules remain
   present with a null Bioguide.  The GovInfo granule ID is the stable row key.
   This keeps historical directory observations available without leaking
   package IDs from bills, hearings, or reports into this table.
*/
with cdir_granules as (
    select
        raw.response #>> '{granuleId}' as govinfo_granule_id,
        raw.response #>> '{packageId}' as govinfo_package_id
    from {{ ref('int_raw_responses__current') }} as raw
    where raw.source = 'govinfo'
      and raw.category = 'govinfo-congressional-directories'
      and raw.node_type = 'govinfo_congressional_directory_granule_detail'
      and nullif(trim(raw.response #>> '{granuleId}'), '') is not null
), directory_observations as (
    select
        directory_members.*,
        bridge.linkable_bioguide_id,
        bridge.resolution_status
    from {{ ref('stg_govinfo__congressional_directory_members') }} as directory_members
    left join {{ ref('bridge_govinfo__member_references') }} as bridge
      on bridge.source_member_reference_key
          = directory_members.source_member_reference_key
) , one_member_observation_per_granule as (
    select distinct on (granule_id)
        granule_id,
        linkable_bioguide_id,
        nullif(trim(gpo_id), '') as gpo_id,
        nullif(trim(authority_id), '') as authority_id
    from directory_observations
    order by
        granule_id,
        (linkable_bioguide_id is not null) desc,
        (nullif(trim(gpo_id), '') is not null) desc,
        (nullif(trim(authority_id), '') is not null) desc,
        source_member_reference_key
)
select
    observation.linkable_bioguide_id as bioguide_id,
    cdir.govinfo_granule_id,
    cdir.govinfo_package_id,
    observation.gpo_id,
    observation.authority_id
from cdir_granules as cdir
left join one_member_observation_per_granule as observation
  on observation.granule_id = cdir.govinfo_granule_id
