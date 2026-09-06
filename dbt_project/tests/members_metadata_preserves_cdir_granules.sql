/* Every harvested CDIR granule is a public metadata observation, including
   frontmatter and unresolved member granules whose Bioguide is null. */
with source_granules as (
    select distinct raw.response #>> '{granuleId}' as govinfo_granule_id
    from {{ ref('int_raw_responses__current') }} as raw
    where raw.source = 'govinfo'
      and raw.category = 'govinfo-congressional-directories'
      and raw.node_type = 'govinfo_congressional_directory_granule_detail'
      and nullif(trim(raw.response #>> '{granuleId}'), '') is not null
), public_granules as (
    select govinfo_granule_id
    from {{ ref('members_metadata') }}
)
select source_granules.govinfo_granule_id
from source_granules
left join public_granules using (govinfo_granule_id)
where public_granules.govinfo_granule_id is null

union all

select public_granules.govinfo_granule_id
from public_granules
left join source_granules using (govinfo_granule_id)
where source_granules.govinfo_granule_id is null
