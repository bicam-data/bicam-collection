{{ config(
    tags=['govinfo_hearings', 'reconciliation', 'intermediate'],
    indexes=[
      {'columns': ['package_id', 'granule_id'], 'unique': true},
      {'columns': ['hearing_id']}
    ]
) }}

select distinct on (
    {{ json_text('raw.response', 'packageId') }},
    {{ json_text('raw.response', 'granuleId') }}
)
    raw.raw_id,
    identity_map.hearing_id,
    {{ json_text('raw.response', 'packageId') }} as package_id,
    {{ json_text('raw.response', 'granuleId') }} as granule_id,
    raw.response as granule_json,
    raw.fetched_at
from {{ ref('int_raw_responses__current') }} as raw
join {{ ref('int_govinfo_hearing_identity_map') }} as identity_map
  on identity_map.package_id = {{ json_text('raw.response', 'packageId') }}
where raw.source = 'govinfo'
  and raw.category = 'govinfo-hearings'
  and raw.node_type = 'govinfo_hearing_granule_detail'
  and {{ json_text('raw.response', 'granuleId') }} is not null
order by
    {{ json_text('raw.response', 'packageId') }},
    {{ json_text('raw.response', 'granuleId') }},
    raw.fetched_at desc,
    raw.raw_id desc
