/* Every authorityId in a GovInfo committees[] array, including arrays nested
   below package/granule subfields, must be represented in the normalization
   map.  Unmapped values may remain reviewable; silently omitted observations
   would make the committee identity audit incomplete. */
with committee_arrays as (
    select arrays.committees_json
    from {{ ref('int_raw_responses__current') }} as raw
    cross join lateral jsonb_path_query(raw.response, '$.**.committees')
        as arrays(committees_json)
    where raw.source = 'govinfo'
      and raw.node_type in (
          'govinfo_committee_report_package_detail',
          'govinfo_committee_report_granule_detail',
          'govinfo_hearing_package_detail',
          'govinfo_hearing_granule_detail',
          'govinfo_committee_print_package_detail',
          'govinfo_committee_print_granule_detail'
      )
      and jsonb_typeof(arrays.committees_json) = 'array'
), source_ids as (
    select distinct lower(nullif(trim(committee.value #>> '{authorityId}'), ''))
        as source_committee_id
    from committee_arrays
    cross join lateral jsonb_array_elements(committee_arrays.committees_json)
        as committee(value)
    where jsonb_typeof(committee.value) = 'object'
      and nullif(trim(committee.value #>> '{authorityId}'), '') is not null
)
select source_ids.source_committee_id
from source_ids
left join {{ ref('int_govinfo_committee_id_map') }} as mapped
  on mapped.source_committee_id = source_ids.source_committee_id
where mapped.source_committee_id is null
