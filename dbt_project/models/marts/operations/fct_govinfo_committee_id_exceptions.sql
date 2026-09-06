{{ config(
    materialized="table",
    tags=["operations", "quality", "govinfo", "identity_review"]
) }}

/* Authority IDs found in explicit committees[] arrays (including nested
   package/granule subfields) that are not currently evidenced by a
   Congress.gov committee system code. Preserve source IDs and
   package/granule/name evidence so historical reviewers can cross-reference
   the exact GovInfo records without reparsing raw JSON manually. */
with committee_arrays as (
    select
        raw.response,
        raw.node_type,
        arrays.committees_json
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
), observations as (
    select
        lower(nullif(trim(committee.value #>> '{authorityId}'), '')) as source_committee_id,
        nullif(committee_arrays.response #>> '{packageId}', '') as package_id,
        nullif(committee_arrays.response #>> '{granuleId}', '') as granule_id,
        committee_arrays.node_type,
        nullif(trim(committee.value #>> '{committeeName}'), '') as committee_name
    from committee_arrays
    cross join lateral jsonb_array_elements(committee_arrays.committees_json)
        as committee(value)
    where jsonb_typeof(committee.value) = 'object'
), exceptions as (
    select source_committee_id
    from {{ ref('int_govinfo_committee_id_map') }}
    where normalization_status in ('unmapped_identifier', 'non_identifier_text')
)
select
    exceptions.source_committee_id,
    mapped.committee_id,
    mapped.normalization_status,
    array_agg(distinct observations.node_type order by observations.node_type) as node_types,
    array_agg(distinct observations.package_id) filter (where observations.package_id is not null) as package_ids,
    array_agg(distinct observations.granule_id) filter (where observations.granule_id is not null) as granule_ids,
    array_agg(distinct observations.committee_name) filter (where observations.committee_name is not null) as committee_names,
    'pending_manual_review' as review_status
from exceptions
join {{ ref('int_govinfo_committee_id_map') }} as mapped
  on mapped.source_committee_id = exceptions.source_committee_id
left join observations
  on observations.source_committee_id = exceptions.source_committee_id
group by exceptions.source_committee_id, mapped.committee_id, mapped.normalization_status
