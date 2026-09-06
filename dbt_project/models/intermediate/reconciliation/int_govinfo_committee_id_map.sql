{{ config(tags=["govinfo_committee_reports", "reconciliation", "quality"]) }}

{#
  GovInfo's authorityId field mixes canonical Congress committee codes with
  older THOMAS/authority identifiers.  Some older values use O/o for zero,
  but a global replacement is unsafe: valid codes such as hsgo00, hsso00,
  and hsro00 contain the letter o.  Only accept the replacement when the
  resulting value is an observed Congress.gov committee system code.
#}
with committee_arrays as (
    /* Committee arrays may be nested below documents or relationships. */
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
), committee_observations as (
    select committee.value as committee_json
    from committee_arrays
    cross join lateral jsonb_array_elements(committee_arrays.committees_json)
        as committee(value)
    where jsonb_typeof(committee.value) = 'object'
), source_ids as (
    select distinct lower(nullif(trim(committee_json #>> '{authorityId}'), ''))
        as source_committee_id
    from committee_observations
    where nullif(trim(committee_json #>> '{authorityId}'), '') is not null
),
congress_ids as (
    select distinct lower(committee_id) as committee_id
    from {{ ref('stg_congress__committee_detail') }}
    where committee_id is not null
),
manual_overrides as (
    select
        lower(nullif(trim(source_committee_id::text), '')) as source_committee_id,
        lower(nullif(trim(committee_id::text), '')) as committee_id
    from {{ ref('govinfo_committee_id_overrides') }}
    where nullif(trim(source_committee_id::text), '') is not null
      and nullif(trim(committee_id::text), '') is not null
),
reviewed_exceptions as (
    select
        lower(nullif(trim(source_committee_id::text), '')) as source_committee_id,
        classification
    from {{ ref('govinfo_committee_id_exceptions') }}
    where nullif(trim(source_committee_id::text), '') is not null
),
mapped as (
    select
        source_ids.source_committee_id,
        case
            when manual.committee_id is not null then manual.committee_id
            when exact.committee_id is not null then exact.committee_id
            when replaced.committee_id is not null then replaced.committee_id
            else null
        end as committee_id,
        case
            when manual.committee_id is not null then 'manual_override'
            when exact.committee_id is not null then 'exact_canonical'
            when replaced.committee_id is not null then 'verified_o_to_zero'
            when reviewed.classification is not null then reviewed.classification
            when source_ids.source_committee_id ~ '^[a-z0-9]+$' then 'unmapped_identifier'
            else 'non_identifier_text'
        end as normalization_status
    from source_ids
    left join manual_overrides as manual
      on manual.source_committee_id = source_ids.source_committee_id
    left join congress_ids as exact
      on exact.committee_id = source_ids.source_committee_id
    left join congress_ids as replaced
      on replaced.committee_id = regexp_replace(source_ids.source_committee_id, 'o', '0', 'g')
     and replaced.committee_id <> source_ids.source_committee_id
    left join reviewed_exceptions as reviewed
      on reviewed.source_committee_id = source_ids.source_committee_id
)

select
    source_committee_id,
    committee_id,
    normalization_status
from mapped
