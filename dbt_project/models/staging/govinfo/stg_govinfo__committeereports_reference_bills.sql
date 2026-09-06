{{ config(tags=["govinfo_committee_reports", "staging"]) }}

with granules as (
    select
        report_id,
        report_set_id,
        package_id,
        granule_id
    from {{ ref('stg_govinfo__committeereports_granules') }}
),
granule_detail as (
    select response as granule_json
    from {{ ref('int_raw_responses__current') }}
    where source = 'govinfo'
      and category = 'govinfo-committee-reports'
      and node_type = 'govinfo_committee_report_granule_detail'
),
references_unnested as (
    select
        granules.report_id,
        granules.report_set_id,
        granules.package_id,
        granules.granule_id,
        reference.value as reference_json
    from granules
    join granule_detail
      on granules.package_id = {{ json_text('granule_json', 'packageId') }}
     and granules.granule_id = {{ json_text('granule_json', 'granuleId') }}
    cross join lateral {{ unnest_array('granule_json', 'references', 'reference') }}
),
bill_references as (
    select
        report_id,
        report_set_id,
        package_id,
        granule_id,
        reference_json,
        content.value as content_json
    from references_unnested
    cross join lateral {{ unnest_array('reference_json', 'contents', 'content') }}
    where {{ json_text('reference_json', 'collectionCode') }} = 'BILLS'
)

select
    report_id,
    report_set_id,
    package_id,
    granule_id,
    {{ build_custom_id([
        "lower(" ~ json_text('content_json', 'type') ~ ")",
        json_text('content_json', 'number')
    ]) }} || '-' || {{ json_int('content_json', 'congress') }}::text as bill_id,
    lower({{ json_text('content_json', 'type') }}) as bill_type,
    {{ json_text('content_json', 'number') }} as bill_number,
    {{ json_int('content_json', 'congress') }} as congress
from bill_references
