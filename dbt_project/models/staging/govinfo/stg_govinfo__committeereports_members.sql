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
    from {{ source('raw', 'raw_responses') }}
    where source = 'govinfo'
      and category = 'govinfo-committee-reports'
      and node_type = 'govinfo_committee_report_granule_detail'
),
unnested as (
    select
        granules.report_id,
        granules.report_set_id,
        granules.package_id,
        granules.granule_id,
        member.value as member_json
    from granules
    join granule_detail
      on granules.package_id = {{ json_text('granule_json', 'packageId') }}
     and granules.granule_id = {{ json_text('granule_json', 'granuleId') }}
    cross join lateral {{ unnest_array('granule_json', 'members', 'member') }}
)

select
    report_id,
    report_set_id,
    package_id,
    granule_id,
    {{ json_text('member_json', 'bioGuideId') }} as bioguide_id,
    {{ json_text('member_json', 'name.0.parsed') }} as parsed_name,
    {{ json_text('member_json', 'name.0.authority-fnf') }} as direct_order_name,
    {{ json_text('member_json', 'name.0.authority-lnf') }} as inverted_order_name,
    {{ json_text('member_json', 'name.0.authority-other') }} as other_name,
    {{ json_text('member_json', 'authorityId') }} as authority_id,
    {{ json_text('member_json', 'gpoId') }} as gpo_id
from unnested
