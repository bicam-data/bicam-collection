{{ config(tags=["govinfo_committee_reports", "staging"]) }}

with reports as (
    select
        raw_id,
        package_id,
        report_set_id,
        is_errata
    from {{ ref('stg_govinfo__committeereports_packages') }}
),
package_detail as (
    select
        id as raw_id,
        response as package_json
    from {{ source('raw', 'raw_responses') }}
    where source = 'govinfo'
      and category = 'govinfo-committee-reports'
      and node_type = 'govinfo_committee_report_package_detail'
),
package_committees as (
    select
        {{ govinfo_committeereport_report_id(
            'reports.report_set_id',
            "coalesce(" ~ json_int('package_json', 'documentPart') ~ ", 1)",
            'reports.is_errata'
        ) }} as report_id,
        reports.report_set_id,
        reports.package_id,
        cast(null as text) as granule_id,
        {{ json_text('committee.value', 'authorityId') }} as committee_id,
        {{ json_text('committee.value', 'committeeName') }} as committee_name
    from reports
    join package_detail
      on reports.raw_id = package_detail.raw_id
    cross join lateral {{ unnest_array('package_json', 'committees', 'committee') }}
),
granules as (
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
granule_committees as (
    select
        granules.report_id,
        granules.report_set_id,
        granules.package_id,
        granules.granule_id,
        {{ json_text('committee.value', 'authorityId') }} as committee_id,
        {{ json_text('committee.value', 'committeeName') }} as committee_name
    from granules
    join granule_detail
      on granules.package_id = {{ json_text('granule_json', 'packageId') }}
     and granules.granule_id = {{ json_text('granule_json', 'granuleId') }}
    cross join lateral {{ unnest_array('granule_json', 'committees', 'committee') }}
)

select
    report_id,
    report_set_id,
    package_id,
    granule_id,
    committee_id,
    committee_name
from package_committees

union all

select
    report_id,
    report_set_id,
    package_id,
    granule_id,
    committee_id,
    committee_name
from granule_committees
