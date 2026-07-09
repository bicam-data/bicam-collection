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
topics as (
    select
        {{ govinfo_committeereport_report_id(
            'reports.report_set_id',
            "coalesce(" ~ json_int('package_json', 'documentPart') ~ ", 1)",
            'reports.is_errata'
        ) }} as report_id,
        reports.report_set_id,
        reports.package_id,
        topic.value as topic
    from reports
    join package_detail
      on reports.raw_id = package_detail.raw_id
    cross join lateral jsonb_array_elements_text(coalesce(package_json->'subjects'->'topics', '[]'::jsonb))
      as topic(value)
    where reports.package_id like 'SERIALSET-%'
)

select
    report_id,
    report_set_id,
    package_id,
    topic
from topics
