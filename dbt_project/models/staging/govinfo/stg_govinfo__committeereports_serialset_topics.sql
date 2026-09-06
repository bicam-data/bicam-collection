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
        raw_id,
        response as package_json
    from {{ ref('int_raw_responses__current') }}
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
    -- GovInfo encodes topics as either an array or a single scalar string.
    -- Normalize the latter to a singleton array while preserving the value.
    cross join lateral jsonb_array_elements_text(
        case
            when package_json->'subjects'->'topics' is null
              or jsonb_typeof(package_json->'subjects'->'topics') = 'null'
                then '[]'::jsonb
            when jsonb_typeof(package_json->'subjects'->'topics') = 'array'
                then package_json->'subjects'->'topics'
            else jsonb_build_array(package_json->'subjects'->'topics')
        end
    )
      as topic(value)
    where reports.package_id like 'SERIALSET-%'
)

select
    report_id,
    report_set_id,
    package_id,
    topic
from topics
