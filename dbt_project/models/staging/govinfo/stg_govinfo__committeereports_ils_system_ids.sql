{{ config(tags=["govinfo_committee_reports", "staging"]) }}

with reports as (
    select
        package_id,
        report_set_id,
        raw_id
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
)

select
    reports.package_id,
    reports.report_set_id,
    {{ json_text('package_json', 'otherIdentifier.ils-system-id') }} as ils_system_id
from reports
join package_detail
  on reports.raw_id = package_detail.raw_id
where {{ json_text('package_json', 'otherIdentifier.ils-system-id') }} is not null
