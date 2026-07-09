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
serialset_packages as (
    select
        {{ govinfo_committeereport_report_id(
            'reports.report_set_id',
            "coalesce(" ~ json_int('package_json', 'documentPart') ~ ", 1)",
            'reports.is_errata'
        ) }} as report_id,
        reports.report_set_id,
        reports.package_id,
        {{ json_text('package_json', 'serialSet.bagId') }} as bag_id,
        {{ json_text('package_json', 'serialSet.docId') }} as doc_id,
        {{ json_int('package_json', 'serialSet.serialSetNumber') }} as serialset_number,
        {{ json_text('package_json', 'agency') }} as agency,
        {{ json_text('package_json', 'volume') }} as volume,
        {{ json_text('package_json', 'parentId') }} as parent_serialset_id,
        {{ json_boolean('package_json', 'serialSet.isGLP') }} as is_glp,
        {{ json_text('package_json', 'otherIdentifier.oclc') }} as oclc_id,
        {{ json_text('package_json', 'otherIdentifier.lccn') }} as lccn,
        {{ json_text('package_json', 'otherIdentifier.issn') }} as issn,
        {{ json_boolean('package_json', 'dateIssuedNotSpecified') }} as date_issued_not_specified,
        {{ json_text('package_json', 'president.id') }} as president_id,
        {{ json_text('package_json', 'president.names.0.authority-fnf') }} as president_name
    from reports
    join package_detail
      on reports.raw_id = package_detail.raw_id
    where reports.package_id like 'SERIALSET-%'
)

select
    report_id,
    report_set_id,
    package_id,
    bag_id,
    doc_id,
    serialset_number,
    agency,
    volume,
    parent_serialset_id,
    is_glp,
    oclc_id,
    lccn,
    issn,
    date_issued_not_specified,
    president_id,
    president_name
from serialset_packages
