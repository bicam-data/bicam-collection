{{ config(tags=["govinfo_committee_reports", "staging"]) }}

with granule_detail as (
    select
        response as granule_json
    from {{ source('raw', 'raw_responses') }}
    where source = 'govinfo'
      and category = 'govinfo-committee-reports'
      and node_type = 'govinfo_committee_report_granule_detail'
),
reports as (
    select
        package_id,
        report_set_id
    from {{ ref('stg_govinfo__committeereports_packages') }}
),
typed as (
    select
        {{ json_text('granule_json', 'packageId') }} as package_id,
        {{ json_text('granule_json', 'granuleId') }} as granule_id,
        reports.report_set_id as report_set_id,
        {{ govinfo_committeereport_report_part(
            json_int('granule_json', 'partNumber'),
            json_text('granule_json', 'heading'),
            json_text('granule_json', 'granuleId'),
            'null',
            json_text('granule_json', 'granuleClass')
        ) }} as report_part,
        {{ json_text('granule_json', 'title') }} as title,
        {{ json_date('granule_json', 'dateIssued') }} as issued_at,
        {{ json_text('granule_json', 'heading') }} as heading,
        {{ govinfo_committeereport_is_errata(
            json_text('granule_json', 'title'),
            json_text('granule_json', 'heading'),
            json_text('granule_json', 'granuleId')
        ) }} as is_errata,
        {{ json_text('granule_json', 'granuleClass') }} as granule_class,
        {{ json_text('granule_json', 'collectionCode') }} as collection_code,
        {{ json_text('granule_json', 'collectionName') }} as collection_name,
        {{ json_text('granule_json', 'category') }} as category,
        lower({{ json_text('granule_json', 'docClass') }}) as doc_class,
        {{ json_timestamptz('granule_json', 'lastModified') }} as last_modified
    from granule_detail
    left join reports
      on reports.package_id = {{ json_text('granule_json', 'packageId') }}
)

select
    package_id,
    granule_id,
    {{ govinfo_committeereport_report_id('report_set_id', 'report_part', 'is_errata') }} as report_id,
    report_set_id,
    report_part,
    title,
    issued_at,
    heading,
    is_errata,
    granule_class,
    collection_code,
    collection_name,
    category,
    doc_class,
    last_modified
from typed
