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
references_unnested as (
    select
        reports.package_id,
        reports.report_set_id,
        {{ govinfo_committeereport_report_id('reports.report_set_id', '0', 'reports.is_errata') }} as report_id,
        reports.is_errata,
        reference.value as reference_json,
        reference.ordinality as reference_ordinality
    from reports
    join package_detail
      on reports.raw_id = package_detail.raw_id
    cross join lateral jsonb_array_elements(coalesce(package_json->'references', '[]'::jsonb))
      with ordinality as reference(value, ordinality)
    where {{ json_text('reference.value', 'collectionCode') }} = 'PLAW'
),
contents_unnested as (
    select
        package_id,
        report_set_id,
        report_id,
        is_errata,
        content.value as content_json,
        content.ordinality as content_ordinality
    from references_unnested
    cross join lateral jsonb_array_elements(coalesce(reference_json->'contents', '[]'::jsonb))
      with ordinality as content(value, ordinality)
)

select
    report_id,
    report_set_id,
    package_id,
    'pl'
    || {{ json_int('content_json', 'congress') }}::text
    || '-'
    || {{ json_text('content_json', 'number') }} as law_id,
    lower(split_part({{ json_text('content_json', 'label') }}, ' ', 1)) as law_type,
    {{ json_int('content_json', 'congress') }}::text
    || '-'
    || {{ json_text('content_json', 'number') }} as law_number,
    {{ json_int('content_json', 'congress') }} as congress,
    {{ json_int('content_json', 'number') }} as order_number
from contents_unnested
