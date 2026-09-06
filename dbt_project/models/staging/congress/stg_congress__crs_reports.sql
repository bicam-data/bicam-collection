{{ config(tags=["crs_reports", "staging"]) }}

with listed as (
    select
        raw_id,
        endpoint_url,
        raw_response_version_id,
        report.value as report_json
    from {{ ref('int_raw_responses__current') }}
    cross join lateral jsonb_array_elements(
        coalesce(response -> 'CRSReports', '[]'::jsonb)
    ) as report(value)
    where source = 'congress'
      and category = 'crs-reports'
      and node_type = 'crs_report_list'
),
deduped as (
    select
        *,
        row_number() over (
            partition by nullif(report_json #>> '{id}', '')
            order by report_json #>> '{updateDate}' desc nulls last, raw_id desc
        ) as _rn
    from listed
)

select
    raw_id,
    endpoint_url,
    raw_response_version_id,
    nullif(report_json #>> '{id}', '') as crs_report_id,
    nullif(report_json #>> '{contentType}', '') as content_type,
    nullif(report_json #>> '{status}', '') as status,
    nullif(report_json #>> '{title}', '') as title,
    (nullif(report_json #>> '{publishDate}', ''))::timestamptz as published_at,
    (nullif(report_json #>> '{updateDate}', ''))::timestamptz as updated_at,
    (nullif(report_json #>> '{version}', ''))::integer as version,
    nullif(report_json #>> '{url}', '') as url
from deduped
where _rn = 1
