{{ config(tags=["crs_reports", "staging"]) }}

with details as (
    select
        raw_id,
        endpoint_url,
        raw_response_version_id,
        coalesce(response -> 'CRSReport', response) as report_json
    from {{ ref('int_raw_responses__current') }}
    where source = 'congress'
      and category = 'crs-reports'
      and node_type = 'crs_report_detail'
)

select
    raw_id,
    endpoint_url,
    raw_response_version_id,
    nullif(report_json #>> '{id}', '') as crs_report_id,
    nullif(report_json #>> '{contentType}', '') as content_type,
    nullif(report_json #>> '{status}', '') as status,
    nullif(report_json #>> '{title}', '') as title,
    nullif(report_json #>> '{summary}', '') as summary,
    (nullif(report_json #>> '{publishDate}', ''))::timestamptz as published_at,
    (nullif(report_json #>> '{updateDate}', ''))::timestamptz as updated_at,
    (nullif(report_json #>> '{version}', ''))::integer as version,
    nullif(report_json #>> '{url}', '') as url,
    report_json -> 'authors' as authors,
    report_json -> 'formats' as formats,
    report_json -> 'relatedMaterials' as related_materials,
    report_json -> 'topics' as topics
from details
