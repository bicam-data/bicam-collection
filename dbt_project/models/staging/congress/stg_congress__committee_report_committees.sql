{{ config(tags=["committee_reports", "staging"]) }}

with detail as (
    select
        id as parent_raw_id,
        endpoint_url as parent_endpoint_url,
        response
    from {{ source('raw', 'raw_responses') }}
    where source = 'congress'
      and category = 'committee-reports'
      and node_type = 'committee_report_detail'
),
reports as (
    select
        parent_raw_id,
        parent_endpoint_url,
        report.value as report_json
    from detail
    cross join lateral {{ unnest_array('response', 'committeeReports', 'report') }}
),
unnested as (
    select
        parent_raw_id,
        parent_endpoint_url,
        committee.value as committee_json
    from reports
    cross join lateral {{ unnest_array('report_json', 'committees', 'committee') }}
)

select
    parent_raw_id,
    parent_endpoint_url,
    {{ extract_fields('committee_json', [
        ('name', 'text'),
        ('systemCode', 'text', 'system_code'),
        ('url', 'text', 'committee_url')
    ]) }}
from unnested
