{{ config(
    materialized="table",
    indexes=[{"columns": ["report_record_key"], "unique": true}, {"columns": ["source_raw_id"]}]
) }}

select
    current_response.raw_id as source_raw_id,
    current_response.endpoint_url as source_endpoint_url,
    current_response.raw_response_version_id as source_raw_response_version_id,
    report.ordinality::integer as report_ordinal,
    current_response.raw_id::text || ':' || report.ordinality::text as report_record_key,
    report.value as report_json
from {{ ref('int_raw_responses__current') }} as current_response
cross join lateral jsonb_array_elements(
    case
        when jsonb_typeof(current_response.response -> 'committeeReports') = 'array'
            then current_response.response -> 'committeeReports'
        else '[]'::jsonb
    end
) with ordinality as report(value, ordinality)
where current_response.source = 'congress'
  and current_response.category = 'committee-reports'
  and current_response.node_type = 'committee_report_detail'
