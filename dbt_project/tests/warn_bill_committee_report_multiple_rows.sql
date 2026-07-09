{{ config(severity='warn') }}

select
    endpoint_url,
    jsonb_array_length(coalesce(response -> 'committeeReports', '[]'::jsonb)) as report_count
from {{ source('raw', 'raw_responses') }}
where source = 'congress'
  and category = 'bills'
  and node_type = 'bill_committee_report_detail'
  and jsonb_array_length(coalesce(response -> 'committeeReports', '[]'::jsonb)) > 1
