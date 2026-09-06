{{ config(severity='warn') }}

with observed as (
    select
        endpoint_url,
        jsonb_array_length(coalesce(response -> 'committeeReports', '[]'::jsonb)) as report_count
    from {{ source('raw', 'raw_responses') }}
    where source = 'congress'
      and category = 'bills'
      and node_type = 'bill_committee_report_detail'
      and jsonb_array_length(coalesce(response -> 'committeeReports', '[]'::jsonb)) > 1
)
select observed.endpoint_url, observed.report_count
from observed
left join {{ ref('bill_committee_report_multiplicity_allowlist') }} as allowlist
  on allowlist.endpoint_url = observed.endpoint_url
 and allowlist.expected_report_count = observed.report_count
where allowlist.endpoint_url is null
