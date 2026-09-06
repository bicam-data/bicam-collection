with observed as (
    select
        endpoint_url,
        jsonb_array_length(coalesce(response -> 'committeeReports', '[]'::jsonb)) as report_count
    from {{ source('raw', 'raw_responses') }}
    where source = 'congress'
      and category = 'bills'
      and node_type = 'bill_committee_report_detail'
), allowlist as (
    select endpoint_url, expected_report_count
    from {{ ref('bill_committee_report_multiplicity_allowlist') }}
)
select allowlist.endpoint_url
from allowlist
left join observed using (endpoint_url)
where observed.endpoint_url is null
   or observed.report_count <> allowlist.expected_report_count
