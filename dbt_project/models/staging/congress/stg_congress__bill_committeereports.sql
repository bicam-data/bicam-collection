{{ config(tags=["bills", "staging"]) }}

with report_responses as (
    {{ resolve_child('bill_detail', 'bill_committee_report_detail', 'bills') }}
),
bill_detail as (
    select
        raw_id,
        bill_id
    from {{ ref('stg_congress__bill_detail') }}
),
selected as (
    select
        bill_detail.bill_id,
        report_responses.endpoint_url,
        report_responses.response #> '{committeeReports,0}' as report_json
    from report_responses
    join bill_detail
      on report_responses.parent_raw_id = bill_detail.raw_id
    where report_responses.response #> '{committeeReports,0}' is not null
)

select
    bill_id,
    coalesce(report_json #>> '{type}', '')
        || coalesce(report_json #>> '{number}', '')
        || '-'
        || coalesce(report_json #>> '{part}', '')
        || '-'
        || coalesce(report_json #>> '{congress}', '') as report_id,
    nullif(report_json #>> '{type}', '') as report_type,
    (nullif(report_json #>> '{number}', ''))::integer as report_number,
    coalesce((nullif(report_json #>> '{part}', ''))::integer, 1) as report_part,
    (nullif(report_json #>> '{congress}', ''))::integer as congress
from selected
