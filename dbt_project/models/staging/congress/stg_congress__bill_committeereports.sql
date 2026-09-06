{{ config(tags=["bills", "staging"]) }}

with bill_details as (
    select
        detail.raw_id,
        detail.endpoint_url,
        detail.raw_response_version_id,
        bills.bill_id,
        bills.bill_type,
        bills.bill_number,
        bills.congress as bill_congress,
        detail.response -> 'bill' as bill_json
    from {{ ref('int_raw_responses__current') }} as detail
    join {{ ref('stg_congress__bill_detail') }} as bills
      on bills.raw_id = detail.raw_id
    where detail.source = 'congress'
      and detail.category = 'bills'
      and detail.node_type = 'bill_detail'
),
unnested as (
    select
        bill_details.raw_id,
        bill_details.endpoint_url,
        bill_details.raw_response_version_id,
        bill_details.bill_id,
        bill_details.bill_type,
        bill_details.bill_number,
        bill_details.bill_congress,
        report.value as report_json
    from bill_details
    cross join lateral jsonb_array_elements(
        case
            when jsonb_typeof(bill_details.bill_json -> 'committeeReports') = 'array'
                then bill_details.bill_json -> 'committeeReports'
            else '[]'::jsonb
        end
    ) as report(value)
),
typed as (
    select
        raw_id,
        endpoint_url,
        raw_response_version_id,
        bill_id,
        bill_type,
        bill_number,
        bill_congress,
        report_json,
        lower(coalesce(
            nullif(report_json #>> '{type}', ''),
            substring(report_json #>> '{url}' from '/committee-report/[0-9]+/([^/?]+)/')
        )) as report_type,
        coalesce(
            nullif(report_json #>> '{number}', '')::integer,
            substring(report_json #>> '{url}' from '/committee-report/[0-9]+/[A-Za-z]+/([0-9]+)')::integer
        ) as report_number,
        coalesce(
            nullif(report_json #>> '{congress}', '')::integer,
            substring(report_json #>> '{url}' from '/committee-report/([0-9]+)/')::integer
        ) as congress
    from unnested
)

select
    raw_id,
    endpoint_url,
    raw_response_version_id,
    bill_id,
    bill_type,
    bill_number,
    bill_congress,
    report_type
        || report_number::text
        || '-'
        || congress::text as report_id,
    report_type
        || report_number::text
        || '-'
        || coalesce(nullif(report_json #>> '{part}', ''), '1')
        || case
            when report_json #>> '{citation}' ilike '%errata%' then 'e'
            else ''
           end
        || '-'
        || congress::text as report_part_id,
    report_type,
    report_number,
    coalesce((nullif(report_json #>> '{part}', ''))::integer, 1) as report_part,
    congress,
    report_json #>> '{citation}' ilike '%errata%' as is_errata,
    nullif(report_json #>> '{citation}', '') as citation
from typed
