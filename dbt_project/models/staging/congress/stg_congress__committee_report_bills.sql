{{ config(tags=["committee_reports", "staging"]) }}

with detail as (
    select
        response
    from {{ source('raw', 'raw_responses') }}
    where source = 'congress'
      and category = 'committee-reports'
      and node_type = 'committee_report_detail'
),
reports as (
    select
        report.value as report_json
    from detail
    cross join lateral {{ unnest_array('response', 'committeeReports', 'report') }}
),
unnested as (
    select
        report_json,
        bill.value as bill_json
    from reports
    cross join lateral {{ unnest_array('report_json', 'associatedBill', 'bill') }}
)

select
    {{ build_custom_id([
        "lower(" ~ json_text('report_json', 'type') ~ ")",
        "(" ~ json_text('report_json', 'number') ~ ")::text"
    ]) }} || '-' || {{ json_int('report_json', 'congress') }}::text as report_id,
    {{ build_custom_id([
        "lower(" ~ json_text('report_json', 'type') ~ ")",
        "(" ~ json_text('report_json', 'number') ~ ")::text"
    ]) }}
    || '-'
    || coalesce(({{ json_int('report_json', 'part') }})::text, '1')
    || case
        when {{ json_text('report_json', 'citation') }} ilike '%errata%' then 'e'
        else ''
       end
    || '-'
    || {{ json_int('report_json', 'congress') }}::text as report_part_id,
    {{ build_custom_id([
        "lower(" ~ json_text('bill_json', 'type') ~ ")",
        json_text('bill_json', 'number')
    ]) }} || '-' || {{ json_int('bill_json', 'congress') }}::text as bill_id,
    lower({{ json_text('bill_json', 'type') }}) as bill_type,
    {{ json_text('bill_json', 'number') }} as bill_number,
    {{ json_int('bill_json', 'congress') }} as congress
from unnested
