{{ config(tags=["committee_reports", "staging"]) }}

with unnested as (
    select
        reports.report_json,
        bill.value as bill_json
    from {{ ref('int_congress_committee_report_records') }} as reports
    cross join lateral jsonb_array_elements(
        case
            when jsonb_typeof(reports.report_json -> 'associatedBill') = 'array'
                then reports.report_json -> 'associatedBill'
            else '[]'::jsonb
        end
    ) as bill(value)
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
    lower({{ json_text('bill_json', 'type') }})
    || {{ normalize_congress_bill_number_text(json_text('bill_json', 'number')) }}
    || '-' || {{ json_int('bill_json', 'congress') }}::text as bill_id,
    lower({{ json_text('bill_json', 'type') }}) as bill_type,
    {{ normalize_congress_bill_number_text(json_text('bill_json', 'number')) }} as bill_number,
    {{ json_int('bill_json', 'congress') }} as congress
from unnested
