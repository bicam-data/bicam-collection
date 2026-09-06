{{ config(tags=["committee_reports", "staging"]) }}

with unnested as (
    select
        reports.report_json,
        treaty.value as treaty_json
    from {{ ref('int_congress_committee_report_records') }} as reports
    cross join lateral jsonb_array_elements(
        case
            when jsonb_typeof(reports.report_json -> 'associatedTreaties') = 'array'
                then reports.report_json -> 'associatedTreaties'
            else '[]'::jsonb
        end
    ) as treaty(value)
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
    'td'
    || {{ json_int('treaty_json', 'congress') }}::text
    || '-'
    || {{ json_int('treaty_json', 'number') }}::text as treaty_id,
    'td'
    || {{ json_int('treaty_json', 'congress') }}::text
    || '-'
    || {{ json_int('treaty_json', 'number') }}::text
    || coalesce({{ json_text('treaty_json', 'part') }}, '') as treaty_part_id,
    {{ json_int('treaty_json', 'congress') }} as congress,
    {{ json_int('treaty_json', 'number') }} as treaty_number,
    {{ json_text('treaty_json', 'part') }} as treaty_part
from unnested
