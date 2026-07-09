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
        treaty.value as treaty_json
    from reports
    cross join lateral {{ unnest_array('report_json', 'associatedTreaties', 'treaty') }}
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
    || coalesce(({{ json_int('treaty_json', 'part') }})::text, '') as treaty_part_id,
    {{ json_int('treaty_json', 'congress') }} as congress,
    {{ json_int('treaty_json', 'number') }} as treaty_number,
    {{ json_int('treaty_json', 'part') }} as treaty_part
from unnested
