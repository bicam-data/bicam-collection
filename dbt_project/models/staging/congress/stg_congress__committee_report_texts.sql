{{ config(tags=["committee_reports", "staging"]) }}

with detail_reports as (
    select
        report.value as report_json
    from {{ source('raw', 'raw_responses') }} as raw
    cross join lateral {{ unnest_array('response', 'committeeReports', 'report') }}
    where raw.source = 'congress'
      and raw.category = 'committee-reports'
      and raw.node_type = 'committee_report_detail'
),
report_lookup as (
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
        {{ json_text('report_json', 'text,url') }} as text_url
    from detail_reports
),
text_responses as (
    {{ resolve_child('committee_report_detail', 'committee_report_texts', 'committee-reports') }}
),
formats as (
    select
        report_lookup.report_id,
        report_lookup.report_part_id,
        format_item.value as format_json
    from text_responses
    inner join report_lookup
        on text_responses.endpoint_url = report_lookup.text_url
    cross join lateral {{ unnest_array('response', 'text', 'text_item') }}
    cross join lateral jsonb_array_elements(
        case
            when jsonb_typeof(text_item.value -> 'formats') = 'array' then text_item.value -> 'formats'
            else '[]'::jsonb
        end
    ) as format_item(value)
)

select
    report_id,
    report_part_id,
    null::text as raw_text,
    max(case when {{ json_text('format_json', 'type') }} = 'Formatted Text' then {{ json_text('format_json', 'url') }} end) as formatted_text,
    coalesce(bool_or(case when {{ json_text('format_json', 'type') }} = 'Formatted Text' then {{ json_text('format_json', 'isErrata') }} = 'Y' end), false) as formatted_text_is_errata,
    max(case when {{ json_text('format_json', 'type') }} = 'PDF' then {{ json_text('format_json', 'url') }} end) as pdf,
    coalesce(bool_or(case when {{ json_text('format_json', 'type') }} = 'PDF' then {{ json_text('format_json', 'isErrata') }} = 'Y' end), false) as pdf_is_errata
from formats
group by 1, 2
