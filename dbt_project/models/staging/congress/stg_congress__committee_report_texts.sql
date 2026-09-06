{{ config(tags=["committee_reports", "staging"]) }}

with formats as (
    select report_id, report_part_id, format_json
    from {{ ref('int_congress_committee_report_text_formats') }}
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
