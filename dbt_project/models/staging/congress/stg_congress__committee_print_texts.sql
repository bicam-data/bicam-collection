{{ config(tags=["committee_prints", "staging"]) }}

with formats as (
    select
        print_id,
        raw_response_version_id,
        child_fetched_at,
        format_json,
        {{ json_text('format_json', 'type') }} as format_type,
        {{ json_text('format_json', 'isErrata') }} as is_errata,
        {{ json_text('format_json', 'url') }} as format_url,
        row_number() over (
            partition by print_id,
                         {{ json_text('format_json', 'type') }},
                     coalesce({{ json_text('format_json', 'isErrata') }}, '')
            order by child_fetched_at desc nulls last,
                     raw_response_version_id desc nulls last,
                     {{ json_text('format_json', 'url') }} desc nulls last
        ) as rendition_rank
    from {{ ref('int_congress_committee_print_text_formats') }}
), text_items as (
    select print_id, format_type, format_url
    from formats
    where rendition_rank = 1
)

select
    print_id,
    null::text as raw_text,
    max(case when format_type = 'PDF' then format_url end) as pdf,
    max(case when format_type = 'Formatted Text' then format_url end) as formatted_text,
    max(case when format_type = 'Formatted XML' then format_url end) as xml,
    max(case when format_type = 'Generated HTML' then format_url end) as html
from text_items
group by 1
