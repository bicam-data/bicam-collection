{{ config(tags=["committee_prints", "staging"]) }}

with detail_prints as (
    select
        print.value as print_json
    from {{ source('raw', 'raw_responses') }} as raw
    cross join lateral {{ unnest_array('response', 'committeePrint', 'print') }}
    where raw.source = 'congress'
      and raw.category = 'committee-prints'
      and raw.node_type = 'committee_print_detail'
),
print_lookup as (
    select
        lower(left({{ json_text('print_json', 'chamber') }}, 1))
        || 'prt'
        || {{ json_int('print_json', 'jacketNumber') }}::text
        || '-'
        || {{ json_int('print_json', 'congress') }}::text as print_id,
        {{ json_text('print_json', 'text,url') }} as text_url
    from detail_prints
),
text_responses as (
    {{ resolve_child('committee_print_detail', 'committee_print_texts', 'committee-prints') }}
),
text_items as (
    select
        print_lookup.print_id,
        text_item.value as text_json
    from text_responses
    inner join print_lookup
        on text_responses.endpoint_url = print_lookup.text_url
    cross join lateral {{ unnest_array('response', 'text', 'text_item') }}
)

select
    print_id,
    null::text as raw_text,
    max(case when {{ json_text('text_json', 'type') }} = 'PDF' then {{ json_text('text_json', 'url') }} end) as pdf,
    max(case when {{ json_text('text_json', 'type') }} = 'Formatted Text' then {{ json_text('text_json', 'url') }} end) as formatted_text,
    max(case when {{ json_text('text_json', 'type') }} = 'Formatted XML' then {{ json_text('text_json', 'url') }} end) as xml,
    max(case when {{ json_text('text_json', 'type') }} = 'Generated HTML' then {{ json_text('text_json', 'url') }} end) as html
from text_items
group by 1
