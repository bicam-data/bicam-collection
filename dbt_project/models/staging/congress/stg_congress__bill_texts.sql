{{ config(tags=["bills", "staging"]) }}

with formats as (
    select
        bill_id,
        text_version_json,
        format_json
    from {{ ref('int_congress_bill_text_formats') }}
),
pivoted as (
    select
        bill_id,
        text_version_json,
        max(case when format_json #>> '{type}' = 'Formatted Text' then nullif(format_json #>> '{url}', '') end) as formatted_text,
        max(case when format_json #>> '{type}' = 'PDF' then nullif(format_json #>> '{url}', '') end) as pdf,
        max(case when format_json #>> '{type}' = 'Formatted XML' then nullif(format_json #>> '{url}', '') end) as xml
    from formats
    group by 1, 2
)

select
    bill_id,
    (nullif(text_version_json #>> '{date}', ''))::timestamptz as date,
    nullif(text_version_json #>> '{type}', '') as type,
    ''::text as raw_text,
    formatted_text,
    pdf,
    xml
from pivoted
