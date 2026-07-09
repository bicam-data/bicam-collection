{{ config(tags=["bills", "staging"]) }}

with text_responses as (
    {{ resolve_child('bill_detail', 'bill_text', 'bills') }}
),
bill_detail as (
    select
        raw_id,
        bill_id
    from {{ ref('stg_congress__bill_detail') }}
),
text_versions as (
    select
        bill_detail.bill_id,
        text_version.value as text_version_json
    from text_responses
    join bill_detail
      on text_responses.parent_raw_id = bill_detail.raw_id
    cross join lateral {{ unnest_array('response', 'textVersions', 'text_version') }}
),
formats as (
    select
        bill_id,
        text_version_json,
        format.value as format_json
    from text_versions
    cross join lateral {{ unnest_array('text_version_json', 'formats', 'format') }}
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
