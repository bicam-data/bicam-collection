{{ config(tags=["amendments", "staging"]) }}

with text_responses as (
    {{ resolve_child('amendment_detail', 'amendment_texts', 'amendments') }}
),
amendment_detail as (
    select
        raw_id,
        amendment_id
    from {{ ref('stg_congress__amendment_detail') }}
),
text_versions as (
    select
        amendment_detail.amendment_id,
        text_version.value as text_version_json
    from text_responses
    join amendment_detail
      on text_responses.parent_raw_id = amendment_detail.raw_id
    cross join lateral {{ unnest_array('response', 'textVersions', 'text_version') }}
),
formats as (
    select
        amendment_id,
        text_version_json,
        format.value as format_json
    from text_versions
    cross join lateral {{ unnest_array('text_version_json', 'formats', 'format') }}
),
pivoted as (
    select
        amendment_id,
        text_version_json,
        max(case when format_json #>> '{type}' = 'PDF' then nullif(format_json #>> '{url}', '') end) as pdf,
        max(case when format_json #>> '{type}' = 'HTML' then nullif(format_json #>> '{url}', '') end) as html
    from formats
    group by 1, 2
)

select
    amendment_id,
    {{ json_timestamptz('text_version_json', 'date') }} as date,
    {{ json_text('text_version_json', 'type') }} as type,
    null::text as raw_text,
    pdf,
    html
from pivoted
