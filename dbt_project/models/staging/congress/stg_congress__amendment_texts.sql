{{ config(tags=["amendments", "staging"]) }}

with formats as (
    select
        amendment_id,
        text_version_key,
        text_version_json,
        format_json
    from {{ ref('int_congress_amendment_text_formats') }}
),
format_pivoted as (
    select
        amendment_id,
        text_version_key,
        text_version_json,
        max(case when format_json #>> '{type}' = 'PDF' then nullif(format_json #>> '{url}', '') end) as pdf,
        max(case when format_json #>> '{type}' = 'HTML' then nullif(format_json #>> '{url}', '') end) as html
    from formats
    group by 1, 2, 3
), versions_without_formats as (
    select
        versions.amendment_id,
        versions.text_version_key,
        versions.text_version_json,
        null::text as pdf,
        null::text as html
    from {{ ref('int_congress_amendment_text_versions') }} as versions
    where not exists (
        select 1
        from formats
        where formats.text_version_key = versions.text_version_key
    )
), pivoted as (
    select * from format_pivoted
    union all
    select * from versions_without_formats
)

select
    amendment_id,
    {{ json_timestamptz('text_version_json', 'date') }} as date,
    {{ json_text('text_version_json', 'type') }} as type,
    null::text as raw_text,
    pdf,
    html
from pivoted
