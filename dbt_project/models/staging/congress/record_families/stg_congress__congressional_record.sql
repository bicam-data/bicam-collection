{{ config(tags=["congressional_record", "staging"]) }}

with source_rows as (
    select
        raw_id,
        endpoint_url,
        raw_response_version_id,
        response
    from {{ ref('int_raw_responses__current') }}
    where source = 'congress'
      and category = 'congressional-record'
      and node_type = 'congressional_record_list'
),
unnested as (
    select
        raw_id,
        endpoint_url,
        raw_response_version_id,
        issue.value as issue_json
    from source_rows
    cross join lateral jsonb_array_elements(
        coalesce(response #> '{Results,Issues}', '[]'::jsonb)
    ) as issue(value)
)

select
    raw_id,
    endpoint_url,
    raw_response_version_id,
    {{ json_int('issue_json', 'Id') }} as record_issue_id,
    {{ json_text('issue_json', 'Congress') }}::integer as congress,
    {{ json_text('issue_json', 'Issue') }} as issue_number,
    {{ json_text('issue_json', 'Volume') }}::integer as volume_number,
    {{ json_text('issue_json', 'Session') }} as session_number,
    {{ json_date('issue_json', 'PublishDate') }} as published_at,
    issue_json -> 'Links' as links
from unnested
