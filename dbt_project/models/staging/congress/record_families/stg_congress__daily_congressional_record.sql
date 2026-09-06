{{ config(tags=["daily_congressional_record", "staging"]) }}

with source_rows as (
    select
        raw_id,
        endpoint_url,
        raw_response_version_id,
        response
    from {{ ref('int_raw_responses__current') }}
    where source = 'congress'
      and category = 'daily-congressional-record'
      and node_type = 'daily_congressional_record_list'
),
unnested as (
    select
        raw_id,
        endpoint_url,
        raw_response_version_id,
        issue.value as issue_json
    from source_rows
    cross join lateral {{ unnest_array('response', 'dailyCongressionalRecord', 'issue') }}
)

select
    raw_id,
    endpoint_url,
    raw_response_version_id,
    {{ json_text('issue_json', 'volumeNumber') }} || '-' || {{ json_text('issue_json', 'issueNumber') }} as daily_record_issue_id,
    {{ json_text('issue_json', 'volumeNumber') }}::integer as volume_number,
    {{ json_text('issue_json', 'issueNumber') }} as issue_number,
    {{ json_text('issue_json', 'congress') }}::integer as congress,
    {{ json_text('issue_json', 'sessionNumber') }}::integer as session_number,
    {{ json_timestamptz('issue_json', 'issueDate') }} as issue_date,
    {{ json_timestamptz('issue_json', 'updateDate') }} as updated_at,
    {{ json_text('issue_json', 'url') }} as issue_url
from unnested
