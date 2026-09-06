{{ config(tags=["daily_congressional_record", "staging"]) }}

with detail as (
    select
        raw_id,
        endpoint_url,
        raw_response_version_id,
        response -> 'issue' as issue_json
    from {{ ref('int_raw_responses__current') }}
    where source = 'congress'
      and category = 'daily-congressional-record'
      and node_type = 'daily_congressional_record_detail'
)

select
    raw_id,
    endpoint_url,
    raw_response_version_id,
    {{ json_text('issue_json', 'volumeNumber') }} || '-' || {{ json_text('issue_json', 'issueNumber') }} as daily_record_issue_id,
    {{ json_text('issue_json', 'volumeNumber') }}::integer as volume_number,
    {{ json_text('issue_json', 'issueNumber') }} as issue_number,
    {{ json_int('issue_json', 'congress') }} as congress,
    {{ json_int('issue_json', 'sessionNumber') }} as session_number,
    {{ json_timestamptz('issue_json', 'issueDate') }} as issue_date,
    {{ json_timestamptz('issue_json', 'updateDate') }} as updated_at,
    {{ json_text('issue_json', 'url') }} as issue_url,
    {{ json_int('issue_json', 'fullIssue.articles.count', 0) }} as articles_count,
    issue_json -> 'fullIssue' -> 'entireIssue' as entire_issue,
    issue_json -> 'fullIssue' -> 'sections' as sections,
    issue_json -> 'fullIssue' -> 'articles' ->> 'url' as articles_url
from detail
