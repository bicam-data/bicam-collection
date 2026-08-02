{{ config(tags=["committee_reports", "staging"]) }}

with detail_responses as (
    select
        id as raw_id,
        endpoint_url,
        raw_response_version_id,
        response
    from {{ source('raw', 'raw_responses') }}
    where source = 'congress'
      and category = 'committee-reports'
      and node_type = 'committee_report_detail'
),
unnested as (
    select
        raw_id,
        endpoint_url,
        raw_response_version_id,
        report.value as report_json
    from detail_responses
    cross join lateral {{ unnest_array('response', 'committeeReports', 'report') }}
),
typed as (
    select
        raw_id,
        endpoint_url,
        raw_response_version_id,
        {{ build_custom_id([
            "lower(" ~ json_text('report_json', 'type') ~ ")",
            "(" ~ json_text('report_json', 'number') ~ ")::text"
        ]) }} || '-' || {{ json_int('report_json', 'congress') }}::text as report_id,
        {{ build_custom_id([
            "lower(" ~ json_text('report_json', 'type') ~ ")",
            "(" ~ json_text('report_json', 'number') ~ ")::text"
        ]) }}
        || '-'
        || coalesce(({{ json_int('report_json', 'part') }})::text, '1')
        || case
            when {{ json_text('report_json', 'citation') }} ilike '%errata%' then 'e'
            else ''
           end
        || '-'
        || {{ json_int('report_json', 'congress') }}::text as report_part_id,
        lower({{ json_text('report_json', 'type') }}) as report_type,
        {{ json_int('report_json', 'number') }} as report_number,
        coalesce({{ json_int('report_json', 'part') }}, 1) as report_part,
        {{ json_int('report_json', 'congress') }} as congress,
        {{ json_int('report_json', 'sessionNumber') }} as session,
        {{ json_text('report_json', 'title') }} as title,
        lower({{ json_text('report_json', 'chamber') }}) as chamber,
        {{ json_boolean('report_json', 'isConferenceReport', false) }} as is_conference_report,
        {{ json_text('report_json', 'citation') }} ilike '%errata%' as is_errata,
        {{ json_timestamptz('report_json', 'issueDate') }} as issued_at,
        {{ json_text('report_json', 'citation') }} as citation,
        {{ json_int('report_json', 'text,count', 0) }} as texts_count,
        {{ json_timestamptz('report_json', 'updateDate') }} as updated_at
    from unnested
),
deduped as (
    select
        *,
        row_number() over (
            partition by report_part_id
            order by updated_at desc nulls last, texts_count desc nulls last, citation desc nulls last
        ) as _rn
    from typed
)

select
    raw_id,
    endpoint_url,
    raw_response_version_id,
    report_id,
    report_part_id,
    report_type,
    report_number,
    report_part,
    congress,
    session,
    title,
    chamber,
    is_conference_report,
    is_errata,
    issued_at,
    citation,
    texts_count,
    updated_at
from deduped
where _rn = 1
