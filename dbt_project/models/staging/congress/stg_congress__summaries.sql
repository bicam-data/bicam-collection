{{ config(tags=["bills", "summaries", "staging"]) }}

with listed as (
    select
        raw_id,
        endpoint_url,
        raw_response_version_id,
        summary.value as summary_json
    from {{ ref('int_raw_responses__current') }}
    cross join lateral jsonb_array_elements(
        coalesce(response -> 'summaries', '[]'::jsonb)
    ) as summary(value)
    where source = 'congress'
      and category = 'summaries'
      and node_type = 'summary_list'
), identified as (
    select
        raw_id,
        endpoint_url,
        raw_response_version_id,
        summary_json,
        lower(nullif(summary_json #>> '{bill,type}', '')) as bill_type,
        {{ congress_bill_number("summary_json #> '{bill}'") }} as bill_number,
        nullif(summary_json #>> '{bill,congress}', '')::integer as congress,
        nullif(summary_json #>> '{actionDate}', '')::date as action_date,
        nullif(summary_json #>> '{actionDesc}', '') as action_desc,
        nullif(summary_json #>> '{versionCode}', '') as version_code,
        nullif(summary_json #>> '{updateDate}', '')::timestamptz as updated_at
    from listed
), normalized as (
    select
        *,
        {{ build_custom_id(["bill_type", "bill_number"]) }} || '-' || coalesce(congress::text, '') as bill_id
    from identified
), deduped as (
    select
        *,
        row_number() over (
            partition by bill_id, action_date, action_desc, coalesce(version_code, '')
            order by updated_at desc nulls last, raw_id desc
        ) as _rn
    from normalized
)

select
    {{ hash_to_uuid(
        "bill_id || '|' || coalesce(action_date::text, '') || '|' || coalesce(action_desc, '') || '|' || coalesce(version_code, '')"
    ) }} as summary_id,
    raw_id,
    endpoint_url,
    raw_response_version_id,
    bill_id,
    bill_type,
    bill_number,
    congress,
    nullif(summary_json #>> '{bill,url}', '') as bill_url,
    nullif(summary_json #>> '{bill,title}', '') as bill_title,
    nullif(summary_json #>> '{bill,originChamber}', '') as bill_origin_chamber,
    nullif(summary_json #>> '{bill,originChamberCode}', '') as bill_origin_chamber_code,
    action_date,
    action_desc,
    nullif(summary_json #>> '{currentChamber}', '') as current_chamber,
    nullif(summary_json #>> '{currentChamberCode}', '') as current_chamber_code,
    nullif(summary_json #>> '{lastSummaryUpdateDate}', '')::timestamptz as last_summary_updated_at,
    nullif(summary_json #>> '{text}', '') as text,
    version_code,
    updated_at,
    nullif(summary_json #>> '{bill,updateDateIncludingText}', '')::timestamptz as bill_updated_text_at
from deduped
where _rn = 1
