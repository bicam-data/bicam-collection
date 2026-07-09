{{ config(tags=["bills", "staging"]) }}

with summary_responses as (
    {{ resolve_child('bill_detail', 'bill_summaries', 'bills') }}
),
bill_detail as (
    select
        raw_id,
        bill_id
    from {{ ref('stg_congress__bill_detail') }}
),
unnested as (
    select
        bill_detail.bill_id,
        summary.value as summary_json
    from summary_responses
    join bill_detail
      on summary_responses.parent_raw_id = bill_detail.raw_id
    cross join lateral {{ unnest_array('response', 'summaries', 'summary') }}
)

select
    bill_id,
    (nullif(summary_json #>> '{actionDate}', ''))::date as action_date,
    nullif(summary_json #>> '{actionDesc}', '') as action_desc,
    nullif(summary_json #>> '{text}', '') as text,
    nullif(summary_json #>> '{versionCode}', '') as version_code,
    (nullif(summary_json #>> '{updateDate}', ''))::timestamptz as updated_at
from unnested
