{{ config(tags=["laws", "staging"]) }}

with detail as (
    select
        raw_id,
        endpoint_url,
        raw_response_version_id,
        coalesce(response -> 'bill', response) as bill_json
    from {{ ref('int_raw_responses__current') }}
    where source = 'congress'
      and category = 'laws'
      and node_type = 'law_detail'
), typed as (
    select
        raw_id,
        endpoint_url,
        raw_response_version_id,
        lower({{ json_text('bill_json', 'type') }})
        || {{ congress_bill_number('bill_json') }}
        || '-' || {{ json_int('bill_json', 'congress') }}::text as bill_id,
        {{ json_int('bill_json', 'congress') }} as congress,
        lower({{ json_text('bill_json', 'type') }}) as bill_type,
        {{ congress_bill_number('bill_json') }} as bill_number,
        {{ json_text('bill_json', 'title') }} as title,
        {{ json_text('bill_json', 'originChamber') }} as origin_chamber,
        {{ json_text('bill_json', 'originChamberCode') }} as origin_chamber_code,
        {{ json_date('bill_json', 'introducedDate') }} as introduced_at,
        {{ json_date('bill_json', 'latestAction.actionDate') }} as latest_action_date,
        {{ json_text('bill_json', 'latestAction.text') }} as latest_action_text,
        {{ json_timestamptz('bill_json', 'updateDate') }} as updated_at
    from detail
)
select *
from typed
