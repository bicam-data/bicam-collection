{{ config(tags=["committees", "staging"]) }}

with bill_responses as (
    {{ resolve_child('committee_detail', 'committee_bills', 'committees') }}
),
unnested as (
    select
        parent_raw_id,
        bill.value as bill_json
    from bill_responses
    cross join lateral {{ unnest_array('response', 'committee-bills.bills', 'bill') }}
),
committee_lookup as (
    select
        id as raw_id,
        response -> 'committee' as committee_json
    from {{ source('raw', 'raw_responses') }}
    where source = 'congress'
      and category = 'committees'
      and node_type = 'committee_detail'
)

select
    {{ json_text('committee_json', 'systemCode') }} as committee_id,
    {{ build_custom_id([
        "lower(" ~ json_text('bill_json', 'type') ~ ")",
        json_text('bill_json', 'number')
    ]) }} || '-' || {{ json_int('bill_json', 'congress') }}::text as bill_id,
    lower({{ json_text('bill_json', 'type') }}) as bill_type,
    {{ json_text('bill_json', 'number') }} as bill_number,
    {{ json_int('bill_json', 'congress') }} as congress,
    {{ json_text('bill_json', 'relationshipType') }} as relationship_type,
    {{ json_timestamptz('bill_json', 'actionDate') }} as committee_action_date,
    {{ json_timestamptz('bill_json', 'updateDate') }} as updated_at
from unnested
inner join committee_lookup
    on unnested.parent_raw_id = committee_lookup.raw_id
