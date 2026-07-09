{{ config(tags=["bills", "staging"]) }}

with amendment_responses as (
    {{ resolve_child('bill_detail', 'bill_amendments', 'bills') }}
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
        amendment_item.value as amendment_json
    from amendment_responses
    join bill_detail
      on amendment_responses.parent_raw_id = bill_detail.raw_id
    cross join lateral {{ unnest_array('response', 'amendments', 'amendment_item') }}
)

select
    bill_id,
    {{ build_custom_id([
        "lower(nullif(amendment_json #>> '{type}', ''))",
        "amendment_json #>> '{number}'"
    ]) }} || '-' || coalesce(amendment_json #>> '{congress}', '') as amendment_id,
    lower(nullif(amendment_json #>> '{type}', '')) as amendment_type,
    (nullif(amendment_json #>> '{number}', ''))::integer as amendment_number,
    (nullif(amendment_json #>> '{congress}', ''))::integer as congress
from unnested
