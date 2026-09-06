{{ config(tags=["bills", "staging"]) }}

with detail as (
    select
        bill_detail.bill_id,
        raw_responses.response -> 'bill' as bill_json
    from {{ ref('int_raw_responses__current') }} as raw_responses
    join {{ ref('stg_congress__bill_detail') }} as bill_detail
      on raw_responses.raw_id = bill_detail.raw_id
    where raw_responses.source = 'congress'
      and raw_responses.category = 'bills'
      and raw_responses.node_type = 'bill_detail'
),
unnested as (
    select
        bill_id,
        law_item.value as law_json
    from detail
    cross join lateral {{ unnest_array('bill_json', 'laws', 'law_item') }}
),
parsed as (
    select
        bill_id,
        nullif(law_json #>> '{type}', '') as raw_law_type,
        nullif(law_json #>> '{number}', '') as raw_law_number
    from unnested
)

select
    bill_id,
    coalesce(raw_law_type, '') || coalesce(raw_law_number, '') as law_id,
    lower(split_part(coalesce(raw_law_type, ''), ' ', 1)) as law_type,
    raw_law_number as law_number,
    (nullif(split_part(coalesce(raw_law_number, ''), '-', 1), ''))::integer as congress,
    (nullif(split_part(coalesce(raw_law_number, ''), '-', 2), ''))::integer as order_number
from parsed
