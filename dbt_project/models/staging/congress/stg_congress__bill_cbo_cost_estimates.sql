{{ config(tags=["bills", "staging"]) }}

with detail as (
    select
        bill_detail.bill_id,
        raw_responses.response -> 'bill' as bill_json
    from {{ source('raw', 'raw_responses') }} as raw_responses
    join {{ ref('stg_congress__bill_detail') }} as bill_detail
      on raw_responses.id = bill_detail.raw_id
    where raw_responses.source = 'congress'
      and raw_responses.category = 'bills'
      and raw_responses.node_type = 'bill_detail'
),
unnested as (
    select
        bill_id,
        estimate_item.value as estimate_json
    from detail
    cross join lateral {{ unnest_array('bill_json', 'cboCostEstimates', 'estimate_item') }}
)

select
    bill_id,
    nullif(estimate_json #>> '{description}', '') as description,
    (nullif(estimate_json #>> '{pubDate}', ''))::timestamptz as pub_date,
    nullif(estimate_json #>> '{title}', '') as title,
    nullif(estimate_json #>> '{url}', '') as url
from unnested
