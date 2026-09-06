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
        sponsor_item.value as sponsor_json
    from detail
    cross join lateral {{ unnest_array('bill_json', 'sponsors', 'sponsor_item') }}
)

select
    bill_id,
    nullif(sponsor_json #>> '{bioguideId}', '') as bioguide_id,
    nullif(sponsor_json #>> '{fullName}', '') as display_name,
    nullif(sponsor_json #>> '{party}', '') as party,
    nullif(sponsor_json #>> '{state}', '') as state,
    (nullif(sponsor_json #>> '{district}', ''))::integer as district,
    case
        when upper(coalesce(nullif(sponsor_json #>> '{isByRequest}', ''), 'N')) = 'Y' then true
        else false
    end as is_by_external_request
from unnested
