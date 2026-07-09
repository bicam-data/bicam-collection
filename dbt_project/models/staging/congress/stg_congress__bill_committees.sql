{{ config(tags=["bills", "staging"]) }}

with committee_responses as (
    {{ resolve_child('bill_detail', 'bill_committees', 'bills') }}
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
        committee_item.value as committee_json
    from committee_responses
    join bill_detail
      on committee_responses.parent_raw_id = bill_detail.raw_id
    cross join lateral {{ unnest_array('response', 'committees', 'committee_item') }}
)

select
    bill_id,
    nullif(committee_json #>> '{systemCode}', '') as committee_code,
    nullif(committee_json #>> '{name}', '') as committee_name
from unnested
