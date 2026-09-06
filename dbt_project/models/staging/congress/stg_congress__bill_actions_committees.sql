{{ config(tags=["bills", "staging"]) }}

with unnested_actions as (
    select
        bill_id,
        action_json
    from {{ ref('int_congress_bill_action_observations') }}
),
unnested_committees as (
    select
        bill_id,
        {{ hash_to_uuid("bill_id || '|' || coalesce(action_json::text, '')") }} as action_id,
        committee_item.value as committee_json
    from unnested_actions
    cross join lateral {{ unnest_array('action_json', 'committees', 'committee_item') }}
),
extracted as (
    select
        action_id,
        bill_id,
        nullif(committee_json #>> '{systemCode}', '') as committee_code,
        nullif(committee_json #>> '{name}', '') as committee_name
    from unnested_committees
), keyed as (
    select
        extracted.*,
        row_number() over (
            partition by action_id, committee_code
            order by committee_name desc nulls last
        ) as key_rank,
        committee_code is not null as key_complete
    from extracted
)

select action_id, bill_id, committee_code, committee_name
from keyed
where key_rank = 1 or not key_complete
