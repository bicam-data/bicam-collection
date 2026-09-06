{{ config(tags=["amendments", "staging"]) }}

with unnested_actions as (
    select
        amendment_id,
        action_json,
        source_ordinal as array_ordinal,
        {{ hash_to_uuid("amendment_id || '|' || coalesce(action_json::text, '')") }} as action_id
    from {{ ref('int_congress_amendment_action_observations') }}
),
unnested_committees as (
    select
        amendment_id,
        action_id,
        array_ordinal,
        committee_item.value as committee_json
    from unnested_actions
    cross join lateral {{ unnest_array('action_json', 'committees', 'committee_item') }}
), extracted as (
select
    amendment_id,
    action_id,
    array_ordinal,
    {{ json_text('committee_json', 'systemCode') }} as committee_code,
    {{ json_text('committee_json', 'name') }} as committee_name
from unnested_committees
), keyed as (
    select
        extracted.*,
        row_number() over (
            partition by action_id, committee_code
            order by array_ordinal desc, committee_name desc nulls last
        ) as key_rank,
        action_id is not null and committee_code is not null as key_complete
    from extracted
)

select amendment_id, action_id, committee_code, committee_name
from keyed
where key_rank = 1 or not key_complete
