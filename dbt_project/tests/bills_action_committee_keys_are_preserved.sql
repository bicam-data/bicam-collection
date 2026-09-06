with source_keys as (
    select distinct action_id::text as action_id, committee_code
    from {{ ref('stg_congress__bill_actions_committees') }}
), published_keys as (
    select action_id, committee_code
    from {{ ref('bills_actions_committees') }}
)
select source_keys.*
from source_keys
left join published_keys
  on published_keys.action_id = source_keys.action_id
 and published_keys.committee_code is not distinct from source_keys.committee_code
where published_keys.action_id is null
