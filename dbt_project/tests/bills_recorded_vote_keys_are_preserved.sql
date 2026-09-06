with source_keys as (
    select distinct
        action_id::text as action_id,
        chamber,
        congress,
        session,
        roll_number
    from {{ ref('stg_congress__bill_actions_recordedvotes') }}
), published_keys as (
    select action_id, chamber, congress, session, roll_number
    from {{ ref('bills_actions_recorded_votes') }}
)
select source_keys.*
from source_keys
left join published_keys
  on published_keys.action_id = source_keys.action_id
 and published_keys.chamber is not distinct from source_keys.chamber
 and published_keys.congress is not distinct from source_keys.congress
 and published_keys.session is not distinct from source_keys.session
 and published_keys.roll_number is not distinct from source_keys.roll_number
where published_keys.action_id is null
