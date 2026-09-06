/* Paused House families are not part of the active main-11 source-gap queue. */
select child_collection, canonical_id
from {{ ref('fct_child_state_gaps') }}
where child_collection in (
    'house_requirement_matching_communications',
    'house_vote_members'
)
