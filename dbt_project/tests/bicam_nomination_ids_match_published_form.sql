with public_ids as (
    select nomination_id from {{ ref('nominations') }}
    union all select nomination_id from {{ ref('nominations_actions') }}
    union all select nomination_id from {{ ref('nominations_actions_committees') }}
    union all select nomination_id from {{ ref('nominations_associated_hearings') }}
    union all select nomination_id from {{ ref('nominations_committeeactivities') }}
    union all select nomination_id from {{ ref('nominations_nominees') }}
    union all select nomination_id from {{ ref('nominations_positions') }}
    union all select nomination_id from {{ ref('committee_meetings_associated_nominations') }}
)

select nomination_id
from public_ids
where nomination_id is null
   or nomination_id !~ '^PN[0-9]+-[0-9]+-[0-9]+$'
