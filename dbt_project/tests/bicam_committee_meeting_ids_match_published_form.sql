with public_ids as (
    select meeting_id from {{ ref('committee_meetings') }}
    union all select meeting_id from {{ ref('committee_meetings_associated_bills') }}
    union all select meeting_id from {{ ref('committee_meetings_associated_hearings') }}
    union all select meeting_id from {{ ref('committee_meetings_associated_nominations') }}
    union all select meeting_id from {{ ref('committee_meetings_associated_treaties') }}
    union all select meeting_id from {{ ref('committee_meetings_committees') }}
    union all select meeting_id from {{ ref('committee_meetings_meeting_documents') }}
    union all select meeting_id from {{ ref('committee_meetings_witness_documents') }}
    union all select meeting_id from {{ ref('committee_meetings_witnesses') }}
)

select meeting_id
from public_ids
where meeting_id is null
   or meeting_id !~ '^[0-9]+$'
