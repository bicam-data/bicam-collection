select bridge_id, hearing_key, committee_meeting_id
from {{ ref('bridge_hearings__committee_meetings') }}
where bridge_id is null
   or hearing_key is null
   or committee_meeting_id is null
