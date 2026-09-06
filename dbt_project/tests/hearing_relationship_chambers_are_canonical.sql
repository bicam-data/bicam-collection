select 'committee_meeting' as relationship_source, hearing_id, chamber
from {{ ref('stg_congress__committee_meeting_hearings') }}
where chamber not in ('house', 'senate', 'joint')
   or hearing_id ~ '^nhrg'
   or (chamber = 'joint' and hearing_id !~ '^jhrg')

union all

select 'nomination' as relationship_source, hearing_id, chamber
from {{ ref('stg_congress__nomination_hearings') }}
where chamber not in ('house', 'senate', 'joint')
   or hearing_id ~ '^nhrg'
   or (chamber = 'joint' and hearing_id !~ '^jhrg')
