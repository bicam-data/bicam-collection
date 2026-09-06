select hearing_id, chamber, source_chamber, citation
from {{ ref('int_hearings__congress') }}
where chamber not in ('house', 'senate', 'joint')
