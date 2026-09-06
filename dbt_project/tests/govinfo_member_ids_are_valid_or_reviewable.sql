select source_member_key, bioguide_id, bioguide_resolution_status
from {{ ref('stg_govinfo__committeereports_members') }}
where bioguide_id is not null
  and bioguide_id !~ '^[A-Z][0-9]{6}$'
  and bioguide_resolution_status <> 'manual_override'
