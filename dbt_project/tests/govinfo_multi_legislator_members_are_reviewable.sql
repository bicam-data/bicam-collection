select source_member_key, bioguide_id, bioguide_resolution_status
from {{ ref('stg_govinfo__committeereports_members') }}
where multi_legislator_flag
  and bioguide_resolution_status not in ('multi_legislator_review', 'manual_override')
