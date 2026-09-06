select source_member_key, bioguide_id
from {{ ref('govinfo_member_identity_overrides') }}
where bioguide_id::text !~ '^[A-Z][0-9]{6}$'
