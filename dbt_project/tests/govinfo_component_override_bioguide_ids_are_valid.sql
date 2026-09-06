select component_key, candidate_bioguide_id
from {{ ref('govinfo_member_component_identity_overrides') }}
where upper(candidate_bioguide_id::text) !~ '^[A-Z][0-9]{6}$'
