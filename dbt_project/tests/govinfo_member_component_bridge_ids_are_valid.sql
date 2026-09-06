/* Proposed and resolved component IDs must be valid Bioguide identifiers. */
select component_key
from {{ ref('bridge_govinfo__member_components') }}
where (proposed_bioguide_id is not null
       and proposed_bioguide_id !~ '^[A-Z][0-9]{6}$')
   or (resolved_bioguide_id is not null
       and resolved_bioguide_id !~ '^[A-Z][0-9]{6}$')
