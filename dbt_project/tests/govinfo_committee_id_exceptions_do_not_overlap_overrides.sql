/* An authority cannot be both an explicit canonical override and a reviewed
   unmapped exception. Overlap would make the exception silently unreachable
   because canonical overrides take precedence in the map. */
select exceptions.source_committee_id
from {{ ref('govinfo_committee_id_exceptions') }} as exceptions
join {{ ref('govinfo_committee_id_overrides') }} as overrides
  on lower(exceptions.source_committee_id) = lower(overrides.source_committee_id)
