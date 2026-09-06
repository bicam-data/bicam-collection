/* Composite source objects with a resolved component bridge are intentionally
   non-singular, not unresolved. Keep them out of the whole-object identity
   queue while retaining every component row for joins and provenance. */
with resolved_parents as (
    select source_member_reference_key
    from {{ ref('bridge_govinfo__member_reference_components') }}
    group by source_member_reference_key
    having bool_and(resolved_bioguide_id is not null)
       and bool_and(not requires_manual_review)
), queued as (
    select refs.source_member_reference_key
    from {{ ref('bridge_govinfo__member_references') }} as refs
    join {{ ref('fct_govinfo_member_reference_identity_exceptions') }} as exceptions
      on exceptions.member_reference_bridge_id = refs.member_reference_bridge_id
)
select resolved_parents.source_member_reference_key
from resolved_parents
join queued
  on queued.source_member_reference_key = resolved_parents.source_member_reference_key
