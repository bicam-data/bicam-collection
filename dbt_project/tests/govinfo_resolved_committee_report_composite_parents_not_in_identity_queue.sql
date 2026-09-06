/* A committee-report composite parent is not unresolved when all named
   components have evidence-backed Bioguide IDs in the component bridge. */
with resolved_parents as (
    select source_member_key
    from {{ ref('bridge_govinfo__member_components') }}
    group by source_member_key
    having bool_and(resolved_bioguide_id is not null)
       and bool_and(not requires_manual_review)
), queued as (
    select source_member_key
    from {{ ref('fct_govinfo_member_identity_exceptions') }}
)
select resolved_parents.source_member_key
from resolved_parents
join queued using (source_member_key)
